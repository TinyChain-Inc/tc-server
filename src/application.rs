use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::Arc,
};

use async_hash::{Hash, Sha256};
use bytes::Bytes;
use destream::{EncodeMap, de, en};
use futures::{TryStreamExt, stream};
use pathlink::{Id, Link, PathSegment};
use semver::Version;
use tc_error::{TCError, TCResult};
use tc_ir::{Handler, Method, PutHandler, Route, Scalar, Transact, TxnId};
use tc_value::Value;

use crate::cluster::{Cluster, Dir, Resolved, Staging};

mod bootstrap;
use bootstrap::BootstrapDir;

pub(crate) type Digest = [u8; 32];
pub(crate) type Requirements = BTreeMap<Link, BTreeSet<Method>>;
type Root<T> = Cluster<Dir<T>>;

pub(crate) trait ApplicationItem:
    Clone + Route<crate::State> + Transact + Send + Sync + 'static
{
    fn identity(&self) -> &Link;
    fn digest(&self) -> Digest;
    fn bind(&self, txn: &crate::TxnHandle) -> crate::TxnHandle;
    fn staging(&self) -> Staging;
    fn has_same_content(&self, other: &Self) -> bool;
    async fn stage(&self, txn: &crate::TxnHandle) -> TCResult<()>;
}

struct LibraryRoot<'a>(&'a ApplicationOwners);
struct ClassRoot<'a>(&'a ApplicationOwners);
struct ServiceRoot<'a>(&'a ApplicationOwners);

fn install_parts(key: Scalar, value: crate::State) -> TCResult<(Link, Scalar)> {
    let Scalar::Value(Value::Link(identity)) = key else {
        return Err(TCError::bad_request(
            "application installation requires its identity as the PUT key",
        ));
    };
    Ok((identity, scalar_from_state(value)?))
}

pub(crate) fn scalar_from_state(state: crate::State) -> TCResult<Scalar> {
    match state {
        crate::State::None => Ok(Scalar::default()),
        crate::State::Scalar(scalar) => Ok(scalar),
        crate::State::Map(map) => map
            .into_iter()
            .map(|(id, state)| scalar_from_state(state).map(|state| (id, state)))
            .collect::<TCResult<_>>()
            .map(Scalar::Map),
        crate::State::Tuple(tuple) => tuple
            .into_iter()
            .map(scalar_from_state)
            .collect::<TCResult<_>>()
            .map(Scalar::Tuple),
        other => Err(TCError::bad_request(format!(
            "expected an application definition, found {other:?}"
        ))),
    }
}

impl<'a, 'owners: 'a> Handler<'a, crate::State> for LibraryRoot<'owners> {
    fn put<'txn>(self: Box<Self>) -> Option<PutHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key, value| {
            Box::pin(async move {
                let library = match value {
                    crate::State::Scalar(Scalar::Value(Value::Bytes(module)))
                        if matches!(key, Scalar::Value(Value::None)) =>
                    {
                        let root = &self.0.libraries;
                        self.0
                            .compiler
                            .module(module, |identity| root.state().leaf(identity.clone()))
                            .await?
                    }
                    value => {
                        let (identity, definition) = install_parts(key, value)?;
                        validate_identity(&identity, "lib")?;
                        let manifest = encode_definition(
                            &identity,
                            &definition,
                            crate::library::MAX_LIBRARY_BYTES,
                        )
                        .await?
                        .into();
                        let leaf = self.0.libraries.state().leaf(identity.clone());
                        crate::library::Library::from_definition(
                            leaf, identity, definition, manifest,
                        )
                        .await?
                    }
                };
                self.0.install_library(txn, library).await.map(drop)
            })
        }))
    }
}

impl<'a, 'owners: 'a> Handler<'a, crate::State> for ClassRoot<'owners> {
    fn put<'txn>(self: Box<Self>) -> Option<PutHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key, value| {
            Box::pin(async move {
                let (identity, definition) = install_parts(key, value)?;
                validate_identity(&identity, "class")?;
                let body = tc_state::ClassBody::try_from(definition.clone())
                    .map_err(|error| TCError::bad_request(error.to_string()))?;
                let manifest =
                    encode_definition(&identity, &definition, crate::class::MAX_CLASS_BYTES)
                        .await?
                        .into();
                let leaf = self.0.classes.state().leaf(identity.clone());
                let class = crate::class::Class::from_definition(leaf, identity, body, manifest)?;
                if !txn.has_claim(class.identity(), umask::USER_WRITE) {
                    return Err(TCError::unauthorized("unauthorized Class install"));
                }
                let body = class.canonical_body();
                let class = self.0.resolve_class(txn, class).await?;
                let cluster = self.0.classes.stage_item(txn, class).await?;
                cluster
                    .replicate(
                        txn,
                        crate::replication::CanonicalBody {
                            identity: cluster.state().identity().clone(),
                            body,
                            content_type: "application/json".into(),
                        },
                    )
                    .await
            })
        }))
    }
}

impl<'a, 'owners: 'a> Handler<'a, crate::State> for ServiceRoot<'owners> {
    fn put<'txn>(self: Box<Self>) -> Option<PutHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key, value| {
            Box::pin(async move {
                let (identity, definition) = install_parts(key, value)?;
                validate_identity(&identity, "service")?;
                let manifest = encode_definition(&identity, &definition, MAX_DEFINITION_BYTES)
                    .await?
                    .into();
                let leaf = self.0.services.state().leaf(identity.clone());
                let service = crate::service::Service::from_definition(
                    leaf,
                    identity.clone(),
                    definition,
                    manifest,
                )?;
                if !txn.has_claim(&identity, umask::USER_WRITE) {
                    return Err(TCError::unauthorized("unauthorized Service install"));
                }
                let body = service.canonical_body();
                let cluster = self.0.services.stage_item(txn, service).await?;
                cluster
                    .replicate(
                        txn,
                        crate::replication::CanonicalBody {
                            identity,
                            body,
                            content_type: "application/json".into(),
                        },
                    )
                    .await
            })
        }))
    }
}

#[derive(Clone)]
pub(crate) struct ApplicationScope {
    identity: Link,
    digest: Digest,
    dependencies: BTreeMap<Link, (Digest, BTreeSet<Method>)>,
}

impl ApplicationScope {
    pub(crate) fn authorize(&self, target: &Link, method: Method) -> Option<&Digest> {
        if target == &self.identity {
            return Some(&self.digest);
        }
        self.dependencies
            .get(target)
            .and_then(|(digest, methods)| methods.contains(&method).then_some(digest))
    }
}

#[derive(Clone)]
pub struct ApplicationOwners {
    pub(crate) libraries: Root<crate::library::Library>,
    pub(crate) classes: Root<crate::class::Class>,
    pub(crate) services: Root<crate::service::Service>,
    compiler: crate::library::compiler::Compiler,
}

impl ApplicationOwners {
    pub(crate) async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        self.libraries.finalize(cutoff).await?;
        self.classes.finalize(cutoff).await?;
        self.services.finalize(cutoff).await
    }

    pub async fn new(
        roots: crate::storage::ApplicationRoots,
        protocol: crate::ProtocolAuthority,
        replication: Arc<dyn crate::replication::ClusterGateway>,
    ) -> TCResult<Self> {
        let protocol = Arc::new(protocol);
        let (class_root, library_root, service_root) = roots.into_parts();
        let path = |root: &str| std::iter::once(root.parse().expect("application root")).collect();
        let compiler = crate::library::compiler::Compiler::new();
        let library_compiler = compiler.clone();
        let (class_tree, library_tree, service_tree) = tokio::try_join!(
            BootstrapDir::load(
                class_root.clone(),
                class_root.clone(),
                path("class"),
                "class",
                |leaf| crate::class::ClassDraft::load(leaf, Staging::default())
            ),
            BootstrapDir::load(
                library_root.clone(),
                library_root.clone(),
                path("lib"),
                "lib",
                move |leaf| crate::library::LibraryDraft::load(
                    library_compiler.clone(),
                    leaf,
                    Staging::default()
                )
            ),
            BootstrapDir::load(
                service_root.clone(),
                service_root.clone(),
                path("service"),
                "service",
                |leaf| crate::service::Service::load(leaf, Staging::default())
            ),
        )?;

        let class_drafts = class_tree.items();
        let library_drafts = library_tree.items();
        let services = service_tree.items();
        let digests = library_drafts
            .iter()
            .map(|item| (item.identity().clone(), item.digest()))
            .chain(
                class_drafts
                    .iter()
                    .map(|item| (item.identity().clone(), item.digest())),
            )
            .chain(
                services
                    .iter()
                    .map(|item| (item.identity().clone(), item.digest())),
            )
            .collect::<BTreeMap<_, _>>();
        let class_defs = class_drafts
            .iter()
            .map(|class| (class.identity().clone(), class.class_def().clone()))
            .collect::<BTreeMap<_, _>>();
        let class_requirements = tc_state::analyze_classes(&class_defs, class_defs.keys().cloned())
            .map_err(|error| TCError::bad_request(error.to_string()))?;

        let libraries = library_tree.try_map_items(
            Arc::clone(&protocol),
            Arc::clone(&replication),
            |library| {
                let scope = resolve_scope(
                    library.identity(),
                    library.digest(),
                    library.requirements(),
                    &digests,
                )?;
                let staging = library.staging();
                Ok((library.finish(scope), staging))
            },
        )?;
        let classes =
            class_tree.try_map_items(Arc::clone(&protocol), Arc::clone(&replication), |class| {
                let requirements = class_requirements.get(class.identity()).ok_or_else(|| {
                    TCError::bad_request(format!(
                        "missing effective Class analysis for {}",
                        class.identity()
                    ))
                })?;
                let scope =
                    resolve_scope(class.identity(), class.digest(), requirements, &digests)?;
                let staging = class.staging();
                Ok((class.finish(scope), staging))
            })?;
        let services = service_tree.try_map_items(protocol, replication, |service| {
            let staging = service.staging();
            Ok((service, staging))
        })?;
        Ok(Self {
            libraries,
            classes,
            services,
            compiler,
        })
    }

    pub(crate) async fn dispatch(
        &self,
        txn: &crate::TxnHandle,
        target: &Link,
        expected_digest: Option<&Digest>,
        method: Method,
        body: Option<crate::State>,
    ) -> TCResult<Option<crate::State>> {
        match target.path().first().map(PathSegment::as_str) {
            Some("lib") => {
                self.libraries
                    .clone()
                    .dispatch_application(
                        txn,
                        target,
                        expected_digest,
                        method,
                        body,
                        Box::new(LibraryRoot(self)),
                    )
                    .await
            }
            Some("class") => {
                self.classes
                    .clone()
                    .dispatch_application(
                        txn,
                        target,
                        expected_digest,
                        method,
                        body,
                        Box::new(ClassRoot(self)),
                    )
                    .await
            }
            Some("service") => {
                self.services
                    .clone()
                    .dispatch_application(
                        txn,
                        target,
                        expected_digest,
                        method,
                        body,
                        Box::new(ServiceRoot(self)),
                    )
                    .await
            }
            _ => Err(TCError::not_found(target.to_string())),
        }
    }

    async fn resource_digest(&self, txn: &crate::TxnHandle, identity: &Link) -> TCResult<Digest> {
        let segments = identity_segments(identity)?;
        let digest = match identity.path().first().map(PathSegment::as_str) {
            Some("lib") => self
                .libraries
                .clone()
                .exact(txn, &segments)
                .await?
                .map(|item| item.state().digest()),
            Some("class") => self
                .classes
                .clone()
                .exact(txn, &segments)
                .await?
                .map(|item| item.state().digest()),
            Some("service") => self
                .services
                .clone()
                .exact(txn, &segments)
                .await?
                .map(|item| item.state().digest()),
            _ => None,
        };
        digest.ok_or_else(|| TCError::not_found(identity.to_string()))
    }

    async fn dependency_digests(
        &self,
        txn: &crate::TxnHandle,
        batch: &BTreeMap<Link, Digest>,
        requirements: impl IntoIterator<Item = Requirements>,
    ) -> TCResult<BTreeMap<Link, Digest>> {
        let mut digests = batch.clone();
        for requirements in requirements {
            for identity in requirements.keys() {
                if !digests.contains_key(identity) {
                    let digest = self.resource_digest(txn, identity).await.map_err(|_| {
                        TCError::bad_request(format!("missing application dependency {identity}"))
                    })?;
                    digests.insert(identity.clone(), digest);
                }
            }
        }
        Ok(digests)
    }

    pub(crate) async fn install_library(
        &self,
        txn: &crate::TxnHandle,
        library: crate::library::LibraryDraft,
    ) -> TCResult<Link> {
        if !txn.has_claim(library.identity(), umask::USER_WRITE) {
            return Err(TCError::unauthorized("unauthorized Library install"));
        }
        let identity = library.identity().clone();
        let batch = BTreeMap::from([(identity.clone(), library.digest())]);
        let digests = self
            .dependency_digests(txn, &batch, [library.requirements().clone()])
            .await?;
        let scope = resolve_scope(
            library.identity(),
            library.digest(),
            library.requirements(),
            &digests,
        )?;
        let (body, content_type) = library.canonical_body();
        let cluster = self
            .libraries
            .stage_item(txn, library.finish(scope))
            .await?;
        cluster
            .replicate(
                txn,
                crate::replication::CanonicalBody {
                    identity: identity.clone(),
                    body,
                    content_type: content_type.into(),
                },
            )
            .await?;
        Ok(identity)
    }

    async fn resolve_class(
        &self,
        txn: &crate::TxnHandle,
        class: crate::class::ClassDraft,
    ) -> TCResult<crate::class::Class> {
        let definitions = BTreeMap::from([(class.identity().clone(), class.class_def().clone())]);
        let definitions = definitions.into_values().collect::<Vec<_>>();
        let requirements = self.classes.validate_batch(txn, &definitions).await?;
        let batch = BTreeMap::from([(class.identity().clone(), class.digest())]);
        let digests = self
            .dependency_digests(txn, &batch, requirements.values().cloned())
            .await?;
        let scope = resolve_scope(
            class.identity(),
            class.digest(),
            &requirements[class.identity()],
            &digests,
        )?;
        Ok(class.finish(scope))
    }
}

impl<T> Cluster<Dir<T>>
where
    T: ApplicationItem,
    crate::cluster::HostResource: From<Cluster<T>> + From<Root<T>>,
{
    async fn dispatch_application<'a>(
        self,
        txn: &'a crate::TxnHandle,
        target: &Link,
        expected_digest: Option<&Digest>,
        method: Method,
        body: Option<crate::State>,
        namespace: Box<dyn Handler<'a, crate::State> + 'a>,
    ) -> TCResult<Option<crate::State>> {
        let (_, segments, route) = split_application_link(target)?;
        match self.lookup(txn, &segments).await? {
            Resolved::Dir { cluster, unmatched } => {
                let suffix = unmatched
                    .iter()
                    .map(|id| id.as_str().parse().expect("Id is a path segment"))
                    .chain(route.iter().cloned())
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                if expected_digest.is_some() {
                    return Err(TCError::not_found(target.to_string()));
                }
                let root = (segments.is_empty() && suffix.is_empty()).then_some(namespace);
                cluster
                    .invoke(txn, suffix, method, body, root, &target.to_string())
                    .await
            }
            Resolved::Item {
                cluster,
                suffix,
                binding,
            } => {
                let suffix = suffix
                    .iter()
                    .cloned()
                    .chain(route.iter().cloned())
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                if let Some(expected) = expected_digest {
                    if cluster.state().digest() != *expected {
                        return Err(TCError::conflict(format!(
                            "application digest mismatch for {target}"
                        )));
                    }
                }
                let explicit_delete = method == Method::Delete
                    && suffix.is_empty()
                    && matches!(
                        &body,
                        Some(crate::State::None)
                            | Some(crate::State::Scalar(Scalar::Value(Value::None)))
                    );
                let txn = cluster.state().bind(txn);
                let state = cluster
                    .clone()
                    .invoke(&txn, suffix, method, body, None, &target.to_string())
                    .await?;
                if explicit_delete {
                    binding.remove(&txn).await?;
                    cluster
                        .replicate_delete(&txn, cluster.state().identity())
                        .await?;
                }
                Ok(state)
            }
        }
    }
}

fn resolve_scope(
    identity: &Link,
    digest: Digest,
    requirements: &Requirements,
    digests: &BTreeMap<Link, Digest>,
) -> TCResult<Arc<ApplicationScope>> {
    let dependencies = requirements
        .iter()
        .map(|(target, methods)| {
            let digest = digests.get(target).copied().ok_or_else(|| {
                TCError::bad_request(format!("missing application dependency {target}"))
            })?;
            Ok((target.clone(), (digest, methods.clone())))
        })
        .collect::<TCResult<_>>()?;
    Ok(Arc::new(ApplicationScope {
        identity: identity.clone(),
        digest,
        dependencies,
    }))
}

pub(crate) fn application_requirements<'a>(
    values: impl IntoIterator<Item = &'a Scalar>,
) -> Requirements {
    let mut requirements = Requirements::new();
    for value in values {
        value.visit_referenced_methods(&mut |target, method| {
            if let Ok(identity) = application_identity(target) {
                requirements.entry(identity).or_default().insert(method);
            }
        });
    }
    requirements
}

pub(crate) fn validate_identity(identity: &Link, expected_root: &str) -> TCResult<Vec<Id>> {
    if identity.host().is_some() {
        return Err(TCError::bad_request(
            "an installed application identity must be local",
        ));
    }
    let (root, segments, suffix) = split_application_link(identity)?;
    if root != expected_root || !suffix.is_empty() || !is_identity_segments(&segments) {
        return Err(TCError::bad_request(format!(
            "expected an exact /{expected_root} application identity"
        )));
    }
    Ok(segments)
}

pub(crate) fn identity_segments(identity: &Link) -> TCResult<Vec<Id>> {
    let (_, segments, suffix) = split_application_link(identity)?;
    if !suffix.is_empty() || !is_identity_segments(&segments) {
        return Err(TCError::bad_request(
            "expected an exact application identity",
        ));
    }
    Ok(segments)
}

fn is_identity_segments(segments: &[Id]) -> bool {
    segments.len() >= 3
        && segments
            .last()
            .is_some_and(|version| Version::parse(version.as_str()).is_ok())
}

pub(crate) fn application_identity(target: &Link) -> TCResult<Link> {
    let (_, segments, _) = split_application_link(target)?;
    let Some(version) = segments.last() else {
        return Err(TCError::bad_request("application target has no version"));
    };
    if Version::parse(version.as_str()).is_err() {
        return Err(TCError::bad_request(
            "application target has no terminal version",
        ));
    }
    let path = pathlink::PathBuf::from_slice(&target.path()[..segments.len() + 1]);
    Ok(match target.host().cloned() {
        Some(host) => Link::new(host, path),
        None => path.into(),
    })
}

pub(crate) fn split_application_link(target: &Link) -> TCResult<(&str, Vec<Id>, &[PathSegment])> {
    let path = target.path();
    let root = path
        .first()
        .ok_or_else(|| TCError::bad_request("an application path is empty"))?
        .as_str();
    if !matches!(root, "lib" | "class" | "service") {
        return Err(TCError::bad_request(format!(
            "unsupported application root /{root}"
        )));
    }
    let version = path[1..]
        .iter()
        .position(|segment| Version::parse(segment.as_str()).is_ok())
        .map(|offset| offset + 1);
    let structural_end = version.map_or(path.len(), |version| version + 1);
    if version.is_some_and(|version| version < 3) {
        return Err(TCError::bad_request(
            "an application identity requires a publisher and resource before its version",
        ));
    }
    let mut segments = Vec::with_capacity(structural_end.saturating_sub(1));
    for segment in &path[1..structural_end] {
        if segment.as_str() == ".txfs" {
            return Err(TCError::bad_request(
                ".txfs is a reserved application segment",
            ));
        }
        segments.push(segment.as_str().parse().map_err(|error| {
            TCError::bad_request(format!("invalid application path segment: {error}"))
        })?);
    }
    let suffix = version.map_or(&path[path.len()..], |version| &path[version + 1..]);
    Ok((root, segments, suffix))
}

pub(crate) fn definition_digest<T>(identity: &Link, definition: T) -> Digest
where
    T: Hash<Sha256>,
{
    Hash::<Sha256>::hash((identity, definition)).into()
}

#[cfg(feature = "http-server")]
pub(crate) fn parse_digest(value: &str) -> TCResult<Digest> {
    let bytes = hex::decode(value).map_err(|_| TCError::bad_request("invalid SHA-256 digest"))?;
    bytes
        .try_into()
        .map_err(|_| TCError::bad_request("a SHA-256 digest must contain 32 bytes"))
}

#[derive(Clone)]
struct Definition(Link, Scalar);

impl de::FromStream for Definition {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_context: (), decoder: &mut D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor for Visitor {
            type Value = Definition;
            fn expecting() -> &'static str {
                "one application URI mapped to its definition"
            }
            async fn visit_map<A: de::MapAccess>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let identity = map
                    .next_key::<String>(())
                    .await?
                    .ok_or_else(|| de::Error::custom("empty application definition"))?
                    .parse()
                    .map_err(de::Error::custom)?;
                let definition = map.next_value(()).await?;
                if map.next_key::<de::IgnoredAny>(()).await?.is_some() {
                    return Err(de::Error::custom(
                        "an application definition must contain exactly one URI",
                    ));
                }
                Ok(Definition(identity, definition))
            }
        }
        decoder.decode_map(Visitor).await
    }
}

impl<'en> en::IntoStream<'en> for Definition {
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        let mut map = encoder.encode_map(Some(1))?;
        map.encode_entry(self.0.to_string(), self.1)?;
        map.end()
    }
}

pub(crate) const MAX_DEFINITION_BYTES: usize = 1024 * 1024;

pub(crate) async fn encode_definition(
    identity: &Link,
    definition: &Scalar,
    bound: usize,
) -> TCResult<Vec<u8>> {
    encode_json(Definition(identity.clone(), definition.clone()), bound).await
}

pub(crate) async fn decode_definition(bytes: &[u8], bound: usize) -> TCResult<(Link, Scalar)> {
    let Definition(identity, definition) = decode_json(bytes, bound).await?;
    Ok((identity, definition))
}

pub(crate) async fn encode_json<T>(value: T, bound: usize) -> TCResult<Vec<u8>>
where
    T: for<'en> en::IntoStream<'en>,
{
    destream_json::encode(value)
        .map_err(|err| TCError::bad_request(err.to_string()))?
        .map_err(|err| io::Error::other(err.to_string()))
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            if bytes.len().saturating_add(chunk.len()) > bound {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "encoded application value exceeds its bound",
                ));
            }
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
        .map_err(|err| TCError::bad_request(err.to_string()))
}

pub(crate) async fn decode_json<T>(bytes: &[u8], bound: usize) -> TCResult<T>
where
    T: de::FromStream<Context = ()>,
{
    if bytes.len() > bound {
        return Err(TCError::bad_request("application value exceeds its bound"));
    }
    let input = stream::iter([Ok::<_, io::Error>(Bytes::copy_from_slice(bytes))]);
    destream_json::try_decode((), input)
        .await
        .map_err(|err| TCError::bad_request(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn literal_definition_is_exactly_one_entry() {
        let identity: Link = "/service/example/catalog/1.0.0".parse().unwrap();
        let definition = Scalar::from(Value::String("catalog".into()));
        let encoded = encode_definition(&identity, &definition, MAX_DEFINITION_BYTES)
            .await
            .unwrap();
        assert_eq!(
            decode_definition(&encoded, MAX_DEFINITION_BYTES)
                .await
                .unwrap(),
            (identity, definition)
        );
    }

    #[tokio::test]
    async fn literal_definition_rejects_empty_and_multiple_entries() {
        for invalid in [
            br#"{}"#.as_slice(),
            br#"{"/lib/example/a/1.0.0":{},"/lib/example/b/1.0.0":{}}"#.as_slice(),
        ] {
            let error = decode_definition(invalid, MAX_DEFINITION_BYTES)
                .await
                .unwrap_err();
            assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
        }
    }

    #[test]
    fn identity_validation_is_private_to_the_owner() {
        let identity: Link = "/lib/example/math/arithmetic/1.2.3".parse().unwrap();
        assert_eq!(validate_identity(&identity, "lib").unwrap().len(), 4);
        assert!(validate_identity(&identity, "class").is_err());
        let suffix: Link = "/lib/example/math/arithmetic/1.2.3/add".parse().unwrap();
        assert!(validate_identity(&suffix, "lib").is_err());
    }

    #[test]
    fn dependency_routes_collapse_to_their_application_identity() {
        let route: Link = "/lib/example/math/1.0.0/add".parse().unwrap();
        let reference = Scalar::from(tc_ir::TCRef::Op(tc_ir::OpRef::Get((
            tc_ir::Subject::Link(route),
            Scalar::default(),
        ))));
        let requirements = application_requirements([&reference]);
        let identity: Link = "/lib/example/math/1.0.0".parse().unwrap();
        assert_eq!(requirements.len(), 1);
        assert!(requirements[&identity].contains(&Method::Get));
    }
}
