#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Replica {
    pub endpoint: String,
    pub host: String,
    pub actor_id: String,
    pub algorithm: rjwt::AlgKind,
    pub public_key_b64: String,
}
