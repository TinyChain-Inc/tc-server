use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
pub struct PeerMembership {
    peers: Arc<RwLock<HashMap<String, PeerHealth>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct PeerIdentity {
    pub peer: String,
    pub actor_id: String,
    pub public_key_b64: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct PeerDescriptor {
    pub peer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_key_b64: Option<String>,
}

#[derive(Clone, Debug)]
struct PeerHealth {
    failures: u8,
    actor_id: Option<String>,
    public_key_b64: Option<String>,
}

impl PeerMembership {
    pub fn new(initial_peers: Vec<String>) -> Self {
        let this = Self::default();
        for peer in initial_peers {
            this.upsert_active(peer);
        }

        this
    }

    pub fn upsert_active(&self, peer: String) -> bool {
        let mut peers = self.peers.write().expect("peer membership write");
        let was_new = !peers.contains_key(&peer);
        peers
            .entry(peer)
            .and_modify(|health| health.failures = 0)
            .or_insert(PeerHealth {
                failures: 0,
                actor_id: None,
                public_key_b64: None,
            });
        was_new
    }

    pub fn upsert_identity(&self, identity: PeerIdentity) -> bool {
        let mut peers = self.peers.write().expect("peer membership write");
        let was_new = !peers.contains_key(&identity.peer);
        peers
            .entry(identity.peer)
            .and_modify(|health| {
                health.failures = 0;
                health.actor_id = Some(identity.actor_id.clone());
                health.public_key_b64 = Some(identity.public_key_b64.clone());
            })
            .or_insert(PeerHealth {
                failures: 0,
                actor_id: Some(identity.actor_id),
                public_key_b64: Some(identity.public_key_b64),
            });

        was_new
    }

    pub fn mark_success(&self, peer: &str) {
        let mut peers = self.peers.write().expect("peer membership write");
        if let Some(health) = peers.get_mut(peer) {
            health.failures = 0;
        }
    }

    pub fn mark_failure(&self, peer: &str) {
        const FAILURE_THRESHOLD: u8 = 3;

        let mut peers = self.peers.write().expect("peer membership write");
        if let Some(health) = peers.get_mut(peer) {
            health.failures = health.failures.saturating_add(1);
            if health.failures >= FAILURE_THRESHOLD {
                peers.remove(peer);
            }
        }
    }

    pub fn remove(&self, peer: &str) {
        self.peers
            .write()
            .expect("peer membership write")
            .remove(peer);
    }

    pub fn retain<F>(&self, mut keep: F)
    where
        F: FnMut(&str) -> bool,
    {
        let mut peers = self.peers.write().expect("peer membership write");
        peers.retain(|peer, _| keep(peer));
    }

    pub fn active_peers(&self) -> Vec<String> {
        let peers = self.peers.read().expect("peer membership read");
        let mut peers = peers.keys().cloned().collect::<Vec<_>>();
        peers.sort();
        peers
    }

    pub fn peer_descriptors(&self) -> Vec<PeerDescriptor> {
        let peers = self.peers.read().expect("peer membership read");
        let mut out = peers
            .iter()
            .map(|(peer, health)| PeerDescriptor {
                peer: peer.clone(),
                actor_id: health.actor_id.clone(),
                public_key_b64: health.public_key_b64.clone(),
            })
            .collect::<Vec<_>>();
        out.sort_by(|left, right| left.peer.cmp(&right.peer));
        out
    }
}
