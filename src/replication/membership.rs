use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
pub struct PeerMembership {
    peers: Arc<RwLock<HashMap<String, PeerHealth>>>,
}

#[derive(Clone, Copy, Debug)]
struct PeerHealth {
    failures: u8,
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
        peers.insert(peer, PeerHealth { failures: 0 });
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

    pub fn active_peers(&self) -> Vec<String> {
        let peers = self.peers.read().expect("peer membership read");
        let mut peers = peers.keys().cloned().collect::<Vec<_>>();
        peers.sort();
        peers
    }
}
