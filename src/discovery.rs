use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
#[cfg(feature = "mdns")]
use std::time::Duration;

#[cfg(feature = "mdns")]
use tc_error::{TCError, TCResult};

#[cfg(feature = "mdns")]
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};

#[cfg(feature = "mdns")]
use std::env;

#[cfg(feature = "mdns")]
const SERVICE_TYPE: &str = "_tinychain._tcp.local.";

/// The process-owned mDNS capability.
///
/// `mdns_sd::ServiceDaemon` is a handle to a background thread. Keeping that
/// handle here makes discovery and advertisement share one daemon whose
/// lifetime is bounded by the server, as opposed to detaching a new daemon for
/// each operation.
#[cfg(feature = "mdns")]
pub(crate) struct MdnsService {
    daemon: ServiceDaemon,
}

#[cfg(feature = "mdns")]
impl MdnsService {
    pub(crate) fn new() -> TCResult<Self> {
        let daemon = ServiceDaemon::new().map_err(mdns_error)?;
        Ok(Self { daemon })
    }

    pub(crate) async fn discover(&self, timeout_duration: Duration) -> TCResult<Vec<String>> {
        use tokio::time::{Instant, timeout};

        let receiver = self.daemon.browse(SERVICE_TYPE).map_err(mdns_error)?;
        let deadline = Instant::now() + timeout_duration;
        let mut peers = Vec::new();

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            match timeout(remaining, receiver.recv_async()).await {
                Ok(Ok(ServiceEvent::ServiceResolved(info))) => {
                    let port = info.get_port();
                    for addr in info.get_addresses() {
                        peers.push(SocketAddr::new(*addr, port).to_string());
                    }
                }
                Ok(Ok(_)) => {}
                Ok(Err(cause)) => {
                    self.daemon.stop_browse(SERVICE_TYPE).map_err(mdns_error)?;
                    return Err(TCError::internal(cause.to_string()));
                }
                Err(_) => break,
            }
        }

        self.daemon.stop_browse(SERVICE_TYPE).map_err(mdns_error)?;
        Ok(peers)
    }

    pub(crate) fn advertise(&mut self, instance: &str, ip: IpAddr, port: u16) -> TCResult<()> {
        use std::collections::HashMap;

        let hostname = env::var("TC_HOSTNAME")
            .or_else(|_| env::var("HOSTNAME"))
            .unwrap_or_else(|_| instance.to_string());

        let hostname = if hostname.ends_with(".local.") {
            hostname
        } else if hostname.ends_with(".local") {
            format!("{hostname}.")
        } else {
            format!("{hostname}.local.")
        };

        let service = ServiceInfo::new(
            SERVICE_TYPE,
            instance,
            &hostname,
            ip,
            port,
            HashMap::<String, String>::default(),
        )
        .map_err(mdns_error)?;
        self.daemon.register(service).map_err(mdns_error)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn daemon(&self) -> ServiceDaemon {
        self.daemon.clone()
    }
}

#[cfg(feature = "mdns")]
impl Drop for MdnsService {
    fn drop(&mut self) {
        let _ = self.daemon.shutdown();
    }
}

#[cfg(feature = "mdns")]
fn mdns_error(cause: impl std::fmt::Display) -> TCError {
    TCError::internal(cause.to_string())
}

pub(crate) fn self_peer(bind: SocketAddr, advertise_ip: Option<IpAddr>) -> Option<String> {
    let ip = if bind.ip().is_unspecified() {
        advertise_ip
    } else {
        Some(bind.ip())
    }?;

    Some(format!("http://{}:{}", ip, bind.port()))
}

pub(crate) fn dedupe_peers(peers: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    peers
        .into_iter()
        .filter(|peer| seen.insert(peer.clone()))
        .collect()
}

pub(crate) fn is_self(
    peer: &str,
    bind_ip: IpAddr,
    advertise_ip: Option<IpAddr>,
    port: u16,
) -> bool {
    let Ok(addr) = peer.parse::<SocketAddr>() else {
        return false;
    };

    let self_ip = if bind_ip.is_unspecified() {
        advertise_ip
    } else {
        Some(bind_ip)
    };

    self_ip.is_some_and(|ip| ip == addr.ip() && port == addr.port())
}

pub(crate) async fn discover_k8s_peers(dns: &str, port: u16) -> Vec<String> {
    let mut out = Vec::new();
    let dns = dns.trim().trim_matches('.');
    if dns.is_empty() {
        return out;
    }

    if let Ok(addrs) = (dns, port).to_socket_addrs() {
        for addr in addrs {
            out.push(addr.to_string());
        }
    }

    out
}

#[cfg(feature = "mdns")]
pub(crate) fn advertise_ip(bind: SocketAddr, override_ip: Option<IpAddr>) -> Option<IpAddr> {
    if let Some(ip) = override_ip {
        return Some(ip);
    }

    if bind.ip().is_unspecified() {
        None
    } else {
        Some(bind.ip())
    }
}
