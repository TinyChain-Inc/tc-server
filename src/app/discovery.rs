use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
#[cfg(feature = "mdns")]
use std::time::Duration;

#[cfg(feature = "mdns")]
use tc_error::{TCError, TCResult};

#[cfg(feature = "mdns")]
use std::env;

#[cfg(feature = "mdns")]
const SERVICE_TYPE: &str = "_tinychain._tcp.local.";

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

#[cfg(feature = "mdns")]
pub(crate) async fn discover_mdns_peers(timeout_duration: Duration) -> Vec<String> {
    use mdns_sd::{ServiceDaemon, ServiceEvent};
    use tokio::time::{Instant, timeout};

    let mut out = Vec::new();
    let daemon = match ServiceDaemon::new() {
        Ok(daemon) => daemon,
        Err(_) => return out,
    };

    let receiver = match daemon.browse(SERVICE_TYPE) {
        Ok(receiver) => receiver,
        Err(_) => return out,
    };

    let deadline = Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        match timeout(remaining, receiver.recv_async()).await {
            Ok(Ok(ServiceEvent::ServiceResolved(info))) => {
                let port = info.get_port();
                for addr in info.get_addresses() {
                    out.push(SocketAddr::new(*addr, port).to_string());
                }
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }

    out
}

#[cfg(feature = "mdns")]
pub(crate) async fn advertise_mdns(ip: IpAddr, port: u16) -> TCResult<()> {
    use mdns_sd::{ServiceDaemon, ServiceInfo};
    use std::collections::HashMap;

    let daemon = ServiceDaemon::new().map_err(|err| TCError::internal(err.to_string()))?;
    let hostname = env::var("TC_HOSTNAME")
        .or_else(|_| env::var("HOSTNAME"))
        .unwrap_or_else(|_| "tinychain".to_string());

    let hostname = if hostname.ends_with(".local") {
        hostname
    } else {
        format!("{hostname}.local.")
    };

    let service = ServiceInfo::new(
        SERVICE_TYPE,
        "node",
        &hostname,
        ip,
        port,
        HashMap::<String, String>::default(),
    )
    .map_err(|err| TCError::internal(err.to_string()))?;

    daemon
        .register(service)
        .map_err(|err| TCError::internal(err.to_string()))?;

    Ok(())
}
