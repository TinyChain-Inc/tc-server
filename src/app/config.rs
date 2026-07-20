use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use tc_error::{TCError, TCResult};
use tinychain::replication::parse_psk_list;

const DEFAULT_BIND: &str = "0.0.0.0:8702";
const DEFAULT_DATA_DIR: &str = "/tmp/tinychain";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BootstrapReadinessMode {
    Lenient,
    Strict,
}

impl std::fmt::Display for BootstrapReadinessMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lenient => write!(f, "lenient"),
            Self::Strict => write!(f, "strict"),
        }
    }
}

fn parse_bootstrap_readiness_mode(value: &str) -> Result<BootstrapReadinessMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "lenient" => Ok(BootstrapReadinessMode::Lenient),
        "strict" => Ok(BootstrapReadinessMode::Strict),
        other => Err(format!(
            "invalid bootstrap readiness mode: {other} (expected lenient|strict)"
        )),
    }
}

fn parse_replicate_env(value: &str) -> Result<bool, String> {
    match value.trim() {
        "" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        "1" | "true" | "yes" | "on" => Ok(true),
        other => Err(format!("invalid TC_REPLICATE value: {other}")),
    }
}

fn flatten_list(items: Vec<String>) -> Vec<String> {
    items
        .into_iter()
        .flat_map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(|item| item.to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn flatten_psk_list(items: Vec<String>) -> Vec<String> {
    items
        .into_iter()
        .flat_map(|value| parse_psk_list(&value))
        .collect()
}

#[derive(Debug, Parser)]
#[command(name = "tc-server", about = "TinyChain node runtime")]
pub(crate) struct Config {
    #[arg(long, env = "TC_BIND", default_value = DEFAULT_BIND)]
    pub(crate) bind: String,

    #[arg(long, env = "TC_DATA_DIR", default_value = DEFAULT_DATA_DIR)]
    pub(crate) data_dir: PathBuf,

    #[arg(long, env = "TC_HOST_ID", default_value = "tc-server")]
    pub(crate) host_id: String,

    #[arg(
        long = "cluster-root",
        env = "TC_CLUSTER_ROOT",
        default_value = "/lib/example-devco"
    )]
    pub(crate) cluster_root: String,

    #[arg(long = "peer", env = "TC_PEERS", value_delimiter = ',', action = clap::ArgAction::Append)]
    pub(crate) peers: Vec<String>,

    #[arg(long = "psk", env = "TC_PSK_HEX", value_delimiter = ',', action = clap::ArgAction::Append)]
    pub(crate) psk_keys: Vec<String>,

    #[arg(long, env = "TC_MDNS")]
    pub(crate) mdns: bool,

    #[arg(long = "k8s-dns", env = "TC_K8S_DNS")]
    pub(crate) k8s_dns: Option<String>,

    #[arg(long = "k8s-port", env = "TC_K8S_PORT")]
    pub(crate) k8s_port: Option<u16>,

    #[arg(long = "advertise-ip", env = "TC_ADVERTISE_IP")]
    pub(crate) advertise_ip: Option<std::net::IpAddr>,

    #[arg(long, env = "TC_REPLICATE", value_parser = parse_replicate_env, default_value_t = true)]
    pub(crate) replicate: bool,

    #[arg(long = "no-replicate", action = clap::ArgAction::SetTrue)]
    pub(crate) no_replicate: bool,

    #[arg(long = "max-request-bytes", env = "TC_MAX_REQUEST_BYTES", default_value_t = 1 * 1024 * 1024)]
    pub(crate) max_request_bytes: usize,

    #[arg(
        long = "request-ttl-secs",
        env = "TC_REQUEST_TTL_SECS",
        default_value_t = 3
    )]
    pub(crate) request_ttl_secs: u64,

    #[arg(long = "trusted-installers-json", env = "TC_TRUSTED_INSTALLERS_JSON")]
    pub(crate) trusted_installers_json: Option<String>,

    #[arg(
        long = "trusted-installers-json-path",
        env = "TC_TRUSTED_INSTALLERS_JSON_PATH"
    )]
    pub(crate) trusted_installers_json_path: Option<PathBuf>,

    #[arg(
        long = "bootstrap-readiness",
        env = "TC_BOOTSTRAP_READINESS",
        default_value_t = BootstrapReadinessMode::Lenient,
        value_parser = parse_bootstrap_readiness_mode
    )]
    pub(crate) bootstrap_readiness: BootstrapReadinessMode,

    #[arg(
        long = "bootstrap-max-attempts",
        env = "TC_BOOTSTRAP_MAX_ATTEMPTS",
        default_value_t = 5
    )]
    pub(crate) bootstrap_max_attempts: u8,

    #[arg(
        long = "bootstrap-retry-delay-secs",
        env = "TC_BOOTSTRAP_RETRY_DELAY_SECS",
        default_value_t = 2
    )]
    pub(crate) bootstrap_retry_delay_secs: u64,
}

impl Config {
    pub(crate) fn parse() -> TCResult<Self> {
        let mut config = <Self as Parser>::parse();

        config.peers = flatten_list(config.peers);
        config.psk_keys = flatten_psk_list(config.psk_keys);

        if config.no_replicate {
            config.replicate = false;
        }

        if config.trusted_installers_json.is_some() && config.trusted_installers_json_path.is_some()
        {
            return Err(TCError::bad_request(
                "set only one of TC_TRUSTED_INSTALLERS_JSON or TC_TRUSTED_INSTALLERS_JSON_PATH",
            ));
        }

        Ok(config)
    }

    pub(crate) fn bind_addr(&self) -> TCResult<SocketAddr> {
        SocketAddr::from_str(&self.bind)
            .map_err(|err| TCError::bad_request(format!("invalid bind address: {err}")))
    }
}
