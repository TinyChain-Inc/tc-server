use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use tc_error::{TCError, TCResult};

const DEFAULT_BIND: &str = "0.0.0.0:8702";
const DEFAULT_DATA_DIR: &str = "/tmp/tinychain";
const DEFAULT_WORKSPACE: &str = "/tmp/tinychain-workspace";

#[derive(Debug, Parser)]
#[command(name = "tc-server", about = "TinyChain node runtime")]
pub(crate) struct Config {
    #[arg(long, env = "TC_BIND", default_value = DEFAULT_BIND)]
    pub(crate) bind: String,

    #[arg(long, env = "TC_DATA_DIR", default_value = DEFAULT_DATA_DIR)]
    pub(crate) data_dir: PathBuf,

    #[arg(long, env = "TC_WORKSPACE", default_value = DEFAULT_WORKSPACE)]
    pub(crate) workspace: PathBuf,

    #[arg(long, env = "TC_HOST_ID", default_value = "tc-server")]
    pub(crate) host_id: tc_ir::Id,

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
}

impl Config {
    pub(crate) fn parse() -> TCResult<Self> {
        let mut config = <Self as Parser>::parse();

        config.peers.retain(|peer| !peer.trim().is_empty());
        config.psk_keys.retain(|key| !key.trim().is_empty());

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
