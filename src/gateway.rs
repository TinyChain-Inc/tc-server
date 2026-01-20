use futures::future::BoxFuture;
use tc_ir::TxnId;

use crate::Method;

#[derive(Debug)]
pub enum RpcError {
    InvalidTarget(String),
    Transport(String),
}

#[derive(Clone, Debug, Default)]
pub struct RpcResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub trait RpcGateway: Send + Sync + 'static {
    fn request(
        &self,
        method: Method,
        uri: String,
        txn_id: TxnId,
        bearer_token: Option<String>,
        body: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcResponse, RpcError>>;
}
