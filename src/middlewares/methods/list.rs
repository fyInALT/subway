use async_trait::async_trait;
use jsonrpsee::core::JsonValue;
use jsonrpsee::types::ErrorObjectOwned;
use opentelemetry::trace::FutureExt;
use std::fmt::Debug;

use alloy_consensus::{Transaction, TxEip4844Variant, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::hex::decode;
use alloy_primitives::{Address, TxKind};
use serde::Serialize;

use crate::extensions::list::{BlackList, Whitelist};
use crate::utils::{AddressRule, ToAddress};
use crate::{
    middlewares::{CallRequest, CallResult, Middleware, MiddlewareBuilder, NextFn, RpcMethod, TRACER},
    utils::{TypeRegistry, TypeRegistryRef},
};

/// The related address is banned.
pub const ADDRESS_IS_BANNED: i32 = -33000;

/// The address must be parsed from the rpc parameters.
pub const UNKNOWN_ADDRESS: i32 = -33001;

///The transaction could not be decoded.
pub const ILLEGAL_TX: i32 = -33002;

/// The transaction signature is invalid.
pub const ILLEGAL_TX_SIGNATURE: i32 = -33003;

///The hex could not be decoded.
pub const ILLEGAL_HEX: i32 = -33004;

// Read rpc.
pub const ETH_CALL: &str = "eth_call";

// Write rpc.
pub const SEND_RAW_TX: &str = "eth_sendRawTransaction";
pub const SEND_TX: &str = "eth_sendTransaction";

pub type BlacklistMiddleware = ListMiddleware<BlacklistChecker>;
pub type WhitelistMiddleware = ListMiddleware<WhitelistChecker>;

/// This list middleware should be used at the top level whenever possible.
///
/// It could be a whitelist or blacklist middleware.
#[derive(Debug)]
pub struct ListMiddleware<T: ItemChecker> {
    rpc_type: RpcType,
    checker: T,
}

/// An item checker that check if an item is usable.
pub trait ItemChecker: Send + Sync + Debug {
    /// The item type.
    type Item;

    fn span_name() -> &'static str;

    /// Check if the item is usable.
    ///
    /// Examples:
    /// - For whitelist, it returns true when in list.
    /// - For blacklist, it returns false when in list.
    fn check(&self, item: &Self::Item) -> bool;

    /// Check if checker is enabled.
    fn enabled(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct BlacklistChecker {
    addresses: Vec<AddressRule>,
    enabled: bool,
}

impl BlacklistChecker {
    pub fn new(addresses: Vec<AddressRule>) -> Self {
        Self {
            // TODO: maybe still need to enable it when empty.
            enabled: !addresses.is_empty(),
            addresses,
        }
    }
}

impl ItemChecker for BlacklistChecker {
    type Item = (Address, ToAddress);

    fn span_name() -> &'static str {
        "blacklist"
    }

    fn check(&self, item: &Self::Item) -> bool {
        let (from, to) = item;
        for address in &self.addresses {
            // if satisfy address from/to
            if address.satisfy(from, to) {
                return false;
            }
        }

        true
    }

    fn enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Debug, Clone)]
pub struct WhitelistChecker {
    addresses: Vec<AddressRule>,
    enabled: bool,
}

impl WhitelistChecker {
    pub fn new(addresses: Vec<AddressRule>) -> Self {
        Self {
            // TODO: maybe still need to enable it when empty.
            enabled: !addresses.is_empty(),
            addresses,
        }
    }
}

impl ItemChecker for WhitelistChecker {
    type Item = (Address, ToAddress);

    fn span_name() -> &'static str {
        "whitelist"
    }

    fn check(&self, item: &Self::Item) -> bool {
        let (from, to) = item;
        for address in &self.addresses {
            // if satisfy address from/to
            if address.satisfy(from, to) {
                return true;
            }
        }

        false
    }

    fn enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Debug, Eq, PartialEq)]
enum RpcType {
    EthCall,
    SendRawTX,
    SendTX,
}

#[async_trait]
impl MiddlewareBuilder<RpcMethod, CallRequest, CallResult> for ListMiddleware<BlacklistChecker> {
    async fn build(
        method: &RpcMethod,
        extensions: &TypeRegistryRef,
    ) -> Option<Box<dyn Middleware<CallRequest, CallResult>>> {
        let list = extensions
            .read()
            .await
            .get::<BlackList>()
            .expect("BlackList extension not found");

        match method.method.as_str() {
            ETH_CALL => Some(Box::new(Self {
                rpc_type: RpcType::EthCall,
                checker: BlacklistChecker::new(list.config.eth_call.clone()),
            })),
            SEND_TX => Some(Box::new(Self {
                rpc_type: RpcType::SendTX,
                checker: BlacklistChecker::new(list.config.tx.clone()),
            })),
            SEND_RAW_TX => Some(Box::new(Self {
                rpc_type: RpcType::SendRawTX,
                checker: BlacklistChecker::new(list.config.tx.clone()),
            })),
            _ => {
                // other rpc types will skip this middleware.
                None
            }
        }
    }
}

#[async_trait]
impl MiddlewareBuilder<RpcMethod, CallRequest, CallResult> for ListMiddleware<WhitelistChecker> {
    async fn build(
        method: &RpcMethod,
        extensions: &TypeRegistryRef,
    ) -> Option<Box<dyn Middleware<CallRequest, CallResult>>> {
        let list = extensions
            .read()
            .await
            .get::<Whitelist>()
            .expect("Whitelist extension not found");

        match method.method.as_str() {
            ETH_CALL => Some(Box::new(Self {
                rpc_type: RpcType::EthCall,
                checker: WhitelistChecker::new(list.config.eth_call.clone()),
            })),
            SEND_TX => Some(Box::new(Self {
                rpc_type: RpcType::SendTX,
                checker: WhitelistChecker::new(list.config.tx.clone()),
            })),
            SEND_RAW_TX => Some(Box::new(Self {
                rpc_type: RpcType::SendRawTX,
                checker: WhitelistChecker::new(list.config.tx.clone()),
            })),
            _ => {
                // other rpc types will skip this middleware.
                None
            }
        }
    }
}

/// Extract the address from `eth_call`/`eth_sendTranslation` parameters and convert it into lowercase.
pub fn extract_address_from_to(params: &[JsonValue]) -> Result<(Address, ToAddress), ErrorObjectOwned> {
    let p1 = params.first().ok_or_else(err_illegal_rpc_parameter)?;

    // `from` must exist.
    let from = p1.get("from").ok_or_else(err_unknown_from_address)?;
    let from: Address = serde_json::from_value(from.clone()).map_err(|_err| {
        ErrorObjectOwned::borrowed(UNKNOWN_ADDRESS, "Could not parse `from` from rpc parameter", None)
    })?;

    // When not get, it means `Create`.
    let to = p1.get("to");
    let to = if let Some(to) = to {
        serde_json::from_value(to.clone()).map_err(|_err| {
            ErrorObjectOwned::borrowed(UNKNOWN_ADDRESS, "Could not parse `to` from rpc parameter", None)
        })?
    } else {
        TxKind::Create
    };

    Ok((from, to.into()))
}

#[async_trait]
impl<T: ItemChecker<Item = (Address, ToAddress)>> Middleware<CallRequest, CallResult> for ListMiddleware<T> {
    async fn call(
        &self,
        request: CallRequest,
        context: TypeRegistry,
        next: NextFn<CallRequest, CallResult>,
    ) -> CallResult {
        async move {
            if !self.checker.enabled() {
                return next(request, context).await;
            }

            match self.rpc_type {
                RpcType::EthCall | RpcType::SendTX => {
                    let (from, to) = extract_address_from_to(&request.params)?;
                    if !self.checker.check(&(from, to)) {
                        return Err(err_banned_address());
                    }
                }
                RpcType::SendRawTX => {
                    let p1 = request.params.first().ok_or_else(err_illegal_rpc_parameter)?;
                    let rlp_hex: String =
                        serde_json::from_value(p1.clone()).map_err(|_err| err_illegal_rpc_parameter())?;
                    let rlp = decode(rlp_hex.as_bytes()).map_err(|_err| err_failed_decode_hex(&rlp_hex))?;

                    let tx: TxEnvelope =
                        TxEnvelope::decode_2718(&mut rlp.as_slice()).map_err(|_err| err_failed_decode_txn(&rlp_hex))?;

                    let from = extract_signer_from_tx_envelop(&tx)?;
                    let to = extract_to_address_from_tx_envelop(&tx);
                    if !self.checker.check(&(from, to)) {
                        return Err(err_banned_address());
                    }
                }
            }

            next(request, context).await
        }
        .with_context(TRACER.context(T::span_name()))
        .await
    }
}

#[inline]
fn extract_signer_from_tx_envelop(tx: &TxEnvelope) -> Result<Address, ErrorObjectOwned> {
    let signer = match tx {
        TxEnvelope::Legacy(tx) => tx.recover_signer(),
        TxEnvelope::Eip2930(tx) => tx.recover_signer(),
        TxEnvelope::Eip1559(tx) => tx.recover_signer(),
        TxEnvelope::Eip4844(tx) => tx.recover_signer(),
        _ => {
            unreachable!()
        }
    }
    .map_err(|_err| ErrorObjectOwned::owned(ILLEGAL_TX_SIGNATURE, "Could not recover signer from tx", Some(tx)))?;

    Ok(signer)
}

#[inline]
fn extract_to_address_from_tx_envelop(tx: &TxEnvelope) -> ToAddress {
    let to = match tx {
        TxEnvelope::Legacy(tx) => tx.tx().to,
        TxEnvelope::Eip1559(tx) => tx.tx().to,
        TxEnvelope::Eip2930(tx) => tx.tx().to,
        TxEnvelope::Eip4844(tx) => match tx.tx() {
            TxEip4844Variant::TxEip4844(tx) => tx.to(),
            TxEip4844Variant::TxEip4844WithSidecar(tx) => tx.to(),
        },
        _ => {
            unreachable!()
        }
    };

    to.into()
}

pub fn err_banned_address() -> ErrorObjectOwned {
    ErrorObjectOwned::borrowed(ADDRESS_IS_BANNED, "The address related to the rpc is banned", None)
}

pub fn err_illegal_rpc_parameter() -> ErrorObjectOwned {
    ErrorObjectOwned::borrowed(ILLEGAL_TX, "Failed to get the first param from rpc parameter", None)
}

pub fn err_failed_decode_txn<S: Serialize>(data: S) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(ILLEGAL_TX, "Failed to decode the txn", Some(data))
}

pub fn err_failed_decode_hex<S: Serialize>(data: S) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(ILLEGAL_HEX, "Failed to decode the hex", Some(data))
}

pub fn err_unknown_from_address() -> ErrorObjectOwned {
    ErrorObjectOwned::borrowed(UNKNOWN_ADDRESS, "Could not get the `from` from rpc parameter", None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::list::{BlacklistConfig, WhitelistConfig};
    use crate::extensions::ExtensionsConfig;
    use alloy_primitives::{address, bytes};
    use futures_util::FutureExt;
    use serde_json::Value;

    struct Case {
        request: CallRequest,
        expected_res: CallResult,
        config: Config,
    }

    #[derive(Debug, Clone)]
    enum Config {
        Whitelist(WhitelistConfig),
        Blacklist(BlacklistConfig),
    }

    mod request {
        use crate::middlewares::methods::list::{ETH_CALL, SEND_RAW_TX, SEND_TX};
        use crate::middlewares::CallRequest;
        use alloy_primitives::{hex, Address, TxKind};
        use serde_json::{json, Value};

        pub fn raw_tx(encoded_tx: impl AsRef<[u8]>) -> CallRequest {
            let rlp_hex = hex::encode_prefixed(encoded_tx);
            CallRequest::new(SEND_RAW_TX, vec![Value::String(rlp_hex)])
        }

        pub fn tx(from: Address, to: TxKind) -> CallRequest {
            CallRequest::new(
                SEND_TX,
                vec![json!( {
                    "from": from,
                    "to": to,
                })],
            )
        }

        pub fn tx_with_to(to: TxKind) -> CallRequest {
            CallRequest::new(
                SEND_TX,
                vec![
                    // missing `from`
                    json!( {
                        "to": to,
                    }),
                ],
            )
        }

        pub fn eth_call(from: Address, to: TxKind) -> CallRequest {
            CallRequest::new(
                ETH_CALL,
                vec![json!( {
                    "from": from,
                    "to": to,
                })],
            )
        }

        pub fn eth_call_with_to(to: TxKind) -> CallRequest {
            CallRequest::new(
                ETH_CALL,
                vec![json!( {
                // missing `from`
                    "to": to,
                })],
            )
        }
    }

    impl Case {
        pub async fn assert(&self, rpc_method: &RpcMethod, case_num: usize) {
            let middleware = match &self.config {
                Config::Blacklist(config) => create_blacklist_middleware(rpc_method, config.clone()).await,
                Config::Whitelist(config) => create_whitelist_middleware(rpc_method, config.clone()).await,
            };

            let expected_res = self.expected_res.clone();
            let last = Box::new(move |_, _| async move { expected_res }.boxed());

            let res = middleware.call(self.request.clone(), Default::default(), last);
            let res = res.await;
            assert_eq!(res, self.expected_res, "case num: {}", case_num);
        }
    }

    async fn create_whitelist_middleware(
        rpc_method: &RpcMethod,
        whitelist_config: WhitelistConfig,
    ) -> Box<dyn Middleware<CallRequest, CallResult>> {
        let extensions = ExtensionsConfig {
            whitelist: Some(whitelist_config),
            ..Default::default()
        }
        .create_registry()
        .await
        .expect("Failed to create registry");

        WhitelistMiddleware::build(rpc_method, &extensions).await.unwrap()
    }

    async fn create_blacklist_middleware(
        rpc_method: &RpcMethod,
        config: BlacklistConfig,
    ) -> Box<dyn Middleware<CallRequest, CallResult>> {
        let extensions = ExtensionsConfig {
            blacklist: Some(config),
            ..Default::default()
        }
        .create_registry()
        .await
        .expect("Failed to create registry");

        BlacklistMiddleware::build(rpc_method, &extensions).await.unwrap()
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn test_eth_sendRawTransaction() {
        let rpc_method = r"
method: eth_sendRawTransaction
cache:
    size: 0
params:
    - name: transaction
      ty: Bytes
";

        let rpc_method: RpcMethod = serde_yaml::from_reader(&mut rpc_method.as_bytes()).unwrap();

        let config = r"
    tx:
      - from: 0000000000000000000000000000000000000001
      - from: 0x1aB49795aE1570b8300E47493E090202b88f8F23
";
        let blacklist = Config::Blacklist(serde_yaml::from_reader(&mut config.as_bytes()).unwrap());
        let whitelist = Config::Whitelist(serde_yaml::from_reader(&mut config.as_bytes()).unwrap());

        // See https://optimistic.etherscan.io/tx/0x664c3d2e1ac8b9db3038e7dbdb7402cb4105635e4b8b312f46e363239816d42b
        let right_tx = bytes!("f8aa8208da840393870082fde8940b2c639c533813f4aa9d7837caf62653d097ff8580b844a9059cbb00000000000000000000000026295137fbbd6fa569a844cdb20faea641577b2600000000000000000000000000000000000000000000000000000000000014d738a0fb193ba0e74178ab45cb7fbbce04d785b2be62c6bec9e7e23e7768a501a9c987a06ddef3b059fc47fd17acc83fa32ce6b0172f34e45a53c06bdaf239ec5b3fe5a6");
        // the first byte is changed
        let failed_tx = bytes!("e8aa8208da840393870082fde8940b2c639c533813f4aa9d7837caf62653d097ff8580b844a9059cbb00000000000000000000000026295137fbbd6fa569a844cdb20faea641577b2600000000000000000000000000000000000000000000000000000000000014d738a0fb193ba0e74178ab45cb7fbbce04d785b2be62c6bec9e7e23e7768a501a9c987a06ddef3b059fc47fd17acc83fa32ce6b0172f34e45a53c06bdaf239ec5b3fe5a6");

        // See https://optimistic.etherscan.io/tx/0x664c3d2e1ac8b9db3038e7dbdb7402cb4105635e4b8b312f46e363239816d42b
        let cases = vec![
            Case {
                request: request::raw_tx(&right_tx),
                expected_res: Err(err_banned_address()),
                config: whitelist.clone(),
            },
            Case {
                request: request::raw_tx(&right_tx),
                expected_res: Ok(Value::String(
                    "0x664c3d2e1ac8b9db3038e7dbdb7402cb4105635e4b8b312f46e363239816d42b".to_string(),
                )),
                config: whitelist.clone(),
            },
            // banned
            Case {
                request: request::raw_tx(&right_tx),
                expected_res: Err(err_banned_address()),
                config: blacklist.clone(),
            },
            Case {
                request: request::raw_tx(&failed_tx),
                expected_res: Err(err_failed_decode_txn(&failed_tx)),
                config: whitelist.clone(),
            },
            Case {
                request: request::raw_tx(&failed_tx),
                expected_res: Err(err_failed_decode_txn(&failed_tx)),
                config: blacklist.clone(),
            },
        ];

        for (n, case) in cases.iter().enumerate() {
            case.assert(&rpc_method, n).await;
        }
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn test_eth_sendTransaction() {
        let rpc_method = r"
method: eth_sendTransaction
cache:
    size: 0
params:
    - name: transaction
      ty: Object
";

        let rpc_method: RpcMethod = serde_yaml::from_reader(&mut rpc_method.as_bytes()).unwrap();

        let config = r"
    tx:
      - from: 0000000000000000000000000000000000000001
      - from: 0000000000000000000000000000000000000002
        to:   0000000000000000000000000000000000000003
      - from: 0000000000000000000000000000000000000003
        to: Create
";
        let blacklist = Config::Blacklist(serde_yaml::from_reader(&mut config.as_bytes()).unwrap());
        let whitelist = Config::Whitelist(serde_yaml::from_reader(&mut config.as_bytes()).unwrap());

        let ok_res = Ok(Value::Null);
        // See https://optimistic.etherscan.io/tx/0x664c3d2e1ac8b9db3038e7dbdb7402cb4105635e4b8b312f46e363239816d42b
        let cases = vec![
            Case {
                // transfer to self
                request: request::tx(
                    address!("0000000000000000000000000000000000000001"),
                    TxKind::Call(address!("0000000000000000000000000000000000000001")),
                ),
                // banned
                expected_res: Err(err_banned_address()),
                config: blacklist.clone(),
            },
            Case {
                // transfer to self
                request: request::tx(
                    address!("0000000000000000000000000000000000000001"),
                    TxKind::Call(address!("0000000000000000000000000000000000000001")),
                ),
                // whitelisted
                expected_res: ok_res.clone(),
                config: whitelist.clone(),
            },
            Case {
                // ban 0x01 everything
                request: request::tx(address!("0000000000000000000000000000000000000001"), TxKind::Create),
                expected_res: Err(err_banned_address()),
                config: blacklist.clone(),
            },
            Case {
                // 0x01 pass everything
                request: request::tx(address!("0000000000000000000000000000000000000001"), TxKind::Create),
                expected_res: ok_res.clone(),
                config: whitelist.clone(),
            },
            Case {
                // 0x03 could not create but could call
                request: request::tx(
                    address!("0000000000000000000000000000000000000003"),
                    TxKind::Call(address!("0000000000000000000000000000000000000001")),
                ),
                expected_res: ok_res.clone(),
                config: blacklist.clone(),
            },
            Case {
                // 0x03 could create
                request: request::tx(address!("0000000000000000000000000000000000000003"), TxKind::Create),
                expected_res: ok_res.clone(),
                config: whitelist.clone(),
            },
            Case {
                // missing "from" param.
                request: request::tx_with_to(TxKind::Call(address!("0000000000000000000000000000000000000001"))),
                expected_res: Err(err_unknown_from_address()),
                config: whitelist.clone(),
            },
        ];

        for (n, case) in cases.iter().enumerate() {
            case.assert(&rpc_method, n).await;
        }
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn test_eth_call() {
        let rpc_method = r"
method: eth_call
params:
    - name: transaction
      ty: Bytes
";

        let rpc_method: RpcMethod = serde_yaml::from_reader(&mut rpc_method.as_bytes()).unwrap();

        let config = r"
    eth_call:
      - from: 0000000000000000000000000000000000000001
      - from: 0000000000000000000000000000000000000002
        to: 0000000000000000000000000000000000000003
      - from: 0000000000000000000000000000000000000003
        to: Create
";
        let blacklist = Config::Blacklist(serde_yaml::from_reader(&mut config.as_bytes()).unwrap());
        let whitelist = Config::Whitelist(serde_yaml::from_reader(&mut config.as_bytes()).unwrap());

        let ok_res = Ok(Value::Null);
        // See https://optimistic.etherscan.io/tx/0x664c3d2e1ac8b9db3038e7dbdb7402cb4105635e4b8b312f46e363239816d42b
        let cases = vec![
            Case {
                // transfer to self
                request: request::eth_call(
                    address!("0000000000000000000000000000000000000001"),
                    TxKind::Call(address!("0000000000000000000000000000000000000001")),
                ),
                // banned
                expected_res: Err(err_banned_address()),
                config: blacklist.clone(),
            },
            Case {
                // transfer to self
                request: request::eth_call(
                    address!("0000000000000000000000000000000000000001"),
                    TxKind::Call(address!("0000000000000000000000000000000000000001")),
                ),
                // whitelisted
                expected_res: ok_res.clone(),
                config: whitelist.clone(),
            },
            Case {
                // ban 0x01 everything
                request: request::eth_call(address!("0000000000000000000000000000000000000001"), TxKind::Create),
                expected_res: Err(err_banned_address()),
                config: blacklist.clone(),
            },
            Case {
                // 0x01 pass everything
                request: request::eth_call(address!("0000000000000000000000000000000000000001"), TxKind::Create),
                expected_res: ok_res.clone(),
                config: whitelist.clone(),
            },
            Case {
                // 0x03 could not create but could call
                request: request::eth_call(
                    address!("0000000000000000000000000000000000000003"),
                    TxKind::Call(address!("0000000000000000000000000000000000000001")),
                ),
                expected_res: ok_res.clone(),
                config: blacklist.clone(),
            },
            Case {
                // 0x03 could create
                request: request::eth_call(address!("0000000000000000000000000000000000000003"), TxKind::Create),
                expected_res: ok_res.clone(),
                config: whitelist.clone(),
            },
            Case {
                // missing "from" param.
                request: request::eth_call_with_to(TxKind::Call(address!("0000000000000000000000000000000000000001"))),
                expected_res: Err(err_unknown_from_address()),
                config: whitelist.clone(),
            },
        ];

        for (n, case) in cases.iter().enumerate() {
            case.assert(&rpc_method, n).await;
        }
    }
}
