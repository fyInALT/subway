use async_trait::async_trait;
use jsonrpsee::core::JsonValue;
use jsonrpsee::types::ErrorObjectOwned;
use opentelemetry::trace::FutureExt;

use alloy_consensus::{Transaction, TxEip4844Variant, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::hex::decode;
use alloy_primitives::{Address, TxKind};
use serde::Serialize;

use crate::extensions::whitelist::{WhiteAddress, Whitelist, ETH_CALL, SEND_RAW_TX, SEND_TX};
use crate::utils::ToAddress;
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

/// This whitelist middleware should be used at the top level whenever possible.
pub struct WhitelistMiddleware {
    rpc_type: RpcType,
    addresses: Vec<WhiteAddress>,
}

#[derive(Debug, Eq, PartialEq)]
enum RpcType {
    EthCall,
    SendRawTX,
    SendTX,
}

impl WhitelistMiddleware {
    /// Return true if it's in whitelist.
    pub fn satisfy(&self, from: &Address, to: &ToAddress) -> bool {
        for address in &self.addresses {
            // if satisfy address from/to
            if address.satisfy(from, to) {
                return true;
            }
        }

        false
    }
}

#[async_trait]
impl MiddlewareBuilder<RpcMethod, CallRequest, CallResult> for WhitelistMiddleware {
    async fn build(
        method: &RpcMethod,
        extensions: &TypeRegistryRef,
    ) -> Option<Box<dyn Middleware<CallRequest, CallResult>>> {
        let whitelist = extensions
            .read()
            .await
            .get::<Whitelist>()
            .expect("WhitelistConfig extension not found");

        match method.method.as_str() {
            ETH_CALL => Some(Box::new(Self {
                rpc_type: RpcType::EthCall,
                addresses: whitelist.config.eth_call_whitelist.clone(),
            })),
            SEND_TX => Some(Box::new(Self {
                rpc_type: RpcType::SendTX,
                addresses: whitelist.config.tx_whitelist.clone(),
            })),
            SEND_RAW_TX => Some(Box::new(Self {
                rpc_type: RpcType::SendRawTX,
                addresses: whitelist.config.tx_whitelist.clone(),
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
    let p1 = params.get(0).ok_or_else(err_illegal_rpc_parameter)?;

    // `from` must exist.
    let from = p1.get("from").ok_or_else(|| {
        ErrorObjectOwned::borrowed(UNKNOWN_ADDRESS, "Could not get the `from` from rpc parameter", None)
    })?;
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
impl Middleware<CallRequest, CallResult> for WhitelistMiddleware {
    async fn call(
        &self,
        request: CallRequest,
        context: TypeRegistry,
        next: NextFn<CallRequest, CallResult>,
    ) -> CallResult {
        async move {
            // Not exist whitelist so return directly.
            if self.addresses.is_empty() {
                return next(request, context).await;
            }

            match self.rpc_type {
                RpcType::EthCall | RpcType::SendTX => {
                    let (from, to) = extract_address_from_to(&request.params)?;
                    if !self.satisfy(&from, &to) {
                        return Err(err_banned_address());
                    }
                }
                RpcType::SendRawTX => {
                    let p1 = request.params.get(0).ok_or_else(err_illegal_rpc_parameter)?;
                    let rlp_hex: String =
                        serde_json::from_value(p1.clone()).map_err(|_err| err_illegal_rpc_parameter())?;
                    let rlp = decode(&mut rlp_hex.as_bytes()).map_err(|_err| err_failed_decode_hex(&rlp_hex))?;

                    let tx: TxEnvelope =
                        TxEnvelope::decode_2718(&mut rlp.as_slice()).map_err(|_err| err_failed_decode_txn(&rlp_hex))?;

                    let from = extract_signer_from_tx_envelop(&tx)?;
                    let to = extract_to_address_from_tx_envelop(&tx);
                    if !self.satisfy(&from, &to) {
                        return Err(err_banned_address());
                    }
                }
            }

            // pass the whitelist
            next(request, context).await
        }
        .with_context(TRACER.context("whitelist"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::whitelist::WhitelistConfig;
    use crate::extensions::ExtensionsConfig;
    use alloy_primitives::{bytes, hex, Bytes};
    use futures_util::FutureExt;
    use serde_json::Value;

    struct Case {
        encoded_tx: Bytes,
        expected_res: CallResult,
    }

    async fn ensure_pass(middleware: &dyn Middleware<CallRequest, CallResult>, case: Case) {
        let rlp_hex = hex::encode_prefixed(case.encoded_tx);
        let req = CallRequest::new("eth_sendRawTransaction", vec![Value::String(rlp_hex)]);
        let expected_res = case.expected_res.clone();
        let last = Box::new(move |_, _| async move { expected_res }.boxed());

        let res = middleware.call(req, Default::default(), last);
        let res = res.await;
        assert_eq!(res, case.expected_res);
    }

    async fn create_middleware(
        rpc_method: RpcMethod,
        whitelist_config: WhitelistConfig,
    ) -> Box<dyn Middleware<CallRequest, CallResult>> {
        let extensions = ExtensionsConfig {
            whitelist: Some(whitelist_config),
            ..Default::default()
        }
        .create_registry()
        .await
        .expect("Failed to create registry");

        let middleware = WhitelistMiddleware::build(&rpc_method, &extensions).await.unwrap();

        middleware
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn eth_sendRawTransaction_should_be_banned() {
        let rpc_method = r"
method: eth_sendRawTransaction
cache:
    size: 0
params:
    - name: transaction
      ty: Bytes
";

        let whitelist_config = r"
    tx_whitelist:
      # allow 0x01 to call or create in tx.
      - from: 0000000000000000000000000000000000000001
";

        let rpc_method: RpcMethod = serde_yaml::from_reader(&mut rpc_method.as_bytes()).unwrap();
        let whitelist_config: WhitelistConfig = serde_yaml::from_reader(&mut whitelist_config.as_bytes()).unwrap();
        let middleware = create_middleware(rpc_method, whitelist_config).await;

        // See https://optimistic.etherscan.io/tx/0x664c3d2e1ac8b9db3038e7dbdb7402cb4105635e4b8b312f46e363239816d42b
        let cases = vec![
            Case {
                encoded_tx: bytes!("f8aa8208da840393870082fde8940b2c639c533813f4aa9d7837caf62653d097ff8580b844a9059cbb00000000000000000000000026295137fbbd6fa569a844cdb20faea641577b2600000000000000000000000000000000000000000000000000000000000014d738a0fb193ba0e74178ab45cb7fbbce04d785b2be62c6bec9e7e23e7768a501a9c987a06ddef3b059fc47fd17acc83fa32ce6b0172f34e45a53c06bdaf239ec5b3fe5a6"),
                expected_res: Err(err_banned_address()),
            },
        ];

        for case in cases {
            ensure_pass(middleware.as_ref(), case).await;
        }
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn eth_sendRawTransaction_codec() {
        let rpc_method = r"
method: eth_sendRawTransaction
cache:
    size: 0
params:
    - name: transaction
      ty: Bytes
";

        let whitelist_config = r"
    tx_whitelist:
      # allow 0x1aB49795aE1570b8300E47493E090202b88f8F23 to call or create in tx.
      - from: 0x1aB49795aE1570b8300E47493E090202b88f8F23
";

        let rpc_method: RpcMethod = serde_yaml::from_reader(&mut rpc_method.as_bytes()).unwrap();
        let whitelist_config: WhitelistConfig = serde_yaml::from_reader(&mut whitelist_config.as_bytes()).unwrap();
        let middleware = create_middleware(rpc_method, whitelist_config).await;

        // See https://optimistic.etherscan.io/tx/0x664c3d2e1ac8b9db3038e7dbdb7402cb4105635e4b8b312f46e363239816d42b
        // the first byte is changed
        let failed_tx = bytes!("e8aa8208da840393870082fde8940b2c639c533813f4aa9d7837caf62653d097ff8580b844a9059cbb00000000000000000000000026295137fbbd6fa569a844cdb20faea641577b2600000000000000000000000000000000000000000000000000000000000014d738a0fb193ba0e74178ab45cb7fbbce04d785b2be62c6bec9e7e23e7768a501a9c987a06ddef3b059fc47fd17acc83fa32ce6b0172f34e45a53c06bdaf239ec5b3fe5a6");

        let cases = vec![
            Case {
                encoded_tx: bytes!("f8aa8208da840393870082fde8940b2c639c533813f4aa9d7837caf62653d097ff8580b844a9059cbb00000000000000000000000026295137fbbd6fa569a844cdb20faea641577b2600000000000000000000000000000000000000000000000000000000000014d738a0fb193ba0e74178ab45cb7fbbce04d785b2be62c6bec9e7e23e7768a501a9c987a06ddef3b059fc47fd17acc83fa32ce6b0172f34e45a53c06bdaf239ec5b3fe5a6"),
                expected_res: Ok(Value::String("0x664c3d2e1ac8b9db3038e7dbdb7402cb4105635e4b8b312f46e363239816d42b".to_string())),
            },

            Case {
                expected_res: Err(err_failed_decode_txn(&failed_tx)),
                encoded_tx: failed_tx,
            }
        ];

        for case in cases {
            ensure_pass(middleware.as_ref(), case).await;
        }
    }
}
