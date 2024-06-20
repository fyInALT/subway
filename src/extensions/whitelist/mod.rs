use crate::utils::ToAddress;
use alloy_primitives::Address;
use async_trait::async_trait;
use serde::Deserialize;

use super::{Extension, ExtensionRegistry};

// Read rpc.
pub const ETH_CALL: &'static str = "eth_call";

// Write rpc.
pub const SEND_RAW_TX: &'static str = "eth_sendRawTransaction";
pub const SEND_TX: &'static str = "eth_sendTransaction";

pub struct Whitelist {
    pub config: WhitelistConfig,
}

/// The address whitelist for `eth_call/eth_sendRawTransaction` rpc.
#[derive(Deserialize, Debug, Clone)]
pub struct WhitelistConfig {
    #[serde(default, alias = "eth_call")]
    pub eth_call_whitelist: Vec<WhiteAddress>,
    #[serde(default, alias = "tx")]
    pub tx_whitelist: Vec<WhiteAddress>,
}

/// When an address is None, it means it will satisfy any address.
#[derive(Deserialize, Debug, Clone)]
pub struct WhiteAddress {
    /// Should check the address if Some.
    pub from: Option<Address>,
    /// Should check the address if Some.
    pub to: Option<ToAddress>,
}

impl Whitelist {
    pub fn new(config: WhitelistConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Extension for Whitelist {
    type Config = WhitelistConfig;

    async fn from_config(config: &Self::Config, _registry: &ExtensionRegistry) -> Result<Self, anyhow::Error> {
        Ok(Self::new(config.clone()))
    }
}
