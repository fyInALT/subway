use crate::utils::AddressRule;
use async_trait::async_trait;
use serde::Deserialize;

use super::{Extension, ExtensionRegistry};

/// The address whitelist for `eth_call/eth_sendRawTransaction` rpc.
#[derive(Deserialize, Debug, Clone)]
pub struct WhitelistConfig {
    #[serde(default)]
    pub eth_call: Vec<AddressRule>,
    #[serde(default)]
    pub tx: Vec<AddressRule>,
}

/// The address blacklist for `eth_call/eth_sendRawTransaction` rpc.
#[derive(Deserialize, Debug, Clone)]
pub struct BlacklistConfig {
    #[serde(default)]
    pub eth_call: Vec<AddressRule>,
    #[serde(default)]
    pub tx: Vec<AddressRule>,
}

pub struct Whitelist {
    pub config: WhitelistConfig,
}

pub struct BlackList {
    pub config: BlacklistConfig,
}

#[async_trait]
impl Extension for Whitelist {
    type Config = WhitelistConfig;

    async fn from_config(config: &Self::Config, _registry: &ExtensionRegistry) -> Result<Self, anyhow::Error> {
        Ok(Self { config: config.clone() })
    }
}

#[async_trait]
impl Extension for BlackList {
    type Config = BlacklistConfig;

    async fn from_config(config: &Self::Config, _registry: &ExtensionRegistry) -> Result<Self, anyhow::Error> {
        Ok(Self { config: config.clone() })
    }
}
