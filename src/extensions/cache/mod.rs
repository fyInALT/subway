use async_trait::async_trait;
use serde::Deserialize;

use super::{Extension, ExtensionRegistry};

pub struct Cache {
    pub config: CacheConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CacheConfig {
    // None means no cache expiration
    #[serde(default)]
    pub default_ttl_seconds: Option<u64>,
    pub default_size: usize,
}

impl Cache {
    pub fn new(config: CacheConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Extension for Cache {
    type Config = CacheConfig;

    async fn from_config(config: &Self::Config, _registry: &ExtensionRegistry) -> Result<Self, anyhow::Error> {
        Ok(Self::new(config.clone()))
    }
}

pub struct BlockCache {
    pub config: BlockCacheConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BlockCacheConfig {
    pub default_finalized_size: usize,
    pub default_recent_size: usize,
    pub ttl_unit_seconds: u64,
    #[serde(default)]
    pub default_finalized_ttl_units: Option<u64>,
    #[serde(default)]
    pub default_recent_ttl_units: Option<u64>,
}

impl BlockCacheConfig {
    pub fn default_finalized_ttl_seconds(&self) -> Option<u64> {
        self.default_finalized_ttl_units
            .map(|units| units * self.ttl_unit_seconds)
    }

    pub fn default_recent_ttl_seconds(&self) -> Option<u64> {
        self.default_recent_ttl_units.map(|units| units * self.ttl_unit_seconds)
    }
}

impl BlockCache {
    pub fn new(config: BlockCacheConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Extension for BlockCache {
    type Config = BlockCacheConfig;

    async fn from_config(config: &Self::Config, _registry: &ExtensionRegistry) -> Result<Self, anyhow::Error> {
        Ok(Self::new(config.clone()))
    }
}
