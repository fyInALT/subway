use crate::config::{RpcMethod, RpcSubscription};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Debug, Default)]
pub struct MethodWeights(Arc<BTreeMap<String, u32>>);

impl MethodWeights {
    pub fn get(&self, method: &str) -> u32 {
        self.0.get(method).cloned().unwrap_or(1)
    }
}

impl MethodWeights {
    pub fn from_config(methods: &[RpcMethod], subscriptions: &[RpcSubscription]) -> Self {
        let mut weights = BTreeMap::default();
        for method in methods {
            weights.insert(method.method.to_owned(), method.rate_limit_weight);
        }
        for subscription in subscriptions {
            let (subscribe, unsubscribe) = (&subscription.subscribe, &subscription.unsubscribe);
            weights.insert(subscribe.method.to_owned(), subscribe.rate_limit_weight);
            weights.insert(unsubscribe.method.to_owned(), unsubscribe.rate_limit_weight);
        }

        Self(Arc::new(weights))
    }
}
