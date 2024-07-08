use std::sync::Arc;

use async_trait::async_trait;
use opentelemetry::{trace::FutureExt, KeyValue};

use crate::{
    extensions::client::Client,
    middlewares::{CallRequest, CallResult, Middleware, MiddlewareBuilder, NextFn, RpcMethod, TRACER},
    utils::{TypeRegistry, TypeRegistryRef},
};

pub struct UpstreamMiddleware {
    client: Arc<Client>,
}

impl UpstreamMiddleware {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl MiddlewareBuilder<RpcMethod, CallRequest, CallResult> for UpstreamMiddleware {
    async fn build(
        _method: &RpcMethod,
        extensions: &TypeRegistryRef,
    ) -> Option<Box<dyn Middleware<CallRequest, CallResult>>> {
        let client = extensions
            .read()
            .await
            .get::<Client>()
            .expect("Client extension not found");
        Some(Box::new(UpstreamMiddleware::new(client)))
    }
}

#[async_trait]
impl Middleware<CallRequest, CallResult> for UpstreamMiddleware {
    async fn call(
        &self,
        request: CallRequest,
        _context: TypeRegistry,
        _next: NextFn<CallRequest, CallResult>,
    ) -> CallResult {
        let request_params = serde_json::to_string(&request.params).expect("serialize JSON value shouldn't be fail");
        self.client
            .request(&request.method, request.params)
            .with_context(TRACER.context_with_attrs("upstream", [KeyValue::new("params", request_params)]))
            .await
    }
}
