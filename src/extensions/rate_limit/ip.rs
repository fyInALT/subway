use crate::extensions::rate_limit::MethodWeights;
use crate::utils::errors;
use futures::{future::BoxFuture, FutureExt};
use governor::{DefaultKeyedRateLimiter, Jitter};
use jsonrpsee::{
    server::{middleware::rpc::RpcServiceT, types::Request},
    MethodResponse,
};
use std::{num::NonZeroU32, sync::Arc};

#[derive(Clone)]
pub struct IpRateLimitLayer {
    ip_addr: String,
    limiter: Arc<DefaultKeyedRateLimiter<String>>,
    jitter: Jitter,
    method_weights: MethodWeights,
    blocking: bool,
}

impl IpRateLimitLayer {
    pub fn new(
        ip_addr: String,
        limiter: Arc<DefaultKeyedRateLimiter<String>>,
        jitter: Jitter,
        method_weights: MethodWeights,
    ) -> Self {
        Self {
            ip_addr,
            limiter,
            jitter,
            method_weights,
            blocking: false,
        }
    }

    pub fn blocking(mut self, blocking: bool) -> Self {
        self.blocking = blocking;
        self
    }
}

impl<S> tower::Layer<S> for IpRateLimitLayer {
    type Service = IpRateLimit<S>;

    fn layer(&self, service: S) -> Self::Service {
        IpRateLimit::new(
            service,
            self.ip_addr.clone(),
            self.limiter.clone(),
            self.jitter,
            self.method_weights.clone(),
        )
        .blocking(self.blocking)
    }
}

#[derive(Clone)]
pub struct IpRateLimit<S> {
    service: S,
    ip_addr: String,
    limiter: Arc<DefaultKeyedRateLimiter<String>>,
    jitter: Jitter,
    method_weights: MethodWeights,
    blocking: bool,
}

impl<S> IpRateLimit<S> {
    pub fn new(
        service: S,
        ip_addr: String,
        limiter: Arc<DefaultKeyedRateLimiter<String>>,
        jitter: Jitter,
        method_weights: MethodWeights,
    ) -> Self {
        Self {
            service,
            ip_addr,
            limiter,
            jitter,
            method_weights,
            blocking: false,
        }
    }

    pub fn blocking(mut self, blocking: bool) -> Self {
        self.blocking = blocking;
        self
    }
}

impl<'a, S> RpcServiceT<'a> for IpRateLimit<S>
where
    S: RpcServiceT<'a> + Send + Sync + Clone + 'static,
{
    type Future = BoxFuture<'a, MethodResponse>;

    fn call(&self, req: Request<'a>) -> Self::Future {
        let ip_addr = self.ip_addr.clone();
        let jitter = self.jitter;
        let service = self.service.clone();
        let limiter = self.limiter.clone();
        let weight = self.method_weights.get(req.method_name());
        let blocking = self.blocking;

        async move {
            if let Some(n) = NonZeroU32::new(weight) {
                if blocking {
                    limiter
                        .until_key_n_ready_with_jitter(&ip_addr, n, jitter)
                        .await
                        .expect("check_n have been done during init");
                } else {
                    match limiter
                        .check_key_n(&ip_addr, n)
                        .expect("check_n have been done during init")
                    {
                        Ok(_) => {}
                        Err(_) => return MethodResponse::error(req.id, errors::reached_rate_limit()),
                    }
                }
            }
            service.call(req).await
        }
        .boxed()
    }
}
