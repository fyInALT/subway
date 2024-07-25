use crate::extensions::rate_limit::MethodWeights;
use futures::{future::BoxFuture, FutureExt};
use governor::{DefaultDirectRateLimiter, Jitter};
use jsonrpsee::{
    server::{middleware::rpc::RpcServiceT, types::Request},
    MethodResponse,
};
use std::{num::NonZeroU32, sync::Arc};

#[derive(Clone)]
pub struct GlobalRateLimitLayer {
    limiter: Arc<DefaultDirectRateLimiter>,
    jitter: Jitter,
    method_weights: MethodWeights,
}

impl GlobalRateLimitLayer {
    pub fn new(limiter: Arc<DefaultDirectRateLimiter>, jitter: Jitter, method_weights: MethodWeights) -> Self {
        Self {
            limiter,
            jitter,
            method_weights,
        }
    }
}

impl<S> tower::Layer<S> for GlobalRateLimitLayer {
    type Service = GlobalRateLimit<S>;

    fn layer(&self, service: S) -> Self::Service {
        GlobalRateLimit::new(service, self.limiter.clone(), self.jitter, self.method_weights.clone())
    }
}

#[derive(Clone)]
pub struct GlobalRateLimit<S> {
    service: S,
    limiter: Arc<DefaultDirectRateLimiter>,
    jitter: Jitter,
    method_weights: MethodWeights,
}

impl<S> GlobalRateLimit<S> {
    pub fn new(
        service: S,
        limiter: Arc<DefaultDirectRateLimiter>,
        jitter: Jitter,
        method_weights: MethodWeights,
    ) -> Self {
        Self {
            service,
            limiter,
            jitter,
            method_weights,
        }
    }
}

impl<'a, S> RpcServiceT<'a> for GlobalRateLimit<S>
where
    S: RpcServiceT<'a> + Send + Sync + Clone + 'static,
{
    type Future = BoxFuture<'a, MethodResponse>;

    fn call(&self, req: Request<'a>) -> Self::Future {
        let jitter = self.jitter;
        let service = self.service.clone();
        let limiter = self.limiter.clone();
        let weight = self.method_weights.get(req.method_name());

        async move {
            if let Some(n) = NonZeroU32::new(weight) {
                limiter
                    .until_n_ready_with_jitter(n, jitter)
                    .await
                    .expect("check_n have been done during init");
            }
            service.call(req).await
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::rate_limit::build_quota;
    use governor::RateLimiter;
    use jsonrpsee::types::Id;
    use jsonrpsee::ResponsePayload;
    use std::time::Duration;

    #[derive(Clone)]
    struct MockService;
    impl RpcServiceT<'static> for MockService {
        type Future = BoxFuture<'static, MethodResponse>;

        fn call(&self, req: Request<'static>) -> Self::Future {
            async move { MethodResponse::response(req.id, ResponsePayload::success("ok"), 1024) }.boxed()
        }
    }

    #[tokio::test]
    async fn rate_limit_works() {
        let burst = NonZeroU32::new(10).unwrap();
        let period = Duration::from_millis(100);
        let quota = build_quota(burst, period);
        let limiter = Arc::new(RateLimiter::direct(quota));

        let service = GlobalRateLimit::new(
            MockService,
            limiter,
            Jitter::up_to(Duration::from_millis(10)),
            Default::default(),
        );

        let batch = |service: GlobalRateLimit<MockService>, count: usize, delay| async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            let calls = (1..=count)
                .map(|id| service.call(Request::new("test".into(), None, Id::Number(id as u64))))
                .collect::<Vec<_>>();
            let results = futures::future::join_all(calls).await;
            assert_eq!(results.iter().filter(|r| r.is_success()).count(), count);
        };

        let start = tokio::time::Instant::now();
        // background task to make calls
        let batch1 = tokio::spawn(batch(service.clone(), 30, 0));
        let batch2 = tokio::spawn(batch(service.clone(), 40, 200));
        let batch3 = tokio::spawn(batch(service.clone(), 20, 300));
        batch1.await.unwrap();
        batch2.await.unwrap();
        batch3.await.unwrap();
        let duration = start.elapsed().as_millis();
        println!("duration: {} ms", duration);
        // should take between 800..900 millis. each 100ms period handles 10 calls
        assert!(duration > 800 && duration < 900);
    }
}
