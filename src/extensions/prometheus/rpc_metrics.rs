use substrate_prometheus_endpoint::{register, Counter, CounterVec, HistogramOpts, HistogramVec, Opts, Registry, U64};

#[derive(Clone)]
pub enum RpcMetrics {
    Prometheus(InnerMetrics),
    Noop,
}

impl RpcMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self::Prometheus(InnerMetrics::new(registry))
    }

    pub fn noop() -> Self {
        Self::Noop
    }

    pub fn ws_open(&self) {
        if let Self::Prometheus(inner) = self {
            inner.ws_open();
        }
    }

    pub fn ws_closed(&self) {
        if let Self::Prometheus(inner) = self {
            inner.ws_closed();
        }
    }

    pub fn cache_query(&self, method: &str) {
        if let Self::Prometheus(inner) = self {
            inner.cache_query(method);
        }
    }
    pub fn cache_miss(&self, method: &str) {
        if let Self::Prometheus(inner) = self {
            inner.cache_miss(method);
        }
    }

    pub fn finalized_query(&self, method: &str) {
        if let Self::Prometheus(inner) = self {
            inner.finalized_query(method);
        }
    }
    pub fn finalized_miss(&self, method: &str) {
        if let Self::Prometheus(inner) = self {
            inner.finalized_miss(method);
        }
    }

    pub fn call_metrics(&self) -> Option<(HistogramVec, CounterVec<U64>, CounterVec<U64>)> {
        if let Self::Prometheus(inner) = self {
            return Some((
                inner.call_times.clone(),
                inner.calls_started.clone(),
                inner.calls_finished.clone(),
            ));
        }

        None
    }
}

#[derive(Clone)]
pub struct InnerMetrics {
    open_session_count: Counter<U64>,
    closed_session_count: Counter<U64>,
    cache_query_counter: CounterVec<U64>,
    cache_miss_counter: CounterVec<U64>,
    finalized_query_counter: CounterVec<U64>,
    finalized_miss_counter: CounterVec<U64>,
    call_times: HistogramVec,
    calls_started: CounterVec<U64>,
    calls_finished: CounterVec<U64>,
}

impl InnerMetrics {
    fn new(registry: &Registry) -> Self {
        let open_counter = Counter::new("open_ws_counter", "Total number of opened websocket connections").unwrap();
        let closed_counter = Counter::new("closed_ws_counter", "Total number of closed websocket connections").unwrap();
        let cache_miss_counter = CounterVec::new(
            Opts::new("cache_miss_counter", "Total number of cache misses of RPC requests"),
            &["method"],
        )
        .unwrap();
        let cache_query_counter = CounterVec::new(
            Opts::new("cache_query_counter", "Total number of cache queries of RPC requests"),
            &["method"],
        )
        .unwrap();
        let finalized_miss_counter = CounterVec::new(
            Opts::new(
                "finalized_miss_counter",
                "Total number of finalized block misses of RPC requests with block tag",
            ),
            &["method"],
        )
        .unwrap();
        let finalized_query_counter = CounterVec::new(
            Opts::new(
                "finalized_query_counter",
                "Total number of finalized block queries of RPC requests with block tag",
            ),
            &["method"],
        )
        .unwrap();
        let call_times = HistogramVec::new(
            HistogramOpts::new(
                "rpc_calls_time",
                "Time interval from the initiation to the completion of an RPC request",
            ),
            &["protocol", "method"],
        )
        .unwrap();
        let calls_started_counter = CounterVec::new(
            Opts::new("rpc_calls_started", "Total number of initiated RPC requests"),
            &["protocol", "method"],
        )
        .unwrap();
        let calls_finished_counter = CounterVec::new(
            Opts::new("rpc_calls_finished", "Total number of completed RPC requests"),
            &["protocol", "method", "is_error"],
        )
        .unwrap();

        let open_session_count = register(open_counter, registry).unwrap();
        let closed_session_count = register(closed_counter, registry).unwrap();
        let cache_query_counter = register(cache_query_counter, registry).unwrap();
        let cache_miss_counter = register(cache_miss_counter, registry).unwrap();
        let finalized_query_counter = register(finalized_query_counter, registry).unwrap();
        let finalized_miss_counter = register(finalized_miss_counter, registry).unwrap();

        let call_times = register(call_times, registry).unwrap();
        let calls_started = register(calls_started_counter, registry).unwrap();
        let calls_finished = register(calls_finished_counter, registry).unwrap();

        Self {
            cache_miss_counter,
            cache_query_counter,
            finalized_miss_counter,
            finalized_query_counter,
            open_session_count,
            closed_session_count,
            calls_started,
            calls_finished,
            call_times,
        }
    }
    fn ws_open(&self) {
        self.open_session_count.inc();
    }

    fn ws_closed(&self) {
        self.closed_session_count.inc();
    }

    fn cache_query(&self, method: &str) {
        self.cache_query_counter.with_label_values(&[method]).inc();
    }

    fn cache_miss(&self, method: &str) {
        self.cache_miss_counter.with_label_values(&[method]).inc();
    }

    fn finalized_query(&self, method: &str) {
        self.finalized_query_counter.with_label_values(&[method]).inc();
    }

    fn finalized_miss(&self, method: &str) {
        self.finalized_miss_counter.with_label_values(&[method]).inc();
    }
}
