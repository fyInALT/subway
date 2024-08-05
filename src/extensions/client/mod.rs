use std::{
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::anyhow;
use async_trait::async_trait;
use garde::Validate;
use jsonrpsee::{
    core::{
        client::{ClientT, Error, Subscription, SubscriptionClientT},
        JsonValue,
    },
    ws_client::{WsClient, WsClientBuilder},
};
use opentelemetry::{
    trace::{get_active_span, FutureExt, Span, TraceContextExt},
    Context, KeyValue,
};
use opentelemetry_semantic_conventions::resource as semconv;
use rand::{seq::SliceRandom, thread_rng};
use serde::Deserialize;
use tokio::{
    sync::{mpsc, oneshot, Notify},
    task::JoinHandle,
    time,
};

use crate::{
    extensions::{Extension, ExtensionRegistry},
    middlewares::CallResult,
    utils::{self, errors},
};

#[cfg(test)]
pub mod mock;
#[cfg(test)]
mod tests;

const TRACER: utils::telemetry::Tracer = utils::telemetry::Tracer::new("client");

pub struct Client {
    endpoints: Vec<String>,
    current_endpoint: Arc<AtomicUsize>,

    sender: mpsc::Sender<Message>,
    rotation_notify: Arc<Notify>,
    retries: u32,
    background_task: JoinHandle<()>,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.background_task.abort();
    }
}

#[derive(Deserialize, Validate, Debug)]
#[garde(allow_unvalidated)]
pub struct ClientConfig {
    #[garde(inner(custom(validate_endpoint)))]
    pub endpoints: Vec<String>,
    #[serde(default = "bool_true")]
    pub shuffle_endpoints: bool,
}

fn validate_endpoint(endpoint: &str, _context: &()) -> garde::Result {
    endpoint
        .parse::<jsonrpsee::client_transport::ws::Uri>()
        .map_err(|_| garde::Error::new(format!("Invalid endpoint format: {}", endpoint)))?;

    Ok(())
}

impl ClientConfig {
    pub async fn all_endpoints_can_be_connected(&self) -> bool {
        let join_handles: Vec<_> = self
            .endpoints
            .iter()
            .map(|endpoint| {
                let endpoint = endpoint.clone();
                tokio::spawn(async move {
                    match check_endpoint_connection(&endpoint).await {
                        Ok(_) => {
                            tracing::info!("Connected to endpoint: {endpoint}");
                            true
                        }
                        Err(err) => {
                            tracing::error!("Failed to connect to endpoint: {endpoint}, error: {err:?}",);
                            false
                        }
                    }
                })
            })
            .collect();
        let mut ok_all = true;
        for join_handle in join_handles {
            let ok = join_handle.await.unwrap_or_else(|e| {
                tracing::error!("Failed to join: {e:?}");
                false
            });
            if !ok {
                ok_all = false
            }
        }
        ok_all
    }
}
// simple connection check with default client params and no retries
async fn check_endpoint_connection(endpoint: &str) -> Result<(), anyhow::Error> {
    let _ = WsClientBuilder::default().build(&endpoint).await?;
    Ok(())
}

pub fn bool_true() -> bool {
    true
}

#[async_trait]
impl Extension for Client {
    type Config = ClientConfig;

    async fn from_config(config: &Self::Config, _registry: &ExtensionRegistry) -> Result<Self, anyhow::Error> {
        if config.shuffle_endpoints {
            let mut endpoints = config.endpoints.clone();
            endpoints.shuffle(&mut thread_rng());
            Ok(Self::with_endpoints(endpoints)?)
        } else {
            Ok(Self::with_endpoints(config.endpoints.clone())?)
        }
    }
}

#[derive(Debug)]
struct RequestMessage {
    method: String,
    params: Vec<JsonValue>,
    response: oneshot::Sender<Result<JsonValue, Error>>,
    retries: u32,
}

#[derive(Debug)]
struct SubscribeMessage {
    subscribe: String,
    params: Vec<JsonValue>,
    unsubscribe: String,
    response: oneshot::Sender<Result<Subscription<JsonValue>, Error>>,
    retries: u32,
}

#[derive(Debug)]
enum Message {
    Request(RequestMessage),
    Subscribe(SubscribeMessage),
    RotateEndpoint,
}

struct RpcMessageHandler {
    ws: Arc<WsClient>,
    message_tx: mpsc::Sender<Message>,
    request_timeout: Duration,
    request_backoff_counter: Arc<AtomicU32>,
}

impl RpcMessageHandler {
    // total timeout for a request
    fn request_timeout(&self) -> Duration {
        // buffer 5 seconds for the request to be processed
        self.request_timeout.saturating_add(Duration::from_secs(5))
    }

    fn handle(self, message: Message) {
        tokio::spawn(async move {
            match message {
                Message::Request(message) => self.handle_request(message).await,
                Message::Subscribe(message) => self.handle_subscribe(message).await,
                Message::RotateEndpoint => unreachable!(),
            }
        });
    }

    async fn handle_request(self, mut message: RequestMessage) {
        message.retries = message.retries.saturating_sub(1);

        // make sure it's still connected
        if message.response.is_closed() {
            return;
        }

        let timout = self.request_timeout();
        let request = self.ws.request(&message.method, message.params.clone());
        match time::timeout(timout, request).await {
            Ok(result) => match result {
                result @ Ok(_) => {
                    self.request_backoff_counter.store(0, Ordering::Relaxed);
                    let _ = message.response.send(result);
                }
                Err(err) => {
                    tracing::debug!("Request failed: {}", err);
                    match err {
                        Error::RequestTimeout | Error::Transport(_) | Error::RestartNeeded(_) => {
                            time::sleep(get_backoff_time(&self.request_backoff_counter)).await;

                            // make sure we still have retries left
                            if message.retries == 0 {
                                let _ = message.response.send(Err(Error::RequestTimeout));
                                return;
                            }

                            if matches!(err, Error::RequestTimeout) {
                                self.message_tx
                                    .send(Message::RotateEndpoint)
                                    .await
                                    .expect("Failed to send rotate message");
                            }

                            self.message_tx
                                .send(Message::Request(message))
                                .await
                                .expect("Failed to send request message");
                        }
                        err => {
                            // nothing we can handle, send it back to the caller
                            let _ = message.response.send(Err(err));
                        }
                    }
                }
            },
            Err(_) => {
                tracing::error!(
                    "Request timed out, method: {} params: {:?}",
                    message.method,
                    message.params
                );
                let _ = message.response.send(Err(Error::RequestTimeout));
            }
        }
    }

    async fn handle_subscribe(self, mut message: SubscribeMessage) {
        message.retries = message.retries.saturating_sub(1);

        // make sure it's still connected
        if message.response.is_closed() {
            return;
        }

        let timout = self.request_timeout();
        let subscribe = self
            .ws
            .subscribe(&message.subscribe, message.params.clone(), &message.unsubscribe);
        match time::timeout(timout, subscribe).await {
            Ok(result) => match result {
                result @ Ok(_) => {
                    self.request_backoff_counter.store(0, Ordering::Relaxed);
                    let _ = message.response.send(result);
                }
                Err(err) => {
                    tracing::debug!("Subscribe failed: {}", err);
                    match err {
                        Error::RequestTimeout | Error::Transport(_) | Error::RestartNeeded(_) => {
                            time::sleep(get_backoff_time(&self.request_backoff_counter)).await;

                            // make sure we still have retries left
                            if message.retries == 0 {
                                let _ = message.response.send(Err(Error::RequestTimeout));
                                return;
                            }

                            if matches!(err, Error::RequestTimeout) {
                                self.message_tx
                                    .send(Message::RotateEndpoint)
                                    .await
                                    .expect("Failed to send rotate message");
                            }

                            self.message_tx
                                .send(Message::Subscribe(message))
                                .await
                                .expect("Failed to send subscribe message")
                        }
                        err => {
                            // nothing we can handle, send it back to the caller
                            let _ = message.response.send(Err(err));
                        }
                    }
                }
            },
            Err(_) => {
                tracing::error!(
                    "Subscribe timed out, subscribe: {} params: {:?}",
                    message.subscribe,
                    message.params
                );
                let _ = message.response.send(Err(Error::RequestTimeout));
            }
        }
    }
}

#[derive(Copy, Clone)]
pub struct WsConfig {
    pub request_timeout: Duration,
    pub connection_timeout: Duration,
    pub max_buffer_capacity_per_subscription: usize,
    pub max_concurrent_requests: usize,
    pub max_response_size: u32,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(30),
            max_buffer_capacity_per_subscription: 2048,
            max_concurrent_requests: 2048,
            max_response_size: 20 * 1034 * 1024,
        }
    }
}

impl WsConfig {
    async fn try_connect(&self, url: &str) -> Result<WsClient, Error> {
        tracing::info!("Connecting to endpoint: {}", url);
        WsClientBuilder::default()
            .request_timeout(self.request_timeout)
            .connection_timeout(self.connection_timeout)
            .max_buffer_capacity_per_subscription(self.max_buffer_capacity_per_subscription)
            .max_concurrent_requests(self.max_concurrent_requests)
            .max_response_size(self.max_response_size)
            .build(url)
            .await
    }
}

struct BackgroundTask {
    config: WsConfig,

    current_endpoint: Arc<AtomicUsize>,
    endpoints: Vec<String>,

    connect_backoff_counter: Arc<AtomicU32>,
    request_backoff_counter: Arc<AtomicU32>,

    message_tx: mpsc::Sender<Message>,
    message_rx: mpsc::Receiver<Message>,

    rotation_notify: Arc<Notify>,
}

impl BackgroundTask {
    fn new(
        config: WsConfig,
        endpoints: Vec<String>,
        current_endpoint: Arc<AtomicUsize>,
        message_tx: mpsc::Sender<Message>,
        message_rx: mpsc::Receiver<Message>,
        rotation_notify: Arc<Notify>,
    ) -> Self {
        Self {
            config,
            current_endpoint,
            endpoints,
            connect_backoff_counter: Arc::new(AtomicU32::new(0)),
            request_backoff_counter: Arc::new(AtomicU32::new(0)),
            message_tx,
            message_rx,
            rotation_notify,
        }
    }

    async fn connect(&self) -> (String, Arc<WsClient>) {
        loop {
            let endpoints = &self.endpoints;
            let current_endpoint = self.current_endpoint.fetch_add(1, Ordering::Relaxed);
            let endpoint = &endpoints[current_endpoint % endpoints.len()];

            match self.config.try_connect(endpoint).await {
                Ok(ws) => {
                    let ws = Arc::new(ws);
                    tracing::info!("Endpoint '{endpoint}' connected");
                    self.connect_backoff_counter.store(0, Ordering::Relaxed);
                    break (endpoint.to_string(), ws);
                }
                Err(err) => {
                    tracing::warn!("Unable to connect to endpoint: '{endpoint}' error: {err}");
                    time::sleep(get_backoff_time(&self.connect_backoff_counter)).await;
                }
            }
        }
    }

    fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let (mut current_endpoint, mut ws) = self.connect().await;

            loop {
                tokio::select! {
                    _ = ws.on_disconnect() => {
                        tracing::info!("Endpoint '{current_endpoint}' disconnected");
                        time::sleep(get_backoff_time(&self.connect_backoff_counter)).await;
                        (current_endpoint, ws) = self.connect().await;
                    }
                    message = self.message_rx.recv() => {
                        tracing::trace!("Received message {message:?}");
                        match message {
                            Some(Message::RotateEndpoint) => {
                                self.rotation_notify.notify_waiters();
                                tracing::info!("Rotate endpoint '{current_endpoint}' to another endpoint");
                                (current_endpoint, ws) = self.connect().await;
                            }
                            Some(message) => RpcMessageHandler {
                                ws: ws.clone(),
                                message_tx: self.message_tx.clone(),
                                request_timeout: self.config.request_timeout,
                                request_backoff_counter: self.request_backoff_counter.clone()
                            }
                            .handle(message),
                            None => {
                                tracing::debug!("Client dropped");
                                break;
                            }
                        }
                    },
                }
            }
        })
    }
}

impl Client {
    pub fn new(
        config: WsConfig,
        endpoints: impl IntoIterator<Item = impl AsRef<str>>,
        retries: u32,
    ) -> Result<Self, anyhow::Error> {
        let endpoints = endpoints
            .into_iter()
            .map(|endpoint| endpoint.as_ref().to_string())
            .collect::<Vec<_>>();

        if endpoints.is_empty() {
            return Err(anyhow!("No endpoints provided"));
        }

        if retries == 0 {
            return Err(anyhow!("Retries need to be at least 1"));
        }

        tracing::info!("New client with endpoints: {:?}", endpoints);

        let current_endpoint = Arc::new(AtomicUsize::new(0));
        let current_endpoint_bg = current_endpoint.clone();
        let endpoints_bg = endpoints.clone();

        let (message_tx_bg, message_rx_bg) = mpsc::channel::<Message>(100);
        let message_tx = message_tx_bg.clone();

        let rotation_notify_bg = Arc::new(Notify::new());
        let rotation_notify = rotation_notify_bg.clone();

        let background_task = BackgroundTask::new(
            config,
            endpoints_bg,
            current_endpoint_bg,
            message_tx_bg,
            message_rx_bg,
            rotation_notify_bg,
        )
        .spawn();

        Ok(Self {
            endpoints,
            current_endpoint,
            sender: message_tx,
            rotation_notify,
            retries,
            background_task,
        })
    }

    pub fn with_endpoints(endpoints: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Self, anyhow::Error> {
        Self::new(WsConfig::default(), endpoints, 3)
    }

    pub fn endpoints(&self) -> &Vec<String> {
        &self.endpoints
    }

    pub fn current_endpoint(&self) -> &str {
        let idx = self.current_endpoint.load(Ordering::Relaxed);
        let endpoints = &self.endpoints;
        &endpoints[idx % endpoints.len()]
    }

    pub async fn request(&self, method: &str, params: Vec<JsonValue>) -> CallResult {
        let mut span = TRACER.span("client");

        let request_method = method.to_string();
        let request_params = serde_json::to_string(&params).expect("serialize JSON value shouldn't be fail");
        let request_endpoint = self.current_endpoint().to_string();
        span.set_attributes([
            KeyValue::new(semconv::RPC_METHOD, request_method),
            KeyValue::new("rpc.jsonrpc.params", request_params),
            KeyValue::new("rpc.jsonrpc.endpoint", request_endpoint),
        ]);

        async move {
            let (tx, rx) = oneshot::channel();
            self.sender
                .send(Message::Request(RequestMessage {
                    method: method.into(),
                    params,
                    response: tx,
                    retries: self.retries,
                }))
                .await
                .map_err(|err| {
                    let error = errors::internal_error(err);
                    get_active_span(|span| {
                        span.set_attributes([
                            KeyValue::new(semconv::RPC_JSONRPC_ERROR_CODE, error.code() as i64),
                            KeyValue::new(semconv::RPC_JSONRPC_ERROR_MESSAGE, error.message().to_string()),
                        ]);
                    });
                    error
                })?;

            match rx.await {
                Ok(Ok(value)) => {
                    let response = serde_json::to_string(&value).expect("serialize JSON value shouldn't be fail");
                    get_active_span(|span| {
                        span.set_attribute(KeyValue::new("rpc.jsonrpc.result", response));
                    });
                    Ok(value)
                }
                Ok(Err(err)) => {
                    let error = errors::map_error(err);
                    get_active_span(|span| {
                        span.set_attributes([
                            KeyValue::new(semconv::RPC_JSONRPC_ERROR_CODE, error.code() as i64),
                            KeyValue::new(semconv::RPC_JSONRPC_ERROR_MESSAGE, error.message().to_string()),
                        ]);
                    });
                    Err(error)
                }
                Err(err) => {
                    let error = errors::internal_error(err);
                    get_active_span(|span| {
                        span.set_attributes([
                            KeyValue::new(semconv::RPC_JSONRPC_ERROR_CODE, error.code() as i64),
                            KeyValue::new(semconv::RPC_JSONRPC_ERROR_MESSAGE, error.message().to_string()),
                        ]);
                    });
                    Err(error)
                }
            }
        }
        .with_context(Context::current_with_span(span))
        .await
    }

    pub async fn subscribe(
        &self,
        subscribe: &str,
        params: Vec<JsonValue>,
        unsubscribe: &str,
    ) -> Result<Subscription<JsonValue>, Error> {
        let mut span = TRACER.span("client");

        let subscribe_method = subscribe.to_string();
        let subscribe_params = serde_json::to_string(&params).expect("serialize JSON value shouldn't be fail");
        let subscribe_endpoint = self.current_endpoint().to_string();
        span.set_attributes([
            KeyValue::new(semconv::RPC_METHOD, subscribe_method),
            KeyValue::new("rpc.jsonrpc.params", subscribe_params),
            KeyValue::new("rpc.jsonrpc.endpoint", subscribe_endpoint),
        ]);

        async move {
            let (tx, rx) = oneshot::channel();
            self.sender
                .send(Message::Subscribe(SubscribeMessage {
                    subscribe: subscribe.into(),
                    params,
                    unsubscribe: unsubscribe.into(),
                    response: tx,
                    retries: self.retries,
                }))
                .await
                .map_err(|err| {
                    let error = errors::internal_error(err);
                    get_active_span(|span| {
                        span.set_attributes([
                            KeyValue::new(semconv::RPC_JSONRPC_ERROR_CODE, error.code() as i64),
                            KeyValue::new(semconv::RPC_JSONRPC_ERROR_MESSAGE, error.message().to_string()),
                        ]);
                    });
                    error
                })?;

            rx.await.map_err(|err| {
                let error = errors::internal_error(err);
                get_active_span(|span| {
                    span.set_attributes([
                        KeyValue::new(semconv::RPC_JSONRPC_ERROR_CODE, error.code() as i64),
                        KeyValue::new(semconv::RPC_JSONRPC_ERROR_MESSAGE, error.message().to_string()),
                    ]);
                });
                error
            })?
        }
        .with_context(Context::current_with_span(span))
        .await
    }

    pub async fn rotate_endpoint(&self) {
        self.sender
            .send(Message::RotateEndpoint)
            .await
            .expect("Failed to rotate endpoint");
    }

    /// Returns a future that resolves when the endpoint is rotated.
    pub async fn on_rotation(&self) {
        self.rotation_notify.notified().await
    }
}

fn get_backoff_time(counter: &Arc<AtomicU32>) -> Duration {
    let min_time = 100u64;
    let step = 100u64;
    let max_count = 10u32;

    let backoff_count = counter.fetch_add(1, Ordering::Relaxed);

    let backoff_count = backoff_count.min(max_count) as u64;
    let backoff_time = backoff_count * backoff_count * step;

    Duration::from_millis(backoff_time + min_time)
}

#[test]
fn test_get_backoff_time() {
    let counter = Arc::new(AtomicU32::new(0));

    let mut times = Vec::new();

    for _ in 0..12 {
        times.push(get_backoff_time(&counter));
    }

    let times = times.into_iter().map(|t| t.as_millis()).collect::<Vec<_>>();

    assert_eq!(
        times,
        vec![100, 200, 500, 1000, 1700, 2600, 3700, 5000, 6500, 8200, 10100, 10100]
    );
}
