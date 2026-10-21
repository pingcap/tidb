// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `client.go`: the client value, its options, the HTTP transport seam and
//! the leader-first retry loop.

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use prometheus::{CounterVec, HistogramVec};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Method;
use serde::de::DeserializeOwned;

use super::request_info::{RequestInfo, UPDATE_KEYSPACE_CONFIG_NAME};
use super::service_discovery::{ServiceClient, ServiceDiscovery};
use crate::pd::backoff::Backoffer;
use crate::pd::errs;
use crate::{Error, Result, SecurityManager};

/// pd-client's `requestInfo.logFields()` plus `source` and the extra
/// fields of one log line.
macro_rules! log_request {
    ($level:ident, $info:expr, $source:expr, [$($extra:tt)*], $message:literal) => {
        log::$level!(
            "caller-id":% = $info.caller_id,
            name:% = $info.name,
            uri:% = $info.uri,
            method:% = $info.method,
            "target-url":% = $info.target_url,
            source:% = $source,
            $($extra)*;
            $message
        )
    };
}

/// Go `defaultCallerID`: the default caller ID of the PD HTTP client.
pub(super) const DEFAULT_CALLER_ID: &str = "pd-http-client";
/// Go `defaultInnerCallerID`: the default caller ID of the inner PD HTTP
/// client, used to distinguish the requests sent by the inner client via some
/// internal logic.
pub(super) const DEFAULT_INNER_CALLER_ID: &str = "pd-http-client-inner";
pub(super) const HTTP_SCHEME: &str = "http";
pub(super) const HTTPS_SCHEME: &str = "https";
pub(super) const NETWORK_ERROR_STATUS: &str = "network error";

pub(super) const DEFAULT_MEMBERS_INFO_UPDATE_INTERVAL: Duration = Duration::from_secs(60);
pub(super) const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

// Header key definition constants.
pub(super) const PD_ALLOW_FOLLOWER_HANDLE_KEY: &str = "PD-Allow-Follower-Handle";
pub(super) const X_CALLER_ID_KEY: &str = "X-Caller-ID";

const STATUS_OK: i32 = 200;
const STATUS_BAD_REQUEST: i32 = 400;
const STATUS_FORBIDDEN: i32 = 403;
const STATUS_NOT_FOUND: i32 = 404;
const STATUS_CONFLICT: i32 = 409;
const STATUS_SERVICE_UNAVAILABLE: i32 = 503;

/// One HTTP request as the transport sends it (Go `*http.Request`).
#[derive(Clone, Debug)]
pub struct HttpRequest {
    pub method: Method,
    /// The full URL: the serving member's URL followed by the API URI.
    pub url: String,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

/// One HTTP response with its body already read (Go `*http.Response`).
#[derive(Clone, Debug)]
pub struct HttpResponse {
    /// Go `StatusCode`.
    pub status_code: u16,
    /// Go `Status`, such as `"200 OK"`; the metrics label of the request.
    pub status: String,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
    /// Go `Request`: the request that produced this response.
    pub request: HttpRequest,
}

/// Go `http.RoundTripper`: a custom transport for [`HttpClient`].
#[async_trait]
pub trait RoundTripper: Send + Sync {
    async fn round_trip(&self, req: HttpRequest) -> Result<HttpResponse>;
}

/// Go `*http.Client`: a `reqwest` client or a custom [`RoundTripper`].
#[derive(Clone)]
pub struct HttpClient {
    transport: Transport,
}

#[derive(Clone)]
enum Transport {
    Reqwest(reqwest::Client),
    Custom(Arc<dyn RoundTripper>),
}

impl HttpClient {
    pub fn new(client: reqwest::Client) -> HttpClient {
        HttpClient {
            transport: Transport::Reqwest(client),
        }
    }

    /// Go `&http.Client{Transport: transport}`.
    pub fn with_transport(transport: Arc<dyn RoundTripper>) -> HttpClient {
        HttpClient {
            transport: Transport::Custom(transport),
        }
    }

    /// Go `Do`: send the request and read the whole response body.
    pub async fn do_request(&self, req: HttpRequest) -> Result<HttpResponse> {
        match &self.transport {
            Transport::Custom(transport) => transport.round_trip(req).await,
            Transport::Reqwest(client) => {
                let mut builder = client
                    .request(req.method.clone(), req.url.as_str())
                    .headers(req.headers.clone());
                if !req.body.is_empty() {
                    builder = builder.body(req.body.clone());
                }
                let resp = builder
                    .send()
                    .await
                    .map_err(|error| Error::StringError(error.to_string()))?;
                let status = resp.status();
                let headers = resp.headers().clone();
                let body = resp
                    .bytes()
                    .await
                    .map_err(|error| Error::StringError(error.to_string()))?
                    .to_vec();
                Ok(HttpResponse {
                    status_code: status.as_u16(),
                    status: status.to_string(),
                    headers,
                    body,
                    request: req,
                })
            }
        }
    }
}

impl From<reqwest::Client> for HttpClient {
    fn from(client: reqwest::Client) -> HttpClient {
        HttpClient::new(client)
    }
}

/// Go's `res any` decode target as a response handler sees it.
pub trait ResponseTarget {
    /// Go `json.Unmarshal(body, res)`.
    fn unmarshal(&mut self, body: &[u8]) -> Result<()>;
}

/// Go `respHandleFunc` (the argument of `WithRespHandler`): it takes over the
/// response handling, including the error policy for non-200 statuses. The
/// target is `None` when the API decodes no result; otherwise the handler
/// fills it, or leaves the API's zero value.
pub type RespHandleFunc =
    Arc<dyn Fn(HttpResponse, Option<&mut dyn ResponseTarget>) -> Result<()> + Send + Sync>;

struct JsonTarget<T> {
    value: Option<T>,
}

impl<T: DeserializeOwned> ResponseTarget for JsonTarget<T> {
    fn unmarshal(&mut self, body: &[u8]) -> Result<()> {
        let value: Option<serde_json::Value> =
            serde_json::from_slice(body).map_err(|error| Error::StringError(error.to_string()))?;
        if let Some(value) = value {
            self.value = Some(from_go_json_value(value)?);
        }
        Ok(())
    }
}

/// Decodes a JSON value as Go's `encoding/json` decodes it into a struct:
/// a `null` member leaves the field's zero value. Every response type
/// defaults its missing fields, so dropping `null` members first gives the
/// same result for slices, maps and strings that serde would otherwise
/// reject as `null` (PD sends `{"history_hot_region": null}`).
fn from_go_json_value<T: DeserializeOwned>(mut value: serde_json::Value) -> Result<T> {
    fn strip_null_members(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(members) => {
                members.retain(|_, member| !member.is_null());
                members.values_mut().for_each(strip_null_members);
            }
            serde_json::Value::Array(elements) => elements.iter_mut().for_each(strip_null_members),
            _ => {}
        }
    }
    strip_null_members(&mut value);
    serde_json::from_value(value).map_err(|error| Error::StringError(error.to_string()))
}

/// Go `json.NewDecoder(body).Decode(&res)`: the first JSON value, where
/// `null` leaves the zero value and an empty body is `EOF`.
fn decode_first_json_value<T: DeserializeOwned>(body: &[u8]) -> Result<Option<T>> {
    let mut stream =
        serde_json::Deserializer::from_slice(body).into_iter::<Option<serde_json::Value>>();
    match stream.next() {
        None => Err(Error::StringError("EOF".to_owned())),
        Some(Err(error)) => Err(Error::StringError(error.to_string())),
        Some(Ok(value)) => value.map(from_go_json_value).transpose(),
    }
}

/// Go `HeaderOption`: configures the HTTP header.
pub type HeaderOption = Box<dyn Fn(&mut HeaderMap) + Send + Sync>;

fn header_name(key: &'static str) -> HeaderName {
    HeaderName::from_bytes(key.as_bytes()).expect("valid header name")
}

/// Go `WithAllowFollowerHandle`: sets the header field to allow a PD follower
/// to handle this request.
pub fn with_allow_follower_handle() -> HeaderOption {
    Box::new(|header: &mut HeaderMap| {
        header.insert(
            header_name(PD_ALLOW_FOLLOWER_HANDLE_KEY),
            HeaderValue::from_static("true"),
        );
    })
}

/// Go `noNeedRetry`.
pub(super) fn no_need_retry(status_code: i32) -> bool {
    status_code == STATUS_NOT_FOUND
        || status_code == STATUS_FORBIDDEN
        || status_code == STATUS_BAD_REQUEST
}

/// Go `noNeedRetryForRequest`.
pub(super) fn no_need_retry_for_request(req_info: &RequestInfo, status_code: i32) -> bool {
    no_need_retry_for_request_name(&req_info.name, status_code)
}

fn no_need_retry_for_request_name(name: &str, status_code: i32) -> bool {
    if no_need_retry(status_code) {
        return true;
    }
    name == UPDATE_KEYSPACE_CONFIG_NAME && status_code == STATUS_CONFLICT
}

/// Go `clientInner`: the fundamental fields every derived client shares.
pub(super) struct ClientInner {
    sd: Arc<dyn ServiceDiscovery>,
    // Marks the source of the client creation.
    source: String,
    cli: HttpClient,
    request_counter: Option<CounterVec>,
    execution_duration: Option<HistogramVec>,
    // Whether the client is created with (and so owns) its service discovery.
    default_sd: bool,
}

impl ClientInner {
    async fn close(&self) {
        // Only close the service discovery if it's created by the client.
        if self.default_sd {
            self.sd.close().await;
        }
    }

    fn req_counter(&self, name: &str, status: &str) {
        if let Some(counter) = &self.request_counter {
            counter.with_label_values(&[name, status]).inc();
        }
    }

    fn exec_duration(&self, name: &str, duration: Duration) {
        if let Some(histogram) = &self.execution_duration {
            histogram
                .with_label_values(&[name])
                .observe(duration.as_secs_f64());
        }
    }

    /// Go `requestWithRetry`: first try to send the request to the PD leader;
    /// if it fails, try the other PD followers to gain a better availability.
    /// With a backoffer, the whole member pass is retried under its budget;
    /// the status code and error classification persist across passes as
    /// Go's captured variables do.
    pub(super) async fn request_with_retry<T>(
        &self,
        req_info: &RequestInfo,
        header_opts: &[HeaderOption],
    ) -> Result<Option<T>>
    where
        T: DeserializeOwned + Send,
    {
        let status_code = Arc::new(AtomicI32::new(0));
        let leader_change = AtomicBool::new(false);
        let output: Mutex<Option<Option<T>>> = Mutex::new(None);
        let status = &*status_code;
        let leader_change_ref = &leader_change;
        let output_ref = &output;
        let exec_func =
            move || self.exec_once(req_info, header_opts, status, leader_change_ref, output_ref);
        match &req_info.bo {
            None => exec_func().await?,
            Some(bo) => {
                // Copy a new backoffer for each request.
                let mut bo = bo.clone();
                // Set the retryable checker for the backoffer if it's not set.
                let checker_status = status_code.clone();
                let name = req_info.name.clone();
                bo.set_retryable_checker(
                    Some(Box::new(move |_error: &Error| {
                        // Backoffer also needs to check the status code to
                        // determine whether to retry.
                        !no_need_retry_for_request_name(
                            &name,
                            checker_status.load(Ordering::SeqCst),
                        )
                    })),
                    false,
                );
                bo.exec(std::future::pending::<Error>(), exec_func).await?
            }
        }
        let output = output
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(output.flatten())
    }

    // One pass over the members, then Go's deferred membership check and log.
    async fn exec_once<T>(
        &self,
        req_info: &RequestInfo,
        header_opts: &[HeaderOption],
        status_code: &AtomicI32,
        leader_change: &AtomicBool,
        output: &Mutex<Option<Option<T>>>,
    ) -> Result<()>
    where
        T: DeserializeOwned + Send,
    {
        let (result, server_url, is_leader) = self
            .exec_members(req_info, header_opts, status_code, leader_change, output)
            .await;
        let code = status_code.load(Ordering::SeqCst);
        // If the status code is 503, it indicates that there may be PD
        // leader/follower changes. If the error message contains the
        // leader/primary change information, it indicates that there may be
        // PD leader/primary change.
        if code == STATUS_SERVICE_UNAVAILABLE || leader_change.load(Ordering::SeqCst) {
            self.sd.schedule_check_member_changed();
        }
        match &result {
            Ok(()) => log_request!(
                debug, req_info, self.source,
                ["server-url":% = server_url, "is-leader" = is_leader, "status-code" = code],
                "[pd] http request finished"
            ),
            Err(error) => log_request!(
                debug, req_info, self.source,
                ["server-url":% = server_url, "is-leader" = is_leader, "status-code" = code, error:% = error],
                "[pd] http request finished"
            ),
        }
        result
    }

    // Try the leader first and then the followers; returns the last tried
    // member for the log.
    async fn exec_members<T>(
        &self,
        req_info: &RequestInfo,
        header_opts: &[HeaderOption],
        status_code: &AtomicI32,
        leader_change: &AtomicBool,
        output: &Mutex<Option<Option<T>>>,
    ) -> (Result<()>, String, bool)
    where
        T: DeserializeOwned + Send,
    {
        let mut server_url = String::new();
        let mut is_leader = false;
        let clients = self.sd.get_all_service_clients().await;
        if clients.is_empty() {
            return (
                Err(Error::Pd(errs::ERR_CLIENT_NO_AVAILABLE_MEMBER.error())),
                server_url,
                is_leader,
            );
        }
        let mut skip_num = 0;
        let mut last_error = None;
        for cli in &clients {
            server_url = cli.get_url().to_owned();
            is_leader = cli.is_connected_to_leader();
            if !req_info.target_url.is_empty() && req_info.target_url != server_url {
                skip_num += 1;
                continue;
            }
            let (code, result) = self
                .do_request::<T>(&server_url, req_info, header_opts)
                .await;
            status_code.store(code, Ordering::SeqCst);
            let error = match result {
                Ok(value) => {
                    leader_change.store(false, Ordering::SeqCst);
                    *output
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(value);
                    return (Ok(()), server_url, is_leader);
                }
                Err(error) => error,
            };
            leader_change.store(errs::is_leader_change(Some(&error)), Ordering::SeqCst);
            if no_need_retry_for_request(req_info, code) {
                return (Err(error), server_url, is_leader);
            }
            log_request!(
                debug, req_info, self.source,
                ["server-url":% = server_url, "is-leader" = is_leader, "status-code" = code, error:% = error],
                "[pd] http request url failed"
            );
            last_error = Some(error);
        }
        if skip_num == clients.len() {
            return (
                Err(Error::Pd(errs::ERR_CLIENT_NO_TARGET_MEMBER.error())),
                server_url,
                is_leader,
            );
        }
        let error = last_error.expect("a member was requested");
        (Err(error), server_url, is_leader)
    }

    /// Go `doRequest`: one request to one member. The status code is -1 when
    /// no response was received.
    async fn do_request<T>(
        &self,
        server_url: &str,
        req_info: &RequestInfo,
        header_opts: &[HeaderOption],
    ) -> (i32, Result<Option<T>>)
    where
        T: DeserializeOwned + Send,
    {
        let name = req_info.name.as_str();
        let url = req_info.get_url(server_url);
        log_request!(debug, req_info, self.source, [url:% = url], "[pd] request the http url");
        let mut headers = HeaderMap::new();
        for opt in header_opts {
            opt(&mut headers);
        }
        match HeaderValue::from_str(&req_info.caller_id) {
            Ok(value) => {
                headers.insert(header_name(X_CALLER_ID_KEY), value);
            }
            Err(error) => {
                log_request!(
                    error, req_info, self.source, [url:% = url, error:% = error],
                    "[pd] create http request failed"
                );
                return (-1, Err(Error::StringError(error.to_string())));
            }
        }
        let req = HttpRequest {
            method: req_info.method.clone(),
            url: url.clone(),
            headers,
            body: req_info.body.clone(),
        };

        let start = Instant::now();
        let resp = match self.cli.do_request(req).await {
            Ok(resp) => resp,
            Err(error) => {
                self.req_counter(name, NETWORK_ERROR_STATUS);
                log_request!(
                    warn, req_info, self.source, [url:% = url, error:% = error],
                    "[pd] do http request failed"
                );
                return (-1, Err(error));
            }
        };
        self.exec_duration(name, start.elapsed());
        self.req_counter(name, &resp.status);
        let status_code = i32::from(resp.status_code);

        // Give away the response handling to the caller if the handler is set.
        if let Some(handler) = &req_info.resp_handler {
            if !req_info.res {
                return (status_code, handler(resp, None).map(|()| None));
            }
            let mut target = JsonTarget::<T> { value: None };
            let result = handler(resp, Some(&mut target as &mut dyn ResponseTarget));
            return (status_code, result.map(|()| target.value));
        }

        if status_code != STATUS_OK {
            // PD will return a JSON body containing the detailed error message
            // when the status code is not `http.StatusOK` 200.
            let body = String::from_utf8_lossy(resp.body.trim_ascii()).into_owned();
            log_request!(
                warn, req_info, self.source,
                [url:% = url, status:% = resp.status, body:% = body],
                "[pd] request failed with a non-200 status"
            );
            return (
                status_code,
                Err(Error::StringError(format!(
                    "request pd http api failed with status: '{}', body: '{}'",
                    resp.status, body
                ))),
            );
        }

        if !req_info.res {
            return (status_code, Ok(None));
        }
        (status_code, decode_first_json_value::<T>(&resp.body))
    }
}

/// Go `client`: a PD (Placement Driver) HTTP client. Clones share the
/// underlying HTTP client and service discovery; the `with_*` methods return
/// a modified copy, as Go's do.
#[derive(Clone)]
pub struct Client {
    pub(super) inner: Arc<ClientInner>,
    pub(super) caller_id: String,
    pub(super) resp_handler: Option<RespHandleFunc>,
    pub(super) bo: Option<Backoffer>,
    pub(super) target_url: String,
}

// The settings Go's options write into `client.inner` before `init`.
#[derive(Default)]
struct ClientOptions {
    cli: Option<HttpClient>,
    tls_conf: Option<Arc<SecurityManager>>,
    request_counter: Option<CounterVec>,
    execution_duration: Option<HistogramVec>,
}

/// Go `ClientOption`: configures the HTTP client.
pub struct ClientOption(Box<dyn FnOnce(&mut ClientOptions) + Send>);

/// Go `WithHTTPClient`: configures the client with the given initialized HTTP
/// client.
pub fn with_http_client(cli: impl Into<HttpClient>) -> ClientOption {
    let cli = cli.into();
    ClientOption(Box::new(move |options: &mut ClientOptions| {
        options.cli = Some(cli)
    }))
}

/// Go `WithTLSConfig`: configures the client with the given TLS configuration.
/// A manager without a CA leaves the client on plain HTTP. This option won't
/// work if the client is configured with [`with_http_client`].
pub fn with_tls_config(tls_conf: Arc<SecurityManager>) -> ClientOption {
    ClientOption(Box::new(move |options: &mut ClientOptions| {
        options.tls_conf = Some(tls_conf)
    }))
}

/// Go `WithMetrics`: configures the client with metrics. The counter is
/// labeled `(name, status)` and the histogram `(name)`.
pub fn with_metrics(request_counter: CounterVec, execution_duration: HistogramVec) -> ClientOption {
    ClientOption(Box::new(move |options: &mut ClientOptions| {
        options.request_counter = Some(request_counter);
        options.execution_duration = Some(execution_duration);
    }))
}

impl ClientOptions {
    fn apply(opts: Vec<ClientOption>) -> ClientOptions {
        let mut options = ClientOptions::default();
        for opt in opts {
            (opt.0)(&mut options);
        }
        options
    }

    // Go `clientInner.init`: the HTTP client, if it's not configured.
    fn http_client(&mut self) -> Result<HttpClient> {
        match self.cli.take() {
            Some(cli) => Ok(cli),
            None => Ok(HttpClient::new(build_default_http_client(
                self.tls_conf.as_deref(),
            )?)),
        }
    }

    fn finish(
        self,
        source: &str,
        cli: HttpClient,
        sd: Arc<dyn ServiceDiscovery>,
        default_sd: bool,
    ) -> Client {
        Client {
            inner: Arc::new(ClientInner {
                sd,
                source: source.to_owned(),
                cli,
                request_counter: self.request_counter,
                execution_duration: self.execution_duration,
                default_sd,
            }),
            caller_id: DEFAULT_CALLER_ID.to_owned(),
            resp_handler: None,
            bo: None,
            target_url: String::new(),
        }
    }
}

// Go `&http.Client{Timeout: defaultTimeout}` with the TLS transport. As with a
// Go `tls.Config` carrying `RootCAs`, only the configured CA is trusted.
fn build_default_http_client(tls_conf: Option<&SecurityManager>) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(DEFAULT_TIMEOUT);
    if let Some(security) = tls_conf.filter(|security| security.tls_configured()) {
        let (ca_path, cert_path, key_path) = security.tls_paths();
        let ca_path =
            ca_path.ok_or_else(|| Error::StringError("TLS is not configured".to_owned()))?;
        let ca = std::fs::read(ca_path)?;
        let certificates = reqwest::Certificate::from_pem_bundle(&ca)
            .map_err(|error| Error::StringError(format!("failed to append ca certs: {error}")))?;
        builder = builder.tls_built_in_root_certs(false);
        for certificate in certificates {
            builder = builder.add_root_certificate(certificate);
        }
        if let (Some(cert_path), Some(key_path)) = (cert_path, key_path) {
            let certificate = std::fs::read(cert_path)?;
            let key = std::fs::read(key_path)?;
            let certificate = openssl::x509::X509::from_pem(&certificate).map_err(|error| {
                Error::StringError(format!("could not load client certificate: {error}"))
            })?;
            let key = openssl::pkey::PKey::private_key_from_pem(&key).map_err(|error| {
                Error::StringError(format!("could not load client private key: {error}"))
            })?;
            let identity = openssl::pkcs12::Pkcs12::builder()
                .name("")
                .pkey(&key)
                .cert(&certificate)
                .build2("")
                .and_then(|identity| identity.to_der())
                .map_err(|error| {
                    Error::StringError(format!("could not build client identity: {error}"))
                })?;
            let identity = reqwest::Identity::from_pkcs12_der(&identity, "").map_err(|error| {
                Error::StringError(format!("could not load client key pair: {error}"))
            })?;
            builder = builder.identity(identity);
        }
    }
    builder
        .build()
        .map_err(|error| Error::StringError(error.to_string()))
}

/// Go `NewClientWithServiceDiscovery`: creates a PD HTTP client with the given
/// service discovery, which the client never closes. Go cannot fail here;
/// loading the TLS PEM files can.
pub fn new_client_with_service_discovery(
    source: &str,
    sd: Arc<dyn ServiceDiscovery>,
    opts: Vec<ClientOption>,
) -> Result<Client> {
    // Apply the options first.
    let mut options = ClientOptions::apply(opts);
    let cli = options.http_client()?;
    Ok(options.finish(source, cli, sd, false))
}

/// Go `NewClient`: creates a PD HTTP client with the given PD addresses and
/// its own service discovery: client-rust's PD client, which discovers the
/// members and leader over gRPC and is closed with this client. Go logs and
/// returns nil when the discovery cannot initialize; this returns the error.
pub async fn new_client(
    source: &str,
    pd_addrs: &[String],
    opts: Vec<ClientOption>,
) -> Result<Client> {
    // Apply the options first.
    let mut options = ClientOptions::apply(opts);
    let cli = options.http_client()?;
    let security = options.tls_conf.clone().unwrap_or_default();
    let sd = match DefaultServiceDiscovery::new(pd_addrs, security).await {
        Ok(sd) => sd,
        Err(error) => {
            log::error!(
                source:% = source,
                "pd-addrs":serde = pd_addrs,
                error:% = error;
                "[pd] init service discovery failed"
            );
            return Err(error);
        }
    };
    Ok(options.finish(source, cli, Arc::new(sd), true))
}

// Go `sd.NewDefaultServiceDiscovery` as `NewClient` owns it.
struct DefaultServiceDiscovery {
    pd: Arc<crate::pd::RetryClient>,
    sd: Arc<dyn ServiceDiscovery>,
}

impl DefaultServiceDiscovery {
    async fn new(
        pd_addrs: &[String],
        security: Arc<SecurityManager>,
    ) -> Result<DefaultServiceDiscovery> {
        let pd = crate::pd::RetryClient::connect_with_options(
            pd_addrs,
            security,
            crate::pd::opt::Options::new(),
            None,
        )
        .await?;
        let sd = pd.get_service_discovery();
        Ok(DefaultServiceDiscovery {
            pd: Arc::new(pd),
            sd,
        })
    }
}

#[async_trait]
impl ServiceDiscovery for DefaultServiceDiscovery {
    async fn get_all_service_clients(&self) -> Vec<ServiceClient> {
        self.sd.get_all_service_clients().await
    }

    fn schedule_check_member_changed(&self) {
        self.sd.schedule_check_member_changed();
    }

    async fn close(&self) {
        self.pd.close().await;
    }
}

impl Client {
    /// Go `Close`: gracefully closes the HTTP client. A client-owned service
    /// discovery (from [`new_client`]) is closed; idle connections are released
    /// when the last clone is dropped.
    pub async fn close(&self) {
        self.inner.close().await;
        log::info!(source:% = self.inner.source; "[pd] http client closed");
    }

    /// Go `WithCallerID`: sets and returns a new client with the given caller ID.
    pub fn with_caller_id(&self, caller_id: impl Into<String>) -> Client {
        let mut new_client = self.clone();
        new_client.caller_id = caller_id.into();
        new_client
    }

    /// Go `WithRespHandler`: sets and returns a new client with the given HTTP
    /// response handler. This allows the caller to customize how the response
    /// is handled, including error handling logic. `None` restores the
    /// default handling.
    pub fn with_resp_handler(&self, handler: impl Into<Option<RespHandleFunc>>) -> Client {
        let mut new_client = self.clone();
        new_client.resp_handler = handler.into();
        new_client
    }

    /// Go `WithBackoffer`: sets and returns a new client with the given
    /// backoffer. Each request retries on a copy of it.
    pub fn with_backoffer(&self, bo: impl Into<Option<Backoffer>>) -> Client {
        let mut new_client = self.clone();
        new_client.bo = bo.into();
        new_client
    }

    /// Go `WithTargetURL`: sets and returns a new client with the given target
    /// URL; requests go only to the member with exactly that URL.
    pub fn with_target_url(&self, target_url: impl Into<String>) -> Client {
        let mut new_client = self.clone();
        new_client.target_url = target_url.into();
        new_client
    }

    /// Go `request`.
    pub(super) async fn request<T>(
        &self,
        req_info: RequestInfo,
        header_opts: &[HeaderOption],
    ) -> Result<Option<T>>
    where
        T: DeserializeOwned + Send,
    {
        let req_info = req_info
            .with_caller_id(self.caller_id.clone())
            .with_resp_handler(self.resp_handler.clone())
            .with_backoffer(self.bo.clone())
            .with_target_url(self.target_url.clone());
        self.inner.request_with_retry(&req_info, header_opts).await
    }

    /// A request that decodes its result; a JSON `null` or an untouched
    /// handler target is the zero value, as in Go.
    pub(super) async fn request_resp<T>(
        &self,
        req_info: RequestInfo,
        header_opts: &[HeaderOption],
    ) -> Result<T>
    where
        T: DeserializeOwned + Default + Send,
    {
        Ok(self
            .request::<T>(req_info.with_resp(), header_opts)
            .await?
            .unwrap_or_default())
    }

    /// A request without a result.
    pub(super) async fn request_no_resp(
        &self,
        req_info: RequestInfo,
        header_opts: &[HeaderOption],
    ) -> Result<()> {
        self.request::<serde_json::Value>(req_info, header_opts)
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
