// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The complete pinned PD `client/http` package (pdhttp,
//! `github.com/tikv/pd/client@v0.0.0-20260805103528-afa43111d149/http`): the
//! PD HTTP API client TiDB uses for regions, stores, placement rules, label
//! rules, schedulers, keyspaces, affinity groups and the other PD RESTful
//! endpoints.
//!
//! Every request tries the PD leader first and then each follower, unless a
//! target URL pins it to one member. A 400, 403 or 404 (and a 409 for
//! `UpdateKeyspaceConfig`) ends the pass, as does success; a 503 or a
//! leader-change error schedules a membership check on the service discovery.
//! With a backoffer, the whole pass repeats under the backoffer's budget.
//!
//! The files map one to one: `api.go` -> `api.rs`, `client.go` -> `client.rs`,
//! `codec.go` -> `codec.rs`, `interface.go` -> `interface.rs`,
//! `request_info.go` -> `request_info.rs`, `types.go` -> `types.rs`,
//! `testutil.go` -> `testutil.rs`; the tests are `client_tests.rs`,
//! `codec_tests.rs` and `types_tests.rs`. The part of PD's
//! `servicediscovery` package this client calls is `service_discovery.rs`.
//!
//! # Narrowings, by name
//!
//! - `context.Context` parameters: dropped from every method. A request is
//!   cancelled by dropping its future and bounded with `tokio::time::timeout`;
//!   a backoffer therefore stops only on its own budget or when dropped.
//! - `Client` (Go interface): a concrete cloneable struct; `Close` is async
//!   because closing a client-owned discovery closes client-rust's PD client.
//!   Go's `CloseIdleConnections` has no reqwest counterpart; idle connections
//!   are released with the last clone.
//! - `WithHTTPClient(*http.Client)`: takes an [`HttpClient`], either a
//!   `reqwest::Client` or a [`RoundTripper`] (Go's `http.RoundTripper` seam,
//!   which `NewHTTPClientWithRequestChecker` uses). The transport reads the
//!   whole body, so a body read failure counts as a transport failure (status
//!   -1, `network error` label) rather than a decode or non-200 error.
//! - `WithRespHandler(func(*http.Response, any) error)`: the handler receives
//!   the buffered [`HttpResponse`] and, when the API decodes a result, a
//!   [`ResponseTarget`] standing for Go's `res any`.
//! - `WithTLSConfig(*tls.Config)`: takes client-rust's `SecurityManager`; its
//!   PEM files are loaded when the client is built, so the constructors return
//!   `Result`.
//! - `WithMetrics`: `prometheus::CounterVec` labeled `(name, status)` and
//!   `HistogramVec` labeled `(name)`, as in Go.
//! - `NewClient`: async, returns the initialization error instead of nil, and
//!   its default discovery is client-rust's PD client (`RetryClient`), which
//!   also starts that client's TSO dispatcher; Go's default discovery opens
//!   only membership RPCs.
//! - `NewClientWithServiceDiscovery(source, sd.ServiceDiscovery, ...)`: the
//!   discovery is the [`ServiceDiscovery`] trait, reduced to
//!   `GetAllServiceClients`, `ScheduleCheckMemberChanged` and `Close`.
//! - `KeyRange`/`NewKeyRange`: defined here, since client-rust has no port of
//!   PD's `clients/router` package that Go aliases.
//! - Methods returning a Go pointer that is nil for a JSON `null`
//!   (`GetCluster`, `GetClusterStatus`, `GetStatus`) return `Option`;
//!   `GetMinResolvedTSByStoresIDs` returns its nil map as `None`; Go `any`
//!   is `serde_json::Value`; Go `int` is `i64`; Go `time.Time` is [`Time`].
//! - JSON: Go's decoder matches field names case-insensitively, while these
//!   types match the exact tag; a nil and an empty Go slice or map both encode
//!   as an empty JSON array or object here, where Go writes `null` for nil.
//! - `TestMain` (goleak) has no Rust counterpart.

mod api;
mod client;
mod codec;
mod interface;
mod request_info;
mod service_discovery;
mod testutil;
mod types;

pub use self::api::*;
pub use self::client::{
    new_client, new_client_with_service_discovery, with_allow_follower_handle, with_http_client,
    with_metrics, with_tls_config, Client, ClientOption, HeaderOption, HttpClient, HttpRequest,
    HttpResponse, RespHandleFunc, ResponseTarget, RoundTripper,
};
pub use self::interface::{with_skip_exist_check, CreateAffinityGroupsOption};
pub use self::request_info::{
    DELETE_GC_SAFE_POINT_NAME, GET_GC_SAFE_POINT_NAME, GET_KEYSPACE_META_BY_ID_NAME,
    GET_KEYSPACE_META_BY_NAME_NAME, UPDATE_KEYSPACE_CONFIG_NAME,
    UPDATE_KEYSPACE_GC_MANAGEMENT_TYPE_NAME,
};
pub use self::service_discovery::{ServiceClient, ServiceDiscovery};
pub use self::testutil::new_http_client_with_request_checker;
pub use self::types::*;
