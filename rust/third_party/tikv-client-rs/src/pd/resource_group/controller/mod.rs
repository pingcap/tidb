// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! PD client's `resource_group/controller` package: the client-side resource
//! group controller that charges request units against local token buckets
//! and acquires tokens from the resource manager. Its `metrics` subpackage
//! is [`metrics`].
//!
//! The controller is installed for TiKV requests as client-rust's resource
//! control interceptor, as client-go installs it with
//! `tikv.SetResourceControlInterceptor`:
//!
//! ```ignore
//! let controller = new_resource_group_controller(id, pd_client, None, keyspace_id, vec![]).await?;
//! controller.start();
//! tikv_client::set_resource_control_interceptor(controller.clone());
//! ```
//!
//! # Narrowings, by name
//!
//! - **Go `context.Context` parameters** of `NewResourceGroupController`,
//!   `Start`, `OnRequestWait`, `OnResponseWait`, `IsBackgroundRequest` and of
//!   the `ResourceGroupProvider` methods are not taken. A caller cancels by
//!   dropping the future (a pending token reservation is returned to its
//!   limiter, as Go's context cancellation does) and stops the main loop
//!   with `stop`. Consequently a caller deadline never bounds a token
//!   reservation: `LTBMaxWaitDuration` always does. The exported limiter API
//!   (`Limiter::reserve`, `wait_reservations`) keeps its [`Context`]
//!   parameter, whose deadline and cancellation behave as in Go.
//! - **`OnRequestWait` / `OnResponseWait` error results** carry no wait
//!   duration: Go returns the time already waited next to the error, which
//!   every client-go caller discards.
//! - **client-rust interceptor settlement**: the synchronous
//!   `ResourceGroupController::on_response_wait` hook is Go's non-waiting
//!   `OnResponse` (client-go uses it for txn-file commits). Go's waiting
//!   `OnResponseWait` is `ResourceGroupsController::on_response_wait` and the
//!   interceptor's `on_response_wait_async`; client-rust's dispatch path
//!   holds a non-`Send` response across settlement and so still settles
//!   synchronously, letting a large response of a throttled group go into
//!   debt instead of waiting.
//! - **client-rust interceptor background check**: the synchronous
//!   `ResourceGroupController::is_background_request` hook answers from the
//!   local group cache; on a miss it starts the fetch Go performs inline and
//!   answers `false` until the group (or the default group it falls back to)
//!   is cached. `ResourceGroupsController::is_background_request` is Go's
//!   fetching version.
//! - **Negative durations** in the controller configuration are rejected:
//!   Go's `time.Duration` is signed, `std::time::Duration` is not.
//! - **`getSysProcessCPUTime`** reads `getrusage(RUSAGE_SELF)` instead of
//!   gosigar's per-pid process time (both are the process's user plus system
//!   CPU in whole milliseconds); it answers 0 on non-Unix targets.
//! - **Nil-pointer panics** of Go become ordinary paths: a group without RU
//!   settings is "not configured", a watch event without a key-value is
//!   skipped, and a granted bucket without settings uses zero settings.
//! - **`AcquireTokenBuckets` server errors**: when the token stream answers
//!   with an error, PD's Go client drops the request's completion and the
//!   request waits until its context ends; here the request fails with the
//!   `[resource_manager]` error.
//! - **Tests**: `TestMain`'s goroutine-leak check has no counterpart, and
//!   testify's mock is a hand-written mock. `TestDurationTOML` drives
//!   `UnmarshalText` directly because no TOML decoder is a dependency.

mod config;
mod context;
mod global_controller;
mod group_controller;
mod limiter;
pub mod metrics;
mod model;
mod testutil;
mod util;

pub use self::config::{
    default_config, default_request_unit_config, default_ru_config, generate_ru_config, BaseConfig,
    Config, LocalBucketConfig, RUConfig, RUVersion, RUVersionPolicy, RequestUnitConfig,
    TokenRPCParams, DEFAULT_RU_VERSION, RU_VERSION_V1, RU_VERSION_V2,
};
pub use self::context::{CancelFunc, Context, ContextError};
pub use self::global_controller::{
    enable_single_group_by_keyspace, new_resource_group_controller,
    new_resource_group_not_exist_err, with_degraded_mode_wait_duration, with_degraded_ru_settings,
    with_max_wait_duration, with_wait_retry_interval, with_wait_retry_times, ControllerOptions,
    ResourceControlCreateOption, ResourceGroupKVInterceptor, ResourceGroupProvider,
    ResourceGroupRuntimeState, ResourceGroupsController, FROM_LOW_RU, FROM_PERIOD_REPORT,
};
pub use self::limiter::{
    every, new_limiter, new_limiter_with_cfg, wait_reservations, FillRate, Limiter, LimiterOption,
    NotifyMsg, Reservation, TokenBucketReconfigureArgs, INF, INF_DURATION,
};
pub use self::model::{
    AccessLocationType, KVCalculator, RequestInfo, RequestUnit, ResourceCalculator, ResponseInfo,
    SQLCalculator,
};
pub use self::testutil::{
    new_test_request_info, new_test_response_info, TestRequestInfo, TestResponseInfo,
};
pub use self::util::{new_duration, Duration};

#[cfg(test)]
mod global_controller_tests;
#[cfg(test)]
mod group_controller_tests;
#[cfg(test)]
mod limiter_tests;
#[cfg(test)]
mod model_tests;
#[cfg(test)]
mod request_source_metrics_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod util_tests;
