// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/timer/api` lands as a complete package: the timer framework's
//! public surface — the timer record and its schedule policies, the condition
//! algebra stores use to filter records, the store interfaces plus the
//! in-memory implementation, the hook contract, and the default client.
//!
//! File mapping (one Rust module per Go file):
//! - `error.rs` <- `error.go`
//! - `hook.rs` <- `hook.go`
//! - `timer.rs` <- `timer.go`
//! - `store.rs` <- `store.go`
//! - `mem_store.rs` <- `mem_store.go`
//! - `client.rs` <- `client.go`
//!
//! Three further modules stand in for dependencies Go takes from outside the
//! package; each carries its own `boundary:` header explaining exactly what is
//! and is not covered:
//! - [`go_time`] replaces the standard library's `time.Time`/`*time.Location`
//!   pair, which this package stores in every record and compares as a value.
//! - [`cron`] replaces `github.com/robfig/cron/v3`, unavailable to this
//!   offline build. It covers `ParseStandard`'s five-field grammar, the
//!   `@yearly`/`@monthly`/`@weekly`/`@daily`/`@midnight`/`@hourly`/`@every`
//!   descriptors, and `Next`'s five-year horizon; it does not cover the
//!   optional second/year field sets (unreachable through `ParseStandard`) or
//!   the `TZ=`/`CRON_TZ=` spec prefix.
//! - [`uuid`] replaces `github.com/google/uuid`, likewise unavailable. The
//!   package only ever needs `hex.EncodeToString(uuid.New()[:])`, so the
//!   module produces exactly that: a random version-4 value as 32 hex digits.
//!
//! Narrowings, each named at its own definition site:
//! - Go's `errors.New` sentinels plus `errors.ErrorEqual` become variants of
//!   [`error::TimerError`], compared with `==`.
//! - `TimerCond.FieldsSet` and `TimerUpdate.FieldsSet` reflect over their own
//!   struct in Go and exclude fields by `unsafe.Pointer` identity. This
//!   workspace forbids `unsafe`, so the field lists are written out in
//!   declaration order (the order the upstream tests assert) and exclusions
//!   name the same Go field names the result reports.
//! - `TimerRecord` embeds `TimerSpec`, `ManualRequest` and `EventExtra` in Go,
//!   promoting their fields. Rust has no promotion, so they are the named
//!   fields `spec`, `manual_request` and `event_extra`.
//! - `CreateSchedEventPolicy` returns the [`timer::SchedPolicy`] enum rather
//!   than a boxed [`timer::SchedEventPolicy`], so the upstream `require.IsType`
//!   assertions stay expressible without downcasting.
//! - Go's `context.Context` becomes [`store::Context`], carrying only the
//!   cancellation the watch path observes.
//! - `Create`/`Update`'s `record == nil` and `update == nil` guards have no
//!   Rust counterpart: both take references, so the nil case cannot arise.
//! - `NewSchedIntervalPolicy`'s `failpoint.Inject("overwrite-ttl-job-interval")`
//!   does not come across; this workspace has no failpoint registry, and the
//!   injection only shortens the interval inside TTL's own integration tests.
//! - `pkg/util.RunWithRetry`, the single symbol `client.go` borrows from
//!   `pkg/util`, is reproduced privately in `client.rs` without its Prometheus
//!   counter.
//!
//! Go's `memoryStoreCore.List` iterates a map, so it returns records in a
//! random order; the Rust port iterates a `HashMap` and is likewise unordered.
//! Every caller either filters to a single record or compares order-insensitively.

pub mod client;
pub mod cron;
pub mod error;
pub mod go_time;
pub mod hook;
pub mod mem_store;
pub mod store;
pub mod timer;
pub mod uuid;

pub use client::{
    new_default_timer_client, with_id, with_key, with_key_prefix, with_set_enable,
    with_set_sched_expr, with_set_summary_data, with_set_tags, with_set_time_zone,
    with_set_watermark, with_tag, DefaultTimerClient, GetTimerOption, TimerClient,
    UpdateTimerOption, DEFAULT_STORE_NAMESPACE,
};
pub use error::{Result, TimerError};
pub use go_time::GoTime;
pub use hook::{Hook, HookFactory, PreSchedEventResult, TimerShedEvent};
pub use mem_store::{
    get_mem_store_time_zone_loc, new_mem_timer_watch_event_notifier, new_memory_timer_store,
    normalize_time_fields, MemTimerWatchEventNotifier, MemoryStoreCore,
};
pub use store::{
    and, not, or, Cond, Context, Operator, OperatorTp, OptionalVal, TimerCond, TimerStore,
    TimerStoreCore, TimerUpdate, TimerWatchEventNotifier, WatchTimerChan, WatchTimerEvent,
    WatchTimerEventType, WatchTimerResponse,
};
pub use timer::{
    create_sched_event_policy, validate_time_zone, CronPolicy, EventExtra, ManualRequest,
    SchedEventPolicy, SchedEventStatus, SchedIntervalPolicy, SchedPolicy, SchedPolicyType,
    TimerRecord, TimerSpec,
};
