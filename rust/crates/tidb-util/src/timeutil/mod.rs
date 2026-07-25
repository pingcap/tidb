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

//! Complete transcreation of Go `pkg/util/timeutil` (`errors.go`, `time.go`,
//! `time_zone.go`): TiDB's time-zone infrastructure — system-timezone
//! inference, named/offset zone parsing, and the day-time-period check.
//!
//! Go's `*time.Location` maps to [`TimeZone`]: the process-local zone
//! (`System`), an IANA zone backed by `chrono-tz`'s compiled tzdata (the
//! existing equal library — no hand-maintained timezone table), or a fixed
//! offset (`time.FixedZone`). Go's `locCache` exists to amortize
//! `time.LoadLocation`'s file I/O; `chrono-tz` resolves names from static
//! data with no I/O, so the cache is a non-observable performance artifact
//! with nothing to port.
//!
//! `time.go`'s `Sleep(ctx, d)` is a Go-context-interruptible sleep; the
//! cancellation primitive belongs to the async server runtime
//! (`tokio::select!` over a sleep and a cancel signal) and fabricating a
//! polling flag here would invent API, so it is deliberately not ported.

mod time_zone;

pub use time_zone::{
    construct_time_zone, get_system_tz, infer_system_tz, load_location, parse_time_zone,
    set_system_tz, system_location, within_day_time_period, zone, zone_name, TimeZone,
    TimeZoneError,
};

use std::sync::LazyLock;
use tidb_error::terror::TerrorError;
use tidb_error::tidb::errcode;

/// `ErrUnknownTimeZone` (Go `errors.go`): unknown time zone.
pub static ERR_UNKNOWN_TIME_ZONE: LazyLock<TerrorError> =
    LazyLock::new(|| crate::dbterror::CLASS_VARIABLE.new_std(errcode::ErrUnknownTimeZone));
