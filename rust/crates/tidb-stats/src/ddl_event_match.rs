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

//! DDL event selection before timeout from
//! `pkg/statistics/handle/ddl/testutil/util.go`.
//!
//! The Go helper consumes events until the first matching type arrives or its
//! timeout fires. This leaf models the dependency-closed contract over the
//! caller-owned events observed before timeout: return the first matching item,
//! or `None` when no match was observed. Channel blocking, ticker duration,
//! notifier event decoding, and DDL/session lifecycle remain external.

/// Returns the first matching event observed before the caller's timeout.
///
/// `events` contains only events received before the timeout boundary. An
/// empty slice, or a slice without `target`, models the Go helper returning
/// `nil` after its ticker fires.
#[must_use]
pub fn find_event_with_timeout<T: Copy + PartialEq>(events: &[T], target: T) -> Option<T> {
    events.iter().copied().find(|event| *event == target)
}
