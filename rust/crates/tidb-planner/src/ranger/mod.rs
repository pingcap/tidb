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

//! Go `pkg/util/ranger`: scan-range construction — the machinery that turns
//! predicates into index/table key ranges.
//!
//! Ported file by file toward the whole-package claim:
//! * [`types`] — `types.go`, the `Range`/`Ranges` model (COMPLETE, with
//!   `types_test.go` transcreated).
//! * [`checker`] — `checker.go`, the access-condition admission
//!   (COMPLETE).
//! * [`points`] — `points.go`, on the whole-file track: the point model,
//!   comparators, full-range constructors, and constant fixups are in; the
//!   builder dispatch continues there.
//! * `ranger.go`, `detacher.go` follow on this track, in dependency order.

pub mod checker;
pub mod points;
pub mod types;

pub use types::{HasFullRange, Range, Ranges};
