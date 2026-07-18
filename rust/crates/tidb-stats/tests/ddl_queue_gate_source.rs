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

//! Source-backed tests for DDL event readiness.

use tidb_stats::{ddl_queue_disposition, DdlQueueDisposition};

#[test]
fn source_uninitialized_enabled_queue_retries_ddl_event() {
    assert_eq!(
        ddl_queue_disposition(false, true),
        DdlQueueDisposition::RetryLater
    );
}

#[test]
fn source_uninitialized_disabled_queue_ignores_ddl_event() {
    assert_eq!(
        ddl_queue_disposition(false, false),
        DdlQueueDisposition::Ignore
    );
}

#[test]
fn source_initialized_queue_dispatches_even_when_auto_analyze_is_disabled() {
    assert_eq!(
        ddl_queue_disposition(true, false),
        DdlQueueDisposition::Dispatch
    );
    assert_eq!(
        ddl_queue_disposition(true, true),
        DdlQueueDisposition::Dispatch
    );
}
