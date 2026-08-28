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

//! Port ledger for `pkg/ddl/fail_test.go:27 TestFailBeforeDecodeArgs`
//! (`pkg/ddl.part6` batch b105, item 322 of the pkg/ddl enumeration).
//!
//! The Go test drives an `ADD COLUMN` through the worker state machine with
//! the `errorBeforeDecodeArgs` failpoint armed on its second
//! WriteReorganization visit, asserting the job retries the transient
//! failure (`tidb_ddl_error_count_limit`), that WriteOnly is visited exactly
//! once, and that the column still lands (`testCheckJobDone`). The worker
//! state machine and its failpoint seams are not transcreated.

/// GO PORT of `pkg/ddl/fail_test.go:27 TestFailBeforeDecodeArgs`.
///
/// Re-derived contract (fail_test.go:27-72): `testCreateColumn` adds column
/// `c3 int default 3` to `t1` while `beforeRunOneJobStep` counts
/// `StateWriteOnly` visits (exactly ONE -- the state appears once for this
/// action) and arms `errorBeforeDecodeArgs` on the first
/// `StateWriteReorganization` visit, disarms it on the next; the injected
/// error consumes one of `tidb_ddl_error_count_limit`'s retries and the job
/// still finishes with the column added and its default in place
/// (`testCheckJobDone(t, store, jobID, true)`).
#[test]
#[ignore = "go-parity-gap: the ADD COLUMN worker state machine, beforeRunOneJobStep/errorBeforeDecodeArgs failpoints and the job retry counter are not transcreated"]
fn fail_before_decode_args_recovers_through_the_job_retry_limit() {}
