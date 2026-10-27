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

//! The `auto_random_base` overflow arm of Go `TestAutoRandomID`
//! (`pkg/executor/insert_test.go:340-344`).

use crate::support::testkit::TestKit;

/// Rebasing above the incremental bits fails: `1 << (64-5)` overflows the
/// 48 incremental bits `auto_random(15)` leaves (`1<<(64-16)-1`), with Go's
/// `autoid.AutoRandomRebaseOverflow` text (`pkg/meta/autoid/errors.go:61`).
#[test]
fn alter_auto_random_base_past_the_incremental_bits_overflows() {
    let mut tk = TestKit::new();
    tk.must_exec("create table ar (id bigint key clustered auto_random(15), name char(10))");
    let overflow = 1i64 << (64 - 5);
    let err_msg = format!(
        "alter auto_random_base to {overflow} overflows the incremental bits, max allowed base is {}",
        (1i64 << (64 - 16)) - 1
    );
    tk.must_contain_err_msg(&format!("alter table ar auto_random_base = {overflow}"), &err_msg);
}
