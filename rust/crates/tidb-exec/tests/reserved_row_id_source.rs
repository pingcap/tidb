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

//! Source-shaped tests for `stmtctx.ReservedRowIDAlloc`.

use tidb_exec::reserved_row_id::ReservedRowIdAllocator;

#[test]
fn default_allocator_is_exhausted_and_returns_zero_equivalent_none() {
    // Source: pkg/sessionctx/stmtctx/stmtctx_test.go:566-572.
    let mut reserved = ReservedRowIdAllocator::default();
    assert!(reserved.is_exhausted());
    assert_eq!(reserved.consume(), None);
}

#[test]
fn reset_consumes_base_excluded_max_in_order() {
    // Source: pkg/sessionctx/stmtctx/stmtctx_test.go:573-589.
    let mut reserved = ReservedRowIdAllocator::default();
    reserved.reset(12, 15);
    assert!(!reserved.is_exhausted());
    assert_eq!(reserved.consume(), Some(13));
    assert_eq!(reserved.consume(), Some(14));
    assert_eq!(reserved.consume(), Some(15));
    assert!(reserved.is_exhausted());
    assert_eq!(reserved.consume(), None);
}

#[test]
fn reset_replaces_an_old_reservation_and_invalid_ranges_are_empty() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:134-153.
    let mut reserved = ReservedRowIdAllocator::default();
    reserved.reset(100, 101);
    assert_eq!(reserved.consume(), Some(101));
    reserved.reset(8, 8);
    assert!(reserved.is_exhausted());
    assert_eq!(reserved.consume(), None);
    reserved.reset(10, 5);
    assert!(reserved.is_exhausted());
}
