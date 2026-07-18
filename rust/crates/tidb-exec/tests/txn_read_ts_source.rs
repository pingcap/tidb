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

//! Source-shaped tests for the `tx_read_ts` value boundary.

use tidb_exec::txn_read_ts::TxnReadTs;

#[test]
fn use_marks_timestamp_consumed_without_changing_value() {
    // Source: pkg/sessionctx/variable/session.go:3561-3567.
    let mut value = TxnReadTs::new(42);
    assert_eq!(value.peek(), 42);
    assert!(!value.is_used());
    assert_eq!(value.use_read_ts(), 42);
    assert_eq!(value.peek(), 42);
    assert!(value.is_used());
}

#[test]
fn setting_a_new_timestamp_refreshes_consumption() {
    // Source: pkg/sessionctx/variable/session.go:3570-3576.
    let mut value = TxnReadTs::new(42);
    assert_eq!(value.use_read_ts(), 42);
    value.set_read_ts(84);
    assert_eq!(value.peek(), 84);
    assert!(!value.is_used());
    assert_eq!(value.use_read_ts(), 84);
}

#[test]
fn cleanup_resets_only_a_used_nonzero_timestamp() {
    // Source: pkg/sessionctx/variable/session.go:3587-3595.
    let mut value = TxnReadTs::new(42);
    assert!(!value.cleanup_if_used());
    assert_eq!(value.peek(), 42);
    value.use_read_ts();
    assert!(value.cleanup_if_used());
    assert_eq!(value, TxnReadTs::default());

    // Go records `used` even for zero, but its cleanup condition also checks
    // `readTS > 0`; preserve that edge exactly.
    let mut zero = TxnReadTs::default();
    zero.use_read_ts();
    assert!(zero.is_used());
    assert!(!zero.cleanup_if_used());
    assert!(zero.is_used());
    assert_eq!(zero.peek(), 0);
}

#[test]
fn peek_is_non_consuming_and_default_is_zero() {
    // Source: pkg/sessionctx/variable/session.go:3554-3559, 3579-3584.
    let value = TxnReadTs::default();
    assert_eq!(value.peek(), 0);
    assert!(!value.is_used());
    assert_eq!(value.peek(), 0);
    assert!(!value.is_used());
}
