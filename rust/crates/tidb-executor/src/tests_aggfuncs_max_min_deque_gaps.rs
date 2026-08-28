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

//! Gap tests for Go `pkg/executor/aggfuncs/func_max_min_test.go`. MAX/MIN
//! per-type states and the sliding-window monotonic deque are transcreated
//! in the sibling crate `tidb-exec` (`aggregate/runtime/max_min.rs`,
//! `minmax_deque.rs`), which this crate does not depend on.

/// Go `pkg/executor/aggfuncs/func_max_min_test.go:97::TestMergePartialResult4MaxMin`:
/// over rows 0..4 both MAX and MIN are idempotent under merge -- MAX is 4 in
/// every region (including unsigned longlong), MIN is 0 (2 in rows 2..5) --
/// across longlong/float/double/decimal/string/date/duration/JSON; enum MAX
/// orders by element position ("e" over "c") while MIN picks "a"; set MAX
/// picks the numerically largest membership ("e,d"=3) and MIN the smallest
/// ("c"=4 vs "a") per `func_max_min.go`.
#[test]
#[ignore = "go-parity-gap: MAX/MIN partial/merge state lives in tidb-exec::aggregate::runtime::max_min (sibling crate); Go enum/set ordering fixtures have no equivalent here"]
fn merge_partial_result_4_max_min_is_idempotent_across_types() {}

/// Go `pkg/executor/aggfuncs/func_max_min_test.go:140::TestMaxMin`: streaming
/// MAX ends at 4 and MIN at 0 over rows 0..4 (unsigned longlong included),
/// NULL-only inputs stay NULL, for every arg type except enum/set, which
/// the streaming sweep omits.
#[test]
#[ignore = "go-parity-gap: MAX/MIN update-over-chunks lives in tidb-exec::aggregate::runtime::max_min (sibling crate); aggTest runner not modeled"]
fn max_min_streaming_sweep_ends_at_four_and_zero() {}

/// Go `pkg/executor/aggfuncs/func_max_min_test.go:171::TestMemMaxMin`: each
/// MAX/MIN type state charges its `DefPartialResult4MaxMin*Size` constant
/// plus per-row payload deltas (strings/JSON/enum/set add value lengths
/// via the max/min mem-delta generators, 99-row sweeps).
#[test]
#[ignore = "go-parity-gap: Go's memory-tracker harness and Def*Size constants are not modeled; state lives in tidb-exec (sibling crate)"]
fn mem_max_min_tracks_type_specific_sizes() {}

/// Go `pkg/executor/aggfuncs/func_max_min_test.go:258::TestMaxSlidingWindow`
/// (54 subtests: 9 row types x {no ORDER BY, ORDER BY} x {ROWS, RANGE,
/// default frame}): `max(a) OVER (... UNBOUNDED PRECEDING AND UNBOUNDED
/// FOLLOWING)` returns the partition max for bigint/int unsigned/float/
/// double/decimal(5,2)/text/time/date/datetime inputs -- with ORDER BY the
/// default frame becomes RANGE UNBOUNDED PRECEDING TO CURRENT ROW and the
/// result is the running max (ties included), rendered per the type's
/// text format (3.30, 03:00:00, 2022-09-10 00:00:00, ...).
#[test]
#[ignore = "go-parity-gap: needs full SQL window-frame execution over stored tables; the deque backing it lives in tidb-exec::minmax_deque (sibling crate)"]
fn max_sliding_window_covers_frame_and_order_matrix() {}

/// Go `pkg/executor/aggfuncs/func_max_min_test.go:335::TestDequeReset`:
/// `NewDeque(true, cmp)` starts empty with `IsMax` set; after one
/// `PushBack(0, 12)` a `Reset` empties the items while keeping the
/// max-orientation flag (`func_max_min.go:30 NewDeque`).
#[test]
#[ignore = "go-parity-gap: the monotonic deque is transcreated as tidb-exec::minmax_deque::MinMaxDeque (sibling crate, no dependency edge)"]
fn deque_reset_empties_items_keeps_max_flag() {}

/// Go `pkg/executor/aggfuncs/func_max_min_test.go:345::TestDequePushPop`: with
/// an empty deque the exposed `Front` pair is the zero-value sentinel
/// (`Item == 0`, `Idx == 0`, `isEnd == false`); 15 ordered
/// `PushBack(idx=i, item=i)` calls keep `Back` == `(i, i)` after each push;
/// the pop phase then reads `Back` LIFO (`(times-i-1, times-i-1)`), still
/// observes the zero-valued front sentinel, and `PopBack` returns no error
/// on every one of the 15 pops.
#[test]
#[ignore = "go-parity-gap: same tidb-exec::minmax_deque boundary (sibling crate); Go's any-typed cmp closure maps to generics there"]
fn deque_push_pop_round_trips_from_the_back() {}
