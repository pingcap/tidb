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

//! Source-backed tests for statement-context ID generation.

use tidb_exec::context_id::gen_context_id;

#[test]
fn statement_context_ids_are_strictly_monotonic_and_nonzero() {
    // Source: pkg/util/context/context.go:37-42 and
    // pkg/sessionctx/stmtctx/stmtctx_test.go:505-522.
    let mut previous = gen_context_id();
    assert!(previous > 0);

    // TestStmtCtxID creates a fresh context, another context with a timezone,
    // then resets the original; each operation obtains a larger ID.
    for _ in 0..3 {
        let current = gen_context_id();
        assert!(current > previous);
        previous = current;
    }
}

#[test]
fn context_id_generation_is_unique_across_threads() {
    // Source: pkg/util/context/context.go:40-42. Atomic Add is the only
    // synchronization owned by this leaf; statement lifecycle is external.
    let handles = (0..8)
        .map(|_| std::thread::spawn(gen_context_id))
        .collect::<Vec<_>>();
    let mut ids = handles
        .into_iter()
        .map(|handle| handle.join().expect("context ID worker panicked"))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 8);
}
