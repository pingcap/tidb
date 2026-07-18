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

//! Source-backed tests for chunk-allocation usage state.

use tidb_exec::chunk_alloc_status::ChunkAllocationStatus;

#[test]
fn chunk_allocation_status_matches_source_set_clear_readback() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:934-947 and
    // pkg/sessionctx/variable/tests/session_test.go:678-716 (TestGetReuseChunk).
    let mut status = ChunkAllocationStatus::new();
    assert!(!status.is_used());
    status.clear();
    assert!(!status.is_used());
    status.set_used();
    assert!(status.is_used());
    status.set_used();
    assert!(status.is_used());
    status.clear();
    assert!(!status.is_used());
}
