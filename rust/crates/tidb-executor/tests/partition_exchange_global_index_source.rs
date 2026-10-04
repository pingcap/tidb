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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use tidb_model::index::{
    GLOBAL_INDEX_VERSION_LEGACY, GLOBAL_INDEX_VERSION_V1, GLOBAL_INDEX_VERSION_V2,
};

// --- TestGlobalIndexVersionConstants
//     (pkg/ddl/tests/partition/global_index_version_test.go:266) ---
//
// The three version constants: LEGACY=0, V1=1, V2=2
// (`pkg/meta/model/index.go:51-68`).
#[test]
fn global_index_version_constants() {
    assert_eq!(GLOBAL_INDEX_VERSION_LEGACY, 0);
    assert_eq!(GLOBAL_INDEX_VERSION_V1, 1);
    assert_eq!(GLOBAL_INDEX_VERSION_V2, 2);
}
