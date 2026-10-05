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

//! Behavioral tests retained from Go. Removed documentary entries are
//! indexed in rust/docs/parity/current-audit/comment-test-cleanup-validation.json.

use tidb_meta::transaction::{MemoryTransaction, Mutator};

/// Go `TestMeta` (`pkg/meta/meta_test.go:241`), starter-bootstrap slice:
/// after finishing bootstrap, `GetStarterBootstrapVersion` returns 0,
/// `FinishStarterBootstrap(1)` then reads back 1, and
/// `FinishStarterBootstrap(10)` reads back 10. Go stores the version as a raw
/// decimal string under `mStarterBootstrapKey = []byte("StarterBootstrapKey")`.
#[test]
fn meta_starter_bootstrap_round_trip() {
    let meta = Mutator::new(MemoryTransaction::default());
    assert_eq!(meta.starter_bootstrap_version().unwrap(), 0);
    meta.finish_starter_bootstrap(1).unwrap();
    assert_eq!(meta.starter_bootstrap_version().unwrap(), 1);
    meta.finish_starter_bootstrap(10).unwrap();
    assert_eq!(meta.starter_bootstrap_version().unwrap(), 10);
}
