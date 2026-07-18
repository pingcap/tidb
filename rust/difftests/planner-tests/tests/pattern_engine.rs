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

//! Dependency-closed vectors for `pkg/planner/cascades/pattern/engine.go`.
//!
//! The source test anchor is `TestEngineTypeSet` at
//! `pkg/planner/cascades/pattern/engine_test.go:23`.

use tidb_planner::pattern_engine::{EngineType, EngineTypeSet};

#[test]
fn engine_sets_preserve_source_membership() {
    let all = EngineTypeSet::ALL;
    assert!(all.contains(EngineType::TiDb));
    assert!(all.contains(EngineType::TiKv));
    assert!(all.contains(EngineType::TiFlash));

    assert!(EngineTypeSet::TIDB_ONLY.contains(EngineType::TiDb));
    assert!(!EngineTypeSet::TIDB_ONLY.contains(EngineType::TiKv));
    assert!(!EngineTypeSet::TIDB_ONLY.contains(EngineType::TiFlash));

    assert!(!EngineTypeSet::TIKV_ONLY.contains(EngineType::TiDb));
    assert!(EngineTypeSet::TIKV_ONLY.contains(EngineType::TiKv));
    assert!(!EngineTypeSet::TIKV_ONLY.contains(EngineType::TiFlash));

    assert!(!EngineTypeSet::TIFLASH_ONLY.contains(EngineType::TiDb));
    assert!(!EngineTypeSet::TIFLASH_ONLY.contains(EngineType::TiKv));
    assert!(EngineTypeSet::TIFLASH_ONLY.contains(EngineType::TiFlash));

    assert!(!EngineTypeSet::TIKV_OR_TIFLASH.contains(EngineType::TiDb));
    assert!(EngineTypeSet::TIKV_OR_TIFLASH.contains(EngineType::TiKv));
    assert!(EngineTypeSet::TIKV_OR_TIFLASH.contains(EngineType::TiFlash));
}

#[test]
fn engine_labels_and_raw_bits_are_stable() {
    assert_eq!(EngineType::TiDb.bits(), 1);
    assert_eq!(EngineType::TiKv.bits(), 2);
    assert_eq!(EngineType::TiFlash.bits(), 4);
    assert_eq!(EngineType::TiDb.as_str(), "EngineTiDB");
    assert_eq!(EngineType::TiKv.to_string(), "EngineTiKV");
    assert_eq!(EngineType::TiFlash.to_string(), "EngineTiFlash");
    assert_eq!(
        EngineTypeSet::from_bits(EngineType::TiDb.bits() | EngineType::TiFlash.bits()).bits(),
        5
    );
}
