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

//! Source-backed tests for status-provider registration and collection.

use std::collections::BTreeMap;

use tidb_exec::status_registry::{
    StatusProvider, StatusRegistry, StatusScope, StatusVal, StatusValue,
};

struct MockStatistics;

impl StatusProvider for MockStatistics {
    fn scope(&self, status: &str) -> StatusScope {
        if status == "test_session_status" {
            StatusScope::SESSION
        } else {
            StatusScope::DEFAULT
        }
    }

    fn stats(&self) -> Result<BTreeMap<String, StatusValue>, String> {
        Ok(BTreeMap::from([(
            "test_status".to_owned(),
            StatusValue::Text("test_status_val".to_owned()),
        )]))
    }
}

#[test]
fn status_registry_matches_source_provider_contract() {
    // Source: pkg/sessionctx/variable/statusvar.go:29-91 and
    // pkg/sessionctx/variable/statusvar_test.go:53-70.
    let mut registry = StatusRegistry::default();
    let registration = registry.register(MockStatistics);
    let mock = MockStatistics;

    assert_eq!(mock.scope("test_status"), StatusScope::DEFAULT);
    assert_eq!(mock.scope("test_session_status"), StatusScope::SESSION);

    let values = registry.collect().expect("status provider failed");
    assert_eq!(
        values.get("test_status"),
        Some(&StatusVal {
            scope: StatusScope::DEFAULT,
            value: StatusValue::Text("test_status_val".to_owned()),
        })
    );

    assert!(registry.unregister(registration));
    assert!(!registry.unregister(registration));
}

#[test]
fn status_scope_bits_preserve_global_session_default() {
    assert_eq!(StatusScope::GLOBAL.bits(), 0b01);
    assert_eq!(StatusScope::SESSION.bits(), 0b10);
    assert_eq!(
        (StatusScope::GLOBAL | StatusScope::SESSION).bits(),
        StatusScope::DEFAULT.bits()
    );
}
