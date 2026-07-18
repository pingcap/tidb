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

//! Source-backed tests for the session variable mock accessor.

use tidb_exec::mock_global_accessor::{
    MockGlobalAccessor, MockGlobalAccessorError, DEFAULT_AUTH_PLUGIN, TIKV_GC_LIFE_TIME,
};

#[test]
fn mock_api_matches_source_contract() {
    // Source: pkg/sessionctx/variable/mock_globalaccessor.go:23-126 and
    // pkg/sessionctx/variable/mock_globalaccessor_test.go:26-56.
    let mut mock = MockGlobalAccessor::for_tests();

    let value = mock.get_global_sys_var("illegalopt");
    assert_eq!(
        value,
        Err(MockGlobalAccessorError::UnknownSystemVariable(
            "illegalopt".to_owned()
        ))
    );

    assert!(mock.set_global_sys_var("illegalopt", "val").is_err());
    assert!(mock.set_global_sys_var_only("illegalopt", "val").is_err());

    assert!(mock
        .set_global_sys_var(DEFAULT_AUTH_PLUGIN, "invalidvalue")
        .is_err());
    mock.set_global_sys_var(DEFAULT_AUTH_PLUGIN, "mysql_native_password")
        .expect("valid authentication plugin");
    mock.set_global_sys_var_only(DEFAULT_AUTH_PLUGIN, "mysql_native_password")
        .expect("known authentication plugin");

    assert_eq!(
        mock.get_tidb_table_value(TIKV_GC_LIFE_TIME)
            .expect("GC lifetime table value"),
        "10m0s"
    );
}

#[test]
fn ordinary_accessor_preserves_unknown_empty_value() {
    let mock = MockGlobalAccessor::new();
    assert!(!mock.is_test_suite());
    assert_eq!(mock.get_global_sys_var("illegalopt"), Ok(String::new()));
    assert_eq!(
        mock.get_global_sys_var(DEFAULT_AUTH_PLUGIN),
        Ok("mysql_native_password".to_owned())
    );
}
