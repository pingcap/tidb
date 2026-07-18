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

//! Source-backed tests for system-variable scope flags.

use tidb_exec::sysvar_scope::ScopeFlag;

#[test]
fn sysvar_scope_string_matches_source_order_and_zero_case() {
    // Source: pkg/sessionctx/vardef/tidb_vars.go:2112-2126,2204-2219 and
    // pkg/sessionctx/variable/tests/variable_test.go:497-510
    // (TestScopeToString).
    assert_eq!(ScopeFlag::GLOBAL.to_string(), "GLOBAL");
    assert_eq!(ScopeFlag::SESSION.to_string(), "SESSION");
    assert_eq!(ScopeFlag::INSTANCE.to_string(), "INSTANCE");
    assert_eq!(ScopeFlag::NONE.to_string(), "NONE");
    assert_eq!(
        (ScopeFlag::SESSION | ScopeFlag::GLOBAL).to_string(),
        "SESSION,GLOBAL"
    );
    assert_eq!(
        (ScopeFlag::SESSION | ScopeFlag::INSTANCE).to_string(),
        "SESSION,INSTANCE"
    );
}

#[test]
fn sysvar_scope_preserves_raw_bits_and_unknown_bit_rendering() {
    // Source: pkg/sessionctx/vardef/tidb_vars.go:2204-2219.
    let unknown = ScopeFlag::from_bits(1 << 3);
    assert_eq!(unknown.bits(), 1 << 3);
    assert_eq!(unknown.to_string(), "");

    let mut scope = ScopeFlag::SESSION;
    scope |= ScopeFlag::GLOBAL;
    assert_eq!(
        scope.bits(),
        ScopeFlag::SESSION.bits() | ScopeFlag::GLOBAL.bits()
    );
}
