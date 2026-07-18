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

//! Source-backed tests for system-variable type-kind flags.

use tidb_exec::sysvar_type::SysVarType;

#[test]
fn sysvar_type_preserves_source_discriminants() {
    // Source: pkg/sessionctx/vardef/tidb_vars.go:2115,2128-2143 and
    // pkg/sessionctx/variable/tests/variable_test.go:130-162
    // (TestRegistrationOfNewSysVar constructs a TypeBool SysVar).
    let types = [
        SysVarType::STR,
        SysVarType::BOOL,
        SysVarType::INT,
        SysVarType::ENUM,
        SysVarType::FLOAT,
        SysVarType::UNSIGNED,
        SysVarType::TIME,
        SysVarType::DURATION,
    ];
    assert_eq!(SysVarType::COUNT as usize, types.len());
    for (index, ty) in types.into_iter().enumerate() {
        assert_eq!(ty.bits(), index as u8);
    }
    assert_eq!(SysVarType::BOOL.bits(), 1);
}

#[test]
fn sysvar_type_keeps_byte_backed_unknown_values_visible() {
    // Source: pkg/sessionctx/vardef/tidb_vars.go:2115.
    let unknown = SysVarType::from_bits(255);
    assert_eq!(unknown.bits(), 255);
    assert_ne!(unknown, SysVarType::DURATION);
}
