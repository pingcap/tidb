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

//! Source-backed tests for variable-error code identities.

use tidb_exec::sysvar_error::SysVarErrorCode;

#[test]
fn sysvar_error_codes_match_source_variable_errors() {
    // Source: pkg/sessionctx/variable/error.go:23-52 and
    // pkg/sessionctx/variable/tests/variable_test.go:112-128 (TestError).
    let tested = [
        SysVarErrorCode::UNSUPPORTED_VALUE_FOR_VAR,
        SysVarErrorCode::UNKNOWN_SYSTEM_VAR,
        SysVarErrorCode::INCORRECT_SCOPE,
        SysVarErrorCode::UNKNOWN_TIME_ZONE,
        SysVarErrorCode::READ_ONLY,
        SysVarErrorCode::WRONG_VALUE_FOR_VAR,
        SysVarErrorCode::WRONG_TYPE_FOR_VAR,
        SysVarErrorCode::TRUNCATED_WRONG_VALUE,
        SysVarErrorCode::MAX_PREPARED_STMT_COUNT_REACHED,
        SysVarErrorCode::UNSUPPORTED_ISOLATION_LEVEL,
    ];
    assert!(tested.iter().all(|code| code.code() != 0));
    assert_eq!(SysVarErrorCode::UNSUPPORTED_VALUE_FOR_VAR.code(), 8047);
    assert_eq!(SysVarErrorCode::UNKNOWN_SYSTEM_VAR.code(), 1193);
    assert_eq!(SysVarErrorCode::UNSUPPORTED_ISOLATION_LEVEL.code(), 8048);
}

#[test]
fn sysvar_error_codes_preserve_shared_and_special_source_numbers() {
    // Source: pkg/sessionctx/variable/error.go:25-52.
    assert_eq!(SysVarErrorCode::WARN_DEPRECATED_SYNTAX.code(), 1287);
    assert_eq!(SysVarErrorCode::NOT_SUPPORTED_YET.code(), 1235);
    assert_eq!(SysVarErrorCode::NOT_VALID_PASSWORD.code(), 1819);
    assert_eq!(SysVarErrorCode::VARIABLE_NO_LONGER_SUPPORTED.code(), 8136);
    assert_eq!(
        SysVarErrorCode::INVALID_DEFAULT_UTF8MB4_COLLATION.code(),
        3721
    );
}
