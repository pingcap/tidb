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

//! Ports of Go unit tests from `pkg/util/dbterror` (read from
//! `origin/master`) that exercise this crate's `dbterror` transcreation.
//!
//! - `terror_test.go::TestErrorRedact` is ported as
//!   [`crate::dbterror::tests::error_redact`] next to the implementation.
//! - `plannererrors/errors_test.go::TestError` is ported below. The Go test
//!   iterates every `ClassOptimizer.NewStd(...)` prototype and requires
//!   `terror.ToSQLError(err).Code != mysql.ErrUnknown && code == err.Code()`
//!   — i.e. every prototype resolves to its own registered protocol code,
//!   never the ErrUnknown (1105) fallback.

use tidb_error::mysql::errcode;
use tidb_error::plannererrors::*;
use tidb_error::terror::TerrorError;
use std::sync::LazyLock;

/// Go `pkg/util/dbterror/plannererrors/errors_test.go::TestError`.
#[test]
fn error() {
    let kv_errs: &[&LazyLock<TerrorError>] = &[
        &ERR_UNSUPPORTED_TYPE,
        &ERR_ANALYZE_MISS_INDEX,
        &ERR_ANALYZE_MISS_COLUMN,
        &ERR_WRONG_PARAM_COUNT,
        &ERR_SCHEMA_CHANGED,
        &ERR_TABLENAME_NOT_ALLOWED_HERE,
        &ERR_NOT_SUPPORTED_YET,
        &ERR_WRONG_USAGE,
        &ERR_UNKNOWN_TABLE,
        &ERR_WRONG_ARGUMENTS,
        &ERR_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
        &ERR_BAD_GENERATED_COLUMN,
        &ERR_FIELD_NOT_IN_GROUP_BY,
        &ERR_BAD_TABLE,
        &ERR_KEY_DOES_NOT_EXIST,
        &ERR_OPERAND_COLUMNS,
        &ERR_INVALID_GROUP_FUNC_USE,
        &ERR_ILLEGAL_REFERENCE,
        &ERR_NO_DB,
        &ERR_UNKNOWN_EXPLAIN_FORMAT,
        &ERR_WRONG_GROUP_FIELD,
        &ERR_DUP_FIELD_NAME,
        &ERR_NON_UPDATABLE_TABLE,
        &ERR_INTERNAL,
        &ERR_NON_UNIQ_TABLE,
        &ERR_WINDOW_INVALID_WINDOW_FUNC_USE,
        &ERR_WINDOW_INVALID_WINDOW_FUNC_ALIAS_USE,
        &ERR_WINDOW_NO_SUCH_WINDOW,
        &ERR_WINDOW_CIRCULARITY_IN_WINDOW_GRAPH,
        &ERR_WINDOW_NO_CHILD_PARTITIONING,
        &ERR_WINDOW_NO_INHERENT_FRAME,
        &ERR_WINDOW_NO_REDEFINE_ORDER_BY,
        &ERR_WINDOW_DUPLICATE_NAME,
        &ERR_PARTITION_CLAUSE_ON_NONPARTITIONED,
        &ERR_WINDOW_FRAME_START_ILLEGAL,
        &ERR_WINDOW_FRAME_END_ILLEGAL,
        &ERR_WINDOW_FRAME_ILLEGAL,
        &ERR_WINDOW_RANGE_FRAME_ORDER_TYPE,
        &ERR_WINDOW_RANGE_FRAME_TEMPORAL_TYPE,
        &ERR_WINDOW_RANGE_FRAME_NUMERIC_TYPE,
        &ERR_WINDOW_RANGE_BOUND_NOT_CONSTANT,
        &ERR_WINDOW_ROWS_INTERVAL_USE,
        &ERR_WINDOW_FUNCTION_IGNORES_FRAME,
        &ERR_UNSUPPORTED_ON_GENERATED_COLUMN,
        &ERR_PRIVILEGE_CHECK_FAIL,
        &ERR_INVALID_WILD_CARD,
        &ERR_MIX_OF_GROUP_FUNC_AND_FIELDS,
        &ERR_DBACCESS_DENIED,
        &ERR_TABLEACCESS_DENIED,
        &ERR_SPECIFIC_ACCESS_DENIED,
        &ERR_VIEW_NO_EXPLAIN,
        &ERR_WRONG_VALUE_COUNT_ON_ROW,
        &ERR_VIEW_INVALID,
        &ERR_NO_SUCH_THREAD,
        &ERR_UNKNOWN_COLUMN,
        &ERR_CARTESIAN_PRODUCT_UNSUPPORTED,
        &ERR_STMT_NOT_FOUND,
        &ERR_AMBIGUOUS,
        &ERR_KEY_PART0,
    ];
    for err in kv_errs {
        let code = err.to_sql_error().code;
        assert_ne!(
            code,
            errcode::ErrUnknown,
            "{} resolves to the ErrUnknown fallback",
            err.rfc_code()
        );
        assert_eq!(
            isize::try_from(code).unwrap(),
            err.code().value(),
            "{} protocol code must equal its terror code",
            err.rfc_code()
        );
    }
}
