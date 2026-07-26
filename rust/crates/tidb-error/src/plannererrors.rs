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

//! `pkg/util/dbterror/plannererrors`: the ClassOptimizer error prototypes.
//!
//! Go declares these as package-level `var`s built via
//! `dbterror.ClassOptimizer.NewStd(code)`; here they are `LazyLock` statics
//! built via [`TerrorError::registered_std`], which resolves the message from
//! the MySQL or TiDB catalog. Generated from the Go source by script (the Go
//! errno name is kept in a comment); one entry (ERR_ACCESS_DENIED) uses
//! `NewStdErr` in Go -- code ErrAccessDenied with ErrAccessDeniedNoPassword's
//! message -- ported via `registered_standard` with that catalog message.

use std::sync::LazyLock;

use crate::terror::{TerrorClass, TerrorCode, TerrorError};

/// Go `plannererrors.ErrUnsupportedType` (`ClassOptimizer.NewStd(errno.ErrUnsupportedType)`).
pub static ERR_UNSUPPORTED_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8108)));

/// Go `plannererrors.ErrAnalyzeMissIndex` (`ClassOptimizer.NewStd(errno.ErrAnalyzeMissIndex)`).
pub static ERR_ANALYZE_MISS_INDEX: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8109)));

/// Go `plannererrors.ErrAnalyzeMissColumn` (`ClassOptimizer.NewStd(errno.ErrAnalyzeMissColumn)`).
pub static ERR_ANALYZE_MISS_COLUMN: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8137)));

/// Go `plannererrors.ErrWrongParamCount` (`ClassOptimizer.NewStd(errno.ErrWrongParamCount)`).
pub static ERR_WRONG_PARAM_COUNT: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8112)));

/// Go `plannererrors.ErrSchemaChanged` (`ClassOptimizer.NewStd(errno.ErrSchemaChanged)`).
pub static ERR_SCHEMA_CHANGED: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8113)));

/// Go `plannererrors.ErrTablenameNotAllowedHere` (`ClassOptimizer.NewStd(errno.ErrTablenameNotAllowedHere)`).
pub static ERR_TABLENAME_NOT_ALLOWED_HERE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1250)));

/// Go `plannererrors.ErrNotSupportedYet` (`ClassOptimizer.NewStd(errno.ErrNotSupportedYet)`).
pub static ERR_NOT_SUPPORTED_YET: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1235)));

/// Go `plannererrors.ErrWrongUsage` (`ClassOptimizer.NewStd(errno.ErrWrongUsage)`).
pub static ERR_WRONG_USAGE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1221)));

/// Go `plannererrors.ErrUnknown` (`ClassOptimizer.NewStd(errno.ErrUnknown)`).
pub static ERR_UNKNOWN: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1105)));

/// Go `plannererrors.ErrUnknownTable` (`ClassOptimizer.NewStd(errno.ErrUnknownTable)`).
pub static ERR_UNKNOWN_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1109)));

/// Go `plannererrors.ErrNoSuchTable` (`ClassOptimizer.NewStd(errno.ErrNoSuchTable)`).
pub static ERR_NO_SUCH_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1146)));

/// Go `plannererrors.ErrViewRecursive` (`ClassOptimizer.NewStd(errno.ErrViewRecursive)`).
pub static ERR_VIEW_RECURSIVE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1462)));

/// Go `plannererrors.ErrWrongArguments` (`ClassOptimizer.NewStd(errno.ErrWrongArguments)`).
pub static ERR_WRONG_ARGUMENTS: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1210)));

/// Go `plannererrors.ErrWrongNumberOfColumnsInSelect` (`ClassOptimizer.NewStd(errno.ErrWrongNumberOfColumnsInSelect)`).
pub static ERR_WRONG_NUMBER_OF_COLUMNS_IN_SELECT: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1222)));

/// Go `plannererrors.ErrBadGeneratedColumn` (`ClassOptimizer.NewStd(errno.ErrBadGeneratedColumn)`).
pub static ERR_BAD_GENERATED_COLUMN: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3105)));

/// Go `plannererrors.ErrFieldNotInGroupBy` (`ClassOptimizer.NewStd(errno.ErrFieldNotInGroupBy)`).
pub static ERR_FIELD_NOT_IN_GROUP_BY: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1055)));

/// Go `plannererrors.ErrAggregateOrderNonAggQuery` (`ClassOptimizer.NewStd(errno.ErrAggregateOrderNonAggQuery)`).
pub static ERR_AGGREGATE_ORDER_NON_AGG_QUERY: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3029)));

/// Go `plannererrors.ErrFieldInOrderNotSelect` (`ClassOptimizer.NewStd(errno.ErrFieldInOrderNotSelect)`).
pub static ERR_FIELD_IN_ORDER_NOT_SELECT: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3065)));

/// Go `plannererrors.ErrAggregateInOrderNotSelect` (`ClassOptimizer.NewStd(errno.ErrAggregateInOrderNotSelect)`).
pub static ERR_AGGREGATE_IN_ORDER_NOT_SELECT: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3066)));

/// Go `plannererrors.ErrBadTable` (`ClassOptimizer.NewStd(errno.ErrBadTable)`).
pub static ERR_BAD_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1051)));

/// Go `plannererrors.ErrKeyDoesNotExist` (`ClassOptimizer.NewStd(errno.ErrKeyDoesNotExist)`).
pub static ERR_KEY_DOES_NOT_EXIST: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1176)));

/// Go `plannererrors.ErrOperandColumns` (`ClassOptimizer.NewStd(errno.ErrOperandColumns)`).
pub static ERR_OPERAND_COLUMNS: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1241)));

/// Go `plannererrors.ErrInvalidGroupFuncUse` (`ClassOptimizer.NewStd(errno.ErrInvalidGroupFuncUse)`).
pub static ERR_INVALID_GROUP_FUNC_USE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1111)));

/// Go `plannererrors.ErrIllegalReference` (`ClassOptimizer.NewStd(errno.ErrIllegalReference)`).
pub static ERR_ILLEGAL_REFERENCE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1247)));

/// Go `plannererrors.ErrNoDB` (`ClassOptimizer.NewStd(errno.ErrNoDB)`).
pub static ERR_NO_DB: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1046)));

/// Go `plannererrors.ErrUnknownExplainFormat` (`ClassOptimizer.NewStd(errno.ErrUnknownExplainFormat)`).
pub static ERR_UNKNOWN_EXPLAIN_FORMAT: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1791)));

/// Go `plannererrors.ErrWrongGroupField` (`ClassOptimizer.NewStd(errno.ErrWrongGroupField)`).
pub static ERR_WRONG_GROUP_FIELD: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1056)));

/// Go `plannererrors.ErrDupFieldName` (`ClassOptimizer.NewStd(errno.ErrDupFieldName)`).
pub static ERR_DUP_FIELD_NAME: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1060)));

/// Go `plannererrors.ErrNonUpdatableTable` (`ClassOptimizer.NewStd(errno.ErrNonUpdatableTable)`).
pub static ERR_NON_UPDATABLE_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1288)));

/// Go `plannererrors.ErrMultiUpdateKeyConflict` (`ClassOptimizer.NewStd(errno.ErrMultiUpdateKeyConflict)`).
pub static ERR_MULTI_UPDATE_KEY_CONFLICT: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1706)));

/// Go `plannererrors.ErrInternal` (`ClassOptimizer.NewStd(errno.ErrInternal)`).
pub static ERR_INTERNAL: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1815)));

/// Go `plannererrors.ErrNonUniqTable` (`ClassOptimizer.NewStd(errno.ErrNonuniqTable)`).
pub static ERR_NON_UNIQ_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1066)));

/// Go `plannererrors.ErrWindowInvalidWindowFuncUse` (`ClassOptimizer.NewStd(errno.ErrWindowInvalidWindowFuncUse)`).
pub static ERR_WINDOW_INVALID_WINDOW_FUNC_USE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3593)));

/// Go `plannererrors.ErrWindowInvalidWindowFuncAliasUse` (`ClassOptimizer.NewStd(errno.ErrWindowInvalidWindowFuncAliasUse)`).
pub static ERR_WINDOW_INVALID_WINDOW_FUNC_ALIAS_USE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3594)));

/// Go `plannererrors.ErrWindowNoSuchWindow` (`ClassOptimizer.NewStd(errno.ErrWindowNoSuchWindow)`).
pub static ERR_WINDOW_NO_SUCH_WINDOW: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3579)));

/// Go `plannererrors.ErrWindowCircularityInWindowGraph` (`ClassOptimizer.NewStd(errno.ErrWindowCircularityInWindowGraph)`).
pub static ERR_WINDOW_CIRCULARITY_IN_WINDOW_GRAPH: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3580)));

/// Go `plannererrors.ErrWindowNoChildPartitioning` (`ClassOptimizer.NewStd(errno.ErrWindowNoChildPartitioning)`).
pub static ERR_WINDOW_NO_CHILD_PARTITIONING: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3581)));

/// Go `plannererrors.ErrWindowNoInherentFrame` (`ClassOptimizer.NewStd(errno.ErrWindowNoInherentFrame)`).
pub static ERR_WINDOW_NO_INHERENT_FRAME: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3582)));

/// Go `plannererrors.ErrWindowNoRedefineOrderBy` (`ClassOptimizer.NewStd(errno.ErrWindowNoRedefineOrderBy)`).
pub static ERR_WINDOW_NO_REDEFINE_ORDER_BY: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3583)));

/// Go `plannererrors.ErrWindowDuplicateName` (`ClassOptimizer.NewStd(errno.ErrWindowDuplicateName)`).
pub static ERR_WINDOW_DUPLICATE_NAME: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3591)));

/// Go `plannererrors.ErrPartitionClauseOnNonpartitioned` (`ClassOptimizer.NewStd(errno.ErrPartitionClauseOnNonpartitioned)`).
pub static ERR_PARTITION_CLAUSE_ON_NONPARTITIONED: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1747)));

/// Go `plannererrors.ErrWindowFrameStartIllegal` (`ClassOptimizer.NewStd(errno.ErrWindowFrameStartIllegal)`).
pub static ERR_WINDOW_FRAME_START_ILLEGAL: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3584)));

/// Go `plannererrors.ErrWindowFrameEndIllegal` (`ClassOptimizer.NewStd(errno.ErrWindowFrameEndIllegal)`).
pub static ERR_WINDOW_FRAME_END_ILLEGAL: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3585)));

/// Go `plannererrors.ErrWindowFrameIllegal` (`ClassOptimizer.NewStd(errno.ErrWindowFrameIllegal)`).
pub static ERR_WINDOW_FRAME_ILLEGAL: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3586)));

/// Go `plannererrors.ErrWindowRangeFrameOrderType` (`ClassOptimizer.NewStd(errno.ErrWindowRangeFrameOrderType)`).
pub static ERR_WINDOW_RANGE_FRAME_ORDER_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3587)));

/// Go `plannererrors.ErrWindowRangeFrameTemporalType` (`ClassOptimizer.NewStd(errno.ErrWindowRangeFrameTemporalType)`).
pub static ERR_WINDOW_RANGE_FRAME_TEMPORAL_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3588)));

/// Go `plannererrors.ErrWindowRangeFrameNumericType` (`ClassOptimizer.NewStd(errno.ErrWindowRangeFrameNumericType)`).
pub static ERR_WINDOW_RANGE_FRAME_NUMERIC_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3589)));

/// Go `plannererrors.ErrWindowRangeBoundNotConstant` (`ClassOptimizer.NewStd(errno.ErrWindowRangeBoundNotConstant)`).
pub static ERR_WINDOW_RANGE_BOUND_NOT_CONSTANT: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3590)));

/// Go `plannererrors.ErrWindowRowsIntervalUse` (`ClassOptimizer.NewStd(errno.ErrWindowRowsIntervalUse)`).
pub static ERR_WINDOW_ROWS_INTERVAL_USE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3596)));

/// Go `plannererrors.ErrWindowFunctionIgnoresFrame` (`ClassOptimizer.NewStd(errno.ErrWindowFunctionIgnoresFrame)`).
pub static ERR_WINDOW_FUNCTION_IGNORES_FRAME: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3599)));

/// Go `plannererrors.ErrInvalidNumberOfArgs` (`ClassOptimizer.NewStd(errno.ErrInvalidNumberOfArgs)`).
pub static ERR_INVALID_NUMBER_OF_ARGS: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3601)));

/// Go `plannererrors.ErrFieldInGroupingNotGroupBy` (`ClassOptimizer.NewStd(errno.ErrFieldInGroupingNotGroupBy)`).
pub static ERR_FIELD_IN_GROUPING_NOT_GROUP_BY: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3602)));

/// Go `plannererrors.ErrUnsupportedOnGeneratedColumn` (`ClassOptimizer.NewStd(errno.ErrUnsupportedOnGeneratedColumn)`).
pub static ERR_UNSUPPORTED_ON_GENERATED_COLUMN: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3106)));

/// Go `plannererrors.ErrPrivilegeCheckFail` (`ClassOptimizer.NewStd(errno.ErrPrivilegeCheckFail)`).
pub static ERR_PRIVILEGE_CHECK_FAIL: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8121)));

/// Go `plannererrors.ErrInvalidWildCard` (`ClassOptimizer.NewStd(errno.ErrInvalidWildCard)`).
pub static ERR_INVALID_WILD_CARD: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8122)));

/// Go `plannererrors.ErrMixOfGroupFuncAndFields` (`ClassOptimizer.NewStd(errno.ErrMixOfGroupFuncAndFieldsIncompatible)`).
pub static ERR_MIX_OF_GROUP_FUNC_AND_FIELDS: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8123)));

/// Go `plannererrors.ErrDBaccessDenied` (`ClassOptimizer.NewStd(errno.ErrDBaccessDenied)`).
pub static ERR_DBACCESS_DENIED: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1044)));

/// Go `plannererrors.ErrTableaccessDenied` (`ClassOptimizer.NewStd(errno.ErrTableaccessDenied)`).
pub static ERR_TABLEACCESS_DENIED: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1142)));

/// Go `plannererrors.ErrSpecificAccessDenied` (`ClassOptimizer.NewStd(errno.ErrSpecificAccessDenied)`).
pub static ERR_SPECIFIC_ACCESS_DENIED: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1227)));

/// Go `plannererrors.ErrViewNoExplain` (`ClassOptimizer.NewStd(errno.ErrViewNoExplain)`).
pub static ERR_VIEW_NO_EXPLAIN: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1345)));

/// Go `plannererrors.ErrWrongValueCountOnRow` (`ClassOptimizer.NewStd(errno.ErrWrongValueCountOnRow)`).
pub static ERR_WRONG_VALUE_COUNT_ON_ROW: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1136)));

/// Go `plannererrors.ErrViewInvalid` (`ClassOptimizer.NewStd(errno.ErrViewInvalid)`).
pub static ERR_VIEW_INVALID: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1356)));

/// Go `plannererrors.ErrNoSuchThread` (`ClassOptimizer.NewStd(errno.ErrNoSuchThread)`).
pub static ERR_NO_SUCH_THREAD: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1094)));

/// Go `plannererrors.ErrUnknownColumn` (`ClassOptimizer.NewStd(errno.ErrBadField)`).
pub static ERR_UNKNOWN_COLUMN: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1054)));

/// Go `plannererrors.ErrCartesianProductUnsupported` (`ClassOptimizer.NewStd(errno.ErrCartesianProductUnsupported)`).
pub static ERR_CARTESIAN_PRODUCT_UNSUPPORTED: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8110)));

/// Go `plannererrors.ErrStmtNotFound` (`ClassOptimizer.NewStd(errno.ErrPreparedStmtNotFound)`).
pub static ERR_STMT_NOT_FOUND: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8111)));

/// Go `plannererrors.ErrAmbiguous` (`ClassOptimizer.NewStd(errno.ErrNonUniq)`).
pub static ERR_AMBIGUOUS: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1052)));

/// Go `plannererrors.ErrUnresolvedHintName` (`ClassOptimizer.NewStd(errno.ErrUnresolvedHintName)`).
pub static ERR_UNRESOLVED_HINT_NAME: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3128)));

/// Go `plannererrors.ErrNotHintUpdatable` (`ClassOptimizer.NewStd(errno.ErrNotHintUpdatable)`).
pub static ERR_NOT_HINT_UPDATABLE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3637)));

/// Go `plannererrors.ErrWarnConflictingHint` (`ClassOptimizer.NewStd(errno.ErrWarnConflictingHint)`).
pub static ERR_WARN_CONFLICTING_HINT: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3126)));

/// Go `plannererrors.ErrCTERecursiveRequiresUnion` (`ClassOptimizer.NewStd(errno.ErrCTERecursiveRequiresUnion)`).
pub static ERR_CTERECURSIVE_REQUIRES_UNION: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3573)));

/// Go `plannererrors.ErrCTERecursiveRequiresNonRecursiveFirst` (`ClassOptimizer.NewStd(errno.ErrCTERecursiveRequiresNonRecursiveFirst)`).
pub static ERR_CTERECURSIVE_REQUIRES_NON_RECURSIVE_FIRST: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3574)));

/// Go `plannererrors.ErrCTERecursiveForbidsAggregation` (`ClassOptimizer.NewStd(errno.ErrCTERecursiveForbidsAggregation)`).
pub static ERR_CTERECURSIVE_FORBIDS_AGGREGATION: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3575)));

/// Go `plannererrors.ErrCTERecursiveForbiddenJoinOrder` (`ClassOptimizer.NewStd(errno.ErrCTERecursiveForbiddenJoinOrder)`).
pub static ERR_CTERECURSIVE_FORBIDDEN_JOIN_ORDER: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3576)));

/// Go `plannererrors.ErrInvalidLateralJoin` (`ClassOptimizer.NewStd(errno.ErrInvalidLateralJoin)`).
pub static ERR_INVALID_LATERAL_JOIN: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3809)));

/// Go `plannererrors.ErrInvalidRequiresSingleReference` (`ClassOptimizer.NewStd(errno.ErrInvalidRequiresSingleReference)`).
pub static ERR_INVALID_REQUIRES_SINGLE_REFERENCE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(3577)));

/// Go `plannererrors.ErrSQLInReadOnlyMode` (`ClassOptimizer.NewStd(errno.ErrReadOnlyMode)`).
pub static ERR_SQLIN_READ_ONLY_MODE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1836)));

/// Go `plannererrors.ErrDeleteNotFoundColumn` (`ClassOptimizer.NewStd(errno.ErrDeleteNotFoundColumn)`).
pub static ERR_DELETE_NOT_FOUND_COLUMN: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8177)));

/// Go `plannererrors.ErrBadNull` (`ClassOptimizer.NewStd(errno.ErrBadNull)`).
pub static ERR_BAD_NULL: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1048)));

/// Go `plannererrors.ErrNotSupportedWithSem` (`ClassOptimizer.NewStd(errno.ErrNotSupportedWithSem)`).
pub static ERR_NOT_SUPPORTED_WITH_SEM: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8132)));

/// Go `plannererrors.ErrAsOf` (`ClassOptimizer.NewStd(errno.ErrAsOf)`).
pub static ERR_AS_OF: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8135)));

/// Go `plannererrors.ErrOptOnTemporaryTable` (`ClassOptimizer.NewStd(errno.ErrOptOnTemporaryTable)`).
pub static ERR_OPT_ON_TEMPORARY_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8006)));

/// Go `plannererrors.ErrOptOnCacheTable` (`ClassOptimizer.NewStd(errno.ErrOptOnCacheTable)`).
pub static ERR_OPT_ON_CACHE_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8242)));

/// Go `plannererrors.ErrDropTableOnTemporaryTable` (`ClassOptimizer.NewStd(errno.ErrDropTableOnTemporaryTable)`).
pub static ERR_DROP_TABLE_ON_TEMPORARY_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8007)));

/// Go `plannererrors.ErrPartitionNoTemporary` (`ClassOptimizer.NewStd(errno.ErrPartitionNoTemporary)`).
pub static ERR_PARTITION_NO_TEMPORARY: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1562)));

/// Go `plannererrors.ErrViewSelectTemporaryTable` (`ClassOptimizer.NewStd(errno.ErrViewSelectTmptable)`).
pub static ERR_VIEW_SELECT_TEMPORARY_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1352)));

/// Go `plannererrors.ErrSubqueryMoreThan1Row` (`ClassOptimizer.NewStd(errno.ErrSubqueryNo1Row)`).
pub static ERR_SUBQUERY_MORE_THAN1_ROW: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1242)));

/// Go `plannererrors.ErrKeyPart0` (`ClassOptimizer.NewStd(errno.ErrKeyPart0)`).
pub static ERR_KEY_PART0: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1391)));

/// Go `plannererrors.ErrGettingNoopVariable` (`ClassOptimizer.NewStd(errno.ErrGettingNoopVariable)`).
pub static ERR_GETTING_NOOP_VARIABLE: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8145)));

/// Go `plannererrors.ErrRowIsReferenced2` (`ClassOptimizer.NewStd(errno.ErrRowIsReferenced2)`).
pub static ERR_ROW_IS_REFERENCED2: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1451)));

/// Go `plannererrors.ErrNoReferencedRow2` (`ClassOptimizer.NewStd(errno.ErrNoReferencedRow2)`).
pub static ERR_NO_REFERENCED_ROW2: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1452)));

/// Go `plannererrors.ErrSpDoesNotExist` (`ClassOptimizer.NewStd(errno.ErrSpDoesNotExist)`).
pub static ERR_SP_DOES_NOT_EXIST: LazyLock<TerrorError> =
    LazyLock::new(|| TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1305)));

/// Go `plannererrors.ErrAccessDenied` (`ClassOptimizer.NewStd(errno.ErrAccessDenied)`).
pub static ERR_ACCESS_DENIED: LazyLock<TerrorError> = LazyLock::new(|| {
    let message = crate::mysql::message_by_code(1698)
        .copied()
        .expect("catalog message");
    TerrorError::registered_standard(TerrorClass::Optimizer, TerrorCode::new(1045), message)
});

#[cfg(test)]
mod tests {
    use super::*;

    // Forcing every prototype proves each code resolves in the MySQL or
    // TiDB catalog (registered_std panics otherwise) -- no hidden gaps.
    #[test]
    fn all_prototypes_resolve() {
        let _ = ERR_UNSUPPORTED_TYPE.code();
        let _ = ERR_ANALYZE_MISS_INDEX.code();
        let _ = ERR_ANALYZE_MISS_COLUMN.code();
        let _ = ERR_WRONG_PARAM_COUNT.code();
        let _ = ERR_SCHEMA_CHANGED.code();
        let _ = ERR_TABLENAME_NOT_ALLOWED_HERE.code();
        let _ = ERR_NOT_SUPPORTED_YET.code();
        let _ = ERR_WRONG_USAGE.code();
        let _ = ERR_UNKNOWN.code();
        let _ = ERR_UNKNOWN_TABLE.code();
        let _ = ERR_NO_SUCH_TABLE.code();
        let _ = ERR_VIEW_RECURSIVE.code();
        let _ = ERR_WRONG_ARGUMENTS.code();
        let _ = ERR_WRONG_NUMBER_OF_COLUMNS_IN_SELECT.code();
        let _ = ERR_BAD_GENERATED_COLUMN.code();
        let _ = ERR_FIELD_NOT_IN_GROUP_BY.code();
        let _ = ERR_AGGREGATE_ORDER_NON_AGG_QUERY.code();
        let _ = ERR_FIELD_IN_ORDER_NOT_SELECT.code();
        let _ = ERR_AGGREGATE_IN_ORDER_NOT_SELECT.code();
        let _ = ERR_BAD_TABLE.code();
        let _ = ERR_KEY_DOES_NOT_EXIST.code();
        let _ = ERR_OPERAND_COLUMNS.code();
        let _ = ERR_INVALID_GROUP_FUNC_USE.code();
        let _ = ERR_ILLEGAL_REFERENCE.code();
        let _ = ERR_NO_DB.code();
        let _ = ERR_UNKNOWN_EXPLAIN_FORMAT.code();
        let _ = ERR_WRONG_GROUP_FIELD.code();
        let _ = ERR_DUP_FIELD_NAME.code();
        let _ = ERR_NON_UPDATABLE_TABLE.code();
        let _ = ERR_MULTI_UPDATE_KEY_CONFLICT.code();
        let _ = ERR_INTERNAL.code();
        let _ = ERR_NON_UNIQ_TABLE.code();
        let _ = ERR_WINDOW_INVALID_WINDOW_FUNC_USE.code();
        let _ = ERR_WINDOW_INVALID_WINDOW_FUNC_ALIAS_USE.code();
        let _ = ERR_WINDOW_NO_SUCH_WINDOW.code();
        let _ = ERR_WINDOW_CIRCULARITY_IN_WINDOW_GRAPH.code();
        let _ = ERR_WINDOW_NO_CHILD_PARTITIONING.code();
        let _ = ERR_WINDOW_NO_INHERENT_FRAME.code();
        let _ = ERR_WINDOW_NO_REDEFINE_ORDER_BY.code();
        let _ = ERR_WINDOW_DUPLICATE_NAME.code();
        let _ = ERR_PARTITION_CLAUSE_ON_NONPARTITIONED.code();
        let _ = ERR_WINDOW_FRAME_START_ILLEGAL.code();
        let _ = ERR_WINDOW_FRAME_END_ILLEGAL.code();
        let _ = ERR_WINDOW_FRAME_ILLEGAL.code();
        let _ = ERR_WINDOW_RANGE_FRAME_ORDER_TYPE.code();
        let _ = ERR_WINDOW_RANGE_FRAME_TEMPORAL_TYPE.code();
        let _ = ERR_WINDOW_RANGE_FRAME_NUMERIC_TYPE.code();
        let _ = ERR_WINDOW_RANGE_BOUND_NOT_CONSTANT.code();
        let _ = ERR_WINDOW_ROWS_INTERVAL_USE.code();
        let _ = ERR_WINDOW_FUNCTION_IGNORES_FRAME.code();
        let _ = ERR_INVALID_NUMBER_OF_ARGS.code();
        let _ = ERR_FIELD_IN_GROUPING_NOT_GROUP_BY.code();
        let _ = ERR_UNSUPPORTED_ON_GENERATED_COLUMN.code();
        let _ = ERR_PRIVILEGE_CHECK_FAIL.code();
        let _ = ERR_INVALID_WILD_CARD.code();
        let _ = ERR_MIX_OF_GROUP_FUNC_AND_FIELDS.code();
        let _ = ERR_DBACCESS_DENIED.code();
        let _ = ERR_TABLEACCESS_DENIED.code();
        let _ = ERR_SPECIFIC_ACCESS_DENIED.code();
        let _ = ERR_VIEW_NO_EXPLAIN.code();
        let _ = ERR_WRONG_VALUE_COUNT_ON_ROW.code();
        let _ = ERR_VIEW_INVALID.code();
        let _ = ERR_NO_SUCH_THREAD.code();
        let _ = ERR_UNKNOWN_COLUMN.code();
        let _ = ERR_CARTESIAN_PRODUCT_UNSUPPORTED.code();
        let _ = ERR_STMT_NOT_FOUND.code();
        let _ = ERR_AMBIGUOUS.code();
        let _ = ERR_UNRESOLVED_HINT_NAME.code();
        let _ = ERR_NOT_HINT_UPDATABLE.code();
        let _ = ERR_WARN_CONFLICTING_HINT.code();
        let _ = ERR_CTERECURSIVE_REQUIRES_UNION.code();
        let _ = ERR_CTERECURSIVE_REQUIRES_NON_RECURSIVE_FIRST.code();
        let _ = ERR_CTERECURSIVE_FORBIDS_AGGREGATION.code();
        let _ = ERR_CTERECURSIVE_FORBIDDEN_JOIN_ORDER.code();
        let _ = ERR_INVALID_LATERAL_JOIN.code();
        let _ = ERR_INVALID_REQUIRES_SINGLE_REFERENCE.code();
        let _ = ERR_SQLIN_READ_ONLY_MODE.code();
        let _ = ERR_DELETE_NOT_FOUND_COLUMN.code();
        let _ = ERR_BAD_NULL.code();
        let _ = ERR_NOT_SUPPORTED_WITH_SEM.code();
        let _ = ERR_AS_OF.code();
        let _ = ERR_OPT_ON_TEMPORARY_TABLE.code();
        let _ = ERR_OPT_ON_CACHE_TABLE.code();
        let _ = ERR_DROP_TABLE_ON_TEMPORARY_TABLE.code();
        let _ = ERR_PARTITION_NO_TEMPORARY.code();
        let _ = ERR_VIEW_SELECT_TEMPORARY_TABLE.code();
        let _ = ERR_SUBQUERY_MORE_THAN1_ROW.code();
        let _ = ERR_KEY_PART0.code();
        let _ = ERR_GETTING_NOOP_VARIABLE.code();
        let _ = ERR_ROW_IS_REFERENCED2.code();
        let _ = ERR_NO_REFERENCED_ROW2.code();
        let _ = ERR_SP_DOES_NOT_EXIST.code();
        let _ = ERR_ACCESS_DENIED.code();
        // Spot-check specific codes, incl. the NewStdErr special case.
        assert_eq!(ERR_UNSUPPORTED_TYPE.code().value(), 8108);
        assert_eq!(ERR_ACCESS_DENIED.code().value(), 1045);
    }
}
