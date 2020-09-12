// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
)

type testErrorSuite struct{}

var _ = Suite(testErrorSuite{})

func (s testErrorSuite) TestError(c *C) {
	kvErrs := []*terror.Error{
		ErrUnsupportedType,
		ErrAnalyzeMissIndex,
		ErrWrongParamCount,
		ErrSchemaChanged,
		ErrTablenameNotAllowedHere,
		ErrNotSupportedYet,
		ErrWrongUsage,
		ErrUnknownTable,
		ErrWrongArguments,
		ErrWrongNumberOfColumnsInSelect,
		ErrBadGeneratedColumn,
		ErrFieldNotInGroupBy,
		ErrBadTable,
		ErrKeyDoesNotExist,
		ErrOperandColumns,
		ErrInvalidGroupFuncUse,
		ErrIllegalReference,
		ErrNoDB,
		ErrUnknownExplainFormat,
		ErrWrongGroupField,
		ErrDupFieldName,
		ErrNonUpdatableTable,
		ErrInternal,
		ErrNonUniqTable,
		ErrWindowInvalidWindowFuncUse,
		ErrWindowInvalidWindowFuncAliasUse,
		ErrWindowNoSuchWindow,
		ErrWindowCircularityInWindowGraph,
		ErrWindowNoChildPartitioning,
		ErrWindowNoInherentFrame,
		ErrWindowNoRedefineOrderBy,
		ErrWindowDuplicateName,
		ErrPartitionClauseOnNonpartitioned,
		ErrWindowFrameStartIllegal,
		ErrWindowFrameEndIllegal,
		ErrWindowFrameIllegal,
		ErrWindowRangeFrameOrderType,
		ErrWindowRangeFrameTemporalType,
		ErrWindowRangeFrameNumericType,
		ErrWindowRangeBoundNotConstant,
		ErrWindowRowsIntervalUse,
		ErrWindowFunctionIgnoresFrame,
		ErrUnsupportedOnGeneratedColumn,
		ErrPrivilegeCheckFail,
		ErrInvalidWildCard,
		ErrMixOfGroupFuncAndFields,
		ErrDBaccessDenied,
		ErrTableaccessDenied,
		ErrSpecificAccessDenied,
		ErrViewNoExplain,
		ErrWrongValueCountOnRow,
		ErrViewInvalid,
		ErrNoSuchThread,
		ErrUnknownColumn,
		ErrCartesianProductUnsupported,
		ErrStmtNotFound,
		ErrAmbiguous,
	}
	for _, err := range kvErrs {
		code := err.ToSQLError().Code
		c.Assert(code != mysql.ErrUnknown && code == uint16(err.Code()), IsTrue, Commentf("err: %v", err))
	}
}
