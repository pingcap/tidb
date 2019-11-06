// Copyright 2017 PingCAP, Inc.
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

package table

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
)

var _ = Suite(&testTableSuite{})

type testTableSuite struct{}

func (t *testTableSuite) TestSlice(c *C) {
	sl := make(Slice, 2)
	length := sl.Len()
	c.Assert(length, Equals, 2)
	sl.Swap(0, 1)
}

func (t *testTableSuite) TestErrorCode(c *C) {
	c.Assert(ErrColumnCantNull.ToSQLError().Code, Equals, mysql.ErrBadNull)
	c.Assert(errUnknownColumn.ToSQLError().Code, Equals, mysql.ErrBadField)
	c.Assert(errDuplicateColumn.ToSQLError().Code, Equals, mysql.ErrFieldSpecifiedTwice)
	c.Assert(errGetDefaultFailed.ToSQLError().Code, Equals, mysql.ErrFieldGetDefaultFailed)
	c.Assert(ErrNoDefaultValue.ToSQLError().Code, Equals, mysql.ErrNoDefaultForField)
	c.Assert(ErrIndexOutBound.ToSQLError().Code, Equals, mysql.ErrIndexOutBound)
	c.Assert(ErrUnsupportedOp.ToSQLError().Code, Equals, mysql.ErrUnsupportedOp)
	c.Assert(ErrRowNotFound.ToSQLError().Code, Equals, mysql.ErrRowNotFound)
	c.Assert(ErrTableStateCantNone.ToSQLError().Code, Equals, mysql.ErrTableStateCantNone)
	c.Assert(ErrColumnStateCantNone.ToSQLError().Code, Equals, mysql.ErrColumnStateCantNone)
	c.Assert(ErrColumnStateNonPublic.ToSQLError().Code, Equals, mysql.ErrColumnStateNonPublic)
	c.Assert(ErrIndexStateCantNone.ToSQLError().Code, Equals, mysql.ErrIndexStateCantNone)
	c.Assert(ErrInvalidRecordKey.ToSQLError().Code, Equals, mysql.ErrInvalidRecordKey)
	c.Assert(ErrTruncateWrongValue.ToSQLError().Code, Equals, mysql.ErrTruncatedWrongValueForField)
	c.Assert(ErrTruncatedWrongValueForField.ToSQLError().Code, Equals, mysql.ErrTruncatedWrongValueForField)
	c.Assert(ErrUnknownPartition.ToSQLError().Code, Equals, mysql.ErrUnknownPartition)
	c.Assert(ErrNoPartitionForGivenValue.ToSQLError().Code, Equals, mysql.ErrNoPartitionForGivenValue)
	c.Assert(ErrLockOrActiveTransaction.ToSQLError().Code, Equals, mysql.ErrLockOrActiveTransaction)
}
