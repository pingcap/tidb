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
	mysql "github.com/pingcap/tidb/errno"
)

var _ = Suite(&testTableSuite{})

type testTableSuite struct{}

func (t *testTableSuite) TestErrorCode(c *C) {
	c.Assert(int(ErrColumnCantNull.ToSQLError().Code), Equals, mysql.ErrBadNull)
	c.Assert(int(ErrUnknownColumn.ToSQLError().Code), Equals, mysql.ErrBadField)
	c.Assert(int(errDuplicateColumn.ToSQLError().Code), Equals, mysql.ErrFieldSpecifiedTwice)
	c.Assert(int(errGetDefaultFailed.ToSQLError().Code), Equals, mysql.ErrFieldGetDefaultFailed)
	c.Assert(int(ErrNoDefaultValue.ToSQLError().Code), Equals, mysql.ErrNoDefaultForField)
	c.Assert(int(ErrIndexOutBound.ToSQLError().Code), Equals, mysql.ErrIndexOutBound)
	c.Assert(int(ErrUnsupportedOp.ToSQLError().Code), Equals, mysql.ErrUnsupportedOp)
	c.Assert(int(ErrRowNotFound.ToSQLError().Code), Equals, mysql.ErrRowNotFound)
	c.Assert(int(ErrTableStateCantNone.ToSQLError().Code), Equals, mysql.ErrTableStateCantNone)
	c.Assert(int(ErrColumnStateCantNone.ToSQLError().Code), Equals, mysql.ErrColumnStateCantNone)
	c.Assert(int(ErrColumnStateNonPublic.ToSQLError().Code), Equals, mysql.ErrColumnStateNonPublic)
	c.Assert(int(ErrIndexStateCantNone.ToSQLError().Code), Equals, mysql.ErrIndexStateCantNone)
	c.Assert(int(ErrInvalidRecordKey.ToSQLError().Code), Equals, mysql.ErrInvalidRecordKey)
	c.Assert(int(ErrTruncatedWrongValueForField.ToSQLError().Code), Equals, mysql.ErrTruncatedWrongValueForField)
	c.Assert(int(ErrUnknownPartition.ToSQLError().Code), Equals, mysql.ErrUnknownPartition)
	c.Assert(int(ErrNoPartitionForGivenValue.ToSQLError().Code), Equals, mysql.ErrNoPartitionForGivenValue)
	c.Assert(int(ErrLockOrActiveTransaction.ToSQLError().Code), Equals, mysql.ErrLockOrActiveTransaction)
}
