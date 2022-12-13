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
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
)

var _ = Suite(&testTableSuite{})

type testTableSuite struct{}

func (t *testTableSuite) TestErrorCode(c *C) {
	c.Assert(int(terror.ToSQLError(ErrColumnCantNull).Code), Equals, mysql.ErrBadNull)
	c.Assert(int(terror.ToSQLError(ErrUnknownColumn).Code), Equals, mysql.ErrBadField)
	c.Assert(int(terror.ToSQLError(errDuplicateColumn).Code), Equals, mysql.ErrFieldSpecifiedTwice)
	c.Assert(int(terror.ToSQLError(errGetDefaultFailed).Code), Equals, mysql.ErrFieldGetDefaultFailed)
	c.Assert(int(terror.ToSQLError(ErrNoDefaultValue).Code), Equals, mysql.ErrNoDefaultForField)
	c.Assert(int(terror.ToSQLError(ErrIndexOutBound).Code), Equals, mysql.ErrIndexOutBound)
	c.Assert(int(terror.ToSQLError(ErrUnsupportedOp).Code), Equals, mysql.ErrUnsupportedOp)
	c.Assert(int(terror.ToSQLError(ErrRowNotFound).Code), Equals, mysql.ErrRowNotFound)
	c.Assert(int(terror.ToSQLError(ErrTableStateCantNone).Code), Equals, mysql.ErrTableStateCantNone)
	c.Assert(int(terror.ToSQLError(ErrColumnStateCantNone).Code), Equals, mysql.ErrColumnStateCantNone)
	c.Assert(int(terror.ToSQLError(ErrColumnStateNonPublic).Code), Equals, mysql.ErrColumnStateNonPublic)
	c.Assert(int(terror.ToSQLError(ErrIndexStateCantNone).Code), Equals, mysql.ErrIndexStateCantNone)
	c.Assert(int(terror.ToSQLError(ErrInvalidRecordKey).Code), Equals, mysql.ErrInvalidRecordKey)
	c.Assert(int(terror.ToSQLError(ErrTruncatedWrongValueForField).Code), Equals, mysql.ErrTruncatedWrongValueForField)
	c.Assert(int(terror.ToSQLError(ErrUnknownPartition).Code), Equals, mysql.ErrUnknownPartition)
	c.Assert(int(terror.ToSQLError(ErrNoPartitionForGivenValue).Code), Equals, mysql.ErrNoPartitionForGivenValue)
	c.Assert(int(terror.ToSQLError(ErrLockOrActiveTransaction).Code), Equals, mysql.ErrLockOrActiveTransaction)
}
