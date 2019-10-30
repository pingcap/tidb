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

package kv

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
)

type testErrorSuite struct{}

var _ = Suite(testErrorSuite{})

func (s testErrorSuite) TestError(c *C) {
	c.Assert(ErrNotExist.ToSQLError().Code != mysql.ErrUnknown, IsTrue)
	c.Assert(ErrTxnRetryable.ToSQLError().Code != mysql.ErrUnknown, IsTrue)
	c.Assert(ErrCannotSetNilValue.ToSQLError().Code != mysql.ErrUnknown, IsTrue)
	c.Assert(ErrInvalidTxn.ToSQLError().Code != mysql.ErrUnknown, IsTrue)
	c.Assert(ErrTxnTooLarge.ToSQLError().Code != mysql.ErrUnknown, IsTrue)
	c.Assert(ErrEntryTooLarge.ToSQLError().Code != mysql.ErrUnknown, IsTrue)
	c.Assert(ErrKeyExists.ToSQLError().Code != mysql.ErrUnknown, IsTrue)
	c.Assert(ErrNotImplemented.ToSQLError().Code != mysql.ErrUnknown, IsTrue)
	c.Assert(ErrWriteConflict.ToSQLError().Code != mysql.ErrUnknown, IsTrue)
	c.Assert(ErrWriteConflictInTiDB.ToSQLError().Code != mysql.ErrUnknown, IsTrue)
}
