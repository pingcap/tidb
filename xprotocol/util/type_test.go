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

package util

import (
	"errors"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tipb/go-mysqlx/Resultset"
)

type testUtilTestSuite struct{}

var _ = Suite(&testUtilTestSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (ts *testUtilTestSuite) TestMysqlType2XType(c *C) {
	c.Parallel()
	tps := []byte{mysql.TypeDecimal, mysql.TypeLong, 0xf0}

	urs := []struct {
		xtp Mysqlx_Resultset.ColumnMetaData_FieldType
		err error
	}{
		{Mysqlx_Resultset.ColumnMetaData_DECIMAL, nil},
		{Mysqlx_Resultset.ColumnMetaData_UINT, nil},
		{Mysqlx_Resultset.ColumnMetaData_SINT, errors.New("unknown column type 240")},
	}
	for i, v := range tps {
		xtp, err := MysqlType2XType(v, true)
		c.Assert(urs[i].xtp == xtp, IsTrue, Commentf("%d", i))
		c.Assert(terror.ErrorEqual(urs[i].err, err), IsTrue, Commentf("err %v", err))
	}

	srs := []struct {
		xtp Mysqlx_Resultset.ColumnMetaData_FieldType
		err error
	}{
		{Mysqlx_Resultset.ColumnMetaData_DECIMAL, nil},
		{Mysqlx_Resultset.ColumnMetaData_SINT, nil},
		{Mysqlx_Resultset.ColumnMetaData_SINT, errors.New("unknown column type 240")},
	}
	for i, v := range tps {
		xtp, err := MysqlType2XType(v, false)
		c.Assert(srs[i].xtp == xtp, IsTrue, Commentf("%d", i))
		c.Assert(terror.ErrorEqual(srs[i].err, err), IsTrue, Commentf("err %v", err))
	}
}

func (ts *testUtilTestSuite) TestQuote(c *C) {
	c.Parallel()
	in := []string{"", "a&", "a"}
	out := []string{"``", "`a&`", "a"}

	for i, v := range in {
		c.Assert(out[i], Equals, QuoteIdentifierIfNeeded(v), Commentf("%d", i))
	}

	c.Assert("'a'", Equals, QuoteString("a"))
}
