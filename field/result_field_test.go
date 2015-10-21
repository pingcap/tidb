// Copyright 2015 PingCAP, Inc.
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

package field_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testResultFieldSuite{})

type testResultFieldSuite struct {
}

func (*testResultFieldSuite) TestMain(c *C) {
	col := column.Col{
		ColumnInfo: model.ColumnInfo{
			FieldType: *types.NewFieldType(mysql.TypeLong),
			Name:      model.NewCIStr("c1"),
		},
	}
	col.Flag |= mysql.UnsignedFlag

	r := &field.ResultField{
		Col:          col,
		Name:         "c1",
		OrgTableName: "t1",
	}

	c.Assert(r.String(), Equals, "c1")
	r.TableName = "a"
	c.Assert(r.String(), Equals, "a.c1")
	r.DBName = "test"
	c.Assert(r.String(), Equals, "test.a.c1")

	cr := r.Clone()
	c.Assert(r.String(), Equals, cr.String())

	col1 := column.Col{
		ColumnInfo: model.ColumnInfo{
			FieldType: *types.NewFieldType(mysql.TypeLong),
			Name:      model.NewCIStr("c2"),
		},
	}
	col1.Flag |= mysql.UnsignedFlag

	r1 := &field.ResultField{
		Col:          col1,
		Name:         "c2",
		TableName:    "a",
		OrgTableName: "t1",
		DBName:       "test",
	}

	rs := []*field.ResultField{r, r1}
	ns := field.RFQNames(rs)
	c.Assert(ns, HasLen, 2)
	c.Assert(ns[0], Equals, "\"c1\"")
	c.Assert(ns[1], Equals, "\"c2\"")

	col2 := column.Col{
		ColumnInfo: model.ColumnInfo{
			FieldType: *types.NewFieldType(mysql.TypeVarchar),
			Name:      model.NewCIStr("c3"),
		},
	}
	col2.Flag |= mysql.UnsignedFlag
	col3 := column.Col{
		ColumnInfo: model.ColumnInfo{
			FieldType: *types.NewFieldType(mysql.TypeBlob),
			Name:      model.NewCIStr("c4"),
		},
	}
	col3.Flag |= mysql.UnsignedFlag

	cols := []*column.Col{&col, &col1, &col2, &col3}
	rs = field.ColsToResultFields(cols, "t")
	c.Assert(rs, HasLen, 4)
	c.Assert(rs[2].Tp, Equals, mysql.TypeVarString)
	c.Assert(rs[3].Tp, Equals, mysql.TypeBlob)

	col4 := column.Col{
		ColumnInfo: model.ColumnInfo{
			FieldType: *types.NewFieldType(mysql.TypeVarchar),
			Name:      model.NewCIStr("c2"),
		},
	}
	r2 := &field.ResultField{
		Col:          col4,
		Name:         "c22",
		TableName:    "b",
		OrgTableName: "t2",
		DBName:       "test",
	}
	rs = []*field.ResultField{r, r1, r2}

	// For CloneFieldByName
	_, err := field.CloneFieldByName("cx", rs)
	c.Assert(err, NotNil)
	_, err = field.CloneFieldByName("c2", rs)
	c.Assert(err, IsNil)

	// For ContainAllFieldNames
	names := []string{"cx", "c2"}
	b := field.ContainAllFieldNames(names, rs)
	c.Assert(b, IsFalse)
	names = []string{"c2", "c1"}
	b = field.ContainAllFieldNames(names, rs)
	c.Assert(b, IsTrue)
}

func (*testResultFieldSuite) TestCheckWildcard(c *C) {
	// For CheckWildcardField
	t, w, err := field.CheckWildcardField("a")
	c.Assert(err, IsNil)
	c.Assert(t, Equals, "")
	c.Assert(w, IsFalse)

	t, w, err = field.CheckWildcardField("a.*")
	c.Assert(err, IsNil)
	c.Assert(t, Equals, "a")
	c.Assert(w, IsTrue)
}

func (*testResultFieldSuite) TestCheckFieldEquals(c *C) {
	x := "a.b.c"
	y := "a.b.c"
	c.Assert(field.CheckFieldsEqual(x, y), IsTrue)

	x = "a.b.c"
	y = "a.B.c"
	c.Assert(field.CheckFieldsEqual(x, y), IsTrue)

	x = "b.c"
	y = "a.b.c"
	c.Assert(field.CheckFieldsEqual(x, y), IsTrue)

	x = "a..c"
	y = "a.b.c"
	c.Assert(field.CheckFieldsEqual(x, y), IsTrue)

	x = "a.a..c"
	y = "a.b.c"
	c.Assert(field.CheckFieldsEqual(x, y), IsTrue)

	x = "a.b.d"
	y = "a.b.c"
	c.Assert(field.CheckFieldsEqual(x, y), IsFalse)
	x = "a.d.c"
	y = "a.b.c"
	c.Assert(field.CheckFieldsEqual(x, y), IsFalse)
	x = "d.b.c"
	y = "a.b.c"
	c.Assert(field.CheckFieldsEqual(x, y), IsFalse)
}
