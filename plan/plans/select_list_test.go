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

package plans_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/plans"
)

type testSelectListSuite struct{}

var _ = Suite(&testSelectListSuite{})

func (s *testSelectListSuite) createTestResultFields(colNames []string) []*field.ResultField {
	fs := make([]*field.ResultField, 0, len(colNames))

	for i := 0; i < len(colNames); i++ {
		f := &field.ResultField{
			TableName: "t",
			Name:      colNames[i],
		}

		f.ColumnInfo.Name = model.NewCIStr(colNames[i])

		fs = append(fs, f)
	}

	return fs
}

func (s *testSelectListSuite) TestAmbiguous(c *C) {
	type pair struct {
		Name   string
		AsName string
	}

	createResultFields := func(names []string) []*field.ResultField {
		fs := make([]*field.ResultField, 0, len(names))

		for i := 0; i < len(names); i++ {
			f := &field.ResultField{
				TableName: "t",
				Name:      names[i],
			}

			f.ColumnInfo.Name = model.NewCIStr(names[i])

			fs = append(fs, f)
		}

		return fs
	}

	createFields := func(ps []pair) []*field.Field {
		fields := make([]*field.Field, len(ps))
		for i, f := range ps {
			fields[i] = &field.Field{
				Expr: &expression.Ident{
					CIStr: model.NewCIStr(f.Name),
				},
				AsName: f.AsName,
			}
		}
		return fields
	}

	tbl := []struct {
		Fields []pair
		Name   string
		Err    bool
		Index  int
	}{
		{[]pair{{"id", ""}}, "id", false, 0},
		{[]pair{{"id", "a"}, {"name", "a"}}, "a", true, -1},
		{[]pair{{"id", "a"}, {"name", "a"}}, "id", false, -1},
	}

	for _, t := range tbl {
		rs := createResultFields([]string{"id", "name"})
		fs := createFields(t.Fields)

		sl, err := plans.ResolveSelectList(fs, rs)
		c.Assert(err, IsNil)

		idx, err := sl.CheckAmbiguous(&expression.Ident{
			CIStr: model.NewCIStr(t.Name),
		})

		if t.Err {
			c.Assert(err, NotNil)
			continue
		}

		c.Assert(err, IsNil)
		c.Assert(t.Index, DeepEquals, idx)
	}
}
