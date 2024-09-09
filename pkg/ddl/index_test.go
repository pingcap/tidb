// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestDecodeAddIndexArgsCompatibility(t *testing.T) {
	cases := []struct {
		raw                     json.RawMessage
		uniques                 []bool
		indexNames              []pmodel.CIStr
		indexPartSpecifications [][]*ast.IndexPartSpecification
		indexOptions            []*ast.IndexOption
		hiddenCols              [][]*model.ColumnInfo
	}{
		{
			raw: json.RawMessage(`[
true,
{"O":"t","L":"t"},
[
	{"Column":{"Schema":{"O":"","L":""},"Table":{"O":"","L":""},"Name":{"O":"a","L":"a"}},"Length":-1,"Desc":false,"Expr":null},
	{"Column":{"Schema":{"O":"","L":""},"Table":{"O":"","L":""},"Name":{"O":"b","L":"b"}},"Length":-1,"Desc":false,"Expr":null}
],
null,
[],
false]`),
			uniques: []bool{true},
			indexNames: []pmodel.CIStr{
				{O: "t", L: "t"},
			},
			indexPartSpecifications: [][]*ast.IndexPartSpecification{
				{
					{
						Column: &ast.ColumnName{
							Schema: pmodel.CIStr{O: "", L: ""},
							Table:  pmodel.CIStr{O: "", L: ""},
							Name:   pmodel.CIStr{O: "a", L: "a"},
						},
						Length: -1,
						Desc:   false,
						Expr:   nil,
					},
					{
						Column: &ast.ColumnName{
							Schema: pmodel.CIStr{O: "", L: ""},
							Table:  pmodel.CIStr{O: "", L: ""},
							Name:   pmodel.CIStr{O: "b", L: "b"},
						},
						Length: -1,
						Desc:   false,
						Expr:   nil,
					},
				},
			},
			indexOptions: []*ast.IndexOption{nil},
			hiddenCols:   [][]*model.ColumnInfo{{}},
		},
		{
			raw: json.RawMessage(`[
[false,true],
[{"O":"t","L":"t"},{"O":"t1","L":"t1"}],
[
	[
		{"Column":{"Schema":{"O":"","L":""},"Table":{"O":"","L":""},"Name":{"O":"a","L":"a"}},"Length":-1,"Desc":false,"Expr":null},
		{"Column":{"Schema":{"O":"","L":""},"Table":{"O":"","L":""},"Name":{"O":"b","L":"b"}},"Length":-1,"Desc":false,"Expr":null}
	],
	[
		{"Column":{"Schema":{"O":"","L":""},"Table":{"O":"","L":""},"Name":{"O":"a","L":"a"}},"Length":-1,"Desc":false,"Expr":null}
	]
],
[null,null],
[[],[]],
[false,false]]`),
			uniques: []bool{false, true},
			indexNames: []pmodel.CIStr{
				{O: "t", L: "t"}, {O: "t1", L: "t1"},
			},
			indexPartSpecifications: [][]*ast.IndexPartSpecification{
				{
					{
						Column: &ast.ColumnName{
							Schema: pmodel.CIStr{O: "", L: ""},
							Table:  pmodel.CIStr{O: "", L: ""},
							Name:   pmodel.CIStr{O: "a", L: "a"},
						},
						Length: -1,
						Desc:   false,
						Expr:   nil,
					},
					{
						Column: &ast.ColumnName{
							Schema: pmodel.CIStr{O: "", L: ""},
							Table:  pmodel.CIStr{O: "", L: ""},
							Name:   pmodel.CIStr{O: "b", L: "b"},
						},
						Length: -1,
						Desc:   false,
						Expr:   nil,
					},
				},
				{
					{
						Column: &ast.ColumnName{
							Schema: pmodel.CIStr{O: "", L: ""},
							Table:  pmodel.CIStr{O: "", L: ""},
							Name:   pmodel.CIStr{O: "a", L: "a"},
						},
						Length: -1,
						Desc:   false,
						Expr:   nil,
					},
				},
			},
			indexOptions: []*ast.IndexOption{nil, nil},
			hiddenCols:   [][]*model.ColumnInfo{{}, {}},
		},
	}

	for _, c := range cases {
		job := &model.Job{RawArgs: c.raw}
		uniques, indexNames, specs, indexOptions, hiddenCols, err := decodeAddIndexArgs(job)
		require.NoError(t, err)
		require.Equal(t, c.uniques, uniques)
		require.Equal(t, c.indexNames, indexNames)
		require.Equal(t, c.indexPartSpecifications, specs)
		require.Equal(t, c.indexOptions, indexOptions)
		require.Equal(t, c.hiddenCols, hiddenCols)
	}
}
