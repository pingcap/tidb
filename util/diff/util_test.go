// Copyright 2022 PingCAP, Inc.
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

package diff

import (
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/stretchr/testify/require"
)

func TestIgnoreColumns(t *testing.T) {
	createTableSQL1 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`))"
	tableInfo1, err := dbutil.GetTableInfoBySQL(createTableSQL1, parser.New())
	require.NoError(t, err)
	tbInfo := ignoreColumns(tableInfo1, []string{"a"})
	require.Len(t, tbInfo.Columns, 3)
	require.Len(t, tbInfo.Indices, 0)
	require.Equal(t, 2, tbInfo.Columns[2].Offset)

	createTableSQL2 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`), index idx(`b`, `c`))"
	tableInfo2, err := dbutil.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)
	tbInfo = ignoreColumns(tableInfo2, []string{"a", "b"})
	require.Len(t, tbInfo.Columns, 2)
	require.Len(t, tbInfo.Indices, 0)

	createTableSQL3 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`), index idx(`b`, `c`))"
	tableInfo3, err := dbutil.GetTableInfoBySQL(createTableSQL3, parser.New())
	require.NoError(t, err)
	tbInfo = ignoreColumns(tableInfo3, []string{"b", "c"})
	require.Len(t, tbInfo.Columns, 2)
	require.Len(t, tbInfo.Indices, 1)
}

func TestRowContainsCols(t *testing.T) {
	row := map[string]*dbutil.ColumnData{
		"a": nil,
		"b": nil,
		"c": nil,
	}

	cols := []*model.ColumnInfo{
		{
			Name: model.NewCIStr("a"),
		}, {
			Name: model.NewCIStr("b"),
		}, {
			Name: model.NewCIStr("c"),
		},
	}

	contain := rowContainsCols(row, cols)
	require.Equal(t, true, contain)

	delete(row, "a")
	contain = rowContainsCols(row, cols)
	require.Equal(t, false, contain)
}

func TestRowToString(t *testing.T) {
	row := make(map[string]*dbutil.ColumnData)
	row["id"] = &dbutil.ColumnData{
		Data:   []byte("1"),
		IsNull: false,
	}

	row["name"] = &dbutil.ColumnData{
		Data:   []byte("abc"),
		IsNull: false,
	}

	row["info"] = &dbutil.ColumnData{
		Data:   nil,
		IsNull: true,
	}

	rowStr := rowToString(row)
	require.Regexp(t, ".*id: 1.*", rowStr)
	require.Regexp(t, ".*name: abc.*", rowStr)
	require.Regexp(t, ".*info: IsNull.*", rowStr)
}

func TestMinLenInSlices(t *testing.T) {
	testCases := []struct {
		slices [][]string
		expect int
	}{
		{
			[][]string{
				{"1", "2"},
				{"1", "2", "3"},
			},
			2,
		}, {
			[][]string{
				{"1", "2"},
				{},
			},
			0,
		}, {
			[][]string{},
			0,
		}, {
			[][]string{
				{"1", "2"},
				{},
				{"1", "2", "3"},
			},
			0,
		},
	}

	for _, testCase := range testCases {
		minLen := minLenInSlices(testCase.slices)
		require.Equal(t, testCase.expect, minLen)
	}
}
