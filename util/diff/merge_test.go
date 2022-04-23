// Copyright 2018 PingCAP, Inc.
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

package diff

import (
	"container/heap"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/parser"
)

var _ = Suite(&testMergerSuite{})

type testMergerSuite struct{}

func (s *testMergerSuite) TestMerge(c *C) {
	createTableSQL := "create table test.test(id int(24), name varchar(24), age int(24), primary key(id, name));"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL, parser.New())
	c.Assert(err, IsNil)

	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	ids := []string{"3", "2", "2", "4", "1", "NULL"}
	names := []string{"d", "NULL", "c", "b", "a", "e"}
	ages := []string{"1", "2", "3", "NULL", "5", "4"}

	expectIDs := []string{"NULL", "1", "2", "2", "3", "4"}
	expectNames := []string{"e", "a", "NULL", "c", "d", "b"}

	rowDatas := &RowDatas{
		Rows:         make([]RowData, 0, len(ids)),
		OrderKeyCols: orderKeyCols,
	}

	heap.Init(rowDatas)
	for i, id := range ids {
		data := map[string]*dbutil.ColumnData{
			"id":   {Data: []byte(id), IsNull: (id == "NULL")},
			"name": {Data: []byte(names[i]), IsNull: (names[i] == "NULL")},
			"age":  {Data: []byte(ages[i]), IsNull: (ages[i] == "NULL")},
		}
		heap.Push(rowDatas, RowData{
			Data: data,
		})
	}

	for i := 0; i < len(ids); i++ {
		rowData := heap.Pop(rowDatas).(RowData)
		id := string(rowData.Data["id"].Data)
		name := string(rowData.Data["name"].Data)
		c.Assert(id, Equals, expectIDs[i])
		c.Assert(name, Equals, expectNames[i])
	}
}
