// Copyright 2021 PingCAP, Inc.
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

package executor

import (
	gjson "encoding/json"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/types/json"
)

var _ = SerialSuites(&testShowPlacementSuit{})

type testShowPlacementSuit struct {
}

func (s *testPartitionSuite) TestShowPlacementLabelsBuilder(c *C) {
	cases := []struct {
		stores  [][]*helper.StoreLabel
		expects [][]interface{}
	}{
		{
			stores:  nil,
			expects: nil,
		},
		{
			stores: [][]*helper.StoreLabel{
				{{"zone", "z1"}, {"rack", "r3"}, {"host", "h1"}},
				{{"zone", "z1"}, {"rack", "r1"}, {"host", "h2"}},
				{{"zone", "z1"}, {"rack", "r2"}, {"host", "h2"}},
				{{"zone", "z2"}, {"rack", "r1"}, {"host", "h2"}},
				nil,
				{{"k1", "v1"}},
			},
			expects: [][]interface{}{
				{"host", []string{"h1", "h2"}},
				{"k1", []string{"v1"}},
				{"rack", []string{"r1", "r2", "r3"}},
				{"zone", []string{"z1", "z2"}},
			},
		},
	}

	b := &showPlacementLabelsResultBuilder{}
	toBinaryJSON := func(obj interface{}) (bj json.BinaryJSON) {
		d, err := gjson.Marshal(obj)
		c.Assert(err, IsNil)
		err = bj.UnmarshalJSON(d)
		c.Assert(err, IsNil)
		return
	}

	for _, ca := range cases {
		for _, store := range ca.stores {
			bj := toBinaryJSON(store)
			err := b.AppendStoreLabels(bj)
			c.Assert(err, IsNil)
		}

		rows, err := b.BuildRows()
		c.Assert(err, IsNil)
		c.Assert(len(rows), Equals, len(ca.expects))
		for idx, expect := range ca.expects {
			row := rows[idx]
			bj := toBinaryJSON(expect[1])

			c.Assert(row[0].(string), Equals, expect[0].(string))
			c.Assert(row[1].(json.BinaryJSON).TypeCode, Equals, bj.TypeCode)
			c.Assert(row[1].(json.BinaryJSON).Value, BytesEquals, bj.Value)
		}
	}
}
