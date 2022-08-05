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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	gjson "encoding/json"
	"testing"

	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/types/json"
	"github.com/stretchr/testify/require"
)

func TestShowPlacementLabelsBuilder(t *testing.T) {
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
				{{Key: "zone", Value: "z1"}, {Key: "rack", Value: "r3"}, {Key: "host", Value: "h1"}},
				{{Key: "zone", Value: "z1"}, {Key: "rack", Value: "r1"}, {Key: "host", Value: "h2"}},
				{{Key: "zone", Value: "z1"}, {Key: "rack", Value: "r2"}, {Key: "host", Value: "h2"}},
				{{Key: "zone", Value: "z2"}, {Key: "rack", Value: "r1"}, {Key: "host", Value: "h2"}},
				nil,
				{{Key: "k1", Value: "v1"}},
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
		require.NoError(t, err)
		err = bj.UnmarshalJSON(d)
		require.NoError(t, err)
		return
	}

	for _, ca := range cases {
		for _, store := range ca.stores {
			bj := toBinaryJSON(store)
			err := b.AppendStoreLabels(bj)
			require.NoError(t, err)
		}

		rows, err := b.BuildRows()
		require.NoError(t, err)
		require.Equal(t, len(ca.expects), len(rows))
		for idx, expect := range ca.expects {
			row := rows[idx]
			bj := toBinaryJSON(expect[1])

			require.Equal(t, expect[0].(string), row[0].(string))
			require.Equal(t, bj.TypeCode, row[1].(json.BinaryJSON).TypeCode)
			require.Equal(t, bj.Value, row[1].(json.BinaryJSON).Value)
		}
	}
}
