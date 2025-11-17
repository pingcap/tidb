// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metering

import (
	"maps"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataEquals(t *testing.T) {
	cases := []struct {
		pair  [2]Data
		equal bool
	}{
		{pair: [2]Data{{taskID: 1, getRequests: 1}, {taskID: 1, getRequests: 1}}, equal: true},
		{pair: [2]Data{{taskID: 1, getRequests: 1}, {taskID: 1, getRequests: 2}}, equal: false},
		{pair: [2]Data{{taskID: 1, putRequests: 1}, {taskID: 1, putRequests: 1}}, equal: true},
		{pair: [2]Data{{taskID: 1, putRequests: 1}, {taskID: 1, putRequests: 2}}, equal: false},
		// we only compare the data fields, not taskID
		{pair: [2]Data{{taskID: 1, getRequests: 1}, {taskID: 2, getRequests: 1}}, equal: true},
	}
	for i, c := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, c.equal, c.pair[0].equals(&c.pair[1]), "case %d failed", i)
		})
	}
}

func TestDataCalMeterDataItem(t *testing.T) {
	currData := &Data{getRequests: 10, putRequests: 20, readBytes: 300, writeBytes: 400, taskID: 1, keyspace: "ks", taskType: "tt"}
	require.Nil(t, currData.calMeterDataItem(currData))
	getItemFn := func(in map[string]any) map[string]any {
		maps.Copy(in, map[string]any{
			"version":     "1",
			"cluster_id":  "ks",
			"source_name": category,
			"task_type":   "tt",
			"task_id":     int64(1),
		})
		return in
	}
	require.EqualValues(t, getItemFn(map[string]any{
		"get_requests": uint64(5),
		"put_requests": uint64(15),
		"read_bytes":   uint64(200),
		"write_bytes":  uint64(300),
	}), currData.calMeterDataItem(&Data{getRequests: 5, putRequests: 5, readBytes: 100, writeBytes: 100}))
	require.EqualValues(t, getItemFn(map[string]any{
		"get_requests": uint64(5),
	}), currData.calMeterDataItem(&Data{getRequests: 5, putRequests: 20, readBytes: 300, writeBytes: 400}))
}
