// Copyright 2026 PingCAP, Inc.
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

package importinto_test

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/importinto"
	"github.com/stretchr/testify/require"
)

func TestPlanMetadataCompatibility(t *testing.T) {
	// A task written before the extraction has flattened format fields and
	// capitalized query fields; neither may change when its Go owner moves.
	const legacy = `{"DBName":"db","DBID":7,"FieldsTerminatedBy":",","LinesTerminatedBy":"\n","DataSourceType":"query","OnDupKey":"error","use_new_collate":false,"QuerySource":{"ReadTS":123,"SchemaVersion":9,"EncodeTimestamp":456,"Tables":[{"DBID":7,"ID":11,"Revision":2}],"SessionStates":{"sys-vars":{"time_zone":"+00:00:30"},"rs-group":"rg1"},"InMViewMaintenance":true}}`
	var p importinto.Plan
	require.NoError(t, json.Unmarshal([]byte(legacy), &p))
	require.Equal(t, ",", p.FieldsTerminatedBy)
	require.Equal(t, "\n", p.LinesTerminatedBy)
	require.Equal(t, importinto.DataSourceTypeQuery, p.DataSourceType)
	require.EqualValues(t, 123, p.QuerySource.ReadTS)
	require.Equal(t, "rg1", p.QuerySource.SessionStates.ResourceGroupName)
	require.False(t, p.GetUseNewCollateOrDefault(true))
	require.Equal(t, importinto.OnDupKeyModeError, p.GetOnDupKeyMode())
	encoded, err := json.Marshal(p)
	require.NoError(t, err)
	var actual map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &actual))
	var expected map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(legacy), &expected))
	for key, value := range expected {
		require.JSONEq(t, string(value), string(actual[key]), key)
	}
	require.NotContains(t, actual, "LineFieldsInfo")
	require.NotContains(t, actual, "Parameters")
	require.NotContains(t, actual, "User")

	var old importinto.Plan
	require.NoError(t, json.Unmarshal([]byte(`{"DBName":"old"}`), &old))
	require.True(t, old.GetUseNewCollateOrDefault(true))
	require.Equal(t, importinto.OnDupKeyModeError, old.GetOnDupKeyMode())
	data, err := json.Marshal(old)
	require.NoError(t, err)
	require.NotContains(t, string(data), "QuerySource")
	require.NotContains(t, string(data), "use_new_collate")
}

// TestTaskMetadataCompatibility keeps persisted task fields stable across package moves.
func TestTaskMetadataCompatibility(t *testing.T) {
	const legacy = `{"JobID":12,"DDLJobID":42,"Plan":{"DBName":"db","QuerySource":{"ReadTS":1234}},"Stmt":"import","Summary":{"encode-summary":{"input-bytes":20,"input-rows":3},"row-count":3},"ChunkMap":{"1":[{"Path":"a.csv","Offset":5,"EndOffset":25,"PrevRowIDMax":0,"RowIDMax":3}]},"prepared_meta_external_path":"s3://bucket/meta"}`
	var task importinto.TaskMeta
	require.NoError(t, json.Unmarshal([]byte(legacy), &task))
	require.EqualValues(t, 1234, task.Plan.QuerySource.ReadTS)
	require.EqualValues(t, 20, task.Summary.EncodeSummary.Bytes)
	require.EqualValues(t, 3, task.Summary.ImportedRows)
	require.Equal(t, "a.csv:5", task.ChunkMap[1][0].GetKey())
	require.EqualValues(t, 20, task.ChunkMap[1][0].GetSize())
	require.Equal(t, "a.csv", task.ChunkMap[1][0].ToSourceFileMeta().Path)
	data, err := json.Marshal(task)
	require.NoError(t, err)
	var actual, expected map[string]any
	require.NoError(t, json.Unmarshal(data, &actual))
	require.NoError(t, json.Unmarshal([]byte(legacy), &expected))
	for _, key := range []string{"JobID", "DDLJobID", "Stmt", "prepared_meta_external_path"} {
		require.Equal(t, expected[key], actual[key], key)
	}
	// Empty struct summaries were always emitted even with omitempty.
	for key, value := range expected["Summary"].(map[string]any) {
		require.Equal(t, value, actual["Summary"].(map[string]any)[key], key)
	}
	var restored importinto.TaskMeta
	require.NoError(t, json.Unmarshal(data, &restored))
	require.Equal(t, task, restored)
}
