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
