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

package taskinfo_test

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/importinto/taskinfo"
	"github.com/stretchr/testify/require"
)

func TestTaskMetaCompatibility(t *testing.T) {
	// TaskMeta moved from dxf/importinto; its persisted layout must not change.
	const legacy = `{"JobID":5,"Plan":{"DBName":"db"},"Stmt":"import","Summary":{},"EligibleInstances":null,"ChunkMap":null,"prepared_meta_external_path":"p"}`
	var meta taskinfo.TaskMeta
	require.NoError(t, json.Unmarshal([]byte(legacy), &meta))
	require.EqualValues(t, 5, meta.JobID)
	require.Equal(t, "db", meta.Plan.DBName)
	require.Equal(t, "p", meta.PreparedMetaExternalPath)
	encoded, err := json.Marshal(&meta)
	require.NoError(t, err)
	var actual, expected map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &actual))
	require.NoError(t, json.Unmarshal([]byte(legacy), &expected))
	for _, key := range []string{"JobID", "Stmt", "prepared_meta_external_path"} {
		require.JSONEq(t, string(expected[key]), string(actual[key]), key)
	}
	for key := range expected {
		require.Contains(t, actual, key)
	}
}
