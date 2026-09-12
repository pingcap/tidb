// Copyright 2025 PingCAP, Inc.
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

package ddl

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestDecodeLegacyReorgCheckpoint(t *testing.T) {
	rawMeta := []byte(`{
		"reorg_checkpoint": {
			"global_sync_key": null,
			"physical_id": 114,
			"start_key": "bGVnYWN5LXN0YXJ0",
			"end_key": "bGVnYWN5LWVuZA==",
			"version": 1
		}
	}`)
	var reorgMeta ingest.JobReorgMeta
	err := json.Unmarshal(rawMeta, &reorgMeta)
	require.NoError(t, err)
	roundTripped, err := json.Marshal(reorgMeta)
	require.NoError(t, err)

	var decoded struct {
		Checkpoint map[string]json.RawMessage `json:"reorg_checkpoint"`
	}
	require.NoError(t, json.Unmarshal(roundTripped, &decoded))
	require.Contains(t, decoded.Checkpoint, "start_key")
	require.Contains(t, decoded.Checkpoint, "end_key")
	require.Equal(t, int64(114), reorgMeta.Checkpoint.PhysicalID)
	require.Equal(t, kv.Key("legacy-start"), reorgMeta.Checkpoint.StartKey)
	require.Equal(t, kv.Key("legacy-end"), reorgMeta.Checkpoint.EndKey)

	testCases := []struct {
		name           string
		reorgVersion   int64
		expectedEndKey kv.Key
	}{
		{
			name:           "current end key semantics",
			reorgVersion:   model.CurrentReorgMetaVersion,
			expectedEndKey: kv.Key("legacy-end"),
		},
		{
			name:           "legacy end key semantics",
			reorgVersion:   model.ReorgMetaVersion0,
			expectedEndKey: kv.Key("legacy-end").Next(),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			job := &model.Job{ReorgMeta: &model.DDLReorgMeta{Version: testCase.reorgVersion}}
			reorgInfo := &reorgInfo{
				PhysicalTableID: 175,
				StartKey:        kv.Key("current-start"),
				EndKey:          kv.Key("current-end"),
			}
			require.True(t, overwriteLegacyReorgInfoFromCheckpoint(job, reorgInfo, reorgMeta.Checkpoint))
			require.Equal(t, int64(114), reorgInfo.PhysicalTableID)
			require.Equal(t, kv.Key("legacy-start"), reorgInfo.StartKey)
			require.Equal(t, testCase.expectedEndKey, reorgInfo.EndKey)
		})
	}
}

func TestReorgCtxSetMaxProgress(t *testing.T) {
	rc := &reorgCtx{}

	require.Equal(t, float64(0), rc.maxProgress.Load())

	result := rc.setMaxProgress(0.5)
	require.Equal(t, 0.5, result)
	require.Equal(t, 0.5, rc.maxProgress.Load())

	result = rc.setMaxProgress(0.7)
	require.Equal(t, 0.7, result)
	require.Equal(t, 0.7, rc.maxProgress.Load())

	result = rc.setMaxProgress(0.3)
	require.Equal(t, 0.7, result)                // Returns old max
	require.Equal(t, 0.7, rc.maxProgress.Load()) // Value unchanged

	result = rc.setMaxProgress(0.7)
	require.Equal(t, 0.7, result)
	require.Equal(t, 0.7, rc.maxProgress.Load())

	result = rc.setMaxProgress(0.9)
	require.Equal(t, 0.9, result)
	require.Equal(t, 0.9, rc.maxProgress.Load())
}

func TestReorgCtxSetMaxProgressConcurrent(t *testing.T) {
	rc := &reorgCtx{}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(progress float64) {
			defer wg.Done()
			rc.setMaxProgress(progress)
		}(float64(i) / 100.0)
	}

	wg.Wait()

	require.Equal(t, 0.99, rc.maxProgress.Load())
}
