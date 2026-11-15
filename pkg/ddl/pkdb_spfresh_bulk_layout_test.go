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

package ddl

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestSPFreshBulkLayoutUsesStatisticsAndWorkerCapacity(t *testing.T) {
	rows := uint64(10_000_000)
	payload := uint64(0)
	estimate := spfreshBulkLayoutEstimate{rows: &rows, payloadBytes: &payload}
	for _, test := range []struct {
		name       string
		rows       uint64
		capacities []uint64
		expected   uint32
	}{
		{"tiny", 12, []uint64{5_000_000, 5_000_000, 5_000_000}, 1},
		{"fits one worker", 1_000_000, []uint64{5_000_000}, 1},
		{"more workers do not add shards", 1_000_000, []uint64{5_000_000, 5_000_000, 5_000_000}, 1},
		{"exact memory boundary", 10_000_000, []uint64{5_000_000, 5_000_000, 5_000_000}, 2},
		{"one row beyond boundary", 10_000_001, []uint64{5_000_000, 5_000_000, 5_000_000}, 3},
		{"empty estimate", 0, []uint64{5_000_000, 5_000_000, 5_000_000}, 1},
		{"smallest budget", 10_000_000, []uint64{8_000_000, 2_000_000, 8_000_000}, 5},
		{"fewer workers", 10_000_000, []uint64{8_000_000, 8_000_000}, 2},
		{"coarse limit", 1_000_000_000, []uint64{1_000_000}, model.SPFreshMaxBuildShards},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows = test.rows
			layout, err := planSPFreshBulkLayout(estimate, 0, 768, test.capacities)
			require.NoError(t, err)
			require.Equal(t, test.expected, layout.shardCount)
			require.Equal(t, test.expected > 1, layout.sampleTargetRows > 0)
		})
	}
	// Unknown row statistics do not invent a parallelism-based shard count.
	layout, err := planSPFreshBulkLayout(spfreshBulkLayoutEstimate{}, 0, 1024, []uint64{1, 1, 1})
	require.NoError(t, err)
	require.Equal(t, uint32(1), layout.shardCount)
	// Unknown payload uses the vector-only capacity estimate, not worker count.
	rows = 10_000_000
	estimate.payloadBytes = nil
	layout, err = planSPFreshBulkLayout(estimate, 0, 960, []uint64{4_000_000, 8_000_000})
	require.NoError(t, err)
	require.Equal(t, uint32(3), layout.shardCount)
	_, err = planSPFreshBulkLayout(estimate, 0, 768, []uint64{0})
	require.ErrorContains(t, err, "capacity")
}

func TestSPFreshBulkBuildShardFromIndexParam(t *testing.T) {
	for _, test := range []struct {
		value string
		count uint32
	}{{"", 0}, {"build_shard=1", 1}, {"build_shard=4", 4}, {"build_shard=256", 256}} {
		t.Run(test.value, func(t *testing.T) {
			tbl, idx := newSPFreshBulkTestTable()
			options, _, _, err := model.ParseSPFreshVectorIndexParam(test.value)
			require.NoError(t, err)
			idx.VectorInfo.IndexOptions = options
			// A resumed owner obtains the explicit count from persisted index metadata.
			encoded, err := json.Marshal(idx)
			require.NoError(t, err)
			var restored model.IndexInfo
			require.NoError(t, json.Unmarshal(encoded, &restored))
			job := newSPFreshBulkTestJob()
			job.ReorgMeta.IsDistReorg = true
			run, selected, err := prepareSPFreshBulkAdmission("s3://spfresh-mvp/layout", job, tbl, &restored, 0, []string{"http://worker:19000"})
			require.NoError(t, err)
			require.True(t, selected)
			require.Equal(t, test.count, run.buildShard)
			if test.count == 0 {
				return
			}
			// A nil DDL worker and unreachable endpoint prove explicit layouts
			// skip statistics and worker resource reads.
			layout, err := prepareSPFreshBulkLayout(context.Background(), nil, run)
			require.NoError(t, err)
			require.True(t, layout.explicit)
			require.Equal(t, test.count, layout.shardCount)
			require.Equal(t, test.count > 1, layout.sampleTargetRows > 0)
			require.Empty(t, run.workerMemoryBudgets)
		})
	}
	tbl, idx := newSPFreshBulkTestTable()
	idx.VectorInfo.IndexOptions = &model.SPFreshVectorIndexOptions{BuildShard: model.SPFreshMaxBuildShards + 1}
	job := newSPFreshBulkTestJob()
	job.ReorgMeta.IsDistReorg = true
	_, _, err := prepareSPFreshBulkAdmission("s3://spfresh-mvp/layout", job, tbl, idx, 0, []string{"http://worker:19000"})
	require.ErrorContains(t, err, "build_shard")
}

func TestSPFreshLayoutContractsOnlyAutomaticOverestimates(t *testing.T) {
	rows := uint64(10_000_000)
	payload := uint64(0)
	initial, err := planSPFreshBulkLayout(spfreshBulkLayoutEstimate{rows: &rows, payloadBytes: &payload}, 0, 768, []uint64{4_000_000, 4_000_000, 4_000_000})
	require.NoError(t, err)
	for _, actual := range []uint64{1, 2, 100, 100_000_000} {
		final, err := initial.finish(actual, 768)
		require.NoError(t, err)
		require.Equal(t, uint32(min(actual, 3)), final.shardCount)
		require.LessOrEqual(t, final.sampleTargetRows, initial.sampleTargetRows)
		require.Equal(t, actual > 1, final.sampleTargetRows > 0)
	}
	initial.explicit = true
	_, err = initial.finish(2, 768)
	require.ErrorContains(t, err, "source rows")
	_, err = initial.finish(0, 768)
	require.Error(t, err)
}

func TestSPFreshSampleBudgetIncludesValidationAndBoundsVectorBytes(t *testing.T) {
	for _, dimension := range []uint32{768, 960, 1024, 16383} {
		layout, err := planSPFreshBulkLayout(spfreshBulkLayoutEstimate{}, 256, dimension, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, layout.sampleTargetRows, uint64(256))
		require.LessOrEqual(t, layout.sampleTargetRows*uint64(dimension)*4, uint64(1<<30))
	}
	_, err := planSPFreshBulkLayout(spfreshBulkLayoutEstimate{}, 4, 0, nil)
	require.Error(t, err)
	_, err = planSPFreshBulkLayout(spfreshBulkLayoutEstimate{}, 257, 768, nil)
	require.ErrorContains(t, err, "flat-route limit")
}
