// Copyright 2026 PingCAP, Inc.
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

package storage_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

// buildTestCollector creates a ReservoirRowSampleCollector with nSamples items
// having incrementing int columns and weights.
func buildTestCollector(t *testing.T, nSamples, nCols int) *statistics.ReservoirRowSampleCollector {
	t.Helper()
	c := statistics.NewReservoirRowSampleCollector(nSamples, nCols)
	for range nCols {
		c.Base().FMSketches = append(c.Base().FMSketches, statistics.NewFMSketch(100))
	}
	c.Base().Count = int64(nSamples * 10) // total rows > sample count
	for i := range nSamples {
		cols := make([]types.Datum, nCols)
		for j := range nCols {
			cols[j] = types.NewIntDatum(int64(i*100 + j))
		}
		item := &statistics.ReservoirRowSampleItem{
			Columns: cols,
			Weight:  int64(i + 1),
		}
		c.Base().Samples = append(c.Base().Samples, item)
	}
	for i := range nCols {
		c.Base().NullCount[i] = int64(i + 1)
		c.Base().TotalSizes[i] = int64((i + 1) * 1000)
	}
	return c
}

func TestSaveAndLoadRoundTrip(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := do.StatsHandle()
	_ = tk

	const tableID, partID int64 = 9999, 1001
	const version uint64 = 42

	orig := buildTestCollector(t, 20, 3)

	// Save via the StatsReadWriter wrapper (uses pooled session + txn).
	err := h.SavePartitionSamples(tableID, partID, version, orig)
	require.NoError(t, err)

	// Verify the row exists via raw SQL.
	rows := tk.MustQuery(
		"SELECT table_id, partition_id, version, max_sample_size, sample_count, row_count FROM mysql.stats_samples WHERE table_id = ? AND partition_id = ?",
		tableID, partID,
	).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "9999", rows[0][0].(string))
	require.Equal(t, "1001", rows[0][1].(string))
	require.Equal(t, "42", rows[0][2].(string))
	require.Equal(t, "20", rows[0][3].(string))  // max_sample_size
	require.Equal(t, "20", rows[0][4].(string))  // sample_count
	require.Equal(t, "200", rows[0][5].(string)) // row_count = 20*10

	// Load via a pooled session and verify the data matches.
	var loaded map[int64]*statistics.ReservoirRowSampleCollector
	err = util.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		var loadErr error
		loaded, loadErr = storage.LoadSampleCollectorsFromStorage(sctx, tableID, nil)
		return loadErr
	}, util.FlagWrapTxn)
	require.NoError(t, err)
	require.Len(t, loaded, 1)

	c := loaded[partID]
	require.NotNil(t, c)
	require.Len(t, c.Base().Samples, 20)
	require.Equal(t, orig.Base().Count, c.Base().Count)
	require.Equal(t, orig.MaxSampleSize, c.MaxSampleSize)

	// Verify sample data integrity.
	for i, sample := range c.Base().Samples {
		require.Len(t, sample.Columns, 3, "sample %d column count", i)
		require.Equal(t, orig.Base().Samples[i].Weight, sample.Weight, "sample %d weight", i)
	}
}

func TestSaveOverwritesExisting(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := do.StatsHandle()

	const tableID, partID int64 = 8888, 2002

	c1 := buildTestCollector(t, 5, 1)
	err := h.SavePartitionSamples(tableID, partID, 1, c1)
	require.NoError(t, err)

	// Overwrite with different data.
	c2 := buildTestCollector(t, 10, 1)
	err = h.SavePartitionSamples(tableID, partID, 2, c2)
	require.NoError(t, err)

	// Only one row should exist (REPLACE INTO).
	rows := tk.MustQuery(
		"SELECT COUNT(*) FROM mysql.stats_samples WHERE table_id = ? AND partition_id = ?",
		tableID, partID,
	).Rows()
	require.Equal(t, "1", rows[0][0].(string))

	// Verify it's the second version.
	rows = tk.MustQuery(
		"SELECT version, sample_count FROM mysql.stats_samples WHERE table_id = ? AND partition_id = ?",
		tableID, partID,
	).Rows()
	require.Equal(t, "2", rows[0][0].(string))
	require.Equal(t, "10", rows[0][1].(string))
}

func TestLoadExcludePartitions(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	_ = testkit.NewTestKit(t, store)
	h := do.StatsHandle()

	const tableID int64 = 7777
	partIDs := []int64{101, 102, 103, 104}
	for _, pid := range partIDs {
		c := buildTestCollector(t, 3, 1)
		err := h.SavePartitionSamples(tableID, pid, 1, c)
		require.NoError(t, err)
	}

	// Exclude partitions 102 and 104.
	var loaded map[int64]*statistics.ReservoirRowSampleCollector
	err := util.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		var loadErr error
		loaded, loadErr = storage.LoadSampleCollectorsFromStorage(sctx, tableID, []int64{102, 104})
		return loadErr
	}, util.FlagWrapTxn)
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	require.Contains(t, loaded, int64(101))
	require.Contains(t, loaded, int64(103))
	require.NotContains(t, loaded, int64(102))
	require.NotContains(t, loaded, int64(104))
}

func TestLoadEmptyTable(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	_ = testkit.NewTestKit(t, store)
	h := do.StatsHandle()

	var loaded map[int64]*statistics.ReservoirRowSampleCollector
	err := util.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		var loadErr error
		loaded, loadErr = storage.LoadSampleCollectorsFromStorage(sctx, 12345, nil)
		return loadErr
	}, util.FlagWrapTxn)
	require.NoError(t, err)
	require.Empty(t, loaded)
}

func TestDeleteByPartition(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := do.StatsHandle()

	const tableID int64 = 6666
	for _, pid := range []int64{201, 202, 203} {
		c := buildTestCollector(t, 3, 1)
		err := h.SavePartitionSamples(tableID, pid, 1, c)
		require.NoError(t, err)
	}

	// Delete partition 202 only.
	err := util.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		return storage.DeleteSamplesByPartition(util.StatsCtx, sctx, 202)
	}, util.FlagWrapTxn)
	require.NoError(t, err)

	// 202 gone, others remain.
	rows := tk.MustQuery("SELECT partition_id FROM mysql.stats_samples WHERE table_id = ? ORDER BY partition_id", tableID).Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "201", rows[0][0].(string))
	require.Equal(t, "203", rows[1][0].(string))
}

func TestDeleteByTable(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := do.StatsHandle()

	// Insert samples for two tables.
	for _, pid := range []int64{301, 302} {
		c := buildTestCollector(t, 2, 1)
		require.NoError(t, h.SavePartitionSamples(5555, pid, 1, c))
	}
	c := buildTestCollector(t, 2, 1)
	require.NoError(t, h.SavePartitionSamples(4444, 401, 1, c))

	// Delete table 5555.
	err := util.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		return storage.DeleteSamplesByTable(util.StatsCtx, sctx, 5555)
	}, util.FlagWrapTxn)
	require.NoError(t, err)

	// Table 5555 gone, table 4444 intact.
	rows := tk.MustQuery("SELECT table_id FROM mysql.stats_samples ORDER BY table_id").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "4444", rows[0][0].(string))
}

func TestDeleteNonExistentIsNoOp(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	_ = testkit.NewTestKit(t, store)
	h := do.StatsHandle()

	// Deleting from empty table should succeed without error.
	err := util.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		return storage.DeleteSamplesByPartition(util.StatsCtx, sctx, 99999)
	}, util.FlagWrapTxn)
	require.NoError(t, err)

	err = util.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		return storage.DeleteSamplesByTable(util.StatsCtx, sctx, 99999)
	}, util.FlagWrapTxn)
	require.NoError(t, err)
}

func TestProtoRoundTrip(t *testing.T) {
	// Verify that ToProto -> Marshal -> Unmarshal -> FromProto preserves data
	// at the proto level (independent of SQL storage).
	orig := buildTestCollector(t, 15, 2)

	pb := orig.Base().ToProto()
	data, err := pb.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	pb2 := &tipb.RowSampleCollector{}
	err = pb2.Unmarshal(data)
	require.NoError(t, err)

	tracker := memory.NewTracker(-1, -1)
	restored := statistics.NewReservoirRowSampleCollector(15, 0)
	restored.Base().FromProto(pb2, tracker)

	require.Equal(t, orig.Base().Count, restored.Base().Count)
	require.Len(t, restored.Base().Samples, 15)
	for i, sample := range restored.Base().Samples {
		require.Equal(t, orig.Base().Samples[i].Weight, sample.Weight, "sample %d weight", i)
		require.Len(t, sample.Columns, 2, "sample %d columns", i)
	}
	require.Len(t, restored.Base().FMSketches, len(orig.Base().FMSketches))
	require.Equal(t, orig.Base().NullCount, restored.Base().NullCount)
	require.Equal(t, orig.Base().TotalSizes, restored.Base().TotalSizes)
}
