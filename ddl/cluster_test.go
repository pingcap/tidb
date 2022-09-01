// Copyright 2022 PingCAP, Inc.
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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestGetFlashbackKeyRanges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	kvRanges, err := ddl.GetFlashbackKeyRanges(se, tablecodec.EncodeTablePrefix(0))
	require.NoError(t, err)
	// The results are 6 key ranges
	// 0: (stats_meta,stats_histograms,stats_buckets)
	// 1: (stats_feedback)
	// 2: (stats_top_n)
	// 3: (stats_extended)
	// 4: (stats_fm_sketch)
	// 5: (stats_history, stats_meta_history)
	require.Len(t, kvRanges, 6)
	// tableID for mysql.stats_meta is 20
	require.Equal(t, kvRanges[0].StartKey, tablecodec.EncodeTablePrefix(20))
	// tableID for mysql.stats_feedback is 30
	require.Equal(t, kvRanges[1].StartKey, tablecodec.EncodeTablePrefix(30))
	// tableID for mysql.stats_meta_history is 62
	require.Equal(t, kvRanges[5].EndKey, tablecodec.EncodeTablePrefix(62+1))

	// The original table ID for range is [60, 63)
	// startKey is 61, so return [61, 63)
	kvRanges, err = ddl.GetFlashbackKeyRanges(se, tablecodec.EncodeTablePrefix(61))
	require.NoError(t, err)
	require.Len(t, kvRanges, 1)
	require.Equal(t, kvRanges[0].StartKey, tablecodec.EncodeTablePrefix(61))

	// The original ranges are [48, 49), [60, 63)
	// startKey is 59, so return [60, 63)
	kvRanges, err = ddl.GetFlashbackKeyRanges(se, tablecodec.EncodeTablePrefix(59))
	require.NoError(t, err)
	require.Len(t, kvRanges, 1)
	require.Equal(t, kvRanges[0].StartKey, tablecodec.EncodeTablePrefix(60))

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE employees (" +
		"    id INT NOT NULL," +
		"    store_id INT NOT NULL" +
		") PARTITION BY RANGE (store_id) (" +
		"    PARTITION p0 VALUES LESS THAN (6)," +
		"    PARTITION p1 VALUES LESS THAN (11)," +
		"    PARTITION p2 VALUES LESS THAN (16)," +
		"    PARTITION p3 VALUES LESS THAN (21)" +
		");")
	kvRanges, err = ddl.GetFlashbackKeyRanges(se, tablecodec.EncodeTablePrefix(63))
	require.NoError(t, err)
	// start from table ID is 63, so only 1 kv range.
	require.Len(t, kvRanges, 1)
	// 1 tableID and 4 partitions.
	require.Equal(t, tablecodec.DecodeTableID(kvRanges[0].EndKey)-tablecodec.DecodeTableID(kvRanges[0].StartKey), int64(5))

	tk.MustExec("truncate table mysql.analyze_jobs")

	// truncate all `stats_` tables, make table ID consecutive.
	tk.MustExec("truncate table mysql.stats_meta")
	tk.MustExec("truncate table mysql.stats_histograms")
	tk.MustExec("truncate table mysql.stats_buckets")
	tk.MustExec("truncate table mysql.stats_feedback")
	tk.MustExec("truncate table mysql.stats_top_n")
	tk.MustExec("truncate table mysql.stats_extended")
	tk.MustExec("truncate table mysql.stats_fm_sketch")
	tk.MustExec("truncate table mysql.stats_history")
	tk.MustExec("truncate table mysql.stats_meta_history")
	kvRanges, err = ddl.GetFlashbackKeyRanges(se, tablecodec.EncodeTablePrefix(0))
	require.NoError(t, err)
	require.Len(t, kvRanges, 2)

	tk.MustExec("truncate table test.employees")
	kvRanges, err = ddl.GetFlashbackKeyRanges(se, tablecodec.EncodeTablePrefix(0))
	require.NoError(t, err)
	require.Len(t, kvRanges, 1)
}
