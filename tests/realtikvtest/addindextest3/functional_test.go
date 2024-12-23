// Copyright 2024 PingCAP, Inc.
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

package addindextest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestAddIndexPresplitIndexRegions(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	var splitKeyHex [][]byte
	err := failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/beforePresplitIndex", func(splitKeys [][]byte) {
		for _, k := range splitKeys {
			splitKeyHex = append(splitKeyHex, bytes.Clone(k))
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/beforePresplitIndex")
		require.NoError(t, err)
	})
	checkSplitKeys := func(idxID int64, count int, reset bool) {
		cnt := 0
		for _, k := range splitKeyHex {
			indexID, err := tablecodec.DecodeIndexID(k)
			if err == nil && indexID == idxID {
				cnt++
			}
		}
		require.Equal(t, count, cnt, splitKeyHex)
		if reset {
			splitKeyHex = nil
		}
	}
	var idxID int64
	nextIdxID := func() int64 {
		idxID++
		return idxID
	}
	resetIdxID := func() {
		idxID = 0
	}

	tk.MustExec("create table t (a int primary key, b int);")
	for i := 0; i < 10; i++ {
		insertSQL := fmt.Sprintf("insert into t values (%[1]d, %[1]d);", 10000*i)
		tk.MustExec(insertSQL)
	}
	retRows := tk.MustQuery("show table t regions;").Rows()
	require.Len(t, retRows, 1)
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = off;")
	tk.MustExec("set @@global.tidb_enable_dist_task = off;")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = 4;")
	checkSplitKeys(nextIdxID(), 3, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));")
	checkSplitKeys(nextIdxID(), 3, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("alter table t add index idx(b) /*T![pre_split] pre_split_regions = (by (10000), (20000), (30000)) */;")
	checkSplitKeys(nextIdxID(), 3, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 3);")
	checkSplitKeys(nextIdxID(), 2, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = on;")

	tk.MustExec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));")
	nextID := nextIdxID()
	checkSplitKeys(nextID, 0, false)
	checkSplitKeys(tablecodec.TempIndexPrefix|nextID, 3, true)

	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = off;")

	// Test partition tables.
	resetIdxID()
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int primary key, b int) partition by hash(a) partitions 4;")
	for i := 0; i < 10; i++ {
		insertSQL := fmt.Sprintf("insert into t values (%[1]d, %[1]d);", 10000*i)
		tk.MustExec(insertSQL)
	}
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));")
	checkSplitKeys(nextIdxID(), 3*4, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 3);")
	checkSplitKeys(nextIdxID(), 2*4, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = on;")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (by (10000), (20000), (30000));")
	checkSplitKeys(nextIdxID(), 0, false)
	checkSplitKeys(tablecodec.TempIndexPrefix|3, 12, true)
	tk.MustExec("drop index idx on t;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = off;")

	resetIdxID()
	tk.MustExec("drop table t;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = on;")
	tk.MustExec("set @@global.tidb_enable_dist_task = off;")
	tk.MustExec("create table t (a int, b int) partition by range (b)" +
		" (partition p0 values less than (10), " +
		"  partition p1 values less than (maxvalue));")
	tk.MustExec("alter table t add unique index p_a (a) global pre_split_regions = (by (5), (15));")
	checkSplitKeys(tablecodec.TempIndexPrefix|nextIdxID(), 2, true)
}

func TestAddIndexPresplitFunctional(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int);")

	tk.MustGetErrMsg("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 0);",
		"Split index region num should be greater than 0")
	tk.MustGetErrMsg("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 10000);",
		"Split index region num exceeded the limit 1000")
	err := failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockSplitIndexRegionAndWaitErr", "2*return")
	require.NoError(t, err)
	t.Cleanup(func() {
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockSplitIndexRegionAndWaitErr")
		require.NoError(t, err)
	})
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (between (0) and (10 * 10000) regions 3);")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a bigint primary key, b int);")
	tk.MustExec("insert into t values (1, 1), (10, 1);")
	tk.MustExec("alter table t add index idx(b) pre_split_regions = (between (1) and (2) regions 3);")
	tk.MustExec("drop table t;")
}
