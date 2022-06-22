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
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	testddlutil "github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/stretchr/testify/require"
)

const indexModifyLease = 600 * time.Millisecond

func TestAddPrimaryKey1(t *testing.T) {
	testAddIndex(t, testPlain, "create table test_add_index (c1 bigint, c2 bigint, c3 bigint, unique key(c1))", "primary")
}

func TestAddPrimaryKey2(t *testing.T) {
	testAddIndex(t, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, key(c1))
			      partition by range (c3) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "primary")
}

func TestAddPrimaryKey3(t *testing.T) {
	testAddIndex(t, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, key(c1))
			      partition by hash (c3) partitions 4;`, "primary")
}

func TestAddPrimaryKey4(t *testing.T) {
	testAddIndex(t, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, key(c1))
			      partition by range columns (c3) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "primary")
}

func TestAddIndex1(t *testing.T) {
	testAddIndex(t, testPlain,
		"create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))", "")
}

func TestAddIndex1WithShardRowID(t *testing.T) {
	testAddIndex(t, testPartition|testShardRowID,
		"create table test_add_index (c1 bigint, c2 bigint, c3 bigint) SHARD_ROW_ID_BITS = 4 pre_split_regions = 4;", "")
}

func TestAddIndex2(t *testing.T) {
	testAddIndex(t, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by range (c1) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "")
}

func TestAddIndex2WithShardRowID(t *testing.T) {
	testAddIndex(t, testPartition|testShardRowID,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint)
				  SHARD_ROW_ID_BITS = 4 pre_split_regions = 4
			      partition by range (c1) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "")
}

func TestAddIndex3(t *testing.T) {
	testAddIndex(t, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by hash (c1) partitions 4;`, "")
}

func TestAddIndex3WithShardRowID(t *testing.T) {
	testAddIndex(t, testPartition|testShardRowID,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint)
				  SHARD_ROW_ID_BITS = 4 pre_split_regions = 4
			      partition by hash (c1) partitions 4;`, "")
}

func TestAddIndex4(t *testing.T) {
	testAddIndex(t, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by range columns (c1) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "")
}

func TestAddIndex4WithShardRowID(t *testing.T) {
	testAddIndex(t, testPartition|testShardRowID,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint)
				  SHARD_ROW_ID_BITS = 4 pre_split_regions = 4
			      partition by range columns (c1) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "")
}

func TestAddIndex5(t *testing.T) {
	testAddIndex(t, testClusteredIndex,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c2, c3))`, "")
}

type testAddIndexType uint8

const (
	testPlain          testAddIndexType = 1
	testPartition      testAddIndexType = 1 << 1
	testClusteredIndex testAddIndexType = 1 << 2
	testShardRowID     testAddIndexType = 1 << 3
)

func testAddIndex(t *testing.T, tp testAddIndexType, createTableSQL, idxTp string) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	isTestPartition := (testPartition & tp) > 0
	isTestShardRowID := (testShardRowID & tp) > 0
	if isTestShardRowID {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
		tk.MustExec("set global tidb_scatter_region = 1")
		defer func() {
			atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
			tk.MustExec("set global tidb_scatter_region = 0")
		}()
	}
	if isTestPartition {
		tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	} else if (testClusteredIndex & tp) > 0 {
		tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	}
	tk.MustExec("drop table if exists test_add_index")
	tk.MustExec(createTableSQL)

	done := make(chan error, 1)
	start := -10
	num := defaultBatchSize
	// first add some rows
	batchInsert(tk, "test_add_index", start, num)

	// Add some discrete rows.
	maxBatch := 20
	batchCnt := 100
	otherKeys := make([]int, 0, batchCnt*maxBatch)
	// Make sure there are no duplicate keys.
	base := defaultBatchSize * 20
	for i := 1; i < batchCnt; i++ {
		if isTestShardRowID {
			base = i % 4 << 61
		}
		n := base + i*defaultBatchSize + i
		for j := 0; j < rand.Intn(maxBatch); j++ {
			n += j
			sql := fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", n, n, n)
			tk.MustExec(sql)
			otherKeys = append(otherKeys, n)
		}
	}
	// Encounter the value of math.MaxInt64 in middle of
	v := math.MaxInt64 - defaultBatchSize/2
	tk.MustExec(fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", v, v, v))
	otherKeys = append(otherKeys, v)

	addIdxSQL := fmt.Sprintf("alter table test_add_index add %s key c3_index(c3)", idxTp)
	testddlutil.SessionExecInGoroutine(store, "test", addIdxSQL, done)

	deletedKeys := make(map[int]struct{})

	ticker := time.NewTicker(indexModifyLease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			require.NoError(t, err)
		case <-ticker.C:
			// When the server performance is particularly poor,
			// the adding index operation can not be completed.
			// So here is a limit to the number of rows inserted.
			if num > defaultBatchSize*10 {
				break
			}
			step := 5
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				deletedKeys[n] = struct{}{}
				sql := fmt.Sprintf("delete from test_add_index where c1 = %d", n)
				tk.MustExec(sql)
				sql = fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", i, i, i)
				tk.MustExec(sql)
			}
			num += step
		}
	}

	if isTestShardRowID {
		rows := tk.MustQuery("show table test_add_index regions").Rows()
		require.GreaterOrEqual(t, len(rows), 16)
		tk.MustExec("admin check table test_add_index")
		return
	}

	// get exists keys
	keys := make([]int, 0, num)
	for i := start; i < num; i++ {
		if _, ok := deletedKeys[i]; ok {
			continue
		}
		keys = append(keys, i)
	}
	keys = append(keys, otherKeys...)

	// test index key
	expectedRows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		expectedRows = append(expectedRows, []interface{}{fmt.Sprintf("%v", key)})
	}
	tk.MustQuery(fmt.Sprintf("select c1 from test_add_index where c3 >= %d order by c1", start)).Check(expectedRows)
	tk.MustExec("admin check table test_add_index")
	if isTestPartition {
		return
	}

	// TODO: Support explain in future.
	// rows := tk.MustQuery("explain select c1 from test_add_index where c3 >= 100").Rows()
	// ay := dumpRows(c, rows)
	// require.Contains(t, fmt.Sprintf("%v", ay), "c3_index")

	// get all row handles
	require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	tbl := external.GetTableByName(t, tk, "test", "test_add_index")
	handles := kv.NewHandleMap()
	err := tables.IterRecords(tbl, tk.Session(), tbl.Cols(),
		func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
			handles.Set(h, struct{}{})
			return true, nil
		})
	require.NoError(t, err)

	// check in index
	var nidx table.Index
	idxName := "c3_index"
	if len(idxTp) != 0 {
		idxName = "primary"
	}
	for _, tidx := range tbl.Indices() {
		if tidx.Meta().Name.L == idxName {
			nidx = tidx
			break
		}
	}
	// Make sure there is index with name c3_index.
	require.NotNil(t, nidx)
	require.Greater(t, nidx.Meta().ID, int64(0))
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	require.NoError(t, txn.Rollback())

	require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	tk.MustExec("admin check table test_add_index")
	tk.MustExec("drop table test_add_index")
}

func TestAddIndexForGeneratedColumn(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(y year NOT NULL DEFAULT '2155')")
	for i := 0; i < 50; i++ {
		tk.MustExec("insert into t values (?)", i)
	}
	tk.MustExec("insert into t values()")
	tk.MustExec("ALTER TABLE t ADD COLUMN y1 year as (y + 2)")
	tk.MustExec("ALTER TABLE t ADD INDEX idx_y(y1)")

	tbl := external.GetTableByName(t, tk, "test", "t")
	for _, idx := range tbl.Indices() {
		require.False(t, strings.EqualFold(idx.Meta().Name.L, "idx_c2"))
	}
	// NOTE: this test case contains a bug, it should be uncommented after the bug is fixed.
	// TODO: Fix bug https://github.com/pingcap/tidb/issues/12181
	// tk.MustExec("delete from t where y = 2155")
	// tk.MustExec("alter table t add index idx_y(y1)")
	// tk.MustExec("alter table t drop index idx_y")

	// Fix issue 9311.
	tk.MustExec("drop table if exists gcai_table")
	tk.MustExec("create table gcai_table (id int primary key);")
	tk.MustExec("insert into gcai_table values(1);")
	tk.MustExec("ALTER TABLE gcai_table ADD COLUMN d date DEFAULT '9999-12-31';")
	tk.MustExec("ALTER TABLE gcai_table ADD COLUMN d1 date as (DATE_SUB(d, INTERVAL 31 DAY));")
	tk.MustExec("ALTER TABLE gcai_table ADD INDEX idx(d1);")
	tk.MustQuery("select * from gcai_table").Check(testkit.Rows("1 9999-12-31 9999-11-30"))
	tk.MustQuery("select d1 from gcai_table use index(idx)").Check(testkit.Rows("9999-11-30"))
	tk.MustExec("admin check table gcai_table")
	// The column is PKIsHandle in generated column expression.
	tk.MustExec("ALTER TABLE gcai_table ADD COLUMN id1 int as (id+5);")
	tk.MustExec("ALTER TABLE gcai_table ADD INDEX idx1(id1);")
	tk.MustQuery("select * from gcai_table").Check(testkit.Rows("1 9999-12-31 9999-11-30 6"))
	tk.MustQuery("select id1 from gcai_table use index(idx1)").Check(testkit.Rows("6"))
	tk.MustExec("admin check table gcai_table")
}

// TestAddPrimaryKeyRollback1 is used to test scenarios that will roll back when a duplicate primary key is encountered.
func TestAddPrimaryKeyRollback1(t *testing.T) {
	idxName := "PRIMARY"
	addIdxSQL := "alter table t1 add primary key c3_index (c3);"
	errMsg := "[kv:1062]Duplicate entry '" + strconv.Itoa(defaultBatchSize*2-10) + "' for key 'PRIMARY'"
	testAddIndexRollback(t, idxName, addIdxSQL, errMsg, false)
}

// TestAddPrimaryKeyRollback2 is used to test scenarios that will roll back when a null primary key is encountered.
func TestAddPrimaryKeyRollback2(t *testing.T) {
	idxName := "PRIMARY"
	addIdxSQL := "alter table t1 add primary key c3_index (c3);"
	errMsg := "[ddl:1138]Invalid use of NULL value"
	testAddIndexRollback(t, idxName, addIdxSQL, errMsg, true)
}

func TestAddUniqueIndexRollback(t *testing.T) {
	idxName := "c3_index"
	addIdxSQL := "create unique index c3_index on t1 (c3)"
	errMsg := "[kv:1062]Duplicate entry '" + strconv.Itoa(defaultBatchSize*2-10) + "' for key 'c3_index'"
	testAddIndexRollback(t, idxName, addIdxSQL, errMsg, false)
}

func testAddIndexRollback(t *testing.T, idxName, addIdxSQL, errMsg string, hasNullValsInKey bool) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int, unique key(c1))")
	// defaultBatchSize is equal to ddl.defaultBatchSize
	base := defaultBatchSize * 2
	count := base
	// add some rows
	batchInsert(tk, "t1", 0, count)
	// add some null rows
	if hasNullValsInKey {
		for i := count - 10; i < count; i++ {
			tk.MustExec("insert into t1 values (?, ?, null)", i+10, i)
		}
	} else {
		// add some duplicate rows
		for i := count - 10; i < count; i++ {
			tk.MustExec("insert into t1 values (?, ?, ?)", i+10, i, i)
		}
	}

	done := make(chan error, 1)
	go backgroundExec(store, addIdxSQL, done)

	times := 0
	ticker := time.NewTicker(indexModifyLease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			require.EqualError(t, err, errMsg)
			break LOOP
		case <-ticker.C:
			if times >= 10 {
				break
			}
			step := 5
			// delete some rows, and add some data
			for i := count; i < count+step; i++ {
				n := rand.Intn(count)
				// (2048, 2038, 2038) and (2038, 2038, 2038)
				// Don't delete rows where c1 is 2048 or 2038, otherwise, the entry value in duplicated error message would change.
				if n == defaultBatchSize*2-10 || n == defaultBatchSize*2 {
					continue
				}
				tk.MustExec("delete from t1 where c1 = ?", n)
				tk.MustExec("insert into t1 values (?, ?, ?)", i+10, i, i)
			}
			count += step
			times++
		}
	}

	tbl := external.GetTableByName(t, tk, "test", "t1")
	for _, tidx := range tbl.Indices() {
		require.False(t, strings.EqualFold(tidx.Meta().Name.L, idxName))
	}

	// delete duplicated/null rows, then add index
	for i := base - 10; i < base; i++ {
		tk.MustExec("delete from t1 where c1 = ?", i+10)
	}
	tk.MustExec(addIdxSQL)
	tk.MustExec("drop table t1")
}

func TestAddIndexWithSplitTable(t *testing.T) {
	createSQL := "CREATE TABLE test_add_index(a bigint PRIMARY KEY AUTO_RANDOM(4), b varchar(255), c bigint)"
	stSQL := fmt.Sprintf("SPLIT TABLE test_add_index BETWEEN (%d) AND (%d) REGIONS 16;", math.MinInt64, math.MaxInt64)
	testAddIndexWithSplitTable(t, createSQL, stSQL)
}

func TestAddIndexWithShardRowID(t *testing.T) {
	createSQL := "create table test_add_index(a bigint, b bigint, c bigint) SHARD_ROW_ID_BITS = 4 pre_split_regions = 4;"
	testAddIndexWithSplitTable(t, createSQL, "")
}

func testAddIndexWithSplitTable(t *testing.T, createSQL, splitTableSQL string) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	hasAutoRandomField := len(splitTableSQL) > 0
	if !hasAutoRandomField {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
		tk.MustExec("set global tidb_scatter_region = 1")
		defer func() {
			atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
			tk.MustExec("set global tidb_scatter_region = 0")
		}()
	}
	tk.MustExec(createSQL)

	batchInsertRows := func(tk *testkit.TestKit, needVal bool, tbl string, start, end int) error {
		dml := fmt.Sprintf("insert into %s values", tbl)
		for i := start; i < end; i++ {
			if needVal {
				dml += fmt.Sprintf("(%d, %d, %d)", i, i, i)
			} else {
				dml += "()"
			}
			if i != end-1 {
				dml += ","
			}
		}
		_, err := tk.Exec(dml)
		return err
	}

	done := make(chan error, 1)
	start := -20
	num := defaultBatchSize
	// Add some discrete rows.
	goCnt := 10
	errCh := make(chan error, goCnt)
	for i := 0; i < goCnt; i++ {
		base := (i % 8) << 60
		go func(b int, eCh chan error) {
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			eCh <- batchInsertRows(tk1, !hasAutoRandomField, "test_add_index", base+start, base+num)
		}(base, errCh)
	}
	for i := 0; i < goCnt; i++ {
		err := <-errCh
		require.NoError(t, err)
	}

	if hasAutoRandomField {
		tk.MustQuery(splitTableSQL).Check(testkit.Rows("15 1"))
	}
	tk.MustQuery("select @@session.tidb_wait_split_region_finish").Check(testkit.Rows("1"))
	rows := tk.MustQuery("show table test_add_index regions").Rows()
	require.Len(t, rows, 16)
	addIdxSQL := "alter table test_add_index add index idx(a)"
	testddlutil.SessionExecInGoroutine(store, "test", addIdxSQL, done)

	ticker := time.NewTicker(indexModifyLease / 5)
	defer ticker.Stop()
	num = 0
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			require.NoError(t, err)
		case <-ticker.C:
			// When the server performance is particularly poor,
			// the adding index operation can not be completed.
			// So here is a limit to the number of rows inserted.
			if num >= 1000 {
				break
			}
			step := 20
			// delete, insert and update some data
			for i := num; i < num+step; i++ {
				sql := fmt.Sprintf("delete from test_add_index where a = %d", i+1)
				tk.MustExec(sql)
				if hasAutoRandomField {
					sql = "insert into test_add_index values ()"
				} else {
					sql = fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", i, i, i)
				}
				tk.MustExec(sql)
				sql = fmt.Sprintf("update test_add_index set b = %d", i*10)
				tk.MustExec(sql)
			}
			num += step
		}
	}

	tk.MustExec("admin check table test_add_index")
}

func TestAddAnonymousIndex(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_anonymous_index (c1 int, c2 int, C3 int)")
	tk.MustExec("alter table t_anonymous_index add index (c1, c2)")
	// for dropping empty index
	err := tk.ExecToErr("alter table t_anonymous_index drop index")
	require.Error(t, err)
	// The index name is c1 when adding index (c1, c2).
	tk.MustExec("alter table t_anonymous_index drop index c1")
	tbl := external.GetTableByName(t, tk, "test", "t_anonymous_index")
	require.Len(t, tbl.Indices(), 0)
	// for adding some indices that the first column name is c1
	tk.MustExec("alter table t_anonymous_index add index (c1)")
	err = tk.ExecToErr("alter table t_anonymous_index add index c1 (c2)")
	require.Error(t, err)
	tbl = external.GetTableByName(t, tk, "test", "t_anonymous_index")
	require.Len(t, tbl.Indices(), 1)
	require.Equal(t, "c1", tbl.Indices()[0].Meta().Name.L)
	// The MySQL will be a warning.
	tk.MustExec("alter table t_anonymous_index add index c1_3 (c1)")
	tk.MustExec("alter table t_anonymous_index add index (c1, c2, C3)")
	// The MySQL will be a warning.
	tk.MustExec("alter table t_anonymous_index add index (c1)")
	tbl = external.GetTableByName(t, tk, "test", "t_anonymous_index")
	require.Len(t, tbl.Indices(), 4)
	tk.MustExec("alter table t_anonymous_index drop index c1")
	tk.MustExec("alter table t_anonymous_index drop index c1_2")
	tk.MustExec("alter table t_anonymous_index drop index c1_3")
	tk.MustExec("alter table t_anonymous_index drop index c1_4")
	// for case-insensitive
	tk.MustExec("alter table t_anonymous_index add index (C3)")
	tk.MustExec("alter table t_anonymous_index drop index c3")
	tk.MustExec("alter table t_anonymous_index add index c3 (C3)")
	tk.MustExec("alter table t_anonymous_index drop index C3")
	// for anonymous index with column name `primary`
	tk.MustExec("create table t_primary (`primary` int, b int, key (`primary`))")
	tbl = external.GetTableByName(t, tk, "test", "t_primary")
	require.Equal(t, "primary_2", tbl.Indices()[0].Meta().Name.L)
	tk.MustExec("alter table t_primary add index (`primary`);")
	tbl = external.GetTableByName(t, tk, "test", "t_primary")
	require.Equal(t, "primary_2", tbl.Indices()[0].Meta().Name.L)
	require.Equal(t, "primary_3", tbl.Indices()[1].Meta().Name.L)
	tk.MustExec("alter table t_primary add primary key(b);")
	tbl = external.GetTableByName(t, tk, "test", "t_primary")
	require.Equal(t, "primary_2", tbl.Indices()[0].Meta().Name.L)
	require.Equal(t, "primary_3", tbl.Indices()[1].Meta().Name.L)
	require.Equal(t, "primary", tbl.Indices()[2].Meta().Name.L)
	tk.MustExec("create table t_primary_2 (`primary` int, key primary_2 (`primary`), key (`primary`))")
	tbl = external.GetTableByName(t, tk, "test", "t_primary_2")
	require.Equal(t, "primary_2", tbl.Indices()[0].Meta().Name.L)
	require.Equal(t, "primary_3", tbl.Indices()[1].Meta().Name.L)
	tk.MustExec("create table t_primary_3 (`primary_2` int, key(`primary_2`), `primary` int, key(`primary`));")
	tbl = external.GetTableByName(t, tk, "test", "t_primary_3")
	require.Equal(t, "primary_2", tbl.Indices()[0].Meta().Name.L)
	require.Equal(t, "primary_3", tbl.Indices()[1].Meta().Name.L)
}

func TestAddIndexWithPK(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tests := []struct {
		name string
		mode variable.ClusteredIndexDefMode
	}{
		{
			"ClusteredIndexDefModeIntOnly",
			variable.ClusteredIndexDefModeIntOnly,
		},
		{
			"ClusteredIndexDefModeOn",
			variable.ClusteredIndexDefModeOn,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tk.Session().GetSessionVars().EnableClusteredIndex = test.mode
			tk.MustExec("drop table if exists test_add_index_with_pk")
			tk.MustExec("create table test_add_index_with_pk(a int not null, b int not null default '0', primary key(a))")
			tk.MustExec("insert into test_add_index_with_pk values(1, 2)")
			tk.MustExec("alter table test_add_index_with_pk add index idx (a)")
			tk.MustQuery("select a from test_add_index_with_pk").Check(testkit.Rows("1"))
			tk.MustExec("insert into test_add_index_with_pk values(2, 2)")
			tk.MustExec("alter table test_add_index_with_pk add index idx1 (a, b)")
			tk.MustQuery("select * from test_add_index_with_pk").Check(testkit.Rows("1 2", "2 2"))
			tk.MustExec("drop table if exists test_add_index_with_pk1")
			tk.MustExec("create table test_add_index_with_pk1(a int not null, b int not null default '0', c int, d int, primary key(c))")
			tk.MustExec("insert into test_add_index_with_pk1 values(1, 1, 1, 1)")
			tk.MustExec("alter table test_add_index_with_pk1 add index idx (c)")
			tk.MustExec("insert into test_add_index_with_pk1 values(2, 2, 2, 2)")
			tk.MustQuery("select * from test_add_index_with_pk1").Check(testkit.Rows("1 1 1 1", "2 2 2 2"))
			tk.MustExec("drop table if exists test_add_index_with_pk2")
			tk.MustExec("create table test_add_index_with_pk2(a int not null, b int not null default '0', c int unsigned, d int, primary key(c))")
			tk.MustExec("insert into test_add_index_with_pk2 values(1, 1, 1, 1)")
			tk.MustExec("alter table test_add_index_with_pk2 add index idx (c)")
			tk.MustExec("insert into test_add_index_with_pk2 values(2, 2, 2, 2)")
			tk.MustQuery("select * from test_add_index_with_pk2").Check(testkit.Rows("1 1 1 1", "2 2 2 2"))
			tk.MustExec("drop table if exists t")
			tk.MustExec("create table t (a int, b int, c int, primary key(a, b));")
			tk.MustExec("insert into t values (1, 2, 3);")
			tk.MustExec("create index idx on t (a, b);")
		})
	}
}

func TestAddGlobalIndex(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = true
	})
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table test_t1 (a int, b int) partition by range (b)" +
		" (partition p0 values less than (10), " +
		"  partition p1 values less than (maxvalue));")
	tk.MustExec("insert test_t1 values (1, 1)")
	tk.MustExec("alter table test_t1 add unique index p_a (a);")
	tk.MustExec("insert test_t1 values (2, 11)")
	tbl := external.GetTableByName(t, tk, "test", "test_t1")
	tblInfo := tbl.Meta()
	indexInfo := tblInfo.FindIndexByName("p_a")
	require.NotNil(t, indexInfo)
	require.True(t, indexInfo.Global)

	require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)

	// check row 1
	pid := tblInfo.Partition.Definitions[0].ID
	idxVals := []types.Datum{types.NewDatum(1)}
	rowVals := []types.Datum{types.NewDatum(1), types.NewDatum(1)}
	checkGlobalIndexRow(t, tk.Session(), tblInfo, indexInfo, pid, idxVals, rowVals)

	// check row 2
	pid = tblInfo.Partition.Definitions[1].ID
	idxVals = []types.Datum{types.NewDatum(2)}
	rowVals = []types.Datum{types.NewDatum(2), types.NewDatum(11)}
	checkGlobalIndexRow(t, tk.Session(), tblInfo, indexInfo, pid, idxVals, rowVals)
	require.NoError(t, txn.Commit(context.Background()))

	// Test add global Primary Key index
	tk.MustExec("create table test_t2 (a int, b int) partition by range (b)" +
		" (partition p0 values less than (10), " +
		"  partition p1 values less than (maxvalue));")
	tk.MustExec("insert test_t2 values (1, 1)")
	tk.MustExec("alter table test_t2 add primary key (a) nonclustered;")
	tk.MustExec("insert test_t2 values (2, 11)")
	tbl = external.GetTableByName(t, tk, "test", "test_t2")
	tblInfo = tbl.Meta()
	indexInfo = tblInfo.FindIndexByName("primary")
	require.NotNil(t, indexInfo)
	require.True(t, indexInfo.Global)

	require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)

	// check row 1
	pid = tblInfo.Partition.Definitions[0].ID
	idxVals = []types.Datum{types.NewDatum(1)}
	rowVals = []types.Datum{types.NewDatum(1), types.NewDatum(1)}
	checkGlobalIndexRow(t, tk.Session(), tblInfo, indexInfo, pid, idxVals, rowVals)

	// check row 2
	pid = tblInfo.Partition.Definitions[1].ID
	idxVals = []types.Datum{types.NewDatum(2)}
	rowVals = []types.Datum{types.NewDatum(2), types.NewDatum(11)}
	checkGlobalIndexRow(t, tk.Session(), tblInfo, indexInfo, pid, idxVals, rowVals)

	require.NoError(t, txn.Commit(context.Background()))
}

// checkGlobalIndexRow reads one record from global index and check. Only support int handle.
func checkGlobalIndexRow(
	t *testing.T,
	ctx sessionctx.Context,
	tblInfo *model.TableInfo,
	indexInfo *model.IndexInfo,
	pid int64,
	idxVals []types.Datum,
	rowVals []types.Datum,
) {
	require.NoError(t, sessiontxn.NewTxn(context.Background(), ctx))
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	sc := ctx.GetSessionVars().StmtCtx

	tblColMap := make(map[int64]*types.FieldType, len(tblInfo.Columns))
	for _, col := range tblInfo.Columns {
		tblColMap[col.ID] = &col.FieldType
	}

	// Check local index entry does not exist.
	localPrefix := tablecodec.EncodeTableIndexPrefix(pid, indexInfo.ID)
	it, err := txn.Iter(localPrefix, nil)
	require.NoError(t, err)
	// no local index entry.
	require.False(t, it.Valid() && it.Key().HasPrefix(localPrefix))
	it.Close()

	// Check global index entry.
	encodedValue, err := codec.EncodeKey(sc, nil, idxVals...)
	require.NoError(t, err)
	key := tablecodec.EncodeIndexSeekKey(tblInfo.ID, indexInfo.ID, encodedValue)
	require.NoError(t, err)
	value, err := txn.Get(context.Background(), key)
	require.NoError(t, err)
	idxColInfos := tables.BuildRowcodecColInfoForIndexColumns(indexInfo, tblInfo)
	colVals, err := tablecodec.DecodeIndexKV(key, value, len(indexInfo.Columns), tablecodec.HandleDefault, idxColInfos)
	require.NoError(t, err)
	require.Len(t, colVals, len(idxVals)+2)
	for i, val := range idxVals {
		_, d, err := codec.DecodeOne(colVals[i])
		require.NoError(t, err)
		require.Equal(t, val, d)
	}
	_, d, err := codec.DecodeOne(colVals[len(idxVals)+1]) // pid
	require.NoError(t, err)
	require.Equal(t, pid, d.GetInt64())

	_, d, err = codec.DecodeOne(colVals[len(idxVals)]) // handle
	require.NoError(t, err)
	h := kv.IntHandle(d.GetInt64())
	rowKey := tablecodec.EncodeRowKey(pid, h.Encoded())
	rowValue, err := txn.Get(context.Background(), rowKey)
	require.NoError(t, err)
	rowValueDatums, err := tablecodec.DecodeRowToDatumMap(rowValue, tblColMap, time.UTC)
	require.NoError(t, err)
	require.NotNil(t, rowValueDatums)
	for i, val := range rowVals {
		require.Equal(t, val, rowValueDatums[tblInfo.Columns[i].ID])
	}
}

func TestDropIndexes(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	// drop multiple indexes
	createSQL := "create table test_drop_indexes (id int, c1 int, c2 int, primary key(id), key i1(c1), key i2(c2));"
	dropIdxSQL := "alter table test_drop_indexes drop index i1, drop index i2;"
	idxNames := []string{"i1", "i2"}
	testDropIndexes(t, store, createSQL, dropIdxSQL, idxNames)

	createSQL = "create table test_drop_indexes (id int, c1 int, c2 int, primary key(id) nonclustered, unique key i1(c1), key i2(c2));"
	dropIdxSQL = "alter table test_drop_indexes drop primary key, drop index i1;"
	idxNames = []string{"primary", "i1"}
	testDropIndexes(t, store, createSQL, dropIdxSQL, idxNames)

	createSQL = "create table test_drop_indexes (uuid varchar(32), c1 int, c2 int, primary key(uuid), unique key i1(c1), key i2(c2));"
	dropIdxSQL = "alter table test_drop_indexes drop primary key, drop index i1, drop index i2;"
	idxNames = []string{"primary", "i1", "i2"}
	testDropIndexes(t, store, createSQL, dropIdxSQL, idxNames)

	testDropIndexesIfExists(t, store)
	testDropIndexesFromPartitionedTable(t, store)
}

func testDropIndexes(t *testing.T, store kv.Storage, createSQL, dropIdxSQL string, idxNames []string) {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_drop_indexes")
	tk.MustExec(createSQL)
	done := make(chan error, 1)

	num := 100
	// add some rows
	for i := 0; i < num; i++ {
		tk.MustExec("insert into test_drop_indexes values (?, ?, ?)", i, i, i)
	}
	idxIDs := make([]int64, 0, 3)
	for _, idxName := range idxNames {
		idxIDs = append(idxIDs, external.GetIndexID(t, tk, "test", "test_drop_indexes", idxName))
	}
	testddlutil.SessionExecInGoroutine(store, "test", dropIdxSQL, done)

	ticker := time.NewTicker(indexModifyLease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			require.NoError(t, err)
		case <-ticker.C:
			step := 5
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				tk.MustExec("update test_drop_indexes set c2 = 1 where c1 = ?", n)
				tk.MustExec("insert into test_drop_indexes values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}
}

func testDropIndexesIfExists(t *testing.T, store kv.Storage) {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists test_drop_indexes_if_exists;")
	tk.MustExec("create table test_drop_indexes_if_exists (id int, c1 int, c2 int, primary key(id), key i1(c1), key i2(c2));")

	// Drop different indexes.
	tk.MustGetErrMsg(
		"alter table test_drop_indexes_if_exists drop index i1, drop index i3;",
		"[ddl:1091]index i3 doesn't exist",
	)
	tk.MustExec("alter table test_drop_indexes_if_exists drop index i1, drop index if exists i3;")
	tk.MustQuery("show warnings;").Check(testkit.RowsWithSep("|", "Warning|1091|index i3 doesn't exist"))

	// Verify the impact of deletion order when dropping duplicate indexes.
	tk.MustGetErrMsg(
		"alter table test_drop_indexes_if_exists drop index i2, drop index i2;",
		"[ddl:1091]index i2 doesn't exist",
	)
	tk.MustGetErrMsg(
		"alter table test_drop_indexes_if_exists drop index if exists i2, drop index i2;",
		"[ddl:1091]index i2 doesn't exist",
	)
	tk.MustExec("alter table test_drop_indexes_if_exists drop index i2, drop index if exists i2;")
	tk.MustQuery("show warnings;").Check(testkit.RowsWithSep("|", "Warning|1091|index i2 doesn't exist"))
}

func testDropIndexesFromPartitionedTable(t *testing.T, store kv.Storage) {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists test_drop_indexes_from_partitioned_table;")
	tk.MustExec(`
		create table test_drop_indexes_from_partitioned_table (id int, c1 int, c2 int, primary key(id), key i1(c1), key i2(c2))
		partition by range(id) (partition p0 values less than (6), partition p1 values less than maxvalue);
	`)
	for i := 0; i < 20; i++ {
		tk.MustExec("insert into test_drop_indexes_from_partitioned_table values (?, ?, ?)", i, i, i)
	}
	tk.MustExec("alter table test_drop_indexes_from_partitioned_table drop index i1, drop index if exists i2;")
	tk.MustExec("alter table test_drop_indexes_from_partitioned_table add index i1(c1)")
	tk.MustExec("alter table test_drop_indexes_from_partitioned_table drop index i1, drop index if exists i1;")
	tk.MustExec("alter table test_drop_indexes_from_partitioned_table drop column c1, drop column c2;")
	tk.MustExec("alter table test_drop_indexes_from_partitioned_table add column c1 int")
	tk.MustExec("alter table test_drop_indexes_from_partitioned_table drop column c1, drop column if exists c1;")
}

func TestDropPrimaryKey(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	idxName := "primary"
	createSQL := "create table test_drop_index (c1 int, c2 int, c3 int, unique key(c1), primary key(c3) nonclustered)"
	dropIdxSQL := "alter table test_drop_index drop primary key;"
	testDropIndex(t, store, createSQL, dropIdxSQL, idxName)
}

func TestDropIndex(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	idxName := "c3_index"
	createSQL := "create table test_drop_index (c1 int, c2 int, c3 int, unique key(c1), key c3_index(c3))"
	dropIdxSQL := "alter table test_drop_index drop index c3_index;"
	testDropIndex(t, store, createSQL, dropIdxSQL, idxName)
}

func testDropIndex(t *testing.T, store kv.Storage, createSQL, dropIdxSQL, idxName string) {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_drop_index")
	tk.MustExec(createSQL)
	done := make(chan error, 1)
	tk.MustExec("delete from test_drop_index")

	num := 100
	// add some rows
	for i := 0; i < num; i++ {
		tk.MustExec("insert into test_drop_index values (?, ?, ?)", i, i, i)
	}
	testddlutil.SessionExecInGoroutine(store, "test", dropIdxSQL, done)

	ticker := time.NewTicker(indexModifyLease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			require.NoError(t, err)
		case <-ticker.C:
			step := 5
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				tk.MustExec("update test_drop_index set c2 = 1 where c1 = ?", n)
				tk.MustExec("insert into test_drop_index values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	rows := tk.MustQuery("explain select c1 from test_drop_index where c3 >= 0")
	require.NotContains(t, fmt.Sprintf("%v", rows), idxName)

	tk.MustExec("drop table test_drop_index")
}

func TestAddMultiColumnsIndexClusterIndex(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_add_multi_col_index_clustered;")
	tk.MustExec("create database test_add_multi_col_index_clustered;")
	tk.MustExec("use test_add_multi_col_index_clustered;")

	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (a int, b varchar(10), c int, primary key (a, b));")
	tk.MustExec("insert into t values (1, '1', 1), (2, '2', NULL), (3, '3', 3);")
	tk.MustExec("create index idx on t (a, c);")

	tk.MustExec("admin check index t idx;")
	tk.MustExec("admin check table t;")

	tk.MustExec("insert into t values (5, '5', 5), (6, '6', NULL);")

	tk.MustExec("admin check index t idx;")
	tk.MustExec("admin check table t;")
}

func TestAddIndexWithDupCols(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	err1 := infoschema.ErrColumnExists.GenWithStackByArgs("b")
	err2 := infoschema.ErrColumnExists.GenWithStackByArgs("B")

	tk.MustExec("create table test_add_index_with_dup (a int, b int)")
	err := tk.ExecToErr("create index c on test_add_index_with_dup(b, a, b)")
	require.ErrorIs(t, err, errors.Cause(err1))
	err = tk.ExecToErr("create index c on test_add_index_with_dup(b, a, B)")
	require.ErrorIs(t, err, errors.Cause(err2))
	err = tk.ExecToErr("alter table test_add_index_with_dup add index c (b, a, b)")
	require.ErrorIs(t, err, errors.Cause(err1))
	err = tk.ExecToErr("alter table test_add_index_with_dup add index c (b, a, B)")
	require.ErrorIs(t, err, errors.Cause(err2))

	tk.MustExec("drop table test_add_index_with_dup")
}

func TestAnonymousIndex(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("create table t(bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb int, b int)")
	tk.MustExec("alter table t add index bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb(b)")
	tk.MustExec("alter table t add index (bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb)")
	rows := tk.MustQuery("show index from t where key_name='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'").Rows()
	require.Len(t, rows, 1)
	rows = tk.MustQuery("show index from t where key_name='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb_2'").Rows()
	require.Len(t, rows, 1)
}

func TestAddIndexWithDupIndex(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, indexModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	err1 := dbterror.ErrDupKeyName.GenWithStack("index already exist %s", "idx")
	err2 := dbterror.ErrDupKeyName.GenWithStack("index already exist %s; "+
		"a background job is trying to add the same index, "+
		"please check by `ADMIN SHOW DDL JOBS`", "idx")

	// When there is already an duplicate index, show error message.
	tk.MustExec("create table test_add_index_with_dup (a int, key idx (a))")
	err := tk.ExecToErr("alter table test_add_index_with_dup add index idx (a)")
	require.ErrorIs(t, err, errors.Cause(err1))

	// When there is another session adding duplicate index with state other than
	// StatePublic, show explicit error message.
	tbl := external.GetTableByName(t, tk, "test", "test_add_index_with_dup")
	indexInfo := tbl.Meta().FindIndexByName("idx")
	indexInfo.State = model.StateNone
	err = tk.ExecToErr("alter table test_add_index_with_dup add index idx (a)")
	require.ErrorIs(t, err, errors.Cause(err2))
}
