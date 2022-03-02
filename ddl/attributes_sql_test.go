// Copyright 2021 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/store/gcworker"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/stretchr/testify/require"
)

func TestAlterTableAttributes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table alter_t (c int);`)

	// normal cases
	tk.MustExec(`alter table alter_t attributes="merge_option=allow";`)
	tk.MustExec(`alter table alter_t attributes="merge_option=allow,key=value";`)

	// space cases
	tk.MustExec(`alter table alter_t attributes=" merge_option=allow ";`)
	tk.MustExec(`alter table alter_t attributes=" merge_option = allow , key = value ";`)

	// without equal
	tk.MustExec(`alter table alter_t attributes " merge_option=allow ";`)
	tk.MustExec(`alter table alter_t attributes " merge_option=allow , key=value ";`)

}

func TestAlterTablePartitionAttributes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table alter_p (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11),
	PARTITION p2 VALUES LESS THAN (16),
	PARTITION p3 VALUES LESS THAN (21)
);`)

	// normal cases
	tk.MustExec(`alter table alter_p partition p0 attributes="merge_option=allow";`)
	tk.MustExec(`alter table alter_p partition p1 attributes="merge_option=allow,key=value";`)

	// space cases
	tk.MustExec(`alter table alter_p partition p2 attributes=" merge_option=allow ";`)
	tk.MustExec(`alter table alter_p partition p3 attributes=" merge_option = allow , key = value ";`)

	// without equal
	tk.MustExec(`alter table alter_p partition p1 attributes " merge_option=allow ";`)
	tk.MustExec(`alter table alter_p partition p1 attributes " merge_option=allow , key=value ";`)
}

func TestTruncateTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table truncate_t (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add attributes
	tk.MustExec(`alter table truncate_t attributes="key=value";`)
	tk.MustExec(`alter table truncate_t partition p0 attributes="key1=value1";`)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)
	// truncate table
	tk.MustExec(`truncate table truncate_t;`)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	// check table truncate_t's attribute
	require.Equal(t, "schema/test/truncate_t", rows1[0][0])
	require.Equal(t, `"key=value"`, rows1[0][2])
	require.NotEqual(t, rows[0][3], rows1[0][3])
	// check partition p0's attribute
	require.Equal(t, "schema/test/truncate_t/p0", rows1[1][0])
	require.Equal(t, `"key1=value1"`, rows1[1][2])
	require.NotEqual(t, rows[1][3], rows1[1][3])

	// test only table
	tk.MustExec(`create table truncate_ot (c int);`)

	// add attribute
	tk.MustExec(`alter table truncate_ot attributes="key=value";`)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows2, 3)
	// truncate table
	tk.MustExec(`truncate table truncate_ot;`)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows3, 3)
	// check table truncate_ot's attribute
	require.Equal(t, "schema/test/truncate_ot", rows3[0][0])
	require.Equal(t, `"key=value"`, rows3[0][2])
	require.NotEqual(t, rows2[0][3], rows3[0][3])
}

func TestRenameTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table rename_t (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add attributes
	tk.MustExec(`alter table rename_t attributes="key=value";`)
	tk.MustExec(`alter table rename_t partition p0 attributes="key1=value1";`)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)
	// rename table
	tk.MustExec(`rename table rename_t to rename_t1;`)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	// check table rename_t1's attribute
	require.Equal(t, "schema/test/rename_t1", rows1[0][0])
	require.Equal(t, `"key=value"`, rows1[0][2])
	require.Equal(t, rows[0][3], rows1[0][3])
	// check partition p0's attribute
	require.Equal(t, "schema/test/rename_t1/p0", rows1[1][0])
	require.Equal(t, `"key1=value1"`, rows1[1][2])
	require.Equal(t, rows[1][3], rows1[1][3])

	// test only table
	tk.MustExec(`create table rename_ot (c int);`)

	// add attribute
	tk.MustExec(`alter table rename_ot attributes="key=value";`)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows2, 3)
	// rename table
	tk.MustExec(`rename table rename_ot to rename_ot1;`)

	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows3, 3)
	// check table rename_ot1's attribute
	require.Equal(t, "schema/test/rename_ot1", rows3[0][0])
	require.Equal(t, `"key=value"`, rows3[0][2])
	require.Equal(t, rows2[0][3], rows3[0][3])
}

func TestRecoverTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table recover_t (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	timeBeforeDrop, _, safePointSQL, resetGC := testkit.MockGC(tk)
	defer resetGC()

	// Set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// Set GC enable.
	require.NoError(t, gcutil.EnableGC(tk.Session()))

	// add attributes
	tk.MustExec(`alter table recover_t attributes="key=value";`)
	tk.MustExec(`alter table recover_t partition p0 attributes="key1=value1";`)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)
	// drop table
	tk.MustExec(`drop table recover_t;`)
	// recover table
	tk.MustExec(`recover table recover_t;`)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	// check table recover_t's attribute
	require.Equal(t, "schema/test/recover_t", rows1[0][0])
	require.Equal(t, `"key=value"`, rows1[0][2])
	require.Equal(t, rows[0][3], rows1[0][3])
	// check partition p0's attribute
	require.Equal(t, "schema/test/recover_t/p0", rows1[1][0])
	require.Equal(t, `"key1=value1"`, rows1[1][2])
	require.Equal(t, rows[1][3], rows1[1][3])
}

func TestFlashbackTable(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	_, err := infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table flash_t (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	timeBeforeDrop, _, safePointSQL, resetGC := testkit.MockGC(tk)
	defer resetGC()

	// Set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// Set GC enable.
	err = gcutil.EnableGC(tk.Session())
	require.NoError(t, err)

	// add attributes
	tk.MustExec(`alter table flash_t attributes="key=value";`)
	tk.MustExec(`alter table flash_t partition p0 attributes="key1=value1";`)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)
	// drop table
	tk.MustExec(`drop table flash_t;`)
	// flashback table
	tk.MustExec(`flashback table flash_t to flash_t1;`)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	// check table flash_t1's attribute
	require.Equal(t, "schema/test/flash_t1", rows1[0][0])
	require.Equal(t, `"key=value"`, rows1[0][2])
	require.Equal(t, rows[0][3], rows1[0][3])
	// check partition p0's attribute
	require.Equal(t, "schema/test/flash_t1/p0", rows1[1][0])
	require.Equal(t, `"key1=value1"`, rows1[1][2])
	require.Equal(t, rows[1][3], rows1[1][3])

	// truncate table
	tk.MustExec(`truncate table flash_t1;`)
	// flashback table
	tk.MustExec(`flashback table flash_t1 to flash_t2;`)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	// check table flash_t2's attribute
	require.Equal(t, "schema/test/flash_t2", rows2[0][0])
	require.Equal(t, `"key=value"`, rows2[0][2])
	require.Equal(t, rows[0][3], rows2[0][3])
	// check partition p0's attribute
	require.Equal(t, "schema/test/flash_t2/p0", rows2[1][0])
	require.Equal(t, `"key1=value1"`, rows2[1][2])
	require.Equal(t, rows[1][3], rows2[1][3])
}

func TestDropTable(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	_, err := infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table drop_t (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)
	failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed")
	}()

	timeBeforeDrop, _, safePointSQL, resetGC := testkit.MockGC(tk)
	defer resetGC()

	// Set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// Set GC enable.
	err = gcutil.EnableGC(tk.Session())
	require.NoError(t, err)

	gcWorker, err := gcworker.NewMockGCWorker(store)
	require.NoError(t, err)

	// add attributes
	tk.MustExec(`alter table drop_t attributes="key=value";`)
	tk.MustExec(`alter table drop_t partition p0 attributes="key1=value1";`)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)
	// drop table
	tk.MustExec(`drop table drop_t;`)

	err = gcWorker.DeleteRanges(context.Background(), uint64(math.MaxInt64))
	require.NoError(t, err)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 0)

	tk.MustExec("use test")
	tk.MustExec(`create table drop_t (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 0)
}

func TestCreateWithSameName(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	_, err := infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table recreate_t (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)
	failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed")
	}()

	timeBeforeDrop, _, safePointSQL, resetGC := testkit.MockGC(tk)
	defer resetGC()

	// Set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// Set GC enable.
	err = gcutil.EnableGC(tk.Session())
	require.NoError(t, err)

	gcWorker, err := gcworker.NewMockGCWorker(store)
	require.NoError(t, err)

	// add attributes
	tk.MustExec(`alter table recreate_t attributes="key=value";`)
	tk.MustExec(`alter table recreate_t partition p0 attributes="key1=value1";`)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)
	// drop table
	tk.MustExec(`drop table recreate_t;`)

	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 0)

	tk.MustExec(`create table recreate_t (c int)
	PARTITION BY RANGE (c) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11)
	);`)
	// add attributes
	tk.MustExec(`alter table recreate_t attributes="key=value";`)
	tk.MustExec(`alter table recreate_t partition p1 attributes="key1=value1";`)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)

	err = gcWorker.DeleteRanges(context.Background(), uint64(math.MaxInt64))
	require.NoError(t, err)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)

	// drop table
	tk.MustExec(`drop table recreate_t;`)
	err = gcWorker.DeleteRanges(context.Background(), uint64(math.MaxInt64))
	require.NoError(t, err)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 0)
}

func TestPartition(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	_, err := infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table part (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11),
	PARTITION p2 VALUES LESS THAN (20)
);`)
	tk.MustExec(`create table part1 (c int);`)

	// add attributes
	tk.MustExec(`alter table part attributes="key=value";`)
	tk.MustExec(`alter table part partition p0 attributes="key1=value1";`)
	tk.MustExec(`alter table part partition p1 attributes="key2=value2";`)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 3)
	// drop partition
	// partition p0's attribute will be deleted
	tk.MustExec(`alter table part drop partition p0;`)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	require.Equal(t, "schema/test/part", rows1[0][0])
	require.Equal(t, `"key=value"`, rows1[0][2])
	require.Equal(t, rows[0][3], rows1[0][3])
	require.Equal(t, "schema/test/part/p1", rows1[1][0])
	require.Equal(t, `"key2=value2"`, rows1[1][2])
	require.Equal(t, rows[2][3], rows1[1][3])

	// truncate partition
	// partition p1's key range will be updated
	tk.MustExec(`alter table part truncate partition p1;`)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows2, 2)
	require.Equal(t, "schema/test/part", rows2[0][0])
	require.Equal(t, `"key=value"`, rows2[0][2])
	require.NotEqual(t, rows1[0][3], rows2[0][3])
	require.Equal(t, "schema/test/part/p1", rows2[1][0])
	require.Equal(t, `"key2=value2"`, rows2[1][2])
	require.NotEqual(t, rows1[1][3], rows2[1][3])

	// exchange partition
	// partition p1's attribute will be exchanged to table part1
	tk.MustExec(`set @@tidb_enable_exchange_partition=1;`)
	tk.MustExec(`alter table part exchange partition p1 with table part1;`)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows3, 2)
	require.Equal(t, "schema/test/part", rows3[0][0])
	require.Equal(t, `"key=value"`, rows3[0][2])
	require.Equal(t, rows2[0][3], rows3[0][3])
	require.Equal(t, "schema/test/part1", rows3[1][0])
	require.Equal(t, `"key2=value2"`, rows3[1][2])
	require.Equal(t, rows2[1][3], rows3[1][3])
}

func TestDropSchema(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	_, err := infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table drop_s1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)
	tk.MustExec(`create table drop_s2 (c int);`)

	// add attributes
	tk.MustExec(`alter table drop_s1 attributes="key=value";`)
	tk.MustExec(`alter table drop_s1 partition p0 attributes="key1=value1";`)
	tk.MustExec(`alter table drop_s2 attributes="key=value";`)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	require.Len(t, rows, 3)
	// drop database
	tk.MustExec(`drop database test`)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	require.Len(t, rows, 0)
}

func TestDefaultKeyword(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	_, err := infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table def (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add attributes
	tk.MustExec(`alter table def attributes="key=value";`)
	tk.MustExec(`alter table def partition p0 attributes="key1=value1";`)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	require.Len(t, rows, 2)
	// reset the partition p0's attribute
	tk.MustExec(`alter table def partition p0 attributes=default;`)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	require.Len(t, rows, 1)
	// reset the table def's attribute
	tk.MustExec(`alter table def attributes=default;`)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	require.Len(t, rows, 0)
}
