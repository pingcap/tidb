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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/stretchr/testify/require"
)

func TestAlterTableAttributes(t *testing.T) {
	t.Parallel()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int);`)

	// normal cases
	_, err = tk.Exec(`alter table t1 attributes="merge_option=allow";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 attributes="merge_option=allow,key=value";`)
	require.NoError(t, err)

	// space cases
	_, err = tk.Exec(`alter table t1 attributes=" merge_option=allow ";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 attributes=" merge_option = allow , key = value ";`)
	require.NoError(t, err)

	// without equal
	_, err = tk.Exec(`alter table t1 attributes " merge_option=allow ";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 attributes " merge_option=allow , key=value ";`)
	require.NoError(t, err)
}

func TestAlterTablePartitionAttributes(t *testing.T) {
	t.Parallel()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11),
	PARTITION p2 VALUES LESS THAN (16),
	PARTITION p3 VALUES LESS THAN (21)
);`)

	// normal cases
	_, err = tk.Exec(`alter table t1 partition p0 attributes="merge_option=allow";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 partition p1 attributes="merge_option=allow,key=value";`)
	require.NoError(t, err)

	// space cases
	_, err = tk.Exec(`alter table t1 partition p2 attributes=" merge_option=allow ";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 partition p3 attributes=" merge_option = allow , key = value ";`)
	require.NoError(t, err)

	// without equal
	_, err = tk.Exec(`alter table t1 partition p1 attributes " merge_option=allow ";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 partition p1 attributes " merge_option=allow , key=value ";`)
	require.NoError(t, err)
}

func TestTruncateTable(t *testing.T) {
	t.Parallel()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	require.NoError(t, err)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)
	// truncate table
	_, err = tk.Exec(`truncate table t1;`)
	require.NoError(t, err)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	// check table t1's attribute
	require.Equal(t, "schema/test/t1", rows1[0][0])
	require.Equal(t, `"key=value"`, rows1[0][2])
	require.NotEqual(t, rows[0][3], rows1[0][3])
	// check partition p0's attribute
	require.Equal(t, "schema/test/t1/p0", rows1[1][0])
	require.Equal(t, `"key1=value1"`, rows1[1][2])
	require.NotEqual(t, rows[1][3], rows1[1][3])

	// test only table
	tk.MustExec(`create table t2 (c int);`)

	// add attribute
	_, err = tk.Exec(`alter table t2 attributes="key=value";`)
	require.NoError(t, err)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows2, 3)
	// truncate table
	_, err = tk.Exec(`truncate table t2;`)
	require.NoError(t, err)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows3, 3)
	// check table t1's attribute
	require.Equal(t, "schema/test/t2", rows3[2][0])
	require.Equal(t, `"key=value"`, rows3[2][2])
	require.NotEqual(t, rows2[2][3], rows3[2][3])
}

func TestRenameTable(t *testing.T) {
	t.Parallel()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	require.NoError(t, err)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)
	// rename table
	_, err = tk.Exec(`rename table t1 to t2;`)
	require.NoError(t, err)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	// check table t2's attribute
	require.Equal(t, "schema/test/t2", rows1[0][0])
	require.Equal(t, `"key=value"`, rows1[0][2])
	require.Equal(t, rows[0][3], rows1[0][3])
	// check partition p0's attribute
	require.Equal(t, "schema/test/t2/p0", rows1[1][0])
	require.Equal(t, `"key1=value1"`, rows1[1][2])
	require.Equal(t, rows[1][3], rows1[1][3])

	// test only table
	tk.MustExec(`create table t3 (c int);`)

	// add attribute
	_, err = tk.Exec(`alter table t3 attributes="key=value";`)
	require.NoError(t, err)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows2, 3)
	// rename table
	_, err = tk.Exec(`rename table t3 to t4;`)
	require.NoError(t, err)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows3, 3)
	// check table t4's attribute
	require.Equal(t, "schema/test/t4", rows3[2][0])
	require.Equal(t, `"key=value"`, rows3[2][2])
	require.Equal(t, rows2[2][3], rows3[2][3])
}

func TestRecoverTable(t *testing.T) {
	t.Parallel()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
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

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	require.NoError(t, err)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)
	// drop table
	_, err = tk.Exec(`drop table t1;`)
	require.NoError(t, err)
	// recover table
	_, err = tk.Exec(`recover table t1;`)
	require.NoError(t, err)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	// check table t1's attribute
	require.Equal(t, "schema/test/t1", rows1[0][0])
	require.Equal(t, `"key=value"`, rows1[0][2])
	require.Equal(t, rows[0][3], rows1[0][3])
	// check partition p0's attribute
	require.Equal(t, "schema/test/t1/p0", rows1[1][0])
	require.Equal(t, `"key1=value1"`, rows1[1][2])
	require.Equal(t, rows[1][3], rows1[1][3])
}

func TestFlashbackTable(t *testing.T) {
	t.Parallel()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
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

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	require.NoError(t, err)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 2)
	// drop table
	_, err = tk.Exec(`drop table t1;`)
	require.NoError(t, err)
	// flashback table
	_, err = tk.Exec(`flashback table t1 to t2;`)
	require.NoError(t, err)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	// check table t2's attribute
	require.Equal(t, "schema/test/t2", rows1[0][0])
	require.Equal(t, `"key=value"`, rows1[0][2])
	require.Equal(t, rows[0][3], rows1[0][3])
	// check partition p0's attribute
	require.Equal(t, "schema/test/t2/p0", rows1[1][0])
	require.Equal(t, `"key1=value1"`, rows1[1][2])
	require.Equal(t, rows[1][3], rows1[1][3])

	// truncate table
	_, err = tk.Exec(`truncate table t2;`)
	require.NoError(t, err)
	// flashback table
	_, err = tk.Exec(`flashback table t2 to t3;`)
	require.NoError(t, err)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	// check table t3's attribute
	require.Equal(t, "schema/test/t3", rows2[0][0])
	require.Equal(t, `"key=value"`, rows2[0][2])
	require.Equal(t, rows[0][3], rows2[0][3])
	// check partition p0's attribute
	require.Equal(t, "schema/test/t3/p0", rows2[1][0])
	require.Equal(t, `"key1=value1"`, rows2[1][2])
	require.Equal(t, rows[1][3], rows2[1][3])
}

func TestPartition(t *testing.T) {
	t.Parallel()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11),
	PARTITION p2 VALUES LESS THAN (20)
);`)
	tk.MustExec(`create table t2 (c int);`)

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 partition p1 attributes="key2=value2";`)
	require.NoError(t, err)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows, 3)
	// drop partition
	// partition p0's attribute will be deleted
	_, err = tk.Exec(`alter table t1 drop partition p0;`)
	require.NoError(t, err)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows1, 2)
	require.Equal(t, "schema/test/t1", rows1[0][0])
	require.Equal(t, `"key=value"`, rows1[0][2])
	require.Equal(t, rows[0][3], rows1[0][3])
	require.Equal(t, "schema/test/t1/p1", rows1[1][0])
	require.Equal(t, `"key2=value2"`, rows1[1][2])
	require.Equal(t, rows[2][3], rows1[1][3])

	// truncate partition
	// partition p1's key range will be updated
	_, err = tk.Exec(`alter table t1 truncate partition p1;`)
	require.NoError(t, err)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows2, 2)
	require.Equal(t, "schema/test/t1", rows2[0][0])
	require.Equal(t, `"key=value"`, rows2[0][2])
	require.NotEqual(t, rows1[0][3], rows2[0][3])
	require.Equal(t, "schema/test/t1/p1", rows2[1][0])
	require.Equal(t, `"key2=value2"`, rows2[1][2])
	require.NotEqual(t, rows1[1][3], rows2[1][3])

	// exchange partition
	// partition p1's attribute will be exchanged to table t2
	_, err = tk.Exec(`set @@tidb_enable_exchange_partition=1;`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 exchange partition p1 with table t2;`)
	require.NoError(t, err)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	require.Len(t, rows3, 2)
	require.Equal(t, "schema/test/t1", rows3[0][0])
	require.Equal(t, `"key=value"`, rows3[0][2])
	require.Equal(t, rows2[0][3], rows3[0][3])
	require.Equal(t, "schema/test/t2", rows3[1][0])
	require.Equal(t, `"key2=value2"`, rows3[1][2])
	require.Equal(t, rows2[1][3], rows2[1][3])
}

func TestDefaultKeyword(t *testing.T) {
	t.Parallel()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	require.NoError(t, err)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	require.NoError(t, err)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	require.Len(t, rows, 2)
	// reset the partition p0's attribute
	_, err = tk.Exec(`alter table t1 partition p0 attributes=default;`)
	require.NoError(t, err)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	require.Len(t, rows, 1)
	// reset the table t1's attribute
	_, err = tk.Exec(`alter table t1 attributes=default;`)
	require.NoError(t, err)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	require.Len(t, rows, 0)
}
