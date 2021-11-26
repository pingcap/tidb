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

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/gcworker"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testAttributesDDLSerialSuite{})

type testAttributesDDLSerialSuite struct{}

func (s *testAttributesDDLSerialSuite) TestAlterTableAttributes(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table alter_t (c int);`)

	// normal cases
	_, err = tk.Exec(`alter table alter_t attributes="merge_option=allow";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table alter_t attributes="merge_option=allow,key=value";`)
	c.Assert(err, IsNil)

	// space cases
	_, err = tk.Exec(`alter table alter_t attributes=" merge_option=allow ";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table alter_t attributes=" merge_option = allow , key = value ";`)
	c.Assert(err, IsNil)

	// without equal
	_, err = tk.Exec(`alter table alter_t attributes " merge_option=allow ";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table alter_t attributes " merge_option=allow , key=value ";`)
	c.Assert(err, IsNil)
}

func (s *testAttributesDDLSerialSuite) TestAlterTablePartitionAttributes(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table alter_p (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11),
	PARTITION p2 VALUES LESS THAN (16),
	PARTITION p3 VALUES LESS THAN (21)
);`)

	// normal cases
	_, err = tk.Exec(`alter table alter_p partition p0 attributes="merge_option=allow";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table alter_p partition p1 attributes="merge_option=allow,key=value";`)
	c.Assert(err, IsNil)

	// space cases
	_, err = tk.Exec(`alter table alter_p partition p2 attributes=" merge_option=allow ";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table alter_p partition p3 attributes=" merge_option = allow , key = value ";`)
	c.Assert(err, IsNil)

	// without equal
	_, err = tk.Exec(`alter table alter_p partition p1 attributes " merge_option=allow ";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table alter_p partition p1 attributes " merge_option=allow , key=value ";`)
	c.Assert(err, IsNil)
}

func (s *testAttributesDDLSerialSuite) TestTruncateTable(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table truncate_t (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add attributes
	_, err = tk.Exec(`alter table truncate_t attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table truncate_t partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// truncate table
	_, err = tk.Exec(`truncate table truncate_t;`)
	c.Assert(err, IsNil)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	// check table truncate_t's attribute
	c.Assert(rows1[0][0], Equals, "schema/test/truncate_t")
	c.Assert(rows1[0][2], Equals, `"key=value"`)
	c.Assert(rows1[0][3], Not(Equals), rows[0][3])
	// check partition p0's attribute
	c.Assert(rows1[1][0], Equals, "schema/test/truncate_t/p0")
	c.Assert(rows1[1][2], Equals, `"key1=value1"`)
	c.Assert(rows1[1][3], Not(Equals), rows[1][3])

	// test only table
	tk.MustExec(`create table truncate_ot (c int);`)

	// add attribute
	_, err = tk.Exec(`alter table truncate_ot attributes="key=value";`)
	c.Assert(err, IsNil)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows2), Equals, 3)
	// truncate table
	_, err = tk.Exec(`truncate table truncate_ot;`)
	c.Assert(err, IsNil)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows3), Equals, 3)
	// check table truncate_ot's attribute
	c.Assert(rows3[0][0], Equals, "schema/test/truncate_ot")
	c.Assert(rows3[0][2], Equals, `"key=value"`)
	c.Assert(rows3[0][3], Not(Equals), rows2[0][3])
}

func (s *testAttributesDDLSerialSuite) TestRenameTable(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table rename_t (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add attributes
	_, err = tk.Exec(`alter table rename_t attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table rename_t partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// rename table
	_, err = tk.Exec(`rename table rename_t to rename_t1;`)
	c.Assert(err, IsNil)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	// check table rename_t1's attribute
	c.Assert(rows1[0][0], Equals, "schema/test/rename_t1")
	c.Assert(rows1[0][2], Equals, `"key=value"`)
	c.Assert(rows1[0][3], Equals, rows[0][3])
	// check partition p0's attribute
	c.Assert(rows1[1][0], Equals, "schema/test/rename_t1/p0")
	c.Assert(rows1[1][2], Equals, `"key1=value1"`)
	c.Assert(rows1[1][3], Equals, rows[1][3])

	// test only table
	tk.MustExec(`create table rename_ot (c int);`)

	// add attribute
	_, err = tk.Exec(`alter table rename_ot attributes="key=value";`)
	c.Assert(err, IsNil)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows2), Equals, 3)
	// rename table
	_, err = tk.Exec(`rename table rename_ot to rename_ot1;`)
	c.Assert(err, IsNil)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows3), Equals, 3)
	// check table rename_ot1's attribute
	c.Assert(rows3[0][0], Equals, "schema/test/rename_ot1")
	c.Assert(rows3[0][2], Equals, `"key=value"`)
	c.Assert(rows3[0][3], Equals, rows2[0][3])
}

func (s *testAttributesDDLSerialSuite) TestRecoverTable(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
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
	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// add attributes
	_, err = tk.Exec(`alter table recover_t attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table recover_t partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// drop table
	_, err = tk.Exec(`drop table recover_t;`)
	c.Assert(err, IsNil)
	// recover table
	_, err = tk.Exec(`recover table recover_t;`)
	c.Assert(err, IsNil)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	// check table recover_t's attribute
	c.Assert(rows1[0][0], Equals, "schema/test/recover_t")
	c.Assert(rows1[0][2], Equals, `"key=value"`)
	c.Assert(rows1[0][3], Equals, rows[0][3])
	// check partition p0's attribute
	c.Assert(rows1[1][0], Equals, "schema/test/recover_t/p0")
	c.Assert(rows1[1][2], Equals, `"key1=value1"`)
	c.Assert(rows1[1][3], Equals, rows[1][3])
}

func (s *testAttributesDDLSerialSuite) TestFlashbackTable(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
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
	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// add attributes
	_, err = tk.Exec(`alter table flash_t attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table flash_t partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// drop table
	_, err = tk.Exec(`drop table flash_t;`)
	c.Assert(err, IsNil)
	// flashback table
	_, err = tk.Exec(`flashback table flash_t to flash_t1;`)
	c.Assert(err, IsNil)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	// check table flash_t1's attribute
	c.Assert(rows1[0][0], Equals, "schema/test/flash_t1")
	c.Assert(rows1[0][2], Equals, `"key=value"`)
	c.Assert(rows1[0][3], Equals, rows[0][3])
	// check partition p0's attribute
	c.Assert(rows1[1][0], Equals, "schema/test/flash_t1/p0")
	c.Assert(rows1[1][2], Equals, `"key1=value1"`)
	c.Assert(rows1[1][3], Equals, rows[1][3])

	// truncate table
	_, err = tk.Exec(`truncate table flash_t1;`)
	c.Assert(err, IsNil)
	// flashback table
	_, err = tk.Exec(`flashback table flash_t1 to flash_t2;`)
	c.Assert(err, IsNil)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	// check table flash_t2's attribute
	c.Assert(rows2[0][0], Equals, "schema/test/flash_t2")
	c.Assert(rows2[0][2], Equals, `"key=value"`)
	c.Assert(rows2[0][3], Equals, rows[0][3])
	// check partition p0's attribute
	c.Assert(rows2[1][0], Equals, "schema/test/flash_t2/p0")
	c.Assert(rows2[1][2], Equals, `"key1=value1"`)
	c.Assert(rows2[1][3], Equals, rows[1][3])
}

func (s *testAttributesDDLSerialSuite) TestDropTable(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
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
	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	gcWorker, err := gcworker.NewMockGCWorker(store)
	c.Assert(err, IsNil)

	// add attributes
	_, err = tk.Exec(`alter table drop_t attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table drop_t partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// drop table
	_, err = tk.Exec(`drop table drop_t;`)
	c.Assert(err, IsNil)

	err = gcWorker.DeleteRanges(context.Background(), uint64(math.MaxInt64))
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testAttributesDDLSerialSuite) TestCreateWithSameName(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
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
	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	gcWorker, err := gcworker.NewMockGCWorker(store)
	c.Assert(err, IsNil)

	// add attributes
	_, err = tk.Exec(`alter table recreate_t attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table recreate_t partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// drop table
	_, err = tk.Exec(`drop table recreate_t;`)
	c.Assert(err, IsNil)

	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)

	tk.MustExec(`create table recreate_t (c int)
	PARTITION BY RANGE (c) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11)
	);`)
	// add attributes
	_, err = tk.Exec(`alter table recreate_t attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table recreate_t partition p1 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 3)

	err = gcWorker.DeleteRanges(context.Background(), uint64(math.MaxInt64))
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)

	// drop table
	_, err = tk.Exec(`drop table recreate_t;`)
	c.Assert(err, IsNil)
	err = gcWorker.DeleteRanges(context.Background(), uint64(math.MaxInt64))
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testAttributesDDLSerialSuite) TestPartition(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table part (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11),
	PARTITION p2 VALUES LESS THAN (20)
);`)
	tk.MustExec(`create table part1 (c int);`)

	// add attributes
	_, err = tk.Exec(`alter table part attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table part partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table part partition p1 attributes="key2=value2";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 3)
	// drop partition
	// partition p0's attribute will be deleted
	_, err = tk.Exec(`alter table part drop partition p0;`)
	c.Assert(err, IsNil)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	c.Assert(rows1[0][0], Equals, "schema/test/part")
	c.Assert(rows1[0][2], Equals, `"key=value"`)
	c.Assert(rows1[0][3], Equals, rows[0][3])
	c.Assert(rows1[1][0], Equals, "schema/test/part/p1")
	c.Assert(rows1[1][2], Equals, `"key2=value2"`)
	c.Assert(rows1[1][3], Equals, rows[2][3])

	// truncate partition
	// partition p1's key range will be updated
	_, err = tk.Exec(`alter table part truncate partition p1;`)
	c.Assert(err, IsNil)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows2), Equals, 2)
	c.Assert(rows2[0][0], Equals, "schema/test/part")
	c.Assert(rows2[0][2], Equals, `"key=value"`)
	c.Assert(rows2[0][3], Not(Equals), rows1[0][3])
	c.Assert(rows2[1][0], Equals, "schema/test/part/p1")
	c.Assert(rows2[1][2], Equals, `"key2=value2"`)
	c.Assert(rows2[1][3], Not(Equals), rows1[1][3])

	// exchange partition
	// partition p1's attribute will be exchanged to table part1
	_, err = tk.Exec(`set @@tidb_enable_exchange_partition=1;`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table part exchange partition p1 with table part1;`)
	c.Assert(err, IsNil)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows3), Equals, 2)
	c.Assert(rows3[0][0], Equals, "schema/test/part")
	c.Assert(rows3[0][2], Equals, `"key=value"`)
	c.Assert(rows3[0][3], Equals, rows2[0][3])
	c.Assert(rows3[1][0], Equals, "schema/test/part1")
	c.Assert(rows3[1][2], Equals, `"key2=value2"`)
	c.Assert(rows3[1][3], Equals, rows2[1][3])
}

func (s *testAttributesDDLSerialSuite) TestDropSchema(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table drop_s1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)
	tk.MustExec(`create table drop_s2 (c int);`)

	// add attributes
	_, err = tk.Exec(`alter table drop_s1 attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table drop_s1 partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table drop_s2 attributes="key=value";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	c.Assert(len(rows), Equals, 3)
	// drop database
	_, err = tk.Exec(`drop database test`)
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testAttributesDDLSerialSuite) TestDefaultKeyword(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table def (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add attributes
	_, err = tk.Exec(`alter table def attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table def partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	c.Assert(len(rows), Equals, 2)
	// reset the partition p0's attribute
	_, err = tk.Exec(`alter table def partition p0 attributes=default;`)
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	c.Assert(len(rows), Equals, 1)
	// reset the table def's attribute
	_, err = tk.Exec(`alter table def attributes=default;`)
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	c.Assert(len(rows), Equals, 0)
}
