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
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/gcworker"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testDBSuite8) TestAlterTableAttributes(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int);`)

	// normal cases
	_, err = tk.Exec(`alter table t1 attributes="merge_option=allow";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 attributes="merge_option=allow,key=value";`)
	c.Assert(err, IsNil)

	// space cases
	_, err = tk.Exec(`alter table t1 attributes=" merge_option=allow ";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 attributes=" merge_option = allow , key = value ";`)
	c.Assert(err, IsNil)

	// without equal
	_, err = tk.Exec(`alter table t1 attributes " merge_option=allow ";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 attributes " merge_option=allow , key=value ";`)
	c.Assert(err, IsNil)
}

func (s *testDBSuite8) TestAlterTablePartitionAttributes(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
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

	// reset all
	tk.MustExec(`alter table alter_p partition p0 attributes default;`)
	tk.MustExec(`alter table alter_p partition p1 attributes default;`)
	tk.MustExec(`alter table alter_p partition p2 attributes default;`)
	tk.MustExec(`alter table alter_p partition p3 attributes default;`)

	// add table level attribute
	tk.MustExec(`alter table alter_p attributes="merge_option=deny";`)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 1)

	// add a new partition p4
	tk.MustExec(`alter table alter_p add partition (PARTITION p4 VALUES LESS THAN (60));`)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 1)
	c.Assert(rows[0][3], Not(Equals), rows1[0][3])

	// drop the new partition p4
	tk.MustExec(`alter table alter_p drop partition p4;`)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows2), Equals, 1)
	c.Assert(rows[0][3], Equals, rows2[0][3])

	// add a new partition p5
	tk.MustExec(`alter table alter_p add partition (PARTITION p5 VALUES LESS THAN (80));`)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows3), Equals, 1)
	c.Assert(rows[0][3], Not(Equals), rows3[0][3])

	// truncate the new partition p5
	tk.MustExec(`alter table alter_p truncate partition p5;`)
	rows4 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows4), Equals, 1)
	c.Assert(rows3[0][3], Not(Equals), rows4[0][3])
	c.Assert(rows[0][3], Not(Equals), rows4[0][3])
}

func (s *testDBSuite8) TestTruncateTable(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// truncate table
	_, err = tk.Exec(`truncate table t1;`)
	c.Assert(err, IsNil)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	// check table t1's attribute
	c.Assert(rows1[0][0], Equals, "schema/test/t1")
	c.Assert(rows1[0][2], Equals, `"key=value"`)
	c.Assert(rows1[0][3], Not(Equals), rows[0][3])
	// check partition p0's attribute
	c.Assert(rows1[1][0], Equals, "schema/test/t1/p0")
	c.Assert(rows1[1][2], Equals, `"key1=value1"`)
	c.Assert(rows1[1][3], Not(Equals), rows[1][3])

	// test only table
	tk.MustExec(`create table t2 (c int);`)

	// add attribute
	_, err = tk.Exec(`alter table t2 attributes="key=value";`)
	c.Assert(err, IsNil)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows2), Equals, 3)
	// truncate table
	_, err = tk.Exec(`truncate table t2;`)
	c.Assert(err, IsNil)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows3), Equals, 3)
	// check table t1's attribute
	c.Assert(rows3[2][0], Equals, "schema/test/t2")
	c.Assert(rows3[2][2], Equals, `"key=value"`)
	c.Assert(rows3[2][3], Not(Equals), rows2[2][3])
}

func (s *testDBSuite8) TestRenameTable(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// rename table
	_, err = tk.Exec(`rename table t1 to t2;`)
	c.Assert(err, IsNil)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	// check table t2's attribute
	c.Assert(rows1[0][0], Equals, "schema/test/t2")
	c.Assert(rows1[0][2], Equals, `"key=value"`)
	c.Assert(rows1[0][3], Equals, rows[0][3])
	// check partition p0's attribute
	c.Assert(rows1[1][0], Equals, "schema/test/t2/p0")
	c.Assert(rows1[1][2], Equals, `"key1=value1"`)
	c.Assert(rows1[1][3], Equals, rows[1][3])

	// test only table
	tk.MustExec(`create table t3 (c int);`)

	// add attribute
	_, err = tk.Exec(`alter table t3 attributes="key=value";`)
	c.Assert(err, IsNil)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows2), Equals, 3)
	// rename table
	_, err = tk.Exec(`rename table t3 to t4;`)
	c.Assert(err, IsNil)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows3), Equals, 3)
	// check table t4's attribute
	c.Assert(rows3[2][0], Equals, "schema/test/t4")
	c.Assert(rows3[2][2], Equals, `"key=value"`)
	c.Assert(rows3[2][3], Equals, rows2[2][3])
}

func (s *testDBSuite8) TestRecoverTable(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
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
	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// drop table
	_, err = tk.Exec(`drop table t1;`)
	c.Assert(err, IsNil)
	// recover table
	_, err = tk.Exec(`recover table t1;`)
	c.Assert(err, IsNil)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	// check table t1's attribute
	c.Assert(rows1[0][0], Equals, "schema/test/t1")
	c.Assert(rows1[0][2], Equals, `"key=value"`)
	c.Assert(rows1[0][3], Equals, rows[0][3])
	// check partition p0's attribute
	c.Assert(rows1[1][0], Equals, "schema/test/t1/p0")
	c.Assert(rows1[1][2], Equals, `"key1=value1"`)
	c.Assert(rows1[1][3], Equals, rows[1][3])
}

func (s *testDBSuite8) TestFlashbackTable(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
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
	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// drop table
	_, err = tk.Exec(`drop table t1;`)
	c.Assert(err, IsNil)
	// flashback table
	_, err = tk.Exec(`flashback table t1 to t2;`)
	c.Assert(err, IsNil)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	// check table t2's attribute
	c.Assert(rows1[0][0], Equals, "schema/test/t2")
	c.Assert(rows1[0][2], Equals, `"key=value"`)
	c.Assert(rows1[0][3], Equals, rows[0][3])
	// check partition p0's attribute
	c.Assert(rows1[1][0], Equals, "schema/test/t2/p0")
	c.Assert(rows1[1][2], Equals, `"key1=value1"`)
	c.Assert(rows1[1][3], Equals, rows[1][3])

	// truncate table
	_, err = tk.Exec(`truncate table t2;`)
	c.Assert(err, IsNil)
	// flashback table
	_, err = tk.Exec(`flashback table t2 to t3;`)
	c.Assert(err, IsNil)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	// check table t3's attribute
	c.Assert(rows2[0][0], Equals, "schema/test/t3")
	c.Assert(rows2[0][2], Equals, `"key=value"`)
	c.Assert(rows2[0][3], Equals, rows[0][3])
	// check partition p0's attribute
	c.Assert(rows2[1][0], Equals, "schema/test/t3/p0")
	c.Assert(rows2[1][2], Equals, `"key1=value1"`)
	c.Assert(rows2[1][3], Equals, rows[1][3])
}

func (s *testDBSuite8) TestDropTable(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
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

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// drop table
	_, err = tk.Exec(`drop table t1;`)
	c.Assert(err, IsNil)

	err = gcWorker.DeleteRanges(context.Background(), uint64(math.MaxInt64))
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testDBSuite8) TestCreateWithSameName(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
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

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// drop table
	_, err = tk.Exec(`drop table t1;`)
	c.Assert(err, IsNil)

	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)

	tk.MustExec(`create table t1 (c int)
	PARTITION BY RANGE (c) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11)
	);`)
	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 partition p1 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 3)

	err = gcWorker.DeleteRanges(context.Background(), uint64(math.MaxInt64))
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 2)

	// drop table
	_, err = tk.Exec(`drop table t1;`)
	c.Assert(err, IsNil)
	err = gcWorker.DeleteRanges(context.Background(), uint64(math.MaxInt64))
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testDBSuite8) TestPartition(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
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
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 partition p1 attributes="key2=value2";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows), Equals, 3)
	// drop partition
	// partition p0's attribute will be deleted
	_, err = tk.Exec(`alter table t1 drop partition p0;`)
	c.Assert(err, IsNil)
	rows1 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows1), Equals, 2)
	c.Assert(rows1[0][0], Equals, "schema/test/t1")
	c.Assert(rows1[0][2], Equals, `"key=value"`)
	c.Assert(rows1[0][3], Not(Equals), rows[0][3])
	c.Assert(rows1[1][0], Equals, "schema/test/t1/p1")
	c.Assert(rows1[1][2], Equals, `"key2=value2"`)
	c.Assert(rows1[1][3], Equals, rows[2][3])

	// truncate partition
	// partition p1's key range will be updated
	_, err = tk.Exec(`alter table t1 truncate partition p1;`)
	c.Assert(err, IsNil)
	rows2 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows2), Equals, 2)
	c.Assert(rows2[0][0], Equals, "schema/test/t1")
	c.Assert(rows2[0][2], Equals, `"key=value"`)
	c.Assert(rows2[0][3], Not(Equals), rows1[0][3])
	c.Assert(rows2[1][0], Equals, "schema/test/t1/p1")
	c.Assert(rows2[1][2], Equals, `"key2=value2"`)
	c.Assert(rows2[1][3], Not(Equals), rows1[1][3])

	// exchange partition
	// partition p1's attribute will be exchanged to table t2
	_, err = tk.Exec(`set @@tidb_enable_exchange_partition=1;`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 exchange partition p1 with table t2;`)
	c.Assert(err, IsNil)
	rows3 := tk.MustQuery(`select * from information_schema.attributes;`).Sort().Rows()
	c.Assert(len(rows3), Equals, 2)
	c.Assert(rows3[0][0], Equals, "schema/test/t1")
	c.Assert(rows3[0][2], Equals, `"key=value"`)
	c.Assert(rows3[0][3], Equals, rows2[0][3])
	c.Assert(rows3[1][0], Equals, "schema/test/t2")
	c.Assert(rows3[1][2], Equals, `"key2=value2"`)
	c.Assert(rows3[1][3], Equals, rows2[1][3])
}

func (s *testDBSuite8) TestDropSchema(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)
	tk.MustExec(`create table t2 (c int);`)

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t2 attributes="key=value";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	c.Assert(len(rows), Equals, 3)
	// drop database
	_, err = tk.Exec(`drop database test`)
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testDBSuite8) TestDefaultKeyword(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	// add rules
	_, err = tk.Exec(`alter table t1 attributes="key=value";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 partition p0 attributes="key1=value1";`)
	c.Assert(err, IsNil)
	rows := tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	c.Assert(len(rows), Equals, 2)
	// reset the partition p0's attribute
	_, err = tk.Exec(`alter table t1 partition p0 attributes=default;`)
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	c.Assert(len(rows), Equals, 1)
	// reset the table t1's attribute
	_, err = tk.Exec(`alter table t1 attributes=default;`)
	c.Assert(err, IsNil)
	rows = tk.MustQuery(`select * from information_schema.attributes;`).Rows()
	c.Assert(len(rows), Equals, 0)
}
