// Copyright 2019 PingCAP, Inc.
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
	"strconv"
	"testing"
	"time"

	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestCreateSequence(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop sequence if exists seq")
	tk.MustGetErrCode("create sequence `seq  `", mysql.ErrWrongTableName)

	// increment should not be set as 0.
	tk.MustGetErrCode("create sequence seq increment 0", mysql.ErrSequenceInvalidData)

	// maxvalue should be larger than minvalue.
	tk.MustGetErrCode("create sequence seq maxvalue 1 minvalue 2", mysql.ErrSequenceInvalidData)

	// maxvalue should be larger than minvalue.
	tk.MustGetErrCode("create sequence seq maxvalue 1 minvalue 1", mysql.ErrSequenceInvalidData)

	// maxvalue shouldn't be equal to MaxInt64.
	tk.MustGetErrCode("create sequence seq maxvalue 9223372036854775807 minvalue 1", mysql.ErrSequenceInvalidData)

	// TODO : minvalue shouldn't be equal to MinInt64.

	// maxvalue should be larger than start.
	tk.MustGetErrCode("create sequence seq maxvalue 1 start with 2", mysql.ErrSequenceInvalidData)

	// cacheVal should be less than (math.MaxInt64-maxIncrement)/maxIncrement.
	tk.MustGetErrCode("create sequence seq increment 100000 cache 922337203685477", mysql.ErrSequenceInvalidData)

	// test unsupported table option in sequence.
	tk.MustGetErrCode("create sequence seq CHARSET=utf8", mysql.ErrSequenceUnsupportedTableOption)

	_, err := tk.Exec("create sequence seq comment=\"test\"")
	require.NoError(t, err)

	sequenceTable := external.GetTableByName(t, tk, "test", "seq")

	require.Equal(t, true, sequenceTable.Meta().IsSequence())
	require.Equal(t, model.DefaultSequenceIncrementValue, sequenceTable.Meta().Sequence.Increment)
	require.Equal(t, model.DefaultPositiveSequenceStartValue, sequenceTable.Meta().Sequence.Start)
	require.Equal(t, model.DefaultPositiveSequenceMinValue, sequenceTable.Meta().Sequence.MinValue)
	require.Equal(t, model.DefaultPositiveSequenceMaxValue, sequenceTable.Meta().Sequence.MaxValue)
	require.Equal(t, true, sequenceTable.Meta().Sequence.Cache)
	require.Equal(t, model.DefaultSequenceCacheValue, sequenceTable.Meta().Sequence.CacheValue)
	require.Equal(t, false, sequenceTable.Meta().Sequence.Cycle)

	// Test create privilege.
	tk.MustExec("drop user if exists myuser@localhost")
	tk.MustExec("create user myuser@localhost")

	tk1 := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil))
	tk1.SetSession(se)

	// grant the myuser the access to database test.
	tk.MustExec("grant select on test.* to 'myuser'@'localhost'")

	tk1.MustExec("use test")
	_, err = tk1.Exec("create sequence my_seq")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1142]CREATE command denied to user 'myuser'@'localhost' for table 'my_seq'")
}

// Test for sequence still works with a infoschema attached by temporary table
func TestIssue28881(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop sequence if exists s")
	tk.MustExec("create sequence s")
	defer tk.MustExec("drop sequence s")
	tk.MustExec("create temporary table tmp1 (id int)")

	tk.MustQuery("select nextval(s)").Check(testkit.Rows("1"))
	tk.MustQuery("select lastval(s)").Check(testkit.Rows("1"))
}

func TestDropSequence(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop sequence if exists seq")

	// Test sequence is unknown.
	tk.MustGetErrCode("drop sequence seq", mysql.ErrUnknownSequence)

	// Test non-existed sequence can't drop successfully.
	tk.MustExec("create sequence seq")
	_, err := tk.Exec("drop sequence seq, seq2")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:4139]Unknown SEQUENCE: 'test.seq2'")

	// Test the specified object is not sequence.
	tk.MustExec("create table seq3 (a int)")
	_, err = tk.Exec("drop sequence seq3")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, dbterror.ErrWrongObject))

	// Test schema is not exist.
	_, err = tk.Exec("drop sequence unknown.seq")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:4139]Unknown SEQUENCE: 'unknown.seq'")

	// Test drop sequence successfully.
	tk.MustExec("create sequence seq")
	_, err = tk.Exec("drop sequence seq")
	require.NoError(t, err)
	_, err = tk.Exec("drop sequence seq")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:4139]Unknown SEQUENCE: 'test.seq'")

	// Test drop table when the object is a sequence.
	tk.MustExec("create sequence seq")
	_, err = tk.Exec("drop table seq")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1051]Unknown table 'test.seq'")

	// Test drop view when the object is a sequence.
	_, err = tk.Exec("drop view seq")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, dbterror.ErrWrongObject))
	tk.MustExec("drop sequence seq")

	// Test drop privilege.
	tk.MustExec("drop user if exists myuser@localhost")
	tk.MustExec("create user myuser@localhost")

	tk1 := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil))
	tk1.SetSession(se)

	// grant the myuser the access to database test.
	tk.MustExec("create sequence my_seq")
	tk.MustExec("grant select on test.* to 'myuser'@'localhost'")

	tk1.MustExec("use test")
	_, err = tk1.Exec("drop sequence my_seq")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1142]DROP command denied to user 'myuser'@'localhost' for table 'my_seq'")

	// Test for `drop sequence if exists`.
	tk.MustExec("drop sequence if exists seq_if_exists")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Note 4139 Unknown SEQUENCE: 'test.seq_if_exists'"))
}

func TestShowCreateSequence(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create table t(a int)")
	tk.MustExec("create sequence seq")

	// Test show privilege.
	tk.MustExec("drop user if exists myuser@localhost")
	tk.MustExec("create user myuser@localhost")

	tk1 := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil))
	tk1.SetSession(se)

	// Grant the myuser the access to table t in database test, but sequence seq.
	tk.MustExec("grant select on test.t to 'myuser'@'localhost'")

	tk1.MustExec("use test")
	tk1.MustExec("show create table t")
	_, err = tk1.Exec("show create sequence seq")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1142]SHOW command denied to user 'myuser'@'localhost' for table 'seq'")

	// Grant the myuser the access to sequence seq in database test.
	tk.MustExec("grant select on test.seq to 'myuser'@'localhost'")

	tk1.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	// Test show sequence detail.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq start 10")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 10 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq minvalue 0")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 0 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq maxvalue 100")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 100 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = -2")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with -1 minvalue -9223372036854775807 maxvalue -1 increment by -2 cache 1000 nocycle ENGINE=InnoDB"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq nocache")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 nocache nocycle ENGINE=InnoDB"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq cycle")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 cycle ENGINE=InnoDB"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq comment=\"ccc\"")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB COMMENT='ccc'"))

	// Test show create sequence with a normal table.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create table seq (a int)")
	err = tk.QueryToErr("show create sequence seq")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:1347]'test.seq' is not SEQUENCE")
	tk.MustExec("drop table if exists seq")

	// Test use the show create sequence result to create sequence.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	showString := tk.MustQuery("show create sequence seq").Rows()[0][1].(string)
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec(showString)
}

func TestSequenceAsDefaultValue(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")

	// test the use sequence's nextval as default.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null default next value for seq key)")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null default nextval(seq), b int, primary key(a))")

	tk.MustExec("create table t1 (a int default next value for seq)")
	tk.MustGetErrMsg("create table t2 (a char(1) default next value for seq)", "[ddl:8228]Unsupported sequence default value for column type 'a'")

	tk.MustExec("create table t3 (a int default nextval(seq))")

	tk.MustExec("create table t4 (a int)")
	tk.MustExec("alter table t4 alter column a set default (next value for seq)")
	tk.MustExec("alter table t4 alter column a set default (nextval(seq))")

	tk.MustExec("create table t5 (a char(1))")
	tk.MustGetErrMsg("alter table t5 alter column a set default (next value for seq)", "[ddl:8228]Unsupported sequence default value for column type 'a'")

	tk.MustGetErrMsg("alter table t5 alter column a set default (nextval(seq))", "[ddl:8228]Unsupported sequence default value for column type 'a'")

	// Specially, the new added column with sequence as it's default value is forbade.
	// But alter table column with sequence as it's default value is allowed.
	tk.MustGetErrMsg("alter table t5 add column c int default next value for seq", "[ddl:8230]Unsupported using sequence as default value in add column 'c'")

	tk.MustExec("alter table t5 add column c int default -1")
	// Alter with modify.
	tk.MustExec("alter table t5 modify column c int default next value for seq")
	// Alter with alter.
	tk.MustExec("alter table t5 alter column c set default (next value for seq)")
	// Alter with change.
	tk.MustExec("alter table t5 change column c c int default next value for seq")
}

func TestSequenceFunction(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop sequence if exists seq1")
	tk.MustExec("create sequence seq")

	// test normal sequence function.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(test.seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select next value for seq").Check(testkit.Rows("3"))
	tk.MustQuery("select next value for test.seq").Check(testkit.Rows("4"))

	// test sequence function error.
	tk.MustGetErrMsg("select nextval(seq1)", "[schema:1146]Table 'test.seq1' doesn't exist")
	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustQuery("select nextval(test.seq)").Check(testkit.Rows("5"))
	tk.MustQuery("select next value for test.seq").Check(testkit.Rows("6"))
	tk.MustGetErrMsg("select nextval(seq)", "[schema:1146]Table 'test2.seq' doesn't exist")
	tk.MustGetErrMsg("select next value for seq", "[schema:1146]Table 'test2.seq' doesn't exist")
	tk.MustExec("use test")

	// test sequence nocache.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq nocache")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))

	// test sequence option logic.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 5")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 5 start = 3")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("13"))

	// minvalue should be specified lower than start (negative here), default 1 when increment > 0.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq minvalue -5 start = -2 increment = 5")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))

	// test sequence cycle.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 5 start = 3 maxvalue = 12 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))

	// test sequence maxvalue allocation.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 4 start = 2 maxvalue = 10 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))

	// test sequence has run out.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 5 start = 3 maxvalue = 12 nocycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	err := tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 3 start = 3 maxvalue = 9 nocycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")

	// test negative-growth sequence
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = -2 start = 3 minvalue -5 maxvalue = 12 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("12"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = -3 start = 2 minvalue -6 maxvalue = 11 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-4"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = -4 start = 6 minvalue -6 maxvalue = 11")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-6"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = -3 start = 2 minvalue -2 maxvalue 10")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")

	// test sequence setval function.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	// set value to a used value, will get NULL.
	tk.MustQuery("select setval(seq, 2)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	// set value to a unused value, will get itself.
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	// the next value will not be base on next value.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment 3 maxvalue 11")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("4"))
	tk.MustQuery("select setval(seq, 3)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, 4)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))
	tk.MustQuery("select setval(seq, 8)").Check(testkit.Rows("8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")
	tk.MustQuery("select setval(seq, 11)").Check(testkit.Rows("11"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")
	// set value can be bigger than maxvalue.
	tk.MustQuery("select setval(seq, 100)").Check(testkit.Rows("100"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")

	// test setval in second cache round.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment 10 start 5 maxvalue 100 cache 10 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("15"))
	tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("20"))
	// the next value will not be base on next value.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("25"))
	sequenceTable := external.GetTableByName(t, tk, "test", "seq")
	tc, ok := sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round := tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(95), end)
	require.Equal(t, int64(0), round)
	// exhausted the sequence first round in cycle.
	tk.MustQuery("select setval(seq, 95)").Check(testkit.Rows("95"))
	// make sequence alloc the next batch.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(91), end)
	require.Equal(t, int64(1), round)
	tk.MustQuery("select setval(seq, 15)").Check(testkit.Rows("15"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("21"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("31"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment 2 start 0 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select setval(seq, -20)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("20"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-10"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(-6), end)
	require.Equal(t, int64(1), round)

	// test setval in negative-growth sequence.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment -3 start 5 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(-1), end)
	require.Equal(t, int64(0), round)
	// exhausted the sequence first cache batch.
	tk.MustQuery("select setval(seq, -2)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-4"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(-10), end)
	require.Equal(t, int64(0), round)
	// exhausted the sequence second cache batch.
	tk.MustQuery("select setval(seq, -10)").Check(testkit.Rows("-10"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(4), end)
	require.Equal(t, int64(1), round)
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("4"))
	// test the sequence negative rebase.
	tk.MustQuery("select setval(seq, 0)").Check(testkit.Rows("0"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment -2 start 0 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, -20)").Check(testkit.Rows("-20"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(6), end)
	require.Equal(t, int64(1), round)

	// test sequence lastval function.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select next value for seq").Check(testkit.Rows("2"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	// setval won't change the last value.
	tk.MustQuery("select setval(seq, -1)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))

	// test lastval in positive-growth sequence cycle and cache.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment 3 start 3 maxvalue 14 cache 3 cycle")
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(9), end)
	require.Equal(t, int64(0), round)
	// invalidate the current sequence cache.
	tk.MustQuery("select setval(seq, 10)").Check(testkit.Rows("10"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("3"))
	// trigger the next sequence cache.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("12"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(14), end)
	require.Equal(t, int64(0), round)
	// invalidate the current sequence cache.
	tk.MustQuery("select setval(seq, 13)").Check(testkit.Rows("13"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("12"))
	// trigger the next sequence cache.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(7), end)
	require.Equal(t, int64(1), round)
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("1"))

	// test lastval in negative-growth sequence cycle and cache.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment -3 start -2 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(-8), end)
	require.Equal(t, int64(0), round)
	// invalidate the current sequence cache.
	tk.MustQuery("select setval(seq, -8)").Check(testkit.Rows("-8"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(4), end)
	require.Equal(t, int64(1), round)
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("10"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment -1 start 1 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select setval(seq, -8)").Check(testkit.Rows("-8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(-10), end)
	require.Equal(t, int64(0), round)

	// Test the sequence seek formula will overflow Int64.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment 2 start -9223372036854775807 maxvalue 9223372036854775806 minvalue -9223372036854775807 cache 2 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9223372036854775807"))
	tk.MustQuery("select setval(seq, 9223372036854775800)").Check(testkit.Rows("9223372036854775800"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9223372036854775801"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment -2 start 9223372036854775806 maxvalue 9223372036854775806 minvalue -9223372036854775807 cache 2 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9223372036854775806"))
	tk.MustQuery("select setval(seq, -9223372036854775800)").Check(testkit.Rows("-9223372036854775800"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9223372036854775802"))

	// Test sequence function with wrong object name.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop table if exists seq")
	tk.MustExec("drop view if exists seq")
	tk.MustExec("drop sequence if exists seq1")
	tk.MustExec("drop table if exists seq1")
	tk.MustExec("drop view if exists seq1")
	tk.MustExec("create table seq(a int)")
	_, err = tk.Exec("select nextval(seq)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq' is not SEQUENCE")
	_, err = tk.Exec("select lastval(seq)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq' is not SEQUENCE")
	_, err = tk.Exec("select setval(seq, 10)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq' is not SEQUENCE")

	tk.MustExec("create view seq1 as select * from seq")
	_, err = tk.Exec("select nextval(seq1)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq1' is not SEQUENCE")
	_, err = tk.Exec("select lastval(seq1)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq1' is not SEQUENCE")
	_, err = tk.Exec("select setval(seq1, 10)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq1' is not SEQUENCE")
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop table if exists seq")
	tk.MustExec("drop view if exists seq")
	tk.MustExec("drop sequence if exists seq1")
	tk.MustExec("drop table if exists seq1")
	tk.MustExec("drop view if exists seq1")

	// test a bug found in ticase.
	tk.MustExec("create sequence seq")
	tk.MustQuery("select setval(seq, 10)").Check(testkit.Rows("10"))
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("<nil>"))
	tk.MustExec("drop sequence seq")
	tk.MustExec("create sequence seq increment=-1")
	tk.MustQuery("select setval(seq, -10)").Check(testkit.Rows("-10"))
	tk.MustQuery("select setval(seq, -5)").Check(testkit.Rows("<nil>"))
	tk.MustExec("drop sequence seq")

	// test the current value already satisfied setval in other session.
	tk.MustExec("create sequence seq")
	tk.MustQuery("select setval(seq, 100)").Check(testkit.Rows("100"))
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tk1 := testkit.NewTestKit(t, store)
	tk1.SetSession(se)
	tk1.MustExec("use test")
	tk1.MustQuery("select setval(seq, 50)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select nextval(seq)").Check(testkit.Rows("101"))
	tk1.MustQuery("select setval(seq, 100)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, 101)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, 102)").Check(testkit.Rows("102"))
	tk.MustExec("drop sequence seq")

	tk.MustExec("create sequence seq increment=-1")
	tk.MustQuery("select setval(seq, -100)").Check(testkit.Rows("-100"))
	tk1.MustQuery("select setval(seq, -50)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select nextval(seq)").Check(testkit.Rows("-101"))
	tk1.MustQuery("select setval(seq, -100)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, -101)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, -102)").Check(testkit.Rows("-102"))
	tk.MustExec("drop sequence seq")

	// test the sequence name preprocess.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create sequence seq")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1),(2)")
	tk.MustQuery("select nextval(seq), t.a from t").Check(testkit.Rows("1 1", "2 2"))
	_, err = tk.Exec("select nextval(t), t.a from t")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.t' is not SEQUENCE")
	_, err = tk.Exec("select nextval(seq), nextval(t), t.a from t")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.t' is not SEQUENCE")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustExec("drop sequence seq")
	tk.MustExec("drop table t")
}

func TestInsertSequence(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop table if exists t")

	// test insert with sequence default value.
	tk.MustExec("create sequence seq")
	tk.MustExec("create table t (a int default next value for seq)")
	tk.MustExec("insert into t values()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(),(),()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(-1),(default),(-1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1", "5", "-1"))

	// test insert with specified sequence value rather than default.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values(next value for seq)")
	tk.MustQuery("select * from t").Check(testkit.Rows("6"))
	tk.MustExec("insert into t values(next value for seq),(nextval(seq))")
	tk.MustQuery("select * from t").Check(testkit.Rows("6", "7", "8"))

	// test insert with sequence expression.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(next value for seq + 1),(nextval(seq) * 2)")
	tk.MustQuery("select * from t").Check(testkit.Rows("10", "20"))
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values((next value for seq - 1) / 2)")
	tk.MustQuery("select * from t").Check(testkit.Rows("5"))

	// test insert with user specified value.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(-1),(next value for seq),(-1),(nextval(seq))")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1", "12", "-1", "13"))

	// test insert with lastval & setval.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(lastval(seq)),(-1),(nextval(seq))")
	tk.MustQuery("select * from t").Check(testkit.Rows("13", "-1", "14"))
	tk.MustExec("delete from t")
	tk.MustQuery("select setval(seq, 100)").Check(testkit.Rows("100"))
	tk.MustExec("insert into t values(lastval(seq)),(-1),(nextval(seq))")
	tk.MustQuery("select * from t").Check(testkit.Rows("14", "-1", "101"))

	// test insert with generated column.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int default next value for seq, col1 int generated always as (id + 1))")

	tk.MustExec("insert into t values()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec("insert into t values(),()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "2 3", "3 4"))
	tk.MustExec("delete from t")
	tk.MustExec("insert into t (id) values(-1),(default)")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1 0", "4 5"))

	// test sequence run out (overflows MaxInt64).
	setSQL := "select setval(seq," + strconv.FormatInt(model.DefaultPositiveSequenceMaxValue+1, 10) + ")"
	tk.MustQuery(setSQL).Check(testkit.Rows("9223372036854775807"))
	err := tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")
}

func TestUnflodSequence(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// test insert into select from.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("create sequence seq")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int, b int)")
	tk.MustExec("create table t3 (a int, b int, c int)")
	tk.MustExec("insert into t1 values(-1),(-1),(-1)")
	// test sequence function unfold.
	tk.MustQuery("select nextval(seq), a from t1").Check(testkit.Rows("1 -1", "2 -1", "3 -1"))
	tk.MustExec("insert into t2 select nextval(seq), a from t1")
	tk.MustQuery("select * from t2").Check(testkit.Rows("4 -1", "5 -1", "6 -1"))
	tk.MustExec("delete from t2")

	// if lastval is folded, the first result should be always 6.
	tk.MustQuery("select lastval(seq), nextval(seq), a from t1").Check(testkit.Rows("6 7 -1", "7 8 -1", "8 9 -1"))
	tk.MustExec("insert into t3 select lastval(seq), nextval(seq), a from t1")
	tk.MustQuery("select * from t3").Check(testkit.Rows("9 10 -1", "10 11 -1", "11 12 -1"))
	tk.MustExec("delete from t3")

	// if setval is folded, the result should be "101 100 -1"...
	tk.MustQuery("select nextval(seq), setval(seq,100), a from t1").Check(testkit.Rows("13 100 -1", "101 <nil> -1", "102 <nil> -1"))
	tk.MustExec("insert into t3 select nextval(seq), setval(seq,200), a from t1")
	tk.MustQuery("select * from t3").Check(testkit.Rows("103 200 -1", "201 <nil> -1", "202 <nil> -1"))
	tk.MustExec("delete from t3")

	// lastval should be evaluated after nextval in each row.
	tk.MustQuery("select nextval(seq), lastval(seq), a from t1").Check(testkit.Rows("203 203 -1", "204 204 -1", "205 205 -1"))
	tk.MustExec("insert into t3 select nextval(seq), lastval(seq), a from t1")
	tk.MustQuery("select * from t3").Check(testkit.Rows("206 206 -1", "207 207 -1", "208 208 -1"))
	tk.MustExec("delete from t3")

	// double nextval should be also evaluated in each row.
	tk.MustQuery("select nextval(seq), nextval(seq), a from t1").Check(testkit.Rows("209 210 -1", "211 212 -1", "213 214 -1"))
	tk.MustExec("insert into t3 select nextval(seq), nextval(seq), a from t1")
	tk.MustQuery("select * from t3").Check(testkit.Rows("215 216 -1", "217 218 -1", "219 220 -1"))
	tk.MustExec("delete from t3")

	tk.MustQuery("select nextval(seq)+lastval(seq), a from t1").Check(testkit.Rows("442 -1", "444 -1", "446 -1"))
	tk.MustExec("insert into t2 select nextval(seq)+lastval(seq), a from t1")
	tk.MustQuery("select * from t2").Check(testkit.Rows("448 -1", "450 -1", "452 -1"))
	tk.MustExec("delete from t2")

	// sub-query contain sequence function.
	tk.MustQuery("select nextval(seq), b from (select nextval(seq) as b, a from t1) t2").Check(testkit.Rows("227 228", "229 230", "231 232"))
	tk.MustExec("insert into t2 select nextval(seq), b from (select nextval(seq) as b, a from t1) t2")
	tk.MustQuery("select * from t2").Check(testkit.Rows("233 234", "235 236", "237 238"))
	tk.MustExec("delete from t2")

	// For union operator like select1 union select2, select1 and select2 will be executed parallelly,
	// so sequence function in both select are evaluated without order. Besides, the upper union operator
	// will gather results through multi worker goroutine parallelly leading the results unordered.
	// Cases like:
	// `select nextval(seq), a from t1 union select lastval(seq), a from t2`
	// `select nextval(seq), a from t1 union select nextval(seq), a from t2`
	// The executing order of nextval and lastval is implicit, don't make any assumptions on it.
}

// before this PR:
// single insert consume: 50.498672ms
// after this PR:
// single insert consume: 33.213615ms
func BenchmarkInsertCacheDefaultExpr(b *testing.B) {
	store, clean := testkit.CreateMockStore(b)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create sequence seq")
	tk.MustExec("create table t(a int default next value for seq)")
	sql := "insert into t values "
	for i := 0; i < 1000; i++ {
		if i == 0 {
			sql += "()"
		} else {
			sql += ",()"
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec(sql)
	}
}

func TestSequenceFunctionPrivilege(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test sequence function privilege.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default next value for seq)")
	tk.MustExec("drop user if exists myuser@localhost")
	tk.MustExec("create user myuser@localhost")

	tk1 := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil))
	tk1.SetSession(se)

	// grant the myuser the create access to the sequence.
	tk.MustExec("grant insert on test.t to 'myuser'@'localhost'")

	// INSERT privilege required to use nextval.
	tk1.MustExec("use test")
	err = tk1.QueryToErr("select nextval(seq)")
	require.Error(t, err)
	require.EqualError(t, err, "[expression:1142]INSERT command denied to user 'myuser'@'localhost' for table 'seq'")

	_, err = tk1.Exec("insert into t values()")
	require.Error(t, err)
	require.EqualError(t, err, "[expression:1142]INSERT command denied to user 'myuser'@'localhost' for table 'seq'")

	// SELECT privilege required to use lastval.
	err = tk1.QueryToErr("select lastval(seq)")
	require.Error(t, err)
	require.EqualError(t, err, "[expression:1142]SELECT command denied to user 'myuser'@'localhost' for table 'seq'")

	// INSERT privilege required to use setval.
	err = tk1.QueryToErr("select setval(seq, 10)")
	require.Error(t, err)
	require.EqualError(t, err, "[expression:1142]INSERT command denied to user 'myuser'@'localhost' for table 'seq'")

	// grant the myuser the SELECT & UPDATE access to sequence seq.
	tk.MustExec("grant SELECT, INSERT on test.seq to 'myuser'@'localhost'")

	// SELECT privilege required to use nextval.
	tk1.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk1.MustQuery("select lastval(seq)").Check(testkit.Rows("1"))
	tk1.MustQuery("select setval(seq, 10)").Check(testkit.Rows("10"))
	tk1.MustExec("insert into t values()")

	tk.MustExec("drop table t")
	tk.MustExec("drop sequence seq")
	tk.MustExec("drop user myuser@localhost")
}

// Background: the newly added column in TiDB won't fill the known rows with specific
// sequence next value immediately. Every time TiDB select the data from storage, kvDB
// will fill the originDefaultValue to these incomplete rows (but not store).
//
// In sequence case, every time filling these rows, kvDB should eval the sequence
// expr for len(incomplete rows) times, and combine these row data together. That
// means the select result is not always the same.
//
// However, the altered column with sequence as it's default value can work well.
// Because this column has already been added before the alter action, which also
// means originDefaultValue should be something but nil, so the back filling in kvDB
// can work well.
//
// The new altered sequence default value for this column only take effect on the
// subsequent inserted rows.
//
// So under current situation, TiDB will
// [1]: forbid the new added column has sequence as it's default value.
// [2]: allow the altered column with sequence as default value.
func TestSequenceDefaultLogic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create sequence seq")
	tk.MustExec("create table t(a int)")

	// Alter table to use sequence as default value is ok.
	tk.MustExec("insert into t values(-1),(-1),(-1)")
	tk.MustExec("alter table t add column b int default -1")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1 -1", "-1 -1", "-1 -1"))
	tk.MustExec("alter table t modify column b int default next value for seq")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1 -1", "-1 -1", "-1 -1"))
	tk.MustExec("insert into t(a) values(-1),(-1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1 -1", "-1 -1", "-1 -1", "-1 1", "-1 2"))

	// Add column to set sequence as default value is forbade.
	tk.MustExec("drop sequence seq")
	tk.MustExec("drop table t")
	tk.MustExec("create sequence seq")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(-1),(-1),(-1)")
	tk.MustGetErrMsg("alter table t add column b int default next value for seq", "[ddl:8230]Unsupported using sequence as default value in add column 'b'")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1", "-1", "-1"))
}

// Close issue #17945, sequence cache shouldn't be negative.
func TestSequenceCacheShouldNotBeNegative(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop sequence if exists seq")
	_, err := tk.Exec("create sequence seq cache -1")
	require.Error(t, err)
	require.EqualError(t, err, "[ddl:4136]Sequence 'test.seq' values are conflicting")

	_, err = tk.Exec("create sequence seq cache 0")
	require.Error(t, err)
	require.EqualError(t, err, "[ddl:4136]Sequence 'test.seq' values are conflicting")

	// This will error because
	// 1: maxvalue = -1 by default
	// 2: minvalue = -9223372036854775807 by default
	// 3: increment = -9223372036854775807 by user
	// `seqInfo.CacheValue < (math.MaxInt64-absIncrement)/absIncrement` will
	// ensure there is enough value for one cache allocation at least.
	_, err = tk.Exec("create sequence seq INCREMENT -9223372036854775807 cache 1")
	require.Error(t, err)
	require.EqualError(t, err, "[ddl:4136]Sequence 'test.seq' values are conflicting")

	tk.MustExec("create sequence seq cache 1")
}

func TestAlterSequence(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustExec("alter sequence seq increment by 2 start with 2")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` " +
		"start with 2 minvalue 1 maxvalue 9223372036854775806 increment by 2 cache 1000 nocycle ENGINE=InnoDB"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	// Alter sequence will invalidate the sequence cache in memory.
	tk.MustExec("alter sequence seq increment by 2")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` " +
		"start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 2 cache 1000 nocycle ENGINE=InnoDB"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1001"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1003"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1005"))

	// Restart with value will reset the sequence value in kv.
	tk.MustExec("alter sequence seq restart with 9")
	// Like setval does, the sequence current value change won't affect the increment frequency.
	// By now the step frequency is: 1, 3, 5, 7, 9, 11, 13, 15...
	// After restart with 9, the current value rebased to 8, the next valid value will be 9, coincidentally equal to what we restarted.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("13"))

	tk.MustExec("alter sequence seq restart with 10")
	// After restart with 10, the current value rebased to 9, the next valid value will be 11, rather than what we restart.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("13"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("15"))

	// Restart will reset the sequence value to start value by default.
	tk.MustExec("alter sequence seq restart")
	// After restart, the base will be pointed to 0, the first value will be 1 here, then plus the increment 2, the second value will be 3.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment by 3")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("4"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))

	tk.MustExec("alter sequence seq increment by 4")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3001"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3005"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3009"))
	tk.MustExec("drop sequence if exists seq")
}

func TestAlterSequencePrivilege(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop sequence if exists my_seq")
	tk.MustExec("create sequence my_seq")

	// Test create privilege.
	tk.MustExec("drop user if exists myuser@localhost")
	tk.MustExec("create user myuser@localhost")

	tk1 := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil))
	tk1.SetSession(se)

	// grant the myuser the access to database test.
	tk.MustExec("grant select on test.* to 'myuser'@'localhost'")

	tk1.MustExec("use test")
	_, err = tk1.Exec("alter sequence my_seq increment = 2")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1142]ALTER command denied to user 'myuser'@'localhost' for table 'my_seq'")
	tk.MustExec("drop sequence if exists my_seq")
}

func TestDdl_AlterSequenceIssue31265(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create sequence seq cache=1 nocache")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 nocache nocycle ENGINE=InnoDB"))

	tk.MustExec("create sequence cache_to_nocache_seq;")
	tk.MustExec("alter sequence cache_to_nocache_seq nocache;")
	tk.MustQuery("show create sequence cache_to_nocache_seq;").Check(testkit.Rows("cache_to_nocache_seq CREATE SEQUENCE `cache_to_nocache_seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 nocache nocycle ENGINE=InnoDB"))

	tk.MustExec("create sequence nocache_to_cache_seq nocache;")
	tk.MustExec("alter sequence nocache_to_cache_seq cache 10;")
	tk.MustQuery("show create sequence nocache_to_cache_seq;").Check(testkit.Rows("nocache_to_cache_seq CREATE SEQUENCE `nocache_to_cache_seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 10 nocycle ENGINE=InnoDB"))
}
