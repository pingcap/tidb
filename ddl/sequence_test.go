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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testSequenceSuite{&testDBSuite{}})

type testSequenceSuite struct{ *testDBSuite }

func (s *testSequenceSuite) TestCreateSequence(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustGetErrCode("create sequence `seq  `", mysql.ErrWrongTableName)

	// increment should not be set as 0.
	s.tk.MustGetErrCode("create sequence seq increment 0", mysql.ErrSequenceInvalidData)

	// maxvalue should be larger than minvalue.
	s.tk.MustGetErrCode("create sequence seq maxvalue 1 minvalue 2", mysql.ErrSequenceInvalidData)

	// maxvalue should be larger than minvalue.
	s.tk.MustGetErrCode("create sequence seq maxvalue 1 minvalue 1", mysql.ErrSequenceInvalidData)

	// maxvalue shouldn't be equal to MaxInt64.
	s.tk.MustGetErrCode("create sequence seq maxvalue 9223372036854775807 minvalue 1", mysql.ErrSequenceInvalidData)

	// TODO : minvalue shouldn't be equal to MinInt64.

	// maxvalue should be larger than start.
	s.tk.MustGetErrCode("create sequence seq maxvalue 1 start with 2", mysql.ErrSequenceInvalidData)

	// cacheVal should be less than (math.MaxInt64-maxIncrement)/maxIncrement.
	s.tk.MustGetErrCode("create sequence seq increment 100000 cache 922337203685477", mysql.ErrSequenceInvalidData)

	// test unsupported table option in sequence.
	s.tk.MustGetErrCode("create sequence seq CHARSET=utf8", mysql.ErrSequenceUnsupportedTableOption)

	_, err := s.tk.Exec("create sequence seq comment=\"test\"")
	c.Assert(err, IsNil)

	sequenceTable := testGetTableByName(c, s.s, "test", "seq")
	c.Assert(sequenceTable.Meta().IsSequence(), Equals, true)
	c.Assert(sequenceTable.Meta().Sequence.Increment, Equals, model.DefaultSequenceIncrementValue)
	c.Assert(sequenceTable.Meta().Sequence.Start, Equals, model.DefaultPositiveSequenceStartValue)
	c.Assert(sequenceTable.Meta().Sequence.MinValue, Equals, model.DefaultPositiveSequenceMinValue)
	c.Assert(sequenceTable.Meta().Sequence.MaxValue, Equals, model.DefaultPositiveSequenceMaxValue)
	c.Assert(sequenceTable.Meta().Sequence.Cache, Equals, true)
	c.Assert(sequenceTable.Meta().Sequence.CacheValue, Equals, model.DefaultSequenceCacheValue)
	c.Assert(sequenceTable.Meta().Sequence.Cycle, Equals, false)
	c.Assert(sequenceTable.Meta().Sequence.Order, Equals, false)

	// Test create privilege.
	s.tk.MustExec("create user myuser@localhost")
	s.tk.MustExec("flush privileges")

	tk1 := testkit.NewTestKit(c, s.store)
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil), IsTrue)
	tk1.Se = se

	// grant the myuser the access to database test.
	s.tk.MustExec("grant select on test.* to 'myuser'@'localhost'")
	s.tk.MustExec("flush privileges")

	tk1.MustExec("use test")
	_, err = tk1.Exec("create sequence my_seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1142]CREATE command denied to user 'localhost'@'myuser' for table 'my_seq'")
}

func (s *testSequenceSuite) TestDropSequence(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop sequence if exists seq")

	// Test sequence is unknown.
	s.tk.MustGetErrCode("drop sequence seq", mysql.ErrUnknownSequence)

	// Test non-existed sequence can't drop successfully.
	s.tk.MustExec("create sequence seq")
	_, err := s.tk.Exec("drop sequence seq, seq2")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schema:4139]Unknown SEQUENCE: 'test.seq2'")

	// Test the specified object is not sequence.
	s.tk.MustExec("create table seq3 (a int)")
	_, err = s.tk.Exec("drop sequence seq3")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ddl.ErrWrongObject), IsTrue)

	// Test schema is not exist.
	_, err = s.tk.Exec("drop sequence unknown.seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schema:4139]Unknown SEQUENCE: 'unknown.seq'")

	// Test drop sequence successfully.
	s.tk.MustExec("create sequence seq")
	_, err = s.tk.Exec("drop sequence seq")
	c.Assert(err, IsNil)
	_, err = s.tk.Exec("drop sequence seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schema:4139]Unknown SEQUENCE: 'test.seq'")

	// Test drop table when the object is a sequence.
	s.tk.MustExec("create sequence seq")
	_, err = s.tk.Exec("drop table seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schema:1051]Unknown table 'test.seq'")

	// Test drop view when the object is a sequence.
	_, err = s.tk.Exec("drop view seq")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ddl.ErrWrongObject), IsTrue)
	s.tk.MustExec("drop sequence seq")

	// Test drop privilege.
	s.tk.MustExec("drop user if exists myuser@localhost")
	s.tk.MustExec("create user myuser@localhost")
	s.tk.MustExec("flush privileges")

	tk1 := testkit.NewTestKit(c, s.store)
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil), IsTrue)
	tk1.Se = se

	// grant the myuser the access to database test.
	s.tk.MustExec("create sequence my_seq")
	s.tk.MustExec("grant select on test.* to 'myuser'@'localhost'")
	s.tk.MustExec("flush privileges")

	tk1.MustExec("use test")
	_, err = tk1.Exec("drop sequence my_seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1142]DROP command denied to user 'localhost'@'myuser' for table 'my_seq'")

	// Test for `drop sequence if exists`.
	s.tk.MustExec("drop sequence if exists seq_if_exists")
	s.tk.MustQuery("show warnings;").Check(testkit.Rows("Note 4139 Unknown SEQUENCE: 'test.seq_if_exists'"))
}

func (s *testSequenceSuite) TestShowCreateSequence(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists t")
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create table t(a int)")
	s.tk.MustExec("create sequence seq")

	// Test show privilege.
	s.tk.MustExec("drop user if exists myuser@localhost")
	s.tk.MustExec("create user myuser@localhost")
	s.tk.MustExec("flush privileges")

	tk1 := testkit.NewTestKit(c, s.store)
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil), IsTrue)
	tk1.Se = se

	// Grant the myuser the access to table t in database test, but sequence seq.
	s.tk.MustExec("grant select on test.t to 'myuser'@'localhost'")
	s.tk.MustExec("flush privileges")

	tk1.MustExec("use test")
	tk1.MustExec("show create table t")
	_, err = tk1.Exec("show create sequence seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1142]SHOW command denied to user 'myuser'@'localhost' for table 'seq'")

	// Grant the myuser the access to sequence seq in database test.
	s.tk.MustExec("grant select on test.seq to 'myuser'@'localhost'")
	s.tk.MustExec("flush privileges")

	tk1.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	// Test show sequence detail.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq")
	s.tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq start 10")
	s.tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 10 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq minvalue 0")
	s.tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 0 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq maxvalue 100")
	s.tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 100 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment = -2")
	s.tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with -1 minvalue -9223372036854775807 maxvalue -1 increment by -2 cache 1000 nocycle ENGINE=InnoDB"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq nocache")
	s.tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 nocache nocycle ENGINE=InnoDB"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq cycle")
	s.tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 cycle ENGINE=InnoDB"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq comment=\"ccc\"")
	s.tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB COMMENT='ccc'"))

	// Test show create sequence with a normal table.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create table seq (a int)")
	err = s.tk.QueryToErr("show create sequence seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[executor:1347]'test.seq' is not SEQUENCE")
	s.tk.MustExec("drop table if exists seq")

	// Test use the show create sequence result to create sequence.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq")
	showString := s.tk.MustQuery("show create sequence seq").Rows()[0][1].(string)
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec(showString)
}

func (s *testSequenceSuite) TestSequenceAsDefaultValue(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq")

	// test the use sequence's nextval as default.
	s.tk.MustExec("create table t1 (a int default next value for seq)")
	s.tk.MustGetErrMsg("create table t2 (a char(1) default next value for seq)", "[ddl:8228]Unsupported sequence default value for column type 'a'")

	s.tk.MustExec("create table t3 (a int default nextval(seq))")

	s.tk.MustExec("create table t4 (a int)")
	s.tk.MustExec("alter table t4 alter column a set default (next value for seq)")
	s.tk.MustExec("alter table t4 alter column a set default (nextval(seq))")

	s.tk.MustExec("create table t5 (a char(1))")
	s.tk.MustGetErrMsg("alter table t5 alter column a set default (next value for seq)", "[ddl:8228]Unsupported sequence default value for column type 'a'")

	s.tk.MustGetErrMsg("alter table t5 alter column a set default (nextval(seq))", "[ddl:8228]Unsupported sequence default value for column type 'a'")

	s.tk.MustGetErrMsg("alter table t5 add column b char(1) default next value for seq", "[ddl:8228]Unsupported sequence default value for column type 'b'")

	s.tk.MustExec("alter table t5 add column b int default nextval(seq)")

}

func (s *testSequenceSuite) TestSequenceFunction(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("drop sequence if exists seq1")
	s.tk.MustExec("create sequence seq")

	// test normal sequence function.
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	s.tk.MustQuery("select nextval(test.seq)").Check(testkit.Rows("2"))
	s.tk.MustQuery("select next value for seq").Check(testkit.Rows("3"))
	s.tk.MustQuery("select next value for test.seq").Check(testkit.Rows("4"))

	// test sequence function error.
	s.tk.MustGetErrMsg("select nextval(seq1)", "[schema:1146]Table 'test.seq1' doesn't exist")
	s.tk.MustExec("create database test2")
	s.tk.MustExec("use test2")
	s.tk.MustQuery("select nextval(test.seq)").Check(testkit.Rows("5"))
	s.tk.MustQuery("select next value for test.seq").Check(testkit.Rows("6"))
	s.tk.MustGetErrMsg("select nextval(seq)", "[schema:1146]Table 'test2.seq' doesn't exist")
	s.tk.MustGetErrMsg("select next value for seq", "[schema:1146]Table 'test2.seq' doesn't exist")
	s.tk.MustExec("use test")

	// test sequence nocache.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq nocache")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))

	// test sequence option logic.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment = 5")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment = 5 start = 3")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("13"))

	// minvalue should be specified lower than start (negative here), default 1 when increment > 0.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq minvalue -5 start = -2 increment = 5")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))

	// test sequence cycle.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment = 5 start = 3 maxvalue = 12 cycle")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))

	// test sequence maxvalue allocation.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment = 4 start = 2 maxvalue = 10 cycle")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))

	// test sequence has run out.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment = 5 start = 3 maxvalue = 12 nocycle")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	err := s.tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[table:4135]Sequence 'test.seq' has run out")

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment = 3 start = 3 maxvalue = 9 nocycle")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9"))
	err = s.tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[table:4135]Sequence 'test.seq' has run out")

	// test negative-growth sequence
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment = -2 start = 3 minvalue -5 maxvalue = 12 cycle")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-3"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-5"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("12"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment = -3 start = 2 minvalue -6 maxvalue = 11 cycle")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-4"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment = -4 start = 6 minvalue -6 maxvalue = 11")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-6"))
	err = s.tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[table:4135]Sequence 'test.seq' has run out")

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment = -3 start = 2 minvalue -2 maxvalue 10")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	err = s.tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[table:4135]Sequence 'test.seq' has run out")

	// test sequence setval function.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	// set value to a used value, will get NULL.
	s.tk.MustQuery("select setval(seq, 2)").Check(testkit.Rows("<nil>"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	// set value to a unused value, will get itself.
	s.tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	// the next value will not be base on next value.
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment 3 maxvalue 11")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("4"))
	s.tk.MustQuery("select setval(seq, 3)").Check(testkit.Rows("<nil>"))
	s.tk.MustQuery("select setval(seq, 4)").Check(testkit.Rows("<nil>"))
	s.tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))
	s.tk.MustQuery("select setval(seq, 8)").Check(testkit.Rows("8"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	err = s.tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[table:4135]Sequence 'test.seq' has run out")
	s.tk.MustQuery("select setval(seq, 11)").Check(testkit.Rows("11"))
	err = s.tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[table:4135]Sequence 'test.seq' has run out")
	// set value can be bigger than maxvalue.
	s.tk.MustQuery("select setval(seq, 100)").Check(testkit.Rows("100"))
	err = s.tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[table:4135]Sequence 'test.seq' has run out")

	// test setval in second cache round.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment 10 start 5 maxvalue 100 cache 10 cycle")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("15"))
	s.tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("20"))
	// the next value will not be base on next value.
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("25"))
	sequenceTable := testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok := sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round := tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(95))
	c.Assert(round, Equals, int64(0))
	// exhausted the sequence first round in cycle.
	s.tk.MustQuery("select setval(seq, 95)").Check(testkit.Rows("95"))
	// make sequence alloc the next batch.
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(91))
	c.Assert(round, Equals, int64(1))
	s.tk.MustQuery("select setval(seq, 15)").Check(testkit.Rows("15"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("21"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("31"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment 2 start 0 maxvalue 10 minvalue -10 cache 3 cycle")
	s.tk.MustQuery("select setval(seq, -20)").Check(testkit.Rows("<nil>"))
	s.tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("20"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-10"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(-6))
	c.Assert(round, Equals, int64(1))

	// test setval in negative-growth sequence.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment -3 start 5 maxvalue 10 minvalue -10 cache 3 cycle")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(-1))
	c.Assert(round, Equals, int64(0))
	// exhausted the sequence first cache batch.
	s.tk.MustQuery("select setval(seq, -2)").Check(testkit.Rows("-2"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-4"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(-10))
	c.Assert(round, Equals, int64(0))
	// exhausted the sequence second cache batch.
	s.tk.MustQuery("select setval(seq, -10)").Check(testkit.Rows("-10"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(4))
	c.Assert(round, Equals, int64(1))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("4"))
	// test the sequence negative rebase.
	s.tk.MustQuery("select setval(seq, 0)").Check(testkit.Rows("0"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment -2 start 0 maxvalue 10 minvalue -10 cache 3 cycle")
	s.tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("<nil>"))
	s.tk.MustQuery("select setval(seq, -20)").Check(testkit.Rows("-20"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(6))
	c.Assert(round, Equals, int64(1))

	// test sequence lastval function.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq")
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("1"))
	s.tk.MustQuery("select next value for seq").Check(testkit.Rows("2"))
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	// setval won't change the last value.
	s.tk.MustQuery("select setval(seq, -1)").Check(testkit.Rows("<nil>"))
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	s.tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))

	// test lastval in positive-growth sequence cycle and cache.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment 3 start 3 maxvalue 14 cache 3 cycle")
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(9))
	c.Assert(round, Equals, int64(0))
	// invalidate the current sequence cache.
	s.tk.MustQuery("select setval(seq, 10)").Check(testkit.Rows("10"))
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("3"))
	// trigger the next sequence cache.
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("12"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(14))
	c.Assert(round, Equals, int64(0))
	// invalidate the current sequence cache.
	s.tk.MustQuery("select setval(seq, 13)").Check(testkit.Rows("13"))
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("12"))
	// trigger the next sequence cache.
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(7))
	c.Assert(round, Equals, int64(1))
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("1"))

	// test lastval in negative-growth sequence cycle and cache.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment -3 start -2 maxvalue 10 minvalue -10 cache 3 cycle")
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(-8))
	c.Assert(round, Equals, int64(0))
	// invalidate the current sequence cache.
	s.tk.MustQuery("select setval(seq, -8)").Check(testkit.Rows("-8"))
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("-2"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(4))
	c.Assert(round, Equals, int64(1))
	s.tk.MustQuery("select lastval(seq)").Check(testkit.Rows("10"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment -1 start 1 maxvalue 10 minvalue -10 cache 3 cycle")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	s.tk.MustQuery("select setval(seq, -8)").Check(testkit.Rows("-8"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9"))
	sequenceTable = testGetTableByName(c, s.tk.Se, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(-10))
	c.Assert(round, Equals, int64(0))

	// Test the sequence seek formula will overflow Int64.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment 2 start -9223372036854775807 maxvalue 9223372036854775806 minvalue -9223372036854775807 cache 2 cycle")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9223372036854775807"))
	s.tk.MustQuery("select setval(seq, 9223372036854775800)").Check(testkit.Rows("9223372036854775800"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9223372036854775801"))

	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq increment -2 start 9223372036854775806 maxvalue 9223372036854775806 minvalue -9223372036854775807 cache 2 cycle")
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9223372036854775806"))
	s.tk.MustQuery("select setval(seq, -9223372036854775800)").Check(testkit.Rows("-9223372036854775800"))
	s.tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9223372036854775802"))
}

func (s *testSequenceSuite) TestInsertSequence(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("drop table if exists t")

	// test insert with sequence default value.
	s.tk.MustExec("create sequence seq")
	s.tk.MustExec("create table t (a int default next value for seq)")
	s.tk.MustExec("insert into t values()")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	s.tk.MustExec("insert into t values(),(),()")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("1", "2", "3", "4"))
	s.tk.MustExec("delete from t")
	s.tk.MustExec("insert into t values(-1),(default),(-1)")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("-1", "5", "-1"))

	// test insert with specified sequence value rather than default.
	s.tk.MustExec("drop table if exists t")
	s.tk.MustExec("create table t (a int)")
	s.tk.MustExec("insert into t values(next value for seq)")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("6"))
	s.tk.MustExec("insert into t values(next value for seq),(nextval(seq))")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("6", "7", "8"))

	// test insert with sequence expression.
	s.tk.MustExec("delete from t")
	s.tk.MustExec("insert into t values(next value for seq + 1),(nextval(seq) * 2)")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("10", "20"))
	s.tk.MustExec("delete from t")
	s.tk.MustExec("insert into t values((next value for seq - 1) / 2)")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("5"))

	// test insert with user specified value.
	s.tk.MustExec("delete from t")
	s.tk.MustExec("insert into t values(-1),(next value for seq),(-1),(nextval(seq))")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("-1", "12", "-1", "13"))

	// test insert with lastval & setval.
	s.tk.MustExec("delete from t")
	s.tk.MustExec("insert into t values(lastval(seq)),(-1),(nextval(seq))")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("13", "-1", "14"))
	s.tk.MustExec("delete from t")
	s.tk.MustQuery("select setval(seq, 100)").Check(testkit.Rows("100"))
	s.tk.MustExec("insert into t values(lastval(seq)),(-1),(nextval(seq))")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("14", "-1", "101"))

	// test insert with generated column.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("create sequence seq")
	s.tk.MustExec("drop table if exists t")
	s.tk.MustExec("create table t (id int default next value for seq, col1 int generated always as (id + 1))")

	s.tk.MustExec("insert into t values()")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	s.tk.MustExec("insert into t values(),()")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "2 3", "3 4"))
	s.tk.MustExec("delete from t")
	s.tk.MustExec("insert into t (id) values(-1),(default)")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("-1 0", "4 5"))

	// test sequence run out (overflow MaxInt64).
	setSQL := "select setval(seq," + strconv.FormatInt(model.DefaultPositiveSequenceMaxValue+1, 10) + ")"
	s.tk.MustQuery(setSQL).Check(testkit.Rows("9223372036854775807"))
	err := s.tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[table:4135]Sequence 'test.seq' has run out")
}

func (s *testSequenceSuite) TestUnflodSequence(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	// test insert into select from.
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("drop table if exists t1,t2,t3")
	s.tk.MustExec("create sequence seq")
	s.tk.MustExec("create table t1 (a int)")
	s.tk.MustExec("create table t2 (a int, b int)")
	s.tk.MustExec("create table t3 (a int, b int, c int)")
	s.tk.MustExec("insert into t1 values(-1),(-1),(-1)")
	// test sequence function unfold.
	s.tk.MustQuery("select nextval(seq), a from t1").Check(testkit.Rows("1 -1", "2 -1", "3 -1"))
	s.tk.MustExec("insert into t2 select nextval(seq), a from t1")
	s.tk.MustQuery("select * from t2").Check(testkit.Rows("4 -1", "5 -1", "6 -1"))
	s.tk.MustExec("delete from t2")

	// if lastval is folded, the first result should be always 6.
	s.tk.MustQuery("select lastval(seq), nextval(seq), a from t1").Check(testkit.Rows("6 7 -1", "7 8 -1", "8 9 -1"))
	s.tk.MustExec("insert into t3 select lastval(seq), nextval(seq), a from t1")
	s.tk.MustQuery("select * from t3").Check(testkit.Rows("9 10 -1", "10 11 -1", "11 12 -1"))
	s.tk.MustExec("delete from t3")

	// if setval is folded, the result should be "101 100 -1"...
	s.tk.MustQuery("select nextval(seq), setval(seq,100), a from t1").Check(testkit.Rows("13 100 -1", "101 <nil> -1", "102 <nil> -1"))
	s.tk.MustExec("insert into t3 select nextval(seq), setval(seq,200), a from t1")
	s.tk.MustQuery("select * from t3").Check(testkit.Rows("103 200 -1", "201 <nil> -1", "202 <nil> -1"))
	s.tk.MustExec("delete from t3")

	// lastval should be evaluated after nextval in each row.
	s.tk.MustQuery("select nextval(seq), lastval(seq), a from t1").Check(testkit.Rows("203 203 -1", "204 204 -1", "205 205 -1"))
	s.tk.MustExec("insert into t3 select nextval(seq), lastval(seq), a from t1")
	s.tk.MustQuery("select * from t3").Check(testkit.Rows("206 206 -1", "207 207 -1", "208 208 -1"))
	s.tk.MustExec("delete from t3")

	// double nextval should be also evaluated in each row.
	s.tk.MustQuery("select nextval(seq), nextval(seq), a from t1").Check(testkit.Rows("209 210 -1", "211 212 -1", "213 214 -1"))
	s.tk.MustExec("insert into t3 select nextval(seq), nextval(seq), a from t1")
	s.tk.MustQuery("select * from t3").Check(testkit.Rows("215 216 -1", "217 218 -1", "219 220 -1"))
	s.tk.MustExec("delete from t3")

	s.tk.MustQuery("select nextval(seq)+lastval(seq), a from t1").Check(testkit.Rows("442 -1", "444 -1", "446 -1"))
	s.tk.MustExec("insert into t2 select nextval(seq)+lastval(seq), a from t1")
	s.tk.MustQuery("select * from t2").Check(testkit.Rows("448 -1", "450 -1", "452 -1"))
	s.tk.MustExec("delete from t2")

	// sub-query contain sequence function.
	s.tk.MustQuery("select nextval(seq), b from (select nextval(seq) as b, a from t1) t2").Check(testkit.Rows("227 228", "229 230", "231 232"))
	s.tk.MustExec("insert into t2 select nextval(seq), b from (select nextval(seq) as b, a from t1) t2")
	s.tk.MustQuery("select * from t2").Check(testkit.Rows("233 234", "235 236", "237 238"))
	s.tk.MustExec("delete from t2")

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
// Notice: use go test -check.b Benchmarkxxx to test it.
func (s *testSequenceSuite) BenchmarkInsertCacheDefaultExpr(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop sequence if exists seq")
	s.tk.MustExec("drop table if exists t")
	s.tk.MustExec("create sequence seq")
	s.tk.MustExec("create table t(a int default next value for seq)")
	sql := "insert into t values "
	for i := 0; i < 1000; i++ {
		if i == 0 {
			sql += "()"
		} else {
			sql += ",()"
		}
	}
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		s.tk.MustExec(sql)
	}
}
