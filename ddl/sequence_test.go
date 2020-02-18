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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testSequenceSuite{&testDBSuite{}})

type testSequenceSuite struct{ *testDBSuite }

func (s *testSequenceSuite) TestCreateSequence(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustGetErrCode("create sequence `seq  `", mysql.ErrWrongTableName)

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
