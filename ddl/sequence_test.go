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
	s.tk.MustGetErrCode("create sequence seq ENGINE=InnoDB", mysql.ErrSequenceUnsupportedTableOption)

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
}
