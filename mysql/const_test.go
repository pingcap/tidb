// Copyright 2017 PingCAP, Inc.
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

package mysql_test

import (
	"flag"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"testing"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testMySQLConstSuite{})

type testMySQLConstSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore *mocktikv.MvccStore
	store     kv.Storage
	*parser.Parser
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in executor test")

func (s *testMySQLConstSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		s.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.NewMvccStore()
		store, err := tikv.NewMockTikvStore(
			tikv.WithCluster(s.cluster),
			tikv.WithMVCCStore(s.mvccStore),
		)
		c.Assert(err, IsNil)
		s.store = store
		tidb.SetSchemaLease(0)
		tidb.SetStatsLease(0)
	} else {
		store, err := tidb.NewStore("memory://test/test")
		c.Assert(err, IsNil)
		s.store = store
	}
	_, err := tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testMySQLConstSuite) TestGetSQLMode(c *C) {
	defer testleak.AfterTest(c)()

	positiveCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE"},
		{",,NO_ZERO_DATE"},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE"},
		{""},
		{", "},
		{","},
	}

	for _, t := range positiveCases {
		_, err := mysql.GetSQLMode(mysql.FormatSQLModeStr(t.arg))
		c.Assert(err, IsNil)
	}

	negativeCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE, NO_ZERO_IN_DATE"},
		{"NO_ZERO_DATE,adfadsdfasdfads"},
		{", ,NO_ZERO_DATE"},
		{" ,"},
	}

	for _, t := range negativeCases {
		_, err := mysql.GetSQLMode(mysql.FormatSQLModeStr(t.arg))
		c.Assert(err, NotNil)
	}
}

func (s *testMySQLConstSuite) TestSQLMode(c *C) {
	defer testleak.AfterTest(c)()

	tests := []struct {
		arg                           string
		hasNoZeroDateMode             bool
		hasNoZeroInDateMode           bool
		hasErrorForDivisionByZeroMode bool
	}{
		{"NO_ZERO_DATE", true, false, false},
		{"NO_ZERO_IN_DATE", false, true, false},
		{"ERROR_FOR_DIVISION_BY_ZERO", false, false, true},
		{"NO_ZERO_IN_DATE,NO_ZERO_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", true, true, true},
		{"NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", false, true, true},
		{"", false, false, false},
	}

	for _, t := range tests {
		sqlMode, _ := mysql.GetSQLMode(t.arg)
		c.Assert(sqlMode.HasNoZeroDateMode(), Equals, t.hasNoZeroDateMode)
		c.Assert(sqlMode.HasNoZeroInDateMode(), Equals, t.hasNoZeroInDateMode)
		c.Assert(sqlMode.HasErrorForDivisionByZeroMode(), Equals, t.hasErrorForDivisionByZeroMode)
	}
}

func (s *testMySQLConstSuite) TestRealAsFloatMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a real);")
	result := tk.MustQuery("desc t")
	c.Check(result.Rows(), HasLen, 1)
	row := result.Rows()[0]
	c.Assert(row[1], Equals, "double")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("set sql_mode='REAL_AS_FLOAT'")
	tk.MustExec("create table t (a real)")
	result = tk.MustQuery("desc t")
	c.Check(result.Rows(), HasLen, 1)
	row = result.Rows()[0]
	c.Assert(row[1], Equals, "float")
}
