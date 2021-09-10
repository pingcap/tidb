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
// See the License for the specific language governing permissions and
// limitations under the License.
package statistics_test

import (
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = SerialSuites(&testSerialIntegrationSuite{})

type testSerialIntegrationSuite struct {
	store kv.Storage
	do    *domain.Domain
}

func (s *testSerialIntegrationSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	// Add the hook here to avoid data race.
	var err error
	s.store, s.do, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testSerialIntegrationSuite) TearDownSuite(c *C) {
	s.do.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testSerialIntegrationSuite) TestOutdatedStatsCheck(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")

	h := s.do.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	c.Assert(h.HandleDDLEvent(<-h.DDLEventCh()), IsNil)
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 19)) // 20 rows
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	is := s.do.InfoSchema()
	c.Assert(h.Update(is), IsNil)
	// To pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 1")
	c.Assert(h.LoadNeededHistograms(), IsNil)

	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 13)) // 34 rows
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	c.Assert(tk.HasPseudoStats("select * from t where a = 1"), IsFalse)

	tk.MustExec("insert into t values (1)") // 35 rows
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	c.Assert(tk.HasPseudoStats("select * from t where a = 1"), IsTrue)

	tk.MustExec("analyze table t")

	tk.MustExec("delete from t limit 24") // 11 rows
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	c.Assert(tk.HasPseudoStats("select * from t where a = 1"), IsFalse)

	tk.MustExec("delete from t limit 1") // 10 rows
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	c.Assert(tk.HasPseudoStats("select * from t where a = 1"), IsTrue)
}
