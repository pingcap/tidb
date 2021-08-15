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

package core_test

import (
	"context"
	"math"
	"regexp"
	"sort"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/set"
)

var _ = Suite(&extractorSuite{})

type extractorSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *extractorSuite) SetUpSuite(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	c.Assert(store, NotNil)

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	c.Assert(dom, NotNil)

	s.store = store
	s.dom = dom
}

func (s *extractorSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *extractorSuite) getLogicalMemTable(c *C, se session.Session, parser *parser.Parser, sql string) *plannercore.LogicalMemTable {
	stmt, err := parser.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)

	ctx := context.Background()
	builder, _ := plannercore.NewPlanBuilder().Init(se, s.dom.InfoSchema(), &hint.BlockHintProcessor{})
	plan, err := builder.Build(ctx, stmt)
	c.Assert(err, IsNil)

	logicalPlan, err := plannercore.LogicalOptimize(ctx, builder.GetOptFlag(), plan.(plannercore.LogicalPlan))
	c.Assert(err, IsNil)

	// Obtain the leaf plan
	leafPlan := logicalPlan
	for len(leafPlan.Children()) > 0 {
		leafPlan = leafPlan.Children()[0]
	}

	logicalMemTable := leafPlan.(*plannercore.LogicalMemTable)
	return logicalMemTable
}

func (s *extractorSuite) TestClusterConfigTableExtractor(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	parser := parser.New()
	var cases = []struct {
		sql         string
		nodeTypes   set.StringSet
		instances   set.StringSet
		skipRequest bool
	}{
		{
			sql:       "select * from information_schema.cluster_config",
			nodeTypes: nil,
			instances: nil,
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'tikv'=type",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'TiKV'=type",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'tikv'=type",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'TiKV'=type or type='tidb'",
			nodeTypes: set.NewStringSet("tikv", "tidb"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'TiKV'=type or type='tidb' or type='pd'",
			nodeTypes: set.NewStringSet("tikv", "tidb", "pd"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where (type='tidb' or type='pd') and (instance='123.1.1.2:1234' or instance='123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tidb", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'pd')",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'pd') and instance='123.1.1.2:1234'",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'pd') and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and instance='123.1.1.4:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and instance='123.1.1.4:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and instance='cNs2dm.tikv.pingcap.com:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("cNs2dm.tikv.pingcap.com:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='TIKV' and instance='cNs2dm.tikv.pingcap.com:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("cNs2dm.tikv.pingcap.com:1234"),
		},
		{
			sql:         "select * from information_schema.cluster_config where type='tikv' and type='pd'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and type in ('pd', 'tikv')",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:         "select * from information_schema.cluster_config where type='tikv' and type in ('pd', 'tidb')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'tidb') and type in ('pd', 'tidb')",
			nodeTypes: set.NewStringSet("tidb"),
			instances: set.NewStringSet(),
		},
		{
			sql:         "select * from information_schema.cluster_config where instance='123.1.1.4:1234' and instance='123.1.1.5:1234'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_config where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:         "select * from information_schema.cluster_config where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_config where instance in ('123.1.1.5:1234', '123.1.1.4:1234') and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			sql: `select * from information_schema.cluster_config
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and type in ('tikv', 'tidb')
				  and type in ('pd', 'tidb')`,
			nodeTypes: set.NewStringSet("tidb"),
			instances: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			sql: `select * from information_schema.cluster_config
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and instance in ('123.1.1.6:1234', '123.1.1.7:1234')
				  and instance in ('123.1.1.7:1234', '123.1.1.8:1234')`,
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
	}
	for _, ca := range cases {
		logicalMemTable := s.getLogicalMemTable(c, se, parser, ca.sql)
		c.Assert(logicalMemTable.Extractor, NotNil)

		clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.ClusterTableExtractor)
		c.Assert(clusterConfigExtractor.NodeTypes, DeepEquals, ca.nodeTypes, Commentf("SQL: %v", ca.sql))
		c.Assert(clusterConfigExtractor.Instances, DeepEquals, ca.instances, Commentf("SQL: %v", ca.sql))
		c.Assert(clusterConfigExtractor.SkipRequest, DeepEquals, ca.skipRequest, Commentf("SQL: %v", ca.sql))
	}
}

func timestamp(c *C, s string) int64 {
	t, err := time.ParseInLocation("2006-01-02 15:04:05.999", s, time.Local)
	c.Assert(err, IsNil)
	return t.UnixNano() / int64(time.Millisecond)
}

func (s *extractorSuite) TestClusterLogTableExtractor(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	parser := parser.New()
	var cases = []struct {
		sql                string
		nodeTypes          set.StringSet
		instances          set.StringSet
		skipRequest        bool
		startTime, endTime int64
		patterns           []string
		level              set.StringSet
	}{
		{
			// Test for invalid time.
			sql:       "select * from information_schema.cluster_log where time='2019-10-10 10::10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where 'tikv'=type",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where 'TiKV'=type",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where 'TiKV'=type or type='tidb'",
			nodeTypes: set.NewStringSet("tikv", "tidb"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where 'TiKV'=type or type='tidb' or type='pd'",
			nodeTypes: set.NewStringSet("tikv", "tidb", "pd"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where (type='tidb' or type='pd') and (instance='123.1.1.2:1234' or instance='123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tidb", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type in ('tikv', 'pd')",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where type in ('tikv', 'pd') and instance='123.1.1.2:1234'",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type in ('tikv', 'pd') and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv' and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv' and instance='123.1.1.4:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv' and instance='123.1.1.4:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv' and instance='cNs2dm.tikv.pingcap.com:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("cNs2dm.tikv.pingcap.com:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='TIKV' and instance='cNs2dm.tikv.pingcap.com:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("cNs2dm.tikv.pingcap.com:1234"),
		},
		{
			sql:         "select * from information_schema.cluster_log where type='tikv' and type='pd'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv' and type in ('pd', 'tikv')",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:         "select * from information_schema.cluster_log where type='tikv' and type in ('pd', 'tidb')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where type in ('tikv', 'tidb') and type in ('pd', 'tidb')",
			nodeTypes: set.NewStringSet("tidb"),
			instances: set.NewStringSet(),
		},
		{
			sql:         "select * from information_schema.cluster_log where instance='123.1.1.4:1234' and instance='123.1.1.5:1234'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:         "select * from information_schema.cluster_log where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where instance in ('123.1.1.5:1234', '123.1.1.4:1234') and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			sql: `select * from information_schema.cluster_log
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and type in ('tikv', 'tidb')
				  and type in ('pd', 'tidb')`,
			nodeTypes: set.NewStringSet("tidb"),
			instances: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			sql: `select * from information_schema.cluster_log
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and instance in ('123.1.1.6:1234', '123.1.1.7:1234')
				  and instance in ('123.1.1.7:1234', '123.1.1.8:1234')`,
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where time='2019-10-10 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(c, "2019-10-10 10:10:10"),
			endTime:   timestamp(c, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and time<='2019-10-11 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(c, "2019-10-10 10:10:10"),
			endTime:   timestamp(c, "2019-10-11 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>'2019-10-10 10:10:10' and time<'2019-10-11 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(c, "2019-10-10 10:10:10") + 1,
			endTime:   timestamp(c, "2019-10-11 10:10:10") - 1,
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and time<'2019-10-11 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(c, "2019-10-10 10:10:10"),
			endTime:   timestamp(c, "2019-10-11 10:10:10") - 1,
		},
		{
			sql:         "select * from information_schema.cluster_log where time>='2019-10-12 10:10:10' and time<'2019-10-11 10:10:10'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			startTime:   timestamp(c, "2019-10-12 10:10:10"),
			endTime:     timestamp(c, "2019-10-11 10:10:10") - 1,
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(c, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and  time>='2019-10-11 10:10:10' and  time>='2019-10-12 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(c, "2019-10-12 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and  time>='2019-10-11 10:10:10' and  time>='2019-10-12 10:10:10' and time='2019-10-13 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(c, "2019-10-13 10:10:10"),
			endTime:   timestamp(c, "2019-10-13 10:10:10"),
		},
		{
			sql:         "select * from information_schema.cluster_log where time<='2019-10-10 10:10:10' and time='2019-10-13 10:10:10'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			startTime:   timestamp(c, "2019-10-13 10:10:10"),
			endTime:     timestamp(c, "2019-10-10 10:10:10"),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where time='2019-10-10 10:10:10' and time<='2019-10-13 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(c, "2019-10-10 10:10:10"),
			endTime:   timestamp(c, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and message like '%a%'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(c, "2019-10-10 10:10:10"),
			patterns:  []string{".*a.*"},
		},
		{
			sql:       "select * from information_schema.cluster_log where message like '%a%' and message regexp '^b'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			patterns:  []string{".*a.*", "^b"},
		},
		{
			sql:       "select * from information_schema.cluster_log where message='gc'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			patterns:  []string{"^gc$"},
		},
		{
			sql:       "select * from information_schema.cluster_log where message='.*txn.*'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			patterns:  []string{"^" + regexp.QuoteMeta(".*txn.*") + "$"},
		},
		{
			sql: `select * from information_schema.cluster_log
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and (type='tidb' or type='pd')
				  and message like '%coprocessor%'
				  and message regexp '.*txn=123.*'
				  and level in ('debug', 'info', 'ERROR')`,
			nodeTypes: set.NewStringSet("tidb", "pd"),
			instances: set.NewStringSet("123.1.1.5:1234", "123.1.1.4:1234"),
			level:     set.NewStringSet("debug", "info", "error"),
			patterns:  []string{".*coprocessor.*", ".*txn=123.*"},
		},
		{
			sql:       "select * from information_schema.cluster_log where (message regexp '.*pd.*' or message regexp '.*tidb.*' or message like '%tikv%')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			patterns:  []string{".*pd.*|.*tidb.*|.*tikv.*"},
		},
		{
			sql:       "select * from information_schema.cluster_log where (level = 'debug' or level = 'ERROR')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			level:     set.NewStringSet("debug", "error"),
		},
	}
	for _, ca := range cases {
		logicalMemTable := s.getLogicalMemTable(c, se, parser, ca.sql)
		c.Assert(logicalMemTable.Extractor, NotNil)

		clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.ClusterLogTableExtractor)
		c.Assert(clusterConfigExtractor.NodeTypes, DeepEquals, ca.nodeTypes, Commentf("SQL: %v", ca.sql))
		c.Assert(clusterConfigExtractor.Instances, DeepEquals, ca.instances, Commentf("SQL: %v", ca.sql))
		c.Assert(clusterConfigExtractor.SkipRequest, DeepEquals, ca.skipRequest, Commentf("SQL: %v", ca.sql))
		if ca.startTime > 0 {
			c.Assert(clusterConfigExtractor.StartTime, Equals, ca.startTime, Commentf("SQL: %v", ca.sql))
		}
		if ca.endTime > 0 {
			c.Assert(clusterConfigExtractor.EndTime, Equals, ca.endTime, Commentf("SQL: %v", ca.sql))
		}
		c.Assert(clusterConfigExtractor.Patterns, DeepEquals, ca.patterns, Commentf("SQL: %v", ca.sql))
		if len(ca.level) > 0 {
			c.Assert(clusterConfigExtractor.LogLevels, DeepEquals, ca.level, Commentf("SQL: %v", ca.sql))
		}
	}
}

func (s *extractorSuite) TestMetricTableExtractor(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	parseTime := func(c *C, s string) time.Time {
		t, err := time.ParseInLocation(plannercore.MetricTableTimeFormat, s, time.Local)
		c.Assert(err, IsNil)
		return t
	}

	parser := parser.New()
	var cases = []struct {
		sql                string
		skipRequest        bool
		startTime, endTime time.Time
		labelConditions    map[string]set.StringSet
		quantiles          []float64
		promQL             string
	}{
		{
			sql:    "select * from metrics_schema.tidb_query_duration",
			promQL: "histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
		},
		{
			sql:    "select * from metrics_schema.tidb_query_duration where instance='127.0.0.1:10080'",
			promQL: `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{instance="127.0.0.1:10080"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080"),
			},
		},
		{
			sql:    "select * from metrics_schema.tidb_query_duration where instance='127.0.0.1:10080' or instance='127.0.0.1:10081'",
			promQL: `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{instance=~"127.0.0.1:10080|127.0.0.1:10081"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080", "127.0.0.1:10081"),
			},
		},
		{
			sql:    "select * from metrics_schema.tidb_query_duration where instance='127.0.0.1:10080' and sql_type='general'",
			promQL: `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{instance="127.0.0.1:10080",sql_type="general"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080"),
				"sql_type": set.NewStringSet("general"),
			},
		},
		{
			sql:    "select * from metrics_schema.tidb_query_duration where instance='127.0.0.1:10080' or sql_type='general'",
			promQL: `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))`,
		},
		{
			sql:    "select * from metrics_schema.tidb_query_duration where instance='127.0.0.1:10080' and sql_type='Update' and time='2019-10-10 10:10:10'",
			promQL: `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{instance="127.0.0.1:10080",sql_type="Update"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080"),
				"sql_type": set.NewStringSet("Update"),
			},
			startTime: parseTime(c, "2019-10-10 10:10:10"),
			endTime:   parseTime(c, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from metrics_schema.tidb_query_duration where time>'2019-10-10 10:10:10' and time<'2019-10-11 10:10:10'",
			promQL:    `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))`,
			startTime: parseTime(c, "2019-10-10 10:10:10.001"),
			endTime:   parseTime(c, "2019-10-11 10:10:09.999"),
		},
		{
			sql:       "select * from metrics_schema.tidb_query_duration where time>='2019-10-10 10:10:10'",
			promQL:    `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))`,
			startTime: parseTime(c, "2019-10-10 10:10:10"),
			endTime:   parseTime(c, "2019-10-10 10:20:10"),
		},
		{
			sql:         "select * from metrics_schema.tidb_query_duration where time>='2019-10-10 10:10:10' and time<='2019-10-09 10:10:10'",
			promQL:      "",
			startTime:   parseTime(c, "2019-10-10 10:10:10"),
			endTime:     parseTime(c, "2019-10-09 10:10:10"),
			skipRequest: true,
		},
		{
			sql:       "select * from metrics_schema.tidb_query_duration where time<='2019-10-09 10:10:10'",
			promQL:    "histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
			startTime: parseTime(c, "2019-10-09 10:00:10"),
			endTime:   parseTime(c, "2019-10-09 10:10:10"),
		},
		{
			sql: "select * from metrics_schema.tidb_query_duration where quantile=0.9 or quantile=0.8",
			promQL: "histogram_quantile(0.8, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))," +
				"histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
			quantiles: []float64{0.8, 0.9},
		},
		{
			sql:       "select * from metrics_schema.tidb_query_duration where quantile=0",
			promQL:    "histogram_quantile(0, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
			quantiles: []float64{0},
		},
	}
	se.GetSessionVars().StmtCtx.TimeZone = time.Local
	for _, ca := range cases {
		logicalMemTable := s.getLogicalMemTable(c, se, parser, ca.sql)
		c.Assert(logicalMemTable.Extractor, NotNil)
		metricTableExtractor := logicalMemTable.Extractor.(*plannercore.MetricTableExtractor)
		if len(ca.labelConditions) > 0 {
			c.Assert(metricTableExtractor.LabelConditions, DeepEquals, ca.labelConditions, Commentf("SQL: %v", ca.sql))
		}
		c.Assert(metricTableExtractor.SkipRequest, DeepEquals, ca.skipRequest, Commentf("SQL: %v", ca.sql))
		if len(metricTableExtractor.Quantiles) > 0 {
			c.Assert(metricTableExtractor.Quantiles, DeepEquals, ca.quantiles)
		}
		if !ca.skipRequest {
			promQL := metricTableExtractor.GetMetricTablePromQL(se, "tidb_query_duration")
			c.Assert(promQL, DeepEquals, ca.promQL, Commentf("SQL: %v", ca.sql))
			start, end := metricTableExtractor.StartTime, metricTableExtractor.EndTime
			c.Assert(start.UnixNano() <= end.UnixNano(), IsTrue)
			if ca.startTime.Unix() > 0 {
				c.Assert(metricTableExtractor.StartTime, DeepEquals, ca.startTime, Commentf("SQL: %v, start_time: %v", ca.sql, metricTableExtractor.StartTime))
			}
			if ca.endTime.Unix() > 0 {
				c.Assert(metricTableExtractor.EndTime, DeepEquals, ca.endTime, Commentf("SQL: %v, end_time: %v", ca.sql, metricTableExtractor.EndTime))
			}
		}
	}
}

func (s *extractorSuite) TestMetricsSummaryTableExtractor(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	var cases = []struct {
		sql         string
		names       set.StringSet
		quantiles   []float64
		skipRequest bool
	}{
		{
			sql: "select * from information_schema.metrics_summary",
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile='0.999'",
			quantiles: []float64{0.999},
		},
		{
			sql:       "select * from information_schema.metrics_summary where '0.999'=quantile or quantile='0.95'",
			quantiles: []float64{0.999, 0.95},
		},
		{
			sql:       "select * from information_schema.metrics_summary where '0.999'=quantile or quantile='0.95' or quantile='0.99'",
			quantiles: []float64{0.999, 0.95, 0.99},
		},
		{
			sql:       "select * from information_schema.metrics_summary where (quantile='0.95' or quantile='0.99') and (metrics_name='metric_name3' or metrics_name='metric_name1')",
			quantiles: []float64{0.95, 0.99},
			names:     set.NewStringSet("metric_name3", "metric_name1"),
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile in ('0.999', '0.99')",
			quantiles: []float64{0.999, 0.99},
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile in ('0.999', '0.99') and metrics_name='metric_name1'",
			quantiles: []float64{0.999, 0.99},
			names:     set.NewStringSet("metric_name1"),
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile in ('0.999', '0.99') and metrics_name in ('metric_name1', 'metric_name2')",
			quantiles: []float64{0.999, 0.99},
			names:     set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile='0.999' and metrics_name in ('metric_name1', 'metric_name2')",
			quantiles: []float64{0.999},
			names:     set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile='0.999' and metrics_name='metric_NAME3'",
			quantiles: []float64{0.999},
			names:     set.NewStringSet("metric_name3"),
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile='0.999' and quantile in ('0.99', '0.999')",
			quantiles: []float64{0.999},
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile in ('0.999', '0.95') and quantile in ('0.99', '0.95')",
			quantiles: []float64{0.95},
		},
		{
			sql: `select * from information_schema.metrics_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and quantile in ('0.999', '0.95')
				  and quantile in ('0.99', '0.95')
				  and quantile in (0.80, 0.90)`,
			skipRequest: true,
		},
		{
			sql: `select * from information_schema.metrics_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name1')
				  and metrics_name in ('metric_name1', 'metric_name3')`,
			skipRequest: true,
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		sort.Float64s(ca.quantiles)

		logicalMemTable := s.getLogicalMemTable(c, se, parser, ca.sql)
		c.Assert(logicalMemTable.Extractor, NotNil)

		extractor := logicalMemTable.Extractor.(*plannercore.MetricSummaryTableExtractor)
		if len(ca.quantiles) > 0 {
			c.Assert(extractor.Quantiles, DeepEquals, ca.quantiles, Commentf("SQL: %v", ca.sql))
		}
		if len(ca.names) > 0 {
			c.Assert(extractor.MetricsNames, DeepEquals, ca.names, Commentf("SQL: %v", ca.sql))
		}
		c.Assert(extractor.SkipRequest, Equals, ca.skipRequest, Commentf("SQL: %v", ca.sql))
	}
}

func (s *extractorSuite) TestInspectionResultTableExtractor(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	var cases = []struct {
		sql            string
		rules          set.StringSet
		items          set.StringSet
		skipInspection bool
	}{
		{
			sql: "select * from information_schema.inspection_result",
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl'",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_result where 'ddl'=rule",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_result where 'ddl'=rule",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_result where 'ddl'=rule or rule='config'",
			rules: set.NewStringSet("ddl", "config"),
		},
		{
			sql:   "select * from information_schema.inspection_result where 'ddl'=rule or rule='config' or rule='slow_query'",
			rules: set.NewStringSet("ddl", "config", "slow_query"),
		},
		{
			sql:   "select * from information_schema.inspection_result where (rule='config' or rule='slow_query') and (item='ddl.owner' or item='ddl.lease')",
			rules: set.NewStringSet("config", "slow_query"),
			items: set.NewStringSet("ddl.owner", "ddl.lease"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule in ('ddl', 'slow_query')",
			rules: set.NewStringSet("ddl", "slow_query"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule in ('ddl', 'slow_query') and item='ddl.lease'",
			rules: set.NewStringSet("ddl", "slow_query"),
			items: set.NewStringSet("ddl.lease"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule in ('ddl', 'slow_query') and item in ('ddl.lease', '123.1.1.4:1234')",
			rules: set.NewStringSet("ddl", "slow_query"),
			items: set.NewStringSet("ddl.lease", "123.1.1.4:1234"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and item in ('ddl.lease', '123.1.1.4:1234')",
			rules: set.NewStringSet("ddl"),
			items: set.NewStringSet("ddl.lease", "123.1.1.4:1234"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and item='123.1.1.4:1234'",
			rules: set.NewStringSet("ddl"),
			items: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and item='123.1.1.4:1234'",
			rules: set.NewStringSet("ddl"),
			items: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and item='DDL.lease'",
			rules: set.NewStringSet("ddl"),
			items: set.NewStringSet("ddl.lease"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and item='ddl.OWNER'",
			rules: set.NewStringSet("ddl"),
			items: set.NewStringSet("ddl.owner"),
		},
		{
			sql:            "select * from information_schema.inspection_result where rule='ddl' and rule='slow_query'",
			skipInspection: true,
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and rule in ('slow_query', 'ddl')",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule in ('ddl', 'config') and rule in ('slow_query', 'config')",
			rules: set.NewStringSet("config"),
		},
		{
			sql:            "select * from information_schema.inspection_result where item='ddl.lease' and item='raftstore.threadpool'",
			skipInspection: true,
		},
		{
			sql:   "select * from information_schema.inspection_result where item='raftstore.threadpool' and item in ('raftstore.threadpool', 'ddl.lease')",
			items: set.NewStringSet("raftstore.threadpool"),
		},
		{
			sql:            "select * from information_schema.inspection_result where item='raftstore.threadpool' and item in ('ddl.lease', 'scheduler.limit')",
			skipInspection: true,
		},
		{
			sql:   "select * from information_schema.inspection_result where item in ('ddl.lease', 'scheduler.limit') and item in ('raftstore.threadpool', 'scheduler.limit')",
			items: set.NewStringSet("scheduler.limit"),
		},
		{
			sql: `select * from information_schema.inspection_result
				where item in ('ddl.lease', 'scheduler.limit')
				  and item in ('raftstore.threadpool', 'scheduler.limit')
				  and rule in ('ddl', 'config')
				  and rule in ('slow_query', 'config')`,
			rules: set.NewStringSet("config"),
			items: set.NewStringSet("scheduler.limit"),
		},
		{
			sql: `select * from information_schema.inspection_result
				where item in ('ddl.lease', 'scheduler.limit')
				  and item in ('raftstore.threadpool', 'scheduler.limit')
				  and item in ('raftstore.threadpool', 'ddl.lease')
				  and item in ('ddl.lease', 'ddl.owner')`,
			skipInspection: true,
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable := s.getLogicalMemTable(c, se, parser, ca.sql)
		c.Assert(logicalMemTable.Extractor, NotNil)

		clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.InspectionResultTableExtractor)
		if len(ca.rules) > 0 {
			c.Assert(clusterConfigExtractor.Rules, DeepEquals, ca.rules, Commentf("SQL: %v", ca.sql))
		}
		if len(ca.items) > 0 {
			c.Assert(clusterConfigExtractor.Items, DeepEquals, ca.items, Commentf("SQL: %v", ca.sql))
		}
		c.Assert(clusterConfigExtractor.SkipInspection, Equals, ca.skipInspection, Commentf("SQL: %v", ca.sql))
	}
}

func (s *extractorSuite) TestInspectionSummaryTableExtractor(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	var cases = []struct {
		sql            string
		rules          set.StringSet
		names          set.StringSet
		quantiles      set.Float64Set
		skipInspection bool
	}{
		{
			sql: "select * from information_schema.inspection_summary",
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule='ddl'",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where 'ddl'=rule or rule='config'",
			rules: set.NewStringSet("ddl", "config"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where 'ddl'=rule or rule='config' or rule='slow_query'",
			rules: set.NewStringSet("ddl", "config", "slow_query"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where (rule='config' or rule='slow_query') and (metrics_name='metric_name3' or metrics_name='metric_name1')",
			rules: set.NewStringSet("config", "slow_query"),
			names: set.NewStringSet("metric_name3", "metric_name1"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule in ('ddl', 'slow_query')",
			rules: set.NewStringSet("ddl", "slow_query"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule in ('ddl', 'slow_query') and metrics_name='metric_name1'",
			rules: set.NewStringSet("ddl", "slow_query"),
			names: set.NewStringSet("metric_name1"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule in ('ddl', 'slow_query') and metrics_name in ('metric_name1', 'metric_name2')",
			rules: set.NewStringSet("ddl", "slow_query"),
			names: set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule='ddl' and metrics_name in ('metric_name1', 'metric_name2')",
			rules: set.NewStringSet("ddl"),
			names: set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule='ddl' and metrics_name='metric_NAME3'",
			rules: set.NewStringSet("ddl"),
			names: set.NewStringSet("metric_name3"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule='ddl' and rule in ('slow_query', 'ddl')",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule in ('ddl', 'config') and rule in ('slow_query', 'config')",
			rules: set.NewStringSet("config"),
		},
		{
			sql: `select * from information_schema.inspection_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and rule in ('ddl', 'config')
				  and rule in ('slow_query', 'config')
				  and quantile in (0.80, 0.90)`,
			rules:     set.NewStringSet("config"),
			names:     set.NewStringSet("metric_name4"),
			quantiles: set.NewFloat64Set(0.80, 0.90),
		},
		{
			sql: `select * from information_schema.inspection_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name1')
				  and metrics_name in ('metric_name1', 'metric_name3')`,
			skipInspection: true,
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable := s.getLogicalMemTable(c, se, parser, ca.sql)
		c.Assert(logicalMemTable.Extractor, NotNil)

		clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.InspectionSummaryTableExtractor)
		if len(ca.rules) > 0 {
			c.Assert(clusterConfigExtractor.Rules, DeepEquals, ca.rules, Commentf("SQL: %v", ca.sql))
		}
		if len(ca.names) > 0 {
			c.Assert(clusterConfigExtractor.MetricNames, DeepEquals, ca.names, Commentf("SQL: %v", ca.sql))
		}
		c.Assert(clusterConfigExtractor.SkipInspection, Equals, ca.skipInspection, Commentf("SQL: %v", ca.sql))
	}
}

func (s *extractorSuite) TestInspectionRuleTableExtractor(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	var cases = []struct {
		sql  string
		tps  set.StringSet
		skip bool
	}{
		{
			sql: "select * from information_schema.inspection_rules",
		},
		{
			sql: "select * from information_schema.inspection_rules where type='inspection'",
			tps: set.NewStringSet("inspection"),
		},
		{
			sql: "select * from information_schema.inspection_rules where type='inspection' or type='summary'",
			tps: set.NewStringSet("inspection", "summary"),
		},
		{
			sql:  "select * from information_schema.inspection_rules where type='inspection' and type='summary'",
			skip: true,
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable := s.getLogicalMemTable(c, se, parser, ca.sql)
		c.Assert(logicalMemTable.Extractor, NotNil)

		clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.InspectionRuleTableExtractor)
		if len(ca.tps) > 0 {
			c.Assert(clusterConfigExtractor.Types, DeepEquals, ca.tps, Commentf("SQL: %v", ca.sql))
		}
		c.Assert(clusterConfigExtractor.SkipRequest, Equals, ca.skip, Commentf("SQL: %v", ca.sql))
	}
}

func (s *extractorSuite) TestTiDBHotRegionsHistoryTableExtractor(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	parser := parser.New()
	var cases = []struct {
		sql                             string
		skipRequest                     bool
		startTime, endTime              int64
		hotRegionTypes                  set.StringSet
		regionIDs, storeIDs             []uint64
		tableIDs, indexIDs              []uint64
		dbNames, tableNames, indexNames set.StringSet
		lowHotDegree, highHotDegree     int64
		lowFlowBytes, highFlowBytes     float64
	}{
		// Test full data, will not call Extract(),and executor(retriver) will panic and remind user to add conditions to save network IO.
		{
			sql: "select * from information_schema.tidb_hot_regions_history",
		},
		// Test startTime and endTime
		{
			// Test for invalid update_time.
			// Fixme Simulated from cluster_log, SQL Parser can parse the error time formate in fact, timestamp() function can not.
			// It rerurn start_time = end_time !=0 due to the implementation of extractHelper.extractTimeRange.
			sql: "select * from information_schema.tidb_hot_regions_history where update_time='2019-10-10 10::10'",
			// highHotDegree,highFlowBytes will be set to the maximum number of the type to cover all range.
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where update_time='2019-10-10 10:10:10'",
			startTime:     timestamp(c, "2019-10-10 10:10:10"),
			endTime:       timestamp(c, "2019-10-10 10:10:10"),
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and update_time<='2019-10-11 10:10:10'",
			startTime:     timestamp(c, "2019-10-10 10:10:10"),
			endTime:       timestamp(c, "2019-10-11 10:10:10"),
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where update_time>'2019-10-10 10:10:10' and update_time<'2019-10-11 10:10:10'",
			startTime:     timestamp(c, "2019-10-10 10:10:10") + 1,
			endTime:       timestamp(c, "2019-10-11 10:10:10") - 1,
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and update_time<'2019-10-11 10:10:10'",
			startTime:     timestamp(c, "2019-10-10 10:10:10"),
			endTime:       timestamp(c, "2019-10-11 10:10:10") - 1,
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-12 10:10:10' and update_time<'2019-10-11 10:10:10'",
			startTime:     timestamp(c, "2019-10-12 10:10:10"),
			endTime:       timestamp(c, "2019-10-11 10:10:10") - 1,
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			skipRequest:   true,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10'",
			startTime:     timestamp(c, "2019-10-10 10:10:10"),
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and  update_time>='2019-10-11 10:10:10' and  update_time>='2019-10-12 10:10:10'",
			startTime:     timestamp(c, "2019-10-12 10:10:10"),
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and  update_time>='2019-10-11 10:10:10' and  update_time>='2019-10-12 10:10:10' and update_time='2019-10-13 10:10:10'",
			startTime:     timestamp(c, "2019-10-13 10:10:10"),
			endTime:       timestamp(c, "2019-10-13 10:10:10"),
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where update_time<='2019-10-10 10:10:10' and update_time='2019-10-13 10:10:10'",
			startTime:     timestamp(c, "2019-10-13 10:10:10"),
			endTime:       timestamp(c, "2019-10-10 10:10:10"),
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			skipRequest:   true,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where update_time='2019-10-10 10:10:10' and update_time<='2019-10-13 10:10:10'",
			startTime:     timestamp(c, "2019-10-10 10:10:10"),
			endTime:       timestamp(c, "2019-10-10 10:10:10"),
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
		// Test hotRegionTypes
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where type='read'",
			hotRegionTypes: set.NewStringSet("read"),
			highHotDegree:  math.MaxInt64,
			highFlowBytes:  math.MaxFloat64,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where type in('read')",
			hotRegionTypes: set.NewStringSet("read"),
			highHotDegree:  math.MaxInt64,
			highFlowBytes:  math.MaxFloat64,
		},
		{
			sql: "select * from information_schema.tidb_hot_regions_history where type='read' and type='write'",
			// skip set maximum code
			// highHotDegree: math.MaxInt64,
			// highFlowBytes: math.MaxFloat64,
			skipRequest: true,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where type='read' and type in ('read', 'write')",
			hotRegionTypes: set.NewStringSet("read"),
			highHotDegree:  math.MaxInt64,
			highFlowBytes:  math.MaxFloat64,
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where type in ('read') and type in ('write')",
			skipRequest: true,
		},
		// Test regionIDs and storeIDs
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where region_id=100",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100},
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where 100=region_id",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100},
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where 100=region_id or region_id=101",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100, 101},
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where 100=region_id or region_id=101 or region_id=102 or 103 = region_id",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100, 101, 102, 103},
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where (region_id=100 or region_id=101) and (store_id=200 or store_id=201)",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100, 101},
			storeIDs:      []uint64{200, 201},
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where region_id in (100, 101)",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100, 101},
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where region_id in (100, 101) and store_id=200",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100, 101},
			storeIDs:      []uint64{200},
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where region_id in (100, 101) and store_id in (200, 201)",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100, 101},
			storeIDs:      []uint64{200, 201},
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where region_id=100 and store_id in (200, 201)",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100},
			storeIDs:      []uint64{200, 201},
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where region_id=100 and store_id=200",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100},
			storeIDs:      []uint64{200},
		},
		{ // and case
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id=101",
			skipRequest: true,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (100,101)",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100},
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (100,101) and store_id=200 and store_id in (200,201)",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{100},
			storeIDs:      []uint64{200},
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (101,102)",
			skipRequest: true,
		},
		{ // region_id skip, store_id not skip
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (101,102) and store_id=200 and store_id in (200,201)",
			skipRequest: true,
		},
		{ // region_id not skip, store_id skip
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (100,101) and store_id=200 and store_id in (201,202)",
			skipRequest: true,
		},
		{ // region_id skip, store_id not skip, table_id not skip, index_id not skip
			sql: `select * from information_schema.tidb_hot_regions_history 
								where region_id=100 and region_id in (100,101) 
								and store_id=200 and store_id in (201,202)
								and table_id=10 and store_id in (11,12)
								and index_id=20 and index_id in (21,22)								`,
			skipRequest: true,
		},
		{ // Intersection
			sql:           "select * from information_schema.tidb_hot_regions_history where region_id in (100,101) and region_id in (101,102)",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{101},
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history 
							where region_id in (100,101) 
							and region_id in (101,102) 
							and store_id in (200,201) 
							and store_id in (201,202)`,
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{101},
			storeIDs:      []uint64{201},
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history 
							where region_id in (100,101) 
							and region_id in (101,102) 
							and store_id in (200,201) 
							and store_id in (201,202)
							and table_id in (10,11)
							and table_id in (11,12)
							and index_id in (20,21)
							and index_id in (21,22)`,
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			regionIDs:     []uint64{101},
			storeIDs:      []uint64{201},
			tableIDs:      []uint64{11},
			indexIDs:      []uint64{21},
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history 
							where region_id in (100,101) 
							and region_id in (100,102) 
							and region_id in (102,103) 
							and region_id in (103,104)`,
			skipRequest: true,
		},
		// Test dbNames, tableNames and indexNames
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where db_name='database1'",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1"),
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where 'database1'=db_name",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1"),
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where 'database1'=db_name or db_name='database2'",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1", "database2"),
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where 'database1'=db_name or db_name='database2' or db_name='database3' or 'database4' = db_name",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1", "database2", "database3", "database4"),
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where (db_name='database1' or db_name='database2') and (table_name='table1' or table_name='table2')",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1", "database2"),
			tableNames:    set.NewStringSet("table1", "table2"),
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where db_name in ('database1', 'database2')",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1", "database2"),
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where db_name in ('database1', 'database2') and table_name='table1'",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1", "database2"),
			tableNames:    set.NewStringSet("table1"),
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where db_name in ('database1', 'database2') and table_name in ('table1', 'table2')",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1", "database2"),
			tableNames:    set.NewStringSet("table1", "table2"),
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where db_name='database1' and table_name in ('table1', 'table2')",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1"),
			tableNames:    set.NewStringSet("table1", "table2"),
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where db_name='database1' and table_name='table1'",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1"),
			tableNames:    set.NewStringSet("table1"),
		},
		{ // and case
			sql:         "select * from information_schema.tidb_hot_regions_history where db_name='database1' and db_name='database2'",
			skipRequest: true,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where db_name='database1' and db_name in ('database1','database2')",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1"),
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where db_name='database1' and db_name in ('database1','database2') and table_name='table1' and table_name in ('table1','table2')",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1"),
			tableNames:    set.NewStringSet("table1"),
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history 
							where db_name='database1' and db_name in ('database1','database2') 
							and table_name='table1' and table_name in ('table1','table2')
							and index_name='index1' and index_name in ('index1','index2')`,
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database1"),
			tableNames:    set.NewStringSet("table1"),
			indexNames:    set.NewStringSet("index1"),
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where db_name='database1' and db_name in ('database2','database3')",
			skipRequest: true,
		},
		{ // db_name skip, table_name not skip
			sql:         "select * from information_schema.tidb_hot_regions_history where db_name='database1' and db_name in ('database2','database3') and table_name='table1' and table_name in ('table1','table2')",
			skipRequest: true,
		},
		{ // db_name not skip, table_name skip
			sql:         "select * from information_schema.tidb_hot_regions_history where db_name='database1' and db_name in ('database1','database2') and table_name='table1' and table_name in ('table2','table3')",
			skipRequest: true,
		},
		{ // db_name skip, table_name not skip, index_name skip
			sql: `select * from information_schema.tidb_hot_regions_history 
							where db_name='database1' and db_name in ('database2','database3') 
							and table_name='table1' and table_name in ('table1','table2')
							and index_name='index1' and index_name in ('index1','index2')`,
			skipRequest: true,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where db_name in ('database1','database2') and db_name in ('database2','database3')",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database2"),
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history 
							where db_name in ('database1','database2') 
							and db_name in ('database2','database3') 
							and table_name in ('table1','table2') 
							and table_name in ('table2','table3')`,
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database2"),
			tableNames:    set.NewStringSet("table2"),
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history 
							where db_name in ('database1','database2') 
							and db_name in ('database2','database3') 
							and table_name in ('table1','table2') 
							and table_name in ('table2','table3')
							and index_name in ('index1','index2')
							and index_name in ('index2','index3')`,
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
			dbNames:       set.NewStringSet("database2"),
			tableNames:    set.NewStringSet("table2"),
			indexNames:    set.NewStringSet("index2"),
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history 
							where db_name in ('database1','database2') 
							and db_name in ('database1','database3') 
							and db_name in ('database3','database4') 
							and db_name in ('database4','database5')`,
			skipRequest: true,
		},
		// Test lowHotDegree, highHotDegree, lowFlowBytes, highFlowBytes
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where hot_degree > 89",
			lowHotDegree:  89 + 1,
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where hot_degree < 100",
			highHotDegree: 100 - 1,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where flow_bytes > 89",
			highHotDegree: math.MaxInt64,
			lowFlowBytes:  89 + 1e-12,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where flow_bytes < 100",
			highHotDegree: math.MaxInt64,
			highFlowBytes: 100 - 1e-12,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where (hot_degree=80 or hot_degree=100) and hot_degree > 89",
			lowHotDegree:  89 + 1,
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where hot_degree > 89  and hot_degree<=100",
			lowHotDegree:  89 + 1,
			highHotDegree: 100,
			highFlowBytes: math.MaxFloat64,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where hot_degree > 89  and hot_degree<=100 and flow_bytes >= 89 and flow_bytes <100",
			lowHotDegree:  89 + 1,
			highHotDegree: 100,
			lowFlowBytes:  89,
			highFlowBytes: 100 - 1e-12,
		},
		{
			sql:           "select * from information_schema.tidb_hot_regions_history where hot_degree=100 and flow_bytes = 89",
			highHotDegree: math.MaxInt64,
			highFlowBytes: math.MaxFloat64,
		},
	}

	for _, ca := range cases {
		logicalMemTable := s.getLogicalMemTable(c, se, parser, ca.sql)
		c.Assert(logicalMemTable.Extractor, NotNil, Commentf("SQL: %v", ca.sql))

		hotRegionsHistoryExtractor := logicalMemTable.Extractor.(*plannercore.HotRegionsHistoryTableExtractor)
		if ca.startTime > 0 {
			c.Assert(hotRegionsHistoryExtractor.StartTime, Equals, ca.startTime, Commentf("SQL: %v", ca.sql))
		}
		if ca.endTime > 0 {
			c.Assert(hotRegionsHistoryExtractor.EndTime, Equals, ca.endTime, Commentf("SQL: %v", ca.sql))
		}
		c.Assert(hotRegionsHistoryExtractor.SkipRequest, DeepEquals, ca.skipRequest, Commentf("SQL: %v", ca.sql))
		if len(ca.hotRegionTypes) > 0 {
			c.Assert(hotRegionsHistoryExtractor.HotRegionTypes, DeepEquals, ca.hotRegionTypes, Commentf("SQL: %v", ca.sql))
		}
		// ues length to avoid case uint64{} != uint64(nil)
		if len(ca.regionIDs) > 0 {
			c.Assert(hotRegionsHistoryExtractor.RegionIDs, DeepEquals, ca.regionIDs, Commentf("SQL: %v", ca.sql))
		}
		if len(ca.storeIDs) > 0 {
			c.Assert(hotRegionsHistoryExtractor.StoreIDs, DeepEquals, ca.storeIDs, Commentf("SQL: %v", ca.sql))
		}
		if len(ca.tableIDs) > 0 {
			c.Assert(hotRegionsHistoryExtractor.TableIDs, DeepEquals, ca.tableIDs, Commentf("SQL: %v", ca.sql))
		}
		if len(ca.indexIDs) > 0 {
			c.Assert(hotRegionsHistoryExtractor.IndexIDs, DeepEquals, ca.indexIDs, Commentf("SQL: %v", ca.sql))
		}
		if len(ca.dbNames) > 0 {
			c.Assert(hotRegionsHistoryExtractor.DBNames, DeepEquals, ca.dbNames, Commentf("SQL: %v", ca.sql))
		}
		if len(ca.tableNames) > 0 {
			c.Assert(hotRegionsHistoryExtractor.TableNames, DeepEquals, ca.tableNames, Commentf("SQL: %v", ca.sql))
		}
		if len(ca.indexNames) > 0 {
			c.Assert(hotRegionsHistoryExtractor.IndexNames, DeepEquals, ca.indexNames, Commentf("SQL: %v", ca.sql))
		}
		c.Assert(hotRegionsHistoryExtractor.LowHotDegree, DeepEquals, ca.lowHotDegree)
		c.Assert(hotRegionsHistoryExtractor.HighHotDegree, DeepEquals, ca.highHotDegree)
		c.Assert(hotRegionsHistoryExtractor.LowFlowBytes, DeepEquals, ca.lowFlowBytes)
		c.Assert(hotRegionsHistoryExtractor.HighFlowBytes, DeepEquals, ca.highFlowBytes)
	}
}
