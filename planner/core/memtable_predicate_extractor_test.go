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
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/set"
	"github.com/stretchr/testify/require"
)

func getLogicalMemTable(t *testing.T, dom *domain.Domain, se session.Session, parser *parser.Parser, sql string) *plannercore.LogicalMemTable {
	stmt, err := parser.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	ctx := context.Background()
	builder, _ := plannercore.NewPlanBuilder().Init(se, dom.InfoSchema(), &hint.BlockHintProcessor{})
	plan, err := builder.Build(ctx, stmt)
	require.NoError(t, err)

	logicalPlan, err := plannercore.LogicalOptimize(ctx, builder.GetOptFlag(), plan.(plannercore.LogicalPlan))
	require.NoError(t, err)

	// Obtain the leaf plan
	leafPlan := logicalPlan
	for len(leafPlan.Children()) > 0 {
		leafPlan = leafPlan.Children()[0]
	}

	logicalMemTable := leafPlan.(*plannercore.LogicalMemTable)
	return logicalMemTable
}

func TestClusterConfigTableExtractor(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

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
		logicalMemTable := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor)

		clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.ClusterTableExtractor)
		require.EqualValues(t, ca.nodeTypes, clusterConfigExtractor.NodeTypes, "SQL: %v", ca.sql)
		require.EqualValues(t, ca.instances, clusterConfigExtractor.Instances, "SQL: %v", ca.sql)
		require.EqualValues(t, ca.skipRequest, clusterConfigExtractor.SkipRequest, "SQL: %v", ca.sql)
	}
}

func timestamp(t *testing.T, s string) int64 {
	tt, err := time.ParseInLocation("2006-01-02 15:04:05.999", s, time.Local)
	require.NoError(t, err)
	return tt.UnixNano() / int64(time.Millisecond)
}

func TestClusterLogTableExtractor(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

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
			sql:       "select * from information_schema.cluster_log",
			nodeTypes: nil,
			instances: nil,
		},
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
			startTime: timestamp(t, "2019-10-10 10:10:10"),
			endTime:   timestamp(t, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and time<='2019-10-11 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10"),
			endTime:   timestamp(t, "2019-10-11 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>'2019-10-10 10:10:10' and time<'2019-10-11 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10") + 1,
			endTime:   timestamp(t, "2019-10-11 10:10:10") - 1,
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and time<'2019-10-11 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10"),
			endTime:   timestamp(t, "2019-10-11 10:10:10") - 1,
		},
		{
			sql:         "select * from information_schema.cluster_log where time>='2019-10-12 10:10:10' and time<'2019-10-11 10:10:10'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			startTime:   timestamp(t, "2019-10-12 10:10:10"),
			endTime:     timestamp(t, "2019-10-11 10:10:10") - 1,
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and  time>='2019-10-11 10:10:10' and  time>='2019-10-12 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-12 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and  time>='2019-10-11 10:10:10' and  time>='2019-10-12 10:10:10' and time='2019-10-13 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-13 10:10:10"),
			endTime:   timestamp(t, "2019-10-13 10:10:10"),
		},
		{
			sql:         "select * from information_schema.cluster_log where time<='2019-10-10 10:10:10' and time='2019-10-13 10:10:10'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			startTime:   timestamp(t, "2019-10-13 10:10:10"),
			endTime:     timestamp(t, "2019-10-10 10:10:10"),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where time='2019-10-10 10:10:10' and time<='2019-10-13 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10"),
			endTime:   timestamp(t, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and message like '%a%'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10"),
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
		logicalMemTable := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor)

		clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.ClusterLogTableExtractor)
		require.EqualValues(t, ca.nodeTypes, clusterConfigExtractor.NodeTypes, "SQL: %v", ca.sql)
		require.EqualValues(t, ca.instances, clusterConfigExtractor.Instances, "SQL: %v", ca.sql)
		require.EqualValues(t, ca.skipRequest, clusterConfigExtractor.SkipRequest, "SQL: %v", ca.sql)
		if ca.startTime > 0 {
			require.Equal(t, ca.startTime, clusterConfigExtractor.StartTime, "SQL: %v", ca.sql)
		}
		if ca.endTime > 0 {
			require.Equal(t, ca.endTime, clusterConfigExtractor.EndTime, "SQL: %v", ca.sql)
		}
		require.EqualValues(t, ca.patterns, clusterConfigExtractor.Patterns, "SQL: %v", ca.sql)
		if len(ca.level) > 0 {
			require.EqualValues(t, ca.level, clusterConfigExtractor.LogLevels, "SQL: %v", ca.sql)
		}
	}
}

func TestMetricTableExtractor(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	parseTime := func(t *testing.T, s string) time.Time {
		tt, err := time.ParseInLocation(plannercore.MetricTableTimeFormat, s, time.Local)
		require.NoError(t, err)
		return tt
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
			startTime: parseTime(t, "2019-10-10 10:10:10"),
			endTime:   parseTime(t, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from metrics_schema.tidb_query_duration where time>'2019-10-10 10:10:10' and time<'2019-10-11 10:10:10'",
			promQL:    `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))`,
			startTime: parseTime(t, "2019-10-10 10:10:10.001"),
			endTime:   parseTime(t, "2019-10-11 10:10:09.999"),
		},
		{
			sql:       "select * from metrics_schema.tidb_query_duration where time>='2019-10-10 10:10:10'",
			promQL:    `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))`,
			startTime: parseTime(t, "2019-10-10 10:10:10"),
			endTime:   parseTime(t, "2019-10-10 10:20:10"),
		},
		{
			sql:         "select * from metrics_schema.tidb_query_duration where time>='2019-10-10 10:10:10' and time<='2019-10-09 10:10:10'",
			promQL:      "",
			startTime:   parseTime(t, "2019-10-10 10:10:10"),
			endTime:     parseTime(t, "2019-10-09 10:10:10"),
			skipRequest: true,
		},
		{
			sql:       "select * from metrics_schema.tidb_query_duration where time<='2019-10-09 10:10:10'",
			promQL:    "histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
			startTime: parseTime(t, "2019-10-09 10:00:10"),
			endTime:   parseTime(t, "2019-10-09 10:10:10"),
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
		logicalMemTable := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor)
		metricTableExtractor := logicalMemTable.Extractor.(*plannercore.MetricTableExtractor)
		if len(ca.labelConditions) > 0 {
			require.EqualValues(t, ca.labelConditions, metricTableExtractor.LabelConditions, "SQL: %v", ca.sql)
		}
		require.EqualValues(t, ca.skipRequest, metricTableExtractor.SkipRequest, "SQL: %v", ca.sql)
		if len(metricTableExtractor.Quantiles) > 0 {
			require.EqualValues(t, ca.quantiles, metricTableExtractor.Quantiles)
		}
		if !ca.skipRequest {
			promQL := metricTableExtractor.GetMetricTablePromQL(se, "tidb_query_duration")
			require.EqualValues(t, promQL, ca.promQL, "SQL: %v", ca.sql)
			start, end := metricTableExtractor.StartTime, metricTableExtractor.EndTime
			require.GreaterOrEqual(t, end.UnixNano(), start.UnixNano())
			if ca.startTime.Unix() > 0 {
				require.EqualValues(t, ca.startTime, metricTableExtractor.StartTime, "SQL: %v, start_time: %v", ca.sql, metricTableExtractor.StartTime)
			}
			if ca.endTime.Unix() > 0 {
				require.EqualValues(t, ca.endTime, metricTableExtractor.EndTime, "SQL: %v, end_time: %v", ca.sql, metricTableExtractor.EndTime)
			}
		}
	}
}

func TestMetricsSummaryTableExtractor(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

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

		logicalMemTable := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor)

		extractor := logicalMemTable.Extractor.(*plannercore.MetricSummaryTableExtractor)
		if len(ca.quantiles) > 0 {
			require.EqualValues(t, ca.quantiles, extractor.Quantiles, "SQL: %v", ca.sql)
		}
		if len(ca.names) > 0 {
			require.EqualValues(t, ca.names, extractor.MetricsNames, "SQL: %v", ca.sql)
		}
		require.Equal(t, ca.skipRequest, extractor.SkipRequest, "SQL: %v", ca.sql)
	}
}

func TestInspectionResultTableExtractor(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

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
		logicalMemTable := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor)

		clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.InspectionResultTableExtractor)
		if len(ca.rules) > 0 {
			require.EqualValues(t, ca.rules, clusterConfigExtractor.Rules, "SQL: %v", ca.sql)
		}
		if len(ca.items) > 0 {
			require.EqualValues(t, ca.items, clusterConfigExtractor.Items, "SQL: %v", ca.sql)
		}
		require.Equal(t, ca.skipInspection, clusterConfigExtractor.SkipInspection, "SQL: %v", ca.sql)
	}
}

func TestInspectionSummaryTableExtractor(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

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
		logicalMemTable := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor)

		clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.InspectionSummaryTableExtractor)
		if len(ca.rules) > 0 {
			require.EqualValues(t, ca.rules, clusterConfigExtractor.Rules, "SQL: %v", ca.sql)
		}
		if len(ca.names) > 0 {
			require.EqualValues(t, ca.names, clusterConfigExtractor.MetricNames, "SQL: %v", ca.sql)
		}
		require.Equal(t, ca.skipInspection, clusterConfigExtractor.SkipInspection, "SQL: %v", ca.sql)
	}
}

func TestInspectionRuleTableExtractor(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

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
		logicalMemTable := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor)

		clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.InspectionRuleTableExtractor)
		if len(ca.tps) > 0 {
			require.EqualValues(t, ca.tps, clusterConfigExtractor.Types, "SQL: %v", ca.sql)
		}
		require.Equal(t, ca.skip, clusterConfigExtractor.SkipRequest, "SQL: %v", ca.sql)
	}
}

func TestTiDBHotRegionsHistoryTableExtractor(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	se.GetSessionVars().StmtCtx.TimeZone = time.Local

	var cases = []struct {
		sql                          string
		skipRequest                  bool
		startTime, endTime           int64
		regionIDs, storeIDs, peerIDs []uint64
		isLearners, isLeaders        []bool
		hotRegionTypes               set.StringSet
	}{
		// Test full data, it will not call Extract() and executor(retriver) will panic and remind user to add conditions to save network IO.
		{
			sql: "select * from information_schema.tidb_hot_regions_history",
		},
		// Test startTime and endTime.
		{
			// Test for invalid update_time.
			sql: "select * from information_schema.tidb_hot_regions_history where update_time='2019-10-10 10::10'",
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time='2019-10-10 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10"),
			endTime:        timestamp(t, "2019-10-10 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and update_time<='2019-10-11 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10"),
			endTime:        timestamp(t, "2019-10-11 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>'2019-10-10 10:10:10' and update_time<'2019-10-11 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10") + 1,
			endTime:        timestamp(t, "2019-10-11 10:10:10") - 1,
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and update_time<'2019-10-11 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10"),
			endTime:        timestamp(t, "2019-10-11 10:10:10") - 1,
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-12 10:10:10' and update_time<'2019-10-11 10:10:10'",
			startTime:      timestamp(t, "2019-10-12 10:10:10"),
			endTime:        timestamp(t, "2019-10-11 10:10:10") - 1,
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
			skipRequest:    true,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and  update_time>='2019-10-11 10:10:10' and  update_time>='2019-10-12 10:10:10'",
			startTime:      timestamp(t, "2019-10-12 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and  update_time>='2019-10-11 10:10:10' and  update_time>='2019-10-12 10:10:10' and update_time='2019-10-13 10:10:10'",
			startTime:      timestamp(t, "2019-10-13 10:10:10"),
			endTime:        timestamp(t, "2019-10-13 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time<='2019-10-10 10:10:10' and update_time='2019-10-13 10:10:10'",
			startTime:      timestamp(t, "2019-10-13 10:10:10"),
			endTime:        timestamp(t, "2019-10-10 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
			skipRequest:    true,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time='2019-10-10 10:10:10' and update_time<='2019-10-13 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10"),
			endTime:        timestamp(t, "2019-10-10 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},

		// Test `region_id`, `store_id`, `peer_id` columns.
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id=100",
			regionIDs:      []uint64{100},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where 100=region_id",
			regionIDs:      []uint64{100},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where 100=region_id or region_id=101",
			regionIDs:      []uint64{100, 101},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where 100=region_id or region_id=101 or region_id=102 or 103 = region_id",
			regionIDs:      []uint64{100, 101, 102, 103},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where (region_id=100 or region_id=101) and (store_id=200 or store_id=201)",
			regionIDs:      []uint64{100, 101},
			storeIDs:       []uint64{200, 201},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id in (100, 101)",
			regionIDs:      []uint64{100, 101},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id in (100, 101) and store_id=200",
			regionIDs:      []uint64{100, 101},
			storeIDs:       []uint64{200},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id in (100, 101) and store_id in (200, 201)",
			regionIDs:      []uint64{100, 101},
			storeIDs:       []uint64{200, 201},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id=100 and store_id in (200, 201)",
			regionIDs:      []uint64{100},
			storeIDs:       []uint64{200, 201},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id=100 and store_id=200",
			regionIDs:      []uint64{100},
			storeIDs:       []uint64{200},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id=101",
			skipRequest: true,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (100,101)",
			regionIDs:      []uint64{100},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (100,101) and store_id=200 and store_id in (200,201)",
			regionIDs:      []uint64{100},
			storeIDs:       []uint64{200},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (101,102)",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (101,102) and store_id=200 and store_id in (200,201)",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (100,101) and store_id=200 and store_id in (201,202)",
			skipRequest: true,
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history
								where region_id=100 and region_id in (100,101)
								and store_id=200 and store_id in (201,202)`,
			skipRequest: true,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id in (100,101) and region_id in (101,102)",
			regionIDs:      []uint64{101},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history
							where region_id in (100,101)
							and region_id in (101,102)
							and store_id in (200,201)
							and store_id in (201,202)`,
			regionIDs:      []uint64{101},
			storeIDs:       []uint64{201},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history
							where region_id in (100,101)
							and region_id in (101,102)
							and store_id in (200,201)
							and store_id in (201,202)
							and peer_id in (3000,3001)
							and peer_id in (3001,3002)`,
			regionIDs:      []uint64{101},
			storeIDs:       []uint64{201},
			peerIDs:        []uint64{3001},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history
							where region_id in (100,101)
							and region_id in (100,102)
							and region_id in (102,103)
							and region_id in (103,104)`,
			skipRequest: true,
		},
		// Test `type` column.
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where type='read'",
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where type in('read')",
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead),
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where type='read' and type='write'",
			skipRequest: true,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where type in ('read', 'write')",
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where type='read' and type in ('read', 'write')",
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead),
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where type in ('read') and type in ('write')",
			skipRequest: true,
		},
		// Test `is_learner`, `is_leaeder` columns.
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_learner=1",
			isLearners:     []bool{true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_leader=0",
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_learner=true",
			isLearners:     []bool{true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:        "select * from information_schema.tidb_hot_regions_history where is_learner in(0,1)",
			isLearners: []bool{false, true},
			isLeaders:  []bool{false, true},
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_learner in(true,false)",
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_learner in(3,4)",
			isLearners:     []bool{false},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_learner in(3,4) and is_leader in(0,1,true,false,3,4)",
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
			isLearners:     []bool{false},
			isLeaders:      []bool{false, true},
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where is_learner=1 and is_learner=0",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where is_learner=3 and is_learner=false",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where is_learner=3 and is_learner=4",
			skipRequest: true,
		},
	}

	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor, "SQL: %v", ca.sql)

		hotRegionsHistoryExtractor := logicalMemTable.Extractor.(*plannercore.HotRegionsHistoryTableExtractor)
		if ca.startTime > 0 {
			require.Equal(t, ca.startTime, hotRegionsHistoryExtractor.StartTime, "SQL: %v", ca.sql)
		}
		if ca.endTime > 0 {
			require.Equal(t, ca.endTime, hotRegionsHistoryExtractor.EndTime, "SQL: %v", ca.sql)
		}
		require.EqualValues(t, ca.skipRequest, hotRegionsHistoryExtractor.SkipRequest, "SQL: %v", ca.sql)
		if len(ca.isLearners) > 0 {
			require.EqualValues(t, ca.isLearners, hotRegionsHistoryExtractor.IsLearners, "SQL: %v", ca.sql)
		}
		if len(ca.isLeaders) > 0 {
			require.EqualValues(t, ca.isLeaders, hotRegionsHistoryExtractor.IsLeaders, "SQL: %v", ca.sql)
		}
		if ca.hotRegionTypes.Count() > 0 {
			require.EqualValues(t, ca.hotRegionTypes, hotRegionsHistoryExtractor.HotRegionTypes, "SQL: %v", ca.sql)
		}
		if len(ca.regionIDs) > 0 {
			require.EqualValues(t, ca.regionIDs, hotRegionsHistoryExtractor.RegionIDs, "SQL: %v", ca.sql)
		}
		if len(ca.storeIDs) > 0 {
			require.EqualValues(t, ca.storeIDs, hotRegionsHistoryExtractor.StoreIDs, "SQL: %v", ca.sql)
		}
		if len(ca.peerIDs) > 0 {
			require.EqualValues(t, ca.peerIDs, hotRegionsHistoryExtractor.PeerIDs, "SQL: %v", ca.sql)
		}
	}
}

func TestTikvRegionPeersExtractor(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	var cases = []struct {
		sql                 string
		regionIDs, storeIDs []uint64
		skipRequest         bool
	}{
		// Test `region_id`, `store_id` columns.
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id=100",
			regionIDs: []uint64{100},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where 100=region_id",
			regionIDs: []uint64{100},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where 100=region_id or region_id=101",
			regionIDs: []uint64{100, 101},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where 100=region_id or region_id=101 or region_id=102 or 103 = region_id",
			regionIDs: []uint64{100, 101, 102, 103},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where (region_id=100 or region_id=101) and (store_id=200 or store_id=201)",
			regionIDs: []uint64{100, 101},
			storeIDs:  []uint64{200, 201},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id in (100, 101)",
			regionIDs: []uint64{100, 101},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id in (100, 101) and store_id=200",
			regionIDs: []uint64{100, 101},
			storeIDs:  []uint64{200},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id in (100, 101) and store_id in (200, 201)",
			regionIDs: []uint64{100, 101},
			storeIDs:  []uint64{200, 201},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id=100 and store_id in (200, 201)",
			regionIDs: []uint64{100},
			storeIDs:  []uint64{200, 201},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id=100 and store_id=200",
			regionIDs: []uint64{100},
			storeIDs:  []uint64{200},
		},
		{
			sql:         "select * from information_schema.tikv_region_peers where region_id=100 and region_id=101",
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id=100 and region_id in (100,101)",
			regionIDs: []uint64{100},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id=100 and region_id in (100,101) and store_id=200 and store_id in (200,201)",
			regionIDs: []uint64{100},
			storeIDs:  []uint64{200},
		},
		{
			sql:         "select * from information_schema.tikv_region_peers where region_id=100 and region_id in (101,102)",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tikv_region_peers where region_id=100 and region_id in (101,102) and store_id=200 and store_id in (200,201)",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tikv_region_peers where region_id=100 and region_id in (100,101) and store_id=200 and store_id in (201,202)",
			skipRequest: true,
		},
		{
			sql: `select * from information_schema.tikv_region_peers
								where region_id=100 and region_id in (100,101)
								and store_id=200 and store_id in (201,202)`,
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id in (100,101) and region_id in (101,102)",
			regionIDs: []uint64{101},
		},
		{
			sql: `select * from information_schema.tikv_region_peers
							where region_id in (100,101)
							and region_id in (101,102)
							and store_id in (200,201)
							and store_id in (201,202)`,
			regionIDs: []uint64{101},
			storeIDs:  []uint64{201},
		},
		{
			sql: `select * from information_schema.tikv_region_peers
							where region_id in (100,101)
							and region_id in (100,102)
							and region_id in (102,103)
							and region_id in (103,104)`,
			skipRequest: true,
		},
		// Test columns that is not extracted by TikvRegionPeersExtractor
		{
			sql: `select * from information_schema.tikv_region_peers
							where peer_id=100
							and is_learner=0
							and is_leader=1
							and status='NORMAL'
							and down_seconds=1000`,
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor)

		tikvRegionPeersExtractor := logicalMemTable.Extractor.(*plannercore.TikvRegionPeersExtractor)
		if len(ca.regionIDs) > 0 {
			require.EqualValues(t, ca.regionIDs, tikvRegionPeersExtractor.RegionIDs, "SQL: %v", ca.sql)
		}
		if len(ca.storeIDs) > 0 {
			require.EqualValues(t, ca.storeIDs, tikvRegionPeersExtractor.StoreIDs, "SQL: %v", ca.sql)
		}
	}
}

func TestColumns(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	var cases = []struct {
		sql                string
		columnName         set.StringSet
		tableSchema        set.StringSet
		tableName          set.StringSet
		columnNamePattern  []string
		tableSchemaPattern []string
		tableNamePattern   []string
		skipRequest        bool
	}{
		{
			sql:        `select * from INFORMATION_SCHEMA.COLUMNS where column_name='T';`,
			columnName: set.NewStringSet("t"),
		},
		{
			sql:         `select * from INFORMATION_SCHEMA.COLUMNS where table_schema='TEST';`,
			tableSchema: set.NewStringSet("test"),
		},
		{
			sql:       `select * from INFORMATION_SCHEMA.COLUMNS where table_name='TEST';`,
			tableName: set.NewStringSet("test"),
		},
		{
			sql:        "select * from information_schema.COLUMNS where table_name in ('TEST','t') and column_name in ('A','b')",
			columnName: set.NewStringSet("a", "b"),
			tableName:  set.NewStringSet("test", "t"),
		},
		{
			sql:       `select * from information_schema.COLUMNS where table_name='a' and table_name in ('a', 'B');`,
			tableName: set.NewStringSet("a"),
		},
		{
			sql:         `select * from information_schema.COLUMNS where table_name='a' and table_name='B';`,
			skipRequest: true,
		},
		{
			sql:              `select * from information_schema.COLUMNS where table_name like 'T%';`,
			tableNamePattern: []string{"t%"},
		},
		{
			sql:               `select * from information_schema.COLUMNS where column_name like 'T%';`,
			columnNamePattern: []string{"t%"},
		},
		{
			sql:               `select * from information_schema.COLUMNS where column_name like 'i%';`,
			columnNamePattern: []string{"i%"},
		},
		{
			sql:               `select * from information_schema.COLUMNS where column_name like 'abc%' or column_name like "def%";`,
			columnNamePattern: []string{},
		},
		{
			sql:               `select * from information_schema.COLUMNS where column_name like 'abc%' and column_name like "%def";`,
			columnNamePattern: []string{"abc%", "%def"},
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor)

		columnsTableExtractor := logicalMemTable.Extractor.(*plannercore.ColumnsTableExtractor)
		require.Equal(t, ca.skipRequest, columnsTableExtractor.SkipRequest, "SQL: %v", ca.sql)

		require.Equal(t, ca.columnName.Count(), columnsTableExtractor.ColumnName.Count())
		if ca.columnName.Count() > 0 && columnsTableExtractor.ColumnName.Count() > 0 {
			require.EqualValues(t, ca.columnName, columnsTableExtractor.ColumnName, "SQL: %v", ca.sql)
		}

		require.Equal(t, ca.tableSchema.Count(), columnsTableExtractor.TableSchema.Count())
		if ca.tableSchema.Count() > 0 && columnsTableExtractor.TableSchema.Count() > 0 {
			require.EqualValues(t, ca.tableSchema, columnsTableExtractor.TableSchema, "SQL: %v", ca.sql)
		}
		require.Equal(t, ca.tableName.Count(), columnsTableExtractor.TableName.Count())
		if ca.tableName.Count() > 0 && columnsTableExtractor.TableName.Count() > 0 {
			require.EqualValues(t, ca.tableName, columnsTableExtractor.TableName, "SQL: %v", ca.sql)
		}
		require.Equal(t, len(ca.tableNamePattern), len(columnsTableExtractor.TableNamePatterns))
		if len(ca.tableNamePattern) > 0 && len(columnsTableExtractor.TableNamePatterns) > 0 {
			require.EqualValues(t, ca.tableNamePattern, columnsTableExtractor.TableNamePatterns, "SQL: %v", ca.sql)
		}
		require.Equal(t, len(ca.columnNamePattern), len(columnsTableExtractor.ColumnNamePatterns))
		if len(ca.columnNamePattern) > 0 && len(columnsTableExtractor.ColumnNamePatterns) > 0 {
			require.EqualValues(t, ca.columnNamePattern, columnsTableExtractor.ColumnNamePatterns, "SQL: %v", ca.sql)
		}
		require.Equal(t, len(ca.tableSchemaPattern), len(columnsTableExtractor.TableSchemaPatterns))
		if len(ca.tableSchemaPattern) > 0 && len(columnsTableExtractor.TableSchemaPatterns) > 0 {
			require.EqualValues(t, ca.tableSchemaPattern, columnsTableExtractor.TableSchemaPatterns, "SQL: %v", ca.sql)
		}
	}
}

func TestPredicateQuery(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, abctime int,DATETIME_PRECISION int);")
	tk.MustExec("create table abclmn(a int);")
	tk.MustQuery("select TABLE_NAME from information_schema.columns where table_schema = 'test' and column_name like 'i%'").Check(testkit.Rows("t"))
	tk.MustQuery("select TABLE_NAME from information_schema.columns where table_schema = 'TEST' and column_name like 'I%'").Check(testkit.Rows("t"))
	tk.MustQuery("select TABLE_NAME from information_schema.columns where table_schema = 'TEST' and column_name like 'ID'").Check(testkit.Rows("t"))
	tk.MustQuery("select TABLE_NAME from information_schema.columns where table_schema = 'TEST' and column_name like 'id'").Check(testkit.Rows("t"))
	tk.MustQuery("select column_name from information_schema.columns where table_schema = 'TEST' and (column_name like 'i%' or column_name like '%d')").Check(testkit.Rows("id"))
	tk.MustQuery("select column_name from information_schema.columns where table_schema = 'TEST' and (column_name like 'abc%' and column_name like '%time')").Check(testkit.Rows("abctime"))
	result := tk.MustQuery("select TABLE_NAME, column_name from information_schema.columns where table_schema = 'TEST' and column_name like '%time';")
	require.Len(t, result.Rows(), 1)
	tk.MustQuery("describe t").Check(testkit.Rows("id int(11) YES  <nil> ", "abctime int(11) YES  <nil> ", "DATETIME_PRECISION int(11) YES  <nil> "))
	tk.MustQuery("describe t id").Check(testkit.Rows("id int(11) YES  <nil> "))
	tk.MustQuery("describe t ID").Check(testkit.Rows("id int(11) YES  <nil> "))
	tk.MustGetErrCode("describe t 'I%'", errno.ErrParse)
	tk.MustGetErrCode("describe t I%", errno.ErrParse)

	tk.MustQuery("show columns from t like 'abctime'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show columns from t like 'ABCTIME'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show columns from t like 'abc%'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show columns from t like 'ABC%'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show columns from t like '%ime'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show columns from t like '%IME'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show columns in t like '%ime'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show columns in t like '%IME'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show fields in t like '%ime'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show fields in t like '%IME'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))

	tk.MustQuery("show columns from t where field like '%time'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show columns from t where field = 'abctime'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show columns in t where field = 'abctime'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show fields from t where field = 'abctime'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("show fields in t where field = 'abctime'").Check(testkit.RowsWithSep(",", "abctime,int(11),YES,,<nil>,"))
	tk.MustQuery("explain t").Check(testkit.Rows("id int(11) YES  <nil> ", "abctime int(11) YES  <nil> ", "DATETIME_PRECISION int(11) YES  <nil> "))

	tk.MustGetErrCode("show columns from t like id", errno.ErrBadField)
	tk.MustGetErrCode("show columns from t like `id`", errno.ErrBadField)

	tk.MustQuery("show tables like 't'").Check(testkit.Rows("t"))
	tk.MustQuery("show tables like 'T'").Check(testkit.Rows("t"))
	tk.MustQuery("show tables like 'ABCLMN'").Check(testkit.Rows("abclmn"))
	tk.MustQuery("show tables like 'ABC%'").Check(testkit.Rows("abclmn"))
	tk.MustQuery("show tables like '%lmn'").Check(testkit.Rows("abclmn"))
	tk.MustQuery("show full tables like '%lmn'").Check(testkit.Rows("abclmn BASE TABLE"))
	tk.MustGetErrCode("show tables like T", errno.ErrBadField)
	tk.MustGetErrCode("show tables like `T`", errno.ErrBadField)
}

func TestTikvRegionStatusExtractor(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	var cases = []struct {
		sql      string
		tableIDs []int64
	}{
		{
			sql:      "select * from information_schema.TIKV_REGION_STATUS where table_id = 1",
			tableIDs: []int64{1},
		},
		{
			sql:      "select * from information_schema.TIKV_REGION_STATUS where table_id = 1 or table_id = 2",
			tableIDs: []int64{1, 2},
		},
		{
			sql:      "select * from information_schema.TIKV_REGION_STATUS where table_id in (1,2,3)",
			tableIDs: []int64{1, 2, 3},
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor)
		rse := logicalMemTable.Extractor.(*plannercore.TiKVRegionStatusExtractor)
		tableids := rse.GetTablesID()
		sort.Slice(tableids, func(i, j int) bool {
			return tableids[i] < tableids[j]
		})
		require.Equal(t, ca.tableIDs, tableids)
	}
}
