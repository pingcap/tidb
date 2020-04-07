// Copyright 2020 PingCAP, Inc.
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

package executor_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&inspectionSummarySuite{})

type inspectionSummarySuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *inspectionSummarySuite) SetUpSuite(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
}

func (s *inspectionSummarySuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *inspectionSummarySuite) TestValidInspectionSummaryRules(c *C) {
	for rule, tbls := range executor.InspectionSummaryRules {
		tables := set.StringSet{}
		for _, t := range tbls {
			c.Assert(tables.Exist(t), IsFalse, Commentf("duplicate table name: %v in rule: %v", t, rule))
			tables.Insert(t)

			_, found := infoschema.MetricTableMap[t]
			c.Assert(found, IsTrue, Commentf("metric table %v not define", t))
		}
	}
}

func (s *inspectionSummarySuite) TestInspectionSummary(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	fpName := "github.com/pingcap/tidb/executor/mockMetricsTableData"
	c.Assert(failpoint.Enable(fpName, "return"), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	datetime := func(s string) types.Time {
		t, err := types.ParseTime(tk.Se.GetSessionVars().StmtCtx, s, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		return t
	}

	// construct some mock data
	mockData := map[string][][]types.Datum{
		// columns: time, instance, type, result, value
		"tidb_qps": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "Query", "OK", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "Query", "Error", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-1", "Quit", "Error", 5.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-1", "Quit", "Error", 9.0),
		},
		// columns: time, instance, sql_type, quantile, value
		"tidb_query_duration": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "Select", 0.99, 0.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", "Update", 0.99, 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", "Update", 0.99, 3.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-2", "Delete", 0.99, 5.0),
		},
	}

	ctx := context.WithValue(context.Background(), "__mockMetricsTableData", mockData)
	ctx = failpoint.WithHook(ctx, func(_ context.Context, fpname string) bool {
		return fpName == fpname
	})

	// Test for select * from information_schema.inspection_summary without specify rules.
	rs, err := tk.Se.Execute(ctx, "select * from information_schema.inspection_summary where metrics_name in ('tidb_qps', 'tidb_query_duration')")
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect SQL failed"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetSessionVars().StmtCtx.GetWarnings()))
	result.Check(testkit.Rows(
		"query-summary tikv-0 tidb_query_duration Select 0.99 0 0 0 The quantile of TiDB query durations(second)",
		"query-summary tikv-1 tidb_query_duration Update 0.99 2 1 3 The quantile of TiDB query durations(second)",
		"query-summary tikv-2 tidb_query_duration Delete 0.99 5 5 5 The quantile of TiDB query durations(second)",
		"query-summary tidb-0 tidb_qps Query, Error <nil> 1 1 1 TiDB query processing numbers per second",
		"query-summary tidb-0 tidb_qps Query, OK <nil> 0 0 0 TiDB query processing numbers per second",
		"query-summary tidb-1 tidb_qps Quit, Error <nil> 7 5 9 TiDB query processing numbers per second",
	))
}
