// Copyright 2026 PingCAP, Inc.
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

package ingest_test

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestTempIndexOpsCollectorClearTableDeletesSeries(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	defer testutil.InjectMockBackendCtx(t, store)()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")

	for i := range 10 {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}

	tblInfo := external.GetTableByName(t, tk, "test", "t").Meta()
	tableIDLabel := strconv.FormatInt(tblInfo.ID, 10)

	hasTableSeries := func() bool {
		mfs, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() != "tidb_ddl_temp_index_op_total" {
				continue
			}
			for _, m := range mf.GetMetric() {
				seen := false
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "table_id" && lp.GetValue() == tableIDLabel {
						seen = true
						break
					}
				}
				if seen {
					return true
				}
			}
		}
		return false
	}

	// During ADD INDEX, insert extra rows so temp-index writer paths run.
	// Use afterMockWriterWriteRow so the injected DML doesn't race with the
	// internal temp-index writer and cause reorg panic.
	var (
		rowCnt         atomic.Int32
		observedSeries atomic.Bool
	)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/ingest/afterMockWriterWriteRow", func() {
		if rowCnt.Add(1) == 1 {
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			tk1.MustExec("insert into t values (100, 100)")
		}
		if !observedSeries.Load() && hasTableSeries() {
			observedSeries.Store(true)
		}
	})

	tk.MustExec("alter table t add index idx_b(b)")
	require.Greater(t, int(rowCnt.Load()), 0)
	require.NoError(t, dom.Reload())

	// The InfoSchema builder clears metrics when all indexes are public, which can
	// happen immediately after ADD INDEX finishes. So it is more stable to assert
	// that we saw the series during the DDL job.
	require.True(t, observedSeries.Load(), "expected tidb_ddl_temp_index_op_total series during ADD INDEX")

	// Trigger InfoSchema builder to call metrics.DDLClearTempIndexOps(tableID).
	// It happens when all indexes are public during table rebuild.
	tk.MustExec("alter table t add column c int")

	require.False(t, hasTableSeries())
}
