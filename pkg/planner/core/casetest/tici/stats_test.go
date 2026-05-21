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

package tici

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestTiCISearchEstimateOnlyForMultiTable(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress"))
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, time.Second, mockstore.WithMockTiFlash(2))
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(fmt.Sprintf("set global %s = on", vardef.TiDBEnableTiCIEstimate))
	defer tk.MustExec(fmt.Sprintf("set global %s = on", vardef.TiDBEnableTiCIEstimate))

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	tk.MustExec("use test")
	tk.MustExec(`create table t(
		id int primary key,
		title text,
		fulltext index idx_title(title)
	)`)
	tk.MustExec("create table t2(a int primary key)")
	for i := 1; i <= 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, 'hello')", i))
		tk.MustExec(fmt.Sprintf("insert into t2 values (%d)", i))
	}
	tk.MustExec("analyze table t")

	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t")

	singleTablePlan := testdata.ConvertRowsToStrings(
		tk.MustQuery("explain format='brief' select id from t where fts_match_word('hello', title)").Rows())
	requirePlanLineContains(t, singleTablePlan, "IndexRangeScan 10.00", "search func:fts_match_word")

	multiTablePlan := testdata.ConvertRowsToStrings(
		tk.MustQuery("explain format='brief' select /*+ inl_join(t) */ t.id from t2, t where t.id = t2.a and fts_match_word('hello', t.title)").Rows())
	requirePlanLineContains(t, multiTablePlan, "IndexRangeScan 100.00", "search func:fts_match_word")

	tk.MustExec(fmt.Sprintf("set global %s = off", vardef.TiDBEnableTiCIEstimate))
	disabledEstimatePlan := testdata.ConvertRowsToStrings(
		tk.MustQuery("explain format='brief' select /*+ inl_join(t) */ t.id from t2, t where t.id = t2.a and match(t.title) against ('hello' in boolean mode)").Rows())
	requirePlanLineContains(t, disabledEstimatePlan, "IndexRangeScan 10.00", "search func:fts_match_word")
}

func requirePlanLineContains(t *testing.T, plan []string, expected ...string) {
	t.Helper()
	for _, line := range plan {
		matched := true
		for _, item := range expected {
			if !strings.Contains(line, item) {
				matched = false
				break
			}
		}
		if matched {
			return
		}
	}
	require.Failf(t, "plan does not contain expected text", "expected: %s\nplan:\n%s", strings.Join(expected, ", "), strings.Join(plan, "\n"))
}
