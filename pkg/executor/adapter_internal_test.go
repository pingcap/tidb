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

package executor

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlmock "github.com/pingcap/tidb/pkg/util/topsql/collector/mock"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/stretchr/testify/require"
)

func TestObserveStmtBeginForTopSQL_RegisterSQLPlanMeta_WhenTopRUEnabledAndTopSQLDisabled(t *testing.T) {
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.EnableTopRU()
	t.Cleanup(func() {
		topsqlstate.DisableTopSQL()
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
	})

	topCollector := topsqlmock.NewTopSQLCollector()
	topsql.SetupTopSQLForTest(topCollector)

	sctx := mock.NewContext()
	sc := sctx.GetSessionVars().StmtCtx
	sc.OriginalSQL = "select * from t where a = 1"
	normalizedSQL, sqlDigest := sc.SQLDigest()
	require.NotNil(t, sqlDigest)
	const normalizedPlan = "TableReader(table:t)->Selection(eq(test.t.a, ?))"
	planDigest := parser.NewDigest([]byte("topru-plan-digest"))
	sc.SetPlanDigest(normalizedPlan, planDigest)

	stmt := &ExecStmt{
		Ctx:   sctx,
		GoCtx: context.Background(),
	}
	_ = stmt.observeStmtBeginForTopSQL(context.Background())

	require.Equal(t, normalizedSQL, topCollector.GetSQL(sqlDigest.Bytes()))
	require.Equal(t, normalizedPlan, topCollector.GetPlan(planDigest.Bytes()))
}
