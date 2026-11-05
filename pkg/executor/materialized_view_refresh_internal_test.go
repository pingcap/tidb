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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

// MVRefreshStepForTest exposes runtime step information to external executor tests.
type MVRefreshStepForTest = mvRefreshObserveStep

// NewRefreshMaterializedViewExecForTest exercises the runtime directly with a
// step observer.
func NewRefreshMaterializedViewExecForTest(sctx sessionctx.Context, stmt *ast.RefreshMaterializedViewStmt, observer mvRefreshStepObserver) *RefreshMaterializedViewExec {
	return &RefreshMaterializedViewExec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0),
		stmt:         stmt,
		stepObserver: observer,
	}
}

func TestMVRefreshRuntimeStepContracts(t *testing.T) {
	for _, tc := range []struct {
		mode  ast.RefreshMaterializedViewMode
		names []string
	}{
		{ast.RefreshMaterializedViewModeFast, []string{"TXN_BEGIN", "LOCK_REFRESH_INFO", "INSERT_HIST_RUNNING", "DATA_CHANGE_FAST_MERGE", "PERSIST_REFRESH_INFO", "TXN_COMMIT", "FINALIZE_HIST"}},
		{ast.RefreshMaterializedViewModeCompleteInPlace, []string{"TXN_BEGIN", "LOCK_REFRESH_INFO", "INSERT_HIST_RUNNING", "DATA_CHANGE_COMPLETE_DELETE", "DATA_CHANGE_COMPLETE_INSERT", "PERSIST_REFRESH_INFO", "TXN_COMMIT", "FINALIZE_HIST"}},
		{ast.RefreshMaterializedViewModeCompleteDeltaApply, []string{"TXN_BEGIN", "LOCK_REFRESH_INFO", "INSERT_HIST_RUNNING", "DATA_CHANGE_COMPLETE_DELTA_APPLY", "PERSIST_REFRESH_INFO", "TXN_COMMIT", "FINALIZE_HIST"}},
		{ast.RefreshMaterializedViewModeCompleteOutOfPlace, []string{"INSERT_HIST_RUNNING", "OUT_OF_PLACE_CREATE_SHADOW", "OUT_OF_PLACE_LOAD_SHADOW", "OUT_OF_PLACE_CUTOVER", "FINALIZE_HIST"}},
	} {
		set, err := newMVRefreshStepSet(tc.mode)
		require.NoError(t, err)
		names := make([]string, len(set.steps))
		for i, step := range set.steps {
			names[i] = step.Name
			require.Equal(t, mvRefreshStepID(i+1), step.ID)
		}
		require.Equal(t, tc.names, names)
	}
	_, err := newMVRefreshStepSet(ast.RefreshMaterializedViewMode(255))
	require.ErrorContains(t, err, "unknown mode")
	rows := [][]string{{"TableReader", "1"}, {"TableScan", "2"}}
	cloned := clonePlanRows(rows)
	rows[0][0] = "changed"
	require.Equal(t, "TableReader", cloned[0][0])
}

func TestRestoreRefreshMaterializedViewStmtForSlowLog(t *testing.T) {
	stmt := &ast.RefreshMaterializedViewImplementStmt{
		RefreshStmt: &ast.RefreshMaterializedViewStmt{
			ViewName:     &ast.TableName{Name: ast.NewCIStr("mv")},
			Type:         ast.RefreshMaterializedViewTypeComplete,
			CompleteType: ast.RefreshMaterializedViewCompleteTypeInPlace,
		},
	}

	require.Equal(t,
		"IMPLEMENT FOR REFRESH MATERIALIZED VIEW `mv` COMPLETE IN PLACE USING TIMESTAMP",
		restoreStmtTextForSlowLogWhenEmptySQL(stmt),
	)
	require.Empty(t, restoreStmtTextForSlowLogWhenEmptySQL(&ast.RefreshMaterializedViewImplementStmt{}))
}

func TestBuildMVRefreshOutOfPlaceShadowTableInfoSetsSource(t *testing.T) {
	shadow, err := buildMVRefreshOutOfPlaceShadowTableInfo(
		ast.NewCIStr("test"),
		"__mv_shadow_42",
		&model.TableInfo{
			ID:               42,
			Name:             ast.NewCIStr("mv"),
			MaterializedView: &model.MaterializedViewInfo{},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, shadow.MaterializedViewShadow)
	require.Equal(t, int64(42), shadow.MaterializedViewShadow.SourceMViewID)
}

func TestInitRefreshMaterializedViewSessionAppliesDefinitionDivPrecisionIncrement(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	require.False(t, sessVars.EnableMView)
	sessVars.DivPrecisionIncrement = 2
	sessVars.TimeZone = time.UTC
	sessVars.StmtCtx.SetTimeZone(time.UTC)

	restore, err := initRefreshMaterializedViewSession(sessVars, &model.MaterializedViewInfo{
		DefinitionDivPrecisionIncrement: 9,
		DefinitionTimeZone:              model.TimeZoneLocation{Name: "UTC"},
	})
	require.NoError(t, err)
	require.True(t, sessVars.EnableMView)
	require.Equal(t, 9, sessVars.DivPrecisionIncrement)

	restore()
	require.False(t, sessVars.EnableMView)
	require.Equal(t, 2, sessVars.DivPrecisionIncrement)
}

func TestBuildMVRefreshOutOfPlaceBuildSQLImportOptions(t *testing.T) {
	tblInfo := &model.TableInfo{
		Name: ast.NewCIStr("mv"),
		MaterializedView: &model.MaterializedViewInfo{
			SQLContent: "select a, count(1) from t group by a",
		},
	}

	sql, err := buildMVRefreshOutOfPlaceBuildSQL("test", "__mv_shadow_1", tblInfo, "TiKV", 12, "64gib")
	require.NoError(t, err)
	require.Contains(t, sql, "IMPORT INTO `test`.`__mv_shadow_1` FROM (")
	require.Contains(t, sql, "WITH "+strings.Join(ddl.BuildMViewImportIntoOptions(12, "64gib"), ", "))

	sql, err = buildMVRefreshOutOfPlaceBuildSQL("test", "__mv_shadow_1", tblInfo, kv.TiDB.Name(), 12, "64gib")
	require.NoError(t, err)
	require.Equal(t, "INSERT INTO `test`.`__mv_shadow_1` select a, count(1) from t group by a", sql)
}
