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

package snapclient

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
)

type maskingPolicyRestoreExecutor struct {
	sqlexec.RestrictedSQLExecutor
	rows []chunk.Row
}

func (e maskingPolicyRestoreExecutor) ExecRestrictedSQL(context.Context, []sqlexec.OptionFuncAlias, string, ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	return e.rows, nil, nil
}

func TestRestoreMaskingPoliciesRequiresTargetSupport(t *testing.T) {
	tableFilter, err := filter.Parse([]string{"test.*"})
	require.NoError(t, err)
	row := chunk.MutRowFromValues("p", "test", "t", "c", "mask_full(c)", "ENABLED", "NONE", int64(0)).ToRow()
	unrelated := chunk.MutRowFromValues("p", "other", "t", "c", "mask_full(c)", "ENABLED", "NONE", int64(0)).ToRow()
	for _, tc := range []struct {
		rows                 []chunk.Row
		supported, wantError bool
	}{
		{[]chunk.Row{row}, false, true},
		{[]chunk.Row{row}, true, false},
		{[]chunk.Row{unrelated}, false, false},
		{nil, false, false},
	} {
		calls := 0
		err := restoreMaskingPoliciesFromTemporaryTable(context.Background(), maskingPolicyRestoreExecutor{rows: tc.rows}, "tmp_mysql", true, tableFilter, tc.supported, map[filter.Table]struct{}{{Schema: "test", Name: "t"}: {}},
			func(context.Context, string) error { calls++; return nil })
		if tc.wantError {
			require.Error(t, err)
			require.Zero(t, calls)
		} else {
			require.NoError(t, err)
		}
		if tc.supported && len(tc.rows) > 0 {
			require.Equal(t, 1, calls)
		}
	}
}

func TestRestoreMaskingPoliciesRejectsUnknownStatus(t *testing.T) {
	tableFilter, err := filter.Parse([]string{"test.*"})
	require.NoError(t, err)
	backedUp := map[filter.Table]struct{}{{Schema: "test", Name: "t"}: {}}
	restore := func(status string) ([]string, error) {
		row := chunk.MutRowFromValues("p", "test", "t", "c", "mask_full(c)", status, "NONE", int64(0)).ToRow()
		var executed []string
		err := restoreMaskingPoliciesFromTemporaryTable(context.Background(), maskingPolicyRestoreExecutor{rows: []chunk.Row{row}},
			"tmp_mysql", true, tableFilter, true, backedUp,
			func(_ context.Context, sql string) error { executed = append(executed, sql); return nil })
		return executed, err
	}
	for status, want := range map[string]string{"ENABLED": "ENABLE;", " enable ": "ENABLE;", "DISABLED": "DISABLE;", "disable": "DISABLE;"} {
		executed, err := restore(status)
		require.NoError(t, err, status)
		require.Len(t, executed, 1, status)
		require.True(t, strings.HasSuffix(executed[0], want), executed[0])
	}
	// A status TiDB itself refuses to load must stop the restore instead of
	// silently creating a disabled policy, which would leave the column unmasked.
	for _, status := range []string{"", "PAUSED", "ENABLE_V2"} {
		executed, err := restore(status)
		require.ErrorContains(t, err, "unknown status", status)
		require.Empty(t, executed, status)
	}
}

func TestRestoreMaskingPoliciesSkipsTablesOutsideBackup(t *testing.T) {
	tableFilter, err := filter.Parse([]string{"test.*"})
	require.NoError(t, err)
	rows := []chunk.Row{
		chunk.MutRowFromValues("p1", "test", "t", "c", "mask_full(c)", "ENABLED", "NONE", int64(0)).ToRow(),
		chunk.MutRowFromValues("p2", "test", "u", "c", "mask_null(c)", "DISABLED", "NONE", int64(0)).ToRow(),
	}
	var statements []string
	err = restoreMaskingPoliciesFromTemporaryTable(context.Background(), maskingPolicyRestoreExecutor{rows: rows}, "tmp_mysql", true, tableFilter, true,
		map[filter.Table]struct{}{{Schema: "test", Name: "t"}: {}},
		func(_ context.Context, sql string) error { statements = append(statements, sql); return nil })
	require.NoError(t, err)
	require.Len(t, statements, 1)
	require.Contains(t, statements[0], "`test`.`t`")
}

func TestRestoreMaskingPoliciesRejectsMissingTargetColumn(t *testing.T) {
	tableFilter, err := filter.Parse([]string{"test.t"})
	require.NoError(t, err)
	backedUp := map[filter.Table]struct{}{{Schema: "test", Name: "t"}: {}, {Schema: "test", Name: "u"}: {}}
	restore := func(rows ...chunk.Row) ([]string, error) {
		var executed []string
		err := restoreMaskingPoliciesFromTemporaryTable(context.Background(), maskingPolicyRestoreExecutor{rows: rows},
			"tmp_mysql", true, tableFilter, true, backedUp,
			func(_ context.Context, sql string) error { executed = append(executed, sql); return nil })
		return executed, err
	}
	// The target table is selected but has no column with the backed-up name:
	// the restore must fail instead of finishing with the column unmasked.
	executed, err := restore(chunk.MutRowFromValues("p", "test", "t", "c", "mask_full(c)", "ENABLED", "NONE", int64(1)).ToRow())
	require.ErrorContains(t, err, "does not exist in target table test.t")
	require.Empty(t, executed)
	// Policies of tables outside the restore selection stay skipped.
	executed, err = restore(
		chunk.MutRowFromValues("p1", "test", "u", "c", "mask_full(c)", "ENABLED", "NONE", int64(1)).ToRow(),
		chunk.MutRowFromValues("p2", "other", "t", "c", "mask_full(c)", "ENABLED", "NONE", int64(1)).ToRow(),
		chunk.MutRowFromValues("p3", "test", "t", "c", "mask_full(c)", "ENABLED", "NONE", int64(0)).ToRow(),
	)
	require.NoError(t, err)
	require.Len(t, executed, 1)
	require.Contains(t, executed[0], "`p3`")
}

func TestRestoreMaskingPoliciesRejectsMissingSelectedColumn(t *testing.T) {
	f, err := filter.Parse([]string{"test.selected"})
	require.NoError(t, err)
	for _, selected := range []bool{false, true} {
		name := "unselected"
		if selected {
			name = "selected"
		}
		row := chunk.MutRowFromValues("p", "test", name, "missing", "mask_full(missing)", "ENABLED", "NONE", int64(1)).ToRow()
		calls := 0
		err := restoreMaskingPoliciesFromTemporaryTable(context.Background(), maskingPolicyRestoreExecutor{rows: []chunk.Row{row}},
			"tmp_mysql", true, f, true, map[filter.Table]struct{}{{Schema: "test", Name: name}: {}},
			func(context.Context, string) error { calls++; return nil })
		if selected {
			require.ErrorContains(t, err, "does not exist in target table")
		} else {
			require.NoError(t, err)
		}
		require.Zero(t, calls)
	}
}
