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
	row := chunk.MutRowFromValues("p", "test", "t", "c", "mask_full(c)", "ENABLED", "NONE").ToRow()
	unrelated := chunk.MutRowFromValues("p", "other", "t", "c", "mask_full(c)", "ENABLED", "NONE").ToRow()
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
		err := restoreMaskingPoliciesFromTemporaryTable(context.Background(), maskingPolicyRestoreExecutor{rows: tc.rows}, "tmp_mysql", true, tableFilter, tc.supported,
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
