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

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestMaskingPolicyOperationsRequireSysTable(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMissingMaskingPolicySysTable", "return(true)")

	w := &worker{}
	jobCtx := &jobContext{stepCtx: context.Background()}
	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "table",
			run: func() error {
				return w.dropMaskingPoliciesOnTable(jobCtx, 1)
			},
		},
		{
			name: "column",
			run: func() error {
				return w.dropMaskingPoliciesOnColumn(jobCtx, 1, 1)
			},
		},
		{
			name: "modify_column",
			run: func() error {
				return w.syncMaskingPolicyForModifiedColumn(
					jobCtx,
					&model.TableInfo{ID: 1},
					&model.ColumnInfo{},
					&model.ColumnInfo{},
				)
			},
		},
		{
			name: "truncate",
			run: func() error {
				return w.updateMaskingPolicyTableIDAfterTruncate(jobCtx, 1, 2)
			},
		},
		{
			name: "rename",
			run: func() error {
				return w.updateMaskingPolicyNamesAfterRename(
					jobCtx.stepCtx,
					1,
					ast.NewCIStr("old_db"), ast.NewCIStr("new_db"),
					ast.NewCIStr("old_table"), ast.NewCIStr("new_table"),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			require.Error(t, err)
			require.True(t, infoschema.ErrTableNotExists.Equal(err), "unexpected error: %v", err)
		})
	}
}
