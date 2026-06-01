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
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	execpkg "github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type mockMLogDDLExecutor struct {
	ddl.Executor

	generatedNames []string
	createErrs     []error

	generateCalls int
	createNames   []string
}

func (m *mockMLogDDLExecutor) GenerateMLogTableName(sessionctx.Context, *ast.CreateMaterializedViewLogStmt) (string, error) {
	if m.generateCalls >= len(m.generatedNames) {
		return "", errors.New("unexpected GenerateMLogTableName call")
	}
	name := m.generatedNames[m.generateCalls]
	m.generateCalls++
	return name, nil
}

func (m *mockMLogDDLExecutor) CreateMaterializedViewLog(_ sessionctx.Context, _ *ast.CreateMaterializedViewLogStmt, mlogTableName string) error {
	m.createNames = append(m.createNames, mlogTableName)
	idx := len(m.createNames) - 1
	if idx >= len(m.createErrs) {
		return nil
	}
	return m.createErrs[idx]
}

func TestDDLExecExecuteCreateMaterializedViewLog(t *testing.T) {
	tableExistsErr := infoschema.ErrTableExists.GenWithStackByArgs("mlog")
	otherErr := errors.New("non table exists error")

	tests := []struct {
		name           string
		generatedNames []string
		createErrs     []error
		expectedNames  []string
		expectedErr    error
	}{
		{
			name:           "retry on table exists",
			generatedNames: []string{"$mlog$t", "$mlog$1"},
			createErrs:     []error{tableExistsErr, nil},
			expectedNames:  []string{"$mlog$t", "$mlog$1"},
		},
		{
			name:           "return non table exists error",
			generatedNames: []string{"$mlog$t"},
			createErrs:     []error{otherErr},
			expectedNames:  []string{"$mlog$t"},
			expectedErr:    otherErr,
		},
		{
			name:           "return nil on success",
			generatedNames: []string{"$mlog$t"},
			createErrs:     []error{nil},
			expectedNames:  []string{"$mlog$t"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddlExecutor := &mockMLogDDLExecutor{
				generatedNames: tt.generatedNames,
				createErrs:     tt.createErrs,
			}
			exec := &DDLExec{
				BaseExecutor: execpkg.NewBaseExecutor(mock.NewContext(), nil, 0),
				ddlExecutor:  ddlExecutor,
			}

			err := exec.executeCreateMaterializedViewLog(&ast.CreateMaterializedViewLogStmt{})
			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedNames, ddlExecutor.createNames)
			require.Equal(t, len(tt.expectedNames), ddlExecutor.generateCalls)
		})
	}
}
