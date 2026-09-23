// Copyright 2025 PingCAP, Inc.
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

package ast

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShowCommand(t *testing.T) {
	for i := 1; i < showTpCount; i++ {
		stmt := &ShowStmt{
			Tp: ShowStmtType(i),
		}

		require.NotEqual(t, stmt.SEMCommand(), UnknownCommand, "SEMCommand should not be UnknownCommand for ShowStmtType %d", i)
	}
}

func TestAdminCommand(t *testing.T) {
	for i := 1; i < int(adminTpCount); i++ {
		stmt := &AdminStmt{
			Tp: AdminStmtType(i),
		}

		require.NotEqual(t, stmt.SEMCommand(), UnknownCommand, "SEMCommand should not be UnknownCommand for AdminStmtType %d", i)
	}
}

func TestBRIECommand(t *testing.T) {
	for i := range brieKindCount {
		stmt := &BRIEStmt{
			Kind: i,
		}

		require.NotEqual(t, stmt.SEMCommand(), UnknownCommand, "SEMCommand should not be UnknownCommand for BRIEKind %s", i)
	}
}

func TestRefreshMaterializedViewCommand(t *testing.T) {
	require.Equal(t, RefreshMaterializedViewCommand, (&RefreshMaterializedViewStmt{}).SEMCommand())
	require.Equal(t, RefreshMaterializedViewCommand, (&RefreshMaterializedViewImplementStmt{}).SEMCommand())
	require.Equal(t, "CANCEL MATERIALIZED VIEW REFRESH JOB", (&CancelMaterializedViewJobStmt{Tp: CancelMaterializedViewJobTypeRefresh}).SEMCommand())
}

func TestRefreshMaterializedViewMode(t *testing.T) {
	mode, err := (&RefreshMaterializedViewStmt{Type: RefreshMaterializedViewTypeFast}).Mode()
	require.NoError(t, err)
	require.Equal(t, RefreshMaterializedViewModeFast, mode)

	mode, err = (&RefreshMaterializedViewStmt{Type: RefreshMaterializedViewTypeComplete, CompleteType: RefreshMaterializedViewCompleteTypeDeltaApply}).Mode()
	require.NoError(t, err)
	require.Equal(t, RefreshMaterializedViewModeCompleteDeltaApply, mode)

	_, err = (&RefreshMaterializedViewStmt{Type: RefreshMaterializedViewTypeComplete}).Mode()
	require.ErrorContains(t, err, "COMPLETE refresh mode must be specified explicitly")
}
