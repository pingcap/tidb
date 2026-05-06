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

	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

func TestRewriteContextCanceledWithKillSignal(t *testing.T) {
	stmt := &ExecStmt{Ctx: mock.NewContext()}
	stmt.Ctx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.MaxExecTimeExceeded)

	err := rewriteContextCanceledWithKillSignal(stmt, context.Canceled)
	require.True(t, exeerrors.ErrMaxExecTimeExceeded.Equal(err), "expected max execution time exceeded, got %v", err)
}

func TestRewriteContextCanceledWithoutKillSignal(t *testing.T) {
	stmt := &ExecStmt{Ctx: mock.NewContext()}

	err := rewriteContextCanceledWithKillSignal(stmt, context.Canceled)
	require.ErrorIs(t, err, context.Canceled)
}
