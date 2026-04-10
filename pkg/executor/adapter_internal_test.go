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
