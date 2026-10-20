// Copyright 2024 PingCAP, Inc.
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

package session

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetStartMode(t *testing.T) {
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion))
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion+1))
	require.Equal(t, ddl.Upgrade, getStartMode(currentBootstrapVersion-1))
	require.Equal(t, ddl.Bootstrap, getStartMode(0))
}

func TestNormalizeStmtCancellationError(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	require.NoError(t, handlePendingSQLKillerSignal(vars))
	require.ErrorIs(t, executor.NormalizeStmtCancellationError(vars, context.Canceled), context.Canceled)
	vars.SQLKiller.SendKillSignal(sqlkiller.MaxExecTimeExceeded)
	require.True(t, exeerrors.ErrMaxExecTimeExceeded.Equal(handlePendingSQLKillerSignal(vars)))

	// A successful or undetermined commit result takes priority over a timeout signal.
	require.NoError(t, executor.NormalizeStmtCancellationError(vars, nil))
	require.True(t, terror.ErrResultUndetermined.Equal(
		executor.NormalizeStmtCancellationError(vars, terror.ErrResultUndetermined),
	))

	otherErr := errors.New("other error")
	require.ErrorIs(t, executor.NormalizeStmtCancellationError(vars, otherErr), otherErr)

	err := executor.NormalizeStmtCancellationError(vars, context.Canceled)
	require.True(t, exeerrors.ErrMaxExecTimeExceeded.Equal(err))
	err = executor.NormalizeStmtCancellationError(vars, context.DeadlineExceeded)
	require.True(t, exeerrors.ErrMaxExecTimeExceeded.Equal(err))
	err = executor.NormalizeStmtCancellationError(vars, fmt.Errorf("request canceled: %w", context.Canceled))
	require.True(t, exeerrors.ErrMaxExecTimeExceeded.Equal(err))
	err = executor.NormalizeStmtCancellationError(vars, fmt.Errorf("request deadline exceeded: %w", context.DeadlineExceeded))
	require.True(t, exeerrors.ErrMaxExecTimeExceeded.Equal(err))
	err = executor.NormalizeStmtCancellationError(vars, status.Error(codes.Canceled, "canceled"))
	require.True(t, exeerrors.ErrMaxExecTimeExceeded.Equal(err))
	err = executor.NormalizeStmtCancellationError(vars, status.Error(codes.DeadlineExceeded, "deadline exceeded"))
	require.True(t, exeerrors.ErrMaxExecTimeExceeded.Equal(err))

	vars.SQLKiller.Reset()
	vars.SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	require.True(t, exeerrors.ErrQueryInterrupted.Equal(handlePendingSQLKillerSignal(vars)))
	require.True(t, exeerrors.ErrQueryInterrupted.Equal(executor.NormalizeStmtCancellationError(vars, context.Canceled)))
}

func TestSetProcessInfoDuringRetry(t *testing.T) {
	se := &session{sessionVars: variable.NewSessionVars(nil)}
	start := time.Unix(1, 0)
	se.SetProcessInfo("commit", start, mysql.ComQuery, 0)

	// Transaction replay can publish different SQL while retaining the outer statement's start time.
	se.sessionVars.RetryInfo.Retrying = true
	se.SetProcessInfo("update t set a = 2", time.Unix(2, 0), mysql.ComQuery, 0)
	require.Equal(t, start, se.ShowProcess().Time)

	se.sessionVars.RetryInfo.Retrying = false
	nextStart := time.Unix(3, 0)
	se.SetProcessInfo("select 1", nextStart, mysql.ComQuery, 0)
	require.Equal(t, nextStart, se.ShowProcess().Time)
}
