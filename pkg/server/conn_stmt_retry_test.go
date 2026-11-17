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

package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/internal/packetio"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	serverutil "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

type stmtAttemptSession struct {
	sessionapi.Session
	vars        *variable.SessionVars
	executeStmt func(context.Context, ast.StmtNode) (sqlexec.RecordSet, error)
}

func (s *stmtAttemptSession) GetSessionVars() *variable.SessionVars { return s.vars }

func (s *stmtAttemptSession) ExecuteStmt(ctx context.Context, stmt ast.StmtNode) (sqlexec.RecordSet, error) {
	return s.executeStmt(ctx, stmt)
}

type stmtAttemptTxnManager struct {
	sessiontxn.TxnManager
	onError func(context.Context, sessiontxn.StmtErrorHandlePoint, error) (sessiontxn.StmtErrorAction, error)
}

func (m *stmtAttemptTxnManager) OnStmtErrorForNextAction(ctx context.Context, point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	return m.onError(ctx, point, err)
}

func TestRunStmtWithRetry(t *testing.T) {
	ctx := context.Background()
	queryErr := errors.New("query failed")
	txnErr := errors.New("transaction failed")
	timeout := errors.Annotate(storeerr.ErrTiFlashServerTimeout, "prepared statement")
	for _, tt := range []struct {
		name          string
		firstErr      error
		safety        stmtRetrySafety
		action        sessiontxn.StmtErrorAction
		txnErr        error
		allowFallback bool
		retryErr      error
		attempts      int
		wantErr       error
		fallback      bool
	}{
		{name: "success", safety: stmtRetryUnsafe, attempts: 1},
		{name: "unclassified error", firstErr: queryErr, safety: stmtRetrySafe, attempts: 1, wantErr: queryErr},
		{name: "transaction replaces unsafe error", firstErr: queryErr, safety: stmtRetryUnsafe, txnErr: txnErr, attempts: 1, wantErr: txnErr},
		{name: "transaction retry", firstErr: queryErr, safety: stmtRetrySafe, action: sessiontxn.StmtActionRetryReady, attempts: 2},
		{name: "transaction retry after output prohibited", firstErr: queryErr, safety: stmtRetryUnsafe, action: sessiontxn.StmtActionRetryReady, attempts: 1, wantErr: queryErr},
		{name: "transaction retry takes precedence", firstErr: timeout, safety: stmtRetrySafe, action: sessiontxn.StmtActionRetryReady, allowFallback: true, attempts: 2},
		{name: "no fallback after transaction retry", firstErr: queryErr, safety: stmtRetrySafe, action: sessiontxn.StmtActionRetryReady, allowFallback: true, retryErr: timeout, attempts: 2, wantErr: timeout},
		{name: "wrapped timeout fallback", firstErr: timeout, safety: stmtRetrySafe, allowFallback: true, attempts: 2, fallback: true},
		{name: "fallback after output prohibited", firstErr: timeout, safety: stmtRetryUnsafe, allowFallback: true, attempts: 1, wantErr: timeout},
		{name: "fallback disabled", firstErr: timeout, safety: stmtRetrySafe, attempts: 1, wantErr: timeout},
		{name: "non-timeout cannot fallback", firstErr: queryErr, safety: stmtRetrySafe, allowFallback: true, attempts: 1, wantErr: queryErr},
		{name: "transaction replaces timeout", firstErr: timeout, safety: stmtRetrySafe, txnErr: txnErr, allowFallback: true, attempts: 1, wantErr: txnErr},
		{name: "failed fallback is final", firstErr: timeout, safety: stmtRetrySafe, allowFallback: true, retryErr: timeout, attempts: 2, wantErr: timeout, fallback: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			vars := variable.NewSessionVars(nil)
			vars.IsolationReadEngines = map[kv.StoreType]struct{}{kv.TiKV: {}, kv.TiFlash: {}}
			vars.AllowFallbackToTiKV = make(map[kv.StoreType]struct{})
			if tt.allowFallback {
				vars.AllowFallbackToTiKV[kv.TiFlash] = struct{}{}
			}
			classifications := 0
			vars.TxnManager = &stmtAttemptTxnManager{onError: func(gotCtx context.Context, point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
				classifications++
				require.Equal(t, ctx, gotCtx)
				require.Equal(t, sessiontxn.StmtErrAfterQuery, point)
				require.Same(t, tt.firstErr, err)
				if tt.txnErr != nil {
					return sessiontxn.ErrorAction(tt.txnErr)
				}
				if tt.action == sessiontxn.StmtActionRetryReady {
					return sessiontxn.RetryReady()
				}
				return sessiontxn.NoIdea()
			}}
			cc := &clientConn{}
			cc.SetCtx(&TiDBContext{Session: &stmtAttemptSession{vars: vars}})
			attempts := 0
			err := cc.runStmtWithRetry(ctx, func(fallbackErr error) (stmtRetrySafety, error) {
				attempts++
				if attempts == 1 {
					require.Nil(t, fallbackErr)
					require.False(t, vars.RetryInfo.Retrying)
					return tt.safety, tt.firstErr
				}
				require.Equal(t, 2, attempts)
				if tt.fallback {
					require.Same(t, tt.firstErr, fallbackErr)
					require.NotContains(t, vars.IsolationReadEngines, kv.TiFlash)
					require.False(t, vars.RetryInfo.Retrying)
				} else {
					require.Nil(t, fallbackErr)
					require.Contains(t, vars.IsolationReadEngines, kv.TiFlash)
					require.True(t, vars.RetryInfo.Retrying)
				}
				return stmtRetrySafe, tt.retryErr
			})
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Same(t, tt.wantErr, err)
			}
			require.Equal(t, tt.attempts, attempts)
			if tt.firstErr == nil {
				require.Zero(t, classifications)
			} else {
				require.Equal(t, 1, classifications)
			}
			require.False(t, vars.RetryInfo.Retrying)
			require.Equal(t, map[kv.StoreType]struct{}{kv.TiKV: {}, kv.TiFlash: {}}, vars.IsolationReadEngines)
		})
	}
}

func TestRunStmtWithRetryRestoresState(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		for _, initiallySet := range []bool{false, true} {
			for _, exit := range []string{"success", "error", "panic"} {
				t.Run(fmt.Sprintf("fallback=%t/initially_set=%t/%s", fallback, initiallySet, exit), func(t *testing.T) {
					vars := variable.NewSessionVars(nil)
					vars.RetryInfo.Retrying = initiallySet
					vars.IsolationReadEngines = map[kv.StoreType]struct{}{kv.TiKV: {}}
					if initiallySet {
						vars.IsolationReadEngines[kv.TiFlash] = struct{}{}
					}
					vars.AllowFallbackToTiKV = map[kv.StoreType]struct{}{kv.TiFlash: {}}
					vars.TxnManager = &stmtAttemptTxnManager{onError: func(context.Context, sessiontxn.StmtErrorHandlePoint, error) (sessiontxn.StmtErrorAction, error) {
						if fallback {
							return sessiontxn.NoIdea()
						}
						return sessiontxn.RetryReady()
					}}
					cc := &clientConn{}
					cc.SetCtx(&TiDBContext{Session: &stmtAttemptSession{vars: vars}})
					retryErr := errors.New("retry failed")
					attempts := 0
					run := func() {
						err := cc.runStmtWithRetry(context.Background(), func(fallbackErr error) (stmtRetrySafety, error) {
							attempts++
							if attempts == 1 {
								return stmtRetrySafe, storeerr.ErrTiFlashServerTimeout
							}
							if fallback {
								require.NotNil(t, fallbackErr)
								require.NotContains(t, vars.IsolationReadEngines, kv.TiFlash)
								require.Equal(t, initiallySet, vars.RetryInfo.Retrying)
							} else {
								require.True(t, vars.RetryInfo.Retrying)
							}
							switch exit {
							case "panic":
								panic(retryErr)
							case "error":
								return stmtRetryUnsafe, retryErr
							default:
								return stmtRetryUnsafe, nil
							}
						})
						if exit == "error" {
							require.Same(t, retryErr, err)
						} else {
							require.NoError(t, err)
						}
					}
					if exit == "panic" {
						require.PanicsWithValue(t, retryErr, run)
					} else {
						run()
					}
					require.Equal(t, 2, attempts)
					require.Equal(t, initiallySet, vars.RetryInfo.Retrying)
					_, hasTiFlash := vars.IsolationReadEngines[kv.TiFlash]
					require.Equal(t, initiallySet, hasTiFlash)
					require.Contains(t, vars.IsolationReadEngines, kv.TiKV)
				})
			}
		}
	}
}

func TestStmtRetryWarningHandling(t *testing.T) {
	store := testkit.CreateMockStore(t)
	for _, tt := range []struct {
		name         string
		prepared     bool
		failFallback bool
		multiStmt    bool
		warnings     int
	}{
		{name: "text success retains parser warning", warnings: 2},
		{name: "prepared success", prepared: true, warnings: 1},
		{name: "text failed fallback omits timeout", failFallback: true, warnings: 1},
		{name: "prepared failed fallback retains timeout", prepared: true, failFallback: true, warnings: 2},
		{name: "text nonfinal fallback omits timeout", multiStmt: true, warnings: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			vars := tk.Session().GetSessionVars()
			vars.AllowFallbackToTiKV = map[kv.StoreType]struct{}{kv.TiFlash: {}}
			vars.IsolationReadEngines[kv.TiFlash] = struct{}{}
			fallbackErr := errors.New("fallback execution failed")
			attempts := 0
			se := &stmtAttemptSession{Session: tk.Session(), vars: vars}
			se.executeStmt = func(ctx context.Context, stmt ast.StmtNode) (sqlexec.RecordSet, error) {
				attempts++
				if attempts == 1 || tt.failFallback {
					// Model the warning reset even when execution fails before producing a result set.
					vars.StmtCtx.SetWarnings(nil)
					if attempts == 1 {
						return nil, storeerr.ErrTiFlashServerTimeout
					}
					return nil, fallbackErr
				}
				return tk.Session().ExecuteStmt(ctx, stmt)
			}
			cc := &clientConn{
				alloc:      arena.NewAllocator(1024),
				chunkAlloc: chunk.NewAllocator(),
				pkt:        packetio.NewForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
			}
			cc.SetCtx(&TiDBContext{Session: se, stmts: make(map[int]*TiDBStatement)})
			ctx := context.Background()
			var err error
			if tt.prepared {
				stmt, _, _, prepareErr := cc.ctx.Prepare("select 1")
				require.NoError(t, prepareErr)
				err = cc.executePlanCacheStmt(ctx, stmt, nil, false)
			} else {
				sql := "select /*+ unknown_hint() */ 1"
				if tt.multiStmt {
					cc.ctx.SetClientCapability(vars.ClientCapability | mysql.ClientMultiStatements)
					sql += "; select 2"
				}
				err = cc.handleQuery(ctx, sql)
			}
			if tt.failFallback {
				require.True(t, errors.ErrorEqual(err, fallbackErr), "%v", err)
			} else {
				require.NoError(t, err)
			}
			wantAttempts := 2
			if tt.multiStmt {
				wantAttempts++
			}
			require.Equal(t, wantAttempts, attempts)
			warnings := vars.StmtCtx.GetWarnings()
			require.Len(t, warnings, tt.warnings)
			if tt.prepared || (!tt.failFallback && !tt.multiStmt) {
				require.True(t, errors.ErrorEqual(warnings[len(warnings)-1].Err, storeerr.ErrTiFlashServerTimeout))
			} else {
				require.False(t, errors.ErrorEqual(warnings[0].Err, storeerr.ErrTiFlashServerTimeout))
			}
			if !tt.prepared && !tt.failFallback {
				require.ErrorContains(t, warnings[0].Err, "hint")
			}
			require.Contains(t, vars.IsolationReadEngines, kv.TiFlash)
		})
	}
}

func TestTextFallbackReusesWarningBuffer(t *testing.T) {
	store := testkit.CreateMockStore(t)
	for _, parserWarning := range []bool{false, true} {
		for _, previousWarnings := range []int{0, 1} {
			t.Run(fmt.Sprintf("parser=%t/previous=%d", parserWarning, previousWarnings), func(t *testing.T) {
				tk := testkit.NewTestKit(t, store)
				// Use a cached statement context with spare warning capacity. The fallback
				// will reset and reuse this context after the first execution swaps it out.
				tk.MustExec("do 0")
				vars := tk.Session().GetSessionVars()
				vars.StmtCtx.SetWarnings(make([]contextutil.SQLWarn, 0, 8))
				for range previousWarnings {
					vars.StmtCtx.AppendWarning(errors.New("previous statement warning"))
				}
				vars.AllowFallbackToTiKV = map[kv.StoreType]struct{}{kv.TiFlash: {}}
				vars.IsolationReadEngines[kv.TiFlash] = struct{}{}
				executionWarnings := []error{
					errors.New("fallback warning 1"),
					errors.New("fallback warning 2"),
					errors.New("fallback warning 3"),
				}
				attempts := 0
				se := &stmtAttemptSession{Session: tk.Session(), vars: vars}
				se.executeStmt = func(ctx context.Context, stmt ast.StmtNode) (sqlexec.RecordSet, error) {
					attempts++
					rs, err := tk.Session().ExecuteStmt(ctx, stmt)
					require.NoError(t, err)
					if attempts == 1 {
						require.NoError(t, rs.Close())
						return nil, storeerr.ErrTiFlashServerTimeout
					}
					// Warnings produced before ExecuteStmt returns must not overwrite
					// the saved parser warnings or the timeout warning.
					for _, warning := range executionWarnings {
						vars.StmtCtx.AppendWarning(warning)
					}
					return rs, nil
				}
				cc := &clientConn{
					alloc:      arena.NewAllocator(1024),
					chunkAlloc: chunk.NewAllocator(),
					pkt:        packetio.NewForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
				}
				cc.SetCtx(&TiDBContext{Session: se, stmts: make(map[int]*TiDBStatement)})
				sql := "select 1"
				if parserWarning {
					sql = "select /*+ unknown_hint() */ 1"
				}
				require.NoError(t, cc.handleQuery(context.Background(), sql))
				require.Equal(t, 2, attempts)
				warnings := vars.StmtCtx.GetWarnings()
				wantCount := len(executionWarnings) + 1
				if parserWarning {
					wantCount++
				}
				require.Len(t, warnings, wantCount)
				for i, warning := range executionWarnings {
					require.Same(t, warning, warnings[i].Err)
					require.Equal(t, contextutil.WarnLevelWarning, warnings[i].Level)
				}
				if parserWarning {
					require.ErrorContains(t, warnings[len(executionWarnings)].Err, "hint")
				}
				require.True(t, errors.ErrorEqual(warnings[len(warnings)-1].Err, storeerr.ErrTiFlashServerTimeout))
				require.Equal(t, contextutil.WarnLevelError, warnings[len(warnings)-1].Level)
			})
		}
	}
}

func TestLoadDataLocalRetrySafety(t *testing.T) {
	store := testkit.CreateMockStore(t)
	for _, tt := range []struct {
		name   string
		local  bool
		err    error
		action sessiontxn.StmtErrorAction
	}{
		{name: "transaction retry", local: true, err: errors.New("execution failed"), action: sessiontxn.StmtActionRetryReady},
		{name: "TiFlash fallback", local: true, err: storeerr.ErrTiFlashServerTimeout, action: sessiontxn.StmtActionNoIdea},
		{name: "server file", err: errors.New("execution failed"), action: sessiontxn.StmtActionNoIdea},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			vars := tk.Session().GetSessionVars()
			vars.AllowFallbackToTiKV = map[kv.StoreType]struct{}{kv.TiFlash: {}}
			vars.TxnManager = &stmtAttemptTxnManager{onError: func(context.Context, sessiontxn.StmtErrorHandlePoint, error) (sessiontxn.StmtErrorAction, error) {
				return tt.action, nil
			}}
			// The client terminates its file response with an empty packet, after
			// the server sends the sequence-0 local-infile request.
			input := &testutil.BytesConn{Buffer: *bytes.NewBuffer([]byte{0, 0, 0, 1})}
			var output bytes.Buffer
			cc := &clientConn{
				capability: mysql.ClientLocalFiles,
				alloc:      arena.NewAllocator(1024),
				chunkAlloc: chunk.NewAllocator(),
				pkt:        packetio.New(serverutil.NewBufferedReadConn(input)),
			}
			cc.pkt.SetBufWriter(bufio.NewWriter(&output))
			attempts := 0
			se := &stmtAttemptSession{Session: tk.Session(), vars: vars}
			se.executeStmt = func(context.Context, ast.StmtNode) (sqlexec.RecordSet, error) {
				attempts++
				if tt.local {
					builder, ok := se.Value(executor.LoadDataReaderBuilderKey).(executor.LoadDataReaderBuilder)
					require.True(t, ok)
					reader, err := builder.Build("file.csv")
					require.NoError(t, err)
					require.NoError(t, reader.Close())
				}
				return nil, tt.err
			}
			cc.SetCtx(&TiDBContext{Session: se})
			stmt := &ast.LoadDataStmt{FileLocRef: ast.FileLocServerOrRemote}
			if tt.local {
				stmt.FileLocRef = ast.FileLocClient
			}
			err := cc.runStmtWithRetry(context.Background(), func(error) (stmtRetrySafety, error) {
				safety, err := cc.handleStmt(context.Background(), stmt, nil, true)
				require.Equal(t, stmtRetrySafety(!tt.local), safety)
				return safety, err
			})
			require.Same(t, tt.err, err)
			require.Equal(t, 1, attempts)
			require.Nil(t, se.Value(executor.LoadDataReaderBuilderKey))
			if tt.local {
				require.Equal(t, append([]byte{9, 0, 0, 0, mysql.LocalInFileHeader}, []byte("file.csv")...), output.Bytes())
			} else {
				require.Empty(t, output.Bytes())
			}
		})
	}
}
