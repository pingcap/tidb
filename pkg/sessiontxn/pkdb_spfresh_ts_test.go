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

package sessiontxn_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit/spfreshtest"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type spfreshTSFuture struct {
	ctx         context.Context
	ts          uint64
	err         error
	waited      bool
	waitStarted chan struct{}
}

func (f *spfreshTSFuture) Wait() (uint64, error) {
	if f.waited {
		panic("TSO future consumed more than once")
	}
	f.waited = true
	if f.waitStarted != nil {
		close(f.waitStarted)
		<-f.ctx.Done()
		return 0, f.ctx.Err()
	}
	if err := f.ctx.Err(); err != nil {
		return 0, err
	}
	return f.ts, f.err
}

type spfreshTSOracle struct {
	oracle.Oracle
	futures []*spfreshTSFuture
	scopes  []string
	err     error
}

func (o *spfreshTSOracle) GetTimestampAsync(ctx context.Context, option *oracle.Option) oracle.Future {
	future := &spfreshTSFuture{ctx: ctx, ts: uint64(len(o.futures) + 100), err: o.err}
	o.futures = append(o.futures, future)
	o.scopes = append(o.scopes, option.TxnScope)
	return future
}

type spfreshTSStore struct {
	kv.Storage
	oracle  *spfreshTSOracle
	retries int
	scope   string
}

func (s *spfreshTSStore) GetOracle() oracle.Oracle { return s.oracle }

func (s *spfreshTSStore) CurrentVersion(scope string) (kv.Version, error) {
	s.retries++
	s.scope = scope
	return kv.NewVersion(1000), nil
}

func TestSPFreshPrefetchIsConsumedOncePerExecution(t *testing.T) {
	spfreshtest.SetEnabled(t, true)
	sctx := mock.NewContext()
	o := &spfreshTSOracle{}
	store := &spfreshTSStore{oracle: o}
	sctx.Store = store
	sctx.GetSessionVars().TxnCtx.TxnScope = "local"
	sctx.GetSessionVars().TxnCtx.StartTS = 10
	sctx.GetSessionVars().SnapshotTS = 20
	sctx.GetSessionVars().ConnectionID = 1
	require.NoError(t, sctx.GetSessionVars().SetSystemVar("tidb_low_resolution_tso", "1"))
	require.True(t, sctx.GetSessionVars().UseLowResolutionTSO())
	ctx := context.Background()
	sessiontxn.PrefetchSPFreshReadVersion(ctx, sctx)
	sessiontxn.PrefetchSPFreshReadVersion(ctx, sctx)
	require.Len(t, o.futures, 1)
	require.False(t, o.futures[0].waited, "planning must start the request without waiting")
	first, err := sessiontxn.SPFreshReadVersion(ctx, sctx)
	require.NoError(t, err)
	require.Equal(t, uint64(100), first.Ver)
	require.Equal(t, []string{"local"}, o.scopes)
	second, err := sessiontxn.SPFreshReadVersion(ctx, sctx)
	require.NoError(t, err)
	require.Greater(t, second.Ver, first.Ver, "direct/repeated executor entry must request a new TS")
	require.Len(t, o.futures, 2)
	require.Zero(t, store.retries)
}

func TestSPFreshPrefetchDoesNotSurviveStatementOrRetryReset(t *testing.T) {
	spfreshtest.SetEnabled(t, true)
	for _, retry := range []bool{false, true} {
		t.Run(map[bool]string{false: "statement", true: "retry"}[retry], func(t *testing.T) {
			sctx := mock.NewContext()
			o := &spfreshTSOracle{}
			sctx.Store = &spfreshTSStore{oracle: o}
			sessiontxn.PrefetchSPFreshReadVersion(context.Background(), sctx)
			if retry {
				sctx.GetSessionVars().StmtCtx.ResetForRetry()
			} else {
				require.True(t, sctx.GetSessionVars().StmtCtx.Reset())
			}
			version, err := sessiontxn.SPFreshReadVersion(context.Background(), sctx)
			require.NoError(t, err)
			require.Equal(t, uint64(101), version.Ver)
			require.False(t, o.futures[0].waited)
			require.True(t, o.futures[1].waited)
		})
	}
}

func TestSPFreshPrefetchFailureKeepsCurrentVersionRetry(t *testing.T) {
	spfreshtest.SetEnabled(t, true)
	sctx := mock.NewContext()
	o := &spfreshTSOracle{err: errors.New("temporary TSO failure")}
	store := &spfreshTSStore{oracle: o}
	sctx.Store = store
	sctx.GetSessionVars().TxnCtx.TxnScope = "local"
	sessiontxn.PrefetchSPFreshReadVersion(context.Background(), sctx)
	version, err := sessiontxn.SPFreshReadVersion(context.Background(), sctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), version.Ver)
	require.Equal(t, 1, store.retries)
	require.Equal(t, "local", store.scope)
	ctx, cancel := context.WithCancel(context.Background())
	sessiontxn.PrefetchSPFreshReadVersion(ctx, sctx)
	cancel()
	_, err = sessiontxn.SPFreshReadVersion(ctx, sctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, store.retries, "cancellation must not start a retry")
}

func TestSPFreshExplainDoesNotPrefetchUntilAnalyze(t *testing.T) {
	spfreshtest.SetEnabled(t, true)
	sctx := mock.NewContext()
	o := &spfreshTSOracle{}
	sctx.Store = &spfreshTSStore{oracle: o}
	sctx.GetSessionVars().StmtCtx.InExplainStmt = true
	sessiontxn.PrefetchSPFreshReadVersion(context.Background(), sctx)
	require.Empty(t, o.futures)
	sctx.GetSessionVars().StmtCtx.InExplainAnalyzeStmt = true
	sessiontxn.PrefetchSPFreshReadVersion(context.Background(), sctx)
	require.Len(t, o.futures, 1)
}

func TestSPFreshPrefetchCancellationWhileWaitingDoesNotRetry(t *testing.T) {
	spfreshtest.SetEnabled(t, true)
	sctx := mock.NewContext()
	o := &spfreshTSOracle{}
	store := &spfreshTSStore{oracle: o}
	sctx.Store = store
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sessiontxn.PrefetchSPFreshReadVersion(ctx, sctx)
	started := make(chan struct{})
	o.futures[0].waitStarted = started
	done := make(chan error, 1)
	go func() {
		_, err := sessiontxn.SPFreshReadVersion(ctx, sctx)
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("executor did not wait on the prefetched request")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, store.retries)
}

func TestSPFreshPrefetchIsIsolatedAcrossNestedStatements(t *testing.T) {
	spfreshtest.SetEnabled(t, true)
	for _, outerPrefetch := range []bool{false, true} {
		for _, consumeInner := range []bool{false, true} {
			sctx := mock.NewContext()
			o := &spfreshTSOracle{}
			sctx.Store = &spfreshTSStore{oracle: o}
			ctx := context.Background()
			if outerPrefetch {
				sessiontxn.PrefetchSPFreshReadVersion(ctx, sctx)
			}
			sc := sctx.GetSessionVars().StmtCtx
			taskID := sc.TaskID
			restore := sc.ResetExecutionCaches()
			sessiontxn.PrefetchSPFreshReadVersion(ctx, sctx)
			inner := o.futures[len(o.futures)-1]
			if consumeInner {
				version, err := sessiontxn.SPFreshReadVersion(ctx, sctx)
				require.NoError(t, err)
				require.Equal(t, inner.ts, version.Ver)
			}
			restore()
			require.Equal(t, taskID, sc.TaskID, "nested routines share the caller's TaskID")
			version, err := sessiontxn.SPFreshReadVersion(ctx, sctx)
			require.NoError(t, err)
			if outerPrefetch {
				require.Equal(t, uint64(100), version.Ver, "restore the caller's own pending request")
			} else {
				require.Greater(t, version.Ver, inner.ts, "discard any unused inner future")
			}
			require.Len(t, o.futures, 2, "inner and outer executions require separate TSO requests")
		}
	}
}
