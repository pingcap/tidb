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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// inflightTestInner controls the actual Open/Next boundary used by
// fetchAllInners, so a retry by any waiter is observable independently of
// whether the query's consumer has received the owner's error yet.
type inflightTestInner struct {
	exec.BaseExecutor
	open func(context.Context) error
	next func(*chunk.Chunk) error
}

func (e *inflightTestInner) Open(ctx context.Context) error {
	if e.open != nil {
		return e.open(ctx)
	}
	return nil
}

func (e *inflightTestInner) Next(_ context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.next != nil {
		return e.next(chk)
	}
	return nil
}

func TestParallelApplyInflightOutcome(t *testing.T) {
	for _, outcome := range []string{"open-error", "next-error", "panic", "cancel", "shutdown", "cache-rejected"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			sctx := mock.NewContext()
			if outcome == "cache-rejected" {
				sctx.GetSessionVars().MemQuotaApplyCache = 1
			}
			fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
			schema := expression.NewSchema(&expression.Column{RetType: fields[0]})
			const concurrency = 4
			e := &ParallelNestedLoopApplyExec{
				BaseExecutor: exec.NewBaseExecutor(sctx, schema, 0),
				outerExec:    &inflightTestInner{BaseExecutor: exec.NewBaseExecutor(sctx, schema, 1)},
				concurrency:  concurrency,
				useCache:     true,
				corCols:      make([][]*expression.CorrelatedColumn, concurrency),
				innerFilter:  make([]expression.CNFExprs, concurrency),
				innerExecs:   make([]exec.Executor, concurrency),
			}
			ownerStarted := make(chan struct{})
			releaseOwner := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseOwner) }) }
			defer release()
			ownerErr := errors.New("inflight owner read failed")
			var waiterOpens atomic.Int32
			for id := range concurrency {
				inner := &inflightTestInner{BaseExecutor: exec.NewBaseExecutor(sctx, schema, id+2)}
				inner.open = func(context.Context) error {
					waiterOpens.Add(1)
					return nil
				}
				e.innerExecs[id] = inner
			}
			owner := e.innerExecs[0].(*inflightTestInner)
			owner.open = func(ctx context.Context) error {
				close(ownerStarted)
				select {
				case <-releaseOwner:
				case <-ctx.Done():
					return ctx.Err()
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if outcome == "open-error" {
					return ownerErr
				}
				return nil
			}
			owner.next = func(chk *chunk.Chunk) error {
				if outcome == "next-error" {
					return ownerErr
				}
				return nil
			}
			require.NoError(t, e.Open(ctx))
			defer func() { require.NoError(t, e.Close()) }()
			for id := range concurrency {
				data := types.NewIntDatum(7)
				e.corCols[id] = []*expression.CorrelatedColumn{{
					Column: expression.Column{Index: 0, RetType: fields[0]},
					Data:   &data,
				}}
				row := chunk.MutRowFromValues(int64(7)).ToRow()
				e.outerRow[id] = &row
			}

			waitersStarted := make(chan struct{})
			var waiterCount atomic.Int32
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/parallelApplyWaitForInflight", func() {
				if waiterCount.Add(1) == concurrency-1 {
					close(waitersStarted)
				}
			})
			if outcome == "panic" {
				// Unlike an inner executor panic (converted to an error by
				// exec.Next), this panics in fetchAllInners itself.
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/parallelApplySetCachePanic", "panic(\"inflight owner panic\")")
			}
			if outcome == "cache-rejected" {
				returned := false
				owner.next = func(chk *chunk.Chunk) error {
					if !returned {
						chk.AppendInt64(0, 42)
						returned = true
					}
					return nil
				}
			}
			type readResult struct {
				id    int
				err   error
				panic any
			}
			results := make(chan readResult, concurrency)
			read := func(id int) {
				r := readResult{id: id}
				defer func() {
					r.panic = recover()
					results <- r
				}()
				r.err = e.fetchAllInners(ctx, id)
			}
			await := func(ch <-chan struct{}) {
				select {
				case <-ch:
				case <-ctx.Done():
					t.Fatal("timed out waiting for owner/waiters")
				}
			}
			go read(0)
			await(ownerStarted)
			for id := 1; id < concurrency; id++ {
				go read(id)
			}
			await(waitersStarted)
			switch outcome {
			case "cancel":
				cancel()
			case "shutdown":
				close(e.exit)
			}
			release()
			reads := make([]readResult, 0, concurrency)
			for range concurrency {
				select {
				case r := <-results:
					reads = append(reads, r)
				case <-time.After(10 * time.Second):
					t.Fatal("inflight read did not finish")
				}
			}
			for _, r := range reads {
				switch outcome {
				case "open-error", "next-error":
					require.ErrorIs(t, r.err, ownerErr)
					require.Nil(t, r.panic)
				case "panic":
					if r.id == 0 {
						require.NotNil(t, r.panic, "owner panic must still reach worker recovery")
					} else {
						require.Nil(t, r.panic)
						require.ErrorContains(t, r.err, "inflight owner panic")
					}
				case "cancel":
					require.ErrorIs(t, r.err, context.Canceled)
					require.Nil(t, r.panic)
				default:
					require.NoError(t, r.err)
					require.Nil(t, r.panic)
				}
			}
			require.Zero(t, waiterOpens.Load(), "waiters must not retry the owner's inner read")
			require.Empty(t, e.inflight)
			if outcome == "cache-rejected" {
				key, err := codec.EncodeKey(sctx.GetSessionVars().StmtCtx.TimeZone(), nil, types.NewIntDatum(7))
				require.NoError(t, err)
				cached, err := e.cache.Get(key)
				require.NoError(t, err)
				require.Nil(t, cached, "completed list should exceed the cache quota")
				for id := range concurrency {
					require.Same(t, e.innerList[0], e.innerList[id])
					require.Equal(t, int64(42), chunk.NewIterator4List(e.innerList[id]).Begin().GetInt64(0))
				}
			}
		})
	}
}
