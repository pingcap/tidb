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
	"time"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type unionEmptyExec struct {
	*exec.BaseExecutor
	nextEntered chan struct{}
	closeCh     <-chan struct{}
}

func (e *unionEmptyExec) Open(context.Context) error { return nil }
func (e *unionEmptyExec) Next(_ context.Context, req *chunk.Chunk) error {
	close(e.nextEntered)
	<-e.closeCh
	req.Reset()
	return nil
}
func (e *unionEmptyExec) Close() error { return nil }

type unionPanicExec struct {
	*exec.BaseExecutor
	nextEntered chan struct{}
	panicCh     <-chan struct{}
}

func (e *unionPanicExec) Open(context.Context) error { return nil }
func (e *unionPanicExec) Next(_ context.Context, _ *chunk.Chunk) error {
	close(e.nextEntered)
	<-e.panicCh
	panic("union exec panic during close")
}
func (e *unionPanicExec) Close() error { return nil }

func TestUnionExecCloseWaitsForWorkers(t *testing.T) {
	ctx := mock.NewContext()
	schema := expression.NewSchema()
	closeCh := make(chan struct{})
	nextEntered := make(chan struct{})
	childBase := exec.NewBaseExecutor(ctx, schema, 0)
	child := &unionEmptyExec{BaseExecutor: &childBase, nextEntered: nextEntered, closeCh: closeCh}
	unionBase := exec.NewBaseExecutor(ctx, schema, 1, child)
	union := &UnionExec{BaseExecutor: unionBase, concurrency: 1}

	require.NoError(t, union.Open(context.Background()))
	chk := exec.NewFirstChunk(union)

	nextDone := make(chan struct{})
	go func() {
		_ = union.Next(context.Background(), chk)
		close(nextDone)
	}()

	select {
	case <-nextEntered:
	case <-time.After(2 * time.Second):
		t.Fatalf("union worker did not enter Next")
	}

	closeDone := make(chan struct{})
	go func() {
		_ = union.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		t.Fatalf("union Close returned before worker exited")
	case <-time.After(100 * time.Millisecond):
	}

	close(closeCh)

	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("union Close did not wait for worker exit")
	}

	select {
	case <-nextDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("union Next did not return after Close")
	}
}

func TestUnionExecCloseReturnsAfterWorkerPanicDuringShutdown(t *testing.T) {
	ctx := mock.NewContext()
	schema := expression.NewSchema()
	panicCh := make(chan struct{})
	nextEntered := make(chan struct{})
	childBase := exec.NewBaseExecutor(ctx, schema, 0)
	child := &unionPanicExec{BaseExecutor: &childBase, nextEntered: nextEntered, panicCh: panicCh}
	unionBase := exec.NewBaseExecutor(ctx, schema, 1, child)
	union := &UnionExec{BaseExecutor: unionBase, concurrency: 1}

	require.NoError(t, union.Open(context.Background()))
	chk := exec.NewFirstChunk(union)

	nextDone := make(chan struct{})
	go func() {
		_ = union.Next(context.Background(), chk)
		close(nextDone)
	}()

	select {
	case <-nextEntered:
	case <-time.After(2 * time.Second):
		t.Fatalf("union worker did not enter Next")
	}

	closeDone := make(chan struct{})
	go func() {
		_ = union.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		t.Fatalf("union Close returned before worker panic")
	case <-time.After(100 * time.Millisecond):
	}

	close(panicCh)

	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("union Close did not return after worker panic")
	}

	select {
	case <-nextDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("union Next did not return after worker panic")
	}
}
