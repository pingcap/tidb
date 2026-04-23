//go:build intest

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

package ttlworker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

type mockScanWorker struct {
	baseWorker
	curTask       *ttlScanTask
	curTaskResult *ttlScanTaskExecResult

	sessionInfoSchema infoschema.InfoSchema
	executeSQL        func(ctx context.Context, sql string, args ...any) ([]chunk.Row, error)
}

// NewMockScanWorker creates a mock scan worker for external intest packages.
func NewMockScanWorker(_ *testing.T) *mockScanWorker {
	w := &mockScanWorker{
		sessionInfoSchema: infoschema.MockInfoSchema(nil),
	}
	w.init(w.loop)
	return w
}

// SetInfoSchema overrides the session infoschema used by the mock worker.
func (w *mockScanWorker) SetInfoSchema(is infoschema.InfoSchema) {
	w.sessionInfoSchema = is
}

// SetExecuteSQL overrides the ExecuteSQL behavior used by the mock worker.
func (w *mockScanWorker) SetExecuteSQL(fn func(ctx context.Context, sql string, args ...any) ([]chunk.Row, error)) {
	w.executeSQL = fn
}

func (w *mockScanWorker) CouldSchedule() bool {
	w.Lock()
	defer w.Unlock()
	return w.status == workerStatusRunning && w.curTask == nil && w.curTaskResult == nil
}

func (w *mockScanWorker) Schedule(task *ttlScanTask) error {
	w.Lock()
	if w.status != workerStatusRunning {
		w.Unlock()
		return context.Canceled
	}

	if w.curTaskResult != nil {
		w.Unlock()
		return context.Canceled
	}

	if w.curTask != nil {
		w.Unlock()
		return context.Canceled
	}

	w.curTask = task
	w.curTaskResult = nil
	w.Unlock()
	w.baseWorker.ch <- task
	return nil
}

func (w *mockScanWorker) CurrentTask() *ttlScanTask {
	w.Lock()
	defer w.Unlock()
	return w.curTask
}

func (w *mockScanWorker) PollTaskResult() *ttlScanTaskExecResult {
	w.Lock()
	defer w.Unlock()
	if r := w.curTaskResult; r != nil {
		w.curTask = nil
		w.curTaskResult = nil
		return r
	}
	return nil
}

func (w *mockScanWorker) loop() error {
	ctx := w.baseWorker.ctx
	for w.Status() == workerStatusRunning {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-w.baseWorker.ch:
			if !ok {
				return nil
			}
			task, ok := msg.(*ttlScanTask)
			if !ok {
				continue
			}
			w.handleTask(task)
		}
	}
	return nil
}

func (w *mockScanWorker) handleTask(task *ttlScanTask) {
	result := w.executeTask(task)

	w.baseWorker.Lock()
	w.curTaskResult = result
	w.baseWorker.Unlock()
}

func (w *mockScanWorker) executeTask(task *ttlScanTask) *ttlScanTaskExecResult {
	if w.executeSQL == nil {
		select {
		case <-task.ctx.Done():
			return task.result(task.ctx.Err())
		case <-w.baseWorker.ctx.Done():
			result := task.result(w.baseWorker.ctx.Err())
			result.reason = ReasonWorkerStop
			return result
		}
	}

	execCtx, cancel := context.WithCancel(task.ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-w.baseWorker.ctx.Done():
			cancel()
		case <-task.ctx.Done():
			cancel()
		case <-execCtx.Done():
		}
	}()

	delCh := make(chan *ttlDeleteTask, 16)
	go func() {
		for range delCh {
		}
	}()

	se := newMockTTLSession(w.sessionInfoSchema, w.executeSQL)
	err := task.doScanWithSession(execCtx, delCh, se)
	close(delCh)
	<-done

	result := task.result(err)
	if execCtx.Err() != nil {
		if result.err == nil {
			result.err = execCtx.Err()
		}
		if w.baseWorker.ctx.Err() != nil {
			result.reason = ReasonWorkerStop
		}
	}

	return result
}

type mockTTLSession struct {
	sessionVars       *variable.SessionVars
	sessionInfoSchema infoschema.InfoSchema
	executeSQL        func(ctx context.Context, sql string, args ...any) ([]chunk.Row, error)
}

func newMockTTLSession(
	is infoschema.InfoSchema,
	exec func(ctx context.Context, sql string, args ...any) ([]chunk.Row, error),
) *mockTTLSession {
	if is == nil {
		is = infoschema.MockInfoSchema(nil)
	}
	sessionVars := variable.NewSessionVars(nil)
	sessionVars.TimeZone = time.UTC
	return &mockTTLSession{
		sessionVars:       sessionVars,
		sessionInfoSchema: is,
		executeSQL:        exec,
	}
}

func (*mockTTLSession) GetStore() kv.Storage {
	return nil
}

func (s *mockTTLSession) GetLatestInfoSchema() infoschemacontext.MetaOnlyInfoSchema {
	return s.sessionInfoSchema
}

func (s *mockTTLSession) SessionInfoSchema() infoschemacontext.MetaOnlyInfoSchema {
	return s.sessionInfoSchema
}

func (s *mockTTLSession) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

func (*mockTTLSession) GetSQLExecutor() sqlexec.SQLExecutor {
	return nil
}

func (s *mockTTLSession) ExecuteSQL(ctx context.Context, sql string, args ...any) ([]chunk.Row, error) {
	if strings.HasPrefix(strings.ToUpper(sql), "SET ") {
		return nil, nil
	}
	if s.executeSQL != nil {
		return s.executeSQL(ctx, sql, args...)
	}
	return nil, nil
}

func (*mockTTLSession) RunInTxn(_ context.Context, fn func() error, _ session.TxnMode) error {
	return fn()
}

func (*mockTTLSession) ResetWithGlobalTimeZone(_ context.Context) error {
	return nil
}

func (*mockTTLSession) GlobalTimeZone(_ context.Context) (*time.Location, error) {
	return time.UTC, nil
}

func (*mockTTLSession) KillStmt() {}

func (s *mockTTLSession) Now() time.Time {
	loc := s.sessionVars.TimeZone
	if loc == nil {
		loc = time.UTC
	}
	return time.Now().In(loc)
}

func (*mockTTLSession) AvoidReuse() {}

var _ session.Session = (*mockTTLSession)(nil)
