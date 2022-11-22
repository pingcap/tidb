// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttl

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
)

type workerStatus int

const (
	workerStatusCreated workerStatus = iota
	workerStatusRunning
	workerStatusStopping
	workerStatusStopped
)

type session struct {
	pool sessionPool
	sessionctx.Context
	sqlexec.SQLExecutor
}

func getWorkerSession(pool sessionPool) (*session, error) {
	resource, err := pool.Get()
	if err != nil {
		return nil, err
	}

	sctx, ok := resource.(sessionctx.Context)
	if !ok {
		return nil, errors.Errorf("%T cannot be casted to sessionctx.Context", sctx)
	}

	exec, ok := resource.(sqlexec.SQLExecutor)
	if !ok {
		return nil, errors.Errorf("%T cannot be casted to sqlexec.SQLExecutor", sctx)
	}

	se := &session{
		pool:        pool,
		Context:     sctx,
		SQLExecutor: exec,
	}

	if _, err = executeSQL(context.Background(), se, "commit"); err != nil {
		se.Close()
		return nil, err
	}

	if _, err = executeSQL(context.Background(), se, "set @@time_zone=(select @@global.time_zone)"); err != nil {
		se.Close()
		return nil, err
	}

	return se, nil
}

func (s *session) Close() {
	s.pool.Put(s)
	s.Context = nil
	s.SQLExecutor = nil
}

type worker interface {
	Start()
	Stop()
	Status() workerStatus
	Error() error
	Send() chan<- interface{}
}

type baseWorker struct {
	sync.Mutex
	ctx      context.Context
	cancel   func()
	ch       chan interface{}
	loopFunc func() error

	err    error
	status workerStatus
}

func (w *baseWorker) init(loop func() error) {
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.status = workerStatusCreated
	w.loopFunc = loop
	w.ch = make(chan interface{})
}

func (w *baseWorker) Start() {
	w.Lock()
	defer w.Unlock()
	if w.status != workerStatusCreated {
		return
	}

	go w.loop()
	w.status = workerStatusRunning
}

func (w *baseWorker) Stop() {
	w.Lock()
	defer w.Unlock()
	switch w.status {
	case workerStatusCreated:
		w.cancel()
		w.toStopped(nil)
	case workerStatusRunning:
		w.cancel()
		w.status = workerStatusStopping
	}
}

func (w *baseWorker) Status() workerStatus {
	w.Lock()
	defer w.Unlock()
	return w.status
}

func (w *baseWorker) Error() error {
	w.Lock()
	defer w.Unlock()
	return w.err
}

func (w *baseWorker) Send() chan<- interface{} {
	return w.ch
}

func (w *baseWorker) loop() {
	var err error
	defer func() {
		w.Lock()
		defer w.Unlock()
		w.toStopped(err)
	}()
	err = w.loopFunc()
}

func (w *baseWorker) toStopped(err error) {
	w.status = workerStatusStopped
	w.err = err
	close(w.ch)
}
