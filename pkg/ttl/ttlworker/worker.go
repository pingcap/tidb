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

package ttlworker

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type workerStatus int

const (
	workerStatusCreated workerStatus = iota
	workerStatusRunning
	workerStatusStopping
	workerStatusStopped
)

type worker interface {
	Start()
	Stop()
	Status() workerStatus
	Error() error
	Send() chan<- any
	WaitStopped(ctx context.Context, timeout time.Duration) error
}

type baseWorker struct {
	sync.Mutex
	ctx      context.Context
	cancel   func()
	ch       chan any
	loopFunc func() error

	err    error
	status workerStatus
	wg     util.WaitGroupWrapper
}

func (w *baseWorker) init(loop func() error) {
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.status = workerStatusCreated
	w.loopFunc = loop
	w.ch = make(chan any)
}

func (w *baseWorker) Start() {
	w.Lock()
	defer w.Unlock()
	if w.status != workerStatusCreated {
		return
	}

	w.wg.Run(w.loop)
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

func (w *baseWorker) WaitStopped(ctx context.Context, timeout time.Duration) error {
	// consider the situation when the worker has stopped, but the context has also stopped. We should
	// return without error
	if w.Status() == workerStatusStopped {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	go func() {
		w.wg.Wait()
		cancel()
	}()

	<-ctx.Done()
	if w.Status() != workerStatusStopped {
		return ctx.Err()
	}
	return nil
}

func (w *baseWorker) Send() chan<- any {
	return w.ch
}

func (w *baseWorker) loop() {
	var err error
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Info("ttl worker panic", zap.Any("recover", r), zap.Stack("stack"))
		}
		w.Lock()
		w.toStopped(err)
		w.Unlock()
	}()
	err = w.loopFunc()
}

func (w *baseWorker) toStopped(err error) {
	w.status = workerStatusStopped
	w.err = err
	close(w.ch)
}
