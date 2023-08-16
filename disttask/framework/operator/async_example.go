// Copyright 2023 PingCAP, Inc.
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

package operator

import (
	"errors"
	"sync"

	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util/logutil"
)

type asyncChunk struct {
	res *demoChunk
}

type demoChunk struct {
	res int
}

type exampleAsyncOperator struct {
	BaseOperator[asyncChunk, asyncChunk]
}

// Close implements AsyncOperator.
func (oi *exampleAsyncOperator) Close() {
	oi.Source.(*AsyncDataChannel[asyncChunk]).Channel.ReleaseAndWait()
}

// Open implements AsyncOperator.
func (oi *exampleAsyncOperator) Open() error {
	oi.Source.(*AsyncDataChannel[asyncChunk]).Channel.SetCreateWorker(
		func() workerpool.Worker[asyncChunk] {
			return &asyncWorker{oi.Sink}
		},
	)
	oi.Source.(*AsyncDataChannel[asyncChunk]).Channel.Start()
	return nil
}

// Display implements AsyncOperator.
func (oi *exampleAsyncOperator) Display() string {
	return "ExampleAsyncOperator{ source: " + oi.Source.Display() + ", sink: " + oi.Sink.Display() + "}"
}

func newExampleAsyncOperator(name string,
	component poolutil.Component,
	concurrency int,
	sink DataSink[asyncChunk]) *exampleAsyncOperator {
	pool, _ := workerpool.NewWorkerPoolWithoutCreateWorker[asyncChunk](name, component, concurrency)
	source := &AsyncDataChannel[asyncChunk]{Channel: pool}
	op := &exampleAsyncOperator{}
	op.Source = source
	op.Sink = sink
	return op
}

type asyncWorker struct {
	sink DataSink[asyncChunk]
}

// HandleTask define the basic running process for each operator.
func (aw *asyncWorker) HandleTask(task asyncChunk) {
	task.res.res++
	_ = aw.sink.Write(task)
}

// Close implement the Close interface for workerpool.
func (*asyncWorker) Close() {}

type simpleDataSink struct {
	Res int
	mu  sync.Mutex
}

// Write data to sink.
func (s *simpleDataSink) Write(data asyncChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	innerVal := data
	s.Res += innerVal.res.res
	return nil
}

// Read data from source.
func (*simpleDataSink) Next() (any, error) {
	return nil, nil
}

// Display show the DataSink.
func (*simpleDataSink) Display() string {
	return "simpleDataSink"
}

type simpleDataSource struct {
	cnt int
	mu  sync.Mutex
}

// Start the source.
func (*simpleDataSource) Start() error {
	logutil.BgLogger().Info("simpleDataSource start")
	return nil
}

// Next read data.
func (s *simpleDataSource) Next() (asyncChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cnt < 10 {
		s.cnt++
		return asyncChunk{&demoChunk{0}}, nil
	}
	return asyncChunk{}, errors.New("eof")
}

// Display show the DataSource.
func (*simpleDataSource) Display() string {
	return "simpleDataSource"
}

type exampleSourceOperator struct {
	BaseOperator[asyncChunk, asyncChunk]
	pool *workerpool.WorkerPool[bool]
}

// Close implements AsyncOperator.
func (oi *exampleSourceOperator) Close() {
	oi.pool.ReleaseAndWait()
}

// Open implements AsyncOperator.
func (oi *exampleSourceOperator) Open() error {
	err := oi.Source.Start()
	if err != nil {
		return err
	}
	oi.pool.SetCreateWorker(
		func() workerpool.Worker[bool] {
			return &asyncWorker1{oi.Source, oi.Sink}
		},
	)
	oi.pool.Start()
	for i := 0; i < 10; i++ {
		oi.pool.AddTask(true)
	}
	return nil
}

// Display implements AsyncOperator.
func (oi *exampleSourceOperator) Display() string {
	return "ExampleSourceOperator{ source: " + oi.Source.Display() + ", sink: " + oi.Sink.Display() + "}"
}

func newExampleSourceOperator(name string,
	component poolutil.Component,
	concurrency int,
	sink DataSink[asyncChunk]) *exampleSourceOperator {
	pool, _ := workerpool.NewWorkerPoolWithoutCreateWorker[bool](name, component, concurrency)
	source := &simpleDataSource{}
	op := &exampleSourceOperator{}
	op.Source = source
	op.Sink = sink
	op.pool = pool
	return op
}

type asyncWorker1 struct {
	source DataSource[asyncChunk]
	sink   DataSink[asyncChunk]
}

// HandleTask define the basic running process for each operator.
func (aw *asyncWorker1) HandleTask(bool) {
	for {
		asyncChunk, err := aw.source.Next()
		if err != nil {
			return
		}
		asyncChunk.res.res++
		_ = aw.sink.Write(asyncChunk)
	}
}

// Close implement the Close interface for workerpool.
func (*asyncWorker1) Close() {}
