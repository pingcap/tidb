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
	"sync"

	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/resourcemanager/util"
)

type asyncChunk struct {
	res *demoChunk
}

type demoChunk struct {
	res int
}

type exampleAsyncOperator struct {
	BaseOperator
}

// Close implements AsyncOperator.
func (oi *exampleAsyncOperator) Close() {
	oi.Source.(*AsyncDataChannel[asyncChunk]).channel.ReleaseAndWait()
}

// Open implements AsyncOperator.
func (oi *exampleAsyncOperator) Open() error {
	oi.Source.(*AsyncDataChannel[asyncChunk]).channel.SetCreateWorker(
		func() workerpool.Worker[asyncChunk] {
			return &asyncWorker{oi.Sink}
		},
	)
	oi.Source.(*AsyncDataChannel[asyncChunk]).channel.Start()
	return nil
}

// Display implements AsyncOperator.
func (oi *exampleAsyncOperator) Display() string {
	return "ExampleAsyncOperator{ source: " + oi.Source.Display() + ", sink: " + oi.Sink.Display() + "}"
}

func newExampleAsyncOperator(name string,
	component poolutil.Component,
	concurrency int,
	sink DataSink) *exampleAsyncOperator {
	pool, _ := workerpool.NewWorkerPoolWithoutCreateWorker[asyncChunk](name, component, concurrency)
	source := &AsyncDataChannel[asyncChunk]{channel: pool}
	impl := &exampleAsyncOperator{}
	impl.Source = source
	impl.Sink = sink
	return impl
}

type asyncWorker struct {
	sink DataSink
}

// HandleTask define the basic running process for each operator.
func (aw *asyncWorker) HandleTask(task asyncChunk) {
	task.res.res++
	_ = aw.sink.Write(task)
}

// Close implement the Close interface for workerpool.
func (*asyncWorker) Close() {}

type simpleAsyncDataSink struct {
	Res int
	mu  sync.Mutex
}

// Write data to sink.
func (sas *simpleAsyncDataSink) Write(data any) error {
	sas.mu.Lock()
	defer sas.mu.Unlock()
	innerVal := data.(asyncChunk)
	sas.Res += innerVal.res.res
	return nil
}

// Read data from source.
func (*simpleAsyncDataSink) Next() (any, error) {
	return nil, nil
}

// Display show the DataSink.
func (*simpleAsyncDataSink) Display() string {
	return "simpleAsyncDataSink"
}
