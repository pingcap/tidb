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
)

type asyncChunk struct {
	res *demoChunk
}

type demoChunk struct {
	res int
}

type asyncWorker struct {
	sink DataSink
}

// HandleTask define the basic running process for each operator.
func (aw *asyncWorker) HandleTask(task asyncChunk) {
	task.res.res++
	for aw.sink.IsFull() {
		continue
	}
	_ = aw.sink.Write(task)
}

// Close implement the Close interface for workerpool.
func (*asyncWorker) Close() {}

type exampleAsyncOperatorImpl struct {
	AsyncOperator[asyncChunk]
}

func newExampleAsyncOperatorImpl() AsyncOperatorImpl {
	res := &exampleAsyncOperatorImpl{}
	return res
}

func (*exampleAsyncOperatorImpl) preExecute() error {
	return nil
}

func (oi *exampleAsyncOperatorImpl) start() {
	oi.pool.SetCreateWorker(oi.createWorker)
	oi.pool.Start()
}

func (oi *exampleAsyncOperatorImpl) wait() {
	oi.pool.ReleaseAndWait()
}

func (oi *exampleAsyncOperatorImpl) createWorker() workerpool.Worker[asyncChunk] {
	return &asyncWorker{oi.sink}
}

func (*exampleAsyncOperatorImpl) postExecute() error {
	return nil
}

func (*exampleAsyncOperatorImpl) display() string {
	return "ExampleAsyncOperator"
}

func (oi *exampleAsyncOperatorImpl) addTask(data any) {
	oi.pool.AddTask(data.(asyncChunk))
}

type simpleAsyncDataSink struct {
	Res int
	cnt int
	mu  sync.Mutex
}

// IsFull check if it is full.
func (*simpleAsyncDataSink) IsFull() bool {
	return false
}

// Write data to sink.
func (sas *simpleAsyncDataSink) Write(data any) error {
	sas.mu.Lock()
	defer sas.mu.Unlock()
	innerVal := data.(asyncChunk)
	sas.Res += innerVal.res.res
	sas.cnt++
	return nil
}

// HasNext check if it has next data.
func (*simpleAsyncDataSink) HasNext() bool {
	return true
}

// Read data from source.
func (sas *simpleAsyncDataSink) Read() (any, error) {
	sas.mu.Lock()
	defer sas.mu.Unlock()
	if sas.cnt > 0 {
		sas.cnt--
		return asyncChunk{&demoChunk{3}}, nil
	}
	return nil, nil
}

// Display show the name.
func (*simpleAsyncDataSink) Display() string {
	return "simpleAsyncDataSink"
}
