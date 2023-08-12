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
	"testing"

	"github.com/stretchr/testify/require"
)

func NewAsyncPipeline() (*AsyncPipeline, any) {
	impl0 := newExampleAsyncOperatorImpl("impl0")
	impl1 := newExampleAsyncOperatorImpl("impl1")
	impl0.Sink = impl1.Source.(DataSink)
	sink := &simpleAsyncDataSink{0, 0, sync.Mutex{}}

	impl1.Sink = sink
	pipeline := &AsyncPipeline{}
	pipeline.AddOperator(impl0)
	pipeline.AddOperator(impl1)
	return pipeline, impl0.Source
}

func TestPipelineAsync(t *testing.T) {
	pipeline, source := NewAsyncPipeline()
	pipeline.Execute()
	for i := 0; i < 10; i++ {
		_ = source.(*AsyncDataChannel[asyncChunk]).Write(asyncChunk{&demoChunk{0}})
	}
	pipeline.Close()
	require.Equal(t, 20, pipeline.LastOperator().(*exampleAsyncOperatorImpl).Sink.(*simpleAsyncDataSink).Res)
}
