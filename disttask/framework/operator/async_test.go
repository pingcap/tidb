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

	poolutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/stretchr/testify/require"
)

func NewAsyncPipeline() (*AsyncPipeline, any) {
	sink := &simpleAsyncDataSink{0, sync.Mutex{}}
	op1 := newExampleAsyncOperator("impl1", poolutil.DDL, 10, sink)
	op0 := newExampleAsyncOperator("impl0", poolutil.DDL, 10, op1.Source.(DataSink))
	pipeline := &AsyncPipeline{}
	pipeline.AddOperator(op0)
	pipeline.AddOperator(op1)
	return pipeline, op0.Source
}

func TestPipelineAsync(t *testing.T) {
	pipeline, source := NewAsyncPipeline()
	err := pipeline.Execute()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_ = source.(*AsyncDataChannel[asyncChunk]).Write(asyncChunk{&demoChunk{0}})
	}
	pipeline.Close()
	require.Equal(t, "ExampleAsyncOperator{ source: AsyncDataChannel, sink: AsyncDataChannel}\n ExampleAsyncOperator{ source: AsyncDataChannel, sink: simpleAsyncDataSink}", pipeline.Display())
	require.Equal(t, 20, pipeline.LastOperator().(*exampleAsyncOperator).Sink.(*simpleAsyncDataSink).Res)
}
