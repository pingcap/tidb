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
	impl0 := NewAsyncOperatorImpl[asyncChunk]("impl0", newExampleAsyncOperatorImpl, poolutil.DDL, 10)
	impl1 := NewAsyncOperatorImpl[asyncChunk]("impl1", newExampleAsyncOperatorImpl, poolutil.DDL, 10)
	impl2 := NewAsyncOperatorImpl[asyncChunk]("impl2", newExampleAsyncOperatorImpl, poolutil.DDL, 10)
	source0 := impl0.getSource()
	source1 := impl1.getSource()
	source2 := impl2.getSource()
	sink := &simpleAsyncDataSink{0, 0, sync.Mutex{}}

	impl0.setSink(source1.(DataSink))
	impl1.setSink(source2.(DataSink))
	impl2.setSink(sink)

	pipeline := &AsyncPipeline{}
	pipeline.AddOperator(impl0)
	pipeline.AddOperator(impl1)
	pipeline.AddOperator(impl2)
	return pipeline, source0
}

func TestPipelineAsync(t *testing.T) {
	pipeline, source := NewAsyncPipeline()
	pipeline.AsyncExecute()
	for i := 0; i < 10; i++ {
		_ = source.(*AsyncDataChannel[asyncChunk]).Write(asyncChunk{&demoChunk{0}})
	}
	pipeline.Wait()
	require.Equal(t, 30, pipeline.LastOperator().getSink().(*simpleAsyncDataSink).Res)
}
