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

package exec

import (
	"context"
	"sync"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

type nextTraceNameExecutor struct {
	BaseExecutorV2
}

func TestNextTraceName(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	v2 := NewBaseExecutorV2(vars, nil, 0)
	embedded := nextTraceNameExecutor{BaseExecutorV2: NewBaseExecutorV2(vars, nil, 0)}

	require.Equal(t, "*exec.BaseExecutorV2.Next", nextTraceName(&v2))
	require.Equal(t, "*exec.nextTraceNameExecutor.Next", nextTraceName(&embedded))

	const concurrency = 32
	names := make(chan string, concurrency)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			names <- nextTraceName(&v2)
		}()
	}
	wg.Wait()
	close(names)
	for name := range names {
		require.Equal(t, "*exec.BaseExecutorV2.Next", name)
	}
}

func TestNextTraceNameWithOpenTracing(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	executor := nextTraceNameExecutor{BaseExecutorV2: NewBaseExecutorV2(vars, nil, 0)}
	tracer := mocktracer.New()
	parent := tracer.StartSpan("parent")
	ctx := opentracing.ContextWithSpan(context.Background(), parent)

	require.NoError(t, Next(ctx, &executor, chunk.NewChunkWithCapacity(nil, 0)))
	parent.Finish()

	spans := tracer.FinishedSpans()
	require.Len(t, spans, 2)
	require.Equal(t, "*exec.nextTraceNameExecutor.Next", spans[0].OperationName)
}

func BenchmarkNextWrapper(b *testing.B) {
	executor := NewBaseExecutorV2(variable.NewSessionVars(nil), nil, 0)
	req := chunk.NewChunkWithCapacity(nil, 0)
	ctx := context.Background()
	if err := Next(ctx, &executor, req); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := Next(ctx, &executor, req); err != nil {
			b.Fatal(err)
		}
	}
}
