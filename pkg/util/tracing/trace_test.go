// Copyright 2025 PingCAP, Inc.
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

package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type msg struct {
	span Span
	done chan struct{}
}

func parent(ctx context.Context) {
	span := StartSpan(ctx, "parent")
	defer span.Done()
	s := child(ctx)

	ch := make(chan msg)
	ctx = StartGoroutine(ctx)
	go spawn(ctx, ch)

	m := msg{span: s, done: make(chan struct{})}
	ch <- m

	<-m.done

	close(ch)
}

func child(ctx context.Context) Span {
	span := StartSpan(ctx, "child")
	defer span.Done()
	return span
}

func spawn(ctx context.Context, ch chan msg) {
	span := StartSpan(ctx, "span")
	defer span.Done()

	for {
		msg, ok := <-ch
		if !ok {
			return
		}

		span1 := StartSpan(ctx, "Handle")
		Flow(ctx, &msg.span, &span1)
		close(msg.done)
		span1.Done()
	}
}

func TestTrace(t *testing.T) {
	var buf bytes.Buffer
	ctx,_ := NewTask(context.Background(), &buf)
	parent(ctx)

	events, err := Read(&buf)
	require.NoError(t, err)

	res, err := json.Marshal(events)
	fmt.Println(string(res))
}

func recur(ctx context.Context, i int, n int) {
	span := StartSpan(ctx, "recur")
	defer span.Done()

	if i >= n {
		return
	}
	recur(ctx, i+1, n)
}

func BenchmarkTrace(b *testing.B) {
	var buf bytes.Buffer
	ctx, _ := NewTask(context.Background(), &buf)
	recur(ctx, 0, 1)
	buf.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, _ := NewTask(context.Background(), &buf)
		recur(ctx, 0, 10)
		buf.Reset()
	}
}
