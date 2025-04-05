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
	"sync"
	"time"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type msg struct {
	span *AsyncSpan
	flow *Flow
	done chan struct{}
}

func parent(ctx context.Context, ch chan *msg) {
	span := StartSpan(ctx, "parent")
	defer span.Done()

	span1 := StartAsyncSpan(ctx, "async")
	m := msg{span: &span1, done: make(chan struct{})}
	ch <- &m

	<-m.done
	fmt.Println("get flow ==", m.flow)

	m.flow.Done(ctx)
	child(ctx)
}

func child(ctx context.Context) {
	span := StartSpan(ctx, "child")
	defer span.Done()
	time.Sleep(30 * time.Microsecond)

	return
}

func worker(ch chan *msg, wg *sync.WaitGroup) {
	ctx := SetTID(context.Background())
	for {
		msg, ok := <-ch
		if !ok {
			break
		}

		handleMsg(ctx, msg)
	}
	wg.Done()
}

func handleMsg(ctx context.Context, msg *msg) {
	ctx = WithTrace(ctx, msg.span.GetTrace())
	span := StartSpan(ctx, "handleMsg")
	flow := StartFlow(ctx, "flow->test")
	defer span.Done()

	time.Sleep(20 * time.Microsecond)

	msg.span.Done(ctx)

	msg.flow = &flow

	time.Sleep(10 * time.Microsecond)
	close(msg.done)
}

func TestTrace(t *testing.T) {
	ch := make(chan *msg)
	var wg sync.WaitGroup
	wg.Add(1)
	go worker(ch, &wg)

	ctx, task := NewTrace(context.Background(), nil)
	parent(ctx, ch)

	close(ch)
	wg.Wait()

	// events, err := Read(&buf)
	events := task.Close()

	res, err := json.Marshal(events)
	require.NoError(t, err)
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
	var events []Event
	ctx, t := NewTrace(context.Background(), events)
	recur(ctx, 0, 1)
	events = t.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, t := NewTrace(context.Background(), events[:0])
		recur(ctx, 0, 10)
		events = t.Close()
	}
}
