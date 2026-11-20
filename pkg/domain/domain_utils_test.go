// Copyright 2021 PingCAP, Inc.
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

package domain

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestErrorCode(t *testing.T) {
	require.Equal(t, errno.ErrInfoSchemaExpired, int(terror.ToSQLError(ErrInfoSchemaExpired).Code))
	require.Equal(t, errno.ErrInfoSchemaChanged, int(terror.ToSQLError(ErrInfoSchemaChanged).Code))
}

func TestServerIDConstant(t *testing.T) {
	require.Less(t, lostConnectionToPDTimeout, serverIDTTL)
}

func TestIndexUsageSnapshotWorkerBoundedShutdown(t *testing.T) {
	stop := make(chan struct{})
	stopWorker := sync.OnceFunc(func() { close(stop) })
	t.Cleanup(stopWorker)
	release := make(chan struct{})
	releasePublisher := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releasePublisher)
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	publisherReturned := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		runIndexUsageSnapshots(stop, time.Millisecond, 10*time.Millisecond, func(ctx context.Context) error {
			defer close(publisherReturned)
			close(entered)
			<-ctx.Done()
			close(cancelled)
			// Model an in-flight filesystem operation that ignores cancellation.
			<-release
			return ctx.Err()
		})
	}()
	wait := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for snapshot worker")
		}
	}
	wait(entered)
	stopWorker()
	wait(cancelled)
	wait(done)
	releasePublisher()
	wait(publisherReturned)
}
