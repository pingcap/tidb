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

package ttlworker_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func waitAndStopTTLManager(t *testing.T, dom *domain.Domain) {
	maxWaitTime := 300
	for {
		maxWaitTime--
		if maxWaitTime < 0 {
			require.Fail(t, "fail to stop ttl manager")
		}
		if dom.TTLJobManager() != nil {
			dom.TTLJobManager().Stop()
			require.NoError(t, dom.TTLJobManager().WaitStopped(context.Background(), 10*time.Second))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type poolTestWrapper struct {
	syssession.Pool
	pool  syssession.Pool
	inuse atomic.Int64
}

func wrapPoolForTest(pool syssession.Pool) *poolTestWrapper {
	return &poolTestWrapper{pool: pool}
}

func (w *poolTestWrapper) WithSession(fn func(*syssession.Session) error) error {
	return w.pool.WithSession(func(s *syssession.Session) error {
		w.inuse.Add(1)
		defer w.inuse.Add(-1)
		return fn(s)
	})
}

func (w *poolTestWrapper) AssertNoSessionInUse(t *testing.T) {
	require.Zero(t, w.inuse.Load())
}
