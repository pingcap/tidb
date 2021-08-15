// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package worker_test

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
)

type testWorkerPool struct{}

func (s *testWorkerPool) SetUpSuite(c *C)    {}
func (s *testWorkerPool) TearDownSuite(c *C) {}

var _ = Suite(&testWorkerPool{})

func TestNewRestoreWorkerPool(t *testing.T) {
	TestingT(t)
}

func (s *testWorkerPool) TestApplyRecycle(c *C) {
	pool := worker.NewPool(context.Background(), 3, "test")

	w1, w2, w3 := pool.Apply(), pool.Apply(), pool.Apply()
	c.Assert(w1.ID, Equals, int64(1))
	c.Assert(w2.ID, Equals, int64(2))
	c.Assert(w3.ID, Equals, int64(3))
	c.Assert(pool.HasWorker(), Equals, false)

	pool.Recycle(w3)
	c.Assert(pool.HasWorker(), Equals, true)
	c.Assert(pool.Apply(), Equals, w3)
	pool.Recycle(w2)
	c.Assert(pool.Apply(), Equals, w2)
	pool.Recycle(w1)
	c.Assert(pool.Apply(), Equals, w1)

	c.Assert(pool.HasWorker(), Equals, false)

	c.Assert(func() { pool.Recycle(nil) }, PanicMatches, "invalid restore worker")
}
