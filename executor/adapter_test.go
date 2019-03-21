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

package executor

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pkg/errors"
)

var _ = Suite(&adapterTestSuite{})

type adapterTestSuite struct{}

type emptyExec struct {
	baseExecutor
	closeCnt int
	openCnt  int
	openErr  error
}

func (ee *emptyExec) Close() error {
	ee.closeCnt++
	return nil
}

func (ee *emptyExec) Open(ctx context.Context) error {
	ee.openCnt++
	return ee.openErr
}

func newEmptyExec() *emptyExec {
	sctx := mock.NewContext()
	return &emptyExec{newBaseExecutor(sctx, nil, ""), 0, 0, nil}
}

func (s *adapterTestSuite) TestCtxWatcher(c *C) {
	// Open and Close multiple times
	e := newEmptyExec()
	we := wrapCtxWatcher(e)
	ctx, cancel := context.WithCancel(context.Background())
	we.Open(ctx)
	we.Open(ctx)
	c.Assert(e.openCnt, Equals, 1)
	we.Close()
	we.Close()
	c.Assert(e.closeCnt, Equals, 1)

	// Open and Cancel
	e = newEmptyExec()
	we = wrapCtxWatcher(e)
	ctx, cancel = context.WithCancel(context.Background())
	we.Open(ctx)
	c.Assert(e.openCnt, Equals, 1)
	cancel()
	time.Sleep(time.Millisecond * 10)
	c.Assert(e.closeCnt, Equals, 1)
	we.Close()
	c.Assert(e.closeCnt, Equals, 1)

	// Open and Close anc Cancel
	e = newEmptyExec()
	we = wrapCtxWatcher(e)
	ctx, cancel = context.WithCancel(context.Background())
	we.Open(ctx)
	c.Assert(e.openCnt, Equals, 1)
	we.Close()
	c.Assert(e.closeCnt, Equals, 1)
	cancel()
	time.Sleep(time.Millisecond * 10)
	c.Assert(e.closeCnt, Equals, 1)

	// Open error
	e = newEmptyExec()
	we = wrapCtxWatcher(e)
	ctx, cancel = context.WithCancel(context.Background())
	e.openErr = errors.New("foo")
	we.Open(ctx)
	c.Assert(e.openCnt, Equals, 1)
	we.Close()
	c.Assert(e.closeCnt, Equals, 0)
	cancel()
	time.Sleep(time.Millisecond * 10)
	c.Assert(e.closeCnt, Equals, 0)

	// Close and Cancel multiple times
	e = newEmptyExec()
	we = wrapCtxWatcher(e)
	ctx, cancel = context.WithCancel(context.Background())
	we.Open(ctx)
	c.Assert(e.openCnt, Equals, 1)
	for i := 0; i < 10; i++ {
		we.Close()
		cancel()
	}
	time.Sleep(time.Millisecond * 10)
	c.Assert(e.closeCnt, Equals, 1)
}
