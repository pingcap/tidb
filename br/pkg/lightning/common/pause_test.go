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

package common_test

import (
	"context"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
)

// unblocksAfter is a checker which ensures the WaitGroup's Wait() method
// returns between the given durations.
var unblocksBetween Checker = &unblocksChecker{
	&CheckerInfo{Name: "unblocksBetween", Params: []string{"waitGroupPtr", "min", "max"}},
}

type unblocksChecker struct {
	*CheckerInfo
}

func (checker *unblocksChecker) Check(params []interface{}, names []string) (bool, string) {
	wg := params[0].(*sync.WaitGroup)
	min := params[1].(time.Duration)
	max := params[2].(time.Duration)

	ch := make(chan time.Duration)
	start := time.Now()
	go func() {
		wg.Wait()
		ch <- time.Since(start)
	}()
	select {
	case dur := <-ch:
		if dur < min {
			return false, "WaitGroup unblocked before minimum duration, it was " + dur.String()
		}
		return true, ""
	case <-time.After(max):
		select {
		case dur := <-ch:
			return false, "WaitGroup did not unblock after maximum duration, it was " + dur.String()
		case <-time.After(1 * time.Second):
			return false, "WaitGroup did not unblock after maximum duration"
		}
	}
}

var _ = Suite(&pauseSuite{})

type pauseSuite struct{}

func (s *pauseSuite) TestPause(c *C) {
	var wg sync.WaitGroup
	p := common.NewPauser()

	// initially these calls should not be blocking.

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			err := p.Wait(context.Background())
			c.Assert(err, IsNil)
		}()
	}

	// Give them more time to unblock in case of time exceeding due to high pressure of CI.
	c.Assert(&wg, unblocksBetween, 0*time.Millisecond, 100*time.Millisecond)

	// after calling Pause(), these should be blocking...

	p.Pause()

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			err := p.Wait(context.Background())
			c.Assert(err, IsNil)
		}()
	}

	// ... until we call Resume()

	go func() {
		time.Sleep(500 * time.Millisecond)
		p.Resume()
	}()

	// Give them more time to unblock in case of time exceeding due to high pressure of CI.
	c.Assert(&wg, unblocksBetween, 500*time.Millisecond, 800*time.Millisecond)

	// if the context is canceled, Wait() should immediately unblock...

	ctx, cancel := context.WithCancel(context.Background())

	p.Pause()

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			err := p.Wait(ctx)
			c.Assert(err, Equals, context.Canceled)
		}()
	}

	cancel()
	// Give them more time to unblock in case of time exceeding due to high pressure of CI.
	c.Assert(&wg, unblocksBetween, 0*time.Millisecond, 100*time.Millisecond)

	// canceling the context does not affect the state of the pauser

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := p.Wait(context.Background())
		c.Assert(err, IsNil)
	}()

	go func() {
		time.Sleep(500 * time.Millisecond)
		p.Resume()
	}()

	// Give them more time to unblock in case of time exceeding due to high pressure of CI.
	c.Assert(&wg, unblocksBetween, 500*time.Millisecond, 800*time.Millisecond)
}

// Run `go test github.com/pingcap/tidb/br/pkg/lightning/common -check.b -test.v` to get benchmark result.
func (s *pauseSuite) BenchmarkWaitNoOp(c *C) {
	p := common.NewPauser()
	ctx := context.Background()
	for i := 0; i < c.N; i++ {
		_ = p.Wait(ctx)
	}
}

func (s *pauseSuite) BenchmarkWaitCtxCanceled(c *C) {
	p := common.NewPauser()
	p.Pause()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < c.N; i++ {
		_ = p.Wait(ctx)
	}
}

func (s *pauseSuite) BenchmarkWaitContended(c *C) {
	p := common.NewPauser()

	done := make(chan struct{})
	defer close(done)
	go func() {
		isPaused := false
		for {
			select {
			case <-done:
				return
			default:
				if isPaused {
					p.Pause()
				} else {
					p.Resume()
				}
				isPaused = !isPaused
			}
		}
	}()

	ctx := context.Background()
	for i := 0; i < c.N; i++ {
		_ = p.Wait(ctx)
	}
}
