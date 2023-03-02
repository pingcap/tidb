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

package spool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/resourcemanager/gpool"
	"github.com/pingcap/tidb/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func demoFunc() {
	f(2)
}

func f(n int) {
	if n == 0 {
		return
	}
	var useStack [100]byte
	_ = useStack[3]
	f(n - 1)
}

func TestSpool(t *testing.T) {
	var wg sync.WaitGroup
	p, err := NewPool("test", 1, util.UNKNOWN, WithBlocking(true))
	require.NoError(t, err)
	require.NoError(t, err)
	defer p.ReleaseAndWait()

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		_ = p.Run(func() {
			wg.Done()
		})
	}
	wg.Wait()
}

func TestReleaseWhenRunningPool(t *testing.T) {
	var wg sync.WaitGroup
	p, err := NewPool("TestReleaseWhenRunningPool", 1, util.UNKNOWN)
	require.NoError(t, err)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			_ = p.Run(func() {
				time.Sleep(1 * time.Second)
			})
		}
	}()

	go func() {
		defer wg.Done()
		for i := 100; i < 130; i++ {
			_ = p.Run(func() {
				time.Sleep(1 * time.Second)
			})
		}
	}()
	time.Sleep(3 * time.Second)
	p.ReleaseAndWait()
	wg.Wait()
}

func TestPoolTuneScaleUpAndDown(t *testing.T) {
	c := make(chan struct{})
	p, _ := NewPool("TestPoolTuneScaleUp", 2, util.UNKNOWN)
	for i := 0; i < 2; i++ {
		_ = p.Run(func() {
			<-c
		})
	}
	if n := p.Running(); n != 2 {
		t.Errorf("expect 2 workers running, but got %d", n)
	}
	// test pool tune scale up one
	p.Tune(3)
	_ = p.Run(func() {
		<-c
	})
	if n := p.Running(); n != 3 {
		t.Errorf("expect 3 workers running, but got %d", n)
	}
	// test pool tune scale up multiple
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.Run(func() {
				<-c
			})
		}()
	}
	p.Tune(8)
	time.Sleep(500 * time.Millisecond)
	if n := p.Running(); n != 8 {
		t.Errorf("expect 8 workers running, but got %d", n)
	}
	for i := 0; i < 8; i++ {
		c <- struct{}{}
	}
	wg.Wait()
	p.ReleaseAndWait()
}

func TestRunOverload(t *testing.T) {
	var stop atomic.Bool
	longRunningFunc := func() {
		for {
			if stop.Load() {
				break
			}
			runtime.Gosched()
		}
	}
	poolSize := 10
	p, err := NewPool("TestMaxBlockingSubmit", int32(poolSize), util.UNKNOWN, WithBlocking(false))
	require.NoErrorf(t, err, "create TimingPool failed: %v", err)
	defer p.ReleaseAndWait()
	defer stop.Store(true)
	for i := 0; i < poolSize-1; i++ {
		require.NoError(t, p.Run(longRunningFunc), "submit when pool is not full shouldn't return error")
	}
	// p is full now.
	require.NoError(t, p.Run(longRunningFunc), "submit when pool is not full shouldn't return error")
	require.EqualError(t, p.Run(demoFunc), gpool.ErrPoolOverload.Error(),
		"blocking submit when pool reach max blocking submit should return ErrPoolOverload")
}

func TestRunWithNotEnough(t *testing.T) {
	var stop atomic.Bool
	longRunningFunc := func() {
		for {
			if stop.Load() {
				break
			}
			runtime.Gosched()
		}
	}
	poolSize := 10
	p, err := NewPool("TestRunWithNotEnough", int32(poolSize), util.UNKNOWN, WithBlocking(false))
	require.NoErrorf(t, err, "create TimingPool failed: %v", err)
	defer p.ReleaseAndWait()
	defer stop.Store(true)
	require.NoError(t, p.RunWithConcurrency(longRunningFunc, poolSize+100), "submit when pool is not full shouldn't return error")
	require.Equal(t, 10, p.Running())
	require.Error(t, p.RunWithConcurrency(longRunningFunc, 1))
	require.Error(t, p.Run(longRunningFunc))
}

func TestRunWithNotEnough2(t *testing.T) {
	var stop atomic.Bool
	longRunningFunc := func() {
		for {
			if stop.Load() {
				break
			}
			runtime.Gosched()
		}
	}
	p, err := NewPool("TestRunWithNotEnough2", int32(1), util.UNKNOWN, WithBlocking(false))
	require.NoErrorf(t, err, "create TimingPool failed: %v", err)
	defer p.ReleaseAndWait()
	defer stop.Store(true)
	require.NoError(t, p.RunWithConcurrency(longRunningFunc, 2), "submit when pool is not full shouldn't return error")
	require.Equal(t, 1, p.Running())
	require.Error(t, p.RunWithConcurrency(longRunningFunc, 1))
	require.Error(t, p.Run(longRunningFunc))
}

func TestExitTask(t *testing.T) {
	exit := make(chan struct{})
	longRunningFunc := func() {
		<-exit
	}
	p, err := NewPool("TestExitTask", int32(10), util.UNKNOWN, WithBlocking(true))
	require.NoErrorf(t, err, "create TimingPool failed: %v", err)
	defer p.ReleaseAndWait()
	p.RunWithConcurrency(longRunningFunc, 10)
	for i := 0; i < 10; i++ {
		exit <- struct{}{}
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, 10-i-1, p.Running())
	}
	p.RunWithConcurrency(longRunningFunc, 10)
	var wg tidbutil.WaitGroupWrapper
	wg.Run(func() {
		p.Run(longRunningFunc)
	})
	for i := 0; i < 10; i++ {
		exit <- struct{}{}
	}
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, 1, p.Running())
	exit <- struct{}{}
}
