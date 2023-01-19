// Copyright 2022 PingCAP, Inc.
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

package spmc

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/resourcemanager/pooltask"
	rmutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/gpool"
	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	type ConstArgs struct {
		a int
	}
	myArgs := ConstArgs{a: 10}
	// init the pool
	// input type， output type, constArgs type
	pool, err := NewSPMCPool[int, int, ConstArgs, any, pooltask.NilContext]("TestPool", 10, rmutil.UNKNOWN)
	require.NoError(t, err)
	pool.SetConsumerFunc(func(task int, constArgs ConstArgs, ctx any) int {
		return task + constArgs.a
	})
	taskCh := make(chan int, 10)
	for i := 1; i < 11; i++ {
		taskCh <- i
	}
	pfunc := func() (int, error) {
		select {
		case task := <-taskCh:
			return task, nil
		default:
			return 0, gpool.ErrProducerClosed
		}
	}
	// add new task
	resultCh, control := pool.AddProducer(pfunc, myArgs, pooltask.NilContext{}, WithConcurrency(4))

	var count atomic.Uint32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range resultCh {
			count.Add(1)
			require.Greater(t, result, 10)
		}
	}()
	// Waiting task finishing
	control.Wait()
	wg.Wait()
	require.Equal(t, uint32(10), count.Load())
	// close pool
	pool.ReleaseAndWait()

	// test renew is normal
	pool, err = NewSPMCPool[int, int, ConstArgs, any, pooltask.NilContext]("TestPool", 10, rmutil.UNKNOWN)
	require.NoError(t, err)
	pool.ReleaseAndWait()
}

func TestStopPool(t *testing.T) {
	type ConstArgs struct {
		a int
	}
	myArgs := ConstArgs{a: 10}
	// init the pool
	// input type， output type, constArgs type
	pool, err := NewSPMCPool[int, int, ConstArgs, any, pooltask.NilContext]("TestPool", 10, rmutil.UNKNOWN)
	require.NoError(t, err)
	pool.SetConsumerFunc(func(task int, constArgs ConstArgs, ctx any) int {
		return task + constArgs.a
	})

	exit := make(chan struct{})

	pfunc := func() (int, error) {
		select {
		case <-exit:
			return 0, gpool.ErrProducerClosed
		default:
			return 1, nil
		}
	}
	// add new task
	resultCh, control := pool.AddProducer(pfunc, myArgs, pooltask.NilContext{}, WithConcurrency(4))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range resultCh {
			require.Greater(t, result, 10)
		}
	}()
	// Waiting task finishing
	control.Stop()
	close(exit)
	control.Wait()
	// it should pass. Stop can be used after the pool is closed. we should prevent it from panic.
	control.Stop()
	wg.Wait()
	// close pool
	pool.ReleaseAndWait()
}

func TestTuneSimplePool(t *testing.T) {
	testTunePool(t, "TestTuneSimplePool")
}

func TestTuneMultiPool(t *testing.T) {
	var concurrency = 5
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			testTunePool(t, "TestTuneSimplePool"+string(1))
			wg.Done()
		}()
	}
	wg.Wait()
}

func testTunePool(t *testing.T, name string) {
	type ConstArgs struct {
		a int
	}
	myArgs := ConstArgs{a: 10}
	// init the pool
	// input type， output type, constArgs type
	pool, err := NewSPMCPool[int, int, ConstArgs, any, pooltask.NilContext](name, 10, rmutil.UNKNOWN)
	require.NoError(t, err)
	pool.SetConsumerFunc(func(task int, constArgs ConstArgs, ctx any) int {
		return task + constArgs.a
	})

	exit := make(chan struct{})

	pfunc := func() (int, error) {
		select {
		case <-exit:
			return 0, gpool.ErrProducerClosed
		default:
			return 1, nil
		}
	}
	// add new task
	resultCh, control := pool.AddProducer(pfunc, myArgs, pooltask.NilContext{}, WithConcurrency(10))
	tid := control.TaskID()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range resultCh {
			require.Greater(t, result, 10)
		}
	}()
	time.Sleep(1 * time.Second)
	newSize := pool.Cap() - 1
	pool.Tune(newSize)
	time.Sleep(1 * time.Second)
	require.Equal(t, newSize, pool.Cap())
	require.Equal(t, int32(newSize), pool.taskManager.Running(tid))

	newSize = pool.Cap() + 1
	pool.Tune(newSize)
	time.Sleep(1 * time.Second)
	require.Equal(t, newSize, pool.Cap())
	require.Equal(t, int32(newSize), pool.taskManager.Running(tid))

	// exit test
	close(exit)
	control.Wait()
	wg.Wait()
	// close pool
	pool.ReleaseAndWait()
}

func TestPoolWithEnoughCapacity(t *testing.T) {
	const (
		RunTimes    = 1000
		poolsize    = 30
		concurrency = 6
	)
	p, err := NewSPMCPool[struct{}, struct{}, int, any, pooltask.NilContext]("TestPoolWithEnoughCapa", poolsize, rmutil.UNKNOWN, WithExpiryDuration(DefaultExpiredTime))
	require.NoError(t, err)
	defer p.ReleaseAndWait()
	p.SetConsumerFunc(func(a struct{}, b int, c any) struct{} {
		return struct{}{}
	})
	var twg util.WaitGroupWrapper
	for i := 0; i < 3; i++ {
		twg.Run(func() {
			sema := make(chan struct{}, 10)
			var wg util.WaitGroupWrapper
			exitCh := make(chan struct{})
			wg.Run(func() {
				for j := 0; j < RunTimes; j++ {
					sema <- struct{}{}
				}
				close(exitCh)
			})
			producerFunc := func() (struct{}, error) {
				for {
					select {
					case <-sema:
						return struct{}{}, nil
					default:
						select {
						case <-exitCh:
							return struct{}{}, gpool.ErrProducerClosed
						default:
						}
					}
				}
			}
			resultCh, ctl := p.AddProducer(producerFunc, RunTimes, pooltask.NilContext{}, WithConcurrency(concurrency))
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range resultCh {
				}
			}()
			ctl.Wait()
			wg.Wait()
		})
	}
	twg.Wait()
}

func TestPoolWithoutEnoughCapacity(t *testing.T) {
	const (
		RunTimes    = 5
		concurrency = 2
		poolsize    = 2
	)
	p, err := NewSPMCPool[struct{}, struct{}, int, any, pooltask.NilContext]("TestPoolWithoutEnoughCapacity", poolsize, rmutil.UNKNOWN,
		WithExpiryDuration(DefaultExpiredTime))
	require.NoError(t, err)
	defer p.ReleaseAndWait()
	p.SetConsumerFunc(func(a struct{}, b int, c any) struct{} {
		return struct{}{}
	})
	var twg sync.WaitGroup
	for i := 0; i < 10; i++ {
		func() {
			sema := make(chan struct{}, 10)
			var wg util.WaitGroupWrapper
			exitCh := make(chan struct{})
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < RunTimes; j++ {
					sema <- struct{}{}
				}
				close(exitCh)
			}()
			producerFunc := func() (struct{}, error) {
				for {
					select {
					case <-sema:
						return struct{}{}, nil
					default:
						select {
						case <-exitCh:
							return struct{}{}, gpool.ErrProducerClosed
						default:
						}
					}
				}
			}
			resultCh, ctl := p.AddProducer(producerFunc, RunTimes, pooltask.NilContext{}, WithConcurrency(concurrency))

			wg.Add(1)
			go func() {
				defer wg.Done()
				for range resultCh {
				}
			}()
			ctl.Wait()
			wg.Wait()
		}()
	}
	twg.Wait()
}

func TestPoolWithoutEnoughCapacityParallel(t *testing.T) {
	const (
		RunTimes    = 5
		concurrency = 2
		poolsize    = 2
	)
	p, err := NewSPMCPool[struct{}, struct{}, int, any, pooltask.NilContext]("TestPoolWithoutEnoughCapacityParallel", poolsize, rmutil.UNKNOWN,
		WithExpiryDuration(DefaultExpiredTime), WithNonblocking(true))
	require.NoError(t, err)
	defer p.ReleaseAndWait()
	p.SetConsumerFunc(func(a struct{}, b int, c any) struct{} {
		return struct{}{}
	})
	var twg sync.WaitGroup
	for i := 0; i < 10; i++ {
		twg.Add(1)
		go func() {
			defer twg.Done()
			sema := make(chan struct{}, 10)
			var wg sync.WaitGroup
			exitCh := make(chan struct{})
			wg.Add(1)
			go func() {
				wg.Done()
				for j := 0; j < RunTimes; j++ {
					sema <- struct{}{}
				}
				close(exitCh)
			}()
			producerFunc := func() (struct{}, error) {
				for {
					select {
					case <-sema:
						return struct{}{}, nil
					default:
						select {
						case <-exitCh:
							return struct{}{}, gpool.ErrProducerClosed
						default:
						}
					}
				}
			}
			resultCh, ctl := p.AddProducer(producerFunc, RunTimes, pooltask.NilContext{}, WithConcurrency(concurrency))
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range resultCh {
				}
			}()
			ctl.Wait()
			wg.Wait()
		}()
	}
	twg.Wait()
}

func TestBenchPool(t *testing.T) {
	p, err := NewSPMCPool[struct{}, struct{}, int, any, pooltask.NilContext]("TestBenchPool", 10,
		rmutil.UNKNOWN, WithExpiryDuration(DefaultExpiredTime))
	require.NoError(t, err)
	defer p.ReleaseAndWait()
	p.SetConsumerFunc(func(a struct{}, b int, c any) struct{} {
		return struct{}{}
	})

	for i := 0; i < 1000; i++ {
		sema := make(chan struct{}, 10)
		var wg sync.WaitGroup
		exitCh := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < RunTimes; j++ {
				sema <- struct{}{}
			}
			close(exitCh)
		}()
		producerFunc := func() (struct{}, error) {
			for {
				select {
				case <-sema:
					return struct{}{}, nil
				default:
					select {
					case <-exitCh:
						return struct{}{}, gpool.ErrProducerClosed
					default:
					}
				}
			}
		}
		resultCh, ctl := p.AddProducer(producerFunc, RunTimes, pooltask.NilContext{}, WithConcurrency(6))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range resultCh {
			}
		}()
		ctl.Wait()
		wg.Wait()
	}
	p.ReleaseAndWait()
}
