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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	type ConstArgs struct {
		a int
	}
	myArgs := ConstArgs{a: 10}
	// init the pool
	// input type， output type, constArgs type
	pool := NewSPMCPool[int, int, ConstArgs, any, NilContext](10)
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
			return 0, errors.New("not job")
		}
	}
	// add new task
	resultCh, control := pool.AddProducer(pfunc, myArgs, NilContext{}, WithConcurrency(4))

	var count atomic.Uint32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case result := <-resultCh:
				count.Add(1)
				require.Greater(t, result, 10)
			default:
				if control.IsProduceClose() {
					return
				}
			}
		}
	}()
	// Waiting task finishing
	control.Wait()
	wg.Wait()
	require.Equal(t, uint32(10), count.Load())

	// close pool
	pool.ReleaseAndWait()
}

func TestPoolWithEnoughCapa(t *testing.T) {
	const RunTimes = 1000
	p := NewSPMCPool[struct{}, struct{}, int, any, NilContext](30, WithExpiryDuration(DefaultExpiredTime))
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
							return struct{}{}, errors.New("not job")
						default:
						}
					}
				}
			}
			resultCh, ctl := p.AddProducer(producerFunc, RunTimes, NilContext{}, WithConcurrency(6))
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-resultCh:
					default:
						if ctl.IsProduceClose() {
							return
						}
					}
				}
			}()
			ctl.Wait()
			wg.Wait()
		})
	}
	twg.Wait()
}

func TestPoolWithoutEnoughCapa(t *testing.T) {
	const RunTimes = 1000
	p := NewSPMCPool[struct{}, struct{}, int, any, NilContext](30, WithExpiryDuration(DefaultExpiredTime))
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
							return struct{}{}, errors.New("not job")
						default:
						}
					}
				}
			}
			resultCh, ctl := p.AddProducer(producerFunc, RunTimes, NilContext{}, WithConcurrency(6))
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-resultCh:
					default:
						if ctl.IsProduceClose() {
							return
						}
					}
				}
			}()
			ctl.Wait()
			wg.Wait()
		})
	}
	twg.Wait()
}

func TestBenchPool(t *testing.T) {
	p := NewSPMCPool[struct{}, struct{}, int, any, NilContext](10, WithExpiryDuration(DefaultExpiredTime))
	defer p.ReleaseAndWait()
	p.SetConsumerFunc(func(a struct{}, b int, c any) struct{} {
		return struct{}{}
	})

	for i := 0; i < 1000; i++ {
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
						return struct{}{}, errors.New("not job")
					default:
					}
				}
			}
		}
		resultCh, ctl := p.AddProducer(producerFunc, RunTimes, NilContext{}, WithConcurrency(6))
		exitCh2 := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-exitCh2:
					return
				case <-resultCh:
				}
			}
		}()
		ctl.Wait()
		close(exitCh2)
		wg.Wait()
	}
	p.ReleaseAndWait()
}
