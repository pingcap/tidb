// Copyright 2026 PingCAP, Inc.
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

package driver

import (
	"errors"
	"sync"
)

var errStorePoolClosed = errors.New("store pool is closed")

type storePool struct {
	sem       chan struct{}
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newStorePool(concurrency int) *storePool {
	if concurrency < 1 {
		concurrency = 1
	}
	return &storePool{
		sem:     make(chan struct{}, concurrency),
		closeCh: make(chan struct{}),
	}
}

func (p *storePool) Run(fn func()) error {
	if fn == nil {
		return nil
	}
	select {
	case <-p.closeCh:
		return errStorePoolClosed
	default:
	}

	select {
	case p.sem <- struct{}{}:
	case <-p.closeCh:
		return errStorePoolClosed
	}

	select {
	case <-p.closeCh:
		<-p.sem
		return errStorePoolClosed
	default:
	}

	go func() {
		defer func() { <-p.sem }()
		fn()
	}()
	return nil
}

func (p *storePool) Close() {
	p.closeOnce.Do(func() {
		close(p.closeCh)
	})
}
