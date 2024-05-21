// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const maxConcurrency = uint(1024)

// ConcurrencyController is a controller to control the concurrency of the function.
type ConcurrencyController struct {
	ch    chan struct{}
	count uint
	mu    sync.Mutex
}

// NewConcurrencyController creates a ConcurrencyController.
func NewConcurrencyController(concurrency uint) *ConcurrencyController {
	result := &ConcurrencyController{
		ch:    make(chan struct{}, maxConcurrency),
		count: concurrency,
	}
	for i := uint(0); i < concurrency; i++ {
		result.ch <- struct{}{}
	}
	return result
}

// SetConcurrency sets the concurrency of the ConcurrencyController.
func (c *ConcurrencyController) SetConcurrency(concurrency uint) error {
	if concurrency == 0 {
		return errors.New("concurrency should be greater than 0")
	}
	if c.count == concurrency {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if concurrency > maxConcurrency {
		concurrency = maxConcurrency
	}
	if concurrency > c.count {
		for i := uint(0); i < concurrency-c.count; i++ {
			c.ch <- struct{}{}
		}
	} else {
		for i := uint(0); i < c.count-concurrency; i++ {
			<-c.ch
		}
	}
	c.count = concurrency
	return nil
}

// Run runs the function fn with concurrency control.
func (c *ConcurrencyController) Run(fn func()) error {
	_, ok := <-c.ch
	if !ok {
		return errors.New("concurrency controller is closed")
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logutil.BgLogger().Error("panic in ConcurrencyController.Run", zap.Any("recover", r), zap.Stack("stack"))
			}
			c.ch <- struct{}{}
		}()
		fn()
	}()
	return nil
}

// Close closes the ConcurrencyController.
func (c *ConcurrencyController) Close() {
	close(c.ch)
}
