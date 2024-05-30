// Copyright 2015 PingCAP, Inc.
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
	"errors"
	"sync"
)

// Token is used as a permission to keep on running.
type Token struct {
}

// TokenLimiter is used to limit the number of concurrent tasks.
type TokenLimiter struct {
	count uint
	ch    chan *Token
}

// Put releases the token.
func (tl *TokenLimiter) Put(tk *Token) {
	tl.ch <- tk
}

// Get obtains a token.
func (tl *TokenLimiter) Get() *Token {
	return <-tl.ch
}

// NewTokenLimiter creates a TokenLimiter with count tokens.
func NewTokenLimiter(count uint) *TokenLimiter {
	tl := &TokenLimiter{count: count, ch: make(chan *Token, count)}
	for i := uint(0); i < count; i++ {
		tl.ch <- &Token{}
	}

	return tl
}

// DynamicTokenLimiter is a controller to control the concurrency of the function.
type DynamicTokenLimiter struct {
	ch       chan Token
	count    uint
	maxCount uint
	mu       sync.Mutex
}

// NewDynamicTokenLimiter creates a DynamicTokenLimiter.
func NewDynamicTokenLimiter(concurrency, maxCnt uint) *DynamicTokenLimiter {
	maxCnt = min(maxCnt, 1024*1024)
	result := &DynamicTokenLimiter{
		ch:       make(chan Token, maxCnt),
		maxCount: maxCnt,
		count:    concurrency,
	}
	for i := uint(0); i < concurrency; i++ {
		result.ch <- Token{}
	}
	return result
}

// SetToken sets the concurrency of the DynamicTokenLimiter.
func (c *DynamicTokenLimiter) SetToken(concurrency uint) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if concurrency == 0 {
		return errors.New("concurrency should be greater than 0")
	}
	if c.count == concurrency {
		return nil
	}
	if concurrency > c.maxCount {
		concurrency = c.maxCount
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

// Get returns a token.
func (c *DynamicTokenLimiter) Get() Token {
	return <-c.ch
}

// TryGet tries to get a token, if the token is not acquired, it will return false.
func (c *DynamicTokenLimiter) TryGet() (*Token, bool) {
	select {
	case tk := <-c.ch:
		return &tk, true
	default:
		return nil, false
	}
}

// Put releases the token.
func (c *DynamicTokenLimiter) Put(tk Token) {
	c.ch <- tk
}

// TryPut tries to release the token, if the token is not acquired, it will return false.
func (c *DynamicTokenLimiter) TryPut(tk Token) bool {
	select {
	case c.ch <- tk:
		return true
	default:
		return false
	}
}
