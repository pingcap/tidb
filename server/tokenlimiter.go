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
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import "sync/atomic"

// Token is used as a permission to keep on running.
type Token struct {
}

// TokenLimiter is used to limit the number of concurrent tasks.
type TokenLimiter struct {
	capacity  uint32
	allocated uint32
	ch        chan *Token
}

// Put releases the token.
func (tl *TokenLimiter) Put(tk *Token) {
	tl.ch <- tk
	atomic.AddUint32(&tl.allocated, ^uint32(0))
}

// Get obtains a token.
func (tl *TokenLimiter) Get() *Token {
	token := <-tl.ch
	atomic.AddUint32(&tl.allocated, 1)
	return token
}

func (tl *TokenLimiter) Allocated() uint32 {
	return atomic.LoadUint32(&tl.allocated)
}

func (tl *TokenLimiter) Capacity() uint32 {
	return tl.capacity
}

// NewTokenLimiter creates a TokenLimiter with capacity tokens.
func NewTokenLimiter(count uint) *TokenLimiter {
	tl := &TokenLimiter{capacity: uint32(count), ch: make(chan *Token, count)}
	for i := uint(0); i < count; i++ {
		tl.ch <- &Token{}
	}

	return tl
}
