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

package tokenlimiter

type Token struct {
}

type TokenLimiter struct {
	count int
	ch    chan *Token
}

func (tl *TokenLimiter) Put(tk *Token) {
	tl.ch <- tk
}

func (tl *TokenLimiter) Get() *Token {
	return <-tl.ch
}

func NewTokenLimiter(count int) *TokenLimiter {
	tl := &TokenLimiter{count: count, ch: make(chan *Token, count)}
	for i := 0; i < count; i++ {
		tl.ch <- &Token{}
	}

	return tl
}
