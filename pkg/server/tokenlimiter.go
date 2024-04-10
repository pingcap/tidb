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

package server

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
