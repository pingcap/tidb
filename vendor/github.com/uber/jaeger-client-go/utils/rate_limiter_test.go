// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateLimiter(t *testing.T) {
	limiter := NewRateLimiter(2.0, 2.0)
	// stop time
	ts := time.Now()
	limiter.(*rateLimiter).lastTick = ts
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts
	}
	assert.True(t, limiter.CheckCredit(1.0))
	assert.True(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
	// move time 250ms forward, not enough credits to pay for 1.0 item
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts.Add(time.Second / 4)
	}
	assert.False(t, limiter.CheckCredit(1.0))
	// move time 500ms forward, now enough credits to pay for 1.0 item
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts.Add(time.Second/4 + time.Second/2)
	}
	assert.True(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
	// move time 5s forward, enough to accumulate credits for 10 messages, but it should still be capped at 2
	limiter.(*rateLimiter).lastTick = ts
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts.Add(5 * time.Second)
	}
	assert.True(t, limiter.CheckCredit(1.0))
	assert.True(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
}

func TestMaxBalance(t *testing.T) {
	limiter := NewRateLimiter(0.1, 1.0)
	// stop time
	ts := time.Now()
	limiter.(*rateLimiter).lastTick = ts
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts
	}
	// on initialization, should have enough credits for 1 message
	assert.True(t, limiter.CheckCredit(1.0))

	// move time 20s forward, enough to accumulate credits for 2 messages, but it should still be capped at 1
	limiter.(*rateLimiter).timeNow = func() time.Time {
		return ts.Add(time.Second * 20)
	}
	assert.True(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
}
