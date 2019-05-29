// Copyright 2019 PingCAP, Inc.
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

package expensivequery

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMaxExecTime{})

type testMaxExecTime struct{}

var (
	_ InterruptableQuery = &testLongTimeQuery{}
)

type testLongTimeQuery struct {
	queryID       uint32
	duration      time.Duration
	startExecTime time.Time
	timedOut      int32
}

func (ltq *testLongTimeQuery) MaxExecTimeExceeded() bool {
	return time.Now().After(ltq.startExecTime.Add(ltq.duration))
}

func (ltq *testLongTimeQuery) CancelOnTimeout() {
	atomic.StoreInt32(&ltq.timedOut, 1)
}

func (ltq *testLongTimeQuery) QueryID() uint32 {
	return ltq.queryID
}

func (ltq *testLongTimeQuery) isTimedOut() bool {
	return (atomic.LoadInt32(&ltq.timedOut) >= 1)
}

func (s *testMaxExecTime) Test1(c *C) {
	exitCh := make(chan struct{})
	mon := NewExpensiveQueryHandle(exitCh)
	go mon.Run(nil)
	var i int
	var queries []*testLongTimeQuery
	for i = 0; i < 5; i++ {
		query := testLongTimeQuery{
			queryID:       uint32(i),
			duration:      time.Duration((i+1)*1000) * time.Millisecond,
			startExecTime: time.Now(),
			timedOut:      0,
		}
		queries = append(queries, &query)
		mon.AddQuery(&query)
	}

	time.Sleep(time.Duration(2500) * time.Millisecond)

	for i = 0; i < 5; i++ {
		logutil.Logger(context.Background()).Info("checking", zap.Int("i:", i))
		query := queries[i]
		if i < 2 {
			c.Assert(query.QueryID(), Equals, uint32(i))
			c.Assert(query.isTimedOut(), IsTrue)
		} else {
			c.Assert(query.isTimedOut(), IsFalse)
		}
	}
	close(exitCh)
}

func (s *testMaxExecTime) Test2(c *C) {
	exitCh := make(chan struct{})
	mon := NewExpensiveQueryHandle(exitCh)
	go mon.Run(nil)

	query1 := testLongTimeQuery{
		queryID:       uint32(88),
		duration:      time.Duration(100) * time.Millisecond,
		startExecTime: time.Now(),
		timedOut:      0,
	}

	c.Assert(mon.AddQuery(&query1), IsNil)

	query2 := query1

	c.Assert(mon.AddQuery(&query2).Error(), Equals, errors.New("duplicate query id").Error())

	close(exitCh)
}
