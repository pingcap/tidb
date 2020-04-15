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

package stmtctx_test

import (
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/sessionctx/stmtctx"
	"github.com/pingcap/tidb/v4/util/execdetails"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type stmtctxSuit struct{}

var _ = Suite(&stmtctxSuit{})

func (s *stmtctxSuit) TestCopTasksDetails(c *C) {
	ctx := new(stmtctx.StatementContext)
	backoffs := []string{"tikvRPC", "pdRPC", "regionMiss"}
	for i := 0; i < 100; i++ {
		d := &execdetails.ExecDetails{
			CalleeAddress: fmt.Sprintf("%v", i+1),
			ProcessTime:   time.Second * time.Duration(i+1),
			WaitTime:      time.Millisecond * time.Duration(i+1),
			BackoffSleep:  make(map[string]time.Duration),
			BackoffTimes:  make(map[string]int),
		}
		for _, backoff := range backoffs {
			d.BackoffSleep[backoff] = time.Millisecond * 100 * time.Duration(i+1)
			d.BackoffTimes[backoff] = i + 1
		}
		ctx.MergeExecDetails(d, nil)
	}
	d := ctx.CopTasksDetails()
	c.Assert(d.NumCopTasks, Equals, 100)
	c.Assert(d.AvgProcessTime, Equals, time.Second*101/2)
	c.Assert(d.P90ProcessTime, Equals, time.Second*91)
	c.Assert(d.MaxProcessTime, Equals, time.Second*100)
	c.Assert(d.MaxProcessAddress, Equals, "100")
	c.Assert(d.AvgWaitTime, Equals, time.Millisecond*101/2)
	c.Assert(d.P90WaitTime, Equals, time.Millisecond*91)
	c.Assert(d.MaxWaitTime, Equals, time.Millisecond*100)
	c.Assert(d.MaxWaitAddress, Equals, "100")
	fields := d.ToZapFields()
	c.Assert(len(fields), Equals, 9)
	for _, backoff := range backoffs {
		c.Assert(d.MaxBackoffAddress[backoff], Equals, "100")
		c.Assert(d.MaxBackoffTime[backoff], Equals, 100*time.Millisecond*100)
		c.Assert(d.P90BackoffTime[backoff], Equals, time.Millisecond*100*91)
		c.Assert(d.AvgBackoffTime[backoff], Equals, time.Millisecond*100*101/2)
		c.Assert(d.TotBackoffTimes[backoff], Equals, 101*50)
		c.Assert(d.TotBackoffTime[backoff], Equals, 101*50*100*time.Millisecond)
	}
}

func (s *stmtctxSuit) TestStatementContextPushDownFLags(c *C) {
	testCases := []struct {
		in  *stmtctx.StatementContext
		out uint64
	}{
		{&stmtctx.StatementContext{InInsertStmt: true}, 8},
		{&stmtctx.StatementContext{InUpdateStmt: true}, 16},
		{&stmtctx.StatementContext{InDeleteStmt: true}, 16},
		{&stmtctx.StatementContext{InSelectStmt: true}, 32},
		{&stmtctx.StatementContext{IgnoreTruncate: true}, 1},
		{&stmtctx.StatementContext{TruncateAsWarning: true}, 2},
		{&stmtctx.StatementContext{OverflowAsWarning: true}, 64},
		{&stmtctx.StatementContext{IgnoreZeroInDate: true}, 128},
		{&stmtctx.StatementContext{DividedByZeroAsWarning: true}, 256},
		{&stmtctx.StatementContext{InLoadDataStmt: true}, 1024},
		{&stmtctx.StatementContext{InSelectStmt: true, TruncateAsWarning: true}, 34},
		{&stmtctx.StatementContext{DividedByZeroAsWarning: true, IgnoreTruncate: true}, 257},
		{&stmtctx.StatementContext{InUpdateStmt: true, IgnoreZeroInDate: true, InLoadDataStmt: true}, 1168},
	}
	for _, tt := range testCases {
		got := tt.in.PushDownFlags()
		c.Assert(got, Equals, tt.out, Commentf("get %v, want %v", got, tt.out))
	}
}
