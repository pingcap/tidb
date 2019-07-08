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

package stmtctx

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/util/execdetails"
)

func TestCopTasksDetails(t *testing.T) {
	ctx := new(StatementContext)
	for i := 0; i < 100; i++ {
		d := &execdetails.ExecDetails{
			CalleeAddress: fmt.Sprintf("%v", i+1),
			ProcessTime:   time.Second * time.Duration(i+1),
			WaitTime:      time.Millisecond * time.Duration(i+1),
		}
		ctx.MergeExecDetails(d, nil)
	}
	c := ctx.CopTasksDetails()
	if c.NumCopTasks != 100 ||
		c.AvgProcessTime != time.Second*101/2 ||
		c.P90ProcessTime != time.Second*91 ||
		c.MaxProcessTime != time.Second*100 ||
		c.MaxProcessAddress != "100" ||
		c.AvgWaitTime != time.Millisecond*101/2 ||
		c.P90WaitTime != time.Millisecond*91 ||
		c.MaxWaitTime != time.Millisecond*100 ||
		c.MaxWaitAddress != "100" {
		t.Fatal(c)
	}
	fields := c.ToZapFields()
	if len(fields) != 9 {
		t.Fatal(c)
	}
}

func TestStatementContextPushDownFLags(t *testing.T) {
	testCases := []struct {
		in  *StatementContext
		out uint64
	}{
		{&StatementContext{InInsertStmt: true}, 8},
		{&StatementContext{InUpdateStmt: true}, 16},
		{&StatementContext{InDeleteStmt: true}, 16},
		{&StatementContext{InSelectStmt: true}, 32},
		{&StatementContext{IgnoreTruncate: true}, 1},
		{&StatementContext{TruncateAsWarning: true}, 2},
		{&StatementContext{OverflowAsWarning: true}, 64},
		{&StatementContext{IgnoreZeroInDate: true}, 128},
		{&StatementContext{DividedByZeroAsWarning: true}, 256},
		{&StatementContext{PadCharToFullLength: true}, 4},
		{&StatementContext{InLoadDataStmt: true}, 1024},
		{&StatementContext{InSelectStmt: true, TruncateAsWarning: true}, 34},
		{&StatementContext{DividedByZeroAsWarning: true, IgnoreTruncate: true}, 257},
		{&StatementContext{InUpdateStmt: true, IgnoreZeroInDate: true, InLoadDataStmt: true}, 1168},
	}
	for _, tt := range testCases {
		got := tt.in.PushDownFlags()
		if got != tt.out {
			t.Errorf("get %v, want %v\n", got, tt.out)
		}
	}
}
