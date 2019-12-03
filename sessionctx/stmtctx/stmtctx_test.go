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
	for _, backoff := range backoffs {
		if c.MaxBackoffAddress[backoff] != "100" ||
			c.MaxBackoffTime[backoff] != 100*time.Millisecond*100 ||
			c.P90BackoffTime[backoff] != time.Millisecond*100*91 ||
			c.AvgBackoffTime[backoff] != time.Millisecond*100*101/2 ||
			c.TotBackoffTimes[backoff] != 101*50 ||
			c.TotBackoffTime[backoff] != 101*50*100*time.Millisecond {
			t.Fatal(c)
		}
	}
}
