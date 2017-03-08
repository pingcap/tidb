// Copyright 2017 PingCAP, Inc.
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

package executor_test

import (
	"bytes"
	"fmt"
	"runtime/pprof"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/testkit"
)

// This test checks that when a index double read returns before reading all the rows, the goroutine doesn't
// leak. For testing distsql with multiple regions, we need to manually split a mock TiKV.
func (s *testSuite) TestIndexDoubleReadClose(c *C) {
	if _, ok := s.store.GetClient().(*tikv.CopClient); !ok {
		// Make sure the store is tikv store.
		return
	}
	originSize := executor.LookupTableTaskChannelSize
	executor.LookupTableTaskChannelSize = 1
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table dist (id int primary key, c_idx int, c_col int, index (c_idx))")

	// Insert 100 rows.
	var values []string
	for i := 0; i < 100; i++ {
		values = append(values, fmt.Sprintf("(%d, %d, %d)", i, i, i))
	}
	tk.MustExec("insert dist values " + strings.Join(values, ","))

	rss, err := tk.Se.Execute("select * from dist where c_idx between 0 and 100")
	c.Assert(err, IsNil)
	rs := rss[0]
	_, err = rs.Next()
	c.Assert(err, IsNil)
	c.Check(taskGoroutineExists(), IsTrue)
	rs.Close()
	time.Sleep(time.Millisecond * 50)
	c.Check(taskGoroutineExists(), IsFalse)
	executor.LookupTableTaskChannelSize = originSize
}

func taskGoroutineExists() bool {
	buf := new(bytes.Buffer)
	profile := pprof.Lookup("goroutine")
	profile.WriteTo(buf, 1)
	str := buf.String()
	return strings.Contains(str, "pickAndExecTask")
}
