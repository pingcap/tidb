// Copyright 2018 PingCAP, Inc.
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
	"sync"
	"sync/atomic"

	//	gofail "github.com/coreos/gofail/runtime"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/util/testkit"
	log "github.com/sirupsen/logrus"
	// "github.com/pingcap/tidb/ddl"
)

func (s *testSuite) TestParallelExecDDL(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table ddl_fail (c1 int, c2 int default 1, index (c1))")
	//	tc := &ddl.TestDDLCallback{}
	//	s.domain.DDL().SetHook(tc)
	ch := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(2)
	atomic.StoreInt32(&executor.ParallelCnt, 1)
	go func() {
		defer wg.Done()

		close(ch)
		// gofail.Enable("github.com/pingcap/tidb/executor/parallelCnt", `return(0)`)
		log.Warnf("*************************** id %v", tk.Se.GetSessionVars().ConnectionID)
		tk.MustExec("alter table ddl_fail add column c3 int")
		log.Warnf("*************************** id %v, cnt %v", tk.Se.GetSessionVars().ConnectionID, executor.ParallelCnt)
		atomic.StoreInt32(&executor.ParallelCnt, 0)
		// gofail.Enable("github.com/pingcap/tidb/executor/parallelCnt", `return(0)`)
		log.Warnf("*************************** id %v, cnt %v", tk.Se.GetSessionVars().ConnectionID, executor.ParallelCnt)
	}()
	go func() {
		defer wg.Done()

		<-ch
		// gofail.Enable("github.com/pingcap/tidb/executor/parallelCnt", `return(1)`)
		log.Warnf("111 *************************** id %v", tk1.Se.GetSessionVars().ConnectionID)
		tk1.MustExec("alter table ddl_fail add column c3 int")
	}()

	wg.Wait()
	//	gofail.Disable("github.com/pingcap/tidb/executor/parallelCnt")
}
