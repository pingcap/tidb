// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2016 PingCAP, Inc.
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
// +build leak

package testleak

import (
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/check"
)

func interestingGoroutines() (gs []string) {
	buf := make([]byte, 2<<20)
	buf = buf[:runtime.Stack(buf, true)]
	ignoreList := []string{
		"created by github.com/pingcap/tidb.init",
		"testing.RunTests",
		"check.(*resultTracker).start",
		"check.(*suiteRunner).runFunc",
		"check.(*suiteRunner).parallelRun",
		"localstore.(*dbStore).scheduler",
		"tikv.(*noGCHandler).Start",
		"ddl.(*ddl).start",
		"ddl.(*delRange).startEmulator",
		"domain.NewDomain",
		"testing.(*T).Run",
		"domain.(*Domain).LoadPrivilegeLoop",
		"domain.(*Domain).UpdateTableStatsLoop",
		"testing.Main(",
		"runtime.goexit",
		"created by runtime.gc",
		"interestingGoroutines",
		"runtime.MHeap_Scavenger",
		"created by os/signal.init",
		// these go routines are async terminated, so they may still alive after test end, thus cause
		// false positive leak failures
		"google.golang.org/grpc.(*addrConn).resetTransport",
		"google.golang.org/grpc.(*ccBalancerWrapper).watcher",
		"github.com/pingcap/goleveldb/leveldb/util.(*BufferPool).drain",
		"github.com/pingcap/goleveldb/leveldb.(*DB).compactionError",
		"github.com/pingcap/goleveldb/leveldb.(*DB).mpoolDrain",
		"go.etcd.io/etcd/v3/pkg/logutil.(*MergeLogger).outputLoop",
		// import PD will introduce another MergeLogger
		"go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop",
		"oracles.(*pdOracle).updateTS",
		"tikv.(*tikvStore).runSafePointChecker",
		"tikv.(*RegionCache).asyncCheckAndResolveLoop",
		"github.com/pingcap/badger.(*writeWorker).runMergeLSM",
	}
	shouldIgnore := func(stack string) bool {
		if stack == "" {
			return true
		}
		for _, ident := range ignoreList {
			if strings.Contains(stack, ident) {
				return true
			}
		}
		return false
	}
	for _, g := range strings.Split(string(buf), "\n\n") {
		sl := strings.SplitN(g, "\n", 2)
		if len(sl) != 2 {
			continue
		}
		stack := strings.TrimSpace(sl[1])
		if shouldIgnore(stack) {
			continue
		}
		gs = append(gs, stack)
	}
	sort.Strings(gs)
	return
}

var beforeTestGoroutines = map[string]bool{}
var testGoroutinesInited bool

// BeforeTest gets the current goroutines.
// It's used for check.Suite.SetUpSuite() function.
// Now it's only used in the tidb_test.go.
// Note: it's not accurate, consider the following function:
// func loop() {
//   for {
//     select {
//       case <-ticker.C:
//         DoSomething()
//     }
//   }
// }
// If this loop step into DoSomething() during BeforeTest(), the stack for this goroutine will contain DoSomething().
// Then if this loop jumps out of DoSomething during AfterTest(), the stack for this goroutine will not contain DoSomething().
// Resulting in false-positive leak reports.
func BeforeTest() {
	for _, g := range interestingGoroutines() {
		beforeTestGoroutines[g] = true
	}
	testGoroutinesInited = true
}

const defaultCheckCnt = 50

func checkLeakAfterTest(errorFunc func(cnt int, g string)) func() {
	// After `BeforeTest`, `beforeTestGoroutines` may still be empty, in this case,
	// we shouldn't init it again.
	if !testGoroutinesInited && len(beforeTestGoroutines) == 0 {
		for _, g := range interestingGoroutines() {
			beforeTestGoroutines[g] = true
		}
	}

	cnt := defaultCheckCnt
	return func() {
		defer func() {
			beforeTestGoroutines = map[string]bool{}
			testGoroutinesInited = false
		}()

		var leaked []string
		for i := 0; i < cnt; i++ {
			leaked = leaked[:0]
			for _, g := range interestingGoroutines() {
				if !beforeTestGoroutines[g] {
					leaked = append(leaked, g)
				}
			}
			// Bad stuff found, but goroutines might just still be
			// shutting down, so give it some time.
			if len(leaked) != 0 {
				time.Sleep(50 * time.Millisecond)
				continue
			}

			return
		}
		for _, g := range leaked {
			errorFunc(cnt, g)
		}
	}
}

// AfterTest gets the current goroutines and runs the returned function to
// get the goroutines at that time to contrast whether any goroutines leaked.
// Usage: defer testleak.AfterTest(c)()
// It can call with BeforeTest() at the beginning of check.Suite.TearDownSuite() or
// call alone at the beginning of each test.
func AfterTest(c *check.C) func() {
	errorFunc := func(cnt int, g string) {
		c.Errorf("Test %s check-count %d appears to have leaked: %v", c.TestName(), cnt, g)
	}
	return checkLeakAfterTest(errorFunc)
}

// AfterTestT is used after all the test cases is finished.
func AfterTestT(t *testing.T) func() {
	errorFunc := func(cnt int, g string) {
		t.Errorf("Test %s check-count %d appears to have leaked: %v", t.Name(), cnt, g)
	}
	return checkLeakAfterTest(errorFunc)
}
