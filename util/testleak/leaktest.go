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
	for _, g := range strings.Split(string(buf), "\n\n") {
		sl := strings.SplitN(g, "\n", 2)
		if len(sl) != 2 {
			continue
		}
		stack := strings.TrimSpace(sl[1])
		if stack == "" ||
			strings.Contains(stack, "created by github.com/pingcap/tidb.init") ||
			strings.Contains(stack, "testing.RunTests") ||
			strings.Contains(stack, "check.(*resultTracker).start") ||
			strings.Contains(stack, "check.(*suiteRunner).runFunc") ||
			strings.Contains(stack, "check.(*suiteRunner).parallelRun") ||
			strings.Contains(stack, "localstore.(*dbStore).scheduler") ||
			strings.Contains(stack, "tikv.(*noGCHandler).Start") ||
			strings.Contains(stack, "ddl.(*ddl).start") ||
			strings.Contains(stack, "ddl.(*delRange).startEmulator") ||
			strings.Contains(stack, "domain.NewDomain") ||
			strings.Contains(stack, "testing.(*T).Run") ||
			strings.Contains(stack, "domain.(*Domain).LoadPrivilegeLoop") ||
			strings.Contains(stack, "domain.(*Domain).UpdateTableStatsLoop") ||
			strings.Contains(stack, "testing.Main(") ||
			strings.Contains(stack, "runtime.goexit") ||
			strings.Contains(stack, "created by runtime.gc") ||
			strings.Contains(stack, "interestingGoroutines") ||
			strings.Contains(stack, "runtime.MHeap_Scavenger") {
			continue
		}
		gs = append(gs, stack)
	}
	sort.Strings(gs)
	return
}

var beforeTestGorountines = map[string]bool{}

// BeforeTest gets the current goroutines.
// It's used for check.Suite.SetUpSuite() function.
// Now it's only used in the tidb_test.go.
func BeforeTest() {
	for _, g := range interestingGoroutines() {
		beforeTestGorountines[g] = true
	}
}

const defaultCheckCnt = 50

func checkLeakAfterTest(errorFunc func(cnt int, g string)) func() {
	if len(beforeTestGorountines) == 0 {
		for _, g := range interestingGoroutines() {
			beforeTestGorountines[g] = true
		}
	}

	cnt := defaultCheckCnt
	return func() {
		defer func() {
			beforeTestGorountines = map[string]bool{}
		}()

		var leaked []string
		for i := 0; i < cnt; i++ {
			leaked = leaked[:0]
			for _, g := range interestingGoroutines() {
				if !beforeTestGorountines[g] {
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
