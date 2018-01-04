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

package testutil

import (
	"fmt"
	"net"
	"sync"
	"time"

	check "github.com/pingcap/check"
	log "github.com/sirupsen/logrus"
)

var (
	testAddrMutex sync.Mutex
	testAddrMap   = make(map[string]struct{})
)

// AllocTestURL allocates a local URL for testing.
func AllocTestURL() string {
	for i := 0; i < 10; i++ {
		if u := tryAllocTestURL(); u != "" {
			return u
		}
		time.Sleep(time.Second)
	}
	log.Fatal("failed to alloc test URL")
	return ""
}

func tryAllocTestURL() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	addr := fmt.Sprintf("http://%s", l.Addr())
	err = l.Close()
	if err != nil {
		log.Fatal(err)
	}

	testAddrMutex.Lock()
	defer testAddrMutex.Unlock()
	if _, ok := testAddrMap[addr]; ok {
		return ""
	}
	testAddrMap[addr] = struct{}{}
	return addr
}

const (
	waitMaxRetry   = 100
	waitRetrySleep = time.Millisecond * 50
)

// CheckFunc is a condition checker that passed to WaitUntil. Its implementation
// may call c.Fatal() to abort the test, or c.Log() to add more information.
type CheckFunc func(c *check.C) bool

// WaitUntil repeatly evaluates f() for a period of time, util it returns true.
func WaitUntil(c *check.C, f CheckFunc) {
	c.Log("wait start")
	for i := 0; i < waitMaxRetry; i++ {
		if f(c) {
			return
		}
		time.Sleep(waitRetrySleep)
	}
	c.Fatal("wait timeout")
}
