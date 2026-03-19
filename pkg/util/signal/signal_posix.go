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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || freebsd || unix

package signal

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// it's the same function used by "/debug/pprof/goroutine"
// see https://github.com/golang/go/blob/e515ef8bc271f632bb2ebb94e8e700ab67274268/src/runtime/pprof/pprof.go#L750-L769
func getGoroutineStacks() string {
	// We don't know how big the buffer needs to be to collect
	// all the goroutines. Start with 1 MB and try a few times, doubling each time.
	// Give up and use a truncated trace if 64 MB is not enough.
	buf := make([]byte, 1<<20)
	for i := 0; ; i++ {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		if len(buf) >= 64<<20 {
			// Filled 64 MB - stop there.
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	return string(buf)
}

// SetupUSR1Handler sets up a signal handler for SIGUSR1.
// When SIGUSR1 is received, it dumps the current goroutine stack to the log, it's
// useful for debugging issue when the server hasn't started yet or stuck on showdown.
func SetupUSR1Handler() {
	usrDefSignalChan := make(chan os.Signal, 1)
	signal.Notify(usrDefSignalChan, syscall.SIGUSR1)
	go func() {
		for {
			sig := <-usrDefSignalChan
			if sig == syscall.SIGUSR1 {
				log.Printf("\n=== Got signal [%s] to dump goroutine stack. ===\n%s\n=== Finished dumping goroutine stack. ===\n",
					sig, getGoroutineStacks())
			}
		}
	}()
}

// SetupSignalHandler setup signal handler for TiDB Server
func SetupSignalHandler(shutdownFunc func()) {
	closeSignalChan := make(chan os.Signal, 1)
	signal.Notify(closeSignalChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-closeSignalChan
		logutil.BgLogger().Info("got signal to exit", zap.Stringer("signal", sig))
		shutdownFunc()
	}()
}
