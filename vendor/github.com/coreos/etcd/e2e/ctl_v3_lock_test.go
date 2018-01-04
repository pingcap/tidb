// Copyright 2016 The etcd Authors
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

package e2e

import (
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coreos/etcd/pkg/expect"
)

// debugLockSignal forces SIGQUIT to debug etcdctl elect and lock failures
var debugLockSignal os.Signal

func init() {
	// hacks to ignore SIGQUIT debugging for some builds
	switch {
	case os.Getenv("COVERDIR") != "":
		// SIGQUIT interferes with coverage collection
		debugLockSignal = syscall.SIGTERM
	case runtime.GOARCH == "ppc64le":
		// ppc64le's signal handling won't kill processes with SIGQUIT
		// in the same way as amd64/i386, so processes won't terminate
		// as expected. Since this debugging code for CI, just ignore
		// ppc64le.
		debugLockSignal = syscall.SIGKILL
	default:
		// stack dumping OK
		debugLockSignal = syscall.SIGQUIT
	}
}

func TestCtlV3Lock(t *testing.T) {
	oldenv := os.Getenv("EXPECT_DEBUG")
	defer os.Setenv("EXPECT_DEBUG", oldenv)
	os.Setenv("EXPECT_DEBUG", "1")

	testCtl(t, testLock)
}

func testLock(cx ctlCtx) {
	// debugging for #6464
	sig := cx.epc.withStopSignal(debugLockSignal)
	defer cx.epc.withStopSignal(sig)

	name := "a"

	holder, ch, err := ctlV3Lock(cx, name)
	if err != nil {
		cx.t.Fatal(err)
	}

	l1 := ""
	select {
	case <-time.After(2 * time.Second):
		cx.t.Fatalf("timed out locking")
	case l1 = <-ch:
		if !strings.HasPrefix(l1, name) {
			cx.t.Errorf("got %q, expected %q prefix", l1, name)
		}
	}

	// blocked process that won't acquire the lock
	blocked, ch, err := ctlV3Lock(cx, name)
	if err != nil {
		cx.t.Fatal(err)
	}
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ch:
		cx.t.Fatalf("should block")
	}

	// overlap with a blocker that will acquire the lock
	blockAcquire, ch, err := ctlV3Lock(cx, name)
	if err != nil {
		cx.t.Fatal(err)
	}
	defer blockAcquire.Stop()
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ch:
		cx.t.Fatalf("should block")
	}

	// kill blocked process with clean shutdown
	if err = blocked.Signal(os.Interrupt); err != nil {
		cx.t.Fatal(err)
	}
	if err = closeWithTimeout(blocked, time.Second); err != nil {
		cx.t.Fatal(err)
	}

	// kill the holder with clean shutdown
	if err = holder.Signal(os.Interrupt); err != nil {
		cx.t.Fatal(err)
	}
	if err = closeWithTimeout(holder, time.Second); err != nil {
		cx.t.Fatal(err)
	}

	// blockAcquire should acquire the lock
	select {
	case <-time.After(time.Second):
		cx.t.Fatalf("timed out from waiting to holding")
	case l2 := <-ch:
		if l1 == l2 || !strings.HasPrefix(l2, name) {
			cx.t.Fatalf("expected different lock name, got l1=%q, l2=%q", l1, l2)
		}
	}
}

// ctlV3Lock creates a lock process with a channel listening for when it acquires the lock.
func ctlV3Lock(cx ctlCtx, name string) (*expect.ExpectProcess, <-chan string, error) {
	cmdArgs := append(cx.PrefixArgs(), "lock", name)
	proc, err := spawnCmd(cmdArgs)
	outc := make(chan string, 1)
	if err != nil {
		close(outc)
		return proc, outc, err
	}
	proc.StopSignal = debugLockSignal
	go func() {
		s, xerr := proc.ExpectFunc(func(string) bool { return true })
		if xerr != nil {
			cx.t.Errorf("expect failed (%v)", xerr)
		}
		outc <- s
	}()
	return proc, outc, err
}
