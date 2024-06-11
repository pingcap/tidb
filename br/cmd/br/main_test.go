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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// Reference: https://dzone.com/articles/measuring-integration-test-coverage-rate-in-pouchc

import (
	"os"
	"strings"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	skipLeakTest := false
	var newArgs []string
	for _, arg := range os.Args {
		if arg == "--skip-goleak" {
			skipLeakTest = true
		} else {
			newArgs = append(newArgs, arg)
		}
	}
	os.Args = newArgs

	if !skipLeakTest {
		goleak.VerifyTestMain(m,
			goleak.IgnoreCurrent(),
			goleak.IgnoreTopFunction("github.com/pingcap/tidb/br/pkg/utils.StartExitSingleListener.func1"),
			goleak.IgnoreTopFunction("github.com/pingcap/tidb/br/pkg/utils.StartDynamicPProfListener.func1"),
			goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
			goleak.IgnoreTopFunction("go.etcd.io/etcd/client/v3.waitRetryBackoff"),
			goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
			goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
			goleak.IgnoreTopFunction("google.golang.org/grpc.(*ClientConn).WaitForStateChange"),
			goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/config/retry.(*Config).createBackoffFn.newBackoffFn.func2"),
			goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.NewCallbackSerializer"),
		)
	}

	os.Exit(m.Run())
}

func TestRunMain(*testing.T) {
	var args []string
	for _, arg := range os.Args {
		switch {
		case arg == "DEVEL":
		case strings.HasPrefix(arg, "-test."):
		default:
			args = append(args, arg)
		}
	}

	waitCh := make(chan struct{}, 1)

	os.Args = args
	go func() {
		main()
		close(waitCh)
	}()

	<-waitCh
}
