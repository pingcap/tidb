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

	"github.com/stretchr/testify/require"
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

func TestCalculateMemoryLimit(t *testing.T) {
	// f(0 Byte) = 0 Byte
	require.Equal(t, uint64(0), calculateMemoryLimit(0))
	// f(100 KB) = 87.5 KB
	require.Equal(t, uint64(89600), calculateMemoryLimit(100*1024))
	// f(100 MB) = 87.5 MB
	require.Equal(t, uint64(91763188), calculateMemoryLimit(100*1024*1024))
	// f(3.99 GB) = 3.74 GB
	require.Equal(t, uint64(4026531839), calculateMemoryLimit(4*1024*1024*1024-1))
	// f(4 GB) = 3.5 GB
	require.Equal(t, uint64(3758096384), calculateMemoryLimit(4*1024*1024*1024))
	// f(32 GB) = 31.5 GB
	require.Equal(t, uint64(33822867456), calculateMemoryLimit(32*1024*1024*1024))
}
