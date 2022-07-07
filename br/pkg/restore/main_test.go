// Copyright 2021 PingCAP, Inc.
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

package restore_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/testkit/testsetup"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/klauspost/compress/zstd.(*blockDec).startDecoder"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}

	var err error
	mc, err = mock.NewCluster()
	if err != nil {
		panic(err)
	}
	err = mc.Start()
	if err != nil {
		panic(err)
	}
	exitCode := m.Run()
	mc.Stop()
	if exitCode == 0 {
		if err := goleak.Find(opts...); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}
