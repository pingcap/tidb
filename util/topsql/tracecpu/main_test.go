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

package tracecpu_test

import (
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/cpuprofile"
	"github.com/pingcap/tidb/util/testbridge"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/pingcap/tidb/util/topsql/tracecpu/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()

	variable.TopSQLVariable.Enable.Store(false)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = "mock"
	})
	variable.TopSQLVariable.PrecisionSeconds.Store(1)
	tracecpu.GlobalSQLCPUCollector.Run()
	cpuprofile.GlobalCPUProfiler.Start()

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("time.Sleep"),
		goleak.IgnoreTopFunction("runtime/pprof.readProfile"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/util/topsql/tracecpu.(*sqlCPUCollector).startAnalyzeProfileWorker"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/util/cpuprofile.(*ParallelCPUProfiler).profilingLoop"),
	}

	goleak.VerifyTestMain(m, opts...)
}

func TestPProfCPUProfile(t *testing.T) {
	collector := mock.NewTopSQLCollector()
	tracecpu.GlobalSQLCPUCollector.SetCollector(collector)
	collector.WaitCollectCnt(1)
	require.True(t, collector.CollectCnt() >= 1)

	tracecpu.GlobalSQLCPUCollector.ResetCollector()
	require.True(t, tracecpu.GlobalSQLCPUCollector.GetCollector() == nil)
}
