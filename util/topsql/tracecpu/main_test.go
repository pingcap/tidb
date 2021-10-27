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
	"bytes"
	"testing"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testbridge"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
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
	tracecpu.GlobalSQLCPUProfiler.Run()

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("time.Sleep"),
		goleak.IgnoreTopFunction("runtime/pprof.readProfile"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/util/topsql/tracecpu.(*sqlCPUProfiler).startAnalyzeProfileWorker"),
	}

	goleak.VerifyTestMain(m, opts...)
}

func TestPProfCPUProfile(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := tracecpu.StartCPUProfile(buf)
	require.NoError(t, err)
	// enable top sql.
	variable.TopSQLVariable.Enable.Store(true)
	err = tracecpu.StopCPUProfile()
	require.NoError(t, err)
	_, err = profile.Parse(buf)
	require.NoError(t, err)
}
