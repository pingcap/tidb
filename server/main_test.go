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

package server

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/testbridge"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()

	runInGoTest = true // flag for NewServer to known it is running in test environment

	// AsyncCommit will make DDL wait 2.5s before changing to the next state.
	// Set schema lease to avoid it from making CI slow.
	session.SetSchemaLease(0)

	tikv.EnableFailpoints()

	metrics.RegisterMetrics()

	// sanity check: the global config should not be changed by other pkg init function.
	// see also https://github.com/pingcap/tidb/issues/22162
	defaultConfig := config.NewConfig()
	globalConfig := config.GetGlobalConfig()
	if !reflect.DeepEqual(defaultConfig, globalConfig) {
		_, _ = fmt.Fprintf(os.Stderr, "server: the global config has been changed.\n")
		_, _ = fmt.Fprintf(os.Stderr, "default: %#v\nglobal: %#v", defaultConfig, globalConfig)
	}

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("time.Sleep"),
		goleak.IgnoreTopFunction("database/sql.(*Tx).awaitDone"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/server.NewServer.func1"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("github.com/go-sql-driver/mysql.(*mysqlConn).startWatcher.func1"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/util/topsql/tracecpu.(*sqlCPUProfiler).startAnalyzeProfileWorker"),
	}

	goleak.VerifyTestMain(m, opts...)
}
