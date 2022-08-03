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

import (
	"os"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

var isCoverageServer string

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

// TestRunMain is a dummy test case, which contains only the main function of tidb-server,
// and it is used to generate coverage_server.
func TestRunMain(t *testing.T) {
	if isCoverageServer == "1" {
		main()
	}
}

func TestSetGlobalVars(t *testing.T) {
	require.Equal(t, "tikv,tiflash,tidb", variable.GetSysVar(variable.TiDBIsolationReadEngines).Value)
	require.Equal(t, "1073741824", variable.GetSysVar(variable.TiDBMemQuotaQuery).Value)
	require.NotEqual(t, "test", variable.GetSysVar(variable.Version).Value)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.IsolationRead.Engines = []string{"tikv", "tidb"}
		conf.ServerVersion = "test"
	})
	setGlobalVars()

	require.Equal(t, "tikv,tidb", variable.GetSysVar(variable.TiDBIsolationReadEngines).Value)
	require.Equal(t, "test", variable.GetSysVar(variable.Version).Value)
	require.Equal(t, variable.GetSysVar(variable.Version).Value, mysql.ServerVersion)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.ServerVersion = ""
	})
	setGlobalVars()

	// variable.Version won't change when len(conf.ServerVersion) == 0
	require.Equal(t, "test", variable.GetSysVar(variable.Version).Value)
	require.Equal(t, variable.GetSysVar(variable.Version).Value, mysql.ServerVersion)

	cfg := config.GetGlobalConfig()
	require.Equal(t, cfg.Socket, variable.GetSysVar(variable.Socket).Value)

	if hostname, err := os.Hostname(); err == nil {
		require.Equal(t, variable.GetSysVar(variable.Hostname).Value, hostname)
	}
}
