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

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
	"go.uber.org/goleak"
)

var isCoverageServer string

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
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
	defer view.Stop()
	require.Equal(t, "tikv,tiflash,tidb", variable.GetSysVar(vardef.TiDBIsolationReadEngines).Value)
	require.Equal(t, "1073741824", variable.GetSysVar(vardef.TiDBMemQuotaQuery).Value)
	require.NotEqual(t, "test", variable.GetSysVar(vardef.Version).Value)

	// test setInstanceVar
	require.False(t, variable.GetSysVar(vardef.TiDBInstancePlanCacheMaxMemSize).HasInstanceScope())
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.InstancePlanCacheMaxMemSize = "444"
	})
	setGlobalVars()
	require.Equal(t, variable.GetSysVar(vardef.TiDBInstancePlanCacheMaxMemSize).Value, "444")
	require.True(t, variable.GetSysVar(vardef.TiDBInstancePlanCacheMaxMemSize).HasInstanceScope())

	config.UpdateGlobal(func(conf *config.Config) {
		conf.IsolationRead.Engines = []string{"tikv", "tidb"}
		conf.ServerVersion = "test"
	})
	setGlobalVars()

	require.Equal(t, "tikv,tidb", variable.GetSysVar(vardef.TiDBIsolationReadEngines).Value)
	require.Equal(t, "test", variable.GetSysVar(vardef.Version).Value)
	require.Equal(t, variable.GetSysVar(vardef.Version).Value, mysql.ServerVersion)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.ServerVersion = ""
	})
	setGlobalVars()

	// variable.Version won't change when len(conf.ServerVersion) == 0
	require.Equal(t, "test", variable.GetSysVar(vardef.Version).Value)
	require.Equal(t, variable.GetSysVar(vardef.Version).Value, mysql.ServerVersion)

	cfg := config.GetGlobalConfig()
	require.Equal(t, cfg.Socket, variable.GetSysVar(vardef.Socket).Value)

	if hostname, err := os.Hostname(); err == nil {
		require.Equal(t, variable.GetSysVar(vardef.Hostname).Value, hostname)
	}
}

func TestSetVersionByConfigInNextGen(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only for nextgen kernel")
	}
	cfg := config.GetGlobalConfig()
	originCfgEdition := cfg.TiDBEdition
	t.Cleanup(func() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.TiDBEdition = originCfgEdition
		})
	})

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiDBEdition = "Starter"
	})
	require.ErrorContains(t, initVersions(config.GetGlobalConfig()), "are not allowed to set in nextgen kernel")
}

func TestSetVersionByConfigInvalidNextGenReleaseVersion(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only for nextgen kernel")
	}
	originReleaseVersion := mysql.TiDBReleaseVersion
	t.Cleanup(func() {
		mysql.TiDBReleaseVersion = originReleaseVersion
	})

	mysql.TiDBReleaseVersion = "v26.13.1"
	err := initVersions(config.GetGlobalConfig())
	require.ErrorContains(t, err, "invalid tidb release version for nextgen kernel")
}

func TestSetVersionByConfigNormalizeLegacyPlaceholderForNextGen(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only for nextgen kernel")
	}

	require.NoError(t, initVersions(config.GetGlobalConfig()))
	require.Equal(t, "v26.3.0", mysql.TiDBReleaseVersion)
	require.Equal(t, "8.0.11-TiDB-X-CLOUD.202603.0", mysql.ServerVersion)
}
