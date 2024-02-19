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

package expression

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	testmain.ShortCircuitForBench(m)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Experimental.AllowsExpressionIndex = true
	})
	tikv.EnableFailpoints()

	// Some test depends on the values of timeutil.SystemLocation()
	// If we don't SetSystemTZ() here, the value would change unpredictable.
	// Affected by the order whether a testsuite runs before or after integration test.
	// Note, SetSystemTZ() is a sync.Once operation.
	timeutil.SetSystemTZ("system")

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}

	goleak.VerifyTestMain(m, opts...)
}

func createContext(t *testing.T) *mock.Context {
	ctx := mock.NewContext()
	sqlMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)
	require.True(t, sqlMode.HasStrictMode())
	ctx.GetSessionVars().SQLMode = sqlMode
	// sets default time zone to UTC+11 value to make it different with most CI and development environments and forbid
	// some tests are success in some environments but failed in some others.
	tz := time.FixedZone("UTC+11", 11*3600)
	ctx.ResetSessionAndStmtTimeZone(tz)
	sc := ctx.GetSessionVars().StmtCtx
	sc.SetTypeFlags(sc.TypeFlags().WithTruncateAsWarning(true))
	require.NoError(t, ctx.GetSessionVars().SetSystemVar("max_allowed_packet", "67108864"))
	ctx.GetSessionVars().PlanColumnID.Store(0)
	return ctx
}
