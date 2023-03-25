// Copyright 2023 PingCAP, Inc.
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

package loaddatatest

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/testkit"
	"github.com/tikv/client-go/v2/tikv"
	"go.opencensus.io/stats/view"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	autoid.SetStep(5000)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.SlowThreshold = 30000 // 30s
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Experimental.AllowsExpressionIndex = true
	})
	tikv.EnableFailpoints()

	opts := []goleak.Option{
		goleak.Cleanup(func(_ int) {
			view.Stop()
		}),
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
	}

	goleak.VerifyTestMain(m, opts...)
}

func fillData(tk *testkit.TestKit, table string) {
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("create table %s(id int not null default 1, name varchar(255), PRIMARY KEY(id));", table))

	// insert data
	tk.MustExec(fmt.Sprintf("insert INTO %s VALUES (1, \"hello\");", table))
	tk.MustExec(fmt.Sprintf("insert into %s values (2, \"hello\");", table))
}
