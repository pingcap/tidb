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

package ddltest

import (
	"fmt"
	"os"
	"testing"

	zaplog "github.com/pingcap/log"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testbridge"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.SetupForCommonTest()
	err := logutil.InitLogger(&logutil.LogConfig{Config: zaplog.Config{Level: *logLevel}})
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("github.com/go-sql-driver/mysql.(*mysqlConn).startWatcher.func1"),
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
	}
	goleak.VerifyTestMain(m, opts...)
}
