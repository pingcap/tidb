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

package tests

import (
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/goleak"
)

type statusAPIAuditRecord struct {
	tp           extension.StmtEventTp
	sessionAlias string
	sql          string
	err          string
}

type statusAPIAuditRecorder struct {
	mu      sync.Mutex
	records []statusAPIAuditRecord
}

func (r *statusAPIAuditRecorder) onStmtEvent(tp extension.StmtEventTp, info extension.StmtEventInfo) {
	if info.SessionAlias() != "status-api" {
		return
	}
	rec := statusAPIAuditRecord{
		tp:           tp,
		sessionAlias: info.SessionAlias(),
		sql:          info.OriginalText(),
	}
	if err := info.GetError(); err != nil {
		rec.err = err.Error()
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
}

func (r *statusAPIAuditRecorder) sessionHandler() *extension.SessionHandler {
	return &extension.SessionHandler{
		OnStmtEvent: r.onStmtEvent,
	}
}

func (r *statusAPIAuditRecorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = nil
}

func (r *statusAPIAuditRecorder) Records() []statusAPIAuditRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]statusAPIAuditRecord, len(r.records))
	copy(out, r.records)
	return out
}

var statusAPIAuditTestRecorder = &statusAPIAuditRecorder{}

func TestMain(m *testing.M) {
	server.RunInGoTest = true
	server.RunInGoTestChan = make(chan struct{})
	testsetup.SetupForCommonTest()
	topsqlstate.EnableTopSQL()
	unistore.CheckResourceTagForTopSQLInGoTest = true

	tikv.EnableFailpoints()

	if err := extension.Register(
		"status_api_audit_test",
		extension.WithSessionHandlerFactory(statusAPIAuditTestRecorder.sessionHandler),
	); err != nil {
		panic(err)
	}

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
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("time.Sleep"),
		goleak.IgnoreTopFunction("database/sql.(*Tx).awaitDone"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/pkg/server.NewServer.func1"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("github.com/go-sql-driver/mysql.(*mysqlConn).startWatcher.func1"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/test/bufconn.(*Listener).Accept"),
	}

	goleak.VerifyTestMain(m, opts...)
}
