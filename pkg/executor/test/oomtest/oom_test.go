// Copyright 2020 PingCAP, Inc.
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

// Note: All the tests in this file will be executed sequentially.

package oomtest

import (
	"os"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	registerHook()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestMemTracker4UpdateExec(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_MemTracker4UpdateExec (id int, a int, b int, index idx_a(`a`))")

	log.SetLevel(zap.InfoLevel)

	oom.SetTracker("")
	oom.ClearMessageFilter()
	oom.AddMessageFilter("expensive_query during bootstrap phase")

	tk.MustExec("insert into t_MemTracker4UpdateExec values (1,1,1), (2,2,2), (3,3,3)")

	tk.Session().GetSessionVars().MemQuotaQuery = 244
	tk.MustExec("update t_MemTracker4UpdateExec set a = 4")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
}

func TestMemTracker4InsertAndReplaceExec(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, a int, b int, index idx_a(`a`))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (3,3,3)")

	tk.MustExec("create table t_MemTracker4InsertAndReplaceExec (id int, a int, b int, index idx_a(`a`))")

	log.SetLevel(zap.InfoLevel)

	oom.SetTracker("")
	oom.AddMessageFilter("expensive_query during bootstrap phase")

	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	oom.ClearMessageFilter()
	oom.AddMessageFilter("expensive_query during bootstrap phase")
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1

	oom.SetTracker("")
	oom.ClearMessageFilter()
	oom.AddMessageFilter("expensive_query during bootstrap phase")

	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "", oom.GetTracker())
	oom.AddMessageFilter("expensive_query during bootstrap phase")
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1

	oom.ClearMessageFilter()
	oom.SetTracker("")

	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec select * from t")
	require.Equal(t, "", oom.GetTracker())
	oom.AddMessageFilter("expensive_query during bootstrap phase")
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec select * from t")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1

	oom.ClearMessageFilter()
	oom.SetTracker("")

	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec select * from t")
	require.Equal(t, "", oom.GetTracker())
	oom.AddMessageFilter("expensive_query during bootstrap phase")
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec select * from t")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1

	tk.Session().GetSessionVars().DMLBatchSize = 1
	tk.Session().GetSessionVars().BatchInsert = true

	oom.ClearMessageFilter()
	oom.SetTracker("")

	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "", oom.GetTracker())
	oom.AddMessageFilter("expensive_query during bootstrap phase")
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1

	oom.ClearMessageFilter()
	oom.SetTracker("")

	oom.AddMessageFilter("expensive_query during bootstrap phase")

	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "", oom.GetTracker())
	oom.AddMessageFilter("expensive_query during bootstrap phase")
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1
}

func TestMemTracker4DeleteExec(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=1")
	tk.MustExec("create table MemTracker4DeleteExec1 (id int, a int, b int, index idx_a(`a`))")
	tk.MustExec("create table MemTracker4DeleteExec2 (id int, a int, b int, index idx_a(`a`))")

	// delete from single table
	log.SetLevel(zap.InfoLevel)
	tk.MustExec("insert into MemTracker4DeleteExec1 values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")

	oom.SetTracker("")
	oom.ClearMessageFilter()
	oom.AddMessageFilter("expensive_query during bootstrap phase")

	tk.MustExec("delete from MemTracker4DeleteExec1")
	require.Equal(t, "", oom.GetTracker())
	tk.MustExec("insert into MemTracker4DeleteExec1 values (1,1,1), (2,2,2), (3,3,3)")
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.Session().GetSessionVars().MemTracker.SetBytesLimit(1)
	tk.MustExec("delete from MemTracker4DeleteExec1")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())

	// delete from multiple table
	tk.Session().GetSessionVars().MemQuotaQuery = 100000
	tk.MustExec("insert into MemTracker4DeleteExec1 values(1,1,1)")
	tk.MustExec("insert into MemTracker4DeleteExec2 values(1,1,1)")

	oom.SetTracker("")

	tk.MustExec("delete MemTracker4DeleteExec1, MemTracker4DeleteExec2 from MemTracker4DeleteExec1 join MemTracker4DeleteExec2 on MemTracker4DeleteExec1.a=MemTracker4DeleteExec2.a")
	require.Equal(t, "", oom.GetTracker())
	tk.MustExec("insert into MemTracker4DeleteExec1 values(1,1,1)")
	tk.MustExec("insert into MemTracker4DeleteExec2 values(1,1,1)")
	oom.ClearMessageFilter()
	oom.SetTracker("")
	oom.AddMessageFilter("memory exceeds quota, rateLimitAction delegate to fallback action")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/disableFixedRowCountHint", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/disableFixedRowCountHint"))
	}()
	tk.Session().GetSessionVars().EnabledRateLimitAction = true
	tk.Session().GetSessionVars().MemQuotaQuery = 10000
	tk.MustExec("delete MemTracker4DeleteExec1, MemTracker4DeleteExec2 from MemTracker4DeleteExec1 join MemTracker4DeleteExec2 on MemTracker4DeleteExec1.a=MemTracker4DeleteExec2.a")
	require.Equal(t, "memory exceeds quota, rateLimitAction delegate to fallback action", oom.GetTracker())
}

var oom *oomCapture

func registerHook() {
	conf := &log.Config{Level: os.Getenv("log_level"), File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	oom = &oomCapture{r.Core, "", syncutil.Mutex{}, set.NewStringSet()}
	lg := zap.New(oom)
	log.ReplaceGlobals(lg, r)
}

type oomCapture struct {
	zapcore.Core
	tracker       string
	mu            syncutil.Mutex
	messageFilter set.StringSet
}

func (h *oomCapture) AddMessageFilter(vals ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, val := range vals {
		h.messageFilter.Insert(val)
	}
}

func (h *oomCapture) ClearMessageFilter() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messageFilter.Clear()
}

func (h *oomCapture) SetTracker(tracker string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tracker = tracker
}

func (h *oomCapture) GetTracker() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.tracker
}

func (h *oomCapture) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	if entry.Message == "memory exceeds quota" {
		err, _ := fields[0].Interface.(error)
		str := err.Error()
		begin := strings.Index(str, "8001]")
		if begin == -1 {
			panic("begin not found")
		}
		end := strings.Index(str, " holds")
		if end == -1 {
			panic("end not found")
		}
		h.mu.Lock()
		h.tracker = str[begin+len("8001]") : end]
		h.mu.Unlock()
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	// They are just common background task and not related to the oom.
	if !h.messageFilter.Empty() && !h.messageFilter.Exist(entry.Message) {
		return nil
	}
	h.tracker = entry.Message
	return nil
}

func (h *oomCapture) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if h.Enabled(e.Level) {
		return ce.AddCore(e, h)
	}
	return ce
}
