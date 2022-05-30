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
	"sync"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	registerHook()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestMemTracker4UpdateExec(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_MemTracker4UpdateExec (id int, a int, b int, index idx_a(`a`))")

	log.SetLevel(zap.InfoLevel)

	oom.SetTracker("")

	tk.MustExec("insert into t_MemTracker4UpdateExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "schemaLeaseChecker is not set for this transaction", oom.GetTracker())

	tk.Session().GetSessionVars().MemQuotaQuery = 244
	tk.MustExec("update t_MemTracker4UpdateExec set a = 4")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
}

func TestMemTracker4InsertAndReplaceExec(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, a int, b int, index idx_a(`a`))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (3,3,3)")

	tk.MustExec("create table t_MemTracker4InsertAndReplaceExec (id int, a int, b int, index idx_a(`a`))")

	log.SetLevel(zap.InfoLevel)

	oom.SetTracker("")

	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "schemaLeaseChecker is not set for this transaction", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1

	oom.SetTracker("")

	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1

	oom.SetTracker("")

	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec select * from t")
	require.Equal(t, "", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec select * from t")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1

	oom.SetTracker("")

	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec select * from t")
	require.Equal(t, "", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec select * from t")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1

	tk.Session().GetSessionVars().DMLBatchSize = 1
	tk.Session().GetSessionVars().BatchInsert = true

	oom.SetTracker("")

	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1

	oom.SetTracker("")

	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	require.Equal(t, "expensive_query during bootstrap phase", oom.GetTracker())
	tk.Session().GetSessionVars().MemQuotaQuery = -1
}

func TestMemTracker4DeleteExec(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table MemTracker4DeleteExec1 (id int, a int, b int, index idx_a(`a`))")
	tk.MustExec("create table MemTracker4DeleteExec2 (id int, a int, b int, index idx_a(`a`))")

	// delete from single table
	log.SetLevel(zap.InfoLevel)
	tk.MustExec("insert into MemTracker4DeleteExec1 values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")

	oom.SetTracker("")

	tk.MustExec("delete from MemTracker4DeleteExec1")
	require.Equal(t, "", oom.GetTracker())
	tk.MustExec("insert into MemTracker4DeleteExec1 values (1,1,1), (2,2,2), (3,3,3)")
	tk.Session().GetSessionVars().MemQuotaQuery = 1
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

	oom.SetTracker("")

	tk.Session().GetSessionVars().MemQuotaQuery = 10000
	tk.MustExec("delete MemTracker4DeleteExec1, MemTracker4DeleteExec2 from MemTracker4DeleteExec1 join MemTracker4DeleteExec2 on MemTracker4DeleteExec1.a=MemTracker4DeleteExec2.a")
	require.Equal(t, "memory exceeds quota, rateLimitAction delegate to fallback action", oom.GetTracker())
}

var oom *oomCapture

func registerHook() {
	conf := &log.Config{Level: os.Getenv("log_level"), File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	oom = &oomCapture{r.Core, "", sync.Mutex{}}
	lg := zap.New(oom)
	log.ReplaceGlobals(lg, r)
}

type oomCapture struct {
	zapcore.Core
	tracker string
	mu      sync.Mutex
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
		h.tracker = str[begin+len("8001]") : end]
		return nil
	}

	h.mu.Lock()
	h.tracker = entry.Message
	h.mu.Unlock()
	return nil
}

func (h *oomCapture) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if h.Enabled(e.Level) {
		return ce.AddCore(e, h)
	}
	return ce
}
