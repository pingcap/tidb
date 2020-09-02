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
// See the License for the specific language governing permissions and
// limitations under the License.

// Note: All the tests in this file will be executed sequentially.

package oomtest

import (
	"os"
	"strings"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ = SerialSuites(&testOOMSuite{})

type testOOMSuite struct {
	store kv.Storage
	do    *domain.Domain
	oom   *oomCapturer
}

func (s *testOOMSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.registerHook()
	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	domain.RunAutoAnalyze = false
	s.do, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionLog
	})
}

func (s *testOOMSuite) TearDownSuite(c *C) {
	s.do.Close()
	s.store.Close()
}

func (s *testOOMSuite) registerHook() {
	conf := &log.Config{Level: os.Getenv("log_level"), File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	s.oom = &oomCapturer{r.Core, "", sync.Mutex{}}
	lg := zap.New(s.oom)
	log.ReplaceGlobals(lg, r)
}

func (s *testOOMSuite) TestMemTracker4UpdateExec(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t_MemTracker4UpdateExec (id int, a int, b int, index idx_a(`a`))")

	log.SetLevel(zap.InfoLevel)
	s.oom.tracker = ""
	tk.MustExec("insert into t_MemTracker4UpdateExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetSessionVars().MemQuotaQuery = 244
	tk.MustExec("update t_MemTracker4UpdateExec set a = 4")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
}

func (s *testOOMSuite) TestMemTracker4InsertAndReplaceExec(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, a int, b int, index idx_a(`a`))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (3,3,3)")

	tk.MustExec("create table t_MemTracker4InsertAndReplaceExec (id int, a int, b int, index idx_a(`a`))")

	log.SetLevel(zap.InfoLevel)
	s.oom.tracker = ""
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetSessionVars().MemQuotaQuery = -1

	s.oom.tracker = ""
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetSessionVars().MemQuotaQuery = -1

	s.oom.tracker = ""
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec select * from t")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec select * from t")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetSessionVars().MemQuotaQuery = -1

	s.oom.tracker = ""
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec select * from t")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec select * from t")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetSessionVars().MemQuotaQuery = -1

	tk.Se.GetSessionVars().DMLBatchSize = 1
	tk.Se.GetSessionVars().BatchInsert = true
	s.oom.tracker = ""
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetSessionVars().MemQuotaQuery = -1

	s.oom.tracker = ""
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetSessionVars().MemQuotaQuery = -1
}

func (s *testOOMSuite) TestMemTracker4DeleteExec(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table MemTracker4DeleteExec1 (id int, a int, b int, index idx_a(`a`))")
	tk.MustExec("create table MemTracker4DeleteExec2 (id int, a int, b int, index idx_a(`a`))")

	// delete from single table
	log.SetLevel(zap.InfoLevel)
	tk.MustExec("insert into MemTracker4DeleteExec1 values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")
	s.oom.tracker = ""
	tk.MustExec("delete from MemTracker4DeleteExec1")
	c.Assert(s.oom.tracker, Equals, "")
	tk.MustExec("insert into MemTracker4DeleteExec1 values (1,1,1), (2,2,2), (3,3,3)")
	tk.Se.GetSessionVars().MemQuotaQuery = 1
	tk.MustExec("delete from MemTracker4DeleteExec1")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")

	// delete from multiple table
	tk.Se.GetSessionVars().MemQuotaQuery = 100000
	tk.MustExec("insert into MemTracker4DeleteExec1 values(1,1,1)")
	tk.MustExec("insert into MemTracker4DeleteExec2 values(1,1,1)")
	s.oom.tracker = ""
	tk.MustExec("delete MemTracker4DeleteExec1, MemTracker4DeleteExec2 from MemTracker4DeleteExec1 join MemTracker4DeleteExec2 on MemTracker4DeleteExec1.a=MemTracker4DeleteExec2.a")
	c.Assert(s.oom.tracker, Equals, "")
	tk.MustExec("insert into MemTracker4DeleteExec1 values(1,1,1)")
	tk.MustExec("insert into MemTracker4DeleteExec2 values(1,1,1)")
	s.oom.tracker = ""
	tk.Se.GetSessionVars().MemQuotaQuery = 10000
	tk.MustExec("delete MemTracker4DeleteExec1, MemTracker4DeleteExec2 from MemTracker4DeleteExec1 join MemTracker4DeleteExec2 on MemTracker4DeleteExec1.a=MemTracker4DeleteExec2.a")
	c.Assert(s.oom.tracker, Equals, "expensive_query during bootstrap phase")
}

type oomCapturer struct {
	zapcore.Core
	tracker string
	mu      sync.Mutex
}

func (h *oomCapturer) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	if strings.Contains(entry.Message, "memory exceeds quota") {
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

func (h *oomCapturer) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if h.Enabled(e.Level) {
		return ce.AddCore(e, h)
	}
	return ce
}
