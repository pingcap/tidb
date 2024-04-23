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
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestAddIndexIngestRecoverPartition(t *testing.T) {
	port := config.GetGlobalConfig().Port
	tc := testkit.NewDistExecutionContext(t, 3)
	defer tc.Close()
	defer ingesttestutil.InjectMockBackendMgr(t, tc.Store)()
	tk := testkit.NewTestKit(t, tc.Store)
	tk.MustExec("use test;")
	tk.MustExec("set global tidb_enable_dist_task = 0")
	tk.MustExec("create table t (a int primary key, b int) partition by hash(a) partitions 8;")
	tk.MustExec("insert into t values (2, 3), (3, 3), (5, 5);")

	partCnt := 0
	changeOwner0To1 := func(job *model.Job, _ int64) {
		partCnt++
		if partCnt == 3 {
			tc.SetOwner(1)
			// TODO(tangenta): mock multiple backends in a better way.
			//nolint: forcetypeassert
			// TODO(tangenta): When owner changes, wait last ddl owner's DDL scheduling loop exits.
			ingest.LitBackCtxMgr.(*ingest.MockBackendCtxMgr).ResetSessCtx()
			bc, _ := ingest.LitBackCtxMgr.Load(job.ID)
			bc.GetCheckpointManager().Close()
			bc.AttachCheckpointManager(nil)
			config.GetGlobalConfig().Port = port + 1
		}
	}
	changeOwner1To2 := func(job *model.Job, _ int64) {
		partCnt++
		if partCnt == 6 {
			tc.SetOwner(2)
			//nolint: forcetypeassert
			ingest.LitBackCtxMgr.(*ingest.MockBackendCtxMgr).ResetSessCtx()
			bc, _ := ingest.LitBackCtxMgr.Load(job.ID)
			bc.GetCheckpointManager().Close()
			bc.AttachCheckpointManager(nil)
			config.GetGlobalConfig().Port = port + 2
		}
	}
	tc.SetOwner(0)
	hook0 := &callback.TestDDLCallback{}
	hook0.OnUpdateReorgInfoExported = changeOwner0To1
	hook1 := &callback.TestDDLCallback{}
	hook1.OnUpdateReorgInfoExported = changeOwner1To2
	tc.GetDomain(0).DDL().SetHook(hook0)
	tc.GetDomain(1).DDL().SetHook(hook1)
	tk.MustExec("alter table t add index idx(b);")
	tk.MustExec("admin check table t;")
}
