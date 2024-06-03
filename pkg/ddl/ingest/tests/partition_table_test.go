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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

func TestAddIndexIngestRecoverPartition(t *testing.T) {
	// TODO we are unregistering LitBackCtxMgr when owner changes, but another owner
	// might access it, so this case is unstable by nature.
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
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeUpdateReorgInfo-addTableIndex",
		func(job *model.Job) {
			partCnt++
			if partCnt == 3 || partCnt == 6 {
				tc.TriggerOwnerChange()
				// TODO(tangenta): mock multiple backends in a better way.
				//nolint: forcetypeassert
				// TODO(tangenta): When owner changes, wait last ddl owner's DDL scheduling loop exits.
				ingest.LitBackCtxMgr.(*ingest.MockBackendCtxMgr).ResetSessCtx()
				bc, _ := ingest.LitBackCtxMgr.Load(job.ID)
				bc.GetCheckpointManager().Close()
				bc.AttachCheckpointManager(nil)
				config.GetGlobalConfig().Port = port + 1
			}
		},
	)
	tk.MustExec("alter table t add index idx(b);")
	tk.MustExec("admin check table t;")
}
