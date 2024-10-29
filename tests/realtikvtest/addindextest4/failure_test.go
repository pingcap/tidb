// Copyright 2024 PingCAP, Inc.
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

package addindextest_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestAddIndexIngestRecoverPartition(t *testing.T) {
	partCnt := 0
	block := make(chan struct{})
	ExecuteBlocks(t, func() {
		store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("drop database if exists addindexlit;")
		tk.MustExec("create database addindexlit;")
		tk.MustExec("use addindexlit;")
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
		tk.MustExec(`set global tidb_enable_dist_task=off;`)
		tk.MustExec("create table t (a int primary key, b int) partition by hash(a) partitions 8;")
		tk.MustExec("insert into t values (2, 3), (3, 3), (5, 5);")
		failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/afterUpdatePartitionReorgInfo", func(job *model.Job) {
			if job.Type != model.ActionAddIndex {
				return
			}
			partCnt++
			if partCnt == 2 {
				dom.DDL().OwnerManager().ResignOwner(context.Background())
				dom.InfoSyncer().RemoveServerInfo()
				os.Exit(0) // Mock TiDB exit abnormally. We use zero because the requirement of `ExecuteBlocks()`.
			}
		})
		tk.MustExec("alter table t add index idx(b);")
	}, func() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.Port += 1
		})
		var (
			store kv.Storage
			dom   *domain.Domain
		)
		failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/afterUpdatePartitionReorgInfo", func(job *model.Job) {
			if job.Type != model.ActionAddIndex {
				return
			}
			partCnt++
			if partCnt == 2 {
				dom.DDL().OwnerManager().ResignOwner(context.Background())
				dom.InfoSyncer().RemoveServerInfo()
				os.Exit(0)
			}
		})
		realtikvtest.RetainOldData = true
		store, dom = realtikvtest.CreateMockStoreAndDomainAndSetup(t)
		realtikvtest.RetainOldData = false
		tk := testkit.NewTestKit(t, store)
		tk.MustQuery("select 1;").Check(testkit.Rows("1"))
		<-block // block forever until os.Exit(0).
	}, func() {
		failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/afterFinishDDLJob", func(job *model.Job) {
			if job.Type != model.ActionAddIndex {
				return
			}
			block <- struct{}{}
		})
		config.UpdateGlobal(func(conf *config.Config) {
			conf.Port += 2
		})
		realtikvtest.RetainOldData = true
		store := realtikvtest.CreateMockStoreAndSetup(t)
		realtikvtest.RetainOldData = false
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use addindexlit;")
		<-block
		tk.MustExec("admin check table t;")
	})
}

func ExecuteBlocks(t *testing.T, fns ...func()) {
	curStep := 0
	env := os.Getenv("TIDB_TEST_STEP")
	if len(env) > 0 {
		i, err := strconv.Atoi(env)
		require.NoError(t, err)
		curStep = i
	}
	if curStep >= len(fns) {
		return
	}
	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("TIDB_TEST_STEP=%d", curStep+1))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	require.NoError(t, err)
	t.Logf("Execute block %d", len(fns)-1-curStep)
	fns[len(fns)-1-curStep]()
}
