// Copyright 2025 PingCAP, Inc.
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

package addindextest

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
)

func TestAlterThreadRightAfterJobFinish(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_dist_task=0;")
	tk.MustExec("create table t (c1 int primary key, c2 int)")
	tk.MustExec("insert t values (1, 1), (2, 2), (3, 3);")
	var updated bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkJobCancelled", func(job *model.Job) {
		if !updated && job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization {
			updated = true
			fmt.Println("TEST-LOG: set thread=1")
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec(fmt.Sprintf("admin alter ddl jobs %d thread = 1", job.ID))
		}
	})
	var pipeClosed atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterPipeLineClose", func() {
		fmt.Println("TEST-LOG: start sleep")
		pipeClosed.Store(true)
		time.Sleep(5 * time.Second)
		fmt.Println("TEST-LOG: end sleep")
	})
	var onUpdateJobParam bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onUpdateJobParam", func() {
		if !onUpdateJobParam {
			onUpdateJobParam = true
			for !pipeClosed.Load() {
				time.Sleep(100 * time.Millisecond)
			}
			fmt.Println("TEST-LOG: proceed update param")
		}
	})
	tk.MustExec("alter table t add index idx(c2)")
}
