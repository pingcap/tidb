// Copyright 2022 PingCAP, Inc.
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

package ddl_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type testCancelJob struct {
	sql         string
	ok          bool
	cancelState model.SchemaState
	onJobBefore bool
	onJobUpdate bool
	prepareSQL  []string
}

var allTestCase = []testCancelJob{
	{"create unique index c3_index on t_partition (c1)", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t_partition add primary key c3_index (c1);", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t add primary key idx_c2 (c2);", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t add unique index idx_c2 (c2);", true, model.StateWriteReorganization, true, true, nil},
}

func cancelSuccess(rs *testkit.Result) bool {
	return strings.Contains(rs.Rows()[0][1].(string), "success")
}

func TestCancelPartitionTable(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 100*time.Millisecond)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tkCancel := testkit.NewTestKit(t, store)

	// Prepare schema.
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_partition;")
	tk.MustExec(`create table t_partition (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
    	partition p0 values less than (1024),
    	partition p1 values less than (2048),
    	partition p2 values less than (3072),
    	partition p3 values less than (4096),
		partition p4 values less than (maxvalue)
   	);`)
	tk.MustExec(`create table t (
		c1 int, c2 int, c3 int
	);`)

	// Prepare data.
	for i := 0; i <= 2048; i++ {
		tk.MustExec(fmt.Sprintf("insert into t_partition values(%d, %d, %d)", i*3, i*2, i))
		tk.MustExec(fmt.Sprintf("insert into t values(%d, %d, %d)", i*3, i*2, i))
	}

	// Change some configurations.
	ddl.ReorgWaitTimeout = 10 * time.Microsecond
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 8")

	hook := &ddl.TestDDLCallback{Do: dom}
	i := 0
	cancel := false

	hookFunc := func(job *model.Job) {
		if job.SchemaState == allTestCase[i].cancelState && !cancel {
			if job.SchemaState == model.StateWriteReorganization && (job.RowCount == 0) {
				return
			}
			rs := tkCancel.MustQuery(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
			require.Equal(t, allTestCase[i].ok, cancelSuccess(rs))
			cancel = true
		}
	}
	dom.DDL().SetHook(hook)

	restHook := func(h *ddl.TestDDLCallback) {
		h.OnJobRunBeforeExported = nil
		h.OnJobUpdatedExported = nil
	}
	registHook := func(h *ddl.TestDDLCallback, onJobRunBefore bool) {
		if onJobRunBefore {
			h.OnJobRunBeforeExported = hookFunc
		} else {
			h.OnJobUpdatedExported = hookFunc
		}
	}

	for j, tc := range allTestCase {
		i = j
		if tc.onJobBefore {
			for _, prepareSQL := range tc.prepareSQL {
				tk.MustExec(prepareSQL)
			}

			cancel = false
			restHook(hook)
			registHook(hook, true)
			logutil.BgLogger().Info("test case", zap.Int("", i))
			if tc.ok {
				tk.MustGetErrCode(tc.sql, errno.ErrCancelledDDLJob)
			} else {
				tk.MustExec(tc.sql)
			}
		}
		if tc.onJobUpdate {
			for _, prepareSQL := range tc.prepareSQL {
				tk.MustExec(prepareSQL)
			}

			cancel = false
			restHook(hook)
			registHook(hook, false)
			logutil.BgLogger().Info("test case", zap.Int("", i))
			if tc.ok {
				tk.MustGetErrCode(tc.sql, errno.ErrCancelledDDLJob)
			} else {
				tk.MustExec(tc.sql)
			}
		}
	}
}
