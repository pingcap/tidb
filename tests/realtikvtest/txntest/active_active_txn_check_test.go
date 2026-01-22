// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txntest

import (
	"errors"
	"testing"

	txnerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestWaitLaggingTSO(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, v int) softdelete retention 1 hour active_active='ON'")
	// insert a very large _tidb_origin_ts to make sure wait TSO can not succeed
	tk.MustExec("insert into t(id, v, _tidb_origin_ts) values(1, 1, 993751704056430613)")

	for _, asyncCommit := range []string{"ON", "OFF"} {
		t.Run("async_commit: "+asyncCommit, func(t *testing.T) {
			tk.MustExec("set @@tidb_enable_async_commit = ?", asyncCommit)
			tk.MustExec("set @@tidb_enable_1pc = ?", asyncCommit)

			// test explicit transaction
			tk.MustExec("begin")
			tk.MustExec("update t set v = v + 1 where id = 1")
			err := tk.ExecToErr("commit")
			require.True(t, errors.Is(err, txnerr.ErrPDTimestampLagsTooMuch))

			// test autocommit
			// If async commit is enabled, the autocommit transaction casual consistency.
			// see https://github.com/tikv/client-go/pull/1847 for causal consistency issue.
			err = tk.ExecToErr("update t set v = v + 1 where id = 1")
			require.True(t, errors.Is(err, txnerr.ErrPDTimestampLagsTooMuch))
		})
	}
}
