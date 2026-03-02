// Copyright 2018 PingCAP, Inc.
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

package sessiontest

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestExecuteInternalShouldHaveRUStats(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("This test only runs with real TiKV")
	}

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("create table t(id int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	// The `intest.Assert` in `GetExecDetailsFromContext` will panic if the context does not have ru details.
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	rs, err := tk.Session().ExecuteInternal(ctx, "select * from t")
	require.NoError(t, err)
	_, err = sqlexec.DrainRecordSet(ctx, rs, 8)
	require.NoError(t, err)
}
