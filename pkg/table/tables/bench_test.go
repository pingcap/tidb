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

package tables_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	_ "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

func BenchmarkAddRecordInPipelinedDML(b *testing.B) {
	store, dom := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)
	_, err := tk.Session().Execute(
		context.Background(),
		"CREATE TABLE test.t (a int primary key auto_increment, b varchar(255))",
	)
	require.NoError(b, err)
	ctx := tk.Session()
	vars := ctx.GetSessionVars()
	vars.BulkDMLEnabled = true
	vars.TxnCtx.EnableMDL = true
	vars.StmtCtx.InInsertStmt = true
	require.Nil(b, sessiontxn.NewTxn(context.Background(), tk.Session()))
	txn, _ := ctx.Txn(true)
	require.True(b, txn.IsPipelined())
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := tb.AddRecord(ctx.GetTableCtx(), types.MakeDatums(i, "test"))
		if err != nil {
			b.Fatal(err)
		}
	}
}
