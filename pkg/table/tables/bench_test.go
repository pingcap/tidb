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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	_ "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
)

const batchSize = 5000

func BenchmarkAddRecordInPipelinedDML(b *testing.B) {
	logutil.InitLogger(&logutil.LogConfig{Config: log.Config{Level: "fatal"}})
	store, dom := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)

	// Create the table
	_, err := tk.Session().Execute(
		context.Background(),
		"CREATE TABLE IF NOT EXISTS test.t (a int primary key auto_increment, b varchar(255))",
	)
	require.NoError(b, err)
	tb, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(b, err)

	variable.EnableMDL.Store(true)

	// Pre-create data to be inserted
	records := make([][]types.Datum, batchSize)
	for j := 0; j < batchSize; j++ {
		records[j] = types.MakeDatums(j, "test")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset environment for each batch
		b.StopTimer()

		ctx := tk.Session()
		vars := ctx.GetSessionVars()
		vars.BulkDMLEnabled = true
		vars.TxnCtx.EnableMDL = true
		vars.StmtCtx.InUpdateStmt = true
		require.Nil(b, sessiontxn.NewTxn(context.Background(), tk.Session()))
		txn, _ := ctx.Txn(true)
		require.True(b, txn.IsPipelined())

		b.StartTimer()
		for j := 0; j < batchSize; j++ {
			_, err := tb.AddRecord(ctx.GetTableCtx(), txn, records[j])
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()

		// Rollback the transaction to avoid interference between batches
		ctx.RollbackTxn(context.Background())
	}

	totalRecords := batchSize * b.N
	avgTimePerRecord := float64(b.Elapsed().Nanoseconds()) / float64(totalRecords)
	b.ReportMetric(avgTimePerRecord, "ns/record")
}

func BenchmarkRemoveRecordInPipelinedDML(b *testing.B) {
	logutil.InitLogger(&logutil.LogConfig{Config: log.Config{Level: "fatal"}})
	store, dom := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)

	// Create the table
	_, err := tk.Session().Execute(
		context.Background(),
		"CREATE TABLE IF NOT EXISTS test.t (a int primary key clustered, b varchar(255))",
	)
	require.NoError(b, err)
	tb, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(b, err)

	variable.EnableMDL.Store(true)

	// Pre-create and add initial records
	records := make([][]types.Datum, batchSize)
	for j := 0; j < batchSize; j++ {
		records[j] = types.MakeDatums(j, "test")
	}

	// Add initial records
	se := tk.Session()
	for j := 0; j < batchSize; j++ {
		tk.MustExec("INSERT INTO test.t VALUES (?, ?)", j, "test")
	}

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset environment for each batch
		vars := se.GetSessionVars()
		vars.BulkDMLEnabled = true
		vars.TxnCtx.EnableMDL = true
		vars.StmtCtx.InUpdateStmt = true
		require.Nil(b, sessiontxn.NewTxn(context.Background(), tk.Session()))
		txn, _ := se.Txn(true)
		require.True(b, txn.IsPipelined())

		b.StartTimer()
		for j := 0; j < batchSize; j++ {
			// Remove record
			handle := kv.IntHandle(j)
			err := tb.RemoveRecord(se.GetTableCtx(), txn, handle, records[j])
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()

		// Rollback the transaction to avoid interference between batches
		se.RollbackTxn(context.Background())
		require.NoError(b, err)
	}

	totalRecords := batchSize * b.N
	avgTimePerRecord := float64(b.Elapsed().Nanoseconds()) / float64(totalRecords)
	b.ReportMetric(avgTimePerRecord, "ns/record")
}

func BenchmarkUpdateRecordInPipelinedDML(b *testing.B) {
	logutil.InitLogger(&logutil.LogConfig{Config: log.Config{Level: "fatal"}})
	store, dom := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)

	// Create the table
	_, err := tk.Session().Execute(
		context.Background(),
		"CREATE TABLE IF NOT EXISTS test.t (a int primary key clustered, b varchar(255))",
	)
	require.NoError(b, err)
	tb, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(b, err)

	// Pre-create data to be inserted and then updated
	records := make([][]types.Datum, batchSize)
	for j := 0; j < batchSize; j++ {
		records[j] = types.MakeDatums(j, "test")
	}

	// Pre-create new data
	newData := make([][]types.Datum, batchSize)
	for j := 0; j < batchSize; j++ {
		newData[j] = types.MakeDatums(j, "updated")
	}

	// Add initial records
	se := tk.Session()
	for j := 0; j < batchSize; j++ {
		tk.MustExec("INSERT INTO test.t VALUES (?, ?)", j, "test")
	}

	touched := make([]bool, len(tb.Meta().Columns))
	touched[1] = true

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset environment for each batch
		vars := se.GetSessionVars()
		vars.BulkDMLEnabled = true
		vars.TxnCtx.EnableMDL = true
		vars.StmtCtx.InUpdateStmt = true
		require.Nil(b, sessiontxn.NewTxn(context.Background(), tk.Session()))
		txn, _ := se.Txn(true)
		require.True(b, txn.IsPipelined())

		b.StartTimer()
		for j := 0; j < batchSize; j++ {
			// Update record
			handle := kv.IntHandle(j)
			err := tb.UpdateRecord(se.GetTableCtx(), txn, handle, records[j], newData[j], touched, table.WithCtx(context.TODO()))
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()

		// Rollback the transaction to avoid interference between batches
		se.RollbackTxn(context.Background())
		require.NoError(b, err)
	}

	totalRecords := batchSize * b.N
	avgTimePerRecord := float64(b.Elapsed().Nanoseconds()) / float64(totalRecords)
	b.ReportMetric(avgTimePerRecord, "ns/record")
}
