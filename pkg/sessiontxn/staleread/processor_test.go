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

package staleread_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type staleReadPoint struct {
	tk *testkit.TestKit
	ts uint64
	dt string
	tm time.Time
	is infoschema.InfoSchema
	tn *ast.TableName
}

func (p *staleReadPoint) checkMatchProcessor(t *testing.T, processor staleread.Processor, hasEvaluator bool) {
	require.True(t, processor.IsStaleness())
	require.Equal(t, p.ts, processor.GetStalenessReadTS())
	require.Equal(t, p.is.SchemaMetaVersion(), processor.GetStalenessInfoSchema().SchemaMetaVersion())
	require.IsTypef(t, processor.GetStalenessInfoSchema(), temptable.AttachLocalTemporaryTableInfoSchema(p.tk.Session(), p.is), "")
	evaluator := processor.GetStalenessTSEvaluatorForPrepare()
	if hasEvaluator {
		require.NotNil(t, evaluator)
		ts, err := evaluator(context.Background(), p.tk.Session())
		require.NoError(t, err)
		require.Equal(t, processor.GetStalenessReadTS(), ts)
	} else {
		require.Nil(t, evaluator)
	}
}

func genStaleReadPoint(t *testing.T, tk *testkit.TestKit) *staleReadPoint {
	tk.MustExec("create table if not exists test.t(a bigint)")
	tk.MustExec(fmt.Sprintf("alter table test.t alter column a set default %d", time.Now().UnixNano()))
	time.Sleep(time.Millisecond * 20)
	is := domain.GetDomain(tk.Session()).InfoSchema()
	dt := tk.MustQuery("select now(3)").Rows()[0][0].(string)
	tm, err := time.ParseInLocation("2006-01-02 15:04:05.999999", dt, tk.Session().GetSessionVars().Location())
	require.NoError(t, err)
	ts := oracle.GoTimeToTS(tm)
	tn := astTableWithAsOf(t, dt)
	return &staleReadPoint{
		tk: tk,
		ts: ts,
		dt: dt,
		tm: tm,
		is: is,
		tn: tn,
	}
}

func astTableWithAsOf(t *testing.T, dt string) *ast.TableName {
	p := parser.New()
	var sql string
	if dt == "" {
		sql = "select * from test.t"
	} else {
		sql = fmt.Sprintf("select * from test.t as of timestamp '%s'", dt)
	}

	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	sel := stmt.(*ast.SelectStmt)
	return sel.From.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName)
}

func getCurrentExternalTimestamp(t *testing.T, tk *testkit.TestKit) uint64 {
	externalTimestampStr := tk.MustQuery("select @@tidb_external_ts").Rows()[0][0].(string)
	externalTimestamp, err := strconv.ParseUint(externalTimestampStr, 10, 64)
	require.NoError(t, err)

	return externalTimestamp
}

func TestStaleReadProcessorWithSelectTable(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	tn := astTableWithAsOf(t, "")
	p1 := genStaleReadPoint(t, tk)
	p2 := genStaleReadPoint(t, tk)
	ctx := context.Background()

	// create local temporary table to check processor's infoschema will consider temporary table
	tk.MustExec("create temporary table test.t2(a int)")

	// no sys variable just select ... as of ...
	processor := createProcessor(t, tk.Session())
	err := processor.OnSelectTable(p1.tn)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)
	err = processor.OnSelectTable(p1.tn)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)
	err = processor.OnSelectTable(p2.tn)
	require.Error(t, err)
	require.Equal(t, "[planner:8135]can not set different time in the as of", err.Error())
	p1.checkMatchProcessor(t, processor, true)

	// the first select has not 'as of'
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(tn)
	require.NoError(t, err)
	require.False(t, processor.IsStaleness())
	err = processor.OnSelectTable(p1.tn)
	require.Equal(t, "[planner:8135]can not set different time in the as of", err.Error())
	require.False(t, processor.IsStaleness())

	// 'as of' is not allowed when @@txn_read_ts is set
	tk.MustExec(fmt.Sprintf("SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", p1.dt))
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(p1.tn)
	require.Error(t, err)
	require.Equal(t, "[planner:8135]invalid as of timestamp: can't use select as of while already set transaction as of", err.Error())
	tk.MustExec("set @@tx_read_ts=''")

	// no 'as of' will consume @txn_read_ts
	tk.MustExec(fmt.Sprintf("SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", p1.dt))
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(tn)
	p1.checkMatchProcessor(t, processor, true)
	tk.Session().GetSessionVars().CleanupTxnReadTSIfUsed()
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
	tk.MustExec("set @@tx_read_ts=''")

	// `@@tidb_read_staleness`
	tk.MustExec("set @@tidb_read_staleness=-100")
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(tn)
	require.True(t, processor.IsStaleness())
	expectedTS, err := staleread.CalculateTsWithReadStaleness(ctx, tk.Session(), -100*time.Second)
	require.NoError(t, err)
	require.Equal(t, expectedTS, processor.GetStalenessReadTS())
	expectedIS, err := domain.GetDomain(tk.Session()).GetSnapshotInfoSchema(expectedTS)
	require.NoError(t, err)
	require.Equal(t, expectedIS.SchemaMetaVersion(), processor.GetStalenessInfoSchema().SchemaMetaVersion())
	evaluator := processor.GetStalenessTSEvaluatorForPrepare()
	evaluatorTS, err := evaluator(ctx, tk.Session())
	require.NoError(t, err)
	require.Equal(t, expectedTS, evaluatorTS)
	tk.MustExec("set @@tidb_read_staleness=''")

	tk.MustExec("do sleep(0.01)")
	evaluatorTS, err = evaluator(ctx, tk.Session())
	require.NoError(t, err)
	expectedTS2, err := staleread.CalculateTsWithReadStaleness(ctx, tk.Session(), -100*time.Second)
	require.NoError(t, err)
	require.Equal(t, expectedTS2, evaluatorTS)

	// `@@tidb_read_staleness` will be ignored when `as of` or `@@tx_read_ts`
	tk.MustExec("set @@tidb_read_staleness=-100")
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(p1.tn)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)

	tk.MustExec(fmt.Sprintf("SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", p1.dt))
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(tn)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)
	tk.MustExec("set @@tidb_read_staleness=''")

	// `@@tidb_external_ts`
	tk.MustExec("start transaction;set global tidb_external_ts=@@tidb_current_ts;commit")
	tk.MustExec("set tidb_enable_external_ts_read=ON")
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(tn)
	require.True(t, processor.IsStaleness())
	expectedTS = getCurrentExternalTimestamp(t, tk)
	require.Equal(t, expectedTS, processor.GetStalenessReadTS())
	tk.MustExec("set tidb_enable_external_ts_read=OFF")

	// `@@tidb_external_ts` will be ignored when `as of`, `@@tx_read_ts` or `@@tidb_read_staleness`
	tk.MustExec("start transaction;set global tidb_external_ts=@@tidb_current_ts;commit")
	tk.MustExec("set tidb_enable_external_ts_read=ON")
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(p1.tn)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)

	tk.MustExec(fmt.Sprintf("SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", p1.dt))
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(tn)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)

	tk.MustExec("set @@tidb_read_staleness=-5")
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(tn)
	require.True(t, processor.IsStaleness())
	expectedTS, err = staleread.CalculateTsWithReadStaleness(ctx, tk.Session(), -5*time.Second)
	require.NoError(t, err)
	require.Equal(t, expectedTS, processor.GetStalenessReadTS())
	expectedIS, err = domain.GetDomain(tk.Session()).GetSnapshotInfoSchema(expectedTS)
	require.NoError(t, err)
	require.Equal(t, expectedIS.SchemaMetaVersion(), processor.GetStalenessInfoSchema().SchemaMetaVersion())
	evaluator = processor.GetStalenessTSEvaluatorForPrepare()
	evaluatorTS, err = evaluator(ctx, tk.Session())
	require.NoError(t, err)
	require.Equal(t, expectedTS, evaluatorTS)
	tk.MustExec("set @@tidb_read_staleness=''")

	tk.MustExec("set tidb_enable_external_ts_read=OFF")
}

func TestStaleReadSupportDateTimeAndTSO(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	p1 := genStaleReadPoint(t, tk)
	p2 := genStaleReadPoint(t, tk)

	require.NotEqual(t, p1.ts, p2.ts)

	// Reading AS OF TIMESTAMP 'TSO' should be parsed correctly.
	processor := createProcessor(t, tk.Session())
	err := processor.OnSelectTable(astTableWithAsOf(t, fmt.Sprintf("%d", p1.ts)))
	require.NoError(t, err)
	require.Equal(t, processor.GetStalenessReadTS(), p1.ts)

	// Reading AS OF TIMESTAMP 'TSO' does not lose precision.
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(astTableWithAsOf(t, fmt.Sprintf("%d", p1.ts+1)))
	require.NoError(t, err)
	require.Equal(t, processor.GetStalenessReadTS(), p1.ts+1)

	// Reading AS OF TIMESTAMP 'YYYY-MM-DD HH:MM:SS' should be parsed correctly.
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(astTableWithAsOf(t, p1.dt))
	require.NoError(t, err)
	require.Equal(t, processor.GetStalenessReadTS(), p1.ts)
}

func TestStaleReadCompactDateTime(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	p1 := genStaleReadPoint(t, tk)

	// AS OF TIMESTAMP 'YYYYMMDDHHMMSS' is a valid datetime and is parsed as
	// 'YYYY-MM-DD HH:MM:SS', not as TSO. This is for backward compatibility.
	compactDatetime := p1.tm.Format("20060102150405") // Go format for YYYYMMDDHHMMSS
	// Truncate to seconds since compact format doesn't have subsecond precision
	expectedTSFromDatetime := oracle.GoTimeToTS(p1.tm.Truncate(time.Second))

	processor := createProcessor(t, tk.Session())
	err := processor.OnSelectTable(astTableWithAsOf(t, compactDatetime))
	require.NoError(t, err)
	// The timestamp should match the datetime interpretation, not the raw integer
	require.Equal(t, expectedTSFromDatetime, processor.GetStalenessReadTS())
	// Verify it's NOT treated as a raw TSO (which would be a completely different value)
	compactAsInt, err := strconv.ParseUint(compactDatetime, 10, 64)
	require.NoError(t, err)
	require.NotEqual(t, compactAsInt, processor.GetStalenessReadTS())
}

func TestStaleReadInvalidExpression(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	// Initialize the test table
	tk.MustExec("create table if not exists test.t(a bigint)")

	// Test invalid formats that can't be parsed as datetime or TSO.
	testCases := []struct {
		input       string
		expectedErr string
	}{
		{"invalid_timestamp", "cannot parse AS OF TIMESTAMP expression as datetime or TSO"},
		{"not-a-date", "cannot parse AS OF TIMESTAMP expression as datetime or TSO"},
		{"2024-13-01 00:00:00", "cannot parse AS OF TIMESTAMP expression as datetime or TSO"}, // invalid month
		{"2024-01-32 00:00:00", "cannot parse AS OF TIMESTAMP expression as datetime or TSO"}, // invalid day
		{"42", "invalid TSO timestamp: TSO is before 2013-01-01"},                             // small integer, not a valid TSO
		{"0", "invalid TSO timestamp: TSO is before 2013-01-01"},                              // zero is not a valid TSO
	}

	for _, tc := range testCases {
		processor := createProcessor(t, tk.Session())
		err := processor.OnSelectTable(astTableWithAsOf(t, tc.input))
		require.Error(t, err, "expected error for invalid format: %s", tc.input)
		require.Contains(t, err.Error(), tc.expectedErr, "unexpected error message for: %s", tc.input)
	}
}

func TestStaleReadProcessorWithExecutePreparedStmt(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	p1 := genStaleReadPoint(t, tk)
	//p2 := genStaleReadPoint(t, tk)
	ctx := context.Background()

	// create local temporary table to check processor's infoschema will consider temporary table
	tk.MustExec("create temporary table test.t2(a int)")

	// execute prepared stmt with ts evaluator
	processor := createProcessor(t, tk.Session())
	err := processor.OnExecutePreparedStmt(func(_ctx context.Context, sctx sessionctx.Context) (uint64, error) {
		return p1.ts, nil
	})
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)

	// will get an error when ts evaluator fails
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(func(_ctx context.Context, sctx sessionctx.Context) (uint64, error) {
		return 0, errors.New("mock error")
	})
	require.Error(t, err)
	require.Equal(t, "mock error", err.Error())
	require.False(t, processor.IsStaleness())

	// execute prepared stmt without stale read
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(nil)
	require.NoError(t, err)
	require.False(t, processor.IsStaleness())

	// execute prepared stmt without ts evaluator will consume tx_read_ts
	tk.MustExec(fmt.Sprintf("SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", p1.dt))
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(nil)
	p1.checkMatchProcessor(t, processor, true)
	tk.Session().GetSessionVars().CleanupTxnReadTSIfUsed()
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
	tk.MustExec("set @@tx_read_ts=''")

	// prepared ts is not allowed when @@txn_read_ts is set
	tk.MustExec(fmt.Sprintf("SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", p1.dt))
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(func(_ctx context.Context, sctx sessionctx.Context) (uint64, error) {
		return p1.ts, nil
	})
	require.Error(t, err)
	require.Equal(t, "[planner:8135]invalid as of timestamp: can't use select as of while already set transaction as of", err.Error())
	tk.MustExec("set @@tx_read_ts=''")

	// `@@tidb_read_staleness`
	tk.MustExec("set @@tidb_read_staleness=-100")
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(nil)
	require.True(t, processor.IsStaleness())
	expectedTS, err := staleread.CalculateTsWithReadStaleness(ctx, tk.Session(), -100*time.Second)
	require.NoError(t, err)
	require.Equal(t, expectedTS, processor.GetStalenessReadTS())
	expectedIS, err := domain.GetDomain(tk.Session()).GetSnapshotInfoSchema(expectedTS)
	require.NoError(t, err)
	require.Equal(t, expectedIS.SchemaMetaVersion(), processor.GetStalenessInfoSchema().SchemaMetaVersion())
	tk.MustExec("set @@tidb_read_staleness=''")

	// `@@tidb_read_staleness` will be ignored when `as of` or `@@tx_read_ts`
	tk.MustExec("set @@tidb_read_staleness=-100")
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(func(_ctx context.Context, sctx sessionctx.Context) (uint64, error) {
		return p1.ts, nil
	})
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)

	tk.MustExec(fmt.Sprintf("SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", p1.dt))
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(nil)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)
	tk.MustExec("set @@tidb_read_staleness=''")

	// `@@tidb_external_ts`
	tk.MustExec("start transaction;set global tidb_external_ts=@@tidb_current_ts;commit")
	tk.MustExec("set tidb_enable_external_ts_read=ON")
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(nil)
	require.True(t, processor.IsStaleness())
	expectedTS = getCurrentExternalTimestamp(t, tk)
	require.Equal(t, expectedTS, processor.GetStalenessReadTS())
	tk.MustExec("set tidb_enable_external_ts_read=OFF")

	// `@@tidb_external_ts` will be ignored when `as of`, `@@tx_read_ts` or `@@tidb_read_staleness`
	tk.MustExec("start transaction;set global tidb_external_ts=@@tidb_current_ts;commit")
	tk.MustExec("set tidb_enable_external_ts_read=ON")

	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(p1.tn)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)

	tk.MustExec(fmt.Sprintf("SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", p1.dt))
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(nil)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)

	tk.MustExec("set @@tidb_read_staleness=-5")
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(nil)
	require.True(t, processor.IsStaleness())
	expectedTS, err = staleread.CalculateTsWithReadStaleness(ctx, tk.Session(), -5*time.Second)
	require.NoError(t, err)
	require.Equal(t, expectedTS, processor.GetStalenessReadTS())
	expectedIS, err = domain.GetDomain(tk.Session()).GetSnapshotInfoSchema(expectedTS)
	require.NoError(t, err)
	require.Equal(t, expectedIS.SchemaMetaVersion(), processor.GetStalenessInfoSchema().SchemaMetaVersion())
	tk.MustExec("set @@tidb_read_staleness=''")

	tk.MustExec("set tidb_enable_external_ts_read=OFF")
}

func TestStaleReadProcessorInTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tn := astTableWithAsOf(t, "")
	p1 := genStaleReadPoint(t, tk)
	_ = genStaleReadPoint(t, tk)

	tk.MustExec("begin")

	// no error when there is no 'as of'
	processor := createProcessor(t, tk.Session())
	err := processor.OnSelectTable(tn)
	require.NoError(t, err)
	require.False(t, processor.IsStaleness())
	err = processor.OnSelectTable(tn)
	require.NoError(t, err)
	require.False(t, processor.IsStaleness())

	// no error when execute prepared stmt without ts evaluator
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(nil)
	require.NoError(t, err)
	require.False(t, processor.IsStaleness())

	// return an error when 'as of' is set
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(p1.tn)
	require.Error(t, err)
	require.Equal(t, "[planner:8135]invalid as of timestamp: as of timestamp can't be set in transaction.", err.Error())

	// return an error when execute prepared stmt with as of
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(func(_ctx context.Context, sctx sessionctx.Context) (uint64, error) {
		return p1.ts, nil
	})
	require.Error(t, err)
	require.Equal(t, "[planner:8135]invalid as of timestamp: as of timestamp can't be set in transaction.", err.Error())

	tk.MustExec("rollback")

	tk.MustExec(fmt.Sprintf("start transaction read only as of timestamp '%s'", p1.dt))

	// processor will use the transaction's stale read context
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(tn)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, false)
	err = processor.OnSelectTable(tn)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, false)

	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(nil)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, false)

	// sys variables will be ignored in txn
	tk.MustExec("set @@tidb_read_staleness=-5")
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(tn)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, false)
	err = processor.OnSelectTable(tn)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, false)

	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(nil)
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, false)
	tk.MustExec("set @@tidb_read_staleness=''")
}

func createProcessor(t *testing.T, se sessionctx.Context) staleread.Processor {
	processor := staleread.NewStaleReadProcessor(context.Background(), se)
	require.False(t, processor.IsStaleness())
	require.Equal(t, uint64(0), processor.GetStalenessReadTS())
	require.Nil(t, processor.GetStalenessTSEvaluatorForPrepare())
	require.Nil(t, processor.GetStalenessInfoSchema())
	return processor
}

func TestConsistentCalculateAsOfTsExpr(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set time_zone = '+00:00'")
	tk.Session().GetSessionVars().TimeZone = time.UTC

	p := parser.New()
	stmt, err := p.ParseOneStmt(`select now(3) - interval 1 second`, "", "")
	require.NoError(t, err)
	secondTsExpr := stmt.(*ast.SelectStmt).Fields.Fields[0].Expr
	stmt, err = p.ParseOneStmt(`select now(3) - interval 3 second`, "", "")
	require.NoError(t, err)
	threeSecondTsExpr := stmt.(*ast.SelectStmt).Fields.Fields[0].Expr

	se := tk.Session()
	se.GetSessionVars().StmtCtx = stmtctx.NewStmtCtxWithTimeZone(time.UTC)

	ts1, err := staleread.CalculateAsOfTsExpr(context.Background(), se.GetPlanCtx(), secondTsExpr)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	ts2, err := staleread.CalculateAsOfTsExpr(context.Background(), se.GetPlanCtx(), secondTsExpr)
	require.NoError(t, err)
	require.Equal(t, ts1, ts2)

	ts3, err := staleread.CalculateAsOfTsExpr(context.Background(), se.GetPlanCtx(), threeSecondTsExpr)
	require.NoError(t, err)
	require.True(t, ts3 < ts1)
	require.Equal(t, ts1-ts3, uint64(2000<<18))
}

const injectStaleReadSafeTSFP = "github.com/pingcap/tidb/pkg/sessiontxn/staleread/injectStaleReadSafeTS"

func TestStaleReadReplicaReadPolicy(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	tn := astTableWithAsOf(t, "")
	p1 := genStaleReadPoint(t, tk)
	vars := tk.Session().GetSessionVars()

	// runStaleSelect evaluates `select ... as of timestamp` on a fresh statement context and marks it read-only
	// so that GetReplicaRead returns the adjusted replica read type.
	runStaleSelect := func() {
		tk.MustExec("do 1")
		processor := createProcessor(t, tk.Session())
		require.NoError(t, processor.OnSelectTable(p1.tn))
		p1.checkMatchProcessor(t, processor, true)
		vars.StmtCtx.IsReadOnly = true
	}

	// disabled by default
	runStaleSelect()
	require.False(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.Equal(t, kv.ReplicaReadLeader, vars.GetReplicaRead())

	tk.MustExec("set @@tidb_stale_read_above_safe_ts_replica_read = 'prefer-leader'")
	tk.MustExec("set @@tidb_stale_read_within_safe_ts_replica_read = 'closest-replicas'")
	tk.MustQuery("select @@tidb_stale_read_above_safe_ts_replica_read, @@tidb_stale_read_within_safe_ts_replica_read").
		Check(testkit.Rows("prefer-leader closest-replicas"))

	// read ts above the min safe ts: policy type and sent as a non stale read
	require.NoError(t, failpoint.Enable(injectStaleReadSafeTSFP, "return(1)"))
	runStaleSelect()
	require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.Equal(t, byte(kv.ReplicaReadPreferLeader), vars.StmtCtx.StaleReadReplicaRead)
	require.Equal(t, kv.ReplicaReadPreferLeader, vars.GetReplicaRead())
	require.True(t, vars.StmtCtx.StaleReadAsNonStale)
	vars.StmtCtx.IsStaleness = true
	require.False(t, staleread.UseStaleReadRequests(tk.Session()))

	// read ts within the min safe ts: policy type and kept as a stale read
	require.NoError(t, failpoint.Enable(injectStaleReadSafeTSFP, fmt.Sprintf("return(%d)", math.MaxInt64)))
	runStaleSelect()
	require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.Equal(t, kv.ReplicaReadClosest, vars.GetReplicaRead())
	require.False(t, vars.StmtCtx.StaleReadAsNonStale)
	vars.StmtCtx.IsStaleness = true
	require.True(t, staleread.UseStaleReadRequests(tk.Session()))

	// an explicit session level tidb_replica_read is never overridden
	tk.MustExec("set @@tidb_replica_read = 'follower'")
	runStaleSelect()
	require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.Equal(t, kv.ReplicaReadFollower, vars.GetReplicaRead())
	tk.MustExec("set @@tidb_replica_read = 'leader'")

	// a replica read hint is never overridden
	runStaleSelect()
	vars.StmtCtx.HasReplicaReadHint = true
	vars.StmtCtx.ReplicaRead = byte(kv.ReplicaReadLearner)
	require.Equal(t, kv.ReplicaReadLearner, vars.GetReplicaRead())

	// the policy is ignored for non read-only statements
	runStaleSelect()
	vars.StmtCtx.IsReadOnly = false
	require.Equal(t, kv.ReplicaReadLeader, vars.GetReplicaRead())

	// disabling one side leaves that side untouched
	tk.MustExec("set @@tidb_stale_read_within_safe_ts_replica_read = ''")
	runStaleSelect()
	require.False(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.Equal(t, kv.ReplicaReadLeader, vars.GetReplicaRead())
	require.NoError(t, failpoint.Enable(injectStaleReadSafeTSFP, "return(1)"))
	runStaleSelect()
	require.Equal(t, kv.ReplicaReadPreferLeader, vars.GetReplicaRead())
	require.True(t, vars.StmtCtx.StaleReadAsNonStale)
	// without the above-safe-ts variable a read above the safe ts stays a stale read
	tk.MustExec("set @@tidb_stale_read_above_safe_ts_replica_read = ''")
	tk.MustExec("set @@tidb_stale_read_within_safe_ts_replica_read = 'follower'")
	runStaleSelect()
	require.False(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.False(t, vars.StmtCtx.StaleReadAsNonStale)
	vars.StmtCtx.IsStaleness = true
	require.True(t, staleread.UseStaleReadRequests(tk.Session()))
	tk.MustExec("set @@tidb_stale_read_above_safe_ts_replica_read = 'prefer-leader'")
	tk.MustExec("set @@tidb_stale_read_within_safe_ts_replica_read = ''")
	require.NoError(t, failpoint.Disable(injectStaleReadSafeTSFP))

	// a non-stale read is not affected
	tk.MustExec("do 1")
	processor := createProcessor(t, tk.Session())
	require.NoError(t, processor.OnSelectTable(tn))
	require.False(t, processor.IsStaleness())
	vars.StmtCtx.IsReadOnly = true
	require.False(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.Equal(t, kv.ReplicaReadLeader, vars.GetReplicaRead())

	// invalid values are rejected
	tk.MustGetErrCode("set @@tidb_stale_read_above_safe_ts_replica_read = 'nope'", errno.ErrWrongValueForVar)
	tk.MustGetErrCode("set @@tidb_stale_read_within_safe_ts_replica_read = 'leaders'", errno.ErrWrongValueForVar)
}

func TestStaleReadReplicaReadPolicyWithPreparedStmt(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values (1)")
	// make sure the stale read ts below is after the table is created
	time.Sleep(1200 * time.Millisecond)
	vars := tk.Session().GetSessionVars()

	require.NoError(t, failpoint.Enable(injectStaleReadSafeTSFP, "return(1)"))
	defer func() { require.NoError(t, failpoint.Disable(injectStaleReadSafeTSFP)) }()

	tk.MustExec("prepare s from 'select * from t as of timestamp now(6) - interval 1 second'")
	tk.MustQuery("execute s").Check(testkit.Rows("1"))
	require.False(t, vars.StmtCtx.HasStaleReadReplicaRead)

	// the decision is re-made on every execution, so changing the variable between executions takes effect
	tk.MustExec("set @@tidb_stale_read_above_safe_ts_replica_read = 'prefer-leader'")
	tk.MustQuery("execute s").Check(testkit.Rows("1"))
	require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.Equal(t, byte(kv.ReplicaReadPreferLeader), vars.StmtCtx.StaleReadReplicaRead)

	tk.MustExec("set @@tidb_stale_read_above_safe_ts_replica_read = 'closest-replicas'")
	tk.MustQuery("execute s").Check(testkit.Rows("1"))
	require.Equal(t, byte(kv.ReplicaReadClosest), vars.StmtCtx.StaleReadReplicaRead)

	tk.MustExec("set @@tidb_stale_read_above_safe_ts_replica_read = ''")
	tk.MustQuery("execute s").Check(testkit.Rows("1"))
	require.False(t, vars.StmtCtx.HasStaleReadReplicaRead)

	// a normal prepared statement is not affected
	tk.MustExec("set @@tidb_stale_read_above_safe_ts_replica_read = 'prefer-leader'")
	tk.MustExec("prepare s2 from 'select * from t'")
	tk.MustQuery("execute s2").Check(testkit.Rows("1"))
	require.False(t, vars.StmtCtx.HasStaleReadReplicaRead)
}

func TestStaleReadNowDerivedTSNotInFuture(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values (1)")
	// make sure the NOW()-derived stale read timestamps below are after the table is created and populated
	time.Sleep(1200 * time.Millisecond)

	// NOW()-derived read timestamps never exceed the current PD timestamp and still read the data.
	for _, expr := range []string{
		"now(6)",
		"now(6) - interval 0 second",
		"now(6) - interval 1 second",
		"tidb_bounded_staleness(now(6) - interval 1 second, now(6))",
	} {
		tk.MustQuery("select * from t as of timestamp " + expr).Check(testkit.Rows("1"))

		tk.MustExec("start transaction read only as of timestamp " + expr)
		readTSStr := tk.MustQuery("select @@tidb_current_ts").Rows()[0][0].(string)
		tk.MustQuery("select * from t").Check(testkit.Rows("1"))
		tk.MustExec("commit")
		readTS, err := strconv.ParseUint(readTSStr, 10, 64)
		require.NoError(t, err, expr)
		cur, err := store.CurrentVersion(kv.GlobalTxnScope)
		require.NoError(t, err)
		require.LessOrEqual(t, readTS, cur.Ver, expr)
	}

	// Expressions that really are in the future are still rejected.
	tk.MustMatchErrMsg("select * from t as of timestamp now(6) + interval 1 hour", "cannot set read timestamp to a future time")
	tk.MustMatchErrMsg("start transaction read only as of timestamp now(6) + interval 1 hour", "cannot set read timestamp to a future time")
	tk.MustMatchErrMsg("select * from t as of timestamp '2038-01-18 03:14:07'", "cannot set read timestamp to a future time")
}

// TestStaleReadReplicaReadPolicyInTxn checks that the policy is decided for every read inside a stale read
// transaction (`START TRANSACTION READ ONLY AS OF ...` or `SET TRANSACTION READ ONLY AS OF ...; BEGIN`).
func TestStaleReadReplicaReadPolicyInTxn(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, v int)")
	tk.MustExec("insert into t values (1, 1)")
	time.Sleep(1200 * time.Millisecond)
	vars := tk.Session().GetSessionVars()
	tk.MustExec("set @@tidb_stale_read_above_safe_ts_replica_read = 'prefer-leader'")
	tk.MustExec("set @@tidb_stale_read_within_safe_ts_replica_read = 'follower'")
	defer func() { require.NoError(t, failpoint.Disable(injectStaleReadSafeTSFP)) }()

	for _, c := range []struct {
		safeTS     string
		expected   kv.ReplicaReadType
		asNonStale bool
	}{
		// above the safe ts: prefer-leader and sent as a non stale read
		{"return(1)", kv.ReplicaReadPreferLeader, true},
		// within the safe ts: follower and kept as a stale read
		{fmt.Sprintf("return(%d)", math.MaxInt64), kv.ReplicaReadFollower, false},
	} {
		require.NoError(t, failpoint.Enable(injectStaleReadSafeTSFP, c.safeTS))

		tk.MustExec("start transaction read only as of timestamp now(6) - interval 1 second")
		for range 3 {
			tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
			require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
			require.Equal(t, c.expected, vars.GetReplicaRead())
			require.Equal(t, c.asNonStale, vars.StmtCtx.StaleReadAsNonStale)
			tk.MustQuery("select * from t where id = 1").Check(testkit.Rows("1 1"))
			require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
			require.Equal(t, c.expected, vars.GetReplicaRead())
			require.Equal(t, c.asNonStale, vars.StmtCtx.StaleReadAsNonStale)
		}
		tk.MustExec("commit")

		tk.MustExec("set transaction read only as of timestamp now(6) - interval 1 second")
		tk.MustExec("begin")
		tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
		require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
		require.Equal(t, c.expected, vars.GetReplicaRead())
		tk.MustExec("commit")
	}

	// a normal transaction is not a stale read and is not affected
	tk.MustExec("begin")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
	require.False(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.Equal(t, kv.ReplicaReadLeader, vars.GetReplicaRead())
	require.False(t, vars.StmtCtx.StaleReadAsNonStale)
	tk.MustExec("commit")
}

// TestStaleReadReplicaReadPolicyUDSConfig mirrors the session configuration and statement shape used by Airbnb UDS
// against TiDB replicas: `tidb_replica_read` explicitly set (prefer-leader by default, closest-replicas by override),
// `tidb_low_resolution_tso=ON`, REPEATABLE-READ, and server-side prepared statements carrying
// `/*T! AS OF TIMESTAMP TIDB_BOUNDED_STALENESS(NOW(6) - INTERVAL n MICROSECOND, NOW(6)) */`. The policy variables must
// never override the explicitly configured replica read type.
func TestStaleReadReplicaReadPolicyUDSConfig(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id bigint primary key, v varchar(32))")
	tk.MustExec("insert into t values (1, 'a'), (2, 'b')")
	time.Sleep(1200 * time.Millisecond)
	vars := tk.Session().GetSessionVars()

	// cluster wide policies, as an operator might set them
	tk.MustExec("set @@tidb_stale_read_above_safe_ts_replica_read = 'leader'")
	tk.MustExec("set @@tidb_stale_read_within_safe_ts_replica_read = 'follower'")
	require.NoError(t, failpoint.Enable(injectStaleReadSafeTSFP, "return(1)"))
	defer func() { require.NoError(t, failpoint.Disable(injectStaleReadSafeTSFP)) }()

	const udsQuery = "select v from t /*T! AS OF TIMESTAMP TIDB_BOUNDED_STALENESS(NOW(6) - INTERVAL 1000000 MICROSECOND, NOW(6)) */ where id = ?"

	for _, c := range []struct {
		replicaRead string
		expected    kv.ReplicaReadType
	}{
		// TIDB_REPLICA / TIDB_MASTER pools
		{"prefer-leader", kv.ReplicaReadPreferLeader},
		// Sitar override for same-AZ shards
		{"closest-replicas", kv.ReplicaReadClosest},
		// Sitar override to leader is indistinguishable from the default, so the policy type (leader) applies
		{"leader", kv.ReplicaReadLeader},
	} {
		tk.MustExec("set @@tidb_replica_read = '" + c.replicaRead + "'")
		tk.MustExec("set @@tidb_low_resolution_tso = ON")
		tk.MustExec("set @@transaction_isolation = 'REPEATABLE-READ'")
		tk.MustExec("prepare uds from '" + udsQuery + "'")
		tk.MustExec("set @id = 1")
		for range 3 {
			tk.MustQuery("execute uds using @id").Check(testkit.Rows("a"))
			require.True(t, vars.StmtCtx.HasStaleReadReplicaRead, c.replicaRead)
			require.Equal(t, c.expected, vars.GetReplicaRead(), c.replicaRead)
			// above the safe ts the bounded staleness read is sent as a non stale read with the session's type
			require.True(t, vars.StmtCtx.StaleReadAsNonStale, c.replicaRead)
		}
		// the same statement as text protocol
		tk.MustQuery(strings.Replace(udsQuery, "?", "2", 1)).Check(testkit.Rows("b"))
		require.Equal(t, c.expected, vars.GetReplicaRead(), c.replicaRead)
		require.True(t, vars.StmtCtx.StaleReadAsNonStale, c.replicaRead)
		tk.MustExec("deallocate prepare uds")
	}

	// within the safe ts the read stays a stale read and the explicit replica read type is kept.
	// TIDB_BOUNDED_STALENESS evaluates (and caches) the min safe ts first, so inject it there as well.
	require.NoError(t, failpoint.Enable(injectStaleReadSafeTSFP, fmt.Sprintf("return(%d)", math.MaxInt64)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/injectSafeTS", fmt.Sprintf("return(%d)", math.MaxInt64)))
	defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/expression/injectSafeTS")) }()
	tk.MustExec("set @@tidb_replica_read = 'prefer-leader'")
	tk.MustQuery(strings.Replace(udsQuery, "?", "1", 1)).Check(testkit.Rows("a"))
	require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.False(t, vars.StmtCtx.StaleReadAsNonStale)
	require.Equal(t, kv.ReplicaReadPreferLeader, vars.GetReplicaRead())

	// UDS qsplit transactions are plain `BEGIN` without a stale read, they are never affected
	tk.MustExec("begin")
	tk.MustQuery("select v from t where id = 1").Check(testkit.Rows("a"))
	require.False(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.False(t, vars.StmtCtx.StaleReadAsNonStale)
	require.Equal(t, kv.ReplicaReadPreferLeader, vars.GetReplicaRead())
	tk.MustExec("commit")
}

// TestStaleReadReplicaReadPolicyMusselConfig mirrors the session configuration and statement shapes used by Airbnb
// Mussel: the query pool sets `tidb_replica_read='closest-replicas'` and `tidb_low_resolution_tso=ON` and rewrites
// selects into `SELECT /*+ SET_VAR(...) */ ... FROM t AS OF TIMESTAMP NOW() - INTERVAL n SECOND ...`; the write pool
// sets no replica read at all; the expiration service uses `tidb_read_staleness='-180'` with closest-replicas.
func TestStaleReadReplicaReadPolicyMusselConfig(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(k varbinary(64) primary key, v blob)")
	tk.MustExec("insert into t values ('k1', 'v1')")
	// Mussel uses NOW() without fractional seconds, so `NOW() - INTERVAL 1 SECOND` can be almost 2 seconds old.
	time.Sleep(2500 * time.Millisecond)
	vars := tk.Session().GetSessionVars()

	tk.MustExec("set @@tidb_stale_read_above_safe_ts_replica_read = 'prefer-leader'")
	tk.MustExec("set @@tidb_stale_read_within_safe_ts_replica_read = 'follower'")
	require.NoError(t, failpoint.Enable(injectStaleReadSafeTSFP, "return(1)"))
	defer func() { require.NoError(t, failpoint.Disable(injectStaleReadSafeTSFP)) }()

	const musselQuery = "SELECT /*+ SET_VAR(tikv_client_read_timeout=200) SET_VAR(max_execution_time=1000) */ v FROM t AS OF TIMESTAMP NOW() - INTERVAL 1 SECOND WHERE k = 'k1'"

	// query pool with enable_stale_read=true: explicit closest-replicas wins
	tk.MustExec("set @@tidb_replica_read = 'closest-replicas'")
	tk.MustExec("set @@tidb_low_resolution_tso = ON")
	for range 3 {
		tk.MustQuery(musselQuery).Check(testkit.Rows("v1"))
		require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
		require.Equal(t, kv.ReplicaReadClosest, vars.GetReplicaRead())
		require.True(t, vars.StmtCtx.StaleReadAsNonStale)
	}
	tk.MustExec("prepare mussel from '" + strings.ReplaceAll(musselQuery, "'", "''") + "'")
	tk.MustQuery("execute mussel").Check(testkit.Rows("v1"))
	require.Equal(t, kv.ReplicaReadClosest, vars.GetReplicaRead())
	require.True(t, vars.StmtCtx.StaleReadAsNonStale)
	tk.MustExec("deallocate prepare mussel")

	// a SET_VAR(tidb_replica_read) hint in the Sitar runtime session variables wins as well
	tk.MustQuery("SELECT /*+ SET_VAR(tidb_replica_read='leader-and-follower') */ v FROM t AS OF TIMESTAMP NOW() - INTERVAL 1 SECOND WHERE k = 'k1'").Check(testkit.Rows("v1"))
	require.Equal(t, kv.ReplicaReadMixed, vars.GetReplicaRead())
	require.True(t, vars.StmtCtx.StaleReadAsNonStale)

	// write pool (no tidb_replica_read set) reading with staleness: the policy applies
	tk.MustExec("set @@tidb_replica_read = 'leader'")
	tk.MustQuery(musselQuery).Check(testkit.Rows("v1"))
	require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.Equal(t, kv.ReplicaReadPreferLeader, vars.GetReplicaRead())
	require.True(t, vars.StmtCtx.StaleReadAsNonStale)

	// without any AS OF clause the write pool is a normal read
	tk.MustQuery("SELECT v FROM t WHERE k = 'k1'").Check(testkit.Rows("v1"))
	require.False(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.False(t, vars.StmtCtx.StaleReadAsNonStale)
	require.Equal(t, kv.ReplicaReadLeader, vars.GetReplicaRead())

	// expiration service: tidb_read_staleness with closest-replicas
	tk.MustExec("set @@tidb_replica_read = 'closest-replicas'")
	tk.MustExec("set @@tidb_read_staleness = '-1'")
	tk.MustQuery("SELECT v FROM t WHERE k = 'k1'").Check(testkit.Rows("v1"))
	require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.Equal(t, kv.ReplicaReadClosest, vars.GetReplicaRead())
	require.True(t, vars.StmtCtx.StaleReadAsNonStale)

	// within the safe ts the Mussel read stays a stale read
	require.NoError(t, failpoint.Enable(injectStaleReadSafeTSFP, fmt.Sprintf("return(%d)", math.MaxInt64)))
	tk.MustExec("set @@tidb_read_staleness = 0")
	tk.MustQuery(musselQuery).Check(testkit.Rows("v1"))
	require.True(t, vars.StmtCtx.HasStaleReadReplicaRead)
	require.False(t, vars.StmtCtx.StaleReadAsNonStale)
	require.Equal(t, kv.ReplicaReadClosest, vars.GetReplicaRead())
}
