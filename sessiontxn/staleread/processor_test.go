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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	testsetup.SetupForCommonTest()
	goleak.VerifyTestMain(m, opts...)
}

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
		ts, err := evaluator(p.tk.Session())
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

func TestStaleReadProcessorWithSelectTable(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tn := astTableWithAsOf(t, "")
	p1 := genStaleReadPoint(t, tk)
	p2 := genStaleReadPoint(t, tk)

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
	tk.MustExec("set @@tidb_read_staleness=-5")
	processor = createProcessor(t, tk.Session())
	err = processor.OnSelectTable(tn)
	require.True(t, processor.IsStaleness())
	require.Equal(t, int64(0), processor.GetStalenessInfoSchema().SchemaMetaVersion())
	expectedTS, err := staleread.CalculateTsWithReadStaleness(tk.Session(), -5*time.Second)
	require.NoError(t, err)
	require.Equal(t, expectedTS, processor.GetStalenessReadTS())
	evaluator := processor.GetStalenessTSEvaluatorForPrepare()
	evaluatorTS, err := evaluator(tk.Session())
	require.NoError(t, err)
	require.Equal(t, expectedTS, evaluatorTS)
	tk.MustExec("set @@tidb_read_staleness=''")

	tk.MustExec("do sleep(0.01)")
	evaluatorTS, err = evaluator(tk.Session())
	require.NoError(t, err)
	expectedTS2, err := staleread.CalculateTsWithReadStaleness(tk.Session(), -5*time.Second)
	require.NoError(t, err)
	require.Equal(t, expectedTS2, evaluatorTS)

	// `@@tidb_read_staleness` will be ignored when `as of` or `@@tx_read_ts`
	tk.MustExec("set @@tidb_read_staleness=-5")
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
}

func TestStaleReadProcessorWithExecutePreparedStmt(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	p1 := genStaleReadPoint(t, tk)
	//p2 := genStaleReadPoint(t, tk)

	// create local temporary table to check processor's infoschema will consider temporary table
	tk.MustExec("create temporary table test.t2(a int)")

	// execute prepared stmt with ts evaluator
	processor := createProcessor(t, tk.Session())
	err := processor.OnExecutePreparedStmt(func(sctx sessionctx.Context) (uint64, error) {
		return p1.ts, nil
	})
	require.NoError(t, err)
	p1.checkMatchProcessor(t, processor, true)

	// will get an error when ts evaluator fails
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(func(sctx sessionctx.Context) (uint64, error) {
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
	err = processor.OnExecutePreparedStmt(func(sctx sessionctx.Context) (uint64, error) {
		return p1.ts, nil
	})
	require.Error(t, err)
	require.Equal(t, "[planner:8135]invalid as of timestamp: can't use select as of while already set transaction as of", err.Error())
	tk.MustExec("set @@tx_read_ts=''")

	// `@@tidb_read_staleness`
	tk.MustExec("set @@tidb_read_staleness=-5")
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(nil)
	require.True(t, processor.IsStaleness())
	require.Equal(t, int64(0), processor.GetStalenessInfoSchema().SchemaMetaVersion())
	expectedTS, err := staleread.CalculateTsWithReadStaleness(tk.Session(), -5*time.Second)
	require.NoError(t, err)
	require.Equal(t, expectedTS, processor.GetStalenessReadTS())
	tk.MustExec("set @@tidb_read_staleness=''")

	// `@@tidb_read_staleness` will be ignored when `as of` or `@@tx_read_ts`
	tk.MustExec("set @@tidb_read_staleness=-5")
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(func(sctx sessionctx.Context) (uint64, error) {
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
}

func TestStaleReadProcessorInTxn(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
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

	// return an error when execute prepared stmt with ts evaluator
	processor = createProcessor(t, tk.Session())
	err = processor.OnExecutePreparedStmt(func(sctx sessionctx.Context) (uint64, error) {
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
	processor := staleread.NewStaleReadProcessor(se)
	require.False(t, processor.IsStaleness())
	require.Equal(t, uint64(0), processor.GetStalenessReadTS())
	require.Nil(t, processor.GetStalenessTSEvaluatorForPrepare())
	require.Nil(t, processor.GetStalenessInfoSchema())
	return processor
}
