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

package sessiontxn_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/internal"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestEnterNewTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp(a int)")
	tk.MustExec("create table t1(id int primary key)")
	is1 := domain.GetDomain(tk.Session()).InfoSchema()
	tk.MustExec("begin")
	stalenessTS, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoError(t, err)
	tk.MustExec("create table t2(id int primary key)")

	mgr := sessiontxn.GetTxnManager(tk.Session())

	cases := []struct {
		name    string
		prepare func(t *testing.T)
		request *sessiontxn.EnterNewTxnRequest
		check   func(t *testing.T, sctx sessionctx.Context)
	}{
		{
			name: "EnterNewTxnDefault",
			request: &sessiontxn.EnterNewTxnRequest{
				Type: sessiontxn.EnterNewTxnDefault,
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				txn := checkBasicActiveTxn(t, sctx)
				checkInfoSchemaVersion(t, sctx, domain.GetDomain(sctx).InfoSchema().SchemaMetaVersion())

				sessVars := sctx.GetSessionVars()
				require.False(t, txn.IsPessimistic())
				require.False(t, sessVars.InTxn())
				require.False(t, sessVars.TxnCtx.IsStaleness)

				require.False(t, sctx.GetSessionVars().TxnCtx.CouldRetry)
				require.True(t, txn.GetOption(kv.GuaranteeLinearizability).(bool))
			},
		},
		{
			name: "EnterNewTxnDefault",
			request: &sessiontxn.EnterNewTxnRequest{
				Type: sessiontxn.EnterNewTxnDefault,
			},
			prepare: func(t *testing.T) {
				tk.MustExec("set @@autocommit=0")
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				txn := checkBasicActiveTxn(t, sctx)
				checkInfoSchemaVersion(t, sctx, domain.GetDomain(sctx).InfoSchema().SchemaMetaVersion())

				sessVars := sctx.GetSessionVars()
				require.False(t, txn.IsPessimistic())
				require.False(t, sessVars.InTxn())
				require.False(t, sessVars.TxnCtx.IsStaleness)

				require.False(t, sctx.GetSessionVars().TxnCtx.CouldRetry)
				require.True(t, txn.GetOption(kv.GuaranteeLinearizability).(bool))
			},
		},
		{
			name: "EnterNewTxnWithBeginStmt simple",
			request: &sessiontxn.EnterNewTxnRequest{
				Type: sessiontxn.EnterNewTxnWithBeginStmt,
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				checkTxnWithBeginStmt(t, sctx, false, true)
			},
		},
		{
			name: "EnterNewTxnWithBeginStmt with tidb_txn_mode=pessimistic",
			prepare: func(t *testing.T) {
				tk.MustExec("set @@tidb_txn_mode='pessimistic'")
			},
			request: &sessiontxn.EnterNewTxnRequest{
				Type:    sessiontxn.EnterNewTxnWithBeginStmt,
				TxnMode: ast.Pessimistic,
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				checkTxnWithBeginStmt(t, sctx, true, true)
			},
		},
		{
			name: "EnterNewTxnWithBeginStmt with tidb_txn_mode=pessimistic;begin optimistic",
			prepare: func(t *testing.T) {
				tk.MustExec("set @@tidb_txn_mode='pessimistic'")
			},
			request: &sessiontxn.EnterNewTxnRequest{
				Type:    sessiontxn.EnterNewTxnWithBeginStmt,
				TxnMode: ast.Optimistic,
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				checkTxnWithBeginStmt(t, sctx, false, true)
			},
		},
		{
			name: "EnterNewTxnWithBeginStmt with tidb_txn_mode=optimistic;begin pessimistic",
			prepare: func(t *testing.T) {
				tk.MustExec("set @@tidb_txn_mode='optimistic'")
			},
			request: &sessiontxn.EnterNewTxnRequest{
				Type:    sessiontxn.EnterNewTxnWithBeginStmt,
				TxnMode: ast.Pessimistic,
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				checkTxnWithBeginStmt(t, sctx, true, true)
			},
		},
		{
			name: "EnterNewTxnWithBeginStmt with CausalConsistencyOnly",
			request: &sessiontxn.EnterNewTxnRequest{
				Type:                  sessiontxn.EnterNewTxnWithBeginStmt,
				CausalConsistencyOnly: true,
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				checkTxnWithBeginStmt(t, sctx, false, false)
			},
		},
		{
			name: "EnterNewTxnWithBeginStmt and reuse txn",
			prepare: func(t *testing.T) {
				err := mgr.EnterNewTxn(context.TODO(), &sessiontxn.EnterNewTxnRequest{
					Type: sessiontxn.EnterNewTxnBeforeStmt,
				})
				require.NoError(t, err)
				require.NoError(t, mgr.OnStmtStart(context.TODO(), nil))
				require.NoError(t, mgr.AdviseWarmup())
			},
			request: &sessiontxn.EnterNewTxnRequest{
				Type: sessiontxn.EnterNewTxnWithBeginStmt,
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				checkTxnWithBeginStmt(t, sctx, false, true)
			},
		},
		{
			name: "EnterNewTxnWithBeginStmt with staleness",
			request: &sessiontxn.EnterNewTxnRequest{
				Type:        sessiontxn.EnterNewTxnWithBeginStmt,
				StaleReadTS: stalenessTS,
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				txn := checkBasicActiveTxn(t, sctx)
				require.Equal(t, stalenessTS, txn.StartTS())
				is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
				require.Equal(t, is1.SchemaMetaVersion(), is.SchemaMetaVersion())
				_, ok := is.(*infoschema.SessionExtendedInfoSchema)
				require.True(t, ok)

				sessVars := sctx.GetSessionVars()
				require.Equal(t, false, txn.IsPessimistic())
				require.True(t, sessVars.InTxn())
				require.True(t, sessVars.TxnCtx.IsStaleness)
			},
		},
		{
			name: "EnterNewTxnBeforeStmt",
			request: &sessiontxn.EnterNewTxnRequest{
				Type: sessiontxn.EnterNewTxnBeforeStmt,
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				checkTxnBeforeStmt(t, sctx)
				checkStmtTxnAfterActive(t, sctx)
				require.True(t, sctx.GetSessionVars().TxnCtx.CouldRetry)
				require.False(t, sctx.GetSessionVars().InTxn())
			},
		},
		{
			name: "EnterNewTxnBeforeStmt and autocommit=0",
			prepare: func(t *testing.T) {
				tk.MustExec("set @@autocommit=0")
			},
			request: &sessiontxn.EnterNewTxnRequest{
				Type: sessiontxn.EnterNewTxnBeforeStmt,
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				checkTxnBeforeStmt(t, sctx)
				checkStmtTxnAfterActive(t, sctx)
				require.False(t, sctx.GetSessionVars().TxnCtx.CouldRetry)
				require.True(t, sctx.GetSessionVars().InTxn())
			},
		},
		{
			name: "EnterNewTxnWithReplaceProvider",
			prepare: func(t *testing.T) {
				err := mgr.EnterNewTxn(context.TODO(), &sessiontxn.EnterNewTxnRequest{
					Type: sessiontxn.EnterNewTxnBeforeStmt,
				})
				require.NoError(t, err)
			},
			request: &sessiontxn.EnterNewTxnRequest{
				Type:     sessiontxn.EnterNewTxnWithReplaceProvider,
				Provider: staleread.NewStalenessTxnContextProvider(tk.Session(), stalenessTS, nil),
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				checkNonActiveStalenessTxn(t, sctx, stalenessTS, is1)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			se := tk.Session()
			tk.MustExec("set @@tidb_txn_mode=''")
			tk.MustExec("set @@autocommit=1")
			tk.MustExec("commit")

			if c.prepare != nil {
				c.prepare(t)
			}

			require.NoError(t, mgr.EnterNewTxn(context.TODO(), c.request))
			if c.check != nil {
				c.check(t, se)
			}
		})
	}
}

func TestGetSnapshot(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key)")

	isSnapshotEqual := func(t *testing.T, snap1 kv.Snapshot, snap2 kv.Snapshot) bool {
		require.NotNil(t, snap1)
		require.NotNil(t, snap2)

		iter1, err := snap1.Iter([]byte{}, []byte{})
		require.NoError(t, err)
		iter2, err := snap2.Iter([]byte{}, []byte{})
		require.NoError(t, err)

		for {
			if iter1.Valid() && iter2.Valid() {
				if iter1.Key().Cmp(iter2.Key()) != 0 {
					return false
				}
				if !bytes.Equal(iter1.Value(), iter2.Value()) {
					return false
				}
				err = iter1.Next()
				require.NoError(t, err)
				err = iter2.Next()
				require.NoError(t, err)
			} else if !iter1.Valid() && !iter2.Valid() {
				return true
			} else {
				return false
			}
		}
	}

	mgr := sessiontxn.GetTxnManager(tk.Session())

	cases := []struct {
		isolation string
		prepare   func(t *testing.T)
		check     func(t *testing.T, sctx sessionctx.Context)
	}{
		{
			isolation: "Pessimistic Repeatable Read",
			prepare: func(t *testing.T) {
				tk.MustExec("set @@tx_isolation='REPEATABLE-READ'")
				tk.MustExec("begin pessimistic")
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				ts, err := mgr.GetStmtReadTS()
				require.NoError(t, err)
				compareSnap := internal.GetSnapshotWithTS(sctx, ts, nil)
				snap, err := mgr.GetSnapshotWithStmtReadTS()
				require.NoError(t, err)
				require.True(t, isSnapshotEqual(t, compareSnap, snap))

				tk2.MustExec("insert into t values(10)")

				tk.MustQuery("select * from t for update").Check(testkit.Rows("1", "3", "10"))
				ts, err = mgr.GetStmtForUpdateTS()
				require.NoError(t, err)
				compareSnap2 := internal.GetSnapshotWithTS(sctx, ts, nil)
				snap, err = mgr.GetSnapshotWithStmtReadTS()
				require.NoError(t, err)
				require.False(t, isSnapshotEqual(t, compareSnap2, snap))
				snap, err = mgr.GetSnapshotWithStmtForUpdateTS()
				require.NoError(t, err)
				require.True(t, isSnapshotEqual(t, compareSnap2, snap))

				require.False(t, isSnapshotEqual(t, compareSnap, snap))
			},
		},
		{
			isolation: "Pessimistic Read Committed",
			prepare: func(t *testing.T) {
				tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
				tk.MustExec("begin pessimistic")
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				ts, err := mgr.GetStmtReadTS()
				require.NoError(t, err)
				compareSnap := internal.GetSnapshotWithTS(sctx, ts, nil)
				snap, err := mgr.GetSnapshotWithStmtReadTS()
				require.NoError(t, err)
				require.True(t, isSnapshotEqual(t, compareSnap, snap))

				tk2.MustExec("insert into t values(10)")

				tk.MustQuery("select * from t").Check(testkit.Rows("1", "3", "10"))
				ts, err = mgr.GetStmtForUpdateTS()
				require.NoError(t, err)
				compareSnap2 := internal.GetSnapshotWithTS(sctx, ts, nil)
				snap, err = mgr.GetSnapshotWithStmtReadTS()
				require.NoError(t, err)
				require.True(t, isSnapshotEqual(t, compareSnap2, snap))
				snap, err = mgr.GetSnapshotWithStmtForUpdateTS()
				require.NoError(t, err)
				require.True(t, isSnapshotEqual(t, compareSnap2, snap))

				require.False(t, isSnapshotEqual(t, compareSnap, snap))
			},
		},
		{
			isolation: "Optimistic",
			prepare: func(t *testing.T) {
				tk.MustExec("begin optimistic")
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				ts, err := mgr.GetStmtReadTS()
				require.NoError(t, err)
				compareSnap := internal.GetSnapshotWithTS(sctx, ts, nil)
				snap, err := mgr.GetSnapshotWithStmtReadTS()
				require.NoError(t, err)
				require.True(t, isSnapshotEqual(t, compareSnap, snap))

				tk2.MustExec("insert into t values(10)")

				tk.MustQuery("select * from t for update").Check(testkit.Rows("1", "3"))
				ts, err = mgr.GetStmtForUpdateTS()
				require.NoError(t, err)
				compareSnap2 := internal.GetSnapshotWithTS(sctx, ts, nil)
				snap, err = mgr.GetSnapshotWithStmtReadTS()
				require.NoError(t, err)
				require.True(t, isSnapshotEqual(t, compareSnap2, snap))
				snap, err = mgr.GetSnapshotWithStmtForUpdateTS()
				require.NoError(t, err)
				require.True(t, isSnapshotEqual(t, compareSnap2, snap))

				require.True(t, isSnapshotEqual(t, compareSnap, snap))
			},
		},
		{
			isolation: "Pessimistic Serializable",
			prepare: func(t *testing.T) {
				tk.MustExec("set tidb_skip_isolation_level_check = 1")
				tk.MustExec("set tx_isolation = 'SERIALIZABLE'")
				tk.MustExec("begin pessimistic")
			},
			check: func(t *testing.T, sctx sessionctx.Context) {
				ts, err := mgr.GetStmtReadTS()
				require.NoError(t, err)
				compareSnap := internal.GetSnapshotWithTS(sctx, ts, nil)
				snap, err := mgr.GetSnapshotWithStmtReadTS()
				require.NoError(t, err)
				require.True(t, isSnapshotEqual(t, compareSnap, snap))

				tk2.MustExec("insert into t values(10)")

				tk.MustQuery("select * from t for update").Check(testkit.Rows("1", "3"))
				ts, err = mgr.GetStmtForUpdateTS()
				require.NoError(t, err)
				compareSnap2 := internal.GetSnapshotWithTS(sctx, ts, nil)
				snap, err = mgr.GetSnapshotWithStmtReadTS()
				require.NoError(t, err)
				require.True(t, isSnapshotEqual(t, compareSnap2, snap))
				snap, err = mgr.GetSnapshotWithStmtForUpdateTS()
				require.NoError(t, err)
				require.True(t, isSnapshotEqual(t, compareSnap2, snap))

				require.True(t, isSnapshotEqual(t, compareSnap, snap))
			},
		},
	}

	for _, c := range cases {
		t.Run(c.isolation, func(t *testing.T) {
			se := tk.Session()
			tk.MustExec("truncate t")
			tk.MustExec("set @@tidb_txn_mode=''")
			tk.MustExec("set @@autocommit=1")
			tk.MustExec("insert into t values(1), (3)")
			tk.MustExec("commit")

			if c.prepare != nil {
				c.prepare(t)
			}

			if c.check != nil {
				c.check(t, se)
			}
			tk.MustExec("rollback")
		})
	}
}

func TestSnapshotInterceptor(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create temporary table test.tmp1 (id int primary key)")
	tbl, err := tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tmp1"))
	require.NoError(t, err)
	require.Equal(t, model.TempTableLocal, tbl.Meta().TempTableType)
	tblID := tbl.Meta().ID

	// prepare a kv pair for temporary table
	k := append(tablecodec.EncodeTablePrefix(tblID), 1)
	require.NoError(t, tk.Session().GetSessionVars().TemporaryTableData.SetTableKey(tblID, k, []byte("v1")))

	initTxnFuncs := []func() error{
		func() error {
			err := tk.Session().PrepareTxnCtx(context.TODO())
			if err == nil {
				err = sessiontxn.GetTxnManager(tk.Session()).AdviseWarmup()
			}
			return err
		},
		func() error {
			return sessiontxn.NewTxn(context.Background(), tk.Session())
		},
		func() error {
			return sessiontxn.GetTxnManager(tk.Session()).EnterNewTxn(context.TODO(), &sessiontxn.EnterNewTxnRequest{
				Type:        sessiontxn.EnterNewTxnWithBeginStmt,
				StaleReadTS: 0,
			})
		},
	}

	for _, initFunc := range initTxnFuncs {
		require.NoError(t, initFunc())

		require.NoError(t, sessiontxn.GetTxnManager(tk.Session()).OnStmtStart(context.TODO(), nil))
		txn, err := tk.Session().Txn(true)
		require.NoError(t, err)

		val, err := txn.Get(context.Background(), k)
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), val)

		val, err = txn.GetSnapshot().Get(context.Background(), k)
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), val)

		tk.Session().RollbackTxn(context.Background())
	}

	// Also check GetSnapshotWithTS
	snap := internal.GetSnapshotWithTS(tk.Session(), 0, temptable.SessionSnapshotInterceptor(tk.Session(), sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()))
	val, err := snap.Get(context.Background(), k)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}

func checkBasicActiveTxn(t *testing.T, sctx sessionctx.Context) kv.Transaction {
	txn, err := sctx.Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())

	sessVars := sctx.GetSessionVars()

	require.Equal(t, txn.IsPessimistic(), sessVars.TxnCtx.IsPessimistic)
	require.Equal(t, txn.StartTS(), sessVars.TxnCtx.StartTS)
	require.Equal(t, sessVars.InTxn(), sessVars.TxnCtx.IsExplicit)
	return txn
}

func checkTxnWithBeginStmt(t *testing.T, sctx sessionctx.Context, pessimistic, guaranteeLinear bool) {
	txn := checkBasicActiveTxn(t, sctx)
	checkInfoSchemaVersion(t, sctx, domain.GetDomain(sctx).InfoSchema().SchemaMetaVersion())

	sessVars := sctx.GetSessionVars()
	require.Equal(t, pessimistic, txn.IsPessimistic())
	require.True(t, sessVars.InTxn())
	require.False(t, sessVars.TxnCtx.IsStaleness)

	require.False(t, sctx.GetSessionVars().TxnCtx.CouldRetry)
	require.Equal(t, guaranteeLinear, txn.GetOption(kv.GuaranteeLinearizability).(bool))
}

func checkTxnBeforeStmt(t *testing.T, sctx sessionctx.Context) {
	txn, err := sctx.Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())
	checkInfoSchemaVersion(t, sctx, domain.GetDomain(sctx).InfoSchema().SchemaMetaVersion())

	sessVars := sctx.GetSessionVars()
	require.False(t, sessVars.TxnCtx.IsPessimistic)
	require.False(t, sessVars.TxnCtx.IsExplicit)
	require.False(t, sessVars.InTxn())
	require.False(t, sessVars.TxnCtx.CouldRetry)
	require.False(t, sessVars.TxnCtx.IsStaleness)
}

func checkStmtTxnAfterActive(t *testing.T, sctx sessionctx.Context) {
	require.NoError(t, sessiontxn.GetTxnManager(sctx).AdviseWarmup())
	_, err := sctx.Txn(true)
	require.NoError(t, err)
	txn := checkBasicActiveTxn(t, sctx)
	checkInfoSchemaVersion(t, sctx, domain.GetDomain(sctx).InfoSchema().SchemaMetaVersion())

	sessVars := sctx.GetSessionVars()
	require.False(t, txn.IsPessimistic())
	require.False(t, sessVars.TxnCtx.IsStaleness)
}

func checkNonActiveStalenessTxn(t *testing.T, sctx sessionctx.Context, ts uint64, is infoschema.InfoSchema) {
	txn, err := sctx.Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())
	require.False(t, sctx.GetSessionVars().InTxn())
	require.False(t, sctx.GetSessionVars().TxnCtx.IsExplicit)
	require.True(t, sctx.GetSessionVars().TxnCtx.IsStaleness)
	checkInfoSchemaVersion(t, sctx, is.SchemaMetaVersion())
	readTS, err := sessiontxn.GetTxnManager(sctx).GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, ts, readTS)
}

func checkInfoSchemaVersion(t *testing.T, sctx sessionctx.Context, ver int64) {
	is1, ok := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema().(*infoschema.SessionExtendedInfoSchema)
	require.True(t, ok)

	is2 := sctx.GetSessionVars().TxnCtx.InfoSchema.(*infoschema.SessionExtendedInfoSchema)
	require.True(t, ok)

	require.Equal(t, ver, is1.SchemaMetaVersion())
	require.Equal(t, ver, is2.SchemaMetaVersion())
}
