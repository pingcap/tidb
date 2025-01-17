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

package staticrecordset_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/cursor"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestStaticRecordSet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	rs, err := tk.Exec("select * from t")
	require.NoError(t, err)
	drs := rs.(sqlexec.DetachableRecordSet)
	srs, ok, err := drs.TryDetach()
	require.True(t, ok)
	require.NoError(t, err)

	// check schema
	require.Len(t, srs.Fields(), 1)
	require.Equal(t, "id", srs.Fields()[0].Column.Name.O)

	// check data
	chk := srs.NewChunk(nil)
	err = srs.Next(context.Background(), chk)
	require.NoError(t, err)
	require.Equal(t, 3, chk.NumRows())
	require.Equal(t, int64(1), chk.GetRow(0).GetInt64(0))
	require.Equal(t, int64(2), chk.GetRow(1).GetInt64(0))
	require.Equal(t, int64(3), chk.GetRow(2).GetInt64(0))

	require.NoError(t, srs.Close())
}

func TestStaticRecordSetWithTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	rs, err := tk.Exec("select * from t")
	require.NoError(t, err)
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	drs := rs.(sqlexec.DetachableRecordSet)
	srs, ok, err := drs.TryDetach()
	require.True(t, ok)
	require.NoError(t, err)

	// The transaction should have been committed.
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())

	// Now, it's fine to run another statement on the session
	// remove all existing data in the table
	tk.MustExec("truncate table t")

	// check data
	chk := srs.NewChunk(nil)
	err = srs.Next(context.Background(), chk)
	require.NoError(t, err)
	require.Equal(t, 3, chk.NumRows())
	require.Equal(t, int64(1), chk.GetRow(0).GetInt64(0))
	require.Equal(t, int64(2), chk.GetRow(1).GetInt64(0))
	require.Equal(t, int64(3), chk.GetRow(2).GetInt64(0))

	require.NoError(t, srs.Close())
}

func TestStaticRecordSetExceedGCTime(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	rs, err := tk.Exec("select * from t")
	require.NoError(t, err)
	// Get the startTS
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	startTS := txn.StartTS()

	// Detach the record set
	drs := rs.(sqlexec.DetachableRecordSet)
	srs, ok, err := drs.TryDetach()
	require.True(t, ok)
	require.NoError(t, err)

	// Now, it's fine to run another statement on the session
	// remove all existing data in the table
	tk.MustExec("truncate table t")

	// Update the safe point
	store.(tikv.Storage).UpdateSPCache(startTS+1, time.Now())

	// Check data, it'll get an error
	chk := srs.NewChunk(nil)
	err = srs.Next(context.Background(), chk)
	require.Error(t, err)
	require.NoError(t, srs.Close())
}

func TestDetachError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	// explicit transaction is not allowed
	tk.MustExec("begin")
	rs, err := tk.Exec("select * from t")
	require.NoError(t, err)
	drs2 := rs.(sqlexec.DetachableRecordSet)
	_, ok, err := drs2.TryDetach()
	require.False(t, ok)
	require.NoError(t, err)
	tk.MustExec("commit")
}

func TestCursorWillBeClosed(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	rs, err := tk.Exec("select * from t")
	require.NoError(t, err)
	drs := rs.(sqlexec.DetachableRecordSet)
	srs, ok, err := drs.TryDetach()
	require.True(t, ok)
	require.NoError(t, err)

	// close the record set
	require.NoError(t, srs.Close())

	// check the cursor is closed
	tk.Session().GetCursorTracker().RangeCursor(func(_ cursor.Handle) bool {
		require.Fail(t, "cursor should be closed")
		return false
	})
}

func TestCursorWillBlockMinStartTS(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	rs, err := tk.Exec("select * from t")
	require.NoError(t, err)
	initialStartTS := tk.Session().GetSessionVars().TxnCtx.StartTS
	drs := rs.(sqlexec.DetachableRecordSet)
	srs, ok, err := drs.TryDetach()
	require.True(t, ok)
	require.NoError(t, err)

	// we can start another transaction on the session now
	tk.MustExec("begin")
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2", "3"))
	secondStartTS := tk.Session().GetSessionVars().TxnCtx.StartTS
	require.Greater(t, secondStartTS, initialStartTS)

	infoSyncer := dom.InfoSyncer()
	require.Eventually(t, func() bool {
		infoSyncer.ReportMinStartTS(store)
		return infoSyncer.GetMinStartTS() == initialStartTS
	}, time.Second*5, time.Millisecond*100)

	// close the record set
	require.NoError(t, srs.Close())

	require.Eventually(t, func() bool {
		infoSyncer.ReportMinStartTS(store)
		return infoSyncer.GetMinStartTS() == secondStartTS
	}, time.Second*5, time.Millisecond*100)
}

func TestFinishStmtError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	rs, err := tk.Exec("select * from t")
	require.NoError(t, err)
	drs := rs.(sqlexec.DetachableRecordSet)

	failpoint.Enable("github.com/pingcap/tidb/pkg/session/finishStmtError", "return")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/session/finishStmtError")
	// Then `TryDetach` should return `true`, because the original record set is detached and cannot be used anymore.
	_, ok, err := drs.TryDetach()
	require.True(t, ok)
	require.Error(t, err)
}
