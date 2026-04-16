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

//go:build fusion

package executor_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

type domainLike interface {
	Reload() error
	InfoSchema() infoschema.InfoSchema
}

type tableLike interface {
	Meta() *model.TableInfo
}

func mustGetDomainWithInfoSchemaV1(t *testing.T, tk *testkit.TestKit, store kv.Storage) domainLike {
	t.Helper()

	dom, err := session.GetDomain(store)
	require.NoError(t, err)

	origSchemaCacheSize := tk.MustQuery("select @@global.tidb_schema_cache_size").Rows()[0][0].(string)
	tk.MustExec("set global tidb_schema_cache_size = 0")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_schema_cache_size = " + origSchemaCacheSize)
	})

	require.NoError(t, dom.Reload())
	isV2, _ := infoschema.IsV2(dom.InfoSchema())
	require.False(t, isV2, "expected infoschema v1 after setting tidb_schema_cache_size=0")

	return dom
}

func mustGetDBInfo(t *testing.T, dom domainLike, schemaName string) *model.DBInfo {
	t.Helper()

	db, ok := dom.InfoSchema().SchemaByName(pmodel.NewCIStr(schemaName))
	require.True(t, ok)
	return db
}

func mustGetTable(t *testing.T, dom domainLike, schemaName, tableName string) tableLike {
	t.Helper()

	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr(tableName))
	require.NoError(t, err)
	return tbl
}

func mustUpdateTableWithTriggerSchemaDiff(t *testing.T, store kv.Storage, schemaID int64, tblInfo *model.TableInfo) {
	t.Helper()

	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(internalCtx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		schemaVer, err := m.GenSchemaVersion()
		if err != nil {
			return err
		}
		if err := m.UpdateTable(schemaID, tblInfo); err != nil {
			return err
		}
		return m.SetSchemaDiff(&model.SchemaDiff{
			Version:  schemaVer,
			Type:     model.ActionCreateTrigger,
			SchemaID: schemaID,
			TableID:  tblInfo.ID,
		})
	})
	require.NoError(t, err)
}

func mustSetTableTriggers(t *testing.T, dom domainLike, store kv.Storage, schemaID int64, schemaName, tableName string, triggers []*model.TriggerInfo) *model.TableInfo {
	t.Helper()

	tbl := mustGetTable(t, dom, schemaName, tableName)
	tblInfo := tbl.Meta().Clone()
	tblInfo.Triggers = triggers
	mustUpdateTableWithTriggerSchemaDiff(t, store, schemaID, tblInfo)
	return tblInfo
}

func mustSetTableTriggersAndReload(t *testing.T, dom domainLike, store kv.Storage, schemaID int64, schemaName, tableName string, triggers []*model.TriggerInfo) *model.TableInfo {
	t.Helper()

	tblInfo := mustSetTableTriggers(t, dom, store, schemaID, schemaName, tableName, triggers)
	require.NoError(t, dom.Reload())
	return tblInfo
}

func mustAppendTriggerToTableAndReload(t *testing.T, dom domainLike, store kv.Storage, schemaID int64, schemaName, tableName string, trig *model.TriggerInfo) {
	t.Helper()

	tbl := mustGetTable(t, dom, schemaName, tableName)
	tblInfo := tbl.Meta().Clone()
	tblInfo.Triggers = append(tblInfo.Triggers, trig)
	mustUpdateTableWithTriggerSchemaDiff(t, store, schemaID, tblInfo)
	require.NoError(t, dom.Reload())
}

func refreshSessionAndUseTestDB(tk *testkit.TestKit, inProcedure bool) {
	tk.RefreshSession()
	if inProcedure {
		tk.InProcedure()
	}
	tk.MustExec("use test")
}

func TestSetVarForPKDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// For global
	tk.MustQuery(`select @@global.tidb_create_from_select_using_import;`).Check(testkit.Rows("0"))
	tk.MustExec(`set @@global.tidb_create_from_select_using_import="ON";`)
	tk.MustQuery(`select @@global.tidb_create_from_select_using_import;`).Check(testkit.Rows("1"))
	tk.MustExec(`set @@global.tidb_create_from_select_using_import=0;`)
	tk.MustQuery(`select @@global.tidb_create_from_select_using_import;`).Check(testkit.Rows("0"))
	// For session
	tk.MustQuery(`select @@session.tidb_create_from_select_using_import;`).Check(testkit.Rows("0"))
	tk.MustExec(`set @@global.tidb_create_from_select_using_import="ON";`)
	tk.MustQuery(`select @@session.tidb_create_from_select_using_import;`).Check(testkit.Rows("0"))
	tk.MustExec(`set @@session.tidb_create_from_select_using_import=1;`)
	tk.MustQuery(`select @@session.tidb_create_from_select_using_import;`).Check(testkit.Rows("1"))
}

func TestPingKaiDBSetVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// test tidbx_fast_path
	tk.MustQuery("select @@global.tidbx_fast_path").Check(testkit.Rows("0")) // default value
	tk.MustExec("set global tidbx_fast_path = 1")
	tk.MustQuery("select @@global.tidbx_fast_path").Check(testkit.Rows("1"))
	require.True(t, variable.EnableFastPath.Load())
	tk.MustExec("set global tidbx_fast_path = 0")
	tk.MustQuery("select @@global.tidbx_fast_path").Check(testkit.Rows("0"))
	require.False(t, variable.EnableFastPath.Load())

	// test tidbx_enable_index_lookup_push_down
	// global scope
	tk.MustQuery("select @@global.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("0")) // default value
	tk.MustExec("set global tidbx_enable_index_lookup_push_down = 1")
	tk.MustQuery("select @@global.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("1"))
	tk.MustExec("set global tidbx_enable_index_lookup_push_down = 0")
	tk.MustQuery("select @@global.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("0"))
	// session scope
	tk.MustQuery("select @@session.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("0")) // default value
	tk.MustExec("set session tidbx_enable_index_lookup_push_down = 1")
	tk.MustQuery("select @@session.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("1"))
	require.True(t, tk.Session().GetSessionVars().EnableIndexLookUpPushDown)
	tk.MustExec("set session tidbx_enable_index_lookup_push_down = 0")
	tk.MustQuery("select @@session.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("0"))
}

func TestSetTxnIsolationLevel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set global pkdb_eal = on")
	t.Cleanup(func() {
		tk.MustExec("set global pkdb_eal = off")
	})

	tk.MustExec("BEGIN")
	tk.MustExec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustExec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	tk.MustExec("set global pkdb_eal = off")
	tk.MustGetDBError("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", exeerrors.ErrCantChangeTxCharacteristics)
	tk.MustExec("COMMIT")
	tk.MustExec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
}

func TestTriggerSetNewPseudoRecordInvalidContextNoPanic(t *testing.T) {
	type testCase struct {
		name        string
		timing      ast.TriggerTiming
		event       ast.TriggerEvent
		setup       func(tk *testkit.TestKit)
		stmt        string
		wantErrCode int
		assertAfter func(tk *testkit.TestKit)
	}

	cases := []testCase{
		{
			name:   "BeforeInsertAllowsSetNew",
			timing: ast.TriggerTimingBefore,
			event:  ast.TriggerEventInsert,
			stmt:   "insert into t values (1, 2)",
			assertAfter: func(tk *testkit.TestKit) {
				tk.MustQuery("select a, b from t").Check(testkit.Rows("42 2"))
			},
		},
		{
			name:        "AfterInsertRejectsSetNew",
			timing:      ast.TriggerTimingAfter,
			event:       ast.TriggerEventInsert,
			stmt:        "insert into t values (1, 2)",
			wantErrCode: mysql.ErrTrgCantChangeRow,
		},
		{
			name:   "BeforeUpdateAllowsSetNew",
			timing: ast.TriggerTimingBefore,
			event:  ast.TriggerEventUpdate,
			setup: func(tk *testkit.TestKit) {
				tk.MustExec("insert into t values (1, 2)")
			},
			stmt: "update t set b = 3 where a = 1",
			assertAfter: func(tk *testkit.TestKit) {
				tk.MustQuery("select a, b from t").Check(testkit.Rows("42 3"))
			},
		},
		{
			name:        "AfterUpdateRejectsSetNew",
			timing:      ast.TriggerTimingAfter,
			event:       ast.TriggerEventUpdate,
			setup:       func(tk *testkit.TestKit) { tk.MustExec("insert into t values (1, 2)") },
			stmt:        "update t set b = 3 where a = 1",
			wantErrCode: mysql.ErrTrgCantChangeRow,
		},
		{
			name:        "BeforeDeleteRejectsSetNew",
			timing:      ast.TriggerTimingBefore,
			event:       ast.TriggerEventDelete,
			setup:       func(tk *testkit.TestKit) { tk.MustExec("insert into t values (1, 2)") },
			stmt:        "delete from t where a = 1",
			wantErrCode: mysql.ErrTrgNoSuchRowInTrg,
		},
		{
			name:        "AfterDeleteRejectsSetNew",
			timing:      ast.TriggerTimingAfter,
			event:       ast.TriggerEventDelete,
			setup:       func(tk *testkit.TestKit) { tk.MustExec("insert into t values (1, 2)") },
			stmt:        "delete from t where a = 1",
			wantErrCode: mysql.ErrTrgNoSuchRowInTrg,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t(a int, b int)")

			dom := mustGetDomainWithInfoSchemaV1(t, tk, store)

			db := mustGetDBInfo(t, dom, "test")

			createSQL := "CREATE TRIGGER trg_set_new " + tc.timing.String() + " " + tc.event.String() +
				" ON t FOR EACH ROW SET NEW.a = 42;"
			trig := &model.TriggerInfo{
				Name:      pmodel.NewCIStr("trg_set_new"),
				Timing:    tc.timing,
				Event:     tc.event,
				Table:     pmodel.NewCIStr("t"),
				CreateSQL: createSQL,
				Body:      "SET NEW.a = 42",
				State:     model.StatePublic,
				SQLMode:   tk.Session().GetSessionVars().SQLMode,
			}

			tblInfo := mustSetTableTriggers(t, dom, store, db.ID, "test", "t", []*model.TriggerInfo{trig})

			var triggerCountInMeta int
			internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
			err := kv.RunInNewTxn(internalCtx, store, false, func(ctx context.Context, txn kv.Transaction) error {
				m := meta.NewMutator(txn)
				loaded, err := m.GetTable(db.ID, tblInfo.ID)
				if err != nil {
					return err
				}
				triggerCountInMeta = len(loaded.Triggers)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 1, triggerCountInMeta)

			require.NoError(t, dom.Reload())

			tblAfter := mustGetTable(t, dom, "test", "t")
			require.Len(t, tblAfter.Meta().Triggers, 1)

			refreshSessionAndUseTestDB(tk, false)

			if tc.setup != nil {
				tc.setup(tk)
			}
			if tc.wantErrCode != 0 {
				require.NotPanics(t, func() {
					tk.MustGetErrCode(tc.stmt, tc.wantErrCode)
				})
			} else {
				tk.MustExec(tc.stmt)
			}
			if tc.assertAfter != nil {
				tc.assertAfter(tk)
			}
		})
	}
}

func TestTriggerProcedureOutParamPseudoRecordDirectionAndNoPanic(t *testing.T) {
	type testCase struct {
		name        string
		timing      ast.TriggerTiming
		event       ast.TriggerEvent
		triggerBody string
		setup       func(tk *testkit.TestKit)
		stmt        string
		wantErrCode int
		assertAfter func(tk *testkit.TestKit)
	}

	cases := []testCase{
		{
			name:        "BeforeInsertOutNewSuccess",
			timing:      ast.TriggerTimingBefore,
			event:       ast.TriggerEventInsert,
			triggerBody: "CALL p(NEW.a)",
			stmt:        "insert into t values (1, 2)",
			assertAfter: func(tk *testkit.TestKit) { tk.MustQuery("select a, b from t").Check(testkit.Rows("42 2")) },
		},
		{
			name:        "BeforeUpdateOutNewSuccess",
			timing:      ast.TriggerTimingBefore,
			event:       ast.TriggerEventUpdate,
			triggerBody: "CALL p(NEW.a)",
			setup:       func(tk *testkit.TestKit) { tk.MustExec("insert into t values (1, 2)") },
			stmt:        "update t set b = 3 where a = 1",
			assertAfter: func(tk *testkit.TestKit) { tk.MustQuery("select a, b from t").Check(testkit.Rows("42 3")) },
		},
		{
			name:        "BeforeUpdateOutOldRejected",
			timing:      ast.TriggerTimingBefore,
			event:       ast.TriggerEventUpdate,
			triggerBody: "CALL p(OLD.a)",
			setup:       func(tk *testkit.TestKit) { tk.MustExec("insert into t values (1, 2)") },
			stmt:        "update t set b = 3 where a = 1",
			wantErrCode: mysql.ErrSpNotVarArg,
			assertAfter: func(tk *testkit.TestKit) { tk.MustQuery("select a, b from t").Check(testkit.Rows("1 2")) },
		},
		{
			name:        "BeforeDeleteOutNewRejectedNoPanic",
			timing:      ast.TriggerTimingBefore,
			event:       ast.TriggerEventDelete,
			triggerBody: "CALL p(NEW.a)",
			setup:       func(tk *testkit.TestKit) { tk.MustExec("insert into t values (1, 2)") },
			stmt:        "delete from t where a = 1",
			wantErrCode: mysql.ErrTrgNoSuchRowInTrg,
			assertAfter: func(tk *testkit.TestKit) { tk.MustQuery("select a, b from t").Check(testkit.Rows("1 2")) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.InProcedure()
			tk.MustExec("use test")
			tk.MustExec("create table t(a int, b int)")
			tk.MustExec("create procedure p(out x int) begin set x = 42; end;")

			dom := mustGetDomainWithInfoSchemaV1(t, tk, store)

			db := mustGetDBInfo(t, dom, "test")

			createSQL := "CREATE TRIGGER trg_call_proc " + tc.timing.String() + " " + tc.event.String() +
				" ON t FOR EACH ROW " + tc.triggerBody + ";"
			trig := &model.TriggerInfo{
				Name:      pmodel.NewCIStr("trg_call_proc"),
				Timing:    tc.timing,
				Event:     tc.event,
				Table:     pmodel.NewCIStr("t"),
				CreateSQL: createSQL,
				Body:      tc.triggerBody,
				State:     model.StatePublic,
				SQLMode:   tk.Session().GetSessionVars().SQLMode,
			}

			mustSetTableTriggersAndReload(t, dom, store, db.ID, "test", "t", []*model.TriggerInfo{trig})

			refreshSessionAndUseTestDB(tk, true)

			if tc.setup != nil {
				tc.setup(tk)
			}
			if tc.wantErrCode != 0 {
				require.NotPanics(t, func() {
					tk.MustGetErrCode(tc.stmt, tc.wantErrCode)
				})
			} else {
				tk.MustExec(tc.stmt)
			}
			if tc.assertAfter != nil {
				tc.assertAfter(tk)
			}
		})
	}
}

func TestTriggerProcedureBlockNestedDMLFiresOtherTableTriggers(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table a(a int)")
	tk.MustExec("create table p(id int primary key)")
	tk.MustExec("insert into p values (1)")
	tk.MustExec("create table b(x int, constraint fk_1 foreign key (x) references p(id))")

	dom := mustGetDomainWithInfoSchemaV1(t, tk, store)
	db := mustGetDBInfo(t, dom, "test")

	trigBBeforeCreateSQL := "CREATE TRIGGER trg_b_set_new BEFORE INSERT ON b FOR EACH ROW SET NEW.x = 42;"
	trigBBefore := &model.TriggerInfo{
		Name:      pmodel.NewCIStr("trg_b_set_new"),
		Timing:    ast.TriggerTimingBefore,
		Event:     ast.TriggerEventInsert,
		Table:     pmodel.NewCIStr("b"),
		CreateSQL: trigBBeforeCreateSQL,
		Body:      "SET NEW.x = 42",
		State:     model.StatePublic,
		SQLMode:   tk.Session().GetSessionVars().SQLMode,
	}
	mustAppendTriggerToTableAndReload(t, dom, store, db.ID, "test", "b", trigBBefore)

	trigBAfterCreateSQL := "CREATE TRIGGER trg_b_after_insert AFTER INSERT ON b FOR EACH ROW SET @dummy = 1;"
	trigBAfter := &model.TriggerInfo{
		Name:      pmodel.NewCIStr("trg_b_after_insert"),
		Timing:    ast.TriggerTimingAfter,
		Event:     ast.TriggerEventInsert,
		Table:     pmodel.NewCIStr("b"),
		CreateSQL: trigBAfterCreateSQL,
		Body:      "SET @dummy = 1",
		State:     model.StatePublic,
		SQLMode:   tk.Session().GetSessionVars().SQLMode,
	}
	mustAppendTriggerToTableAndReload(t, dom, store, db.ID, "test", "b", trigBAfter)

	trigACreateSQL := "CREATE TRIGGER trg_a_insert_b AFTER INSERT ON a FOR EACH ROW BEGIN INSERT INTO b VALUES (NEW.a); END;"
	trigA := &model.TriggerInfo{
		Name:      pmodel.NewCIStr("trg_a_insert_b"),
		Timing:    ast.TriggerTimingAfter,
		Event:     ast.TriggerEventInsert,
		Table:     pmodel.NewCIStr("a"),
		CreateSQL: trigACreateSQL,
		Body:      "BEGIN INSERT INTO b VALUES (NEW.a); END",
		State:     model.StatePublic,
		SQLMode:   tk.Session().GetSessionVars().SQLMode,
	}
	mustAppendTriggerToTableAndReload(t, dom, store, db.ID, "test", "a", trigA)

	refreshSessionAndUseTestDB(tk, true)

	err := tk.ExecToErr("insert into a values (1)")
	if err == nil {
		tk.MustQuery("select x from b").Check(testkit.Rows("42"))
		require.FailNow(t, "expected foreign key constraint error")
	}
	require.Contains(t, err.Error(), "Cannot add or update a child row: a foreign key constraint fails")
	tk.MustQuery("select a from a").Check(testkit.Rows())
	tk.MustQuery("select x from b").Check(testkit.Rows())
}

func TestTriggerNonProcedureStmtReturningResultSetRejected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")

	dom := mustGetDomainWithInfoSchemaV1(t, tk, store)
	db := mustGetDBInfo(t, dom, "test")

	trig := &model.TriggerInfo{
		Name:             pmodel.NewCIStr("trg_select"),
		Timing:           ast.TriggerTimingBefore,
		Event:            ast.TriggerEventInsert,
		Table:            pmodel.NewCIStr("t"),
		CreateSQL:        "CREATE TRIGGER trg_select BEFORE INSERT ON t FOR EACH ROW SELECT 1;",
		Body:             "SELECT 1",
		State:            model.StatePublic,
		SQLMode:          tk.Session().GetSessionVars().SQLMode,
		CreatedTimestamp: 1,
	}

	mustSetTableTriggersAndReload(t, dom, store, db.ID, "test", "t", []*model.TriggerInfo{trig})

	refreshSessionAndUseTestDB(tk, false)
	require.NotPanics(t, func() {
		tk.MustGetErrCode("insert into t values (1)", mysql.ErrSpNoRetset)
	})
	tk.MustQuery("select a from t").Check(testkit.Rows())
}

func TestTriggerExecutionOrderAndActionOrder(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")
	tk.MustExec("create table log(seq int auto_increment primary key, msg varchar(20))")

	dom := mustGetDomainWithInfoSchemaV1(t, tk, store)
	db := mustGetDBInfo(t, dom, "test")

	makeTrig := func(name string, createdTS uint64, order ast.TriggerOrder, createSQL string) *model.TriggerInfo {
		return &model.TriggerInfo{
			Name:             pmodel.NewCIStr(name),
			Timing:           ast.TriggerTimingBefore,
			Event:            ast.TriggerEventInsert,
			Table:            pmodel.NewCIStr("t"),
			Order:            order,
			CreateSQL:        createSQL,
			Body:             "INSERT INTO log(msg) VALUES ('" + name + "')",
			State:            model.StatePublic,
			SQLMode:          tk.Session().GetSessionVars().SQLMode,
			CreatedTimestamp: createdTS,
		}
	}

	trg1 := makeTrig("trg1", 1, ast.TriggerOrder{}, "CREATE TRIGGER trg1 BEFORE INSERT ON t FOR EACH ROW INSERT INTO log(msg) VALUES ('trg1');")
	trg2 := makeTrig("trg2", 2, ast.TriggerOrder{}, "CREATE TRIGGER trg2 BEFORE INSERT ON t FOR EACH ROW INSERT INTO log(msg) VALUES ('trg2');")
	trg3 := makeTrig("trg3", 3, ast.TriggerOrder{OrderType: ast.TriggerOrderFollows, OtherTriggerName: pmodel.NewCIStr("trg1")},
		"CREATE TRIGGER trg3 BEFORE INSERT ON t FOR EACH ROW FOLLOWS trg1 INSERT INTO log(msg) VALUES ('trg3');")
	trg4 := makeTrig("trg4", 4, ast.TriggerOrder{OrderType: ast.TriggerOrderFollows, OtherTriggerName: pmodel.NewCIStr("trg1")},
		"CREATE TRIGGER trg4 BEFORE INSERT ON t FOR EACH ROW FOLLOWS trg1 INSERT INTO log(msg) VALUES ('trg4');")

	mustSetTableTriggersAndReload(t, dom, store, db.ID, "test", "t", []*model.TriggerInfo{trg2, trg1, trg3, trg4})

	refreshSessionAndUseTestDB(tk, false)

	tk.MustExec("insert into t values (1)")
	tk.MustQuery("select msg from log order by seq").Check(testkit.Rows("trg1", "trg4", "trg3", "trg2"))
	tk.MustQuery("select trigger_name, action_order from information_schema.triggers " +
		"where trigger_schema='test' and event_object_table='t' and action_timing='BEFORE' and event_manipulation='INSERT' " +
		"order by action_order",
	).Check(testkit.Rows("trg1 1", "trg4 2", "trg3 3", "trg2 4"))
}

func TestTriggerResetStmtCtxDoesNotLeakStmtTypeFlags(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("create table u(id int primary key, v int)")
	tk.MustExec("insert into u values (1, 0)")

	dom := mustGetDomainWithInfoSchemaV1(t, tk, store)
	db := mustGetDBInfo(t, dom, "test")

	createSQL := "CREATE TRIGGER trg_update_u AFTER INSERT ON t FOR EACH ROW UPDATE u SET v = v + 1 WHERE id = 1;"
	trig := &model.TriggerInfo{
		Name:      pmodel.NewCIStr("trg_update_u"),
		Timing:    ast.TriggerTimingAfter,
		Event:     ast.TriggerEventInsert,
		Table:     pmodel.NewCIStr("t"),
		CreateSQL: createSQL,
		Body:      "UPDATE u SET v = v + 1 WHERE id = 1",
		State:     model.StatePublic,
		SQLMode:   tk.Session().GetSessionVars().SQLMode,
	}

	mustSetTableTriggersAndReload(t, dom, store, db.ID, "test", "t", []*model.TriggerInfo{trig})

	refreshSessionAndUseTestDB(tk, true)

	tk.MustExec("insert into t values (1)")
	stmtCtx := tk.Session().GetSessionVars().StmtCtx
	require.True(t, stmtCtx.InInsertStmt)
	require.False(t, stmtCtx.InUpdateStmt)
	require.False(t, stmtCtx.InDeleteStmt)
	require.False(t, stmtCtx.InSelectStmt)

	tk.MustQuery("select v from u where id = 1").Check(testkit.Rows("1"))
}
