// Copyright 2026 PingCAP, Inc.
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

package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/br/pkg/glue"
	gluemock "github.com/pingcap/tidb/br/pkg/gluetidb/mock"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

type interruptedMaskingReplaySession struct {
	glue.Session
	failAt int
	calls  int
	cancel context.CancelFunc
}

func (s *interruptedMaskingReplaySession) Execute(ctx context.Context, sql string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.Contains(sql, "DROP MASKING POLICY") || strings.HasPrefix(sql, "CREATE OR REPLACE MASKING POLICY") {
		s.calls++
		if s.calls == s.failAt {
			return errors.New("injected policy replay failure")
		}
	}
	err := s.Session.Execute(ctx, sql)
	if err == nil && s.calls == 1 && s.cancel != nil {
		s.cancel()
	}
	return err
}

func TestRestoreMaskingPoliciesFailureKeepsTablesProtected(t *testing.T) {
	for _, failure := range []string{"drop", "create", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table replay_target(c varchar(20), d varchar(20))")
			tk.MustExec("insert into replay_target values ('secret','private')")
			tk.MustExec("create masking policy old_c on replay_target(c) as mask_full(c)")
			tk.MustExec("create masking policy old_d on replay_target(d) as mask_full(d)")
			tk.MustExec("create table unrelated(c varchar(20))")
			tk.MustExec("insert into unrelated values ('public')")
			tk.MustExec("prepare cached_read from 'select c from replay_target'")
			tk.MustExec("execute cached_read")
			stageName := snapclient.MaskingPolicyStagingTableName(123)
			stageSQL := utils.EncloseDBAndTable(snapclient.MaskingPolicyStagingDB, stageName)
			tk.MustExec("create table " + stageSQL + " like mysql.tidb_masking_policy")
			tk.MustExec("insert into " + stageSQL + " (policy_id,policy_name,db_name,table_name,table_id,column_name,column_id,expression,status,masking_type,created_at,updated_at,created_by) values (321,'new_c','upstream','src',42,'c',1,'MASK_FULL(c)','ENABLED','MASK_FULL',now(),now(),'root')")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			target, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr("test"), pmodel.NewCIStr("replay_target"))
			require.NoError(t, err)
			stage, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(snapclient.MaskingPolicyStagingDB), pmodel.NewCIStr(stageName))
			require.NoError(t, err)
			userDB := stream.NewDBReplace("upstream", target.Meta().DBID)
			userDB.TableMap[42] = stream.NewTableReplace("replay_target", target.Meta().ID)
			sysDB := stream.NewDBReplace(mysql.SystemDB, stage.Meta().DBID)
			sysDB.TableMap[99] = stream.NewTableReplace("tidb_masking_policy", stage.Meta().ID)
			schemas := stream.NewSchemasReplace(map[int64]*stream.DBReplace{1: userDB, 2: sysDB}, false, nil, 0, nil, false)
			g := &gluemock.MockGlue{}
			g.SetSession(tk.Session())
			se, err := g.CreateSession(store)
			require.NoError(t, err)
			interrupted := &interruptedMaskingReplaySession{Session: se, failAt: 1}
			if failure == "create" {
				interrupted.failAt = 2
			} else if failure == "cancel" {
				interrupted.failAt = 0
				interrupted.cancel = cancel
			}
			require.Error(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, interrupted, schemas, false, 0))
			tk.MustGetErrCode("select c from replay_target", errno.ErrProtectedTableMode)
			tk.MustGetErrCode("execute cached_read", errno.ErrProtectedTableMode)
			tk.MustQuery("select c from unrelated").Check(testkit.Rows("public"))
			require.False(t, tk.Session().GetSessionVars().InMaskingPolicyRestore)
			require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(context.Background(), dom, se, schemas, false, 0))
			tk.MustGetErrCode("select c from replay_target", errno.ErrProtectedTableMode)
			require.NoError(t, se.AlterTableMode(context.Background(), target.Meta().DBID, target.Meta().ID, model.TableModeNormal))
			tk.MustQuery("select c from replay_target").Check(testkit.Rows("XXXXXX"))
		})
	}
}

func TestRestoreMaskingPoliciesAfterLogReplay(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table routed(c varchar(20))")
	tk.MustExec("insert into routed values ('secret')")
	tk.MustExec("create masking policy p on routed(c) as 'baseline' disable")
	tk.MustExec("create table unrelated(c varchar(20))")
	tk.MustExec("insert into unrelated values ('other')")
	tk.MustExec("create masking policy p on unrelated(c) as 'untouched'")
	stageTableName := snapclient.MaskingPolicyStagingTableName(123)
	stageName := utils.EncloseDBAndTable(snapclient.MaskingPolicyStagingDB, stageTableName)
	tk.MustExec("create table " + stageName + " like mysql.tidb_masking_policy")
	tk.MustExec("insert into " + stageName + " (policy_id,policy_name,db_name,table_name,table_id,column_name,column_id,expression,status,masking_type,restrict_on,created_at,updated_at,created_by) values (321,'p','upstream','original',42,'c',1,'''target''','ENABLED','CUSTOM','INSERT_INTO_SELECT',now(),now(),'root')")
	ctx := context.Background()
	target, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr("test"), pmodel.NewCIStr("routed"))
	require.NoError(t, err)
	stage, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(snapclient.MaskingPolicyStagingDB), pmodel.NewCIStr(stageTableName))
	require.NoError(t, err)
	userDB := stream.NewDBReplace("upstream", target.Meta().DBID)
	userDB.TableMap[42] = stream.NewTableReplace("routed", target.Meta().ID)
	userDB.TableMap[43] = stream.NewTableReplace("unrelated", 999999)
	userDB.TableMap[43].FilteredOut = true
	systemDB := stream.NewDBReplace(mysql.SystemDB, 10)
	systemDB.FilteredOut = true
	systemDB.TableMap[99] = stream.NewTableReplace("tidb_masking_policy", stage.Meta().ID)
	systemDB.TableMap[99].FilteredOut = true
	mapping := stream.NewTableMappingManager()
	mapping.DBReplaceMap = map[int64]*stream.DBReplace{1: userDB, 2: systemDB}
	withSysTable := true
	g := &gluemock.MockGlue{}
	g.SetSession(tk.Session())
	se, err := g.CreateSession(store)
	require.NoError(t, err)
	require.NoError(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, withSysTable, false, true))
	schemas := stream.NewSchemasReplace(mapping.DBReplaceMap, false, nil, 0, nil, false)
	rules := task.BuildRewriteRulesForTest(schemas)
	require.NotContains(t, rules, int64(99))
	task.AddMaskingPolicyRewriteRulesForTest(schemas, rules)
	require.Equal(t, stage.Meta().ID, rules[99].NewTableID)
	require.NotContains(t, rules, int64(43))
	// The target-time stage enables a baseline-disabled policy and rebinds it
	// by source IDs even when the target database and table names differ.
	for range 2 { // Rebuilding after a checkpoint retry must be idempotent.
		require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, false, 0))
		tk.MustQuery("select c from routed").Check(testkit.Rows("target"))
		tk.MustQuery("select c from unrelated").Check(testkit.Rows("untouched"))
		tk.MustQuery("select table_id,column_id,restrict_on from mysql.tidb_masking_policy where table_name='routed'").Check(testkit.Rows(fmt.Sprintf("%d 1 INSERT_INTO_SELECT", target.Meta().ID)))
		tk.MustQuery("select policy_id from " + stageName).Check(testkit.Rows("321"))
	}
	// Disables and drops must replace the previous policy set as well.
	tk.MustExec("update " + stageName + " set status='DISABLED'")
	require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, false, 0))
	tk.MustQuery("select c from routed").Check(testkit.Rows("secret"))
	tk.MustExec("delete from " + stageName)
	require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, false, 0))
	tk.MustQuery("show masking policies for routed").Check(testkit.Rows())
	tk.MustQuery("select c from unrelated").Check(testkit.Rows("untouched"))
	// New policies created entirely after the baseline are also rebuilt.
	tk.MustExec("insert into " + stageName + " (policy_id,policy_name,db_name,table_name,table_id,column_name,column_id,expression,status,masking_type,created_at,updated_at,created_by) values (322,'new_policy','upstream','original',42,'c',1,'MASK_FULL(`upstream`.`original`.`c`)','ENABLED','MASK_FULL',now(),now(),'root')")
	require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, false, 0))
	tk.MustQuery("select c from routed").Check(testkit.Rows("XXXXXX"))

	tk.MustExec("drop table " + stageName)
	require.ErrorContains(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, withSysTable, false, true), "staging table is missing")
	// A policy table introduced after the full snapshot has an empty baseline.
	systemDB.TableMap[99].TableID = 888888
	require.ErrorContains(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, withSysTable, true, true), "staging table is missing")
	require.NoError(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, withSysTable, true, false))
	require.NotEqual(t, int64(888888), systemDB.TableMap[99].TableID)
	require.NoError(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, withSysTable, false, true))
	// Never rewrite source logs directly into the live policy table.
	live, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr("mysql"), pmodel.NewCIStr("tidb_masking_policy"))
	require.NoError(t, err)
	systemDB.TableMap[99].TableID = live.Meta().ID
	require.ErrorContains(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, withSysTable, false, true), "does not reference a retained staging table")
	withSysTable = false
	require.NoError(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, withSysTable, false, true))
}

func TestRestoreMaskingPolicyStagingCheckpointPhases(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database __TiDB_BR_Temporary_mysql")
	tk.MustExec("create table __TiDB_BR_Temporary_mysql.tidb_masking_policy (policy_id bigint primary key nonclustered, table_id bigint)")
	tk.MustExec("set tidb_opt_write_row_id=1")
	tk.MustExec("insert into __TiDB_BR_Temporary_mysql.tidb_masking_policy (_tidb_rowid,policy_id,table_id) values (456,123,42)")
	ctx := context.Background()
	tempDB := utils.TemporaryDBName(mysql.SystemDB)
	staged, err := dom.InfoSchema().TableByName(ctx, tempDB, pmodel.NewCIStr("tidb_masking_policy"))
	require.NoError(t, err)
	source := stream.NewDBReplace(mysql.SystemDB, staged.Meta().DBID)
	source.FilteredOut = true
	source.TableMap[99] = stream.NewTableReplace("tidb_masking_policy", staged.Meta().ID)
	source.TableMap[99].FilteredOut = true
	mapping := stream.NewTableMappingManager()
	mapping.DBReplaceMap[1] = source
	g := &gluemock.MockGlue{}
	g.SetSession(tk.Session())
	se, err := g.CreateSession(store)
	require.NoError(t, err)
	// Until the ID map is saved, a retry must find the snapshot's original name.
	require.NoError(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, true, false, false))
	require.True(t, dom.InfoSchema().TableExists(tempDB, pmodel.NewCIStr("tidb_masking_policy")))
	privateName := pmodel.NewCIStr(snapclient.MaskingPolicyStagingTableName(staged.Meta().ID))
	require.False(t, dom.InfoSchema().TableExists(pmodel.NewCIStr(mysql.SystemDB), privateName))
	for range 2 {
		require.NoError(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, true, false, true))
		retained, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(mysql.SystemDB), privateName)
		require.NoError(t, err)
		require.Equal(t, staged.Meta().ID, retained.Meta().ID, "do not replace the physical table or its MVCC versions")
		require.Equal(t, staged.Meta().ID, source.TableMap[99].TableID)
		tk.MustQuery("select _tidb_rowid,policy_id,table_id from " + utils.EncloseDBAndTable(mysql.SystemDB, privateName.O)).Check(testkit.Rows("456 123 42"))
	}
	require.False(t, dom.InfoSchema().TableExists(tempDB, pmodel.NewCIStr("tidb_masking_policy")))
}

func TestRestoreMaskingPolicyNewLogOnlyStage(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	g := &gluemock.MockGlue{}
	g.SetSession(tk.Session())
	se, err := g.CreateSession(store)
	require.NoError(t, err)
	mapping := stream.NewTableMappingManager()
	mapping.SetFromPiTRIDMap()
	mapping.MergeBaseDBReplace(map[int64]*stream.DBReplace{1: stream.NewDBReplace(mysql.SystemDB, 1)})
	// The predecessor map has no policy table. Observe its committed TableInfo
	// in this interval, rather than relying on an auto-ID-only placeholder.
	startTS := uint64(1<<60) + 10
	info, err := json.Marshal(&model.TableInfo{ID: 99, Name: pmodel.NewCIStr("tidb_masking_policy")})
	require.NoError(t, err)
	collector := stream.NewTableHistoryManager()
	require.NoError(t, mapping.ParseMetaKvAndUpdateIdMapping(&kv.Entry{
		Key: utils.EncodeTxnMetaKey(meta.DBkey(1), meta.TableKey(99), startTS), Value: info,
	}, consts.DefaultCF, startTS, collector))
	require.NoError(t, mapping.ParseMetaKvAndUpdateIdMapping(&kv.Entry{
		Key: utils.EncodeTxnMetaKey(meta.DBkey(1), meta.TableKey(99), startTS+1), Value: codec.EncodeUvarint([]byte{'P'}, startTS),
	}, consts.WriteCF, startTS+1, collector))
	source := mapping.DBReplaceMap[1].TableMap[99]
	source.TableID = 888888
	ctx := context.Background()
	// Committed metadata alone may come from a DDL on a policy table that the
	// previous restore left out. Without the job that created the table in
	// this interval, an empty baseline is not proven.
	require.ErrorContains(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, true, false, false), "staging table is missing")
	createJob, err := (&model.Job{Version: model.JobVersion1, Type: model.ActionCreateTable, TableID: 99, State: model.JobStateSynced}).Encode(true)
	require.NoError(t, err)
	require.NoError(t, mapping.ParseMetaKvAndUpdateIdMapping(&kv.Entry{
		Key: utils.EncodeTxnMetaKey([]byte("DDLJobHistory"), []byte("job"), startTS), Value: createJob,
	}, consts.DefaultCF, startTS, collector))
	require.NoError(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, true, false, false))
	stageName := utils.EncloseDBAndTable(snapclient.MaskingPolicyStagingDB, snapclient.MaskingPolicyStagingTableName(888888))
	tk.MustQuery("select count(*) from " + stageName).Check(testkit.Rows("0"))
	stageID := source.TableID
	require.NoError(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, true, false, true))
	require.Equal(t, stageID, source.TableID)
	// Once saved, this source is part of the baseline. Losing its stage must
	// never be repaired with an empty table, even on a subsequent fresh run.
	base := stream.NewDBReplace(mysql.SystemDB, 1)
	base.TableMap[99] = stream.NewTableReplace("tidb_masking_policy", stageID)
	mapping.MergeBaseDBReplace(map[int64]*stream.DBReplace{1: base})
	tk.MustExec("drop table " + stageName)
	for _, saved := range []bool{false, true} {
		for _, fullSnapshot := range []bool{false, true} {
			require.ErrorContains(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, true, fullSnapshot, saved), "staging table is missing")
		}
	}
	unknown := stream.NewTableMappingManager()
	unknown.DBReplaceMap = mapping.DBReplaceMap
	require.ErrorContains(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, unknown, true, false, false), "staging table is missing")
}

func TestRestoreMaskingPoliciesInRestoreMode(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table protected(c varchar(20))")
	tk.MustExec("insert into protected values ('secret')")
	tk.MustExec("create masking policy p on protected(c) as 'baseline'")
	stageName := snapclient.MaskingPolicyStagingTableName(123)
	qualified := utils.EncloseDBAndTable(snapclient.MaskingPolicyStagingDB, stageName)
	tk.MustExec("create table " + qualified + " like mysql.tidb_masking_policy")
	tk.MustExec("insert into " + qualified + " (policy_id,policy_name,db_name,table_name,table_id,column_name,column_id,expression,status,masking_type,created_at,updated_at,created_by) values (321,'p','upstream','original',42,'c',1,'MASK_FULL(`c`)','ENABLED','MASK_FULL',now(),now(),'root')")
	ctx := context.Background()
	tbl, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr("test"), pmodel.NewCIStr("protected"))
	require.NoError(t, err)
	stage, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(snapclient.MaskingPolicyStagingDB), pmodel.NewCIStr(stageName))
	require.NoError(t, err)
	args := &model.AlterTableModeArgs{SchemaID: tbl.Meta().DBID, TableID: tbl.Meta().ID, TableMode: model.TableModeRestore}
	require.NoError(t, dom.DDLExecutor().AlterTableMode(tk.Session(), args))
	tk.MustGetErrCode("select c from protected", errno.ErrProtectedTableMode)
	userDB := stream.NewDBReplace("upstream", tbl.Meta().DBID)
	userDB.TableMap[42] = stream.NewTableReplace("protected", tbl.Meta().ID)
	sysDB := stream.NewDBReplace(mysql.SystemDB, stage.Meta().DBID)
	sysDB.TableMap[99] = stream.NewTableReplace("tidb_masking_policy", stage.Meta().ID)
	schemas := stream.NewSchemasReplace(map[int64]*stream.DBReplace{1: userDB, 2: sysDB}, false, nil, 0, nil, false)
	g := &gluemock.MockGlue{}
	g.SetSession(tk.Session())
	se, err := g.CreateSession(store)
	require.NoError(t, err)
	for range 2 {
		require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, false, 0))
		require.False(t, tk.Session().GetSessionVars().InMaskingPolicyRestore)
		tk.MustGetErrCode("select c from protected", errno.ErrProtectedTableMode)
		tk.MustGetErrCode("create masking policy other on protected(c) as c", errno.ErrProtectedTableMode)
	}
	// A replay-scoped flag must not authorize unrelated DDL or reads.
	tk.Session().GetSessionVars().InMaskingPolicyRestore = true
	require.ErrorContains(t, se.Execute(ctx, "select c from protected"), "is in mode Restore")
	require.ErrorContains(t, se.Execute(ctx, "alter table protected add column d int"), "is in mode Restore")
	tk.Session().GetSessionVars().InMaskingPolicyRestore = false
	args.TableMode = model.TableModeNormal
	require.NoError(t, dom.DDLExecutor().AlterTableMode(tk.Session(), args))
	tk.MustQuery("select c from protected").Check(testkit.Rows("XXXXXX"))
}

func TestRestoreMaskingPoliciesKeepsBaselineUntilReplaced(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table protected(a varchar(20), b varchar(20), c varchar(20))")
	tk.MustExec("insert into protected values ('secret_a', 'secret_b', 'secret_c')")
	tk.MustExec("create masking policy p_a on protected(a) as 'baseline_a'")
	tk.MustExec("create masking policy p_b on protected(b) as 'baseline_b'")
	// Another selected table is in Restore mode for a reason unknown to the
	// rebuild, and its policies do not change.
	tk.MustExec("create table bystander(a varchar(20))")
	tk.MustExec("create masking policy p on bystander(a) as 'kept'")
	stageName := snapclient.MaskingPolicyStagingTableName(123)
	qualified := utils.EncloseDBAndTable(snapclient.MaskingPolicyStagingDB, stageName)
	tk.MustExec("create table " + qualified + " like mysql.tidb_masking_policy")
	stagePolicy := func(id int, name, column string, columnID int, expr string) {
		tk.MustExec("insert into "+qualified+" (policy_id,policy_name,db_name,table_name,table_id,column_name,column_id,expression,status,masking_type,created_at,updated_at,created_by) values (?,?,'upstream','original',42,?,?,?,'ENABLED','CUSTOM',now(),now(),'root')",
			id, name, column, columnID, expr)
	}
	tk.MustExec("insert into "+qualified+" (policy_id,policy_name,db_name,table_name,table_id,column_name,column_id,expression,status,masking_type,created_at,updated_at,created_by) values (100,'p','upstream','bystander',43,'a',1,?,'ENABLED','CUSTOM',now(),now(),'root')", "'kept'")
	const fenceRecord = "select table_id, restore_id from mysql.tidb_pitr_masking_policy_fence"
	ctx := context.Background()
	tbl, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr("test"), pmodel.NewCIStr("protected"))
	require.NoError(t, err)
	stage, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(snapclient.MaskingPolicyStagingDB), pmodel.NewCIStr(stageName))
	require.NoError(t, err)
	bystander, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr("test"), pmodel.NewCIStr("bystander"))
	require.NoError(t, err)
	setMode := func(id int64, mode model.TableMode) {
		args := &model.AlterTableModeArgs{SchemaID: tbl.Meta().DBID, TableID: id, TableMode: mode}
		require.NoError(t, dom.DDLExecutor().AlterTableMode(tk.Session(), args))
	}
	setMode(bystander.Meta().ID, model.TableModeRestore)
	userDB := stream.NewDBReplace("upstream", tbl.Meta().DBID)
	userDB.TableMap[42] = stream.NewTableReplace("protected", tbl.Meta().ID)
	userDB.TableMap[43] = stream.NewTableReplace("bystander", bystander.Meta().ID)
	sysDB := stream.NewDBReplace(mysql.SystemDB, stage.Meta().DBID)
	sysDB.TableMap[99] = stream.NewTableReplace("tidb_masking_policy", stage.Meta().ID)
	schemas := stream.NewSchemasReplace(map[int64]*stream.DBReplace{1: userDB, 2: sysDB}, false, nil, 0, nil, false)
	g := &gluemock.MockGlue{}
	g.SetSession(tk.Session())
	se, err := g.CreateSession(store)
	require.NoError(t, err)

	// The table is readable (Normal mode) during the rebuild. When a policy
	// cannot be rebuilt, the baseline policies must still be enforced.
	stagePolicy(1, "p_a", "a", 1, "'target_a'")
	stagePolicy(2, "p_b", "b", 2, "'target_b'")
	stagePolicy(3, "a_broken", "c", 3, "no_such_function(c)")
	require.ErrorContains(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, true, 7), "cannot rebuild masking policy after PiTR log replay")
	rows := tk.MustQuery("select a, b from protected").Rows()
	require.Contains(t, []string{"baseline_a", "target_a"}, rows[0][0])
	require.Contains(t, []string{"baseline_b", "target_b"}, rows[0][1])

	// A successor that collides with a baseline policy on the column needs the
	// baseline dropped first. The table is fenced in Restore mode before that,
	// so a failure in between cannot expose the column.
	tk.MustExec("delete from " + qualified + " where table_id = 42")
	stagePolicy(1, "p_a", "a", 1, "'target_a'")
	stagePolicy(4, "p_b_renamed", "b", 2, "no_such_function(b)")
	require.ErrorContains(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, true, 7), "cannot rebuild masking policy after PiTR log replay")
	tk.MustQuery("select policy_name from mysql.tidb_masking_policy where table_name='protected'").Check(testkit.Rows("p_a"))
	tk.MustGetErrCode("select a, b from protected", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("insert into protected values ('x', 'y', 'z')", errno.ErrProtectedTableMode)

	// The fence is recorded, so only the rebuild that raised it lowers it.
	tk.MustQuery(fenceRecord).Check(testkit.Rows(fmt.Sprintf("%d 7", tbl.Meta().ID)))
	// Another restore task that selects the table cannot prove the fence to be
	// its own: it is refused before it changes any policy. So is a restore
	// without a task ID, which cannot be told from another one.
	tk.MustExec("insert into mysql.tidb_pitr_masking_policy_fence values (987654321, 5)")
	tk.MustExec("delete from " + qualified + " where table_id = 42")
	stagePolicy(1, "p_a", "a", 1, "'target_a'")
	stagePolicy(4, "p_b_renamed", "b", 2, "'target_b'")
	for _, otherTask := range []uint64{8, 0} {
		require.ErrorContains(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, true, otherTask),
			"another restore task was replacing their policies and did not finish: protected (restore task 7)")
		tk.MustQuery("select policy_name from mysql.tidb_masking_policy where table_name='protected'").Check(testkit.Rows("p_a"))
	}
	tk.MustGetErrCode("select a, b from protected", errno.ErrProtectedTableMode)
	tk.MustQuery(fenceRecord + " order by table_id").Check(testkit.Rows(fmt.Sprintf("%d 7", tbl.Meta().ID), "987654321 5"))

	// A retry with a rebuildable policy set replaces the whole set: p_a keeps
	// its name and column, p_b is renamed on the same column, the name p_c is
	// new, and nothing stale survives. A filtered restore reopens every
	// selected table itself afterwards, so the rebuild leaves the fence alone.
	tk.MustExec("delete from " + qualified + " where table_id = 42")
	stagePolicy(1, "p_a", "a", 1, "'target_a'")
	stagePolicy(4, "p_b_renamed", "b", 2, "'target_b'")
	stagePolicy(5, "p_c", "c", 3, "'target_c'")
	require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, false, 0))
	tk.MustGetErrCode("select a, b, c from protected", errno.ErrProtectedTableMode)
	// Otherwise the rebuild reopens the table it fenced in the failed attempt,
	// and not the table somebody else put in Restore mode.
	for range 2 {
		require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, true, 7))
		tk.MustQuery("select a, b, c from protected").Check(testkit.Rows("target_a target_b target_c"))
		tk.MustQuery("select policy_name from mysql.tidb_masking_policy where table_name='protected' order by policy_name").
			Check(testkit.Rows("p_a", "p_b_renamed", "p_c"))
		tk.MustGetErrCode("select a from bystander", errno.ErrProtectedTableMode)
		tk.MustQuery("show tables in mysql like 'tidb_pitr_masking_policy_fence'").Check(testkit.Rows())
	}

	// Without a task ID the fence could not be attributed afterwards, so a swap
	// that needs one is refused while the old policies are still in place.
	tk.MustExec("delete from " + qualified + " where table_id = 42")
	stagePolicy(6, "p_a", "c", 3, "'moved'")
	require.ErrorContains(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, true, 0), "needs a restore task ID")
	tk.MustQuery("select a, b, c from protected").Check(testkit.Rows("target_a target_b target_c"))
	tk.MustQuery("show tables in mysql like 'tidb_pitr_masking_policy_fence'").Check(testkit.Rows())

	// A policy name that moves to another column, and a column that loses its
	// policy at the target time. The fence of a successful swap is lowered by
	// the rebuild that owns it.
	tk.MustExec("delete from " + qualified + " where table_id = 42")
	stagePolicy(6, "p_a", "c", 3, "'moved'")
	require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, true, 7))
	tk.MustQuery("select a, b, c from protected").Check(testkit.Rows("secret_a secret_b moved"))
	tk.MustQuery("select policy_name, column_name from mysql.tidb_masking_policy where table_name='protected'").Check(testkit.Rows("p_a c"))
	tk.MustQuery("show tables in mysql like 'tidb_pitr_masking_policy_fence'").Check(testkit.Rows())
	tk.MustGetErrCode("select a from bystander", errno.ErrProtectedTableMode)

	// In a filtered restore a table fenced by a successful swap stays fenced
	// until the caller reopens the selected tables, and nothing is recorded.
	tk.MustExec("delete from " + qualified + " where table_id = 42")
	stagePolicy(7, "p_c_renamed", "c", 3, "'filtered'")
	require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas, false, 0))
	tk.MustGetErrCode("select a, b, c from protected", errno.ErrProtectedTableMode)
	tk.MustQuery("show tables in mysql like 'tidb_pitr_masking_policy_fence'").Check(testkit.Rows())
	setMode(tbl.Meta().ID, model.TableModeNormal)
	tk.MustQuery("select a, b, c from protected").Check(testkit.Rows("secret_a secret_b filtered"))
	setMode(bystander.Meta().ID, model.TableModeNormal)
	tk.MustQuery("select a from bystander").Check(testkit.Rows())
}

// The proof that a table is new to a log-only restore is read from the history
// that a real batch-create job leaves behind, in both job versions.
func TestTableNewToBaseFromBatchCreateHistory(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	original := model.GetJobVerInUse()
	t.Cleanup(func() { model.SetJobVerInUse(original) })
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	testDB, ok := dom.InfoSchema().SchemaByName(pmodel.NewCIStr("test"))
	require.True(t, ok)
	for _, version := range []model.JobVersion{model.JobVersion1, model.JobVersion2} {
		model.SetJobVerInUse(version)
		names := []string{fmt.Sprintf("batch_a_v%d", version), fmt.Sprintf("batch_b_v%d", version)}
		// The DDL sanity check of tests parses the statement text of the job.
		tk.Session().SetValue(sessionctx.QueryString, fmt.Sprintf("CREATE TABLE %s (id INT); CREATE TABLE %s (id INT)", names[0], names[1]))
		require.NoError(t, dom.DDLExecutor().BatchCreateTableWithInfo(tk.Session(), pmodel.NewCIStr("test"), []*model.TableInfo{
			{Name: pmodel.NewCIStr(names[0])}, {Name: pmodel.NewCIStr(names[1])},
		}, ddl.WithOnExist(ddl.OnExistError)))
		jobRow := tk.MustQuery("admin show ddl jobs 1").Rows()[0]
		require.Equal(t, "create tables", jobRow[3])
		jobID, err := strconv.ParseInt(fmt.Sprint(jobRow[0]), 10, 64)
		require.NoError(t, err)
		var history *model.Job
		require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
			var readErr error
			history, readErr = meta.NewMutator(txn).GetHistoryDDLJob(jobID)
			return readErr
		}))
		require.Equal(t, version, history.Version)
		require.Equal(t, "null", string(history.RawArgs))
		historyValue, err := history.Encode(false)
		require.NoError(t, err)

		mapping := stream.NewTableMappingManager()
		mapping.SetFromPiTRIDMap()
		mapping.MergeBaseDBReplace(map[int64]*stream.DBReplace{testDB.ID: stream.NewDBReplace("test", testDB.ID)})
		collector := stream.NewTableHistoryManager()
		var tableIDs []int64
		for _, name := range names {
			tbl, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr("test"), pmodel.NewCIStr(name))
			require.NoError(t, err)
			tableIDs = append(tableIDs, tbl.Meta().ID)
			info, err := json.Marshal(tbl.Meta())
			require.NoError(t, err)
			metaStartTS := history.StartTS + 1
			require.NoError(t, mapping.ParseMetaKvAndUpdateIdMapping(&kv.Entry{
				Key: utils.EncodeTxnMetaKey(meta.DBkey(testDB.ID), meta.TableKey(tbl.Meta().ID), metaStartTS), Value: info,
			}, consts.DefaultCF, metaStartTS, collector))
			require.NoError(t, mapping.ParseMetaKvAndUpdateIdMapping(&kv.Entry{
				Key:   utils.EncodeTxnMetaKey(meta.DBkey(testDB.ID), meta.TableKey(tbl.Meta().ID), metaStartTS+1),
				Value: codec.EncodeUvarint([]byte{'P'}, metaStartTS),
			}, consts.WriteCF, metaStartTS+1, collector))
			require.False(t, mapping.IsTableNewToBase(tbl.Meta().ID, false, history.StartTS))
		}
		require.NoError(t, mapping.ParseMetaKvAndUpdateIdMapping(&kv.Entry{
			Key: utils.EncodeTxnMetaKey([]byte("DDLJobHistory"), []byte("job"), history.StartTS+3), Value: historyValue,
		}, consts.DefaultCF, history.StartTS+3, collector))
		for _, tableID := range tableIDs {
			require.True(t, mapping.IsTableNewToBase(tableID, false, history.StartTS))
			require.False(t, mapping.IsTableNewToBase(tableID, false, history.StartTS+1))
		}
	}
}
