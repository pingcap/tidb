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
	"fmt"
	"testing"

	gluemock "github.com/pingcap/tidb/br/pkg/gluetidb/mock"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/utils"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

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
		require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas))
		tk.MustQuery("select c from routed").Check(testkit.Rows("target"))
		tk.MustQuery("select c from unrelated").Check(testkit.Rows("untouched"))
		tk.MustQuery("select table_id,column_id,restrict_on from mysql.tidb_masking_policy where table_name='routed'").Check(testkit.Rows(fmt.Sprintf("%d 1 INSERT_INTO_SELECT", target.Meta().ID)))
		tk.MustQuery("select policy_id from " + stageName).Check(testkit.Rows("321"))
	}
	// Disables and drops must replace the previous policy set as well.
	tk.MustExec("update " + stageName + " set status='DISABLED'")
	require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas))
	tk.MustQuery("select c from routed").Check(testkit.Rows("secret"))
	tk.MustExec("delete from " + stageName)
	require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas))
	tk.MustQuery("show masking policies for routed").Check(testkit.Rows())
	tk.MustQuery("select c from unrelated").Check(testkit.Rows("untouched"))
	// New policies created entirely after the baseline are also rebuilt.
	tk.MustExec("insert into " + stageName + " (policy_id,policy_name,db_name,table_name,table_id,column_name,column_id,expression,status,masking_type,created_at,updated_at,created_by) values (322,'new_policy','upstream','original',42,'c',1,'MASK_FULL(`upstream`.`original`.`c`)','ENABLED','MASK_FULL',now(),now(),'root')")
	require.NoError(t, task.RestoreMaskingPoliciesAfterLogReplayForTest(ctx, dom, se, schemas))
	tk.MustQuery("select c from routed").Check(testkit.Rows("XXXXXX"))

	tk.MustExec("drop table " + stageName)
	require.ErrorContains(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, withSysTable, false, true), "staging table is missing")
	// A policy table introduced after the full snapshot has an empty baseline.
	systemDB.TableMap[99].TableID = 888888
	require.NoError(t, task.PrepareMaskingPolicyStagingForTest(ctx, dom, se, mapping, withSysTable, true, true))
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
