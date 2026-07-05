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

package session

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestNonTransactionalDMLHandleDescriptorSupportedShapes(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "use test")

	cases := []struct {
		name       string
		createSQL  string
		stmtSQL    string
		kind       nonTransactionalDMLHandleKind
		columnName string
	}{
		{
			name:       "tidb row id",
			createSQL:  "create table t_rowid(a int, b int)",
			stmtSQL:    "batch on _tidb_rowid limit 2 delete from t_rowid where b >= 0",
			kind:       nonTransactionalDMLHandleExtra,
			columnName: "_tidb_rowid",
		},
		{
			name:       "signed int clustered primary key",
			createSQL:  "create table t_int(id bigint primary key clustered, b int)",
			stmtSQL:    "batch on id limit 2 update t_int set b = b + 1 where b >= 0",
			kind:       nonTransactionalDMLHandleInt,
			columnName: "id",
		},
		{
			name:       "varchar binary common handle",
			createSQL:  "create table t_varchar(id varchar(128) collate utf8mb4_bin primary key clustered, b int)",
			stmtSQL:    "batch on id limit 2 delete from t_varchar where id >= 'v1:pacer_largepayload0001'",
			kind:       nonTransactionalDMLHandleCommonBinary,
			columnName: "id",
		},
		{
			name:       "varbinary common handle",
			createSQL:  "create table t_varbinary(id varbinary(128) primary key clustered, b int)",
			stmtSQL:    "batch on id limit 2 update t_varbinary set b = 10 where id >= x'76313a30303031'",
			kind:       nonTransactionalDMLHandleCommonBinary,
			columnName: "id",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			MustExec(t, se, "drop table if exists t_rowid, t_int, t_varchar, t_varbinary")
			MustExec(t, se, tt.createSQL)

			desc, err := buildNonTransactionalDMLHandleDescriptorForTest(t, se, tt.stmtSQL)
			require.NoError(t, err)
			require.Equal(t, tt.kind, desc.kind)
			require.Equal(t, tt.columnName, desc.columnName.Name.L)
		})
	}
}

func TestNonTransactionalDMLHandleDescriptorRejectedShapes(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "use test")

	cases := []struct {
		name      string
		createSQL string
		stmtSQL   string
		errText   string
	}{
		{
			name:      "unsigned clustered primary key",
			createSQL: "create table t_reject(id bigint unsigned primary key clustered, b int)",
			stmtSQL:   "batch on id limit 2 delete from t_reject",
			errText:   "doesn't support unsigned integer clustered primary keys",
		},
		{
			name:      "non binary varchar collation",
			createSQL: "create table t_reject(id varchar(64) collate utf8mb4_general_ci primary key clustered, b int)",
			stmtSQL:   "batch on id limit 2 delete from t_reject",
			errText:   "requires binary collation",
		},
		{
			name:      "composite common handle",
			createSQL: "create table t_reject(id varchar(64) collate utf8mb4_bin, b int, primary key(id, b) clustered)",
			stmtSQL:   "batch on id limit 2 delete from t_reject",
			errText:   "doesn't support composite clustered primary keys",
		},
		{
			name:      "partitioned table",
			createSQL: "create table t_reject(id bigint primary key clustered, b int) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))",
			stmtSQL:   "batch on id limit 2 delete from t_reject",
			errText:   "doesn't support partitioned tables",
		},
		{
			name:      "secondary index shard column",
			createSQL: "create table t_reject(id varchar(64) collate utf8mb4_bin, b int, key(id))",
			stmtSQL:   "batch on id limit 2 delete from t_reject",
			errText:   "requires _tidb_rowid or a clustered primary key",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			MustExec(t, se, "drop table if exists t_reject")
			MustExec(t, se, tt.createSQL)

			_, err := buildNonTransactionalDMLHandleDescriptorForTest(t, se, tt.stmtSQL)
			require.ErrorContains(t, err, tt.errText)
		})
	}
}

func TestNonTransactionalDMLBoundaryEncodeDecode(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "use test")

	cases := []struct {
		name      string
		createSQL string
		stmtSQL   string
		value     types.Datum
		check     func(*testing.T, types.Datum)
	}{
		{
			name:      "signed int",
			createSQL: "create table t_boundary(id bigint primary key clustered, b int)",
			stmtSQL:   "batch on id limit 2 delete from t_boundary",
			value:     types.NewIntDatum(-42),
			check: func(t *testing.T, got types.Datum) {
				require.Equal(t, int64(-42), got.GetInt64())
			},
		},
		{
			name:      "varchar binary common handle",
			createSQL: "create table t_boundary(id varchar(128) collate utf8mb4_bin primary key clustered, b int)",
			stmtSQL:   "batch on id limit 2 delete from t_boundary",
			value:     types.NewStringDatum("v1:pacer_largepayload0001"),
			check: func(t *testing.T, got types.Datum) {
				require.Equal(t, "v1:pacer_largepayload0001", got.GetString())
			},
		},
		{
			name:      "varbinary common handle",
			createSQL: "create table t_boundary(id varbinary(128) primary key clustered, b int)",
			stmtSQL:   "batch on id limit 2 delete from t_boundary",
			value:     types.NewBytesDatum([]byte("v1:\x00bytes")),
			check: func(t *testing.T, got types.Datum) {
				require.Equal(t, []byte("v1:\x00bytes"), got.GetBytes())
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			MustExec(t, se, "drop table if exists t_boundary")
			MustExec(t, se, tt.createSQL)
			desc, err := buildNonTransactionalDMLHandleDescriptorForTest(t, se, tt.stmtSQL)
			require.NoError(t, err)

			boundary, err := encodeNonTransactionalDMLBoundary(se.GetSessionVars().StmtCtx, desc, tt.value, true)
			require.NoError(t, err)
			require.True(t, boundary.hasValue)
			require.True(t, boundary.inclusive)
			require.NotEmpty(t, boundary.encoded)

			decoded, err := decodeNonTransactionalDMLBoundary(se.GetSessionVars().StmtCtx, desc, boundary.encoded, false)
			require.NoError(t, err)
			require.True(t, decoded.hasValue)
			require.False(t, decoded.inclusive)
			tt.check(t, decoded.value)
		})
	}
}

func TestNonTransactionalDMLRangeConditionUsesCheckpointExclusively(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "use test")
	MustExec(t, se, "create table t_condition(id varchar(128) collate utf8mb4_bin primary key clustered, b int)")
	desc, err := buildNonTransactionalDMLHandleDescriptorForTest(t, se, "batch on id limit 2 delete from t_condition")
	require.NoError(t, err)

	lower, err := encodeNonTransactionalDMLBoundary(se.GetSessionVars().StmtCtx, desc, types.NewStringDatum("v1:pacer_largepayload0001"), false)
	require.NoError(t, err)
	upper, err := encodeNonTransactionalDMLBoundary(se.GetSessionVars().StmtCtx, desc, types.NewStringDatum("v1:pacer_largepayload0100"), true)
	require.NoError(t, err)

	condition := buildNonTransactionalDMLRangeCondition(desc, &lower, &upper)
	sql := restoreNonTransactionalDMLExprForTest(t, condition)
	require.Contains(t, sql, "`id` > 'v1:pacer_largepayload0001'")
	require.Contains(t, sql, "`id` <= 'v1:pacer_largepayload0100'")
	require.True(t, strings.Contains(sql, "AND"), sql)
}

func TestNonTransactionalDMLRangeSelectWhereSQLIncludesRegionUpperBound(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "use test")
	MustExec(t, se, "create table t_select_where(id bigint primary key clustered, b int)")
	desc, err := buildNonTransactionalDMLHandleDescriptorForTest(t, se, "batch on id limit 2 delete from t_select_where where b > 0")
	require.NoError(t, err)
	lower, err := encodeNonTransactionalDMLBoundary(se.GetSessionVars().StmtCtx, desc, types.NewIntDatum(10), true)
	require.NoError(t, err)
	upper, err := encodeNonTransactionalDMLBoundary(se.GetSessionVars().StmtCtx, desc, types.NewIntDatum(20), false)
	require.NoError(t, err)

	whereSQL, err := buildNonTransactionalDMLRangeSelectWhereSQL(&nonTransactionalDMLRangeContext{
		Descriptor:       desc,
		OriginalWhereSQL: "`b` > 0",
	}, &lower, &upper)
	require.NoError(t, err)
	require.Contains(t, whereSQL, "(`b` > 0)")
	require.Contains(t, whereSQL, "`id` >= 10")
	require.Contains(t, whereSQL, "`id` < 20")
}

func TestNonTransactionalDMLRangeWorkerCount(t *testing.T) {
	require.Equal(t, 1, nonTransactionalDMLRangeWorkerCount(0, 0))
	require.Equal(t, 1, nonTransactionalDMLRangeWorkerCount(0, 3))
	require.Equal(t, 2, nonTransactionalDMLRangeWorkerCount(2, 5))
	require.Equal(t, 3, nonTransactionalDMLRangeWorkerCount(8, 3))
}

func TestNonTransactionalDMLRegionRangePlanningIntHandle(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "use test")
	MustExec(t, se, "create table t_region_int(id bigint primary key clustered, b int)")
	desc, err := buildNonTransactionalDMLHandleDescriptorForTest(t, se, "batch on id limit 2 delete from t_region_int")
	require.NoError(t, err)

	recordStart := tablecodec.GenTableRecordPrefix(desc.tableInfo.ID)
	recordEnd := recordStart.PrefixNext()
	key0 := tablecodec.EncodeRowKeyWithHandle(desc.tableInfo.ID, kv.IntHandle(0))
	key10 := tablecodec.EncodeRowKeyWithHandle(desc.tableInfo.ID, kv.IntHandle(10))
	ranges, err := buildNonTransactionalDMLRangesFromRegionKeyRanges(se.GetSessionVars().StmtCtx, desc, desc.tableInfo.ID, []kv.KeyRange{
		{StartKey: tablecodec.GenTableRecordPrefix(desc.tableInfo.ID - 1), EndKey: tablecodec.GenTableRecordPrefix(desc.tableInfo.ID - 1).PrefixNext()},
		{StartKey: recordStart, EndKey: key0},
		{StartKey: key0, EndKey: key10},
		{StartKey: key10, EndKey: recordEnd},
		{StartKey: tablecodec.GenTableRecordPrefix(desc.tableInfo.ID + 1), EndKey: tablecodec.GenTableRecordPrefix(desc.tableInfo.ID + 1).PrefixNext()},
	})
	require.NoError(t, err)
	require.Len(t, ranges, 3)
	require.Nil(t, ranges[0].lower)
	requireBoundaryInt(t, ranges[0].upper, 0, false)
	requireBoundaryInt(t, ranges[1].lower, 0, true)
	requireBoundaryInt(t, ranges[1].upper, 10, false)
	requireBoundaryInt(t, ranges[2].lower, 10, true)
	require.Nil(t, ranges[2].upper)
}

func TestNonTransactionalDMLRegionRangePlanningCommonHandleCoalescesUnsafeBoundaries(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "use test")
	MustExec(t, se, "create table t_region_varchar(id varchar(128) collate utf8mb4_bin primary key clustered, b int)")
	desc, err := buildNonTransactionalDMLHandleDescriptorForTest(t, se, "batch on id limit 2 delete from t_region_varchar")
	require.NoError(t, err)

	recordStart := tablecodec.GenTableRecordPrefix(desc.tableInfo.ID)
	recordEnd := recordStart.PrefixNext()
	key10 := encodeCommonHandleRegionKeyForTest(t, se, desc.tableInfo.ID, "v1:pacer_largepayload0010")
	key20 := encodeCommonHandleRegionKeyForTest(t, se, desc.tableInfo.ID, "v1:pacer_largepayload0020")
	unsafeKey := append(recordStart.Clone(), 0xff)
	ranges, err := buildNonTransactionalDMLRangesFromRegionKeyRanges(se.GetSessionVars().StmtCtx, desc, desc.tableInfo.ID, []kv.KeyRange{
		{StartKey: recordStart, EndKey: key10},
		{StartKey: key10, EndKey: unsafeKey},
		{StartKey: unsafeKey, EndKey: key20},
		{StartKey: key20, EndKey: recordEnd},
	})
	require.NoError(t, err)
	require.Len(t, ranges, 3)
	require.Nil(t, ranges[0].lower)
	requireBoundaryString(t, ranges[0].upper, "v1:pacer_largepayload0010", false)
	requireBoundaryString(t, ranges[1].lower, "v1:pacer_largepayload0010", true)
	requireBoundaryString(t, ranges[1].upper, "v1:pacer_largepayload0020", false)
	requireBoundaryString(t, ranges[2].lower, "v1:pacer_largepayload0020", true)
	require.Nil(t, ranges[2].upper)
}

func TestNonTransactionalDMLSessionContextCaptureApply(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	source := CreateSessionAndSetID(t, store)
	worker := CreateSessionAndSetID(t, store)
	MustExec(t, source, "create database if not exists ntdml_ctx")
	MustExec(t, source, "use ntdml_ctx")
	require.NoError(t, source.GetSessionVars().SetSystemVar(variable.SQLModeVar, "ANSI_QUOTES"))
	require.NoError(t, source.GetSessionVars().SetSystemVar(variable.TimeZone, "+08:00"))
	require.NoError(t, source.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, "utf8mb4"))
	require.NoError(t, source.GetSessionVars().SetSystemVar(variable.CollationConnection, "utf8mb4_bin"))
	require.NoError(t, source.GetSessionVars().SetSystemVar(variable.ForeignKeyChecks, "0"))
	require.NoError(t, source.GetSessionVars().SetSystemVar(variable.TiDBConstraintCheckInPlace, "1"))
	require.NoError(t, source.GetSessionVars().SetSystemVar(variable.TiDBRedactLog, variable.Marker))
	source.GetSessionVars().User = &auth.UserIdentity{Username: "ntdml_user", Hostname: "%", AuthUsername: "ntdml_user", AuthHostname: "%"}
	source.GetSessionVars().ActiveRoles = []*auth.RoleIdentity{{Username: "ntdml_role", Hostname: "%"}}
	source.GetSessionVars().SetResourceGroupName("ntdml_rg")
	source.GetSessionVars().StmtCtx.ResourceGroupName = "ntdml_stmt_rg"

	captured, err := captureNonTransactionalDMLSessionContext(source)
	require.NoError(t, err)
	require.NoError(t, applyNonTransactionalDMLSessionContext(worker, captured))

	workerVars := worker.GetSessionVars()
	require.Equal(t, "ntdml_ctx", workerVars.CurrentDB)
	require.True(t, workerVars.SQLMode.HasANSIQuotesMode())
	timeZone, err := workerVars.GetSessionOrGlobalSystemVar(context.Background(), variable.TimeZone)
	require.NoError(t, err)
	require.Equal(t, "+08:00", timeZone)
	charset, collation := workerVars.GetCharsetInfo()
	require.Equal(t, "utf8mb4", charset)
	require.Equal(t, "utf8mb4_bin", collation)
	require.False(t, workerVars.ForeignKeyChecks)
	require.True(t, workerVars.ConstraintCheckInPlace)
	require.Equal(t, errors.RedactLogMarker, workerVars.EnableRedactLog)
	require.Equal(t, "ntdml_user", workerVars.User.Username)
	require.Equal(t, "%", workerVars.User.Hostname)
	require.Len(t, workerVars.ActiveRoles, 1)
	require.Equal(t, "ntdml_role", workerVars.ActiveRoles[0].Username)
	require.Equal(t, "ntdml_rg", workerVars.ResourceGroupName)
	require.Equal(t, "ntdml_stmt_rg", workerVars.StmtCtx.ResourceGroupName)
}

func TestNonTransactionalDMLCheckpointWriteReadSummaryAndCleanup(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "use test")
	MustExec(t, se, "create table t_checkpoint(id bigint primary key clustered, b int)")
	desc, err := buildNonTransactionalDMLHandleDescriptorForTest(t, se, "batch on id limit 2 delete from t_checkpoint")
	require.NoError(t, err)
	lower, err := encodeNonTransactionalDMLBoundary(se.GetSessionVars().StmtCtx, desc, types.NewIntDatum(-10), true)
	require.NoError(t, err)
	upper, err := encodeNonTransactionalDMLBoundary(se.GetSessionVars().StmtCtx, desc, types.NewIntDatum(10), true)
	require.NoError(t, err)
	checkpoint, err := encodeNonTransactionalDMLBoundary(se.GetSessionVars().StmtCtx, desc, types.NewIntDatum(0), false)
	require.NoError(t, err)

	ctx := context.Background()
	done := nonTransactionalDMLCheckpoint{
		JobID:           "job-checkpoint",
		RangeID:         1,
		Mode:            "range",
		DMLType:         "delete",
		DBName:          "test",
		TableID:         100,
		PhysicalTableID: 100,
		HandleKind:      desc.kind,
		Lower:           &lower,
		Upper:           &upper,
		Checkpoint:      &checkpoint,
		Status:          nonTransactionalDMLCheckpointDone,
		Scanned:         11,
		Affected:        7,
	}
	require.NoError(t, writeNonTransactionalDMLCheckpoint(ctx, se, done))
	loaded, err := loadNonTransactionalDMLCheckpoint(ctx, se, done.JobID, done.RangeID, desc)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, done.JobID, loaded.JobID)
	require.Equal(t, done.RangeID, loaded.RangeID)
	require.Equal(t, done.HandleKind, loaded.HandleKind)
	require.Equal(t, done.Status, loaded.Status)
	require.Equal(t, int64(-10), loaded.Lower.value.GetInt64())
	require.Equal(t, int64(10), loaded.Upper.value.GetInt64())
	require.Equal(t, int64(0), loaded.Checkpoint.value.GetInt64())
	require.Equal(t, uint64(11), loaded.Scanned)
	require.Equal(t, uint64(7), loaded.Affected)
	covered, err := nonTransactionalDMLCheckpointCoversBoundary(ctx, se, done.JobID, done.RangeID, desc, checkpoint)
	require.NoError(t, err)
	require.True(t, covered)
	futureCheckpoint, err := encodeNonTransactionalDMLBoundary(se.GetSessionVars().StmtCtx, desc, types.NewIntDatum(5), false)
	require.NoError(t, err)
	covered, err = nonTransactionalDMLCheckpointCoversBoundary(ctx, se, done.JobID, done.RangeID, desc, futureCheckpoint)
	require.NoError(t, err)
	require.False(t, covered)

	failed := done
	failed.RangeID = 2
	failed.Status = nonTransactionalDMLCheckpointFailed
	failed.RetryCount = 3
	failed.ErrorClass = "retryable"
	failed.ErrorText = "write conflict"
	failed.Scanned = 5
	failed.Affected = 1
	require.NoError(t, writeNonTransactionalDMLCheckpoint(ctx, se, failed))
	covered, err = nonTransactionalDMLCheckpointCoversBoundary(ctx, se, failed.JobID, failed.RangeID, desc, checkpoint)
	require.NoError(t, err)
	require.False(t, covered)
	summary, err := summarizeNonTransactionalDMLCheckpoints(ctx, se, done.JobID)
	require.NoError(t, err)
	require.Equal(t, uint64(2), summary.TotalRanges)
	require.Equal(t, uint64(1), summary.DoneRanges)
	require.Equal(t, uint64(1), summary.FailedRanges)
	require.Equal(t, uint64(16), summary.Scanned)
	require.Equal(t, uint64(8), summary.Affected)
	require.Equal(t, uint64(3), summary.RetryCount)

	require.NoError(t, deleteNonTransactionalDMLCheckpoints(ctx, se, done.JobID))
	loaded, err = loadNonTransactionalDMLCheckpoint(ctx, se, done.JobID, done.RangeID, desc)
	require.NoError(t, err)
	require.Nil(t, loaded)

	require.NoError(t, writeNonTransactionalDMLCheckpoint(ctx, se, failed))
	loaded, err = loadNonTransactionalDMLCheckpoint(ctx, se, failed.JobID, failed.RangeID, desc)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, nonTransactionalDMLCheckpointFailed, loaded.Status)
	require.Equal(t, "write conflict", loaded.ErrorText)
}

func TestNonTransactionalDMLRetryableErrorClassification(t *testing.T) {
	require.False(t, isNonTransactionalDMLRetryableError(nil))
	require.True(t, isNonTransactionalDMLRetryableError(kv.ErrTxnRetryable))
	require.True(t, isNonTransactionalDMLRetryableError(kv.ErrWriteConflict))
	require.True(t, isNonTransactionalDMLRetryableError(storeerr.ErrLockWaitTimeout))
	require.True(t, isNonTransactionalDMLRetryableError(&tikverr.ErrDeadlock{Deadlock: &kvrpcpb.Deadlock{}, IsRetryable: true}))
	require.False(t, isNonTransactionalDMLRetryableError(kv.ErrKeyExists))
	require.False(t, isNonTransactionalDMLRetryableError(&tikverr.ErrDeadlock{Deadlock: &kvrpcpb.Deadlock{}, IsRetryable: false}))
}

func buildNonTransactionalDMLHandleDescriptorForTest(t *testing.T, se sessiontypes.Session, sql string) (*nonTransactionalDMLHandleDescriptor, error) {
	ctx := context.Background()
	stmts, err := se.Parse(ctx, sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt, ok := stmts[0].(*ast.NonTransactionalDMLStmt)
	require.True(t, ok)

	nodeW := resolve.NewNodeW(stmt)
	require.NoError(t, core.Preprocess(ctx, se, nodeW))
	tableName, _, shardColumnInfo, tableSources, err := buildSelectSQL(stmt, nodeW.GetResolveContext(), se)
	require.NoError(t, err)
	return buildNonTransactionalDMLHandleDescriptor(se, stmt, tableName, shardColumnInfo, tableSources)
}

func restoreNonTransactionalDMLExprForTest(t *testing.T, expr ast.ExprNode) string {
	var sb strings.Builder
	require.NoError(t, expr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
		format.RestoreNameBackQuotes|
		format.RestoreSpacesAroundBinaryOperation|
		format.RestoreBracketAroundBinaryOperation|
		format.RestoreStringWithoutCharset, &sb)))
	return sb.String()
}

func encodeCommonHandleRegionKeyForTest(t *testing.T, se sessiontypes.Session, tableID int64, value string) kv.Key {
	encoded, err := codec.EncodeKey(se.GetSessionVars().StmtCtx.TimeZone(), nil, types.NewStringDatum(value))
	require.NoError(t, err)
	return tablecodec.EncodeRowKey(tableID, encoded)
}

func requireBoundaryInt(t *testing.T, boundary *nonTransactionalDMLBoundary, expected int64, inclusive bool) {
	require.NotNil(t, boundary)
	require.True(t, boundary.hasValue)
	require.Equal(t, inclusive, boundary.inclusive)
	require.Equal(t, expected, boundary.value.GetInt64())
}

func requireBoundaryString(t *testing.T, boundary *nonTransactionalDMLBoundary, expected string, inclusive bool) {
	require.NotNil(t, boundary)
	require.True(t, boundary.hasValue)
	require.Equal(t, inclusive, boundary.inclusive)
	require.Equal(t, expected, boundary.value.GetString())
}
