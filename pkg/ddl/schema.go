// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/chunk"
	utillogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const dropSchemaTTLRestoreRegistrationsLogKey = "drop_schema_ttl_restore_registrations_failed"

type dropSchemaTTLTableInfo struct {
	id      int64
	name    string
	enabled bool
}

func onCreateSchema(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	args, err := model.GetCreateSchemaArgs(job)
	if err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	dbInfo := args.DBInfo
	dbInfo.ID = schemaID
	dbInfo.State = model.StateNone

	err = checkSchemaNotExists(jobCtx.infoCache, schemaID, dbInfo)
	if err != nil {
		if infoschema.ErrDatabaseExists.Equal(err) {
			// The database already exists, can't create it, we should cancel this job now.
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch dbInfo.State {
	case model.StateNone:
		// none -> public
		dbInfo.State = model.StatePublic
		err = jobCtx.metaMut.CreateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
		return ver, nil
	default:
		// We can't enter here.
		return ver, errors.Errorf("invalid db state %v", dbInfo.State)
	}
}

// checkSchemaNotExists checks whether the database already exists.
// see checkTableNotExists for the rationale of why we check using info schema only.
func checkSchemaNotExists(infoCache *infoschema.InfoCache, schemaID int64, dbInfo *model.DBInfo) error {
	is := infoCache.GetLatest()
	// Check database exists by name.
	if is.SchemaExists(dbInfo.Name) {
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	// Check database exists by ID.
	if _, ok := is.SchemaByID(schemaID); ok {
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	return nil
}

func onModifySchemaCharsetAndCollate(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetModifySchemaArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(jobCtx.metaMut, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if dbInfo.Charset == args.ToCharset && dbInfo.Collate == args.ToCollate {
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
		return ver, nil
	}

	dbInfo.Charset = args.ToCharset
	dbInfo.Collate = args.ToCollate

	if err = jobCtx.metaMut.UpdateDatabase(dbInfo); err != nil {
		return ver, errors.Trace(err)
	}
	if ver, err = updateSchemaVersion(jobCtx, job); err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
	return ver, nil
}

func onModifySchemaDefaultPlacement(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetModifySchemaArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	placementPolicyRef := args.PolicyRef
	metaMut := jobCtx.metaMut
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(metaMut, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Double Check if policy exits while ddl executing
	if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(metaMut, job, placementPolicyRef); err != nil {
		return ver, errors.Trace(err)
	}

	// Notice: dbInfo.DirectPlacementOpts and dbInfo.PlacementPolicyRef can not be both not nil, which checked before constructing ddl job.
	// So that we can just check the two situation that do not need ddl: 1. DB.DP == DDL.DP && nil == nil 2. nil == nil && DB.PP == DDL.PP
	if placementPolicyRef != nil && dbInfo.PlacementPolicyRef != nil && *dbInfo.PlacementPolicyRef == *placementPolicyRef {
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
		return ver, nil
	}

	// If placementPolicyRef and directPlacementOpts are both nil, And placement of dbInfo is not nil, it will remove all placement options.
	dbInfo.PlacementPolicyRef = placementPolicyRef

	if err = metaMut.UpdateDatabase(dbInfo); err != nil {
		return ver, errors.Trace(err)
	}
	if ver, err = updateSchemaVersion(jobCtx, job); err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
	return ver, nil
}

// checkSchemaReadOnlyBeforeJob closes the gap between statement preprocessing
// and execution of a queued DDL. Shared dependencies are read sources.
func checkSchemaReadOnlyBeforeJob(jobCtx *jobContext, job *model.Job) error {
	for _, target := range job.GetInvolvingSchemaInfo() {
		if target.Mode != model.ExclusiveInvolving || target.Database == model.InvolvingNone || target.Database == model.InvolvingAll {
			continue
		}
		db, ok := jobCtx.infoCache.GetLatest().SchemaByName(ast.NewCIStr(target.Database))
		if !ok {
			continue // Let the operation report its normal missing-schema error.
		}
		db, err := jobCtx.metaMut.GetDatabase(db.ID)
		if err != nil {
			return errors.Trace(err)
		}
		if db != nil && db.ReadOnly {
			return infoschema.ErrSchemaInReadOnlyMode.GenWithStackByArgs(db.Name.O)
		}
	}
	return nil
}

func (w *worker) onModifySchemaReadOnly(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetModifySchemaArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(jobCtx.metaMut, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if job.SchemaState == model.StateNone && dbInfo.ReadOnly == args.ReadOnly {
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
		return ver, nil
	}
	// A failed step discards its metadata writes but persists job progress.
	// Retry publication if that happened before the flag became effective.
	if job.SchemaState == model.StatePendingReadOnly && dbInfo.ReadOnly != args.ReadOnly {
		job.SchemaState = model.StateNone
	}
	// If the database is set to read-write from read-only, we don't need the middle state.
	if dbInfo.ReadOnly && !args.ReadOnly {
		// Record progress before any metadata mutation, including on panic paths.
		job.SchemaState = model.StatePendingReadOnly
		dbInfo.ReadOnly = args.ReadOnly
		if err = jobCtx.metaMut.UpdateDatabase(dbInfo); err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = updateSchemaVersion(jobCtx, job)
		failpoint.Inject("mockErrorOnModifySchemaReadOnly2ReadWrite", func() {
			err = errors.New("mock error at read only to read write")
		})
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
		return ver, nil
	}
	trxTableName := infoschema.ClusterTableTiDBTrx
	failpoint.Inject("mockModifySchemaReadOnlyDDL", func() {
		trxTableName = infoschema.TableTiDBTrx
	})

	switch job.SchemaState {
	case model.StateNone:
		// Rollback can skip StateNone only because all mutations follow this assignment.
		job.SchemaState = model.StatePendingReadOnly
		dbInfo.ReadOnly = args.ReadOnly
		if err = jobCtx.metaMut.UpdateDatabase(dbInfo); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		ver, err = updateSchemaVersion(jobCtx, job)
		failpoint.Inject("mockErrorOnModifySchemaReadOnlyStateNone", func() {
			err = errors.New("mock error at StateNone")
		})
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StatePendingReadOnly:
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			return ver, errors.Trace(err)
		}
		defer w.sessPool.Put(sessCtx)
		session := sess.NewSession(sessCtx)
		sampleLogger := utillogutil.SampleLoggerFactory(time.Second, 5, zap.String(utillogutil.LogFieldCategory, "ddl"))
		uncommittedTxn, err := getUncommittedTxnIDs(jobCtx, session, dbInfo.ID, trxTableName, args.DDLStartTS)
		failpoint.Inject("mockErrorOnModifySchemaReadOnlyStatePendingReadOnly", func() {
			err = errors.New("mock error at StatePendingReadOnly")
		})
		if err != nil {
			return ver, errors.Trace(err)
		}

		// check and wait all uncommitted txn
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for len(uncommittedTxn) > 0 {
			select {
			case <-ticker.C:
				// The cluster table enumerates all connections even with an ID
				// predicate, so confirm the whole pending set with one scan.
				txnIDs := make([]string, 0, len(uncommittedTxn))
				for txnID := range uncommittedTxn {
					txnIDs = append(txnIDs, strconv.FormatInt(txnID, 10))
				}
				sql := fmt.Sprintf("SELECT ID FROM INFORMATION_SCHEMA.%s WHERE ID IN (%s)", trxTableName, strings.Join(txnIDs, ","))
				rows, err := executeReadOnlyTxnQuery(jobCtx.stepCtx, session, sql, "check if txns committed")
				if err != nil {
					return ver, errors.Trace(err)
				}
				active := make(map[int64]struct{}, len(rows))
				for _, row := range rows {
					active[row.GetInt64(0)] = struct{}{}
				}
				for txnID := range uncommittedTxn {
					if _, ok := active[txnID]; !ok {
						delete(uncommittedTxn, txnID)
						continue
					}
					sampleLogger().Info("uncommitted txn block read only ddl",
						zap.Int64("txn ID", txnID),
						zap.Int64("job ID", job.ID),
					)
				}
				failpoint.InjectCall("checkUncommittedTxns", uncommittedTxn)
			case <-jobCtx.stepCtx.Done():
				return ver, errors.Trace(jobCtx.stepCtx.Err())
			}
		}

		if err = jobCtx.metaMut.UpdateDatabase(dbInfo); err != nil {
			return ver, errors.Trace(err)
		}
		if ver, err = updateSchemaVersion(jobCtx, job); err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
	}
	return ver, nil
}

func getUncommittedTxnIDs(jobCtx *jobContext, sess *sess.Session, targetDBID int64, trxTableName string, ddlStartTS uint64) (map[int64]struct{}, error) {
	var currTS = uint64(0)
	err := kv.RunInNewTxn(jobCtx.ctx, jobCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		currTS = txn.StartTS()
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	sql := fmt.Sprintf(
		"SELECT RELATED_TABLE_IDS, ID FROM INFORMATION_SCHEMA.%s "+
			"WHERE ID != %d "+ // exclude current ddl
			"AND ID < %d", // exclude new transactions
		trxTableName,
		ddlStartTS,
		currTS,
	)
	t := time.Now()
	r, err := executeReadOnlyTxnQuery(jobCtx.stepCtx, sess, sql, "get uncommitted txn id")
	if err != nil {
		return nil, errors.Trace(err)
	}
	queryDur := time.Since(t)
	ids := make(map[int64]struct{})
	for _, row := range r {
		txnID := row.GetInt64(1)
		relatedTableIDs := row.GetString(0)
		if relatedTableIDs == "" {
			continue
		}
		tblIDs := strings.Split(relatedTableIDs, ",")
		for _, tblID := range tblIDs {
			id, err := strconv.Atoi(tblID)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// A transaction can retain IDs dropped after MDL was disabled. Read
			// authoritative metadata in the target database so those IDs and IDs
			// from other databases are skipped without hiding storage errors.
			tblInfo, err := jobCtx.metaMut.GetTable(targetDBID, int64(id))
			if err != nil {
				return nil, errors.Trace(err)
			}
			if tblInfo != nil {
				ids[txnID] = struct{}{}
				break
			}
		}
	}
	jobCtx.logger.Info("get uncommitted txn id",
		zap.Int("txn count", len(ids)),
		zap.Duration("query time", queryDur),
		zap.Duration("parse time", time.Since(t)-queryDur))
	return ids, nil
}

// executeReadOnlyTxnQuery requires a complete cluster transaction scan. RPC
// failures are warnings rather than SQL errors for cluster information tables.
func executeReadOnlyTxnQuery(ctx context.Context, session *sess.Session, sql, label string) ([]chunk.Row, error) {
	rows, err := session.Execute(ctx, sql, label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	failpoint.InjectCall("afterReadOnlyTxnQuery", session.Context, sql, &rows)
	warnings := session.GetSessionVars().StmtCtx.GetWarnings()
	if len(warnings) > 0 {
		return nil, errors.Annotate(warnings[0].Err, "incomplete transaction scan for database read-only transition")
	}
	return rows, nil
}

func (w *worker) onDropSchema(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	metaMut := jobCtx.metaMut
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(metaMut, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if dbInfo.State == model.StatePublic {
		err = checkDatabaseHasForeignKeyReferredInOwner(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch dbInfo.State {
	case model.StatePublic:
		// public -> write only
		var (
			ttlTables []dropSchemaTTLTableInfo
			ruleIDs   []string
		)
		codec := jobCtx.store.GetCodec()
		err = metaMut.IterTables(job.SchemaID, func(tblInfo *model.TableInfo) error {
			if err := jobCtx.stepCtx.Err(); err != nil {
				return err
			}
			if tblInfo.TTLInfo != nil {
				ttlTables = append(ttlTables, dropSchemaTTLTableInfo{
					id:      tblInfo.ID,
					name:    tblInfo.Name.O,
					enabled: tblInfo.TTLInfo.Enable,
				})
			}
			rules := append(getPartitionRuleIDs(codec, job.SchemaName, tblInfo), label.NewRuleID(codec, job.SchemaName, tblInfo.Name.L, ""))
			ruleIDs = append(ruleIDs, rules...)
			return nil
		})
		if err != nil {
			return ver, errors.Trace(err)
		}
		if jobCtx.oldDDLCtx != nil {
			if err := w.dropSchemaTTLTablesFromExternalWorkload(jobCtx.ctx, job, job.SchemaName, ttlTables); err != nil {
				return ver, err
			}
		}
		dbInfo.State = model.StateWriteOnly
		err = metaMut.UpdateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		patch := label.NewRulePatch([]*label.Rule{}, ruleIDs)
		err = infosync.UpdateLabelRules(context.TODO(), patch)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		dbInfo.State = model.StateDeleteOnly
		err = metaMut.UpdateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteOnly:
		dbInfo.State = model.StateNone
		var (
			miniTables         []*notifier.MiniTableInfoForSchemaEvent
			mviewIDs           []int64
			mlogIDs            []int64
			allDroppedTableIDs []int64
			affinityErr        error
		)
		affinityGroupIDs := make(map[string]struct{})
		allDroppedTableIDs = make([]int64, 0)
		codec := jobCtx.store.GetCodec()
		err = metaMut.IterTables(job.SchemaID, func(tblInfo *model.TableInfo) error {
			if err := jobCtx.stepCtx.Err(); err != nil {
				return err
			}
			miniTables = append(miniTables, notifier.NewMiniTableInfoForSchemaEvent(tblInfo))
			if tblInfo.Affinity != nil && affinityErr == nil {
				affinityErr = collectTableAffinityGroupIDs(codec, tblInfo, affinityGroupIDs)
			}
			if tblInfo.MaterializedView != nil {
				mviewIDs = append(mviewIDs, tblInfo.ID)
			}
			if tblInfo.MaterializedViewLog != nil {
				mlogIDs = append(mlogIDs, tblInfo.ID)
			}
			allDroppedTableIDs = appendPhysicalTableIDs(allDroppedTableIDs, tblInfo)
			return nil
		})
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Best-effort cleanup - log errors but continue with DROP DATABASE
		if affinityErr == nil {
			affinityErr = batchDeleteTableAffinityGroups(jobCtx, affinityGroupIDs)
		}
		if affinityErr != nil {
			logutil.DDLLogger().Warn("failed to delete affinity groups for batch tables, but operation will continue",
				zap.Error(affinityErr),
				zap.Int64("databaseID", dbInfo.ID))
		}

		// Clean up masking policies for all tables in the dropped database.
		if err := w.dropMaskingPoliciesByDBName(jobCtx, job.SchemaName); err != nil {
			logutil.DDLLogger().Warn("failed to delete masking policies for database, but operation will continue",
				zap.Error(err),
				zap.String("dbName", job.SchemaName))
		}

		err = metaMut.UpdateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// we only drop meta key of database, but not drop tables' meta keys.
		if err = metaMut.DropDatabase(dbInfo.ID); err != nil {
			break
		}

		// Refresh and purge records drive MView and MLog maintenance, so their
		// cleanup must roll back DROP DATABASE on failure. Refresh alerts are
		// advisory state, so retaining stale alerts is preferable to blocking DDL.
		if err = w.deleteCreateMaterializedViewRefreshInfos(jobCtx, mviewIDs); err != nil {
			return ver, errors.Trace(err)
		}
		if err = w.deleteCreateMaterializedViewRefreshAlerts(jobCtx, mviewIDs); err != nil {
			logutil.DDLLogger().Warn(
				"drop schema: failed to delete materialized view refresh alerts",
				zap.String("schemaName", job.SchemaName),
				zap.Error(err),
			)
		}
		if err = w.deleteMaterializedViewLogPurgeInfos(jobCtx, mlogIDs); err != nil {
			return ver, errors.Trace(err)
		}

		// Split tables into multiple jobs to avoid too big records in the notifier.
		const tooManyTablesThreshold = 100000
		tablesPerJob := 100
		if len(miniTables) > tooManyTablesThreshold {
			tablesPerJob = 500
		}
		for i := 0; i < len(miniTables); i += tablesPerJob {
			end := min(i+tablesPerJob, len(miniTables))
			dropSchemaEvent := notifier.NewDropSchemaEvent(dbInfo, miniTables[i:end])
			err = asyncNotifyEvent(jobCtx, dropSchemaEvent, job, int64(i/tablesPerJob), w.sess)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
		// Finish this job.
		job.FillFinishedArgs(&model.DropSchemaArgs{
			AllDroppedTableIDs: allDroppedTableIDs,
		})
		job.FinishDBJob(model.JobStateDone, model.StateNone, ver, dbInfo)
	default:
		// We can't enter here.
		return ver, errors.Trace(errors.Errorf("invalid db state %v", dbInfo.State))
	}
	job.SchemaState = dbInfo.State
	return ver, errors.Trace(err)
}

func (w *worker) onRecoverSchema(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetRecoverArgs(job)
	if err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	recoverSchemaInfo := args.RecoverInfo

	schemaInfo := recoverSchemaInfo.DBInfo
	// check GC and safe point
	gcEnable, err := checkGCEnable(w)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	switch schemaInfo.State {
	case model.StateNone:
		// none -> write only
		// check GC enable and update flag.
		if gcEnable {
			args.CheckFlag = recoverCheckFlagEnableGC
		} else {
			args.CheckFlag = recoverCheckFlagDisableGC
		}
		job.FillArgs(args)

		schemaInfo.State = model.StateWriteOnly
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> public
		// do recover schema and tables.
		if gcEnable {
			err = disableGC(w)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Errorf("disable gc failed, try again later. err: %v", err)
			}
		}

		recoverTbls := recoverSchemaInfo.RecoverTableInfos
		if recoverSchemaInfo.LoadTablesOnExecute {
			sid := recoverSchemaInfo.DBInfo.ID
			snap := w.store.GetSnapshot(kv.NewVersion(recoverSchemaInfo.SnapshotTS))
			snapMeta := meta.NewReader(snap)
			tables, err2 := snapMeta.ListTables(jobCtx.stepCtx, sid)
			if err2 != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err2)
			}
			recoverTbls = make([]*model.RecoverTableInfo, 0, len(tables))
			for _, tblInfo := range tables {
				autoIDs, err3 := snapMeta.GetAutoIDAccessors(sid, tblInfo.ID).Get()
				if err3 != nil {
					job.State = model.JobStateCancelled
					return ver, errors.Trace(err3)
				}
				recoverTbls = append(recoverTbls, &model.RecoverTableInfo{
					SchemaID:      sid,
					TableInfo:     tblInfo,
					DropJobID:     recoverSchemaInfo.DropJobID,
					SnapshotTS:    recoverSchemaInfo.SnapshotTS,
					AutoIDs:       autoIDs,
					OldSchemaName: recoverSchemaInfo.OldSchemaName.L,
					OldTableName:  tblInfo.Name.L,
				})
			}
		}

		dbInfo := schemaInfo.Clone()
		dbInfo.State = model.StatePublic
		err = jobCtx.metaMut.CreateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// check GC safe point
		err = checkSafePoint(w, recoverSchemaInfo.SnapshotTS)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		for _, recoverInfo := range recoverTbls {
			if recoverInfo.TableInfo.TTLInfo != nil {
				// force disable TTL job schedule for recovered table
				recoverInfo.TableInfo.TTLInfo.Enable = false
			}
			ver, err = w.recoverTable(jobCtx.stepCtx, jobCtx.metaMut, job, recoverInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
		schemaInfo.State = model.StatePublic
		// use to update InfoSchema
		job.SchemaID = schemaInfo.ID
		ver, err = updateSchemaVersion(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, schemaInfo)
		return ver, nil
	default:
		// We can't enter here.
		return ver, errors.Errorf("invalid db state %v", schemaInfo.State)
	}
	return ver, errors.Trace(err)
}

func (w *worker) dropSchemaTTLTablesFromExternalWorkload(
	ctx context.Context,
	job *model.Job,
	schemaName string,
	tables []dropSchemaTTLTableInfo,
) error {
	tablesNeedingCompensation := make([]dropSchemaTTLTableInfo, 0, len(tables))
	for _, tblInfo := range tables {
		if err := w.deleteTTLTableFromExternalWorkload(ctx, tblInfo.id); err != nil {
			for i := len(tablesNeedingCompensation) - 1; i >= 0; i-- {
				restoreTblInfo := tablesNeedingCompensation[i]
				if compensateErr := w.registerTTLTableIDToExternalWorkload(ctx, restoreTblInfo.id); compensateErr != nil {
					logutil.DDLLogger().Warn("drop schema TTL external workload compensation failed",
						zap.String("keyword", dropSchemaTTLRestoreRegistrationsLogKey),
						zap.String("dbName", schemaName),
						zap.Int64("tableID", restoreTblInfo.id),
						zap.String("tableName", restoreTblInfo.name),
						zap.Error(compensateErr),
						zap.NamedError("deleteTTLTableErr", err))
				}
			}
			return cancelJobOnExternalTTLWorkloadError(job, err)
		}
		if tblInfo.enabled {
			tablesNeedingCompensation = append(tablesNeedingCompensation, tblInfo)
		}
	}
	return nil
}

func checkSchemaExistAndCancelNotExistJob(t *meta.Mutator, job *model.Job) (*model.DBInfo, error) {
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if dbInfo == nil {
		job.State = model.JobStateCancelled
		return nil, infoschema.ErrDatabaseDropExists.GenWithStackByArgs("")
	}
	return dbInfo, nil
}

func getIDs(tables []*model.TableInfo) []int64 {
	ids := make([]int64, 0, len(tables))
	for _, t := range tables {
		ids = appendPhysicalTableIDs(ids, t)
	}

	return ids
}

func appendPhysicalTableIDs(dst []int64, tblInfo *model.TableInfo) []int64 {
	dst = append(dst, tblInfo.ID)
	return append(dst, getPartitionIDs(tblInfo)...)
}
