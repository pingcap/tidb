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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
)

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
		dbInfo.State = model.StateWriteOnly
		err = metaMut.UpdateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		var tables []*model.TableInfo
		tables, err = metaMut.ListTables(jobCtx.stepCtx, job.SchemaID)
		if err != nil {
			return ver, errors.Trace(err)
		}
		var ruleIDs []string
		for _, tblInfo := range tables {
			rules := append(getPartitionRuleIDs(job.SchemaName, tblInfo), fmt.Sprintf(label.TableIDFormat, label.IDPrefix, job.SchemaName, tblInfo.Name.L))
			ruleIDs = append(ruleIDs, rules...)
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
		var tables []*model.TableInfo
		tables, err = metaMut.ListTables(jobCtx.stepCtx, job.SchemaID)
		if err != nil {
			return ver, errors.Trace(err)
		}

		err = metaMut.UpdateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// we only drop meta key of database, but not drop tables' meta keys.
		if err = metaMut.DropDatabase(dbInfo.ID); err != nil {
			break
		}

		// Split tables into multiple jobs to avoid too big records in the notifier.
		const tooManyTablesThreshold = 100000
		tablesPerJob := 100
		if len(tables) > tooManyTablesThreshold {
			tablesPerJob = 500
		}
		for i := 0; i < len(tables); i += tablesPerJob {
			end := i + tablesPerJob
			if end > len(tables) {
				end = len(tables)
			}
			dropSchemaEvent := notifier.NewDropSchemaEvent(dbInfo, tables[i:end])
			err = asyncNotifyEvent(jobCtx, dropSchemaEvent, job, int64(i/tablesPerJob), w.sess)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
		// Finish this job.
		job.FillFinishedArgs(&model.DropSchemaArgs{
			AllDroppedTableIDs: getIDs(tables),
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
		ids = append(ids, t.ID)
		if t.GetPartitionInfo() != nil {
			ids = append(ids, getPartitionIDs(t)...)
		}
	}

	return ids
}
