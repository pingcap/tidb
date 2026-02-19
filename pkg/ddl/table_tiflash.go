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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)
func (w *worker) onSetTableFlashReplica(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetSetTiFlashReplicaArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	replicaInfo := args.TiflashReplica

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Ban setting replica count for tables in system database.
	if metadef.IsMemOrSysDB(job.SchemaName) {
		return ver, errors.Trace(dbterror.ErrUnsupportedTiFlashOperationForSysOrMemTable)
	}

	// Check the validity of the replica count. For example, not exceeding the tiflash store count.
	err = w.checkTiFlashReplicaCount(replicaInfo.Count)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// We should check this first, in order to avoid creating redundant DDL jobs.
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		logutil.DDLLogger().Info("Set TiFlash replica pd rule for partitioned table", zap.Int64("tableID", tblInfo.ID))
		if e := infosync.ConfigureTiFlashPDForPartitions(false, &pi.Definitions, replicaInfo.Count, &replicaInfo.Labels, tblInfo.ID); e != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(e)
		}
		// Partitions that in adding mid-state. They have high priorities, so we should set accordingly pd rules.
		if e := infosync.ConfigureTiFlashPDForPartitions(true, &pi.AddingDefinitions, replicaInfo.Count, &replicaInfo.Labels, tblInfo.ID); e != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(e)
		}
	} else {
		logutil.DDLLogger().Info("Set TiFlash replica pd rule", zap.Int64("tableID", tblInfo.ID))
		if e := infosync.ConfigureTiFlashPDForTable(tblInfo.ID, replicaInfo.Count, &replicaInfo.Labels); e != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(e)
		}
	}

	if replicaInfo.Count > 0 {
		available := false
		if args.ResetAvailable {
			// Reset the available field to false. This is required when fixing the placement rules after native BR.
			// Because the `available` field may be true after native BR restore, but the user should wait before
			// TiFlash peers are rebuilt.
			available = false
		} else if tblInfo.TiFlashReplica != nil {
			// If there is already TiFlash replica info, we should keep the Available field.
			// For example, during the process of increasing the number of TiFlash replicas from 2 to 3,
			// or decreasing it from 3 to 2, if the `available` field of the original TiFlash replica
			// is already `True`, its value remains unchanged. In this case, the optimizer can still choose
			// to route queries to TiFlash for execution, avoiding any impact on service continuity.
			available = tblInfo.TiFlashReplica.Available
		}
		tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:          replicaInfo.Count,
			LocationLabels: replicaInfo.Labels,
			Available:      available,
		}
	} else {
		if tblInfo.TiFlashReplica != nil {
			err = infosync.DeleteTiFlashTableSyncProgress(tblInfo)
			if err != nil {
				logutil.DDLLogger().Error("DeleteTiFlashTableSyncProgress fails", zap.Error(err))
			}
		}
		tblInfo.TiFlashReplica = nil
	}

	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) checkTiFlashReplicaCount(replicaCount uint64) error {
	ctx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(ctx)

	return checkTiFlashReplicaCount(ctx, replicaCount)
}

func onUpdateTiFlashReplicaStatus(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetUpdateTiFlashReplicaStatusArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	available, physicalID := args.Available, args.PhysicalID

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TiFlashReplica == nil || (tblInfo.ID == physicalID && tblInfo.TiFlashReplica.Available == available) ||
		(tblInfo.ID != physicalID && available == tblInfo.TiFlashReplica.IsPartitionAvailable(physicalID)) {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("the replica available status of table %s is already updated", tblInfo.Name.String())
	}

	if tblInfo.ID == physicalID {
		tblInfo.TiFlashReplica.Available = available
	} else if pi := tblInfo.GetPartitionInfo(); pi != nil {
		// Partition replica become available.
		if available {
			allAvailable := true
			for _, p := range pi.Definitions {
				if p.ID == physicalID {
					tblInfo.TiFlashReplica.AvailablePartitionIDs = append(tblInfo.TiFlashReplica.AvailablePartitionIDs, physicalID)
				}
				allAvailable = allAvailable && tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID)
			}
			tblInfo.TiFlashReplica.Available = allAvailable
		} else {
			// Partition replica become unavailable.
			for i, id := range tblInfo.TiFlashReplica.AvailablePartitionIDs {
				if id == physicalID {
					newIDs := tblInfo.TiFlashReplica.AvailablePartitionIDs[:i]
					newIDs = append(newIDs, tblInfo.TiFlashReplica.AvailablePartitionIDs[i+1:]...)
					tblInfo.TiFlashReplica.AvailablePartitionIDs = newIDs
					tblInfo.TiFlashReplica.Available = false
					logutil.DDLLogger().Info("TiFlash replica become unavailable", zap.Int64("tableID", tblInfo.ID), zap.Int64("partitionID", id))
					break
				}
			}
		}
	} else {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("unknown physical ID %v in table %v", physicalID, tblInfo.Name.O)
	}

	if tblInfo.TiFlashReplica.Available {
		logutil.DDLLogger().Info("TiFlash replica available", zap.Int64("tableID", tblInfo.ID))
	}
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}
