// Copyright 2018 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/slice"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
)

const (
	partitionMaxValue = "MAXVALUE"
)

func checkAddPartition(jobCtx *jobContext, job *model.Job) (*model.TableInfo, *model.PartitionInfo, []model.PartitionDefinition, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	args := jobCtx.jobArgs.(*model.TablePartitionArgs)
	partInfo := args.PartInfo
	if len(tblInfo.Partition.AddingDefinitions) > 0 {
		return tblInfo, partInfo, tblInfo.Partition.AddingDefinitions, nil
	}
	return tblInfo, partInfo, []model.PartitionDefinition{}, nil
}

// TODO: Move this into reorganize partition!
func (w *worker) onAddTablePartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	// Handle the rolling back job
	if job.IsRollingback() {
		ver, err := w.rollbackLikeDropPartition(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// notice: addingDefinitions is empty when job is in state model.StateNone
	tblInfo, partInfo, addingDefinitions, err := checkAddPartition(jobCtx, job)
	if err != nil {
		return ver, err
	}

	// In order to skip maintaining the state check in partitionDefinition, TiDB use addingDefinition instead of state field.
	// So here using `job.SchemaState` to judge what the stage of this job is.
	switch job.SchemaState {
	case model.StateNone:
		if tblInfo.Affinity != nil {
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("ADD PARTITION of a table with AFFINITY option")
		}
		// job.SchemaState == model.StateNone means the job is in the initial state of add partition.
		// Here should use partInfo from job directly and do some check action.
		err = checkAddPartitionTooManyPartitions(uint64(len(tblInfo.Partition.Definitions) + len(partInfo.Definitions)))
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = checkAddPartitionValue(tblInfo, partInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = checkAddPartitionNameUnique(tblInfo, partInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		// move the adding definition into tableInfo.
		updateAddingPartitionInfo(partInfo, tblInfo)
		tblInfo.Partition.DDLState = model.StateReplicaOnly
		tblInfo.Partition.DDLAction = job.Type
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// modify placement settings
		for _, def := range tblInfo.Partition.AddingDefinitions {
			if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(jobCtx.metaMut, job, def.PlacementPolicyRef); err != nil {
				return ver, errors.Trace(err)
			}
		}

		if tblInfo.TiFlashReplica != nil {
			// Must set placement rule, and make sure it succeeds.
			if err := infosync.ConfigureTiFlashPDForPartitions(true, &tblInfo.Partition.AddingDefinitions, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels, tblInfo.ID); err != nil {
				logutil.DDLLogger().Error("ConfigureTiFlashPDForPartitions fails", zap.Error(err))
				return ver, errors.Trace(err)
			}
		}

		_, err = alterTablePartitionBundles(jobCtx.metaMut, tblInfo, tblInfo.Partition.AddingDefinitions)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		ids := getIDs([]*model.TableInfo{tblInfo})
		for _, p := range tblInfo.Partition.AddingDefinitions {
			ids = append(ids, p.ID)
		}
		if _, err := alterTableLabelRule(job.SchemaName, tblInfo, ids); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		// none -> replica only
		job.SchemaState = model.StateReplicaOnly
	case model.StateReplicaOnly:
		// replica only -> public
		failpoint.Inject("sleepBeforeReplicaOnly", func(val failpoint.Value) {
			sleepSecond := val.(int)
			time.Sleep(time.Duration(sleepSecond) * time.Second)
		})
		// Here need do some tiflash replica complement check.
		// TODO: If a table is with no TiFlashReplica or it is not available, the replica-only state can be eliminated.
		if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
			// For available state, the new added partition should wait it's replica to
			// be finished. Otherwise the query to this partition will be blocked.
			needRetry, err := checkPartitionReplica(tblInfo.TiFlashReplica.Count, addingDefinitions, jobCtx)
			if err != nil {
				return convertAddTablePartitionJob2RollbackJob(jobCtx, job, err, tblInfo)
			}
			if needRetry {
				// The new added partition hasn't been replicated.
				// Do nothing to the job this time, wait next worker round.
				time.Sleep(tiflashCheckTiDBHTTPAPIHalfInterval)
				// Set the error here which will lead this job exit when it's retry times beyond the limitation.
				return ver, errors.Errorf("[ddl] add partition wait for tiflash replica to complete")
			}
		}

		// When TiFlash Replica is ready, we must move them into `AvailablePartitionIDs`.
		if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
			for _, d := range partInfo.Definitions {
				tblInfo.TiFlashReplica.AvailablePartitionIDs = append(tblInfo.TiFlashReplica.AvailablePartitionIDs, d.ID)
				err = infosync.UpdateTiFlashProgressCache(d.ID, 1)
				if err != nil {
					// just print log, progress will be updated in `refreshTiFlashTicker`
					logutil.DDLLogger().Error("update tiflash sync progress cache failed",
						zap.Error(err),
						zap.Int64("tableID", tblInfo.ID),
						zap.Int64("partitionID", d.ID),
					)
				}
			}
		}
		// For normal and replica finished table, move the `addingDefinitions` into `Definitions`.
		updatePartitionInfo(tblInfo)
		var scatterScope string
		if val, ok := job.GetSystemVars(vardef.TiDBScatterRegion); ok {
			scatterScope = val
		}
		preSplitAndScatter(w.sess.Context, jobCtx.store, tblInfo, addingDefinitions, scatterScope)

		tblInfo.Partition.DDLState = model.StateNone
		tblInfo.Partition.DDLAction = model.ActionNone
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		addPartitionEvent := notifier.NewAddPartitionEvent(tblInfo, partInfo)
		err = asyncNotifyEvent(jobCtx, addPartitionEvent, job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}

	return ver, errors.Trace(err)
}

// alterTableLabelRule updates Label Rules if they exists
// returns true if changed.
func alterTableLabelRule(schemaName string, meta *model.TableInfo, ids []int64) (bool, error) {
	tableRuleID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, schemaName, meta.Name.L)
	oldRule, err := infosync.GetLabelRules(context.TODO(), []string{tableRuleID})
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(oldRule) == 0 {
		return false, nil
	}

	r, ok := oldRule[tableRuleID]
	if ok {
		rule := r.Reset(schemaName, meta.Name.L, "", ids...)
		err = infosync.PutLabelRule(context.TODO(), rule)
		if err != nil {
			return false, errors.Wrapf(err, "failed to notify PD label rule")
		}
		return true, nil
	}
	return false, nil
}

func alterTablePartitionBundles(t *meta.Mutator, tblInfo *model.TableInfo, addDefs []model.PartitionDefinition) (bool, error) {
	// We want to achieve:
	// - before we do any reorganization/write to new partitions/global indexes that the placement rules are in-place
	// - not removing any placement rules for removed partitions
	// So we will:
	// 1) First write the new bundles including both new and old partitions,
	//    EXCEPT if the old partition is in fact a table, then skip that partition
	// 2) Then overwrite the bundles with the final partitioning scheme (second call in onReorg/

	tblInfo = tblInfo.Clone()
	p := tblInfo.Partition
	if p != nil {
		// if partitioning a non-partitioned table, we will first change the metadata,
		// so the table looks like a partitioned table, with the first/only partition having
		// the same partition ID as the table, so we can access the table as a single partition.
		// But in this case we should not add a bundle rule for the same range
		// both as table and partition.
		if p.Definitions[0].ID != tblInfo.ID {
			// prepend with existing partitions
			addDefs = append(p.Definitions, addDefs...)
		}
		p.Definitions = addDefs
	}

	// bundle for table should be recomputed because it includes some default configs for partitions
	tblBundle, err := placement.NewTableBundle(t, tblInfo)
	if err != nil {
		return false, errors.Trace(err)
	}

	var bundles []*placement.Bundle
	if tblBundle != nil {
		bundles = append(bundles, tblBundle)
	}

	partitionBundles, err := placement.NewPartitionListBundles(t, addDefs)
	if err != nil {
		return false, errors.Trace(err)
	}

	bundles = append(bundles, partitionBundles...)

	if len(bundles) > 0 {
		return true, infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
	}
	return false, nil
}

// When drop/truncate a partition, we should still keep the dropped partition's placement settings to avoid unnecessary region schedules.
// When a partition is not configured with a placement policy directly, its rule is in the table's placement group which will be deleted after
// partition truncated/dropped. So it is necessary to create a standalone placement group with partition id after it.
func droppedPartitionBundles(t *meta.Mutator, tblInfo *model.TableInfo, dropPartitions []model.PartitionDefinition) ([]*placement.Bundle, error) {
	partitions := make([]model.PartitionDefinition, 0, len(dropPartitions))
	for _, def := range dropPartitions {
		def = def.Clone()
		if def.PlacementPolicyRef == nil {
			def.PlacementPolicyRef = tblInfo.PlacementPolicyRef
		}

		if def.PlacementPolicyRef != nil {
			partitions = append(partitions, def)
		}
	}

	return placement.NewPartitionListBundles(t, partitions)
}

// updatePartitionInfo merge `addingDefinitions` into `Definitions` in the tableInfo.
func updatePartitionInfo(tblInfo *model.TableInfo) {
	parInfo := &model.PartitionInfo{}
	oldDefs, newDefs := tblInfo.Partition.Definitions, tblInfo.Partition.AddingDefinitions
	parInfo.Definitions = make([]model.PartitionDefinition, 0, len(newDefs)+len(oldDefs))
	parInfo.Definitions = append(parInfo.Definitions, oldDefs...)
	parInfo.Definitions = append(parInfo.Definitions, newDefs...)
	tblInfo.Partition.Definitions = parInfo.Definitions
	tblInfo.Partition.AddingDefinitions = nil
}

// updateAddingPartitionInfo write adding partitions into `addingDefinitions` field in the tableInfo.
func updateAddingPartitionInfo(partitionInfo *model.PartitionInfo, tblInfo *model.TableInfo) {
	newDefs := partitionInfo.Definitions
	tblInfo.Partition.AddingDefinitions = make([]model.PartitionDefinition, 0, len(newDefs))
	tblInfo.Partition.AddingDefinitions = append(tblInfo.Partition.AddingDefinitions, newDefs...)
}

// removePartitionAddingDefinitionsFromTableInfo remove the `addingDefinitions` in the tableInfo.
func removePartitionAddingDefinitionsFromTableInfo(tblInfo *model.TableInfo) ([]int64, []string) {
	physicalTableIDs := make([]int64, 0, len(tblInfo.Partition.AddingDefinitions))
	partNames := make([]string, 0, len(tblInfo.Partition.AddingDefinitions))
	for _, one := range tblInfo.Partition.AddingDefinitions {
		physicalTableIDs = append(physicalTableIDs, one.ID)
		partNames = append(partNames, one.Name.L)
	}
	tblInfo.Partition.AddingDefinitions = nil
	return physicalTableIDs, partNames
}

// checkAddPartitionValue check add Partition Values,
// For Range: values less than value must be strictly increasing for each partition.
// For List: if a Default partition exists,
//
//	no ADD partition can be allowed
//	(needs reorganize partition instead).
func checkAddPartitionValue(meta *model.TableInfo, part *model.PartitionInfo) error {
	switch meta.Partition.Type {
	case ast.PartitionTypeRange:
		if len(meta.Partition.Columns) == 0 {
			newDefs, oldDefs := part.Definitions, meta.Partition.Definitions
			rangeValue := oldDefs[len(oldDefs)-1].LessThan[0]
			if strings.EqualFold(rangeValue, "MAXVALUE") {
				return errors.Trace(dbterror.ErrPartitionMaxvalue)
			}

			currentRangeValue, err := strconv.Atoi(rangeValue)
			if err != nil {
				return errors.Trace(err)
			}

			for i := range newDefs {
				ifMaxvalue := strings.EqualFold(newDefs[i].LessThan[0], "MAXVALUE")
				if ifMaxvalue && i == len(newDefs)-1 {
					return nil
				} else if ifMaxvalue && i != len(newDefs)-1 {
					return errors.Trace(dbterror.ErrPartitionMaxvalue)
				}

				nextRangeValue, err := strconv.Atoi(newDefs[i].LessThan[0])
				if err != nil {
					return errors.Trace(err)
				}
				if nextRangeValue <= currentRangeValue {
					return errors.Trace(dbterror.ErrRangeNotIncreasing)
				}
				currentRangeValue = nextRangeValue
			}
		}
	case ast.PartitionTypeList:
		if meta.Partition.GetDefaultListPartition() != -1 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("ADD List partition, already contains DEFAULT partition. Please use REORGANIZE PARTITION instead")
		}
	}
	return nil
}

func checkPartitionReplica(replicaCount uint64, addingDefinitions []model.PartitionDefinition, jobCtx *jobContext) (needWait bool, err error) {
	failpoint.Inject("mockWaitTiFlashReplica", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(true, nil)
		}
	})
	failpoint.Inject("mockWaitTiFlashReplicaOK", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(false, nil)
		}
	})

	ctx := context.Background()
	pdCli := jobCtx.store.(tikv.Storage).GetRegionCache().PDClient()
	stores, err := pdCli.GetAllStores(ctx)
	if err != nil {
		return needWait, errors.Trace(err)
	}
	// Check whether stores have `count` tiflash engines.
	tiFlashStoreCount := uint64(0)
	for _, store := range stores {
		if storeHasEngineTiFlashLabel(store) {
			tiFlashStoreCount++
		}
	}
	if replicaCount > tiFlashStoreCount {
		return false, errors.Errorf("[ddl] the tiflash replica count: %d should be less than the total tiflash server count: %d", replicaCount, tiFlashStoreCount)
	}
	for _, pDef := range addingDefinitions {
		startKey, endKey := tablecodec.GetTableHandleKeyRange(pDef.ID)
		regions, err := pdCli.BatchScanRegions(ctx, []router.KeyRange{{StartKey: startKey, EndKey: endKey}}, -1, opt.WithAllowFollowerHandle())
		if err != nil {
			return needWait, errors.Trace(err)
		}
		// For every region in the partition, if it has some corresponding peers and
		// no pending peers, that means the replication has completed.
		for _, region := range regions {
			tiflashPeerAtLeastOne := checkTiFlashPeerStoreAtLeastOne(stores, region.Meta.Peers)
			failpoint.Inject("ForceTiflashNotAvailable", func(v failpoint.Value) {
				tiflashPeerAtLeastOne = v.(bool)
			})
			// It's unnecessary to wait all tiflash peer to be replicated.
			// Here only make sure that tiflash peer count > 0 (at least one).
			if tiflashPeerAtLeastOne {
				continue
			}
			needWait = true
			logutil.DDLLogger().Info("partition replicas check failed in replica-only DDL state", zap.Int64("pID", pDef.ID), zap.Uint64("wait region ID", region.Meta.Id), zap.Bool("tiflash peer at least one", tiflashPeerAtLeastOne), zap.Time("check time", time.Now()))
			return needWait, nil
		}
	}
	logutil.DDLLogger().Info("partition replicas check ok in replica-only DDL state")
	return needWait, nil
}

func checkTiFlashPeerStoreAtLeastOne(stores []*metapb.Store, peers []*metapb.Peer) bool {
	for _, peer := range peers {
		for _, store := range stores {
			if peer.StoreId == store.Id && storeHasEngineTiFlashLabel(store) {
				return true
			}
		}
	}
	return false
}

func storeHasEngineTiFlashLabel(store *metapb.Store) bool {
	for _, label := range store.Labels {
		if label.Key == placement.EngineLabelKey && label.Value == placement.EngineLabelTiFlash {
			return true
		}
	}
	return false
}

func checkListPartitions(defs []*ast.PartitionDefinition) error {
	for _, def := range defs {
		_, ok := def.Clause.(*ast.PartitionDefinitionClauseIn)
		if !ok {
			switch def.Clause.(type) {
			case *ast.PartitionDefinitionClauseLessThan:
				return ast.ErrPartitionWrongValues.GenWithStackByArgs("RANGE", "LESS THAN")
			case *ast.PartitionDefinitionClauseNone:
				return ast.ErrPartitionRequiresValues.GenWithStackByArgs("LIST", "IN")
			default:
				return dbterror.ErrUnsupportedCreatePartition.GenWithStack("Only VALUES IN () is supported for LIST partitioning")
			}
		}
	}
	return nil
}

// buildTablePartitionInfo builds partition info and checks for some errors.
func buildTablePartitionInfo(ctx *metabuild.Context, s *ast.PartitionOptions, tbInfo *model.TableInfo) error {
	if s == nil {
		return nil
	}

	var enable bool
	switch s.Tp {
	case ast.PartitionTypeRange:
		enable = true
	case ast.PartitionTypeList:
		enable = true
		err := checkListPartitions(s.Definitions)
		if err != nil {
			return err
		}
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		// Partition by hash and key is enabled by default.
		if s.Sub != nil {
			// Subpartitioning only allowed with Range or List
			return ast.ErrSubpartition
		}
		// Note that linear hash is simply ignored, and creates non-linear hash/key.
		if s.Linear {
			ctx.AppendWarning(dbterror.ErrUnsupportedCreatePartition.FastGen(fmt.Sprintf("LINEAR %s is not supported, using non-linear %s instead", s.Tp.String(), s.Tp.String())))
		}
		if s.Tp == ast.PartitionTypeHash || len(s.ColumnNames) != 0 {
			enable = true
		}
		if s.Tp == ast.PartitionTypeKey && len(s.ColumnNames) == 0 {
			enable = true
		}
	}

	if !enable {
		ctx.AppendWarning(dbterror.ErrUnsupportedCreatePartition.FastGen(fmt.Sprintf("Unsupported partition type %v, treat as normal table", s.Tp)))
		return nil
	}
	if s.Sub != nil {
		ctx.AppendWarning(dbterror.ErrUnsupportedCreatePartition.FastGen(fmt.Sprintf("Unsupported subpartitioning, only using %v partitioning", s.Tp)))
	}

	pi := &model.PartitionInfo{
		Type:   s.Tp,
		Enable: enable,
		Num:    s.Num,
	}
	tbInfo.Partition = pi
	if s.Expr != nil {
		if err := checkPartitionFuncValid(ctx.GetExprCtx(), tbInfo, s.Expr); err != nil {
			return errors.Trace(err)
		}
		buf := new(bytes.Buffer)
		restoreFlags := format.DefaultRestoreFlags | format.RestoreBracketAroundBinaryOperation |
			format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
		restoreCtx := format.NewRestoreCtx(restoreFlags, buf)
		if err := s.Expr.Restore(restoreCtx); err != nil {
			return err
		}
		pi.Expr = buf.String()
	} else if s.ColumnNames != nil {
		pi.Columns = make([]ast.CIStr, 0, len(s.ColumnNames))
		for _, cn := range s.ColumnNames {
			pi.Columns = append(pi.Columns, cn.Name)
		}
		if pi.Type == ast.PartitionTypeKey && len(s.ColumnNames) == 0 {
			if tbInfo.PKIsHandle {
				pi.Columns = append(pi.Columns, tbInfo.GetPkName())
				pi.IsEmptyColumns = true
			} else if key := tbInfo.GetPrimaryKey(); key != nil {
				for _, col := range key.Columns {
					pi.Columns = append(pi.Columns, col.Name)
				}
				pi.IsEmptyColumns = true
			}
		}
		if err := checkColumnsPartitionType(tbInfo); err != nil {
			return err
		}
	}

	exprCtx := ctx.GetExprCtx()
	err := generatePartitionDefinitionsFromInterval(exprCtx, s, tbInfo)
	if err != nil {
		return errors.Trace(err)
	}

	defs, err := buildPartitionDefinitionsInfo(exprCtx, s.Definitions, tbInfo, s.Num)
	if err != nil {
		return errors.Trace(err)
	}

	tbInfo.Partition.Definitions = defs

	if len(s.UpdateIndexes) > 0 {
		updateIndexes := make([]model.UpdateIndexInfo, 0, len(s.UpdateIndexes))
		dupCheck := make(map[string]struct{})
		for _, idxUpdate := range s.UpdateIndexes {
			idxOffset := -1
			for i := range tbInfo.Indices {
				if strings.EqualFold(tbInfo.Indices[i].Name.L, idxUpdate.Name) {
					idxOffset = i
					break
				}
			}
			if idxOffset == -1 {
				if strings.EqualFold("primary", idxUpdate.Name) &&
					tbInfo.PKIsHandle {
					return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("CLUSTERED INDEX")
				}
				return dbterror.ErrWrongNameForIndex.GenWithStackByArgs(idxUpdate.Name)
			}
			if _, ok := dupCheck[strings.ToLower(idxUpdate.Name)]; ok {
				return dbterror.ErrWrongNameForIndex.GenWithStackByArgs(idxUpdate.Name)
			}
			dupCheck[strings.ToLower(idxUpdate.Name)] = struct{}{}
			if idxUpdate.Option != nil && idxUpdate.Option.Global {
				tbInfo.Indices[idxOffset].Global = true
			} else {
				tbInfo.Indices[idxOffset].Global = false
			}
			setGlobalIndexVersion(tbInfo, tbInfo.Indices[idxOffset])
			updateIndexes = append(updateIndexes, model.UpdateIndexInfo{IndexName: idxUpdate.Name, Global: tbInfo.Indices[idxOffset].Global})
			tbInfo.Partition.DDLUpdateIndexes = updateIndexes
		}
	}

	for _, index := range tbInfo.Indices {
		if index.Unique {
			ck, err := checkPartitionKeysConstraint(pi, index.Columns, tbInfo)
			if err != nil {
				return err
			}
			if !ck {
				if index.Primary && tbInfo.IsCommonHandle {
					return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("CLUSTERED INDEX")
				}
				if !index.Global {
					return dbterror.ErrGlobalIndexNotExplicitlySet.GenWithStackByArgs(index.Name.O)
				}
			}
		}

		if index.HasCondition() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs("partial index is not supported on partitioned table")
		}
	}
	if tbInfo.PKIsHandle {
		// This case is covers when the Handle is the PK (only ints), since it would not
		// have an entry in the tblInfo.Indices
		indexCols := []*model.IndexColumn{{
			Name:   tbInfo.GetPkName(),
			Length: types.UnspecifiedLength,
		}}
		ck, err := checkPartitionKeysConstraint(pi, indexCols, tbInfo)
		if err != nil {
			return err
		}
		if !ck {
			return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("CLUSTERED INDEX")
		}
	}

	return nil
}

func rewritePartitionQueryString(ctx sessionctx.Context, s *ast.PartitionOptions, tbInfo *model.TableInfo) error {
	if s == nil {
		return nil
	}

	if s.Interval != nil {
		// Syntactic sugar for INTERVAL partitioning
		// Generate the resulting CREATE TABLE as the query string
		query, ok := ctx.Value(sessionctx.QueryString).(string)
		if ok {
			sqlMode := ctx.GetSessionVars().SQLMode
			var buf bytes.Buffer
			AppendPartitionDefs(tbInfo.Partition, &buf, sqlMode)

			syntacticSugar := s.Interval.OriginalText()
			syntacticStart := strings.Index(query, syntacticSugar)
			if syntacticStart == -1 {
				logutil.DDLLogger().Error("Can't find INTERVAL definition in prepare stmt",
					zap.String("INTERVAL definition", syntacticSugar), zap.String("prepare stmt", query))
				return errors.Errorf("Can't find INTERVAL definition in PREPARE STMT")
			}
			newQuery := query[:syntacticStart] + "(" + buf.String() + ")" + query[syntacticStart+len(syntacticSugar):]
			ctx.SetValue(sessionctx.QueryString, newQuery)
		}
	}
	return nil
}

func getPartitionColSlices(sctx expression.BuildContext, tblInfo *model.TableInfo, s *ast.PartitionOptions) (partCols stringSlice, err error) {
	if s.Expr != nil {
		extractCols := newPartitionExprChecker(sctx, tblInfo)
		s.Expr.Accept(extractCols)
		partColumns, err := extractCols.columns, extractCols.err
		if err != nil {
			return nil, err
		}
		return columnInfoSlice(partColumns), nil
	} else if len(s.ColumnNames) > 0 {
		return columnNameSlice(s.ColumnNames), nil
	} else if len(s.ColumnNames) == 0 {
		if tblInfo.PKIsHandle {
			return columnInfoSlice([]*model.ColumnInfo{tblInfo.GetPkColInfo()}), nil
		} else if key := tblInfo.GetPrimaryKey(); key != nil {
			colInfos := make([]*model.ColumnInfo, 0, len(key.Columns))
			for _, col := range key.Columns {
				colInfos = append(colInfos, model.FindColumnInfo(tblInfo.Cols(), col.Name.L))
			}
			return columnInfoSlice(colInfos), nil
		}
	}
	return nil, errors.Errorf("Table partition metadata not correct, neither partition expression or list of partition columns")
}

func checkColumnsPartitionType(tbInfo *model.TableInfo) error {
	for _, col := range tbInfo.Partition.Columns {
		colInfo := tbInfo.FindPublicColumnByName(col.L)
		if colInfo == nil {
			return errors.Trace(dbterror.ErrFieldNotFoundPart)
		}
		if !isColTypeAllowedAsPartitioningCol(tbInfo.Partition.Type, colInfo.FieldType) {
			return dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(col.O)
		}
	}
	return nil
}

func isValidKeyPartitionColType(fieldType types.FieldType) bool {
	switch fieldType.GetType() {
	case mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeJSON, mysql.TypeGeometry, mysql.TypeTiDBVectorFloat32:
		return false
	default:
		return true
	}
}

func isColTypeAllowedAsPartitioningCol(partType ast.PartitionType, fieldType types.FieldType) bool {
	// For key partition, the permitted partition field types can be all field types except
	// BLOB, JSON, Geometry
	if partType == ast.PartitionTypeKey {
		return isValidKeyPartitionColType(fieldType)
	}
	// The permitted data types are shown in the following list:
	// All integer types
	// DATE and DATETIME
	// CHAR, VARCHAR, BINARY, and VARBINARY
	// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-columns.html
	// Note that also TIME is allowed in MySQL. Also see https://bugs.mysql.com/bug.php?id=84362
	switch fieldType.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeDuration:
	case mysql.TypeVarchar, mysql.TypeString:
	default:
		return false
	}
	return true
}

// getPartitionIntervalFromTable checks if a partitioned table matches a generated INTERVAL partitioned scheme
// will return nil if error occurs, i.e. not an INTERVAL partitioned table
func getPartitionIntervalFromTable(ctx expression.BuildContext, tbInfo *model.TableInfo) *ast.PartitionInterval {
	if tbInfo.Partition == nil ||
		tbInfo.Partition.Type != ast.PartitionTypeRange {
		return nil
	}
	if len(tbInfo.Partition.Columns) > 1 {
		// Multi-column RANGE COLUMNS is not supported with INTERVAL
		return nil
	}
	if len(tbInfo.Partition.Definitions) < 2 {
		// Must have at least two partitions to calculate an INTERVAL
		return nil
	}

	var (
		interval  ast.PartitionInterval
		startIdx  = 0
		endIdx    = len(tbInfo.Partition.Definitions) - 1
		isIntType = true
		minVal    = "0"
	)
	if len(tbInfo.Partition.Columns) > 0 {
		partCol := findColumnByName(tbInfo.Partition.Columns[0].L, tbInfo)
		if partCol.FieldType.EvalType() == types.ETInt {
			minv := getLowerBoundInt(partCol)
			minVal = strconv.FormatInt(minv, 10)
		} else if partCol.FieldType.EvalType() == types.ETDatetime {
			isIntType = false
			minVal = "0000-01-01"
		} else {
			// Only INT and Datetime columns are supported for INTERVAL partitioning
			return nil
		}
	} else {
		if !isPartExprUnsigned(ctx.GetEvalCtx(), tbInfo) {
			minVal = "-9223372036854775808"
		}
	}

	// Check if possible null partition
	firstPartLessThan := driver.UnwrapFromSingleQuotes(tbInfo.Partition.Definitions[0].LessThan[0])
	if strings.EqualFold(firstPartLessThan, minVal) {
		interval.NullPart = true
		startIdx++
		firstPartLessThan = driver.UnwrapFromSingleQuotes(tbInfo.Partition.Definitions[startIdx].LessThan[0])
	}
	// flag if MAXVALUE partition
	lastPartLessThan := driver.UnwrapFromSingleQuotes(tbInfo.Partition.Definitions[endIdx].LessThan[0])
	if strings.EqualFold(lastPartLessThan, partitionMaxValue) {
		interval.MaxValPart = true
		endIdx--
		lastPartLessThan = driver.UnwrapFromSingleQuotes(tbInfo.Partition.Definitions[endIdx].LessThan[0])
	}
	// Guess the interval
	if startIdx >= endIdx {
		// Must have at least two partitions to calculate an INTERVAL
		return nil
	}
	var firstExpr, lastExpr ast.ExprNode
	if isIntType {
		exprStr := fmt.Sprintf("((%s) - (%s)) DIV %d", lastPartLessThan, firstPartLessThan, endIdx-startIdx)
		expr, err := expression.ParseSimpleExpr(ctx, exprStr)
		if err != nil {
			return nil
		}
		val, isNull, err := expr.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if isNull || err != nil || val < 1 {
			// If NULL, error or interval < 1 then cannot be an INTERVAL partitioned table
			return nil
		}
		interval.IntervalExpr.Expr = ast.NewValueExpr(val, "", "")
		interval.IntervalExpr.TimeUnit = ast.TimeUnitInvalid
		firstExpr, err = astIntValueExprFromStr(firstPartLessThan, minVal == "0")
		if err != nil {
			return nil
		}
		interval.FirstRangeEnd = &firstExpr
		lastExpr, err = astIntValueExprFromStr(lastPartLessThan, minVal == "0")
		if err != nil {
			return nil
		}
		interval.LastRangeEnd = &lastExpr
	} else { // types.ETDatetime
		exprStr := fmt.Sprintf("TIMESTAMPDIFF(SECOND, '%s', '%s')", firstPartLessThan, lastPartLessThan)
		expr, err := expression.ParseSimpleExpr(ctx, exprStr)
		if err != nil {
			return nil
		}
		val, isNull, err := expr.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if isNull || err != nil || val < 1 {
			// If NULL, error or interval < 1 then cannot be an INTERVAL partitioned table
			return nil
		}

		// This will not find all matches > 28 days, since INTERVAL 1 MONTH can generate
		// 2022-01-31, 2022-02-28, 2022-03-31 etc. so we just assume that if there is a
		// diff >= 28 days, we will try with Month and not retry with something else...
		i := val / int64(endIdx-startIdx)
		if i < (28 * 24 * 60 * 60) {
			// Since it is not stored or displayed, non need to try Minute..Week!
			interval.IntervalExpr.Expr = ast.NewValueExpr(i, "", "")
			interval.IntervalExpr.TimeUnit = ast.TimeUnitSecond
		} else {
			// Since it is not stored or displayed, non need to try to match Quarter or Year!
			if (endIdx - startIdx) <= 3 {
				// in case February is in the range
				i = i / (28 * 24 * 60 * 60)
			} else {
				// This should be good for intervals up to 5 years
				i = i / (30 * 24 * 60 * 60)
			}
			interval.IntervalExpr.Expr = ast.NewValueExpr(i, "", "")
			interval.IntervalExpr.TimeUnit = ast.TimeUnitMonth
		}

		firstExpr = ast.NewValueExpr(firstPartLessThan, "", "")
		lastExpr = ast.NewValueExpr(lastPartLessThan, "", "")
		interval.FirstRangeEnd = &firstExpr
		interval.LastRangeEnd = &lastExpr
	}

	partitionMethod := ast.PartitionMethod{
		Tp:       ast.PartitionTypeRange,
		Interval: &interval,
	}
	partOption := &ast.PartitionOptions{PartitionMethod: partitionMethod}
	// Generate the definitions from interval, first and last
	err := generatePartitionDefinitionsFromInterval(ctx, partOption, tbInfo)
	if err != nil {
		return nil
	}

	return &interval
}

// comparePartitionAstAndModel compares a generated *ast.PartitionOptions and a *model.PartitionInfo
func comparePartitionAstAndModel(ctx expression.BuildContext, pAst *ast.PartitionOptions, pModel *model.PartitionInfo, partCol *model.ColumnInfo) error {
	a := pAst.Definitions
	m := pModel.Definitions
	if len(pAst.Definitions) != len(pModel.Definitions) {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning: number of partitions generated != partition defined (%d != %d)", len(a), len(m))
	}

	evalCtx := ctx.GetEvalCtx()
	evalFn := func(expr ast.ExprNode) (types.Datum, error) {
		val, err := expression.EvalSimpleAst(ctx, ast.NewValueExpr(expr, "", ""))
		if err != nil || partCol == nil {
			return val, err
		}
		return val.ConvertTo(evalCtx.TypeCtx(), &partCol.FieldType)
	}
	for i := range pAst.Definitions {
		// Allow options to differ! (like Placement Rules)
		// Allow names to differ!

		// Check MAXVALUE
		maxVD := false
		if strings.EqualFold(m[i].LessThan[0], partitionMaxValue) {
			maxVD = true
		}
		generatedExpr := a[i].Clause.(*ast.PartitionDefinitionClauseLessThan).Exprs[0]
		_, maxVG := generatedExpr.(*ast.MaxValueExpr)
		if maxVG || maxVD {
			if maxVG && maxVD {
				continue
			}
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("INTERVAL partitioning: MAXVALUE clause defined for partition %s differs between generated and defined", m[i].Name.O))
		}

		lessThan := m[i].LessThan[0]
		if len(lessThan) > 1 && lessThan[:1] == "'" && lessThan[len(lessThan)-1:] == "'" {
			lessThan = driver.UnwrapFromSingleQuotes(lessThan)
		}
		lessThanVal, err := evalFn(ast.NewValueExpr(lessThan, "", ""))
		if err != nil {
			return err
		}
		generatedExprVal, err := evalFn(generatedExpr)
		if err != nil {
			return err
		}
		cmp, err := lessThanVal.Compare(evalCtx.TypeCtx(), &generatedExprVal, collate.GetBinaryCollator())
		if err != nil {
			return err
		}
		if cmp != 0 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("INTERVAL partitioning: LESS THAN for partition %s differs between generated and defined", m[i].Name.O))
		}
	}
	return nil
}

// comparePartitionDefinitions check if generated definitions are the same as the given ones
// Allow names to differ
// returns error in case of error or non-accepted difference
func comparePartitionDefinitions(ctx expression.BuildContext, a, b []*ast.PartitionDefinition) error {
	if len(a) != len(b) {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("number of partitions generated != partition defined (%d != %d)", len(a), len(b))
	}
	for i := range a {
		if len(b[i].Sub) > 0 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("partition %s does have unsupported subpartitions", b[i].Name.O))
		}
		// TODO: We could extend the syntax to allow for table options too, like:
		// CREATE TABLE t ... INTERVAL ... LAST PARTITION LESS THAN ('2015-01-01') PLACEMENT POLICY = 'cheapStorage'
		// ALTER TABLE t LAST PARTITION LESS THAN ('2022-01-01') PLACEMENT POLICY 'defaultStorage'
		// ALTER TABLE t LAST PARTITION LESS THAN ('2023-01-01') PLACEMENT POLICY 'fastStorage'
		if len(b[i].Options) > 0 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("partition %s does have unsupported options", b[i].Name.O))
		}
		lessThan, ok := b[i].Clause.(*ast.PartitionDefinitionClauseLessThan)
		if !ok {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("partition %s does not have the right type for LESS THAN", b[i].Name.O))
		}
		definedExpr := lessThan.Exprs[0]
		generatedExpr := a[i].Clause.(*ast.PartitionDefinitionClauseLessThan).Exprs[0]
		_, maxVD := definedExpr.(*ast.MaxValueExpr)
		_, maxVG := generatedExpr.(*ast.MaxValueExpr)
		if maxVG || maxVD {
			if maxVG && maxVD {
				continue
			}
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("partition %s differs between generated and defined for MAXVALUE", b[i].Name.O))
		}
		cmpExpr := &ast.BinaryOperationExpr{
			Op: opcode.EQ,
			L:  definedExpr,
			R:  generatedExpr,
		}
		cmp, err := expression.EvalSimpleAst(ctx, cmpExpr)
		if err != nil {
			return err
		}
		if cmp.GetInt64() != 1 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("partition %s differs between generated and defined for expression", b[i].Name.O))
		}
	}
	return nil
}

func getLowerBoundInt(partCols ...*model.ColumnInfo) int64 {
	ret := int64(0)
	for _, col := range partCols {
		if mysql.HasUnsignedFlag(col.FieldType.GetFlag()) {
			return 0
		}
		ret = min(ret, types.IntegerSignedLowerBound(col.GetType()))
	}
	return ret
}

// generatePartitionDefinitionsFromInterval generates partition Definitions according to INTERVAL options on partOptions
func generatePartitionDefinitionsFromInterval(ctx expression.BuildContext, partOptions *ast.PartitionOptions, tbInfo *model.TableInfo) error {
	if partOptions.Interval == nil {
		return nil
	}
	if tbInfo.Partition.Type != ast.PartitionTypeRange {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, only allowed on RANGE partitioning")
	}
	if len(partOptions.ColumnNames) > 1 || len(tbInfo.Partition.Columns) > 1 {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, does not allow RANGE COLUMNS with more than one column")
	}
	var partCol *model.ColumnInfo
	if len(tbInfo.Partition.Columns) > 0 {
		partCol = findColumnByName(tbInfo.Partition.Columns[0].L, tbInfo)
		if partCol == nil {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, could not find any RANGE COLUMNS")
		}
		// Only support Datetime, date and INT column types for RANGE INTERVAL!
		switch partCol.FieldType.EvalType() {
		case types.ETInt, types.ETDatetime:
		default:
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, only supports Date, Datetime and INT types")
		}
	}
	// Allow given partition definitions, but check it later!
	definedPartDefs := partOptions.Definitions
	partOptions.Definitions = make([]*ast.PartitionDefinition, 0, 1)
	if partOptions.Interval.FirstRangeEnd == nil || partOptions.Interval.LastRangeEnd == nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, currently requires FIRST and LAST partitions to be defined")
	}
	switch partOptions.Interval.IntervalExpr.TimeUnit {
	case ast.TimeUnitInvalid, ast.TimeUnitYear, ast.TimeUnitQuarter, ast.TimeUnitMonth, ast.TimeUnitWeek, ast.TimeUnitDay, ast.TimeUnitHour, ast.TimeUnitMinute, ast.TimeUnitSecond:
	default:
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, only supports YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE and SECOND as time unit")
	}
	first := ast.PartitionDefinitionClauseLessThan{
		Exprs: []ast.ExprNode{*partOptions.Interval.FirstRangeEnd},
	}
	last := ast.PartitionDefinitionClauseLessThan{
		Exprs: []ast.ExprNode{*partOptions.Interval.LastRangeEnd},
	}
	if len(tbInfo.Partition.Columns) > 0 {
		colTypes := collectColumnsType(tbInfo)
		if len(colTypes) != len(tbInfo.Partition.Columns) {
			return dbterror.ErrWrongPartitionName.GenWithStack("partition column name cannot be found")
		}
		if _, err := checkAndGetColumnsTypeAndValuesMatch(ctx, colTypes, first.Exprs); err != nil {
			return err
		}
		if _, err := checkAndGetColumnsTypeAndValuesMatch(ctx, colTypes, last.Exprs); err != nil {
			return err
		}
	} else {
		if err := checkPartitionValuesIsInt(ctx, "FIRST PARTITION", first.Exprs, tbInfo); err != nil {
			return err
		}
		if err := checkPartitionValuesIsInt(ctx, "LAST PARTITION", last.Exprs, tbInfo); err != nil {
			return err
		}
	}
	if partOptions.Interval.NullPart {
		var partExpr ast.ExprNode
		if len(tbInfo.Partition.Columns) == 1 && partOptions.Interval.IntervalExpr.TimeUnit != ast.TimeUnitInvalid {
			// Notice compatibility with MySQL, keyword here is 'supported range' but MySQL seems to work from 0000-01-01 too
			// https://dev.mysql.com/doc/refman/8.0/en/datetime.html says range 1000-01-01 - 9999-12-31
			// https://docs.pingcap.com/tidb/dev/data-type-date-and-time says The supported range is '0000-01-01' to '9999-12-31'
			// set LESS THAN to ZeroTime
			partExpr = ast.NewValueExpr("0000-01-01", "", "")
		} else {
			var minv int64
			if partCol != nil {
				minv = getLowerBoundInt(partCol)
			} else {
				if !isPartExprUnsigned(ctx.GetEvalCtx(), tbInfo) {
					minv = math.MinInt64
				}
			}
			partExpr = ast.NewValueExpr(minv, "", "")
		}
		partOptions.Definitions = append(partOptions.Definitions, &ast.PartitionDefinition{
			Name: ast.NewCIStr("P_NULL"),
			Clause: &ast.PartitionDefinitionClauseLessThan{
				Exprs: []ast.ExprNode{partExpr},
			},
		})
	}

	err := GeneratePartDefsFromInterval(ctx, ast.AlterTablePartition, tbInfo, partOptions)
	if err != nil {
		return err
	}

	if partOptions.Interval.MaxValPart {
		partOptions.Definitions = append(partOptions.Definitions, &ast.PartitionDefinition{
			Name: ast.NewCIStr("P_MAXVALUE"),
			Clause: &ast.PartitionDefinitionClauseLessThan{
				Exprs: []ast.ExprNode{&ast.MaxValueExpr{}},
			},
		})
	}

	if len(definedPartDefs) > 0 {
		err := comparePartitionDefinitions(ctx, partOptions.Definitions, definedPartDefs)
		if err != nil {
			return err
		}
		// Seems valid, so keep the defined so that the user defined names are kept etc.
		partOptions.Definitions = definedPartDefs
	} else if len(tbInfo.Partition.Definitions) > 0 {
		err := comparePartitionAstAndModel(ctx, partOptions, tbInfo.Partition, partCol)
		if err != nil {
			return err
		}
	}

	return nil
}

func checkAndGetColumnsTypeAndValuesMatch(ctx expression.BuildContext, colTypes []types.FieldType, exprs []ast.ExprNode) ([]types.Datum, error) {
	// Validate() has already checked len(colNames) = len(exprs)
	// create table ... partition by range columns (cols)
	// partition p0 values less than (expr)
	// check the type of cols[i] and expr is consistent.
	valDatums := make([]types.Datum, 0, len(colTypes))
	for i, colExpr := range exprs {
		if _, ok := colExpr.(*ast.MaxValueExpr); ok {
			valDatums = append(valDatums, types.NewStringDatum(partitionMaxValue))
			continue
		}
		if d, ok := colExpr.(*ast.DefaultExpr); ok {
			if d.Name != nil {
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
			continue
		}
		colType := colTypes[i]
		val, err := expression.EvalSimpleAst(ctx, colExpr)
		if err != nil {
			return nil, err
		}
		// Check val.ConvertTo(colType) doesn't work, so we need this case by case check.
		vkind := val.Kind()
		switch colType.GetType() {
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeDuration:
			switch vkind {
			case types.KindString, types.KindBytes, types.KindNull:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			switch vkind {
			case types.KindInt64, types.KindUint64, types.KindNull:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		case mysql.TypeFloat, mysql.TypeDouble:
			switch vkind {
			case types.KindFloat32, types.KindFloat64, types.KindNull:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		case mysql.TypeString, mysql.TypeVarString:
			switch vkind {
			case types.KindString, types.KindBytes, types.KindNull, types.KindBinaryLiteral:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		}
		evalCtx := ctx.GetEvalCtx()
		newVal, err := val.ConvertTo(evalCtx.TypeCtx(), &colType)
		if err != nil {
			return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
		}
		valDatums = append(valDatums, newVal)
	}
	return valDatums, nil
}

func astIntValueExprFromStr(s string, unsigned bool) (ast.ExprNode, error) {
	if unsigned {
		u, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return ast.NewValueExpr(u, "", ""), nil
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, err
	}
	return ast.NewValueExpr(i, "", ""), nil
}

// GeneratePartDefsFromInterval generates range partitions from INTERVAL partitioning.
// Handles
//   - CREATE TABLE: all partitions are generated
//   - ALTER TABLE FIRST PARTITION (expr): Drops all partitions before the partition matching the expr (i.e. sets that partition as the new first partition)
//     i.e. will return the partitions from old FIRST partition to (and including) new FIRST partition
//   - ALTER TABLE LAST PARTITION (expr): Creates new partitions from (excluding) old LAST partition to (including) new LAST partition
//
// partition definitions will be set on partitionOptions
func GeneratePartDefsFromInterval(ctx expression.BuildContext, tp ast.AlterTableType, tbInfo *model.TableInfo, partitionOptions *ast.PartitionOptions) error {
	if partitionOptions == nil {
		return nil
	}
	var sb strings.Builder
	err := partitionOptions.Interval.IntervalExpr.Expr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	if err != nil {
		return err
	}
	intervalString := driver.UnwrapFromSingleQuotes(sb.String())
	if len(intervalString) < 1 || intervalString[:1] < "1" || intervalString[:1] > "9" {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL, should be a positive number")
	}
	var currVal types.Datum
	var startExpr, lastExpr, currExpr ast.ExprNode
	var timeUnit ast.TimeUnitType
	var partCol *model.ColumnInfo
	if len(tbInfo.Partition.Columns) == 1 {
		partCol = findColumnByName(tbInfo.Partition.Columns[0].L, tbInfo)
		if partCol == nil {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL COLUMNS partitioning: could not find partitioning column")
		}
	}
	timeUnit = partitionOptions.Interval.IntervalExpr.TimeUnit
	switch tp {
	case ast.AlterTablePartition:
		// CREATE TABLE
		startExpr = *partitionOptions.Interval.FirstRangeEnd
		lastExpr = *partitionOptions.Interval.LastRangeEnd
	case ast.AlterTableDropFirstPartition:
		startExpr = *partitionOptions.Interval.FirstRangeEnd
		lastExpr = partitionOptions.Expr
	case ast.AlterTableAddLastPartition:
		startExpr = *partitionOptions.Interval.LastRangeEnd
		lastExpr = partitionOptions.Expr
	default:
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning: Internal error during generating altered INTERVAL partitions, no known alter type")
	}
	lastVal, err := expression.EvalSimpleAst(ctx, lastExpr)
	if err != nil {
		return err
	}
	evalCtx := ctx.GetEvalCtx()
	if partCol != nil {
		lastVal, err = lastVal.ConvertTo(evalCtx.TypeCtx(), &partCol.FieldType)
		if err != nil {
			return err
		}
	}
	partDefs := partitionOptions.Definitions
	if len(partDefs) == 0 {
		partDefs = make([]*ast.PartitionDefinition, 0, 1)
	}
	for i := range mysql.PartitionCountLimit {
		if i == 0 {
			currExpr = startExpr
			// TODO: adjust the startExpr and have an offset for interval to handle
			// Month/Quarters with start partition on day 28/29/30
			if tp == ast.AlterTableAddLastPartition {
				// ALTER TABLE LAST PARTITION ...
				// Current LAST PARTITION/start already exists, skip to next partition
				continue
			}
		} else {
			currExpr = &ast.BinaryOperationExpr{
				Op: opcode.Mul,
				L:  ast.NewValueExpr(i, "", ""),
				R:  partitionOptions.Interval.IntervalExpr.Expr,
			}
			if timeUnit == ast.TimeUnitInvalid {
				currExpr = &ast.BinaryOperationExpr{
					Op: opcode.Plus,
					L:  startExpr,
					R:  currExpr,
				}
			} else {
				currExpr = &ast.FuncCallExpr{
					FnName: ast.NewCIStr("DATE_ADD"),
					Args: []ast.ExprNode{
						startExpr,
						currExpr,
						&ast.TimeUnitExpr{Unit: timeUnit},
					},
				}
			}
		}
		currVal, err = expression.EvalSimpleAst(ctx, currExpr)
		if err != nil {
			return err
		}
		if partCol != nil {
			currVal, err = currVal.ConvertTo(evalCtx.TypeCtx(), &partCol.FieldType)
			if err != nil {
				return err
			}
		}
		cmp, err := currVal.Compare(evalCtx.TypeCtx(), &lastVal, collate.GetBinaryCollator())
		if err != nil {
			return err
		}
		if cmp > 0 {
			lastStr, err := lastVal.ToString()
			if err != nil {
				return err
			}
			sb.Reset()
			err = startExpr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
			if err != nil {
				return err
			}
			startStr := sb.String()
			errStr := fmt.Sprintf("INTERVAL: expr (%s) not matching FIRST + n INTERVALs (%s + n * %s",
				lastStr, startStr, intervalString)
			if timeUnit != ast.TimeUnitInvalid {
				errStr = errStr + " " + timeUnit.String()
			}
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(errStr + ")")
		}
		valStr, err := currVal.ToString()
		if err != nil {
			return err
		}
		if len(valStr) == 0 || valStr[0:1] == "'" {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning: Error when generating partition values")
		}
		partName := "P_LT_" + valStr
		if timeUnit != ast.TimeUnitInvalid {
			currExpr = ast.NewValueExpr(valStr, "", "")
		} else {
			if valStr[:1] == "-" {
				currExpr = ast.NewValueExpr(currVal.GetInt64(), "", "")
			} else {
				currExpr = ast.NewValueExpr(currVal.GetUint64(), "", "")
			}
		}
		partDefs = append(partDefs, &ast.PartitionDefinition{
			Name: ast.NewCIStr(partName),
			Clause: &ast.PartitionDefinitionClauseLessThan{
				Exprs: []ast.ExprNode{currExpr},
			},
		})
		if cmp == 0 {
			// Last partition!
			break
		}
		// The last loop still not reach the max value, return error.
		if i == mysql.PartitionCountLimit-1 {
			return errors.Trace(dbterror.ErrTooManyPartitions)
		}
	}
	if len(tbInfo.Partition.Definitions)+len(partDefs) > mysql.PartitionCountLimit {
		return errors.Trace(dbterror.ErrTooManyPartitions)
	}
	partitionOptions.Definitions = partDefs
	return nil
}

// buildPartitionDefinitionsInfo build partition definitions info without assign partition id. tbInfo will be constant
func buildPartitionDefinitionsInfo(ctx expression.BuildContext, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo, numParts uint64) (partitions []model.PartitionDefinition, err error) {
	switch tbInfo.Partition.Type {
	case ast.PartitionTypeNone:
		if len(defs) != 1 {
			return nil, dbterror.ErrUnsupportedPartitionType
		}
		partitions = []model.PartitionDefinition{{Name: defs[0].Name}}
		if comment, set := defs[0].Comment(); set {
			partitions[0].Comment = comment
		}
	case ast.PartitionTypeRange:
		partitions, err = buildRangePartitionDefinitions(ctx, defs, tbInfo)
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		partitions, err = buildHashPartitionDefinitions(defs, tbInfo, numParts)
	case ast.PartitionTypeList:
		partitions, err = buildListPartitionDefinitions(ctx, defs, tbInfo)
	default:
		err = dbterror.ErrUnsupportedPartitionType
	}

	if err != nil {
		return nil, err
	}

	return partitions, nil
}

func setPartitionPlacementFromOptions(partition *model.PartitionDefinition, options []*ast.TableOption) error {
	// the partition inheritance of placement rules don't have to copy the placement elements to themselves.
	// For example:
	// t placement policy x (p1 placement policy y, p2)
	// p2 will share the same rule as table t does, but it won't copy the meta to itself. we will
	// append p2 range to the coverage of table t's rules. This mechanism is good for cascading change
	// when policy x is altered.
	for _, opt := range options {
		if opt.Tp == ast.TableOptionPlacementPolicy {
			partition.PlacementPolicyRef = &model.PolicyRefInfo{
				Name: ast.NewCIStr(opt.StrValue),
			}
		}
	}

	return nil
}

func isNonDefaultPartitionOptionsUsed(defs []model.PartitionDefinition) bool {
	for i := range defs {
		orgDef := defs[i]
		if orgDef.Name.O != fmt.Sprintf("p%d", i) {
			return true
		}
		if len(orgDef.Comment) > 0 {
			return true
		}
		if orgDef.PlacementPolicyRef != nil {
			return true
		}
	}
	return false
}

func buildHashPartitionDefinitions(defs []*ast.PartitionDefinition, tbInfo *model.TableInfo, numParts uint64) ([]model.PartitionDefinition, error) {
	if err := checkAddPartitionTooManyPartitions(tbInfo.Partition.Num); err != nil {
		return nil, err
	}

	definitions := make([]model.PartitionDefinition, numParts)
	oldParts := uint64(len(tbInfo.Partition.Definitions))
	for i := range numParts {
		if i < oldParts {
			// Use the existing definitions
			def := tbInfo.Partition.Definitions[i]
			definitions[i].Name = def.Name
			definitions[i].Comment = def.Comment
			definitions[i].PlacementPolicyRef = def.PlacementPolicyRef
		} else if i < oldParts+uint64(len(defs)) {
			// Use the new defs
			def := defs[i-oldParts]
			definitions[i].Name = def.Name
			definitions[i].Comment, _ = def.Comment()
			if err := setPartitionPlacementFromOptions(&definitions[i], def.Options); err != nil {
				return nil, err
			}
		} else {
			// Use the default
			definitions[i].Name = ast.NewCIStr(fmt.Sprintf("p%d", i))
		}
	}
	return definitions, nil
}

func buildListPartitionDefinitions(ctx expression.BuildContext, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) ([]model.PartitionDefinition, error) {
	definitions := make([]model.PartitionDefinition, 0, len(defs))
	exprChecker := newPartitionExprChecker(ctx, nil, checkPartitionExprAllowed)
	colTypes := collectColumnsType(tbInfo)
	if len(colTypes) != len(tbInfo.Partition.Columns) {
		return nil, dbterror.ErrWrongPartitionName.GenWithStack("partition column name cannot be found")
	}
	for _, def := range defs {
		if err := def.Clause.Validate(ast.PartitionTypeList, len(tbInfo.Partition.Columns)); err != nil {
			return nil, err
		}
		clause := def.Clause.(*ast.PartitionDefinitionClauseIn)
		partVals := make([][]types.Datum, 0, len(clause.Values))
		if len(tbInfo.Partition.Columns) > 0 {
			for _, vs := range clause.Values {
				vals, err := checkAndGetColumnsTypeAndValuesMatch(ctx, colTypes, vs)
				if err != nil {
					return nil, err
				}
				partVals = append(partVals, vals)
			}
		} else {
			for _, vs := range clause.Values {
				if err := checkPartitionValuesIsInt(ctx, def.Name, vs, tbInfo); err != nil {
					return nil, err
				}
			}
		}
		comment, _ := def.Comment()
		err := checkTooLongTable(def.Name)
		if err != nil {
			return nil, err
		}
		piDef := model.PartitionDefinition{
			Name:    def.Name,
			Comment: comment,
		}

		if err = setPartitionPlacementFromOptions(&piDef, def.Options); err != nil {
			return nil, err
		}

		buf := new(bytes.Buffer)
		for valIdx, vs := range clause.Values {
			inValue := make([]string, 0, len(vs))
			isDefault := false
			if len(vs) == 1 {
				if _, ok := vs[0].(*ast.DefaultExpr); ok {
					isDefault = true
				}
			}
			if len(partVals) > valIdx && !isDefault {
				for colIdx := range partVals[valIdx] {
					partVal, err := generatePartValuesWithTp(partVals[valIdx][colIdx], colTypes[colIdx])
					if err != nil {
						return nil, err
					}
					inValue = append(inValue, partVal)
				}
			} else {
				for i := range vs {
					vs[i].Accept(exprChecker)
					if exprChecker.err != nil {
						return nil, exprChecker.err
					}
					buf.Reset()
					vs[i].Format(buf)
					inValue = append(inValue, buf.String())
				}
			}
			piDef.InValues = append(piDef.InValues, inValue)
			buf.Reset()
		}
		definitions = append(definitions, piDef)
	}
	return definitions, nil
}

func collectColumnsType(tbInfo *model.TableInfo) []types.FieldType {
	if len(tbInfo.Partition.Columns) > 0 {
		colTypes := make([]types.FieldType, 0, len(tbInfo.Partition.Columns))
		for _, col := range tbInfo.Partition.Columns {
			c := findColumnByName(col.L, tbInfo)
			if c == nil {
				return nil
			}
			colTypes = append(colTypes, c.FieldType)
		}

		return colTypes
	}

	return nil
}

func buildRangePartitionDefinitions(ctx expression.BuildContext, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) ([]model.PartitionDefinition, error) {
	definitions := make([]model.PartitionDefinition, 0, len(defs))
	exprChecker := newPartitionExprChecker(ctx, nil, checkPartitionExprAllowed)
	colTypes := collectColumnsType(tbInfo)
	if len(colTypes) != len(tbInfo.Partition.Columns) {
		return nil, dbterror.ErrWrongPartitionName.GenWithStack("partition column name cannot be found")
	}
	for _, def := range defs {
		if err := def.Clause.Validate(ast.PartitionTypeRange, len(tbInfo.Partition.Columns)); err != nil {
			return nil, err
		}
		clause := def.Clause.(*ast.PartitionDefinitionClauseLessThan)
		var partValDatums []types.Datum
		if len(tbInfo.Partition.Columns) > 0 {
			var err error
			if partValDatums, err = checkAndGetColumnsTypeAndValuesMatch(ctx, colTypes, clause.Exprs); err != nil {
				return nil, err
			}
		} else {
			if err := checkPartitionValuesIsInt(ctx, def.Name, clause.Exprs, tbInfo); err != nil {
				return nil, err
			}
		}
		comment, _ := def.Comment()
		evalCtx := ctx.GetEvalCtx()
		comment, err := validateCommentLength(evalCtx.ErrCtx(), evalCtx.SQLMode(), def.Name.L, &comment, dbterror.ErrTooLongTablePartitionComment)
		if err != nil {
			return nil, err
		}
		err = checkTooLongTable(def.Name)
		if err != nil {
			return nil, err
		}
		piDef := model.PartitionDefinition{
			Name:    def.Name,
			Comment: comment,
		}

		if err = setPartitionPlacementFromOptions(&piDef, def.Options); err != nil {
			return nil, err
		}

		buf := new(bytes.Buffer)
		// Range columns partitions support multi-column partitions.
		for i, expr := range clause.Exprs {
			expr.Accept(exprChecker)
			if exprChecker.err != nil {
				return nil, exprChecker.err
			}
			// If multi-column use new evaluated+normalized output, instead of just formatted expression
			if len(partValDatums) > i {
				var partVal string
				if partValDatums[i].Kind() == types.KindNull {
					return nil, dbterror.ErrNullInValuesLessThan
				}
				if _, ok := clause.Exprs[i].(*ast.MaxValueExpr); ok {
					partVal, err = partValDatums[i].ToString()
					if err != nil {
						return nil, err
					}
				} else {
					partVal, err = generatePartValuesWithTp(partValDatums[i], colTypes[i])
					if err != nil {
						return nil, err
					}
				}

				piDef.LessThan = append(piDef.LessThan, partVal)
			} else {
				expr.Format(buf)
				piDef.LessThan = append(piDef.LessThan, buf.String())
				buf.Reset()
			}
		}
		definitions = append(definitions, piDef)
	}
	return definitions, nil
}

func checkPartitionValuesIsInt(ctx expression.BuildContext, defName any, exprs []ast.ExprNode, tbInfo *model.TableInfo) error {
	tp := types.NewFieldType(mysql.TypeLonglong)
	if isPartExprUnsigned(ctx.GetEvalCtx(), tbInfo) {
		tp.AddFlag(mysql.UnsignedFlag)
	}
	for _, exp := range exprs {
		if _, ok := exp.(*ast.MaxValueExpr); ok {
			continue
		}
		if d, ok := exp.(*ast.DefaultExpr); ok {
			if d.Name != nil {
				return dbterror.ErrPartitionConstDomain.GenWithStackByArgs()
			}
			continue
		}
		val, err := expression.EvalSimpleAst(ctx, exp)
		if err != nil {
			return err
		}
		switch val.Kind() {
		case types.KindUint64, types.KindNull:
		case types.KindInt64:
			if mysql.HasUnsignedFlag(tp.GetFlag()) && val.GetInt64() < 0 {
				return dbterror.ErrPartitionConstDomain.GenWithStackByArgs()
			}
		default:
			return dbterror.ErrValuesIsNotIntType.GenWithStackByArgs(defName)
		}

		evalCtx := ctx.GetEvalCtx()
		_, err = val.ConvertTo(evalCtx.TypeCtx(), tp)
		if err != nil && !types.ErrOverflow.Equal(err) {
			return dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
		}
	}

	return nil
}

func checkPartitionNameUnique(pi *model.PartitionInfo) error {
	newPars := pi.Definitions
	partNames := make(map[string]struct{}, len(newPars))
	for _, newPar := range newPars {
		if _, ok := partNames[newPar.Name.L]; ok {
			return dbterror.ErrSameNamePartition.GenWithStackByArgs(newPar.Name)
		}
		partNames[newPar.Name.L] = struct{}{}
	}
	return nil
}

func checkAddPartitionNameUnique(tbInfo *model.TableInfo, pi *model.PartitionInfo) error {
	partNames := make(map[string]struct{})
	if tbInfo.Partition != nil {
		oldPars := tbInfo.Partition.Definitions
		for _, oldPar := range oldPars {
			partNames[oldPar.Name.L] = struct{}{}
		}
	}
	newPars := pi.Definitions
	for _, newPar := range newPars {
		if _, ok := partNames[newPar.Name.L]; ok {
			return dbterror.ErrSameNamePartition.GenWithStackByArgs(newPar.Name)
		}
		partNames[newPar.Name.L] = struct{}{}
	}
	return nil
}

func checkReorgPartitionNames(p *model.PartitionInfo, droppedNames []string, pi *model.PartitionInfo) error {
	partNames := make(map[string]struct{})
	oldDefs := p.Definitions
	for _, oldDef := range oldDefs {
		partNames[oldDef.Name.L] = struct{}{}
	}
	for _, delName := range droppedNames {
		droppedName := strings.ToLower(delName)
		if _, ok := partNames[droppedName]; !ok {
			return dbterror.ErrSameNamePartition.GenWithStackByArgs(delName)
		}
		delete(partNames, droppedName)
	}
	newDefs := pi.Definitions
	for _, newDef := range newDefs {
		if _, ok := partNames[newDef.Name.L]; ok {
			return dbterror.ErrSameNamePartition.GenWithStackByArgs(newDef.Name)
		}
		partNames[newDef.Name.L] = struct{}{}
	}
	return nil
}

func checkAndOverridePartitionID(newTableInfo, oldTableInfo *model.TableInfo) error {
	// If any old partitionInfo has lost, that means the partition ID lost too, so did the data, repair failed.
	if newTableInfo.Partition == nil {
		return nil
	}
	if oldTableInfo.Partition == nil {
		return dbterror.ErrRepairTableFail.GenWithStackByArgs("Old table doesn't have partitions")
	}
	if newTableInfo.Partition.Type != oldTableInfo.Partition.Type {
		return dbterror.ErrRepairTableFail.GenWithStackByArgs("Partition type should be the same")
	}
	// Check whether partitionType is hash partition.
	if newTableInfo.Partition.Type == ast.PartitionTypeHash {
		if newTableInfo.Partition.Num != oldTableInfo.Partition.Num {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Hash partition num should be the same")
		}
	}
	for i, newOne := range newTableInfo.Partition.Definitions {
		found := false
		for _, oldOne := range oldTableInfo.Partition.Definitions {
			// Fix issue 17952 which wanna substitute partition range expr.
			// So eliminate stringSliceEqual(newOne.LessThan, oldOne.LessThan) here.
			if newOne.Name.L == oldOne.Name.L {
				newTableInfo.Partition.Definitions[i].ID = oldOne.ID
				found = true
				break
			}
		}
		if !found {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Partition " + newOne.Name.L + " has lost")
		}
	}
	return nil
}

// checkPartitionFuncValid checks partition function validly.
func checkPartitionFuncValid(ctx expression.BuildContext, tblInfo *model.TableInfo, expr ast.ExprNode) error {
	if expr == nil {
		return nil
	}
	exprChecker := newPartitionExprChecker(ctx, tblInfo, checkPartitionExprArgs, checkPartitionExprAllowed)
	expr.Accept(exprChecker)
	if exprChecker.err != nil {
		return errors.Trace(exprChecker.err)
	}
	if len(exprChecker.columns) == 0 {
		return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
	}
	return nil
}

// checkResultOK derives from https://github.com/mysql/mysql-server/blob/5.7/sql/item_timefunc
// For partition tables, mysql do not support Constant, random or timezone-dependent expressions
// Based on mysql code to check whether field is valid, every time related type has check_valid_arguments_processor function.
func checkResultOK(ok bool) error {
	if !ok {
		return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
	}

	return nil
}

// checkPartitionFuncType checks partition function return type.
func checkPartitionFuncType(ctx expression.BuildContext, anyExpr any, schema string, tblInfo *model.TableInfo) error {
	if anyExpr == nil {
		return nil
	}
	if schema == "" {
		schema = ctx.GetEvalCtx().CurrentDB()
	}
	var e expression.Expression
	var err error
	switch expr := anyExpr.(type) {
	case string:
		if expr == "" {
			return nil
		}
		e, err = expression.ParseSimpleExpr(ctx, expr, expression.WithTableInfo(schema, tblInfo))
	case ast.ExprNode:
		e, err = expression.BuildSimpleExpr(ctx, expr, expression.WithTableInfo(schema, tblInfo))
	default:
		return errors.Trace(dbterror.ErrPartitionFuncNotAllowed.GenWithStackByArgs("PARTITION"))
	}
	if err != nil {
		return errors.Trace(err)
	}
	if e.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
		return nil
	}
	if col, ok := e.(*expression.Column); ok {
		if col2, ok2 := anyExpr.(*ast.ColumnNameExpr); ok2 {
			return errors.Trace(dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(col2.Name.Name.L))
		}
		return errors.Trace(dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(col.OrigName))
	}
	return errors.Trace(dbterror.ErrPartitionFuncNotAllowed.GenWithStackByArgs("PARTITION"))
}

// checkRangePartitionValue checks whether `less than value` is strictly increasing for each partition.
// Side effect: it may simplify the partition range definition from a constant expression to an integer.
func checkRangePartitionValue(ctx expression.BuildContext, tblInfo *model.TableInfo) error {
	pi := tblInfo.Partition
	defs := pi.Definitions
	if len(defs) == 0 {
		return nil
	}

	if strings.EqualFold(defs[len(defs)-1].LessThan[0], partitionMaxValue) {
		defs = defs[:len(defs)-1]
	}
	isUnsigned := isPartExprUnsigned(ctx.GetEvalCtx(), tblInfo)
	var prevRangeValue any
	for i := range defs {
		if strings.EqualFold(defs[i].LessThan[0], partitionMaxValue) {
			return errors.Trace(dbterror.ErrPartitionMaxvalue)
		}

		currentRangeValue, fromExpr, err := getRangeValue(ctx, defs[i].LessThan[0], isUnsigned)
		if err != nil {
			return errors.Trace(err)
		}
		if fromExpr {
			// Constant fold the expression.
			defs[i].LessThan[0] = fmt.Sprintf("%d", currentRangeValue)
		}

		if i == 0 {
			prevRangeValue = currentRangeValue
			continue
		}

		if isUnsigned {
			if currentRangeValue.(uint64) <= prevRangeValue.(uint64) {
				return errors.Trace(dbterror.ErrRangeNotIncreasing)
			}
		} else {
			if currentRangeValue.(int64) <= prevRangeValue.(int64) {
				return errors.Trace(dbterror.ErrRangeNotIncreasing)
			}
		}
		prevRangeValue = currentRangeValue
	}
	return nil
}

func checkListPartitionValue(ctx expression.BuildContext, tblInfo *model.TableInfo) error {
	pi := tblInfo.Partition
	if len(pi.Definitions) == 0 {
		return ast.ErrPartitionsMustBeDefined.GenWithStackByArgs("LIST")
	}
	expStr, err := formatListPartitionValue(ctx, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}

	partitionsValuesMap := make(map[string]struct{})
	for _, s := range expStr {
		if _, ok := partitionsValuesMap[s]; ok {
			return errors.Trace(dbterror.ErrMultipleDefConstInListPart)
		}
		partitionsValuesMap[s] = struct{}{}
	}

	return nil
}

func formatListPartitionValue(ctx expression.BuildContext, tblInfo *model.TableInfo) ([]string, error) {
	defs := tblInfo.Partition.Definitions
	pi := tblInfo.Partition
	var colTps []*types.FieldType
	cols := make([]*model.ColumnInfo, 0, len(pi.Columns))
	if len(pi.Columns) == 0 {
		tp := types.NewFieldType(mysql.TypeLonglong)
		if isPartExprUnsigned(ctx.GetEvalCtx(), tblInfo) {
			tp.AddFlag(mysql.UnsignedFlag)
		}
		colTps = []*types.FieldType{tp}
	} else {
		colTps = make([]*types.FieldType, 0, len(pi.Columns))
		for _, colName := range pi.Columns {
			colInfo := findColumnByName(colName.L, tblInfo)
			if colInfo == nil {
				return nil, errors.Trace(dbterror.ErrFieldNotFoundPart)
			}
			colTps = append(colTps, colInfo.FieldType.Clone())
			cols = append(cols, colInfo)
		}
	}

	haveDefault := false
	exprStrs := make([]string, 0)
	inValueStrs := make([]string, 0, max(len(pi.Columns), 1))
	for i := range defs {
	inValuesLoop:
		for j, vs := range defs[i].InValues {
			inValueStrs = inValueStrs[:0]
			for k, v := range vs {
				// if DEFAULT would be given as string, like "DEFAULT",
				// it would be stored as "'DEFAULT'",
				if strings.EqualFold(v, "DEFAULT") && k == 0 && len(vs) == 1 {
					if haveDefault {
						return nil, dbterror.ErrMultipleDefConstInListPart
					}
					haveDefault = true
					continue inValuesLoop
				}
				if strings.EqualFold(v, "MAXVALUE") {
					return nil, errors.Trace(dbterror.ErrMaxvalueInValuesIn)
				}
				expr, err := expression.ParseSimpleExpr(ctx, v, expression.WithCastExprTo(colTps[k]))
				if err != nil {
					return nil, errors.Trace(err)
				}
				eval, err := expr.Eval(ctx.GetEvalCtx(), chunk.Row{})
				if err != nil {
					return nil, errors.Trace(err)
				}
				s, err := eval.ToString()
				if err != nil {
					return nil, errors.Trace(err)
				}
				if eval.IsNull() {
					s = "NULL"
				} else {
					if colTps[k].EvalType() == types.ETInt {
						defs[i].InValues[j][k] = s
					}
					if colTps[k].EvalType() == types.ETString {
						s = string(hack.String(collate.GetCollator(cols[k].GetCollate()).Key(s)))
						s = driver.WrapInSingleQuotes(s)
					}
				}
				inValueStrs = append(inValueStrs, s)
			}
			exprStrs = append(exprStrs, strings.Join(inValueStrs, ","))
		}
	}
	return exprStrs, nil
}

// getRangeValue gets an integer from the range value string.
// The returned boolean value indicates whether the input string is a constant expression.
func getRangeValue(ctx expression.BuildContext, str string, unsigned bool) (any, bool, error) {
	// Unsigned bigint was converted to uint64 handle.
	if unsigned {
		if value, err := strconv.ParseUint(str, 10, 64); err == nil {
			return value, false, nil
		}

		e, err1 := expression.ParseSimpleExpr(ctx, str)
		if err1 != nil {
			return 0, false, err1
		}
		res, isNull, err2 := e.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if err2 == nil && !isNull {
			return uint64(res), true, nil
		}
	} else {
		if value, err := strconv.ParseInt(str, 10, 64); err == nil {
			return value, false, nil
		}
		// The range value maybe not an integer, it could be a constant expression.
		// For example, the following two cases are the same:
		// PARTITION p0 VALUES LESS THAN (TO_SECONDS('2004-01-01'))
		// PARTITION p0 VALUES LESS THAN (63340531200)
		e, err1 := expression.ParseSimpleExpr(ctx, str)
		if err1 != nil {
			return 0, false, err1
		}
		res, isNull, err2 := e.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if err2 == nil && !isNull {
			return res, true, nil
		}
	}
	return 0, false, dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(str)
}

// CheckDropTablePartition checks if the partition exists and does not allow deleting the last existing partition in the table.
func CheckDropTablePartition(meta *model.TableInfo, partLowerNames []string) error {
	pi := meta.Partition
	if pi.Type != ast.PartitionTypeRange && pi.Type != ast.PartitionTypeList {
		return dbterror.ErrOnlyOnRangeListPartition.GenWithStackByArgs("DROP")
	}

	if meta.Affinity != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("DROP PARTITION of a table with AFFINITY option")
	}

	// To be error compatible with MySQL, we need to do this first!
	// see https://github.com/pingcap/tidb/issues/31681#issuecomment-1015536214
	oldDefs := pi.Definitions
	if len(oldDefs) <= len(partLowerNames) {
		return errors.Trace(dbterror.ErrDropLastPartition)
	}

	dupCheck := make(map[string]bool)
	for _, pn := range partLowerNames {
		found := false
		for _, def := range oldDefs {
			if def.Name.L == pn {
				if _, ok := dupCheck[pn]; ok {
					return errors.Trace(dbterror.ErrDropPartitionNonExistent.GenWithStackByArgs("DROP"))
				}
				dupCheck[pn] = true
				found = true
				break
			}
		}
		if !found {
			return errors.Trace(dbterror.ErrDropPartitionNonExistent.GenWithStackByArgs("DROP"))
		}
	}
	return nil
}

// updateDroppingPartitionInfo move dropping partitions to DroppingDefinitions
func updateDroppingPartitionInfo(tblInfo *model.TableInfo, partLowerNames []string) {
	oldDefs := tblInfo.Partition.Definitions
	newDefs := make([]model.PartitionDefinition, 0, len(oldDefs)-len(partLowerNames))
	droppingDefs := make([]model.PartitionDefinition, 0, len(partLowerNames))

	// consider using a map to probe partLowerNames if too many partLowerNames
	for i := range oldDefs {
		found := slices.Contains(partLowerNames, oldDefs[i].Name.L)
		if found {
			droppingDefs = append(droppingDefs, oldDefs[i])
		} else {
			newDefs = append(newDefs, oldDefs[i])
		}
	}

	tblInfo.Partition.Definitions = newDefs
	tblInfo.Partition.DroppingDefinitions = droppingDefs
}

func getPartitionDef(tblInfo *model.TableInfo, partName string) (index int, def *model.PartitionDefinition, _ error) {
	defs := tblInfo.Partition.Definitions
	for i := range defs {
		if strings.EqualFold(defs[i].Name.L, strings.ToLower(partName)) {
			return i, &(defs[i]), nil
		}
	}
	return index, nil, table.ErrUnknownPartition.GenWithStackByArgs(partName, tblInfo.Name.O)
}

func getPartitionIDsFromDefinitions(defs []model.PartitionDefinition) []int64 {
	pids := make([]int64, 0, len(defs))
	for _, def := range defs {
		pids = append(pids, def.ID)
	}
	return pids
}

func hasGlobalIndex(tblInfo *model.TableInfo) bool {
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.Global {
			return true
		}
	}
	return false
}

// getTableInfoWithDroppingPartitions builds oldTableInfo including dropping partitions, only used by onDropTablePartition.
func getTableInfoWithDroppingPartitions(t *model.TableInfo) *model.TableInfo {
	p := t.Partition
	nt := t.Clone()
	np := *p
	npd := make([]model.PartitionDefinition, 0, len(p.Definitions)+len(p.DroppingDefinitions))
	npd = append(npd, p.Definitions...)
	npd = append(npd, p.DroppingDefinitions...)
	np.Definitions = npd
	np.DroppingDefinitions = nil
	nt.Partition = &np
	return nt
}

func dropLabelRules(ctx context.Context, schemaName, tableName string, partNames []string) error {
	deleteRules := make([]string, 0, len(partNames))
	for _, partName := range partNames {
		deleteRules = append(deleteRules, fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, schemaName, tableName, partName))
	}
	// delete batch rules
	patch := label.NewRulePatch([]*label.Rule{}, deleteRules)
	return infosync.UpdateLabelRules(ctx, patch)
}

// rollbackLikeDropPartition does rollback for Reorganize partition and Add partition.
// It will drop newly created partitions that has not yet been used, including cleaning
// up label rules and bundles as well as changed indexes due to global flag.
func (w *worker) rollbackLikeDropPartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	partInfo := args.PartInfo
	metaMut := jobCtx.metaMut
	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	tblInfo.Partition.DroppingDefinitions = nil
	// Collect table/partition ids to clean up, through args.OldPhysicalTblIDs
	// GC will later also drop matching Placement bundles.
	// If we delete them now, it could lead to non-compliant placement or failure during flashback
	physicalTableIDs, pNames := removePartitionAddingDefinitionsFromTableInfo(tblInfo)
	// TODO: Will this drop LabelRules for existing partitions, if the new partitions have the same name?
	err = dropLabelRules(w.ctx, job.SchemaName, tblInfo.Name.L, pNames)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the label rules")
	}

	if _, err := alterTableLabelRule(job.SchemaName, tblInfo, getIDs([]*model.TableInfo{tblInfo})); err != nil {
		job.State = model.JobStateCancelled
		return ver, err
	}
	if partInfo.Type != ast.PartitionTypeNone {
		// ALTER TABLE ... PARTITION BY
		// Also remove anything with the new table id
		if partInfo.NewTableID != 0 {
			physicalTableIDs = append(physicalTableIDs, partInfo.NewTableID)
		}
		// Reset if it was normal table before
		if tblInfo.Partition.Type == ast.PartitionTypeNone ||
			tblInfo.Partition.DDLType == ast.PartitionTypeNone {
			tblInfo.Partition = nil
		}
	}

	var dropIndices []*model.IndexInfo
	for _, indexInfo := range tblInfo.Indices {
		if indexInfo.State == model.StateWriteOnly {
			dropIndices = append(dropIndices, indexInfo)
		}
	}
	var deleteIndices []model.TableIDIndexID
	for _, indexInfo := range dropIndices {
		DropIndexColumnFlag(tblInfo, indexInfo)
		RemoveDependentHiddenColumns(tblInfo, indexInfo)
		removeIndexInfo(tblInfo, indexInfo)
		if indexInfo.Global {
			deleteIndices = append(deleteIndices, model.TableIDIndexID{TableID: tblInfo.ID, IndexID: indexInfo.ID})
		}
		// All other indexes has only been applied to new partitions, that is deleted in whole,
		// including indexes.
	}
	if tblInfo.Partition != nil {
		tblInfo.Partition.ClearReorgIntermediateInfo()
	}

	_, err = alterTablePartitionBundles(metaMut, tblInfo, nil)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
	}
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	args.OldPhysicalTblIDs = physicalTableIDs
	args.OldGlobalIndexes = deleteIndices
	job.FillFinishedArgs(args)
	return ver, nil
}

// onDropTablePartition deletes old partition meta.
// States in reverse order:
// StateNone
//
//	Old partitions are queued to be deleted (delete_range), global index up-to-date
//
// StateDeleteReorganization
//
//	Old partitions are not accessible/used by any sessions.
//	Inserts/updates of global index which still have entries pointing to old partitions
//	will overwrite those entries.
//	In the background we are reading all old partitions and deleting their entries from
//	the global indexes.
//
// StateDeleteOnly
//
//	Old partitions are no longer visible, but if there is inserts/updates to the global indexes,
//	duplicate key errors will be given, even if the entries are from dropped partitions.
//
// StateWriteOnly
//
//	Old partitions are blocked for read and write. But for read we are allowing
//	"overlapping" partition to be read instead. Which means that write can only
//	happen in the 'overlapping' partitions original range, not into the extended
//	range open by the dropped partitions.
//
// StatePublic
//
//	Original state, unaware of DDL
func (w *worker) onDropTablePartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	partNames := args.PartNames
	metaMut := jobCtx.metaMut
	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch job.SchemaState {
	case model.StatePublic:
		// Here we mark the partitions to be dropped, so they are not read or written
		err = CheckDropTablePartition(tblInfo, partNames)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// Reason, see https://github.com/pingcap/tidb/issues/55888
		// Only mark the partitions as to be dropped, so they are not used, but not yet removed.
		originalDefs := tblInfo.Partition.Definitions
		updateDroppingPartitionInfo(tblInfo, partNames)
		tblInfo.Partition.Definitions = originalDefs
		job.SchemaState = model.StateWriteOnly
		tblInfo.Partition.DDLState = job.SchemaState
		tblInfo.Partition.DDLAction = job.Type

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateWriteOnly:
		// Since the previous state do not use the dropping partitions,
		// we can now actually remove them, allowing to write into the overlapping range
		// of the higher range partition or LIST default partition.
		updateDroppingPartitionInfo(tblInfo, partNames)
		err = dropLabelRules(jobCtx.stepCtx, job.SchemaName, tblInfo.Name.L, partNames)
		if err != nil {
			// TODO: Add failpoint error/cancel injection and test failure/rollback and cancellation!
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the label rules")
		}

		if _, err := alterTableLabelRule(job.SchemaName, tblInfo, getIDs([]*model.TableInfo{tblInfo})); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		var bundles []*placement.Bundle
		// create placement groups for each dropped partition to keep the data's placement before GC
		// These placements groups will be deleted after GC
		bundles, err = droppedPartitionBundles(metaMut, tblInfo, tblInfo.Partition.DroppingDefinitions)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		var tableBundle *placement.Bundle
		// Recompute table bundle to remove dropped partitions rules from its group
		tableBundle, err = placement.NewTableBundle(metaMut, tblInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		if tableBundle != nil {
			bundles = append(bundles, tableBundle)
		}

		if err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		job.SchemaState = model.StateDeleteOnly
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateDeleteOnly:
		// This state is not a real 'DeleteOnly' state, because tidb does not maintain the state check in partitionDefinition.
		// Insert this state to confirm all servers can not see the old partitions when reorg is running,
		// so that no new data will be inserted into old partitions when reorganizing.
		job.SchemaState = model.StateDeleteReorganization
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateDeleteReorganization:
		physicalTableIDs := getPartitionIDsFromDefinitions(tblInfo.Partition.DroppingDefinitions)
		if hasGlobalIndex(tblInfo) {
			oldTblInfo := getTableInfoWithDroppingPartitions(tblInfo)
			var done bool
			done, err = w.cleanGlobalIndexEntriesFromDroppedPartitions(jobCtx, job, oldTblInfo, physicalTableIDs)
			if err != nil || !done {
				return ver, errors.Trace(err)
			}
		}
		removeTiFlashAvailablePartitionIDs(tblInfo, physicalTableIDs)
		droppedDefs := tblInfo.Partition.DroppingDefinitions
		tblInfo.Partition.DroppingDefinitions = nil
		job.SchemaState = model.StateNone
		tblInfo.Partition.DDLState = job.SchemaState
		tblInfo.Partition.DDLAction = model.ActionNone
		// used by ApplyDiff in updateSchemaVersion
		args.OldPhysicalTblIDs = physicalTableIDs
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		dropPartitionEvent := notifier.NewDropPartitionEvent(
			tblInfo,
			&model.PartitionInfo{Definitions: droppedDefs},
		)
		err = asyncNotifyEvent(jobCtx, dropPartitionEvent, job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}

		job.SchemaState = model.StateNone
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		// A background job will be created to delete old partition data.
		job.FillFinishedArgs(args)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}
	return ver, errors.Trace(err)
}

func removeTiFlashAvailablePartitionIDs(tblInfo *model.TableInfo, pids []int64) {
	if tblInfo.TiFlashReplica == nil {
		return
	}
	// Remove the partitions
	ids := tblInfo.TiFlashReplica.AvailablePartitionIDs
	// Rarely called, so OK to take some time, to make it easy
	for _, id := range pids {
		for i, avail := range ids {
			if id == avail {
				tmp := ids[:i]
				tmp = append(tmp, ids[i+1:]...)
				ids = tmp
				break
			}
		}
	}
	tblInfo.TiFlashReplica.AvailablePartitionIDs = ids
}




func checkAddPartitionTooManyPartitions(piDefs uint64) error {
	if piDefs > uint64(mysql.PartitionCountLimit) {
		return errors.Trace(dbterror.ErrTooManyPartitions)
	}
	return nil
}

func checkAddPartitionOnTemporaryMode(tbInfo *model.TableInfo) error {
	if tbInfo.Partition != nil && tbInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrPartitionNoTemporary
	}
	return nil
}

func checkPartitionColumnsUnique(tbInfo *model.TableInfo) error {
	if len(tbInfo.Partition.Columns) <= 1 {
		return nil
	}
	var columnsMap = make(map[string]struct{})
	for _, col := range tbInfo.Partition.Columns {
		if _, ok := columnsMap[col.L]; ok {
			return dbterror.ErrSameNamePartitionField.GenWithStackByArgs(col.L)
		}
		columnsMap[col.L] = struct{}{}
	}
	return nil
}

func checkNoHashPartitions(partitionNum uint64) error {
	if partitionNum == 0 {
		return ast.ErrNoParts.GenWithStackByArgs("partitions")
	}
	return nil
}

func getPartitionIDs(table *model.TableInfo) []int64 {
	if table.GetPartitionInfo() == nil {
		return []int64{}
	}
	physicalTableIDs := make([]int64, 0, len(table.Partition.Definitions))
	for _, def := range table.Partition.Definitions {
		physicalTableIDs = append(physicalTableIDs, def.ID)
	}
	return physicalTableIDs
}

func getPartitionRuleIDs(dbName string, table *model.TableInfo) []string {
	if table.GetPartitionInfo() == nil {
		return []string{}
	}
	partRuleIDs := make([]string, 0, len(table.Partition.Definitions))
	for _, def := range table.Partition.Definitions {
		partRuleIDs = append(partRuleIDs, fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, dbName, table.Name.L, def.Name.L))
	}
	return partRuleIDs
}

// checkPartitioningKeysConstraints checks that the range partitioning key is included in the table constraint.
func checkPartitioningKeysConstraints(ctx *metabuild.Context, s *ast.CreateTableStmt, tblInfo *model.TableInfo) error {
	// Returns directly if there are no unique keys in the table.
	if len(tblInfo.Indices) == 0 && !tblInfo.PKIsHandle {
		return nil
	}

	partCols, err := getPartitionColSlices(ctx.GetExprCtx(), tblInfo, s.Partition)
	if err != nil {
		return errors.Trace(err)
	}

	// Checks that the partitioning key is included in the constraint.
	// Every unique key on the table must use every column in the table's partitioning expression.
	// See https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	for _, index := range tblInfo.Indices {
		if index.Unique && !checkUniqueKeyIncludePartKey(partCols, index.Columns) {
			if index.Primary && tblInfo.IsCommonHandle {
				// global index does not support clustered index
				return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("CLUSTERED INDEX")
			}
		}
	}
	// when PKIsHandle, tblInfo.Indices will not contain the primary key.
	if tblInfo.PKIsHandle {
		indexCols := []*model.IndexColumn{{
			Name:   tblInfo.GetPkName(),
			Length: types.UnspecifiedLength,
		}}
		if !checkUniqueKeyIncludePartKey(partCols, indexCols) {
			return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("CLUSTERED INDEX")
		}
	}
	return nil
}

func checkPartitionKeysConstraint(pi *model.PartitionInfo, indexColumns []*model.IndexColumn, tblInfo *model.TableInfo) (bool, error) {
	var (
		partCols []*model.ColumnInfo
		err      error
	)
	if pi.Type == ast.PartitionTypeNone {
		return true, nil
	}
	// The expr will be an empty string if the partition is defined by:
	// CREATE TABLE t (...) PARTITION BY RANGE COLUMNS(...)
	if partExpr := pi.Expr; partExpr != "" {
		// Parse partitioning key, extract the column names in the partitioning key to slice.
		partCols, err = extractPartitionColumns(partExpr, tblInfo)
		if err != nil {
			return false, err
		}
	} else {
		partCols = make([]*model.ColumnInfo, 0, len(pi.Columns))
		for _, col := range pi.Columns {
			colInfo := tblInfo.FindPublicColumnByName(col.L)
			if colInfo == nil {
				return false, infoschema.ErrColumnNotExists.GenWithStackByArgs(col, tblInfo.Name)
			}
			partCols = append(partCols, colInfo)
		}
	}

	// In MySQL, every unique key on the table must use every column in the table's
	// partitioning expression.(This also includes the table's primary key.)
	// See https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	// TiDB can remove this limitation with Global Index
	return checkUniqueKeyIncludePartKey(columnInfoSlice(partCols), indexColumns), nil
}

type columnNameExtractor struct {
	extractedColumns []*model.ColumnInfo
	tblInfo          *model.TableInfo
	err              error
}

func (*columnNameExtractor) Enter(node ast.Node) (ast.Node, bool) {
	return node, false
}

func (cne *columnNameExtractor) Leave(node ast.Node) (ast.Node, bool) {
	if c, ok := node.(*ast.ColumnNameExpr); ok {
		info := findColumnByName(c.Name.Name.L, cne.tblInfo)
		if info != nil {
			cne.extractedColumns = append(cne.extractedColumns, info)
			return node, true
		}
		cne.err = dbterror.ErrBadField.GenWithStackByArgs(c.Name.Name.O, "expression")
		return nil, false
	}
	return node, true
}

func findColumnByName(colName string, tblInfo *model.TableInfo) *model.ColumnInfo {
	if tblInfo == nil {
		return nil
	}
	for _, info := range tblInfo.Columns {
		if info.Name.L == colName {
			return info
		}
	}
	return nil
}

func extractPartitionColumns(partExpr string, tblInfo *model.TableInfo) ([]*model.ColumnInfo, error) {
	partExpr = "select " + partExpr
	stmts, _, err := parser.New().ParseSQL(partExpr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	extractor := &columnNameExtractor{
		tblInfo:          tblInfo,
		extractedColumns: make([]*model.ColumnInfo, 0),
	}
	stmts[0].Accept(extractor)
	if extractor.err != nil {
		return nil, errors.Trace(extractor.err)
	}
	return extractor.extractedColumns, nil
}

// stringSlice is defined for checkUniqueKeyIncludePartKey.
// if Go supports covariance, the code shouldn't be so complex.
type stringSlice interface {
	Len() int
	At(i int) string
}

// checkUniqueKeyIncludePartKey checks that the partitioning key is included in the constraint.
func checkUniqueKeyIncludePartKey(partCols stringSlice, idxCols []*model.IndexColumn) bool {
	for i := range partCols.Len() {
		partCol := partCols.At(i)
		_, idxCol := model.FindIndexColumnByName(idxCols, partCol)
		if idxCol == nil {
			// Partition column is not found in the index columns.
			return false
		}
		if idxCol.Length > 0 {
			// The partition column is found in the index columns, but the index column is a prefix index
			return false
		}
	}
	return true
}

// columnInfoSlice implements the stringSlice interface.
type columnInfoSlice []*model.ColumnInfo

func (cis columnInfoSlice) Len() int {
	return len(cis)
}

func (cis columnInfoSlice) At(i int) string {
	return cis[i].Name.L
}

// columnNameSlice implements the stringSlice interface.
type columnNameSlice []*ast.ColumnName

func (cns columnNameSlice) Len() int {
	return len(cns)
}

func (cns columnNameSlice) At(i int) string {
	return cns[i].Name.L
}

func isPartExprUnsigned(ectx expression.EvalContext, tbInfo *model.TableInfo) bool {
	ctx := tables.NewPartitionExprBuildCtx()
	expr, err := expression.ParseSimpleExpr(ctx, tbInfo.Partition.Expr, expression.WithTableInfo("", tbInfo))
	if err != nil {
		logutil.DDLLogger().Error("isPartExpr failed parsing expression!", zap.Error(err))
		return false
	}
	if mysql.HasUnsignedFlag(expr.GetType(ectx).GetFlag()) {
		return true
	}
	return false
}

// truncateTableByReassignPartitionIDs reassigns new partition ids.
// it also returns the new partition IDs for cases described below.
func truncateTableByReassignPartitionIDs(t *meta.Mutator, tblInfo *model.TableInfo, pids []int64) ([]int64, error) {
	if len(pids) < len(tblInfo.Partition.Definitions) {
		// To make it compatible with older versions when pids was not given
		// and if there has been any add/reorganize partition increasing the number of partitions
		morePids, err := t.GenGlobalIDs(len(tblInfo.Partition.Definitions) - len(pids))
		if err != nil {
			return nil, errors.Trace(err)
		}
		pids = append(pids, morePids...)
	}
	newDefs := make([]model.PartitionDefinition, 0, len(tblInfo.Partition.Definitions))
	for i, def := range tblInfo.Partition.Definitions {
		newDef := def
		newDef.ID = pids[i]
		newDefs = append(newDefs, newDef)
	}
	tblInfo.Partition.Definitions = newDefs
	return pids, nil
}

type partitionExprProcessor func(expression.BuildContext, *model.TableInfo, ast.ExprNode) error

type partitionExprChecker struct {
	processors []partitionExprProcessor
	ctx        expression.BuildContext
	tbInfo     *model.TableInfo
	err        error

	columns []*model.ColumnInfo
}

func newPartitionExprChecker(ctx expression.BuildContext, tbInfo *model.TableInfo, processor ...partitionExprProcessor) *partitionExprChecker {
	p := &partitionExprChecker{processors: processor, ctx: ctx, tbInfo: tbInfo}
	p.processors = append(p.processors, p.extractColumns)
	return p
}

func (p *partitionExprChecker) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	expr, ok := n.(ast.ExprNode)
	if !ok {
		return n, true
	}
	for _, processor := range p.processors {
		if err := processor(p.ctx, p.tbInfo, expr); err != nil {
			p.err = err
			return n, true
		}
	}

	return n, false
}

func (p *partitionExprChecker) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, p.err == nil
}

func (p *partitionExprChecker) extractColumns(_ expression.BuildContext, _ *model.TableInfo, expr ast.ExprNode) error {
	columnNameExpr, ok := expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil
	}
	colInfo := findColumnByName(columnNameExpr.Name.Name.L, p.tbInfo)
	if colInfo == nil {
		return errors.Trace(dbterror.ErrBadField.GenWithStackByArgs(columnNameExpr.Name.Name.L, "partition function"))
	}

	p.columns = append(p.columns, colInfo)
	return nil
}

func checkPartitionExprAllowed(_ expression.BuildContext, tb *model.TableInfo, e ast.ExprNode) error {
	switch v := e.(type) {
	case *ast.FuncCallExpr:
		if _, ok := expression.AllowedPartitionFuncMap[v.FnName.L]; ok {
			return nil
		}
	case *ast.BinaryOperationExpr:
		if _, ok := expression.AllowedPartition4BinaryOpMap[v.Op]; ok {
			return errors.Trace(checkNoTimestampArgs(tb, v.L, v.R))
		}
	case *ast.UnaryOperationExpr:
		if _, ok := expression.AllowedPartition4UnaryOpMap[v.Op]; ok {
			return errors.Trace(checkNoTimestampArgs(tb, v.V))
		}
	case *ast.ColumnNameExpr, *ast.ParenthesesExpr, *driver.ValueExpr, *ast.MaxValueExpr,
		*ast.DefaultExpr, *ast.TimeUnitExpr:
		return nil
	}
	return errors.Trace(dbterror.ErrPartitionFunctionIsNotAllowed)
}

func checkPartitionExprArgs(_ expression.BuildContext, tblInfo *model.TableInfo, e ast.ExprNode) error {
	expr, ok := e.(*ast.FuncCallExpr)
	if !ok {
		return nil
	}
	argsType, err := collectArgsType(tblInfo, expr.Args...)
	if err != nil {
		return errors.Trace(err)
	}
	switch expr.FnName.L {
	case ast.ToDays, ast.ToSeconds, ast.DayOfMonth, ast.Month, ast.DayOfYear, ast.Quarter, ast.YearWeek,
		ast.Year, ast.Weekday, ast.DayOfWeek, ast.Day:
		return errors.Trace(checkResultOK(hasDateArgs(argsType...)))
	case ast.Hour, ast.Minute, ast.Second, ast.TimeToSec, ast.MicroSecond:
		return errors.Trace(checkResultOK(hasTimeArgs(argsType...)))
	case ast.UnixTimestamp:
		return errors.Trace(checkResultOK(hasTimestampArgs(argsType...)))
	case ast.FromDays:
		return errors.Trace(checkResultOK(hasDateArgs(argsType...) || hasTimeArgs(argsType...)))
	case ast.Extract:
		switch expr.Args[0].(*ast.TimeUnitExpr).Unit {
		case ast.TimeUnitYear, ast.TimeUnitYearMonth, ast.TimeUnitQuarter, ast.TimeUnitMonth, ast.TimeUnitDay:
			return errors.Trace(checkResultOK(hasDateArgs(argsType...)))
		case ast.TimeUnitDayMicrosecond, ast.TimeUnitDayHour, ast.TimeUnitDayMinute, ast.TimeUnitDaySecond:
			return errors.Trace(checkResultOK(hasDatetimeArgs(argsType...)))
		case ast.TimeUnitHour, ast.TimeUnitHourMinute, ast.TimeUnitHourSecond, ast.TimeUnitMinute, ast.TimeUnitMinuteSecond,
			ast.TimeUnitSecond, ast.TimeUnitMicrosecond, ast.TimeUnitHourMicrosecond, ast.TimeUnitMinuteMicrosecond, ast.TimeUnitSecondMicrosecond:
			return errors.Trace(checkResultOK(hasTimeArgs(argsType...)))
		default:
			return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
		}
	case ast.DateDiff:
		return errors.Trace(checkResultOK(slice.AllOf(argsType, func(arg byte) bool {
			return hasDateArgs(arg)
		})))

	case ast.Abs, ast.Ceiling, ast.Floor, ast.Mod:
		has := hasTimestampArgs(argsType...)
		if has {
			return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
		}
	}
	return nil
}

func collectArgsType(tblInfo *model.TableInfo, exprs ...ast.ExprNode) ([]byte, error) {
	ts := make([]byte, 0, len(exprs))
	for _, arg := range exprs {
		col, ok := arg.(*ast.ColumnNameExpr)
		if !ok {
			continue
		}
		columnInfo := findColumnByName(col.Name.Name.L, tblInfo)
		if columnInfo == nil {
			return nil, errors.Trace(dbterror.ErrBadField.GenWithStackByArgs(col.Name.Name.L, "partition function"))
		}
		ts = append(ts, columnInfo.GetType())
	}

	return ts, nil
}

func hasDateArgs(argsType ...byte) bool {
	return slices.ContainsFunc(argsType, func(t byte) bool {
		return t == mysql.TypeDate || t == mysql.TypeDatetime
	})
}

func hasTimeArgs(argsType ...byte) bool {
	return slices.ContainsFunc(argsType, func(t byte) bool {
		return t == mysql.TypeDuration || t == mysql.TypeDatetime
	})
}

func hasTimestampArgs(argsType ...byte) bool {
	return slices.Contains(argsType, mysql.TypeTimestamp)
}

func hasDatetimeArgs(argsType ...byte) bool {
	return slices.ContainsFunc(argsType, func(t byte) bool {
		return t == mysql.TypeDatetime
	})
}

func checkNoTimestampArgs(tbInfo *model.TableInfo, exprs ...ast.ExprNode) error {
	argsType, err := collectArgsType(tbInfo, exprs...)
	if err != nil {
		return err
	}
	if hasTimestampArgs(argsType...) {
		return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
	}
	return nil
}

// hexIfNonPrint checks if printable UTF-8 characters from a single quoted string,
// if so, just returns the string
// else returns a hex string of the binary string (i.e. actual encoding, not unicode code points!)
func hexIfNonPrint(s string) string {
	isPrint := true
	// https://go.dev/blog/strings `for range` of string converts to runes!
	for _, runeVal := range s {
		if !strconv.IsPrint(runeVal) {
			isPrint = false
			break
		}
	}
	if isPrint {
		return s
	}
	// To avoid 'simple' MySQL accepted escape characters, to be showed as hex, just escape them
	// \0 \b \n \r \t \Z, see https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
	isPrint = true
	res := ""
	for _, runeVal := range s {
		switch runeVal {
		case 0: // Null
			res += `\0`
		case 7: // Bell
			res += `\b`
		case '\t': // 9
			res += `\t`
		case '\n': // 10
			res += `\n`
		case '\r': // 13
			res += `\r`
		case 26: // ctrl-z / Substitute
			res += `\Z`
		default:
			if !strconv.IsPrint(runeVal) {
				isPrint = false
				break
			}
			res += string(runeVal)
		}
	}
	if isPrint {
		return res
	}
	// Not possible to create an easy interpreted MySQL string, return as hex string
	// Can be converted to string in MySQL like: CAST(UNHEX('<hex string>') AS CHAR(255))
	return "0x" + hex.EncodeToString([]byte(driver.UnwrapFromSingleQuotes(s)))
}

func writeColumnListToBuffer(partitionInfo *model.PartitionInfo, sqlMode mysql.SQLMode, buf *bytes.Buffer) {
	if partitionInfo.IsEmptyColumns {
		return
	}
	for i, col := range partitionInfo.Columns {
		buf.WriteString(stringutil.Escape(col.O, sqlMode))
		if i < len(partitionInfo.Columns)-1 {
			buf.WriteString(",")
		}
	}
}

// AppendPartitionInfo is used in SHOW CREATE TABLE as well as generation the SQL syntax
// for the PartitionInfo during validation of various DDL commands
func AppendPartitionInfo(partitionInfo *model.PartitionInfo, buf *bytes.Buffer, sqlMode mysql.SQLMode) {
	if partitionInfo == nil {
		return
	}
	// Since MySQL 5.1/5.5 is very old and TiDB aims for 5.7/8.0 compatibility, we will not
	// include the /*!50100 or /*!50500 comments for TiDB.
	// This also solves the issue with comments within comments that would happen for
	// PLACEMENT POLICY options.
	defaultPartitionDefinitions := true
	if partitionInfo.Type == ast.PartitionTypeHash ||
		partitionInfo.Type == ast.PartitionTypeKey {
		for i, def := range partitionInfo.Definitions {
			if def.Name.O != fmt.Sprintf("p%d", i) {
				defaultPartitionDefinitions = false
				break
			}
			if len(def.Comment) > 0 || def.PlacementPolicyRef != nil {
				defaultPartitionDefinitions = false
				break
			}
		}

		if defaultPartitionDefinitions {
			if partitionInfo.Type == ast.PartitionTypeHash {
				fmt.Fprintf(buf, "\nPARTITION BY HASH (%s) PARTITIONS %d", partitionInfo.Expr, partitionInfo.Num)
			} else {
				buf.WriteString("\nPARTITION BY KEY (")
				writeColumnListToBuffer(partitionInfo, sqlMode, buf)
				buf.WriteString(")")
				fmt.Fprintf(buf, " PARTITIONS %d", partitionInfo.Num)
			}
			return
		}
	}
	// this if statement takes care of lists/range/key columns case
	if len(partitionInfo.Columns) > 0 {
		// partitionInfo.Type == model.PartitionTypeRange || partitionInfo.Type == model.PartitionTypeList
		// || partitionInfo.Type == model.PartitionTypeKey
		// Notice that MySQL uses two spaces between LIST and COLUMNS...
		if partitionInfo.Type == ast.PartitionTypeKey {
			fmt.Fprintf(buf, "\nPARTITION BY %s (", partitionInfo.Type.String())
		} else {
			fmt.Fprintf(buf, "\nPARTITION BY %s COLUMNS(", partitionInfo.Type.String())
		}
		writeColumnListToBuffer(partitionInfo, sqlMode, buf)
		buf.WriteString(")\n(")
	} else {
		fmt.Fprintf(buf, "\nPARTITION BY %s (%s)\n(", partitionInfo.Type.String(), partitionInfo.Expr)
	}

	AppendPartitionDefs(partitionInfo, buf, sqlMode)
	buf.WriteString(")")
}

// AppendPartitionDefs generates a list of partition definitions needed for SHOW CREATE TABLE (in executor/show.go)
// as well as needed for generating the ADD PARTITION query for INTERVAL partitioning of ALTER TABLE t LAST PARTITION
// and generating the CREATE TABLE query from CREATE TABLE ... INTERVAL
func AppendPartitionDefs(partitionInfo *model.PartitionInfo, buf *bytes.Buffer, sqlMode mysql.SQLMode) {
	for i, def := range partitionInfo.Definitions {
		if i > 0 {
			fmt.Fprintf(buf, ",\n ")
		}
		fmt.Fprintf(buf, "PARTITION %s", stringutil.Escape(def.Name.O, sqlMode))
		// PartitionTypeHash and PartitionTypeKey do not have any VALUES definition
		if partitionInfo.Type == ast.PartitionTypeRange {
			lessThans := make([]string, len(def.LessThan))
			for idx, v := range def.LessThan {
				lessThans[idx] = hexIfNonPrint(v)
			}
			fmt.Fprintf(buf, " VALUES LESS THAN (%s)", strings.Join(lessThans, ","))
		} else if partitionInfo.Type == ast.PartitionTypeList {
			if len(def.InValues) == 0 {
				fmt.Fprintf(buf, " DEFAULT")
			} else if len(def.InValues) == 1 &&
				len(def.InValues[0]) == 1 &&
				strings.EqualFold(def.InValues[0][0], "DEFAULT") {
				fmt.Fprintf(buf, " DEFAULT")
			} else {
				values := bytes.NewBuffer(nil)
				for j, inValues := range def.InValues {
					if j > 0 {
						values.WriteString(",")
					}
					if len(inValues) > 1 {
						values.WriteString("(")
						tmpVals := make([]string, len(inValues))
						for idx, v := range inValues {
							tmpVals[idx] = hexIfNonPrint(v)
						}
						values.WriteString(strings.Join(tmpVals, ","))
						values.WriteString(")")
					} else if len(inValues) == 1 {
						values.WriteString(hexIfNonPrint(inValues[0]))
					}
				}
				fmt.Fprintf(buf, " VALUES IN (%s)", values.String())
			}
		}
		if len(def.Comment) > 0 {
			fmt.Fprintf(buf, " COMMENT '%s'", format.OutputFormat(def.Comment))
		}
		if def.PlacementPolicyRef != nil {
			// add placement ref info here
			fmt.Fprintf(buf, " /*T![placement] PLACEMENT POLICY=%s */", stringutil.Escape(def.PlacementPolicyRef.Name.O, sqlMode))
		}
	}
}

func generatePartValuesWithTp(partVal types.Datum, tp types.FieldType) (string, error) {
	if partVal.Kind() == types.KindNull {
		return "NULL", nil
	}

	s, err := partVal.ToString()
	if err != nil {
		return "", err
	}

	switch tp.EvalType() {
	case types.ETInt:
		return s, nil
	case types.ETString:
		// The `partVal` can be an invalid utf8 string if it's converted to BINARY, then the content will be lost after
		// marshaling and storing in the schema. In this case, we use a hex literal to work around this issue.
		if tp.GetCharset() == charset.CharsetBin && len(s) != 0 {
			return fmt.Sprintf("_binary 0x%x", s), nil
		}
		return driver.WrapInSingleQuotes(s), nil
	case types.ETDatetime, types.ETDuration:
		return driver.WrapInSingleQuotes(s), nil
	}

	return "", dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
}

func checkPartitionDefinitionConstraints(ctx expression.BuildContext, tbInfo *model.TableInfo) error {
	var err error
	if err = checkPartitionNameUnique(tbInfo.Partition); err != nil {
		return errors.Trace(err)
	}
	if err = checkAddPartitionTooManyPartitions(uint64(len(tbInfo.Partition.Definitions))); err != nil {
		return err
	}
	if err = checkAddPartitionOnTemporaryMode(tbInfo); err != nil {
		return err
	}
	if err = checkPartitionColumnsUnique(tbInfo); err != nil {
		return err
	}

	switch tbInfo.Partition.Type {
	case ast.PartitionTypeRange:
		failpoint.Inject("CheckPartitionByRangeErr", func() {
			panic("mockCheckPartitionByRangeErr")
		})
		err = checkPartitionByRange(ctx, tbInfo)
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		err = checkPartitionByHash(tbInfo)
	case ast.PartitionTypeList:
		err = checkPartitionByList(ctx, tbInfo)
	}
	return errors.Trace(err)
}

func checkPartitionByHash(tbInfo *model.TableInfo) error {
	return checkNoHashPartitions(tbInfo.Partition.Num)
}

// checkPartitionByRange checks validity of a "BY RANGE" partition.
func checkPartitionByRange(ctx expression.BuildContext, tbInfo *model.TableInfo) error {
	pi := tbInfo.Partition

	if len(pi.Columns) == 0 {
		return checkRangePartitionValue(ctx, tbInfo)
	}

	return checkRangeColumnsPartitionValue(ctx, tbInfo)
}

func checkRangeColumnsPartitionValue(ctx expression.BuildContext, tbInfo *model.TableInfo) error {
	// Range columns partition key supports multiple data types with integer、datetime、string.
	pi := tbInfo.Partition
	defs := pi.Definitions
	if len(defs) < 1 {
		return ast.ErrPartitionsMustBeDefined.GenWithStackByArgs("RANGE")
	}

	curr := &defs[0]
	if len(curr.LessThan) != len(pi.Columns) {
		return errors.Trace(ast.ErrPartitionColumnList)
	}
	var prev *model.PartitionDefinition
	for i := 1; i < len(defs); i++ {
		prev, curr = curr, &defs[i]
		succ, err := checkTwoRangeColumns(ctx, curr, prev, pi, tbInfo)
		if err != nil {
			return err
		}
		if !succ {
			return errors.Trace(dbterror.ErrRangeNotIncreasing)
		}
	}
	return nil
}

func checkTwoRangeColumns(ctx expression.BuildContext, curr, prev *model.PartitionDefinition, pi *model.PartitionInfo, tbInfo *model.TableInfo) (bool, error) {
	if len(curr.LessThan) != len(pi.Columns) {
		return false, errors.Trace(ast.ErrPartitionColumnList)
	}
	for i := range pi.Columns {
		// Special handling for MAXVALUE.
		if strings.EqualFold(curr.LessThan[i], partitionMaxValue) && !strings.EqualFold(prev.LessThan[i], partitionMaxValue) {
			// If current is maxvalue, it certainly >= previous.
			return true, nil
		}
		if strings.EqualFold(prev.LessThan[i], partitionMaxValue) {
			// Current is not maxvalue, and previous is maxvalue.
			return false, nil
		}

		// The tuples of column values used to define the partitions are strictly increasing:
		// PARTITION p0 VALUES LESS THAN (5,10,'ggg')
		// PARTITION p1 VALUES LESS THAN (10,20,'mmm')
		// PARTITION p2 VALUES LESS THAN (15,30,'sss')
		colInfo := findColumnByName(pi.Columns[i].L, tbInfo)
		cmp, err := parseAndEvalBoolExpr(ctx, curr.LessThan[i], prev.LessThan[i], colInfo, tbInfo)
		if err != nil {
			return false, err
		}

		if cmp > 0 {
			return true, nil
		}

		if cmp < 0 {
			return false, nil
		}
	}
	return false, nil
}

// equal, return 0
// greater, return 1
// less, return -1
func parseAndEvalBoolExpr(ctx expression.BuildContext, l, r string, colInfo *model.ColumnInfo, tbInfo *model.TableInfo) (int64, error) {
	lexpr, err := expression.ParseSimpleExpr(ctx, l, expression.WithTableInfo("", tbInfo), expression.WithCastExprTo(&colInfo.FieldType))
	if err != nil {
		return 0, err
	}
	rexpr, err := expression.ParseSimpleExpr(ctx, r, expression.WithTableInfo("", tbInfo), expression.WithCastExprTo(&colInfo.FieldType))
	if err != nil {
		return 0, err
	}

	e, err := expression.NewFunctionBase(ctx, ast.EQ, field_types.NewFieldType(mysql.TypeLonglong), lexpr, rexpr)
	if err != nil {
		return 0, err
	}
	e.SetCharsetAndCollation(colInfo.GetCharset(), colInfo.GetCollate())
	res, _, err1 := e.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
	if err1 != nil {
		return 0, err1
	}
	if res == 1 {
		return 0, nil
	}

	e, err = expression.NewFunctionBase(ctx, ast.GT, field_types.NewFieldType(mysql.TypeLonglong), lexpr, rexpr)
	if err != nil {
		return 0, err
	}
	e.SetCharsetAndCollation(colInfo.GetCharset(), colInfo.GetCollate())
	res, _, err1 = e.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
	if err1 != nil {
		return 0, err1
	}
	if res > 0 {
		return 1, nil
	}
	return -1, nil
}

// checkPartitionByList checks validity of a "BY LIST" partition.
func checkPartitionByList(ctx expression.BuildContext, tbInfo *model.TableInfo) error {
	return checkListPartitionValue(ctx, tbInfo)
}
