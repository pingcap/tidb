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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/label"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/slice"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const (
	partitionMaxValue = "MAXVALUE"
)

func checkAddPartition(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.PartitionInfo, []model.PartitionDefinition, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	partInfo := &model.PartitionInfo{}
	err = job.DecodeArgs(&partInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, errors.Trace(err)
	}
	if len(tblInfo.Partition.AddingDefinitions) > 0 {
		return tblInfo, partInfo, tblInfo.Partition.AddingDefinitions, nil
	}
	return tblInfo, partInfo, []model.PartitionDefinition{}, nil
}

func (w *worker) onAddTablePartition(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	// Handle the rolling back job
	if job.IsRollingback() {
		ver, err := w.onDropTablePartition(d, t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// notice: addingDefinitions is empty when job is in state model.StateNone
	tblInfo, partInfo, addingDefinitions, err := checkAddPartition(t, job)
	if err != nil {
		return ver, err
	}

	// In order to skip maintaining the state check in partitionDefinition, TiDB use addingDefinition instead of state field.
	// So here using `job.SchemaState` to judge what the stage of this job is.
	switch job.SchemaState {
	case model.StateNone:
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
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// modify placement settings
		for _, def := range tblInfo.Partition.AddingDefinitions {
			if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(t, job, def.PlacementPolicyRef); err != nil {
				return ver, errors.Trace(err)
			}
		}

		if tblInfo.TiFlashReplica != nil {
			// Must set placement rule, and make sure it succeeds.
			if err := infosync.ConfigureTiFlashPDForPartitions(true, &tblInfo.Partition.AddingDefinitions, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels, tblInfo.ID); err != nil {
				logutil.BgLogger().Error("ConfigureTiFlashPDForPartitions fails", zap.Error(err))
				return ver, errors.Trace(err)
			}
		}

		bundles, err := alterTablePartitionBundles(t, tblInfo, tblInfo.Partition.AddingDefinitions)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		if err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
		}

		ids := getIDs([]*model.TableInfo{tblInfo})
		for _, p := range tblInfo.Partition.AddingDefinitions {
			ids = append(ids, p.ID)
		}
		if err := alterTableLabelRule(job.SchemaName, tblInfo, ids); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		// none -> replica only
		job.SchemaState = model.StateReplicaOnly
	case model.StateReplicaOnly:
		// replica only -> public
		// Here need do some tiflash replica complement check.
		// TODO: If a table is with no TiFlashReplica or it is not available, the replica-only state can be eliminated.
		if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
			// For available state, the new added partition should wait it's replica to
			// be finished. Otherwise the query to this partition will be blocked.
			needRetry, err := checkPartitionReplica(tblInfo.TiFlashReplica.Count, addingDefinitions, d)
			if err != nil {
				ver, err = convertAddTablePartitionJob2RollbackJob(d, t, job, err, tblInfo)
				return ver, err
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
			}
		}
		// For normal and replica finished table, move the `addingDefinitions` into `Definitions`.
		updatePartitionInfo(tblInfo)

		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionAddTablePartition, TableInfo: tblInfo, PartInfo: partInfo})
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}

	return ver, errors.Trace(err)
}

func alterTableLabelRule(schemaName string, meta *model.TableInfo, ids []int64) error {
	tableRuleID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, schemaName, meta.Name.L)
	oldRule, err := infosync.GetLabelRules(context.TODO(), []string{tableRuleID})
	if err != nil {
		return errors.Trace(err)
	}
	if len(oldRule) == 0 {
		return nil
	}

	r, ok := oldRule[tableRuleID]
	if ok {
		rule := r.Reset(schemaName, meta.Name.L, "", ids...)
		err = infosync.PutLabelRule(context.TODO(), rule)
		if err != nil {
			return errors.Wrapf(err, "failed to notify PD label rule")
		}
	}
	return nil
}

func alterTablePartitionBundles(t *meta.Meta, tblInfo *model.TableInfo, addingDefinitions []model.PartitionDefinition) ([]*placement.Bundle, error) {
	var bundles []*placement.Bundle

	// tblInfo do not include added partitions, so we should add them first
	tblInfo = tblInfo.Clone()
	p := *tblInfo.Partition
	p.Definitions = append([]model.PartitionDefinition{}, p.Definitions...)
	p.Definitions = append(tblInfo.Partition.Definitions, addingDefinitions...)
	tblInfo.Partition = &p

	if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Count > 0 && tableHasPlacementSettings(tblInfo) {
		return nil, errors.Trace(dbterror.ErrIncompatibleTiFlashAndPlacement)
	}

	// bundle for table should be recomputed because it includes some default configs for partitions
	tblBundle, err := placement.NewTableBundle(t, tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if tblBundle != nil {
		bundles = append(bundles, tblBundle)
	}

	partitionBundles, err := placement.NewPartitionListBundles(t, addingDefinitions)
	if err != nil {
		return nil, errors.Trace(err)
	}

	bundles = append(bundles, partitionBundles...)
	return bundles, nil
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

// rollbackAddingPartitionInfo remove the `addingDefinitions` in the tableInfo.
func rollbackAddingPartitionInfo(tblInfo *model.TableInfo) ([]int64, []string, []*placement.Bundle) {
	physicalTableIDs := make([]int64, 0, len(tblInfo.Partition.AddingDefinitions))
	partNames := make([]string, 0, len(tblInfo.Partition.AddingDefinitions))
	rollbackBundles := make([]*placement.Bundle, 0, len(tblInfo.Partition.AddingDefinitions))
	for _, one := range tblInfo.Partition.AddingDefinitions {
		physicalTableIDs = append(physicalTableIDs, one.ID)
		partNames = append(partNames, one.Name.L)
		if one.PlacementPolicyRef != nil {
			rollbackBundles = append(rollbackBundles, placement.NewBundle(one.ID))
		}
	}
	tblInfo.Partition.AddingDefinitions = nil
	return physicalTableIDs, partNames, rollbackBundles
}

// checkAddPartitionValue values less than value must be strictly increasing for each partition.
func checkAddPartitionValue(meta *model.TableInfo, part *model.PartitionInfo) error {
	if meta.Partition.Type == model.PartitionTypeRange && len(meta.Partition.Columns) == 0 {
		newDefs, oldDefs := part.Definitions, meta.Partition.Definitions
		rangeValue := oldDefs[len(oldDefs)-1].LessThan[0]
		if strings.EqualFold(rangeValue, "MAXVALUE") {
			return errors.Trace(dbterror.ErrPartitionMaxvalue)
		}

		currentRangeValue, err := strconv.Atoi(rangeValue)
		if err != nil {
			return errors.Trace(err)
		}

		for i := 0; i < len(newDefs); i++ {
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
	return nil
}

func checkPartitionReplica(replicaCount uint64, addingDefinitions []model.PartitionDefinition, d *ddlCtx) (needWait bool, err error) {
	failpoint.Inject("mockWaitTiFlashReplica", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(true, nil)
		}
	})

	ctx := context.Background()
	pdCli := d.store.(tikv.Storage).GetRegionCache().PDClient()
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
	for _, pd := range addingDefinitions {
		startKey, endKey := tablecodec.GetTableHandleKeyRange(pd.ID)
		regions, err := pdCli.ScanRegions(ctx, startKey, endKey, -1)
		if err != nil {
			return needWait, errors.Trace(err)
		}
		// For every region in the partition, if it has some corresponding peers and
		// no pending peers, that means the replication has completed.
		for _, region := range regions {
			regionState, err := pdCli.GetRegionByID(ctx, region.Meta.Id)
			if err != nil {
				return needWait, errors.Trace(err)
			}
			tiflashPeerAtLeastOne := checkTiFlashPeerStoreAtLeastOne(stores, regionState.Meta.Peers)
			failpoint.Inject("ForceTiflashNotAvailable", func(v failpoint.Value) {
				tiflashPeerAtLeastOne = v.(bool)
			})
			// It's unnecessary to wait all tiflash peer to be replicated.
			// Here only make sure that tiflash peer count > 0 (at least one).
			if tiflashPeerAtLeastOne {
				continue
			}
			needWait = true
			logutil.BgLogger().Info("[ddl] partition replicas check failed in replica-only DDL state", zap.Int64("pID", pd.ID), zap.Uint64("wait region ID", region.Meta.Id), zap.Bool("tiflash peer at least one", tiflashPeerAtLeastOne), zap.Time("check time", time.Now()))
			return needWait, nil
		}
	}
	logutil.BgLogger().Info("[ddl] partition replicas check ok in replica-only DDL state")
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

// buildTablePartitionInfo builds partition info and checks for some errors.
func buildTablePartitionInfo(ctx sessionctx.Context, s *ast.PartitionOptions, tbInfo *model.TableInfo) error {
	if s == nil {
		return nil
	}

	if strings.EqualFold(ctx.GetSessionVars().EnableTablePartition, "OFF") {
		ctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrTablePartitionDisabled)
		return nil
	}

	var enable bool
	switch s.Tp {
	case model.PartitionTypeRange:
		// When tidb_enable_table_partition is 'on' or 'auto'.
		if s.Sub == nil {
			// Partition by range expression is enabled by default.
			if s.ColumnNames == nil {
				enable = true
			}
			// Partition by range columns and just one column.
			if len(s.ColumnNames) == 1 {
				enable = true
			}
		}
	case model.PartitionTypeHash:
		// Partition by hash is enabled by default.
		// Note that linear hash is not enabled.
		if !s.Linear && s.Sub == nil {
			enable = true
		}
	case model.PartitionTypeList:
		// Partition by list is enabled only when tidb_enable_list_partition is 'ON'.
		enable = ctx.GetSessionVars().EnableListTablePartition
	}

	if !enable {
		ctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrUnsupportedCreatePartition.GenWithStack(fmt.Sprintf("Unsupported partition type %v, treat as normal table", s.Tp)))
		return nil
	}

	pi := &model.PartitionInfo{
		Type:   s.Tp,
		Enable: enable,
		Num:    s.Num,
	}
	tbInfo.Partition = pi
	if s.Expr != nil {
		if err := checkPartitionFuncValid(ctx, tbInfo, s.Expr); err != nil {
			return errors.Trace(err)
		}
		buf := new(bytes.Buffer)
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreBracketAroundBinaryOperation, buf)
		if err := s.Expr.Restore(restoreCtx); err != nil {
			return err
		}
		pi.Expr = buf.String()
	} else if s.ColumnNames != nil {
		pi.Columns = make([]model.CIStr, 0, len(s.ColumnNames))
		for _, cn := range s.ColumnNames {
			pi.Columns = append(pi.Columns, cn.Name)
		}
		if err := checkColumnsPartitionType(tbInfo); err != nil {
			return err
		}
	}

	defs, err := buildPartitionDefinitionsInfo(ctx, s.Definitions, tbInfo)
	if err != nil {
		return errors.Trace(err)
	}

	tbInfo.Partition.Definitions = defs
	return nil
}

// buildPartitionDefinitionsInfo build partition definitions info without assign partition id. tbInfo will be constant
func buildPartitionDefinitionsInfo(ctx sessionctx.Context, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) (partitions []model.PartitionDefinition, err error) {
	switch tbInfo.Partition.Type {
	case model.PartitionTypeRange:
		partitions, err = buildRangePartitionDefinitions(ctx, defs, tbInfo)
	case model.PartitionTypeHash:
		partitions, err = buildHashPartitionDefinitions(ctx, defs, tbInfo)
	case model.PartitionTypeList:
		partitions, err = buildListPartitionDefinitions(ctx, defs, tbInfo)
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
		switch opt.Tp {
		case ast.TableOptionPlacementPolicy:
			partition.PlacementPolicyRef = &model.PolicyRefInfo{
				Name: model.NewCIStr(opt.StrValue),
			}
		}
	}

	return nil
}

func buildHashPartitionDefinitions(_ sessionctx.Context, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) ([]model.PartitionDefinition, error) {
	if err := checkAddPartitionTooManyPartitions(tbInfo.Partition.Num); err != nil {
		return nil, err
	}

	definitions := make([]model.PartitionDefinition, tbInfo.Partition.Num)
	for i := 0; i < len(definitions); i++ {
		if len(defs) == 0 {
			definitions[i].Name = model.NewCIStr(fmt.Sprintf("p%v", i))
		} else {
			def := defs[i]
			definitions[i].Name = def.Name
			definitions[i].Comment, _ = def.Comment()
			if err := setPartitionPlacementFromOptions(&definitions[i], def.Options); err != nil {
				return nil, err
			}
		}
	}
	return definitions, nil
}

func buildListPartitionDefinitions(ctx sessionctx.Context, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) ([]model.PartitionDefinition, error) {
	definitions := make([]model.PartitionDefinition, 0, len(defs))
	exprChecker := newPartitionExprChecker(ctx, nil, checkPartitionExprAllowed)
	for _, def := range defs {
		if err := def.Clause.Validate(model.PartitionTypeList, len(tbInfo.Partition.Columns)); err != nil {
			return nil, err
		}
		clause := def.Clause.(*ast.PartitionDefinitionClauseIn)
		if len(tbInfo.Partition.Columns) > 0 {
			for _, vs := range clause.Values {
				if err := checkColumnsTypeAndValuesMatch(ctx, tbInfo, vs); err != nil {
					return nil, err
				}
			}
		} else {
			for _, vs := range clause.Values {
				if err := checkPartitionValuesIsInt(ctx, def, vs, tbInfo); err != nil {
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
		for _, vs := range clause.Values {
			inValue := make([]string, 0, len(vs))
			for i := range vs {
				vs[i].Accept(exprChecker)
				if exprChecker.err != nil {
					return nil, exprChecker.err
				}
				buf.Reset()
				vs[i].Format(buf)
				inValue = append(inValue, buf.String())
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
			colTypes = append(colTypes, findColumnByName(col.L, tbInfo).FieldType)
		}

		return colTypes
	}

	return nil
}

func buildRangePartitionDefinitions(ctx sessionctx.Context, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) ([]model.PartitionDefinition, error) {
	definitions := make([]model.PartitionDefinition, 0, len(defs))
	exprChecker := newPartitionExprChecker(ctx, nil, checkPartitionExprAllowed)
	for _, def := range defs {
		if err := def.Clause.Validate(model.PartitionTypeRange, len(tbInfo.Partition.Columns)); err != nil {
			return nil, err
		}
		clause := def.Clause.(*ast.PartitionDefinitionClauseLessThan)
		if len(tbInfo.Partition.Columns) > 0 {
			if err := checkColumnsTypeAndValuesMatch(ctx, tbInfo, clause.Exprs); err != nil {
				return nil, err
			}
		} else {
			if err := checkPartitionValuesIsInt(ctx, def, clause.Exprs, tbInfo); err != nil {
				return nil, err
			}
		}
		comment, _ := def.Comment()
		comment, err := validateCommentLength(ctx.GetSessionVars(), def.Name.L, &comment, dbterror.ErrTooLongTablePartitionComment)
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
		for _, expr := range clause.Exprs {
			expr.Accept(exprChecker)
			if exprChecker.err != nil {
				return nil, exprChecker.err
			}
			expr.Format(buf)
			piDef.LessThan = append(piDef.LessThan, buf.String())
			buf.Reset()
		}
		definitions = append(definitions, piDef)
	}
	return definitions, nil
}

func checkPartitionValuesIsInt(ctx sessionctx.Context, def *ast.PartitionDefinition, exprs []ast.ExprNode, tbInfo *model.TableInfo) error {
	tp := types.NewFieldType(mysql.TypeLonglong)
	if isColUnsigned(tbInfo.Columns, tbInfo.Partition) {
		tp.AddFlag(mysql.UnsignedFlag)
	}
	for _, exp := range exprs {
		if _, ok := exp.(*ast.MaxValueExpr); ok {
			continue
		}
		val, err := expression.EvalAstExpr(ctx, exp)
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
			return dbterror.ErrValuesIsNotIntType.GenWithStackByArgs(def.Name)
		}

		_, err = val.ConvertTo(ctx.GetSessionVars().StmtCtx, tp)
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
	if newTableInfo.Partition.Type == model.PartitionTypeHash {
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
func checkPartitionFuncValid(ctx sessionctx.Context, tblInfo *model.TableInfo, expr ast.ExprNode) error {
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
func checkPartitionFuncType(ctx sessionctx.Context, expr ast.ExprNode, tblInfo *model.TableInfo) error {
	if expr == nil {
		return nil
	}

	e, err := expression.RewriteSimpleExprWithTableInfo(ctx, tblInfo, expr)
	if err != nil {
		return errors.Trace(err)
	}
	if e.GetType().EvalType() == types.ETInt {
		return nil
	}

	if col, ok := expr.(*ast.ColumnNameExpr); ok {
		return errors.Trace(dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(col.Name.Name.L))
	}

	return errors.Trace(dbterror.ErrPartitionFuncNotAllowed.GenWithStackByArgs("PARTITION"))
}

// checkRangePartitionValue checks whether `less than value` is strictly increasing for each partition.
// Side effect: it may simplify the partition range definition from a constant expression to an integer.
func checkRangePartitionValue(ctx sessionctx.Context, tblInfo *model.TableInfo) error {
	pi := tblInfo.Partition
	defs := pi.Definitions
	if len(defs) == 0 {
		return nil
	}

	cols := tblInfo.Columns
	if strings.EqualFold(defs[len(defs)-1].LessThan[0], partitionMaxValue) {
		defs = defs[:len(defs)-1]
	}
	isUnsigned := isColUnsigned(cols, pi)
	var prevRangeValue interface{}
	for i := 0; i < len(defs); i++ {
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

func checkListPartitionValue(ctx sessionctx.Context, tblInfo *model.TableInfo) error {
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

func formatListPartitionValue(ctx sessionctx.Context, tblInfo *model.TableInfo) ([]string, error) {
	defs := tblInfo.Partition.Definitions
	pi := tblInfo.Partition
	var colTps []*types.FieldType
	cols := make([]*model.ColumnInfo, 0, len(pi.Columns))
	if len(pi.Columns) == 0 {
		tp := types.NewFieldType(mysql.TypeLonglong)
		if isColUnsigned(tblInfo.Columns, tblInfo.Partition) {
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

	exprStrs := make([]string, 0)
	inValueStrs := make([]string, 0, mathutil.Max(len(pi.Columns), 1))
	for i := range defs {
		for j, vs := range defs[i].InValues {
			inValueStrs = inValueStrs[:0]
			for k, v := range vs {
				expr, err := expression.ParseSimpleExprCastWithTableInfo(ctx, v, &model.TableInfo{}, colTps[k])
				if err != nil {
					return nil, errors.Trace(err)
				}
				eval, err := expr.Eval(chunk.Row{})
				if err != nil {
					return nil, errors.Trace(err)
				}
				s, err := eval.ToString()
				if err != nil {
					return nil, errors.Trace(err)
				}
				if !eval.IsNull() && colTps[k].EvalType() == types.ETInt {
					defs[i].InValues[j][k] = s
				}
				if colTps[k].EvalType() == types.ETString {
					s = string(hack.String(collate.GetCollator(cols[k].GetCollate()).Key(s)))
				}
				s = strings.ReplaceAll(s, ",", `\,`)
				inValueStrs = append(inValueStrs, s)
			}
			exprStrs = append(exprStrs, strings.Join(inValueStrs, ","))
		}
	}
	return exprStrs, nil
}

// getRangeValue gets an integer from the range value string.
// The returned boolean value indicates whether the input string is a constant expression.
func getRangeValue(ctx sessionctx.Context, str string, unsigned bool) (interface{}, bool, error) {
	// Unsigned bigint was converted to uint64 handle.
	if unsigned {
		if value, err := strconv.ParseUint(str, 10, 64); err == nil {
			return value, false, nil
		}

		e, err1 := expression.ParseSimpleExprWithTableInfo(ctx, str, &model.TableInfo{})
		if err1 != nil {
			return 0, false, err1
		}
		res, isNull, err2 := e.EvalInt(ctx, chunk.Row{})
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
		e, err1 := expression.ParseSimpleExprWithTableInfo(ctx, str, &model.TableInfo{})
		if err1 != nil {
			return 0, false, err1
		}
		res, isNull, err2 := e.EvalInt(ctx, chunk.Row{})
		if err2 == nil && !isNull {
			return res, true, nil
		}
	}
	return 0, false, dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(str)
}

// checkDropTablePartition checks if the partition exists and does not allow deleting the last existing partition in the table.
func checkDropTablePartition(meta *model.TableInfo, partLowerNames []string) error {
	pi := meta.Partition
	if pi.Type != model.PartitionTypeRange && pi.Type != model.PartitionTypeList {
		return dbterror.ErrOnlyOnRangeListPartition.GenWithStackByArgs("DROP")
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

// updateDroppingPartitionInfo move dropping partitions to DroppingDefinitions, and return partitionIDs
func updateDroppingPartitionInfo(tblInfo *model.TableInfo, partLowerNames []string) []int64 {
	oldDefs := tblInfo.Partition.Definitions
	newDefs := make([]model.PartitionDefinition, 0, len(oldDefs)-len(partLowerNames))
	droppingDefs := make([]model.PartitionDefinition, 0, len(partLowerNames))
	pids := make([]int64, 0, len(partLowerNames))

	// consider using a map to probe partLowerNames if too many partLowerNames
	for i := range oldDefs {
		found := false
		for _, partName := range partLowerNames {
			if oldDefs[i].Name.L == partName {
				found = true
				break
			}
		}
		if found {
			pids = append(pids, oldDefs[i].ID)
			droppingDefs = append(droppingDefs, oldDefs[i])
		} else {
			newDefs = append(newDefs, oldDefs[i])
		}
	}

	tblInfo.Partition.Definitions = newDefs
	tblInfo.Partition.DroppingDefinitions = droppingDefs
	return pids
}

func getPartitionDef(tblInfo *model.TableInfo, partName string) (index int, def *model.PartitionDefinition, _ error) {
	defs := tblInfo.Partition.Definitions
	for i := 0; i < len(defs); i++ {
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

func dropLabelRules(d *ddlCtx, schemaName, tableName string, partNames []string) error {
	deleteRules := make([]string, 0, len(partNames))
	for _, partName := range partNames {
		deleteRules = append(deleteRules, fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, schemaName, tableName, partName))
	}
	// delete batch rules
	patch := label.NewRulePatch([]*label.Rule{}, deleteRules)
	return infosync.UpdateLabelRules(context.TODO(), patch)
}

// onDropTablePartition deletes old partition meta.
func (w *worker) onDropTablePartition(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var partNames []string
	if err := job.DecodeArgs(&partNames); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if job.Type == model.ActionAddTablePartition {
		// It is rollbacked from adding table partition, just remove addingDefinitions from tableInfo.
		physicalTableIDs, pNames, rollbackBundles := rollbackAddingPartitionInfo(tblInfo)
		err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), rollbackBundles)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
		}
		err = dropLabelRules(d, job.SchemaName, tblInfo.Name.L, pNames)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the label rules")
		}

		if err := alterTableLabelRule(job.SchemaName, tblInfo, getIDs([]*model.TableInfo{tblInfo})); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		job.Args = []interface{}{physicalTableIDs}
		return ver, nil
	}

	var physicalTableIDs []int64
	// In order to skip maintaining the state check in partitionDefinition, TiDB use droppingDefinition instead of state field.
	// So here using `job.SchemaState` to judge what the stage of this job is.
	originalState := job.SchemaState
	switch job.SchemaState {
	case model.StatePublic:
		// If an error occurs, it returns that it cannot delete all partitions or that the partition doesn't exist.
		err = checkDropTablePartition(tblInfo, partNames)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		physicalTableIDs = updateDroppingPartitionInfo(tblInfo, partNames)
		err = dropLabelRules(d, job.SchemaName, tblInfo.Name.L, partNames)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the label rules")
		}

		if err := alterTableLabelRule(job.SchemaName, tblInfo, getIDs([]*model.TableInfo{tblInfo})); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		job.SchemaState = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != job.SchemaState)
	case model.StateDeleteOnly:
		// This state is not a real 'DeleteOnly' state, because tidb does not maintaining the state check in partitionDefinition.
		// Insert this state to confirm all servers can not see the old partitions when reorg is running,
		// so that no new data will be inserted into old partitions when reorganizing.
		job.SchemaState = model.StateDeleteReorganization
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != job.SchemaState)
	case model.StateDeleteReorganization:
		oldTblInfo := getTableInfoWithDroppingPartitions(tblInfo)
		physicalTableIDs = getPartitionIDsFromDefinitions(tblInfo.Partition.DroppingDefinitions)
		tbl, err := getTable(d.store, job.SchemaID, oldTblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// If table has global indexes, we need reorg to clean up them.
		if pt, ok := tbl.(table.PartitionedTable); ok && hasGlobalIndex(tblInfo) {
			// Build elements for compatible with modify column type. elements will not be used when reorganizing.
			elements := make([]*meta.Element, 0, len(tblInfo.Indices))
			for _, idxInfo := range tblInfo.Indices {
				if idxInfo.Global {
					elements = append(elements, &meta.Element{ID: idxInfo.ID, TypeKey: meta.IndexElementKey})
				}
			}
			reorgInfo, err := getReorgInfoFromPartitions(w.JobContext, d, t, job, tbl, physicalTableIDs, elements)

			if err != nil || reorgInfo.first {
				// If we run reorg firstly, we should update the job snapshot version
				// and then run the reorg next time.
				return ver, errors.Trace(err)
			}
			err = w.runReorgJob(t, reorgInfo, tbl.Meta(), d.lease, func() (dropIndexErr error) {
				defer tidbutil.Recover(metrics.LabelDDL, "onDropTablePartition",
					func() {
						dropIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("drop partition panic")
					}, false)
				return w.cleanupGlobalIndexes(pt, physicalTableIDs, reorgInfo)
			})
			if err != nil {
				if dbterror.ErrWaitReorgTimeout.Equal(err) {
					// if timeout, we should return, check for the owner and re-wait job done.
					return ver, nil
				}
				// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
				w.reorgCtx.cleanNotifyReorgCancel()
				return ver, errors.Trace(err)
			}
			// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
			w.reorgCtx.cleanNotifyReorgCancel()
		}
		tblInfo.Partition.DroppingDefinitions = nil
		// used by ApplyDiff in updateSchemaVersion
		job.CtxVars = []interface{}{physicalTableIDs}
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateNone
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionDropTablePartition, TableInfo: tblInfo, PartInfo: &model.PartitionInfo{Definitions: tblInfo.Partition.Definitions}})
		// A background job will be created to delete old partition data.
		job.Args = []interface{}{physicalTableIDs}
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}
	return ver, errors.Trace(err)
}

// onTruncateTablePartition truncates old partition meta.
func onTruncateTablePartition(d *ddlCtx, t *meta.Meta, job *model.Job) (int64, error) {
	var ver int64
	var oldIDs []int64
	if err := job.DecodeArgs(&oldIDs); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return ver, errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	newPartitions := make([]model.PartitionDefinition, 0, len(oldIDs))
	for _, oldID := range oldIDs {
		for i := 0; i < len(pi.Definitions); i++ {
			def := &pi.Definitions[i]
			if def.ID == oldID {
				pid, err1 := t.GenGlobalID()
				if err1 != nil {
					return ver, errors.Trace(err1)
				}
				def.ID = pid
				// Shallow copy only use the def.ID in event handle.
				newPartitions = append(newPartitions, *def)
				break
			}
		}
	}
	if len(newPartitions) == 0 {
		job.State = model.JobStateCancelled
		return ver, table.ErrUnknownPartition.GenWithStackByArgs(fmt.Sprintf("pid:%v", oldIDs), tblInfo.Name.O)
	}

	// Clear the tiflash replica available status.
	if tblInfo.TiFlashReplica != nil {
		e := infosync.ConfigureTiFlashPDForPartitions(true, &newPartitions, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels, tblInfo.ID)
		failpoint.Inject("FailTiFlashTruncatePartition", func() {
			e = errors.New("enforced error")
		})
		if e != nil {
			logutil.BgLogger().Error("ConfigureTiFlashPDForPartitions fails", zap.Error(e))
			job.State = model.JobStateCancelled
			return ver, e
		}
		tblInfo.TiFlashReplica.Available = false
		// Set partition replica become unavailable.
		for _, oldID := range oldIDs {
			for i, id := range tblInfo.TiFlashReplica.AvailablePartitionIDs {
				if id == oldID {
					newIDs := tblInfo.TiFlashReplica.AvailablePartitionIDs[:i]
					newIDs = append(newIDs, tblInfo.TiFlashReplica.AvailablePartitionIDs[i+1:]...)
					tblInfo.TiFlashReplica.AvailablePartitionIDs = newIDs
					break
				}
			}
		}
	}

	bundles, err := placement.NewPartitionListBundles(t, newPartitions)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
	}

	tableID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, job.SchemaName, tblInfo.Name.L)
	oldPartRules := make([]string, 0, len(oldIDs))
	for _, newPartition := range newPartitions {
		oldPartRuleID := fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, job.SchemaName, tblInfo.Name.L, newPartition.Name.L)
		oldPartRules = append(oldPartRules, oldPartRuleID)
	}

	rules, err := infosync.GetLabelRules(context.TODO(), append(oldPartRules, tableID))
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to get label rules from PD")
	}

	newPartIDs := getPartitionIDs(tblInfo)
	newRules := make([]*label.Rule, 0, len(oldIDs)+1)
	if tr, ok := rules[tableID]; ok {
		newRules = append(newRules, tr.Clone().Reset(job.SchemaName, tblInfo.Name.L, "", append(newPartIDs, tblInfo.ID)...))
	}

	for idx, newPartition := range newPartitions {
		if pr, ok := rules[oldPartRules[idx]]; ok {
			newRules = append(newRules, pr.Clone().Reset(job.SchemaName, tblInfo.Name.L, newPartition.Name.L, newPartition.ID))
		}
	}

	patch := label.NewRulePatch(newRules, []string{})
	err = infosync.UpdateLabelRules(context.TODO(), patch)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the label rules")
	}

	newIDs := make([]int64, len(oldIDs))
	for i := range oldIDs {
		newIDs[i] = newPartitions[i].ID
	}
	job.CtxVars = []interface{}{oldIDs, newIDs}
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
	asyncNotifyEvent(d, &util.Event{Tp: model.ActionTruncateTablePartition, TableInfo: tblInfo, PartInfo: &model.PartitionInfo{Definitions: newPartitions}})
	// A background job will be created to delete old partition data.
	job.Args = []interface{}{oldIDs}
	return ver, nil
}

// onExchangeTablePartition exchange partition data
func (w *worker) onExchangeTablePartition(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var (
		// defID only for updateSchemaVersion
		defID          int64
		ptSchemaID     int64
		ptID           int64
		partName       string
		withValidation bool
	)

	if err := job.DecodeArgs(&defID, &ptSchemaID, &ptID, &partName, &withValidation); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ntDbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	nt, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	pt, err := getTableInfo(t, ptID, ptSchemaID)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	if pt.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrInvalidDDLState.GenWithStack("table %s is not in public, but %s", pt.Name, pt.State)
	}

	err = checkExchangePartition(pt, nt)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = checkTableDefCompatible(pt, nt)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	index, _, err := getPartitionDef(pt, partName)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if withValidation {
		err = checkExchangePartitionRecordValidation(w, pt, index, ntDbInfo.Name, nt.Name)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	// partition table auto IDs.
	ptAutoIDs, err := t.GetAutoIDAccessors(ptSchemaID, ptID).Get()
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// non-partition table auto IDs.
	ntAutoIDs, err := t.GetAutoIDAccessors(job.SchemaID, nt.ID).Get()
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	_, partDef, err := getPartitionDef(pt, partName)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if pt.TiFlashReplica != nil {
		for i, id := range pt.TiFlashReplica.AvailablePartitionIDs {
			if id == partDef.ID {
				pt.TiFlashReplica.AvailablePartitionIDs[i] = nt.ID
				break
			}
		}
	}

	// exchange table meta id
	partDef.ID, nt.ID = nt.ID, partDef.ID

	err = t.UpdateTable(ptSchemaID, pt)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	failpoint.Inject("exchangePartitionErr", func(val failpoint.Value) {
		if val.(bool) {
			job.State = model.JobStateCancelled
			failpoint.Return(ver, errors.New("occur an error after updating partition id"))
		}
	})

	// recreate non-partition table meta info
	err = t.DropTableOrView(job.SchemaID, partDef.ID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = t.CreateTableOrView(job.SchemaID, nt)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// Set both tables to the maximum auto IDs between normal table and partitioned table.
	newAutoIDs := meta.AutoIDGroup{
		RowID:       mathutil.Max(ptAutoIDs.RowID, ntAutoIDs.RowID),
		IncrementID: mathutil.Max(ptAutoIDs.IncrementID, ntAutoIDs.IncrementID),
		RandomID:    mathutil.Max(ptAutoIDs.RandomID, ntAutoIDs.RandomID),
	}
	err = t.GetAutoIDAccessors(ptSchemaID, pt.ID).Put(newAutoIDs)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	err = t.GetAutoIDAccessors(job.SchemaID, nt.ID).Put(newAutoIDs)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// the follow code is a swap function for rules of two partitions
	// though partitions has exchanged their ID, swap still take effect

	bundles, err := bundlesForExchangeTablePartition(t, job, pt, partDef, nt)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
	}

	ntrID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, job.SchemaName, nt.Name.L)
	ptrID := fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, job.SchemaName, pt.Name.L, partDef.Name.L)

	rules, err := infosync.GetLabelRules(context.TODO(), []string{ntrID, ptrID})
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Wrapf(err, "failed to get PD the label rules")
	}

	ntr := rules[ntrID]
	ptr := rules[ptrID]

	partIDs := getPartitionIDs(nt)

	var setRules []*label.Rule
	var deleteRules []string
	if ntr != nil && ptr != nil {
		setRules = append(setRules, ntr.Clone().Reset(job.SchemaName, pt.Name.L, partDef.Name.L, partDef.ID))
		setRules = append(setRules, ptr.Clone().Reset(job.SchemaName, nt.Name.L, "", append(partIDs, nt.ID)...))
	} else if ptr != nil {
		setRules = append(setRules, ptr.Clone().Reset(job.SchemaName, nt.Name.L, "", append(partIDs, nt.ID)...))
		// delete ptr
		deleteRules = append(deleteRules, ptrID)
	} else if ntr != nil {
		setRules = append(setRules, ntr.Clone().Reset(job.SchemaName, pt.Name.L, partDef.Name.L, partDef.ID))
		// delete ntr
		deleteRules = append(deleteRules, ntrID)
	}

	patch := label.NewRulePatch(setRules, deleteRules)
	err = infosync.UpdateLabelRules(context.TODO(), patch)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the label rules")
	}

	ver, err = updateSchemaVersion(d, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StateNone, ver, pt)
	return ver, nil
}

func bundlesForExchangeTablePartition(t *meta.Meta, job *model.Job, pt *model.TableInfo, newPar *model.PartitionDefinition, nt *model.TableInfo) ([]*placement.Bundle, error) {
	bundles := make([]*placement.Bundle, 0, 3)

	ptBundle, err := placement.NewTableBundle(t, pt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ptBundle != nil {
		bundles = append(bundles, ptBundle)
	}

	parBundle, err := placement.NewPartitionBundle(t, *newPar)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if parBundle != nil {
		bundles = append(bundles, parBundle)
	}

	ntBundle, err := placement.NewTableBundle(t, nt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ntBundle != nil {
		bundles = append(bundles, ntBundle)
	}

	if parBundle == nil && ntBundle != nil {
		// newPar.ID is the ID of old table to exchange, so ntBundle != nil means it has some old placement settings.
		// We should remove it in this situation
		bundles = append(bundles, placement.NewBundle(newPar.ID))
	}

	if parBundle != nil && ntBundle == nil {
		// nt.ID is the ID of old partition to exchange, so parBundle != nil means it has some old placement settings.
		// We should remove it in this situation
		bundles = append(bundles, placement.NewBundle(nt.ID))
	}

	return bundles, nil
}

func checkExchangePartitionRecordValidation(w *worker, pt *model.TableInfo, index int, schemaName, tableName model.CIStr) error {
	var sql string
	var paramList []interface{}

	pi := pt.Partition

	switch pi.Type {
	case model.PartitionTypeHash:
		if pi.Num == 1 {
			return nil
		}
		var buf strings.Builder
		buf.WriteString("select 1 from %n.%n where mod(")
		buf.WriteString(pi.Expr)
		buf.WriteString(", %?) != %? limit 1")
		sql = buf.String()
		paramList = append(paramList, schemaName.L, tableName.L, pi.Num, index)
	case model.PartitionTypeRange:
		// Table has only one partition and has the maximum value
		if len(pi.Definitions) == 1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
			return nil
		}
		// For range expression and range columns
		if len(pi.Columns) == 0 {
			sql, paramList = buildCheckSQLForRangeExprPartition(pi, index, schemaName, tableName)
		} else if len(pi.Columns) == 1 {
			sql, paramList = buildCheckSQLForRangeColumnsPartition(pi, index, schemaName, tableName)
		}
	case model.PartitionTypeList:
		if len(pi.Columns) == 0 {
			sql, paramList = buildCheckSQLForListPartition(pi, index, schemaName, tableName)
		} else if len(pi.Columns) == 1 {
			sql, paramList = buildCheckSQLForListColumnsPartition(pi, index, schemaName, tableName)
		}
	default:
		return dbterror.ErrUnsupportedPartitionType.GenWithStackByArgs(pt.Name.O)
	}

	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(w.ddlJobCtx, nil, sql, paramList...)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := len(rows)
	if rowCount != 0 {
		return errors.Trace(dbterror.ErrRowDoesNotMatchPartition)
	}
	return nil
}

func buildCheckSQLForRangeExprPartition(pi *model.PartitionInfo, index int, schemaName, tableName model.CIStr) (string, []interface{}) {
	var buf strings.Builder
	paramList := make([]interface{}, 0, 4)
	// Since the pi.Expr string may contain the identifier, which couldn't be escaped in our ParseWithParams(...)
	// So we write it to the origin sql string here.
	if index == 0 {
		buf.WriteString("select 1 from %n.%n where ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" >= %? limit 1")
		paramList = append(paramList, schemaName.L, tableName.L, trimQuotation(pi.Definitions[index].LessThan[0]))
		return buf.String(), paramList
	} else if index == len(pi.Definitions)-1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
		buf.WriteString("select 1 from %n.%n where ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" < %? limit 1")
		paramList = append(paramList, schemaName.L, tableName.L, trimQuotation(pi.Definitions[index-1].LessThan[0]))
		return buf.String(), paramList
	} else {
		buf.WriteString("select 1 from %n.%n where ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" < %? or ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" >= %? limit 1")
		paramList = append(paramList, schemaName.L, tableName.L, trimQuotation(pi.Definitions[index-1].LessThan[0]), trimQuotation(pi.Definitions[index].LessThan[0]))
		return buf.String(), paramList
	}
}

func trimQuotation(str string) string {
	return strings.Trim(str, "\"")
}

func buildCheckSQLForRangeColumnsPartition(pi *model.PartitionInfo, index int, schemaName, tableName model.CIStr) (string, []interface{}) {
	paramList := make([]interface{}, 0, 6)
	colName := pi.Columns[0].L
	if index == 0 {
		paramList = append(paramList, schemaName.L, tableName.L, colName, trimQuotation(pi.Definitions[index].LessThan[0]))
		return "select 1 from %n.%n where %n >= %? limit 1", paramList
	} else if index == len(pi.Definitions)-1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
		paramList = append(paramList, schemaName.L, tableName.L, colName, trimQuotation(pi.Definitions[index-1].LessThan[0]))
		return "select 1 from %n.%n where %n < %? limit 1", paramList
	} else {
		paramList = append(paramList, schemaName.L, tableName.L, colName, trimQuotation(pi.Definitions[index-1].LessThan[0]), colName, trimQuotation(pi.Definitions[index].LessThan[0]))
		return "select 1 from %n.%n where %n < %? or %n >= %? limit 1", paramList
	}
}

func buildCheckSQLForListPartition(pi *model.PartitionInfo, index int, schemaName, tableName model.CIStr) (string, []interface{}) {
	var buf strings.Builder
	buf.WriteString("select 1 from %n.%n where ")
	buf.WriteString(pi.Expr)
	buf.WriteString(" not in (%?) limit 1")
	inValues := getInValues(pi, index)

	paramList := make([]interface{}, 0, 3)
	paramList = append(paramList, schemaName.L, tableName.L, inValues)
	return buf.String(), paramList
}

func buildCheckSQLForListColumnsPartition(pi *model.PartitionInfo, index int, schemaName, tableName model.CIStr) (string, []interface{}) {
	colName := pi.Columns[0].L
	var buf strings.Builder
	buf.WriteString("select 1 from %n.%n where %n not in (%?) limit 1")
	inValues := getInValues(pi, index)

	paramList := make([]interface{}, 0, 4)
	paramList = append(paramList, schemaName.L, tableName.L, colName, inValues)
	return buf.String(), paramList
}

func getInValues(pi *model.PartitionInfo, index int) []string {
	inValues := make([]string, 0, len(pi.Definitions[index].InValues))
	for _, inValue := range pi.Definitions[index].InValues {
		inValues = append(inValues, inValue...)
	}
	return inValues
}

func checkAddPartitionTooManyPartitions(piDefs uint64) error {
	if piDefs > uint64(PartitionCountLimit) {
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

func checkNoHashPartitions(ctx sessionctx.Context, partitionNum uint64) error {
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
func checkPartitioningKeysConstraints(sctx sessionctx.Context, s *ast.CreateTableStmt, tblInfo *model.TableInfo) error {
	// Returns directly if there are no unique keys in the table.
	if len(tblInfo.Indices) == 0 && !tblInfo.PKIsHandle {
		return nil
	}

	var partCols stringSlice
	if s.Partition.Expr != nil {
		extractCols := newPartitionExprChecker(sctx, tblInfo)
		s.Partition.Expr.Accept(extractCols)
		partColumns, err := extractCols.columns, extractCols.err
		if err != nil {
			return err
		}
		partCols = columnInfoSlice(partColumns)
	} else if len(s.Partition.ColumnNames) > 0 {
		partCols = columnNameSlice(s.Partition.ColumnNames)
	} else {
		// TODO: Check keys constraints for list, key partition type and so on.
		return nil
	}

	// Checks that the partitioning key is included in the constraint.
	// Every unique key on the table must use every column in the table's partitioning expression.
	// See https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	for _, index := range tblInfo.Indices {
		if index.Unique && !checkUniqueKeyIncludePartKey(partCols, index.Columns) {
			if index.Primary {
				return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY KEY")
			}
			if !config.GetGlobalConfig().EnableGlobalIndex {
				return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("UNIQUE INDEX")
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
			return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY KEY")
		}
	}
	return nil
}

func checkPartitionKeysConstraint(pi *model.PartitionInfo, indexColumns []*model.IndexColumn, tblInfo *model.TableInfo) (bool, error) {
	var (
		partCols []*model.ColumnInfo
		err      error
	)
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
			colInfo := getColumnInfoByName(tblInfo, col.L)
			if colInfo == nil {
				return false, infoschema.ErrColumnNotExists.GenWithStackByArgs(col, tblInfo.Name)
			}
			partCols = append(partCols, colInfo)
		}
	}

	// In MySQL, every unique key on the table must use every column in the table's partitioning expression.(This
	// also includes the table's primary key.)
	// In TiDB, global index will be built when this constraint is not satisfied and EnableGlobalIndex is set.
	// See https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	return checkUniqueKeyIncludePartKey(columnInfoSlice(partCols), indexColumns), nil
}

type columnNameExtractor struct {
	extractedColumns []*model.ColumnInfo
	tblInfo          *model.TableInfo
	err              error
}

func (cne *columnNameExtractor) Enter(node ast.Node) (ast.Node, bool) {
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
	for i := 0; i < partCols.Len(); i++ {
		partCol := partCols.At(i)
		idxCol := findColumnInIndexCols(partCol, idxCols)
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

// isColUnsigned returns true if the partitioning key column is unsigned.
func isColUnsigned(cols []*model.ColumnInfo, pi *model.PartitionInfo) bool {
	for _, col := range cols {
		isUnsigned := mysql.HasUnsignedFlag(col.GetFlag())
		if isUnsigned && strings.Contains(strings.ToLower(pi.Expr), col.Name.L) {
			return true
		}
	}
	return false
}

// truncateTableByReassignPartitionIDs reassigns new partition ids.
func truncateTableByReassignPartitionIDs(t *meta.Meta, tblInfo *model.TableInfo) error {
	newDefs := make([]model.PartitionDefinition, 0, len(tblInfo.Partition.Definitions))
	for _, def := range tblInfo.Partition.Definitions {
		pid, err := t.GenGlobalID()
		if err != nil {
			return errors.Trace(err)
		}
		newDef := def
		newDef.ID = pid
		newDefs = append(newDefs, newDef)
	}
	tblInfo.Partition.Definitions = newDefs
	return nil
}

type partitionExprProcessor func(sessionctx.Context, *model.TableInfo, ast.ExprNode) error

type partitionExprChecker struct {
	processors []partitionExprProcessor
	ctx        sessionctx.Context
	tbInfo     *model.TableInfo
	err        error

	columns []*model.ColumnInfo
}

func newPartitionExprChecker(ctx sessionctx.Context, tbInfo *model.TableInfo, processor ...partitionExprProcessor) *partitionExprChecker {
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

func (p *partitionExprChecker) extractColumns(_ sessionctx.Context, _ *model.TableInfo, expr ast.ExprNode) error {
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

func checkPartitionExprAllowed(_ sessionctx.Context, tb *model.TableInfo, e ast.ExprNode) error {
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
		*ast.TimeUnitExpr:
		return nil
	}
	return errors.Trace(dbterror.ErrPartitionFunctionIsNotAllowed)
}

func checkPartitionExprArgs(_ sessionctx.Context, tblInfo *model.TableInfo, e ast.ExprNode) error {
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
		return errors.Trace(checkResultOK(slice.AllOf(argsType, func(i int) bool {
			return hasDateArgs(argsType[i])
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
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeDate || argsType[i] == mysql.TypeDatetime
	})
}

func hasTimeArgs(argsType ...byte) bool {
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeDuration || argsType[i] == mysql.TypeDatetime
	})
}

func hasTimestampArgs(argsType ...byte) bool {
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeTimestamp
	})
}

func hasDatetimeArgs(argsType ...byte) bool {
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeDatetime
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
