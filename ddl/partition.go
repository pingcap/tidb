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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
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
	"github.com/pingcap/tidb/util/mock"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/slice"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/prometheus/client_golang/prometheus"
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
			needRetry, err := checkPartitionReplica(tblInfo.TiFlashReplica.Count, addingDefinitions, d)
			if err != nil {
				return convertAddTablePartitionJob2RollbackJob(d, t, job, err, tblInfo)
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
					logutil.BgLogger().Error("update tiflash sync progress cache failed",
						zap.Error(err),
						zap.Int64("tableID", tblInfo.ID),
						zap.Int64("partitionID", d.ID),
					)
				}
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

func alterTablePartitionBundles(t *meta.Meta, tblInfo *model.TableInfo, addingDefinitions []model.PartitionDefinition) ([]*placement.Bundle, error) {
	var bundles []*placement.Bundle

	// tblInfo do not include added partitions, so we should add them first
	tblInfo = tblInfo.Clone()
	p := *tblInfo.Partition
	p.Definitions = append([]model.PartitionDefinition{}, p.Definitions...)
	p.Definitions = append(tblInfo.Partition.Definitions, addingDefinitions...)
	tblInfo.Partition = &p

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
		if s.Sub == nil {
			enable = true
		}
	case model.PartitionTypeHash:
		// Partition by hash is enabled by default.
		// Note that linear hash is simply ignored, and creates non-linear hash.
		if s.Linear {
			ctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrUnsupportedCreatePartition.GenWithStack("LINEAR HASH is not supported, using non-linear HASH instead"))
		}
		if s.Sub == nil {
			enable = true
		}
	case model.PartitionTypeKey:
		// Note that linear key is simply ignored, and creates non-linear hash.
		if s.Linear {
			ctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrUnsupportedCreatePartition.GenWithStack("LINEAR KEY is not supported, using non-linear KEY instead"))
		}
		if s.Sub == nil && len(s.ColumnNames) != 0 {
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

	err := generatePartitionDefinitionsFromInterval(ctx, s, tbInfo)
	if err != nil {
		return errors.Trace(err)
	}

	defs, err := buildPartitionDefinitionsInfo(ctx, s.Definitions, tbInfo)
	if err != nil {
		return errors.Trace(err)
	}

	tbInfo.Partition.Definitions = defs

	if s.Interval != nil {
		// Syntactic sugar for INTERVAL partitioning
		// Generate the resulting CREATE TABLE as the query string
		query, ok := ctx.Value(sessionctx.QueryString).(string)
		if ok {
			sqlMode := ctx.GetSessionVars().SQLMode
			var buf bytes.Buffer
			AppendPartitionDefs(tbInfo.Partition, &buf, sqlMode)

			syntacticSugar := s.Interval.OriginalText()
			syntacticStart := s.Interval.OriginTextPosition()
			newQuery := query[:syntacticStart] + "(" + buf.String() + ")" + query[syntacticStart+len(syntacticSugar):]
			ctx.SetValue(sessionctx.QueryString, newQuery)
		}
	}

	partCols, err := getPartitionColSlices(ctx, tbInfo, s)
	if err != nil {
		return errors.Trace(err)
	}

	for _, index := range tbInfo.Indices {
		if index.Unique && !checkUniqueKeyIncludePartKey(partCols, index.Columns) {
			index.Global = config.GetGlobalConfig().EnableGlobalIndex
		}
	}
	return nil
}

func getPartitionColSlices(sctx sessionctx.Context, tblInfo *model.TableInfo, s *ast.PartitionOptions) (partCols stringSlice, err error) {
	if s.Expr != nil {
		extractCols := newPartitionExprChecker(sctx, tblInfo)
		s.Expr.Accept(extractCols)
		partColumns, err := extractCols.columns, extractCols.err
		if err != nil {
			return nil, err
		}
		partCols = columnInfoSlice(partColumns)
	} else if len(s.ColumnNames) > 0 {
		partCols = columnNameSlice(s.ColumnNames)
	} else {
		return nil, errors.Errorf("Table partition metadata not correct, neither partition expression or list of partition columns")
	}
	return partCols, nil
}

// getPartitionIntervalFromTable checks if a partitioned table matches a generated INTERVAL partitioned scheme
// will return nil if error occurs, i.e. not an INTERVAL partitioned table
func getPartitionIntervalFromTable(ctx sessionctx.Context, tbInfo *model.TableInfo) *ast.PartitionInterval {
	if tbInfo.Partition == nil ||
		tbInfo.Partition.Type != model.PartitionTypeRange {
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
		startIdx  int    = 0
		endIdx    int    = len(tbInfo.Partition.Definitions) - 1
		isIntType bool   = true
		minVal    string = "0"
	)
	if len(tbInfo.Partition.Columns) > 0 {
		partCol := findColumnByName(tbInfo.Partition.Columns[0].L, tbInfo)
		if partCol.FieldType.EvalType() == types.ETInt {
			min := getLowerBoundInt(partCol)
			minVal = strconv.FormatInt(min, 10)
		} else if partCol.FieldType.EvalType() == types.ETDatetime {
			isIntType = false
			minVal = "0000-01-01"
		} else {
			// Only INT and Datetime columns are supported for INTERVAL partitioning
			return nil
		}
	} else {
		if !isPartExprUnsigned(tbInfo) {
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
		exprs, err := expression.ParseSimpleExprsWithNames(ctx, exprStr, nil, nil)
		if err != nil {
			return nil
		}
		val, isNull, err := exprs[0].EvalInt(ctx, chunk.Row{})
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
		exprs, err := expression.ParseSimpleExprsWithNames(ctx, exprStr, nil, nil)
		if err != nil {
			return nil
		}
		val, isNull, err := exprs[0].EvalInt(ctx, chunk.Row{})
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
		Tp:       model.PartitionTypeRange,
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
func comparePartitionAstAndModel(ctx sessionctx.Context, pAst *ast.PartitionOptions, pModel *model.PartitionInfo) error {
	a := pAst.Definitions
	m := pModel.Definitions
	if len(pAst.Definitions) != len(pModel.Definitions) {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning: number of partitions generated != partition defined (%d != %d)", len(a), len(m))
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
		cmpExpr := &ast.BinaryOperationExpr{
			Op: opcode.EQ,
			L:  ast.NewValueExpr(lessThan, "", ""),
			R:  generatedExpr,
		}
		cmp, err := expression.EvalAstExpr(ctx, cmpExpr)
		if err != nil {
			return err
		}
		if cmp.GetInt64() != 1 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("INTERVAL partitioning: LESS THAN for partition %s differs between generated and defined", m[i].Name.O))
		}
	}
	return nil
}

// comparePartitionDefinitions check if generated definitions are the same as the given ones
// Allow names to differ
// returns error in case of error or non-accepted difference
func comparePartitionDefinitions(ctx sessionctx.Context, a, b []*ast.PartitionDefinition) error {
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
		cmp, err := expression.EvalAstExpr(ctx, cmpExpr)
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
		ret = mathutil.Min(ret, types.IntergerSignedLowerBound(col.GetType()))
	}
	return ret
}

// generatePartitionDefinitionsFromInterval generates partition Definitions according to INTERVAL options on partOptions
func generatePartitionDefinitionsFromInterval(ctx sessionctx.Context, partOptions *ast.PartitionOptions, tbInfo *model.TableInfo) error {
	if partOptions.Interval == nil {
		return nil
	}
	if tbInfo.Partition.Type != model.PartitionTypeRange {
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
	case ast.TimeUnitInvalid, ast.TimeUnitYear, ast.TimeUnitQuarter, ast.TimeUnitMonth, ast.TimeUnitWeek, ast.TimeUnitDay, ast.TimeUnitHour, ast.TimeUnitDayMinute, ast.TimeUnitSecond:
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
			var min int64
			if partCol != nil {
				min = getLowerBoundInt(partCol)
			} else {
				if !isPartExprUnsigned(tbInfo) {
					min = math.MinInt64
				}
			}
			partExpr = ast.NewValueExpr(min, "", "")
		}
		partOptions.Definitions = append(partOptions.Definitions, &ast.PartitionDefinition{
			Name: model.NewCIStr("P_NULL"),
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
			Name: model.NewCIStr("P_MAXVALUE"),
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
		err := comparePartitionAstAndModel(ctx, partOptions, tbInfo.Partition)
		if err != nil {
			return err
		}
	}

	return nil
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
func GeneratePartDefsFromInterval(ctx sessionctx.Context, tp ast.AlterTableType, tbInfo *model.TableInfo, partitionOptions *ast.PartitionOptions) error {
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
	lastVal, err := expression.EvalAstExpr(ctx, lastExpr)
	if err != nil {
		return err
	}
	var partDefs []*ast.PartitionDefinition
	if len(partitionOptions.Definitions) != 0 {
		partDefs = partitionOptions.Definitions
	} else {
		partDefs = make([]*ast.PartitionDefinition, 0, 1)
	}
	for i := 0; i < mysql.PartitionCountLimit; i++ {
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
					FnName: model.NewCIStr("DATE_ADD"),
					Args: []ast.ExprNode{
						startExpr,
						currExpr,
						&ast.TimeUnitExpr{Unit: timeUnit},
					},
				}
			}
		}
		currVal, err = expression.EvalAstExpr(ctx, currExpr)
		if err != nil {
			return err
		}
		cmp, err := currVal.Compare(ctx.GetSessionVars().StmtCtx, &lastVal, collate.GetBinaryCollator())
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
			Name: model.NewCIStr(partName),
			Clause: &ast.PartitionDefinitionClauseLessThan{
				Exprs: []ast.ExprNode{currExpr},
			},
		})
		if cmp == 0 {
			// Last partition!
			break
		}
	}
	if len(tbInfo.Partition.Definitions)+len(partDefs) > mysql.PartitionCountLimit {
		return errors.Trace(dbterror.ErrTooManyPartitions)
	}
	partitionOptions.Definitions = partDefs
	return nil
}

// buildPartitionDefinitionsInfo build partition definitions info without assign partition id. tbInfo will be constant
func buildPartitionDefinitionsInfo(ctx sessionctx.Context, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) (partitions []model.PartitionDefinition, err error) {
	switch tbInfo.Partition.Type {
	case model.PartitionTypeRange:
		partitions, err = buildRangePartitionDefinitions(ctx, defs, tbInfo)
	case model.PartitionTypeHash, model.PartitionTypeKey:
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
	colTypes := collectColumnsType(tbInfo)
	if len(colTypes) != len(tbInfo.Partition.Columns) {
		return nil, dbterror.ErrWrongPartitionName.GenWithStack("partition column name cannot be found")
	}
	for _, def := range defs {
		if err := def.Clause.Validate(model.PartitionTypeList, len(tbInfo.Partition.Columns)); err != nil {
			return nil, err
		}
		clause := def.Clause.(*ast.PartitionDefinitionClauseIn)
		if len(tbInfo.Partition.Columns) > 0 {
			for _, vs := range clause.Values {
				// TODO: use the generated strings / normalized partition values
				_, err := checkAndGetColumnsTypeAndValuesMatch(ctx, colTypes, vs)
				if err != nil {
					return nil, err
				}
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

func buildRangePartitionDefinitions(ctx sessionctx.Context, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) ([]model.PartitionDefinition, error) {
	definitions := make([]model.PartitionDefinition, 0, len(defs))
	exprChecker := newPartitionExprChecker(ctx, nil, checkPartitionExprAllowed)
	colTypes := collectColumnsType(tbInfo)
	if len(colTypes) != len(tbInfo.Partition.Columns) {
		return nil, dbterror.ErrWrongPartitionName.GenWithStack("partition column name cannot be found")
	}
	for _, def := range defs {
		if err := def.Clause.Validate(model.PartitionTypeRange, len(tbInfo.Partition.Columns)); err != nil {
			return nil, err
		}
		clause := def.Clause.(*ast.PartitionDefinitionClauseLessThan)
		var partValStrings []string
		if len(tbInfo.Partition.Columns) > 0 {
			var err error
			if partValStrings, err = checkAndGetColumnsTypeAndValuesMatch(ctx, colTypes, clause.Exprs); err != nil {
				return nil, err
			}
		} else {
			if err := checkPartitionValuesIsInt(ctx, def.Name, clause.Exprs, tbInfo); err != nil {
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
		for i, expr := range clause.Exprs {
			expr.Accept(exprChecker)
			if exprChecker.err != nil {
				return nil, exprChecker.err
			}
			// If multi-column use new evaluated+normalized output, instead of just formatted expression
			if len(partValStrings) > i && len(colTypes) > 1 {
				partVal := partValStrings[i]
				switch colTypes[i].EvalType() {
				case types.ETInt:
					// no wrapping
				case types.ETDatetime, types.ETString, types.ETDuration:
					if _, ok := clause.Exprs[i].(*ast.MaxValueExpr); !ok {
						// Don't wrap MAXVALUE
						partVal = driver.WrapInSingleQuotes(partVal)
					}
				default:
					return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
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

func checkPartitionValuesIsInt(ctx sessionctx.Context, defName interface{}, exprs []ast.ExprNode, tbInfo *model.TableInfo) error {
	tp := types.NewFieldType(mysql.TypeLonglong)
	if isPartExprUnsigned(tbInfo) {
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
			return dbterror.ErrValuesIsNotIntType.GenWithStackByArgs(defName)
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

func checkReorgPartitionNames(p *model.PartitionInfo, droppedNames []model.CIStr, pi *model.PartitionInfo) error {
	partNames := make(map[string]struct{})
	oldDefs := p.Definitions
	for _, oldDef := range oldDefs {
		partNames[oldDef.Name.L] = struct{}{}
	}
	for _, delName := range droppedNames {
		if _, ok := partNames[delName.L]; !ok {
			return dbterror.ErrSameNamePartition.GenWithStackByArgs(delName)
		}
		delete(partNames, delName.L)
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

	e, err := expression.RewriteSimpleExprWithTableInfo(ctx, tblInfo, expr, false)
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

	if strings.EqualFold(defs[len(defs)-1].LessThan[0], partitionMaxValue) {
		defs = defs[:len(defs)-1]
	}
	isUnsigned := isPartExprUnsigned(tblInfo)
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
		if isPartExprUnsigned(tblInfo) {
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

// CheckDropTablePartition checks if the partition exists and does not allow deleting the last existing partition in the table.
func CheckDropTablePartition(meta *model.TableInfo, partLowerNames []string) error {
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
	if job.Type == model.ActionAddTablePartition || job.Type == model.ActionReorganizePartition {
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

		if _, err := alterTableLabelRule(job.SchemaName, tblInfo, getIDs([]*model.TableInfo{tblInfo})); err != nil {
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
		err = CheckDropTablePartition(tblInfo, partNames)
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

		if _, err := alterTableLabelRule(job.SchemaName, tblInfo, getIDs([]*model.TableInfo{tblInfo})); err != nil {
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
		dbInfo, err := t.GetDatabase(job.SchemaID)
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
			sctx, err1 := w.sessPool.get()
			if err1 != nil {
				return ver, err1
			}
			defer w.sessPool.put(sctx)
			rh := newReorgHandler(newSession(sctx))
			reorgInfo, err := getReorgInfoFromPartitions(d.jobContext(job.ID), d, rh, job, dbInfo, pt, physicalTableIDs, elements)

			if err != nil || reorgInfo.first {
				// If we run reorg firstly, we should update the job snapshot version
				// and then run the reorg next time.
				return ver, errors.Trace(err)
			}
			err = w.runReorgJob(rh, reorgInfo, tbl.Meta(), d.lease, func() (dropIndexErr error) {
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
				return ver, errors.Trace(err)
			}
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
	if nt.ExchangePartitionInfo == nil || !nt.ExchangePartitionInfo.ExchangePartitionFlag {
		nt.ExchangePartitionInfo = &model.ExchangePartitionInfo{
			ExchangePartitionFlag:  true,
			ExchangePartitionID:    ptID,
			ExchangePartitionDefID: defID,
		}
		return updateVersionAndTableInfoWithCheck(d, t, job, nt, true)
	}

	if d.lease > 0 {
		delayForAsyncCommit()
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

	failpoint.Inject("exchangePartitionAutoID", func(val failpoint.Value) {
		if val.(bool) {
			se, err := w.sessPool.get()
			defer w.sessPool.put(se)
			if err != nil {
				failpoint.Return(ver, err)
			}
			sess := newSession(se)
			_, err = sess.execute(context.Background(), "insert ignore into test.pt values (40000000)", "exchange_partition_test")
			if err != nil {
				failpoint.Return(ver, err)
			}
		}
	})

	err = checkExchangePartitionPlacementPolicy(t, partDef.PlacementPolicyRef, nt.PlacementPolicyRef)
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

	nt.ExchangePartitionInfo = nil
	ver, err = updateVersionAndTableInfoWithCheck(d, t, job, nt, true)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StateNone, ver, pt)
	return ver, nil
}

func checkReorgPartition(t *meta.Meta, job *model.Job) (*model.TableInfo, []model.CIStr, *model.PartitionInfo, []model.PartitionDefinition, []model.PartitionDefinition, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, nil, nil, errors.Trace(err)
	}
	partInfo := &model.PartitionInfo{}
	var partNames []model.CIStr
	err = job.DecodeArgs(&partNames, &partInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, nil, nil, errors.Trace(err)
	}
	addingDefs := tblInfo.Partition.AddingDefinitions
	droppingDefs := tblInfo.Partition.DroppingDefinitions
	if len(addingDefs) == 0 {
		addingDefs = []model.PartitionDefinition{}
	}
	if len(droppingDefs) == 0 {
		droppingDefs = []model.PartitionDefinition{}
	}
	return tblInfo, partNames, partInfo, droppingDefs, addingDefs, nil
}

func (w *worker) onReorganizePartition(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	// Handle the rolling back job
	if job.IsRollingback() {
		ver, err := w.onDropTablePartition(d, t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	tblInfo, partNamesCIStr, partInfo, _, addingDefinitions, err := checkReorgPartition(t, job)
	if err != nil {
		return ver, err
	}
	partNames := make([]string, len(partNamesCIStr))
	for i := range partNamesCIStr {
		partNames[i] = partNamesCIStr[i].L
	}

	// In order to skip maintaining the state check in partitionDefinition, TiDB use dropping/addingDefinition instead of state field.
	// So here using `job.SchemaState` to judge what the stage of this job is.
	originalState := job.SchemaState
	switch job.SchemaState {
	case model.StateNone:
		// job.SchemaState == model.StateNone means the job is in the initial state of reorg partition.
		// Here should use partInfo from job directly and do some check action.
		// In case there was a race for queueing different schema changes on the same
		// table and the checks was not done on the current schema version.
		// The partInfo may have been checked against an older schema version for example.
		// If the check is done here, it does not need to be repeated, since no other
		// DDL on the same table can be run concurrently.
		err = checkAddPartitionTooManyPartitions(uint64(len(tblInfo.Partition.Definitions) +
			len(partInfo.Definitions) -
			len(partNames)))
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = checkReorgPartitionNames(tblInfo.Partition, partNamesCIStr, partInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		// Re-check that the dropped/added partitions are compatible with current definition
		firstPartIdx, lastPartIdx, idMap, err := getReplacedPartitionIDs(partNamesCIStr, tblInfo.Partition)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		sctx := w.sess.Context
		if err = checkReorgPartitionDefs(sctx, tblInfo, partInfo, firstPartIdx, lastPartIdx, idMap); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		// move the adding definition into tableInfo.
		updateAddingPartitionInfo(partInfo, tblInfo)
		orgDefs := tblInfo.Partition.Definitions
		_ = updateDroppingPartitionInfo(tblInfo, partNames)
		// Reset original partitions, and keep DroppedDefinitions
		tblInfo.Partition.Definitions = orgDefs

		// modify placement settings
		for _, def := range tblInfo.Partition.AddingDefinitions {
			if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(t, job, def.PlacementPolicyRef); err != nil {
				// job.State = model.JobStateCancelled may be set depending on error in function above.
				return ver, errors.Trace(err)
			}
		}

		// From now on we cannot just cancel the DDL, we must roll back if changesMade!
		changesMade := false
		if tblInfo.TiFlashReplica != nil {
			// Must set placement rule, and make sure it succeeds.
			if err := infosync.ConfigureTiFlashPDForPartitions(true, &tblInfo.Partition.AddingDefinitions, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels, tblInfo.ID); err != nil {
				logutil.BgLogger().Error("ConfigureTiFlashPDForPartitions fails", zap.Error(err))
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			changesMade = true
			// In the next step, StateDeleteOnly, wait to verify the TiFlash replicas are OK
		}

		bundles, err := alterTablePartitionBundles(t, tblInfo, tblInfo.Partition.AddingDefinitions)
		if err != nil {
			if !changesMade {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			return convertAddTablePartitionJob2RollbackJob(d, t, job, err, tblInfo)
		}

		if len(bundles) > 0 {
			if err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles); err != nil {
				if !changesMade {
					job.State = model.JobStateCancelled
					return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
				}
				return convertAddTablePartitionJob2RollbackJob(d, t, job, err, tblInfo)
			}
			changesMade = true
		}

		ids := getIDs([]*model.TableInfo{tblInfo})
		for _, p := range tblInfo.Partition.AddingDefinitions {
			ids = append(ids, p.ID)
		}
		changed, err := alterTableLabelRule(job.SchemaName, tblInfo, ids)
		changesMade = changesMade || changed
		if err != nil {
			if !changesMade {
				job.State = model.JobStateCancelled
				return ver, err
			}
			return convertAddTablePartitionJob2RollbackJob(d, t, job, err, tblInfo)
		}

		// Doing the preSplitAndScatter here, since all checks are completed,
		// and we will soon start writing to the new partitions.
		if s, ok := d.store.(kv.SplittableStore); ok && s != nil {
			// partInfo only contains the AddingPartitions
			splitPartitionTableRegion(w.sess.Context, s, tblInfo, partInfo, true)
		}

		// Assume we cannot have more than MaxUint64 rows, set the progress to 1/10 of that.
		metrics.GetBackfillProgressByLabel(metrics.LblReorgPartition, job.SchemaName, tblInfo.Name.String()).Set(0.1 / float64(math.MaxUint64))
		job.SchemaState = model.StateDeleteOnly
		tblInfo.Partition.DDLState = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Is really both StateDeleteOnly AND StateWriteOnly needed?
		// If transaction A in WriteOnly inserts row 1 (into both new and old partition set)
		// and then transaction B in DeleteOnly deletes that row (in both new and old)
		// does really transaction B need to do the delete in the new partition?
		// Yes, otherwise it would still be there when the WriteReorg happens,
		// and WriteReorg would only copy existing rows to the new table, so unless it is
		// deleted it would result in a ghost row!
		// What about update then?
		// Updates also need to be handled for new partitions in DeleteOnly,
		// since it would not be overwritten during Reorganize phase.
		// BUT if the update results in adding in one partition and deleting in another,
		// THEN only the delete must happen in the new partition set, not the insert!
	case model.StateDeleteOnly:
		// This state is to confirm all servers can not see the new partitions when reorg is running,
		// so that all deletes will be done in both old and new partitions when in either DeleteOnly
		// or WriteOnly state.
		// Also using the state for checking that the optional TiFlash replica is available, making it
		// in a state without (much) data and easy to retry without side effects.

		// Reason for having it here, is to make it easy for retry, and better to make sure it is in-sync
		// as early as possible, to avoid a long wait after the data copying.
		if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
			// For available state, the new added partition should wait its replica to
			// be finished, otherwise the query to this partition will be blocked.
			count := tblInfo.TiFlashReplica.Count
			needRetry, err := checkPartitionReplica(count, addingDefinitions, d)
			if err != nil {
				// need to rollback, since we tried to register the new
				// partitions before!
				return convertAddTablePartitionJob2RollbackJob(d, t, job, err, tblInfo)
			}
			if needRetry {
				// The new added partition hasn't been replicated.
				// Do nothing to the job this time, wait next worker round.
				time.Sleep(tiflashCheckTiDBHTTPAPIHalfInterval)
				// Set the error here which will lead this job exit when it's retry times beyond the limitation.
				return ver, errors.Errorf("[ddl] add partition wait for tiflash replica to complete")
			}

			// When TiFlash Replica is ready, we must move them into `AvailablePartitionIDs`.
			// Since onUpdateFlashReplicaStatus cannot see the partitions yet (not public)
			for _, d := range addingDefinitions {
				tblInfo.TiFlashReplica.AvailablePartitionIDs = append(tblInfo.TiFlashReplica.AvailablePartitionIDs, d.ID)
			}
		}

		job.SchemaState = model.StateWriteOnly
		tblInfo.Partition.DDLState = model.StateWriteOnly
		metrics.GetBackfillProgressByLabel(metrics.LblReorgPartition, job.SchemaName, tblInfo.Name.String()).Set(0.2 / float64(math.MaxUint64))
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != job.SchemaState)
	case model.StateWriteOnly:
		// Insert this state to confirm all servers can see the new partitions when reorg is running,
		// so that new data will be updated in both old and new partitions when reorganizing.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
		tblInfo.Partition.DDLState = model.StateWriteReorganization
		metrics.GetBackfillProgressByLabel(metrics.LblReorgPartition, job.SchemaName, tblInfo.Name.String()).Set(0.3 / float64(math.MaxUint64))
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != job.SchemaState)
	case model.StateWriteReorganization:
		physicalTableIDs := getPartitionIDsFromDefinitions(tblInfo.Partition.DroppingDefinitions)
		tbl, err2 := getTable(d.store, job.SchemaID, tblInfo)
		if err2 != nil {
			return ver, errors.Trace(err2)
		}
		// TODO: If table has global indexes, we need reorg to clean up them.
		// and then add the new partition ids back...
		if _, ok := tbl.(table.PartitionedTable); ok && hasGlobalIndex(tblInfo) {
			err = errors.Trace(dbterror.ErrCancelledDDLJob.GenWithStack("global indexes is not supported yet for reorganize partition"))
			return convertAddTablePartitionJob2RollbackJob(d, t, job, err, tblInfo)
		}
		var done bool
		done, ver, err = doPartitionReorgWork(w, d, t, job, tbl, physicalTableIDs)

		if !done {
			return ver, err
		}

		firstPartIdx, lastPartIdx, idMap, err2 := getReplacedPartitionIDs(partNamesCIStr, tblInfo.Partition)
		failpoint.Inject("reorgPartWriteReorgReplacedPartIDsFail", func(val failpoint.Value) {
			if val.(bool) {
				err2 = errors.New("Injected error by reorgPartWriteReorgReplacedPartIDsFail")
			}
		})
		if err2 != nil {
			return ver, err2
		}
		newDefs := getReorganizedDefinitions(tblInfo.Partition, firstPartIdx, lastPartIdx, idMap)

		// From now on, use the new definitions, but keep the Adding and Dropping for double write
		tblInfo.Partition.Definitions = newDefs
		tblInfo.Partition.Num = uint64(len(newDefs))

		// Now all the data copying is done, but we cannot simply remove the droppingDefinitions
		// since they are a part of the normal Definitions that other nodes with
		// the current schema version. So we need to double write for one more schema version
		job.SchemaState = model.StateDeleteReorganization
		tblInfo.Partition.DDLState = model.StateDeleteReorganization
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != job.SchemaState)

	case model.StateDeleteReorganization:
		// Drop the droppingDefinitions and finish the DDL
		// This state is needed for the case where client A sees the schema
		// with version of StateWriteReorg and would not see updates of
		// client B that writes to the new partitions, previously
		// addingDefinitions, since it would not double write to
		// the droppingDefinitions during this time
		// By adding StateDeleteReorg state, client B will write to both
		// the new (previously addingDefinitions) AND droppingDefinitions

		// Register the droppingDefinitions ids for rangeDelete
		// and the addingDefinitions for handling in the updateSchemaVersion
		physicalTableIDs := getPartitionIDsFromDefinitions(tblInfo.Partition.DroppingDefinitions)
		newIDs := getPartitionIDsFromDefinitions(partInfo.Definitions)
		job.CtxVars = []interface{}{physicalTableIDs, newIDs}
		definitionsToDrop := tblInfo.Partition.DroppingDefinitions
		tblInfo.Partition.DroppingDefinitions = nil
		tblInfo.Partition.AddingDefinitions = nil
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		failpoint.Inject("reorgPartWriteReorgSchemaVersionUpdateFail", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.New("Injected error by reorgPartWriteReorgSchemaVersionUpdateFail")
			}
		})
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateNone
		tblInfo.Partition.DDLState = model.StateNone
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		// How to handle this?
		// Seems to only trigger asynchronous update of statistics.
		// Should it actually be synchronous?
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionReorganizePartition, TableInfo: tblInfo, PartInfo: &model.PartitionInfo{Definitions: definitionsToDrop}})
		// A background job will be created to delete old partition data.
		job.Args = []interface{}{physicalTableIDs}

	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}

	return ver, errors.Trace(err)
}

func doPartitionReorgWork(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job, tbl table.Table, physTblIDs []int64) (done bool, ver int64, err error) {
	job.ReorgMeta.ReorgTp = model.ReorgTypeTxn
	sctx, err1 := w.sessPool.get()
	if err1 != nil {
		return done, ver, err1
	}
	defer w.sessPool.put(sctx)
	rh := newReorgHandler(newSession(sctx))
	elements := BuildElements(tbl.Meta().Columns[0], tbl.Meta().Indices)
	partTbl, ok := tbl.(table.PartitionedTable)
	if !ok {
		return false, ver, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
	}
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfoFromPartitions(d.jobContext(job.ID), d, rh, job, dbInfo, partTbl, physTblIDs, elements)
	err = w.runReorgJob(rh, reorgInfo, tbl.Meta(), d.lease, func() (reorgErr error) {
		defer tidbutil.Recover(metrics.LabelDDL, "doPartitionReorgWork",
			func() {
				reorgErr = dbterror.ErrCancelledDDLJob.GenWithStack("reorganize partition for table `%v` panic", tbl.Meta().Name)
			}, false)
		return w.reorgPartitionDataAndIndex(tbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// If timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		if kv.IsTxnRetryableError(err) {
			return false, ver, errors.Trace(err)
		}
		if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
			logutil.BgLogger().Warn("[ddl] reorg partition job failed, RemoveDDLReorgHandle failed, can't convert job to rollback",
				zap.String("job", job.String()), zap.Error(err1))
		}
		logutil.BgLogger().Warn("[ddl] reorg partition job failed, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
		ver, err = convertAddTablePartitionJob2RollbackJob(d, t, job, err, tbl.Meta())
		return false, ver, errors.Trace(err)
	}
	return true, ver, err
}

type reorgPartitionWorker struct {
	*backfillCtx
	metricCounter prometheus.Counter

	// Static allocated to limit memory allocations
	rowRecords        []*rowRecord
	rowDecoder        *decoder.RowDecoder
	rowMap            map[int64]types.Datum
	writeColOffsetMap map[int64]int
	maxOffset         int
	reorgedTbl        table.PartitionedTable

	jobContext *JobContext
}

func newReorgPartitionWorker(sessCtx sessionctx.Context, i int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *JobContext) (*reorgPartitionWorker, error) {
	reorgedTbl, err := tables.GetReorganizedPartitionedTable(t)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pt := t.GetPartitionedTable()
	if pt == nil {
		return nil, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
	}
	partColIDs := pt.GetPartitionColumnIDs()
	writeColOffsetMap := make(map[int64]int, len(partColIDs))
	maxOffset := 0
	for _, col := range pt.Cols() {
		found := false
		for _, id := range partColIDs {
			if col.ID == id {
				found = true
				break
			}
		}
		if !found {
			continue
		}
		writeColOffsetMap[col.ID] = col.Offset
		maxOffset = mathutil.Max[int](maxOffset, col.Offset)
	}
	return &reorgPartitionWorker{
		backfillCtx:       newBackfillCtx(reorgInfo.d, i, sessCtx, reorgInfo.ReorgMeta.ReorgTp, reorgInfo.SchemaName, t, false),
		metricCounter:     metrics.BackfillTotalCounter.WithLabelValues(metrics.GenerateReorgLabel("reorg_partition_rate", reorgInfo.SchemaName, t.Meta().Name.String())),
		rowDecoder:        decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap),
		rowMap:            make(map[int64]types.Datum, len(decodeColMap)),
		jobContext:        jc,
		writeColOffsetMap: writeColOffsetMap,
		maxOffset:         maxOffset,
		reorgedTbl:        reorgedTbl,
	}, nil
}

func (w *reorgPartitionWorker) GetTasks() ([]*BackfillJob, error) {
	panic("[ddl] reorg partition worker GetTask function doesn't implement")
}

func (w *reorgPartitionWorker) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceType(context.Background(), w.jobContext.ddlJobSourceType())
	errInTxn = kv.RunInNewTxn(ctx, w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(handleRange.getJobID()); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

		rowRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		warningsMap := make(map[errors.ErrorID]*terror.Error)
		warningsCountMap := make(map[errors.ErrorID]int64)
		for _, prr := range rowRecords {
			taskCtx.scanCount++

			err = txn.Set(prr.key, prr.vals)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
			if prr.warning != nil {
				if _, ok := warningsCountMap[prr.warning.ID()]; ok {
					warningsCountMap[prr.warning.ID()]++
				} else {
					warningsCountMap[prr.warning.ID()] = 1
					warningsMap[prr.warning.ID()] = prr.warning
				}
			}
			// TODO: Future optimization: also write the indexes here?
			// What if the transaction limit is just enough for a single row, without index?
			// Hmm, how could that be in the first place?
			// For now, implement the batch-txn w.addTableIndex,
			// since it already exists and is in use
		}

		// Collect the warnings.
		taskCtx.warnings, taskCtx.warningsCount = warningsMap, warningsCountMap

		// also add the index entries here? And make sure they are not added somewhere else

		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "BackfillDataInTxn", 3000)

	return
}

func (w *reorgPartitionWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgBackfillTask) ([]*rowRecord, kv.Key, bool, error) {
	w.rowRecords = w.rowRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	sysTZ := w.sessCtx.GetSessionVars().StmtCtx.TimeZone

	tmpRow := make([]types.Datum, w.maxOffset+1)
	var lastAccessedHandle kv.Key
	oprStartTime := startTime
	err := iterateSnapshotKeys(w.GetCtx().jobContext(taskRange.getJobID()), w.sessCtx.GetStore(), taskRange.priority, w.table.RecordPrefix(), txn.StartTS(), taskRange.startKey, taskRange.endKey,
		func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotKeys in reorgPartitionWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			if taskRange.endInclude {
				taskDone = recordKey.Cmp(taskRange.endKey) > 0
			} else {
				taskDone = recordKey.Cmp(taskRange.endKey) >= 0
			}

			if taskDone || len(w.rowRecords) >= w.batchCnt {
				return false, nil
			}

			_, err := w.rowDecoder.DecodeTheExistedColumnMap(w.sessCtx, handle, rawRow, sysTZ, w.rowMap)
			if err != nil {
				return false, errors.Trace(err)
			}

			// Set the partitioning columns and calculate which partition to write to
			for colID, offset := range w.writeColOffsetMap {
				if d, ok := w.rowMap[colID]; ok {
					tmpRow[offset] = d
				} else {
					return false, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
				}
			}
			p, err := w.reorgedTbl.GetPartitionByRow(w.sessCtx, tmpRow)
			if err != nil {
				return false, errors.Trace(err)
			}
			pid := p.GetPhysicalID()
			newKey := tablecodec.EncodeTablePrefix(pid)
			newKey = append(newKey, recordKey[len(newKey):]...)
			w.rowRecords = append(w.rowRecords, &rowRecord{
				key: newKey, vals: rawRow,
			})

			w.cleanRowMap()
			lastAccessedHandle = recordKey
			if recordKey.Cmp(taskRange.endKey) == 0 {
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.rowRecords) == 0 {
		taskDone = true
	}

	logutil.BgLogger().Debug("[ddl] txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()), zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.rowRecords, getNextHandleKey(taskRange, taskDone, lastAccessedHandle), taskDone, errors.Trace(err)
}

func (w *reorgPartitionWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

func (w *reorgPartitionWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

func (w *reorgPartitionWorker) String() string {
	return typeReorgPartitionWorker.String()
}

func (w *reorgPartitionWorker) GetTask() (*BackfillJob, error) {
	panic("[ddl] partition reorg worker does not implement GetTask function")
}

func (w *reorgPartitionWorker) UpdateTask(*BackfillJob) error {
	panic("[ddl] partition reorg worker does not implement UpdateTask function")
}

func (w *reorgPartitionWorker) FinishTask(*BackfillJob) error {
	panic("[ddl] partition reorg worker does not implement FinishTask function")
}

func (w *reorgPartitionWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
}

func (w *worker) reorgPartitionDataAndIndex(t table.Table, reorgInfo *reorgInfo) error {
	// First copy all table data to the new partitions
	// from each of the DroppingDefinitions partitions.
	// Then create all indexes on the AddingDefinitions partitions
	// for each new index, one partition at a time.

	// Copy the data from the DroppingDefinitions to the AddingDefinitions
	if bytes.Equal(reorgInfo.currElement.TypeKey, meta.ColumnElementKey) {
		err := w.updatePhysicalTableRow(t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}

	failpoint.Inject("reorgPartitionAfterDataCopy", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test in reorgPartitionAfterDataCopy")
		}
	})

	// Rewrite this to do all indexes at once in addTableIndex
	// instead of calling it once per index (meaning reading the table multiple times)
	// But for now, try to understand how it works...
	firstNewPartitionID := t.Meta().Partition.AddingDefinitions[0].ID
	startElementOffset := 0
	//startElementOffsetToResetHandle := -1
	// This backfill job starts with backfilling index data, whose index ID is currElement.ID.
	if !bytes.Equal(reorgInfo.currElement.TypeKey, meta.IndexElementKey) {
		// First run, have not yet started backfilling index data
		// Restart with the first new partition.
		// TODO: handle remove partitioning
		reorgInfo.PhysicalTableID = firstNewPartitionID
	} else {
		// The job was interrupted and has been restarted,
		// reset and start from where it was done
		for i, element := range reorgInfo.elements[1:] {
			if reorgInfo.currElement.ID == element.ID {
				startElementOffset = i
				//startElementOffsetToResetHandle = i
				break
			}
		}
	}

	for i := startElementOffset; i < len(reorgInfo.elements[1:]); i++ {
		// Now build the indexes in the new partitions
		var physTbl table.PhysicalTable
		if tbl, ok := t.(table.PartitionedTable); ok {
			physTbl = tbl.GetPartition(reorgInfo.PhysicalTableID)
		} else if tbl, ok := t.(table.PhysicalTable); ok {
			// This may be used when partitioning a non-partitioned table
			physTbl = tbl
		}
		// Get the original start handle and end handle.
		currentVer, err := getValidCurrentVersion(reorgInfo.d.store)
		if err != nil {
			return errors.Trace(err)
		}
		// TODO: Can we improve this in case of a crash?
		// like where the regInfo PhysicalTableID and element is the same,
		// and the tableid in the key-prefix regInfo.StartKey and regInfo.EndKey matches with PhysicalTableID
		// do not change the reorgInfo start/end key
		startHandle, endHandle, err := getTableRange(reorgInfo.d.jobContext(reorgInfo.Job.ID), reorgInfo.d, physTbl, currentVer.Ver, reorgInfo.Job.Priority)
		if err != nil {
			return errors.Trace(err)
		}

		// Always (re)start with the full PhysicalTable range
		reorgInfo.StartKey, reorgInfo.EndKey = startHandle, endHandle

		// Update the element in the reorgCtx to keep the atomic access for daemon-worker.
		w.getReorgCtx(reorgInfo.Job.ID).setCurrentElement(reorgInfo.elements[i+1])

		// Update the element in the reorgInfo for updating the reorg meta below.
		reorgInfo.currElement = reorgInfo.elements[i+1]
		// Write the reorg info to store so the whole reorganize process can recover from panic.
		err = reorgInfo.UpdateReorgMeta(reorgInfo.StartKey, w.sessPool)
		logutil.BgLogger().Info("[ddl] update column and indexes",
			zap.Int64("jobID", reorgInfo.Job.ID),
			zap.ByteString("elementType", reorgInfo.currElement.TypeKey),
			zap.Int64("elementID", reorgInfo.currElement.ID),
			zap.Int64("partitionTableId", physTbl.GetPhysicalID()),
			zap.String("startHandle", hex.EncodeToString(reorgInfo.StartKey)),
			zap.String("endHandle", hex.EncodeToString(reorgInfo.EndKey)))
		if err != nil {
			return errors.Trace(err)
		}
		err = w.addTableIndex(t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
		reorgInfo.PhysicalTableID = firstNewPartitionID
	}
	failpoint.Inject("reorgPartitionAfterIndex", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test in reorgPartitionAfterIndex")
		}
	})
	return nil
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
		} else {
			sql, paramList = buildCheckSQLForRangeColumnsPartition(pi, index, schemaName, tableName)
		}
	case model.PartitionTypeList:
		if len(pi.Columns) == 0 {
			sql, paramList = buildCheckSQLForListPartition(pi, index, schemaName, tableName)
		} else {
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

	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(w.ctx, nil, sql, paramList...)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := len(rows)
	if rowCount != 0 {
		return errors.Trace(dbterror.ErrRowDoesNotMatchPartition)
	}
	return nil
}

func checkExchangePartitionPlacementPolicy(t *meta.Meta, ntPlacementPolicyRef *model.PolicyRefInfo, ptPlacementPolicyRef *model.PolicyRefInfo) error {
	if ntPlacementPolicyRef == nil && ptPlacementPolicyRef == nil {
		return nil
	}
	if ntPlacementPolicyRef == nil || ptPlacementPolicyRef == nil {
		return dbterror.ErrTablesDifferentMetadata
	}

	ptPlacementPolicyInfo, _ := getPolicyInfo(t, ptPlacementPolicyRef.ID)
	ntPlacementPolicyInfo, _ := getPolicyInfo(t, ntPlacementPolicyRef.ID)
	if ntPlacementPolicyInfo == nil && ptPlacementPolicyInfo == nil {
		return nil
	}
	if ntPlacementPolicyInfo == nil || ptPlacementPolicyInfo == nil {
		return dbterror.ErrTablesDifferentMetadata
	}
	if ntPlacementPolicyInfo.Name.L != ptPlacementPolicyInfo.Name.L {
		return dbterror.ErrTablesDifferentMetadata
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
	return strings.Trim(str, "'")
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

	partCols, err := getPartitionColSlices(sctx, tblInfo, s.Partition)
	if err != nil {
		return errors.Trace(err)
	}

	// Checks that the partitioning key is included in the constraint.
	// Every unique key on the table must use every column in the table's partitioning expression.
	// See https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	for _, index := range tblInfo.Indices {
		if index.Unique && !checkUniqueKeyIncludePartKey(partCols, index.Columns) {
			if index.Primary {
				// not support global index with clustered index
				if tblInfo.IsCommonHandle {
					return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("CLUSTERED INDEX")
				}
				if !config.GetGlobalConfig().EnableGlobalIndex {
					return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY KEY")
				}
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

func isPartExprUnsigned(tbInfo *model.TableInfo) bool {
	// We should not rely on any configuration, system or session variables, so use a mock ctx!
	// Same as in tables.newPartitionExpr
	ctx := mock.NewContext()
	expr, err := expression.ParseSimpleExprWithTableInfo(ctx, tbInfo.Partition.Expr, tbInfo)
	if err != nil {
		logutil.BgLogger().Error("isPartExpr failed parsing expression!", zap.Error(err))
		return false
	}
	if mysql.HasUnsignedFlag(expr.GetType().GetFlag()) {
		return true
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
			if strconv.IsPrint(runeVal) {
				res += string(runeVal)
			} else {
				isPrint = false
				break
			}
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
	if partitionInfo.Type == model.PartitionTypeHash ||
		partitionInfo.Type == model.PartitionTypeKey {
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
			if partitionInfo.Type == model.PartitionTypeHash {
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
		if partitionInfo.Type == model.PartitionTypeKey {
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
		if partitionInfo.Type == model.PartitionTypeRange {
			lessThans := make([]string, len(def.LessThan))
			for idx, v := range def.LessThan {
				lessThans[idx] = hexIfNonPrint(v)
			}
			fmt.Fprintf(buf, " VALUES LESS THAN (%s)", strings.Join(lessThans, ","))
		} else if partitionInfo.Type == model.PartitionTypeList {
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
		if len(def.Comment) > 0 {
			buf.WriteString(fmt.Sprintf(" COMMENT '%s'", format.OutputFormat(def.Comment)))
		}
		if def.PlacementPolicyRef != nil {
			// add placement ref info here
			fmt.Fprintf(buf, " /*T![placement] PLACEMENT POLICY=%s */", stringutil.Escape(def.PlacementPolicyRef.Name.O, sqlMode))
		}
	}
}
