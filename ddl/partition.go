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

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	partitionMaxValue = "MAXVALUE"
)

func checkAddPartition(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.PartitionInfo, []model.PartitionDefinition, error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
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
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
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
			needWait, err := checkPartitionReplica(addingDefinitions, d)
			if err != nil {
				ver, err = convertAddTablePartitionJob2RollbackJob(t, job, err, tblInfo)
				return ver, err
			}
			if needWait {
				// The new added partition hasn't been replicated.
				// Do nothing to the job this time, wait next worker round.
				time.Sleep(tiflashCheckTiDBHTTPAPIHalfInterval)
				return ver, nil
			}
		}

		// For normal and replica finished table, move the `addingDefinitions` into `Definitions`.
		updatePartitionInfo(tblInfo)

		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionAddTablePartition, TableInfo: tblInfo, PartInfo: partInfo})
	default:
		err = ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}

	return ver, errors.Trace(err)
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
func rollbackAddingPartitionInfo(tblInfo *model.TableInfo) []int64 {
	physicalTableIDs := make([]int64, 0, len(tblInfo.Partition.AddingDefinitions))
	for _, one := range tblInfo.Partition.AddingDefinitions {
		physicalTableIDs = append(physicalTableIDs, one.ID)
	}
	tblInfo.Partition.AddingDefinitions = nil
	return physicalTableIDs
}

// checkAddPartitionValue values less than value must be strictly increasing for each partition.
func checkAddPartitionValue(meta *model.TableInfo, part *model.PartitionInfo) error {
	if meta.Partition.Type == model.PartitionTypeRange && len(meta.Partition.Columns) == 0 {
		newDefs, oldDefs := part.Definitions, meta.Partition.Definitions
		rangeValue := oldDefs[len(oldDefs)-1].LessThan[0]
		if strings.EqualFold(rangeValue, "MAXVALUE") {
			return errors.Trace(ErrPartitionMaxvalue)
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
				return errors.Trace(ErrPartitionMaxvalue)
			}

			nextRangeValue, err := strconv.Atoi(newDefs[i].LessThan[0])
			if err != nil {
				return errors.Trace(err)
			}
			if nextRangeValue <= currentRangeValue {
				return errors.Trace(ErrRangeNotIncreasing)
			}
			currentRangeValue = nextRangeValue
		}
	}
	return nil
}

func checkPartitionReplica(addingDefinitions []model.PartitionDefinition, d *ddlCtx) (needWait bool, err error) {
	ctx := context.Background()
	pdCli := d.store.(tikv.Storage).GetRegionCache().PDClient()
	stores, err := pdCli.GetAllStores(ctx)
	if err != nil {
		return needWait, errors.Trace(err)
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
		if label.Key == "engine" && label.Value == "tiflash" {
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
		ctx.GetSessionVars().StmtCtx.AppendWarning(errTablePartitionDisabled)
		return nil
	}

	var enable bool
	// When tidb_enable_table_partition is 'on' or 'auto'.
	if s.Tp == model.PartitionTypeRange {
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
	}
	// Partition by hash is enabled by default.
	// Note that linear hash is not enabled.
	if s.Tp == model.PartitionTypeHash {
		if !s.Linear && s.Sub == nil {
			enable = true
		}
	}
	if s.Tp == model.PartitionTypeList {
		enable = true
	}

	if !enable {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errUnsupportedCreatePartition)
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
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
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
func buildPartitionDefinitionsInfo(ctx sessionctx.Context, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) ([]model.PartitionDefinition, error) {
	switch tbInfo.Partition.Type {
	case model.PartitionTypeRange:
		return buildRangePartitionDefinitions(ctx, defs, tbInfo)
	case model.PartitionTypeHash:
		return buildHashPartitionDefinitions(ctx, defs, tbInfo)
	case model.PartitionTypeList:
		return buildListPartitionDefinitions(ctx, defs, tbInfo)
	}
	return nil, nil
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
		}
	}
	return definitions, nil
}

func buildListPartitionDefinitions(ctx sessionctx.Context, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) ([]model.PartitionDefinition, error) {
	definitions := make([]model.PartitionDefinition, 0, len(defs))
	exprChecker := newPartitionExprChecker(ctx, nil, checkPartitionExprAllowed, checkPartitionExprFuncAllowed)
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
				if err := checkPartitionValuesIsInt(ctx, def, vs); err != nil {
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
	exprChecker := newPartitionExprChecker(ctx, nil, checkPartitionExprAllowed, checkPartitionExprFuncAllowed)
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
			if err := checkPartitionValuesIsInt(ctx, def, clause.Exprs); err != nil {
				return nil, err
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

func checkPartitionValuesIsInt(ctx sessionctx.Context, def *ast.PartitionDefinition, exprs []ast.ExprNode) error {
	for _, exp := range exprs {
		if _, ok := exp.(*ast.MaxValueExpr); ok {
			continue
		}
		val, err := expression.EvalAstExpr(ctx, exp)
		if err != nil {
			return err
		}
		switch val.Kind() {
		case types.KindInt64, types.KindUint64, types.KindNull:
		default:
			return ErrValuesIsNotIntType.GenWithStackByArgs(def.Name)
		}
	}

	return nil
}

func checkPartitionNameUnique(pi *model.PartitionInfo) error {
	newPars := pi.Definitions
	partNames := make(map[string]struct{}, len(newPars))
	for _, newPar := range newPars {
		if _, ok := partNames[newPar.Name.L]; ok {
			return ErrSameNamePartition.GenWithStackByArgs(newPar.Name)
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
			return ErrSameNamePartition.GenWithStackByArgs(newPar.Name)
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
		return ErrRepairTableFail.GenWithStackByArgs("Old table doesn't have partitions")
	}
	if newTableInfo.Partition.Type != oldTableInfo.Partition.Type {
		return ErrRepairTableFail.GenWithStackByArgs("Partition type should be the same")
	}
	// Check whether partitionType is hash partition.
	if newTableInfo.Partition.Type == model.PartitionTypeHash {
		if newTableInfo.Partition.Num != oldTableInfo.Partition.Num {
			return ErrRepairTableFail.GenWithStackByArgs("Hash partition num should be the same")
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
			return ErrRepairTableFail.GenWithStackByArgs("Partition " + newOne.Name.L + " has lost")
		}
	}
	return nil
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	// Accelerate the compare by eliminate index bound check.
	b = b[:len(a)]
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// checkPartitionFuncValid checks partition function validly.
func checkPartitionFuncValid(ctx sessionctx.Context, tblInfo *model.TableInfo, expr ast.ExprNode) error {
	if expr == nil {
		return nil
	}
	exprChecker := newPartitionExprChecker(ctx, tblInfo, checkPartitionExprArgs, checkPartitionExprAllowed, checkPartitionExprFuncAllowed)
	expr.Accept(exprChecker)
	if exprChecker.err != nil {
		return errors.Trace(exprChecker.err)
	}
	if len(exprChecker.columns) == 0 {
		return errors.Trace(ErrWrongExprInPartitionFunc)
	}
	return nil
}

// checkResultOK derives from https://github.com/mysql/mysql-server/blob/5.7/sql/item_timefunc
// For partition tables, mysql do not support Constant, random or timezone-dependent expressions
// Based on mysql code to check whether field is valid, every time related type has check_valid_arguments_processor function.
func checkResultOK(ok bool, err error) error {
	if err != nil {
		return err
	}

	if !ok {
		return errors.Trace(ErrWrongExprInPartitionFunc)
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
		return errors.Trace(ErrNotAllowedTypeInPartition.GenWithStackByArgs(col.Name.Name.L))
	}

	return errors.Trace(ErrPartitionFuncNotAllowed.GenWithStackByArgs("PARTITION"))
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
	isUnsignedBigint := isRangePartitionColUnsignedBigint(cols, pi)
	var prevRangeValue interface{}
	for i := 0; i < len(defs); i++ {
		if strings.EqualFold(defs[i].LessThan[0], partitionMaxValue) {
			return errors.Trace(ErrPartitionMaxvalue)
		}

		currentRangeValue, fromExpr, err := getRangeValue(ctx, defs[i].LessThan[0], isUnsignedBigint)
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

		if isUnsignedBigint {
			if currentRangeValue.(uint64) <= prevRangeValue.(uint64) {
				return errors.Trace(ErrRangeNotIncreasing)
			}
		} else {
			if currentRangeValue.(int64) <= prevRangeValue.(int64) {
				return errors.Trace(ErrRangeNotIncreasing)
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
			return errors.Trace(ErrMultipleDefConstInListPart)
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
		if isRangePartitionColUnsignedBigint(tblInfo.Columns, tblInfo.Partition) {
			tp.Flag |= mysql.UnsignedFlag
		}
		colTps = []*types.FieldType{tp}
	} else {
		colTps = make([]*types.FieldType, 0, len(pi.Columns))
		for _, colName := range pi.Columns {
			colInfo := findColumnByName(colName.L, tblInfo)
			if colInfo == nil {
				return nil, errors.Trace(ErrFieldNotFoundPart)
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
					s = string(hack.String(collate.GetCollator(cols[k].Collate).Key(s)))
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
func getRangeValue(ctx sessionctx.Context, str string, unsignedBigint bool) (interface{}, bool, error) {
	// Unsigned bigint was converted to uint64 handle.
	if unsignedBigint {
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
	return 0, false, ErrNotAllowedTypeInPartition.GenWithStackByArgs(str)
}

// validRangePartitionType checks the type supported by the range partitioning key.
func validRangePartitionType(col *model.ColumnInfo) bool {
	switch col.FieldType.EvalType() {
	case types.ETInt:
		return true
	default:
		return false
	}
}

// checkDropTablePartition checks if the partition exists and does not allow deleting the last existing partition in the table.
func checkDropTablePartition(meta *model.TableInfo, partLowerNames []string) error {
	pi := meta.Partition
	if pi.Type != model.PartitionTypeRange && pi.Type != model.PartitionTypeList {
		return errOnlyOnRangeListPartition.GenWithStackByArgs("DROP")
	}
	oldDefs := pi.Definitions
	for _, pn := range partLowerNames {
		found := false
		for _, def := range oldDefs {
			if def.Name.L == pn {
				found = true
				break
			}
		}
		if !found {
			return errors.Trace(ErrDropPartitionNonExistent.GenWithStackByArgs(pn))
		}
	}
	if len(oldDefs) == len(partLowerNames) {
		return errors.Trace(ErrDropLastPartition)
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

func dropRuleBundles(d *ddlCtx, physicalTableIDs []int64) error {
	if d.infoHandle != nil && d.infoHandle.IsValid() {
		bundles := make([]*placement.Bundle, 0, len(physicalTableIDs))
		for _, ID := range physicalTableIDs {
			oldBundle, ok := d.infoHandle.Get().BundleByName(placement.GroupID(ID))
			if ok && !oldBundle.IsEmpty() {
				bundles = append(bundles, placement.BuildPlacementDropBundle(ID))
			}
		}
		err := infosync.PutRuleBundles(nil, bundles)
		return err
	}
	return nil
}

// onDropTablePartition deletes old partition meta.
func (w *worker) onDropTablePartition(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var partNames []string
	if err := job.DecodeArgs(&partNames); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	var physicalTableIDs []int64
	if job.Type == model.ActionAddTablePartition {
		// It is rollbacked from adding table partition, just remove addingDefinitions from tableInfo.
		physicalTableIDs = rollbackAddingPartitionInfo(tblInfo)
		err = dropRuleBundles(d, physicalTableIDs)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
		}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		job.Args = []interface{}{physicalTableIDs}
		return ver, nil
	}

	if job.State == model.JobStateRunning && job.SchemaState == model.StateNone {
		// Manually set first state.
		job.SchemaState = model.StatePublic
	}
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
		err = dropRuleBundles(d, physicalTableIDs)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
		}
		updateDroppingPartitionInfo(tblInfo, partNames)
		job.SchemaState = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != job.SchemaState)
	case model.StateDeleteOnly:
		// This state is not a real 'DeleteOnly' state, because tidb does not maintaining the state check in partitionDefinition.
		// Insert this state to confirm all servers can not see the old partitions when reorg is running,
		// so that no new data will be inserted into old partitions when reorganizing.
		job.SchemaState = model.StateDeleteReorganization
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != job.SchemaState)
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
			reorgInfo, err := getReorgInfoFromPartitions(d, t, job, tbl, physicalTableIDs, elements)

			if err != nil || reorgInfo.first {
				// If we run reorg firstly, we should update the job snapshot version
				// and then run the reorg next time.
				return ver, errors.Trace(err)
			}
			err = w.runReorgJob(t, reorgInfo, tbl.Meta(), d.lease, func() (dropIndexErr error) {
				defer tidbutil.Recover(metrics.LabelDDL, "onDropTablePartition",
					func() {
						dropIndexErr = errCancelledDDLJob.GenWithStack("drop partition panic")
					}, false)
				return w.cleanupGlobalIndexes(pt, physicalTableIDs, reorgInfo)
			})
			if err != nil {
				if errWaitReorgTimeout.Equal(err) {
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
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		// A background job will be created to delete old partition data.
		job.Args = []interface{}{physicalTableIDs}
	default:
		err = ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
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
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return ver, errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	newPartitions := make([]model.PartitionDefinition, 0, len(oldIDs))
	for _, oldID := range oldIDs {
		for i := 0; i < len(pi.Definitions); i++ {
			def := &pi.Definitions[i]
			if def.ID == oldID {
				pid, err1 := t.GenGlobalID()
				if err != nil {
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
		return ver, table.ErrUnknownPartition.GenWithStackByArgs("drop?", tblInfo.Name.O)
	}

	// Clear the tiflash replica available status.
	if tblInfo.TiFlashReplica != nil {
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

	if d.infoHandle != nil && d.infoHandle.IsValid() {
		bundles := make([]*placement.Bundle, 0, len(oldIDs))

		yoldIDs := make([]int64, 0, len(oldIDs))
		newIDs := make([]int64, 0, len(oldIDs))
		for i, oldID := range oldIDs {
			oldBundle, ok := d.infoHandle.Get().BundleByName(placement.GroupID(oldID))
			if ok && !oldBundle.IsEmpty() {
				yoldIDs = append(yoldIDs, oldID)
				newIDs = append(newIDs, newIDs[i])
				bundles = append(bundles, placement.BuildPlacementDropBundle(oldID))
				bundles = append(bundles, placement.BuildPlacementCopyBundle(oldBundle, newIDs[i]))
			}
		}
		job.CtxVars = []interface{}{yoldIDs, newIDs}

		err = infosync.PutRuleBundles(nil, bundles)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
		}
	}

	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
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

	nt, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
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
		return ver, ErrInvalidDDLState.GenWithStack("table %s is not in public, but %s", pt.Name, pt.State)
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

	// partition table base auto id
	ptBaseID, err := t.GetAutoTableID(ptSchemaID, pt.ID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ptRandID, err := t.GetAutoRandomID(ptSchemaID, pt.ID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// non-partition table base auto id
	ntBaseID, err := t.GetAutoTableID(job.SchemaID, nt.ID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ntRandID, err := t.GetAutoRandomID(job.SchemaID, nt.ID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	_, partDef, err := getPartitionDef(pt, partName)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tempID := partDef.ID
	// exchange table meta id
	partDef.ID = nt.ID

	if pt.TiFlashReplica != nil {
		for i, id := range pt.TiFlashReplica.AvailablePartitionIDs {
			if id == tempID {
				pt.TiFlashReplica.AvailablePartitionIDs[i] = partDef.ID
				break
			}
		}
	}

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
	err = t.DropTableOrView(job.SchemaID, nt.ID, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	nt.ID = tempID

	err = t.CreateTableOrView(job.SchemaID, nt)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// both pt and nt set the maximum auto_id between ntBaseID and ptBaseID
	if ntBaseID > ptBaseID {
		_, err = t.GenAutoTableID(ptSchemaID, pt.ID, ntBaseID-ptBaseID)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	_, err = t.GenAutoTableID(job.SchemaID, nt.ID, mathutil.MaxInt64(ptBaseID, ntBaseID))
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if ntRandID != 0 || ptRandID != 0 {
		if ntRandID > ptRandID {
			_, err = t.GenAutoRandomID(ptSchemaID, pt.ID, ntRandID-ptRandID)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}

		_, err = t.GenAutoRandomID(job.SchemaID, nt.ID, mathutil.MaxInt64(ptRandID, ntRandID))
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StateNone, ver, pt)
	return ver, nil
}

func checkExchangePartitionRecordValidation(w *worker, pt *model.TableInfo, index int, schemaName, tableName model.CIStr) error {
	var sql string

	pi := pt.Partition

	switch pi.Type {
	case model.PartitionTypeHash:
		if pi.Num == 1 {
			return nil
		}
		sql = fmt.Sprintf("select 1 from `%s`.`%s` where mod(%s, %d) != %d limit 1", schemaName.L, tableName.L, pi.Expr, pi.Num, index)
	case model.PartitionTypeRange:
		// Table has only one partition and has the maximum value
		if len(pi.Definitions) == 1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
			return nil
		}
		// For range expression and range columns
		if len(pi.Columns) == 0 {
			sql = buildCheckSQLForRangeExprPartition(pi, index, schemaName, tableName)
		} else if len(pi.Columns) == 1 {
			sql = buildCheckSQLForRangeColumnsPartition(pi, index, schemaName, tableName)
		}
	default:
		return errUnsupportedPartitionType.GenWithStackByArgs(pt.Name.O)
	}

	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := len(rows)
	if rowCount != 0 {
		return errors.Trace(ErrRowDoesNotMatchPartition)
	}
	return nil
}

func buildCheckSQLForRangeExprPartition(pi *model.PartitionInfo, index int, schemaName, tableName model.CIStr) string {
	if index == 0 {
		return fmt.Sprintf("select 1 from `%s`.`%s` where %s >= %s limit 1", schemaName.L, tableName.L, pi.Expr, pi.Definitions[index].LessThan[0])
	} else if index == len(pi.Definitions)-1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
		return fmt.Sprintf("select 1 from `%s`.`%s` where %s < %s limit 1", schemaName.L, tableName.L, pi.Expr, pi.Definitions[index-1].LessThan[0])
	} else {
		return fmt.Sprintf("select 1 from `%s`.`%s` where %s < %s or %s >= %s limit 1", schemaName.L, tableName.L, pi.Expr, pi.Definitions[index-1].LessThan[0], pi.Expr, pi.Definitions[index].LessThan[0])
	}
}

func buildCheckSQLForRangeColumnsPartition(pi *model.PartitionInfo, index int, schemaName, tableName model.CIStr) string {
	colName := pi.Columns[0].L
	if index == 0 {
		return fmt.Sprintf("select 1 from `%s`.`%s` where `%s` >= %s limit 1", schemaName.L, tableName.L, colName, pi.Definitions[index].LessThan[0])
	} else if index == len(pi.Definitions)-1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
		return fmt.Sprintf("select 1 from `%s`.`%s` where `%s` < %s limit 1", schemaName.L, tableName.L, colName, pi.Definitions[index-1].LessThan[0])
	} else {
		return fmt.Sprintf("select 1 from `%s`.`%s` where `%s` < %s or `%s` >= %s limit 1", schemaName.L, tableName.L, colName, pi.Definitions[index-1].LessThan[0], colName, pi.Definitions[index].LessThan[0])
	}
}

func checkAddPartitionTooManyPartitions(piDefs uint64) error {
	if piDefs > uint64(PartitionCountLimit) {
		return errors.Trace(ErrTooManyPartitions)
	}
	return nil
}

func checkNoHashPartitions(ctx sessionctx.Context, partitionNum uint64) error {
	if partitionNum == 0 {
		return ast.ErrNoParts.GenWithStackByArgs("partitions")
	}
	return nil
}

func checkNoRangePartitions(partitionNum int) error {
	if partitionNum == 0 {
		return ast.ErrPartitionsMustBeDefined.GenWithStackByArgs("RANGE")
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
				return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY KEY")
			}
			return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("UNIQUE INDEX")
		}
	}
	// when PKIsHandle, tblInfo.Indices will not contain the primary key.
	if tblInfo.PKIsHandle {
		indexCols := []*model.IndexColumn{{
			Name:   tblInfo.GetPkName(),
			Length: types.UnspecifiedLength,
		}}
		if !checkUniqueKeyIncludePartKey(partCols, indexCols) {
			return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY KEY")
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
		cne.err = ErrBadField.GenWithStackByArgs(c.Name.Name.O, "expression")
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
	stmts, _, err := parser.New().Parse(partExpr, "", "")
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

// isRangePartitionColUnsignedBigint returns true if the partitioning key column type is unsigned bigint type.
func isRangePartitionColUnsignedBigint(cols []*model.ColumnInfo, pi *model.PartitionInfo) bool {
	for _, col := range cols {
		isUnsigned := col.Tp == mysql.TypeLonglong && mysql.HasUnsignedFlag(col.Flag)
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

func onAlterTablePartition(t *meta.Meta, job *model.Job) (ver int64, err error) {
	var partitionID int64
	bundle := &placement.Bundle{}
	err = job.DecodeArgs(&partitionID, bundle)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, err
	}

	ptInfo := tblInfo.GetPartitionInfo()
	if ptInfo.GetNameByID(partitionID) == "" {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs("drop?", tblInfo.Name.O))
	}

	pstate := ptInfo.GetStateByID(partitionID)
	switch pstate {
	case model.StatePublic:
		ptInfo.SetStateByID(partitionID, model.StateGlobalTxnOnly)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateGlobalTxnOnly
	case model.StateGlobalTxnOnly:
		err = infosync.PutRuleBundles(nil, []*placement.Bundle{bundle})
		if err != nil {
			job.State = model.JobStateCancelled
			return 0, errors.Wrapf(err, "failed to notify PD the placement rules")
		}
		ptInfo.SetStateByID(partitionID, model.StatePublic)
		// used by ApplyDiff in updateSchemaVersion
		job.CtxVars = []interface{}{partitionID}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	}
	return ver, nil
}

type partitionExprProcessor func(sessionctx.Context, *model.TableInfo, ast.ExprNode) error

type partitionExprChecker struct {
	processors []partitionExprProcessor
	ctx        sessionctx.Context
	tbInfo     *model.TableInfo
	expr       ast.ExprNode
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
		return errors.Trace(ErrBadField.GenWithStackByArgs(columnNameExpr.Name.Name.L, "partition function"))
	}

	p.columns = append(p.columns, colInfo)
	return nil
}

func checkPartitionExprAllowed(_ sessionctx.Context, _ *model.TableInfo, e ast.ExprNode) error {
	switch v := e.(type) {
	case *ast.FuncCastExpr, *ast.CaseExpr, *ast.SubqueryExpr, *ast.WindowFuncExpr, *ast.RowExpr, *ast.DefaultExpr, *ast.ValuesExpr,
		*ast.SetCollationExpr:
		return errors.Trace(ErrPartitionFunctionIsNotAllowed)
	case *ast.BinaryOperationExpr:
		// The DIV operator (opcode.IntDiv) is also supported; the / operator ( opcode.Div ) is not permitted.
		// see https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations.html
		switch v.Op {
		case opcode.Or, opcode.And, opcode.Xor, opcode.LeftShift, opcode.RightShift, opcode.BitNeg, opcode.Div:
			return errors.Trace(ErrPartitionFunctionIsNotAllowed)
		}
	case *ast.UnaryOperationExpr:
		if v.Op == opcode.BitNeg {
			return errors.Trace(ErrPartitionFunctionIsNotAllowed)
		}
	}
	return nil
}

func checkPartitionExprFuncAllowed(_ sessionctx.Context, _ *model.TableInfo, e ast.ExprNode) error {
	expr, ok := e.(*ast.FuncCallExpr)
	if !ok {
		return nil
	}
	allowedFuncMap := map[string]struct{}{
		ast.ToDays: {}, ast.ToSeconds: {}, ast.DayOfMonth: {}, ast.Month: {}, ast.DayOfYear: {},
		ast.Quarter: {}, ast.YearWeek: {}, ast.Year: {}, ast.Weekday: {}, ast.DayOfWeek: {}, ast.Day: {},
		ast.Hour: {}, ast.Minute: {}, ast.Second: {}, ast.TimeToSec: {}, ast.MicroSecond: {},
		ast.UnixTimestamp: {}, ast.FromDays: {}, ast.Extract: {}, ast.Abs: {}, ast.Ceiling: {},
		ast.DateDiff: {}, ast.Floor: {}, ast.Mod: {},
	}
	if _, ok := allowedFuncMap[expr.FnName.L]; ok {
		return nil
	}
	return errors.Trace(ErrPartitionFunctionIsNotAllowed)
}

func checkPartitionExprArgs(_ sessionctx.Context, tblInfo *model.TableInfo, e ast.ExprNode) error {
	expr, ok := e.(*ast.FuncCallExpr)
	if !ok {
		return nil
	}
	switch expr.FnName.L {
	case ast.ToDays, ast.ToSeconds, ast.DayOfMonth, ast.Month, ast.DayOfYear, ast.Quarter, ast.YearWeek,
		ast.Year, ast.Weekday, ast.DayOfWeek, ast.Day:
		return errors.Trace(checkResultOK(hasDateArgs(tblInfo, expr)))
	case ast.Hour, ast.Minute, ast.Second, ast.TimeToSec, ast.MicroSecond:
		return errors.Trace(checkResultOK(hasTimeArgs(tblInfo, expr)))
	case ast.UnixTimestamp:
		return errors.Trace(checkResultOK(hasTimestampArgs(tblInfo, expr)))
	case ast.FromDays:
		if err := checkResultOK(hasDateArgs(tblInfo, expr)); err != nil {
			return errors.Trace(err)
		}
		return errors.Trace(checkResultOK(hasTimeArgs(tblInfo, expr)))
	case ast.Extract:
		switch expr.Args[0].(*ast.TimeUnitExpr).Unit {
		case ast.TimeUnitYear, ast.TimeUnitYearMonth, ast.TimeUnitQuarter, ast.TimeUnitMonth, ast.TimeUnitDay:
			return errors.Trace(checkResultOK(hasDateArgs(tblInfo, expr)))
		case ast.TimeUnitDayMicrosecond, ast.TimeUnitDayHour, ast.TimeUnitDayMinute, ast.TimeUnitDaySecond:
			return errors.Trace(checkResultOK(hasDatetimeArgs(tblInfo, expr)))
		case ast.TimeUnitHour, ast.TimeUnitHourMinute, ast.TimeUnitHourSecond, ast.TimeUnitMinute, ast.TimeUnitMinuteSecond,
			ast.TimeUnitSecond, ast.TimeUnitMicrosecond, ast.TimeUnitHourMicrosecond, ast.TimeUnitMinuteMicrosecond, ast.TimeUnitSecondMicrosecond:
			return errors.Trace(checkResultOK(hasTimeArgs(tblInfo, expr)))
		default:
			return errors.Trace(ErrWrongExprInPartitionFunc)
		}
	case ast.Abs, ast.Ceiling, ast.DateDiff, ast.Floor, ast.Mod:
		has, err := hasTimestampArgs(tblInfo, expr)
		if err != nil {
			return errors.Trace(err)
		}
		if has {
			return errors.Trace(ErrWrongExprInPartitionFunc)
		}
	}
	return nil
}

func hasDateArgs(tblInfo *model.TableInfo, expr *ast.FuncCallExpr) (bool, error) {
	return hasSpecifyArgs(tblInfo, expr, mysql.TypeDate, mysql.TypeDatetime)
}

func hasTimeArgs(tblInfo *model.TableInfo, expr *ast.FuncCallExpr) (bool, error) {
	return hasSpecifyArgs(tblInfo, expr, mysql.TypeDuration, mysql.TypeDatetime)
}

func hasTimestampArgs(tblInfo *model.TableInfo, expr *ast.FuncCallExpr) (bool, error) {
	return hasSpecifyArgs(tblInfo, expr, mysql.TypeTimestamp)
}

func hasDatetimeArgs(tblInfo *model.TableInfo, expr *ast.FuncCallExpr) (bool, error) {
	return hasSpecifyArgs(tblInfo, expr, mysql.TypeDatetime)
}

func hasSpecifyArgs(tblInfo *model.TableInfo, expr *ast.FuncCallExpr, ts ...byte) (bool, error) {
	for _, arg := range expr.Args {
		col, ok := arg.(*ast.ColumnNameExpr)
		if !ok {
			continue
		}
		columnInfo := findColumnByName(col.Name.Name.L, tblInfo)
		if columnInfo == nil {
			return false, errors.Trace(ErrBadField.GenWithStackByArgs(col.Name.Name.L, "partition function"))
		}
		for _, t := range ts {
			if columnInfo.Tp == t {
				return true, nil
			}
		}
	}
	return false, nil
}
