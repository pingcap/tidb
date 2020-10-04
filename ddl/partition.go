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

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
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

func onAddTablePartition(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	// Handle the rolling back job
	if job.IsRollingback() {
		ver, err := onDropTablePartition(t, job)
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
		// none -> replica only
		job.SchemaState = model.StateReplicaOnly
		// move the adding definition into tableInfo.
		updateAddingPartitionInfo(partInfo, tblInfo)
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, true)
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
		regions, _, err := pdCli.ScanRegions(ctx, startKey, endKey, -1)
		if err != nil {
			return needWait, errors.Trace(err)
		}
		// For every region in the partition, if it has some corresponding peers and
		// no pending peers, that means the replication has completed.
		for _, region := range regions {
			regionState, err := pdCli.GetRegionByID(ctx, region.Id)
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
			logutil.BgLogger().Info("[ddl] partition replicas check failed in replica-only DDL state", zap.Int64("pID", pd.ID), zap.Uint64("wait region ID", region.Id), zap.Bool("tiflash peer at least one", tiflashPeerAtLeastOne), zap.Time("check time", time.Now()))
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
func buildTablePartitionInfo(ctx sessionctx.Context, s *ast.CreateTableStmt) (*model.PartitionInfo, error) {
	if s.Partition == nil {
		return nil, nil
	}

	if ctx.GetSessionVars().EnableTablePartition == "off" {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errTablePartitionDisabled)
		return nil, nil
	}

	var enable bool
	// When tidb_enable_table_partition is 'on' or 'auto'.
	if s.Partition.Tp == model.PartitionTypeRange {
		if s.Partition.Sub == nil {
			// Partition by range expression is enabled by default.
			if s.Partition.ColumnNames == nil {
				enable = true
			}
			// Partition by range columns and just one column.
			if len(s.Partition.ColumnNames) == 1 {
				enable = true
			}
		}
	}
	// Partition by hash is enabled by default.
	// Note that linear hash is not enabled.
	if s.Partition.Tp == model.PartitionTypeHash {
		if !s.Partition.Linear && s.Partition.Sub == nil {
			enable = true
		}
	}

	if !enable {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errUnsupportedCreatePartition)
		return nil, nil
	}

	pi := &model.PartitionInfo{
		Type:   s.Partition.Tp,
		Enable: enable,
		Num:    s.Partition.Num,
	}
	if s.Partition.Expr != nil {
		buf := new(bytes.Buffer)
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
		if err := s.Partition.Expr.Restore(restoreCtx); err != nil {
			return nil, err
		}
		pi.Expr = buf.String()
	} else if s.Partition.ColumnNames != nil {
		// TODO: Support multiple columns for 'PARTITION BY RANGE COLUMNS'.
		if len(s.Partition.ColumnNames) != 1 {
			pi.Enable = false
			ctx.GetSessionVars().StmtCtx.AppendWarning(ErrUnsupportedPartitionByRangeColumns)
		}
		pi.Columns = make([]model.CIStr, 0, len(s.Partition.ColumnNames))
		for _, cn := range s.Partition.ColumnNames {
			pi.Columns = append(pi.Columns, cn.Name)
		}
	}

	if s.Partition.Tp == model.PartitionTypeRange {
		if err := buildRangePartitionDefinitions(ctx, s, pi); err != nil {
			return nil, errors.Trace(err)
		}
	} else if s.Partition.Tp == model.PartitionTypeHash {
		if err := buildHashPartitionDefinitions(ctx, s, pi); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return pi, nil
}

func buildHashPartitionDefinitions(ctx sessionctx.Context, s *ast.CreateTableStmt, pi *model.PartitionInfo) error {
	if err := checkAddPartitionTooManyPartitions(pi.Num); err != nil {
		return err
	}

	defs := make([]model.PartitionDefinition, pi.Num)
	for i := 0; i < len(defs); i++ {
		if len(s.Partition.Definitions) == 0 {
			defs[i].Name = model.NewCIStr(fmt.Sprintf("p%v", i))
		} else {
			def := s.Partition.Definitions[i]
			defs[i].Name = def.Name
			defs[i].Comment, _ = def.Comment()
		}
	}
	pi.Definitions = defs
	return nil
}

func buildRangePartitionDefinitions(ctx sessionctx.Context, s *ast.CreateTableStmt, pi *model.PartitionInfo) error {
	for _, def := range s.Partition.Definitions {
		comment, _ := def.Comment()
		piDef := model.PartitionDefinition{
			Name:    def.Name,
			Comment: comment,
		}

		buf := new(bytes.Buffer)
		// Range columns partitions support multi-column partitions.
		for _, expr := range def.Clause.(*ast.PartitionDefinitionClauseLessThan).Exprs {
			expr.Format(buf)
			piDef.LessThan = append(piDef.LessThan, buf.String())
			buf.Reset()
		}
		pi.Definitions = append(pi.Definitions, piDef)
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

// See https://github.com/mysql/mysql-server/blob/5.7/sql/item_func.h#L387
func hasTimestampField(ctx sessionctx.Context, tblInfo *model.TableInfo, expr ast.ExprNode) (bool, error) {
	partCols, err := checkPartitionColumns(tblInfo, expr)
	if err != nil {
		return false, err
	}

	for _, c := range partCols {
		if c.FieldType.Tp == mysql.TypeTimestamp {
			return true, nil
		}
	}

	return false, nil
}

// See https://github.com/mysql/mysql-server/blob/5.7/sql/item_func.h#L399
func hasDateField(ctx sessionctx.Context, tblInfo *model.TableInfo, expr ast.ExprNode) (bool, error) {
	partCols, err := checkPartitionColumns(tblInfo, expr)
	if err != nil {
		return false, err
	}

	for _, c := range partCols {
		if c.FieldType.Tp == mysql.TypeDate || c.FieldType.Tp == mysql.TypeDatetime {
			return true, nil
		}
	}

	return false, nil
}

// See https://github.com/mysql/mysql-server/blob/5.7/sql/item_func.h#L412
func hasTimeField(ctx sessionctx.Context, tblInfo *model.TableInfo, expr ast.ExprNode) (bool, error) {
	partCols, err := checkPartitionColumns(tblInfo, expr)
	if err != nil {
		return false, err
	}

	for _, c := range partCols {
		if c.FieldType.Tp == mysql.TypeDatetime || c.FieldType.Tp == mysql.TypeDuration {
			return true, nil
		}
	}

	return false, nil
}

// We assume the result of any function that has a TIMESTAMP argument to be
// timezone-dependent, since a TIMESTAMP value in both numeric and string
// contexts is interpreted according to the current timezone.
// The only exception is UNIX_TIMESTAMP() which returns the internal
// representation of a TIMESTAMP argument verbatim, and thus does not depend on
// the timezone.
// See https://github.com/mysql/mysql-server/blob/5.7/sql/item_func.h#L445
func defaultTimezoneDependent(ctx sessionctx.Context, tblInfo *model.TableInfo, expr ast.ExprNode) (bool, error) {
	v, err := hasTimestampField(ctx, tblInfo, expr)
	if err != nil {
		return false, err
	}

	return !v, nil
}

func checkPartitionFuncCallValid(ctx sessionctx.Context, tblInfo *model.TableInfo, expr *ast.FuncCallExpr) error {
	// We assume the result of any function that has a TIMESTAMP argument to be
	// timezone-dependent, since a TIMESTAMP value in both numeric and string
	// contexts is interpreted according to the current timezone.
	// The only exception is UNIX_TIMESTAMP() which returns the internal
	// representation of a TIMESTAMP argument verbatim, and thus does not depend on
	// the timezone.
	// See https://github.com/mysql/mysql-server/blob/5.7/sql/item_func.h#L445
	if expr.FnName.L != ast.UnixTimestamp {
		for _, arg := range expr.Args {
			if colName, ok := arg.(*ast.ColumnNameExpr); ok {
				col := findColumnByName(colName.Name.Name.L, tblInfo)
				if col == nil {
					return ErrBadField.GenWithStackByArgs(colName.Name.Name.O, "expression")
				}

				if ok && col.FieldType.Tp == mysql.TypeTimestamp {
					return errors.Trace(errWrongExprInPartitionFunc)
				}
			}
		}
	}

	// check function which allowed in partitioning expressions
	// see https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-limitations-functions.html
	switch expr.FnName.L {
	// Mysql don't allow creating partitions with expressions with non matching
	// arguments as a (sub)partitioning function,
	// but we want to allow such expressions when opening existing tables for
	// easier maintenance. This exception should be deprecated at some point in future so that we always throw an error.
	// See https://github.com/mysql/mysql-server/blob/5.7/sql/sql_partition.cc#L1072
	case ast.Day, ast.DayOfMonth, ast.DayOfWeek, ast.DayOfYear, ast.Month, ast.Quarter, ast.ToDays, ast.ToSeconds,
		ast.Weekday, ast.Year, ast.YearWeek:
		return checkResultOK(hasDateField(ctx, tblInfo, expr))
	case ast.Hour, ast.MicroSecond, ast.Minute, ast.Second, ast.TimeToSec:
		return checkResultOK(hasTimeField(ctx, tblInfo, expr))
	case ast.UnixTimestamp:
		if len(expr.Args) != 1 {
			return errors.Trace(errWrongExprInPartitionFunc)
		}
		col, err := expression.RewriteSimpleExprWithTableInfo(ctx, tblInfo, expr.Args[0])
		if err != nil {
			return errors.Trace(err)
		}
		if col.GetType().Tp != mysql.TypeTimestamp {
			return errors.Trace(errWrongExprInPartitionFunc)
		}
		return nil
	case ast.Abs, ast.Ceiling, ast.DateDiff, ast.Extract, ast.Floor, ast.Mod:
		for _, arg := range expr.Args {
			if err := checkPartitionExprValid(ctx, tblInfo, arg); err != nil {
				return err
			}
		}
		return nil
	}
	return errors.Trace(ErrPartitionFunctionIsNotAllowed)
}

// checkPartitionExprValid checks partition expression validly.
func checkPartitionExprValid(ctx sessionctx.Context, tblInfo *model.TableInfo, expr ast.ExprNode) error {
	switch v := expr.(type) {
	case *ast.FuncCastExpr, *ast.CaseExpr, *ast.SubqueryExpr, *ast.WindowFuncExpr, *ast.RowExpr, *ast.DefaultExpr, *ast.ValuesExpr:
		return errors.Trace(ErrPartitionFunctionIsNotAllowed)
	case *ast.FuncCallExpr:
		return checkPartitionFuncCallValid(ctx, tblInfo, v)
	case *ast.BinaryOperationExpr:
		// The DIV operator (opcode.IntDiv) is also supported; the / operator ( opcode.Div ) is not permitted.
		// see https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations.html
		switch v.Op {
		case opcode.Or, opcode.And, opcode.Xor, opcode.LeftShift, opcode.RightShift, opcode.BitNeg, opcode.Div:
			return errors.Trace(ErrPartitionFunctionIsNotAllowed)
		default:
			if err := checkPartitionExprValid(ctx, tblInfo, v.L); err != nil {
				return errors.Trace(err)
			}
			if err := checkPartitionExprValid(ctx, tblInfo, v.R); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	case *ast.UnaryOperationExpr:
		if v.Op == opcode.BitNeg {
			return errors.Trace(ErrPartitionFunctionIsNotAllowed)
		}
		if err := checkPartitionExprValid(ctx, tblInfo, v.V); err != nil {
			return errors.Trace(err)
		}
		return nil
	case *ast.ParenthesesExpr:
		return checkPartitionExprValid(ctx, tblInfo, v.Expr)
	}
	return nil
}

// checkPartitionFuncValid checks partition function validly.
func checkPartitionFuncValid(ctx sessionctx.Context, tblInfo *model.TableInfo, expr ast.ExprNode) error {
	err := checkPartitionExprValid(ctx, tblInfo, expr)
	if err != nil {
		return err
	}
	// check constant.
	_, err = checkPartitionColumns(tblInfo, expr)
	return err
}

// For partition tables, mysql do not support Constant, random or timezone-dependent expressions
// Based on mysql code to check whether field is valid, every time related type has check_valid_arguments_processor function.
// See https://github.com/mysql/mysql-server/blob/5.7/sql/item_timefunc.
func checkResultOK(ok bool, err error) error {
	if err != nil {
		return err
	}

	if !ok {
		return errors.Trace(errWrongExprInPartitionFunc)
	}

	return nil
}

func checkPartitionColumns(tblInfo *model.TableInfo, expr ast.ExprNode) ([]*model.ColumnInfo, error) {
	buf := new(bytes.Buffer)
	expr.Format(buf)
	partCols, err := extractPartitionColumns(buf.String(), tblInfo)
	if err != nil {
		return nil, err
	}

	if len(partCols) == 0 {
		return nil, errors.Trace(errWrongExprInPartitionFunc)
	}

	return partCols, nil
}

// checkPartitionFuncType checks partition function return type.
func checkPartitionFuncType(ctx sessionctx.Context, s *ast.CreateTableStmt, tblInfo *model.TableInfo) error {
	if s.Partition.Expr == nil {
		return nil
	}
	buf := new(bytes.Buffer)
	s.Partition.Expr.Format(buf)
	exprStr := buf.String()
	if s.Partition.Tp == model.PartitionTypeRange || s.Partition.Tp == model.PartitionTypeHash {
		// if partition by columnExpr, check the column type
		if _, ok := s.Partition.Expr.(*ast.ColumnNameExpr); ok {
			for _, col := range tblInfo.Columns {
				name := strings.Replace(col.Name.String(), ".", "`.`", -1)
				// Range partitioning key supported types: tinyint, smallint, mediumint, int and bigint.
				if !validRangePartitionType(col) && fmt.Sprintf("`%s`", name) == exprStr {
					return errors.Trace(ErrNotAllowedTypeInPartition.GenWithStackByArgs(exprStr))
				}
			}
		}
	}

	e, err := expression.ParseSimpleExprWithTableInfo(ctx, exprStr, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}
	if e.GetType().EvalType() == types.ETInt {
		return nil
	}
	if s.Partition.Tp == model.PartitionTypeHash {
		if _, ok := s.Partition.Expr.(*ast.ColumnNameExpr); ok {
			return ErrNotAllowedTypeInPartition.GenWithStackByArgs(exprStr)
		}
	}

	return ErrPartitionFuncNotAllowed.GenWithStackByArgs("PARTITION")
}

// checkCreatePartitionValue checks whether `less than value` is strictly increasing for each partition.
// Side effect: it may simplify the partition range definition from a constant expression to an integer.
func checkCreatePartitionValue(ctx sessionctx.Context, tblInfo *model.TableInfo) error {
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

// removePartitionInfo each ddl job deletes a partition.
func removePartitionInfo(tblInfo *model.TableInfo, partLowerNames []string) []int64 {
	oldDefs := tblInfo.Partition.Definitions
	newDefs := make([]model.PartitionDefinition, 0, len(oldDefs)-1)
	var pids []int64
	for _, partName := range partLowerNames {
		for i := 0; i < len(oldDefs); i++ {
			if oldDefs[i].Name.L != partName {
				continue
			}
			pids = append(pids, oldDefs[i].ID)
			newDefs = append(oldDefs[:i], oldDefs[i+1:]...)
			break
		}
	}

	tblInfo.Partition.Definitions = newDefs
	return pids
}

// onDropTablePartition deletes old partition meta.
func onDropTablePartition(t *meta.Meta, job *model.Job) (ver int64, _ error) {
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
	} else {
		// If an error occurs, it returns that it cannot delete all partitions or that the partition doesn't exist.
		err = checkDropTablePartition(tblInfo, partNames)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		physicalTableIDs = removePartitionInfo(tblInfo, partNames)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	// Finish this job.
	if job.IsRollingback() {
		job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	} else {
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
	}

	// A background job will be created to delete old partition data.
	job.Args = []interface{}{physicalTableIDs}
	return ver, nil
}

// onDropTablePartition truncates old partition meta.
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
		// Parse partitioning key, extract the column names in the partitioning key to slice.
		buf := new(bytes.Buffer)
		s.Partition.Expr.Format(buf)
		partColumns, err := extractPartitionColumns(buf.String(), tblInfo)
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

func checkPartitionKeysConstraint(pi *model.PartitionInfo, indexColumns []*model.IndexColumn, tblInfo *model.TableInfo, isPK bool) error {
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
			return err
		}
	} else {
		partCols = make([]*model.ColumnInfo, 0, len(pi.Columns))
		for _, col := range pi.Columns {
			colInfo := getColumnInfoByName(tblInfo, col.L)
			if colInfo == nil {
				return infoschema.ErrColumnNotExists.GenWithStackByArgs(col, tblInfo.Name)
			}
			partCols = append(partCols, colInfo)
		}
	}

	// Every unique key on the table must use every column in the table's partitioning expression.(This
	// also includes the table's primary key.)
	// See https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	if !checkUniqueKeyIncludePartKey(columnInfoSlice(partCols), indexColumns) {
		if isPK {
			return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY")
		}
		return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("UNIQUE INDEX")
	}
	return nil
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
		return nil, err
	}
	extractor := &columnNameExtractor{
		tblInfo:          tblInfo,
		extractedColumns: make([]*model.ColumnInfo, 0),
	}
	stmts[0].Accept(extractor)
	if extractor.err != nil {
		return nil, extractor.err
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
