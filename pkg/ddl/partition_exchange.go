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
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

// onExchangeTablePartition exchange partition data
func (w *worker) onExchangeTablePartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetExchangeTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	defID, ptSchemaID, ptID, partName :=
		args.PartitionID, args.PTSchemaID, args.PTTableID, args.PartitionName
	metaMut := jobCtx.metaMut

	ntDbInfo, err := checkSchemaExistAndCancelNotExistJob(metaMut, job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ptDbInfo, err := metaMut.GetDatabase(ptSchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	nt, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if job.IsRollingback() {
		return rollbackExchangeTablePartition(jobCtx, job, nt)
	}
	pt, err := getTableInfo(metaMut, ptID, ptSchemaID)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	_, partDef, err := getPartitionDef(pt, partName)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if job.SchemaState == model.StateNone {
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

		err = checkExchangePartitionPlacementPolicy(metaMut, nt.PlacementPolicyRef, pt.PlacementPolicyRef, partDef.PlacementPolicyRef)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		if defID != partDef.ID {
			logutil.DDLLogger().Info("Exchange partition id changed, updating to actual id",
				zap.Stringer("job", job), zap.Int64("defID", defID), zap.Int64("partDef.ID", partDef.ID))
			args.PartitionID = partDef.ID
			job.FillArgs(args)
			defID = partDef.ID
			err = updateDDLJob2Table(jobCtx.stepCtx, w.sess, job, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
		var ptInfo []schemaIDAndTableInfo
		if len(nt.Constraints) > 0 {
			pt.ExchangePartitionInfo = &model.ExchangePartitionInfo{
				ExchangePartitionTableID: nt.ID,
				ExchangePartitionDefID:   defID,
			}
			ptInfo = append(ptInfo, schemaIDAndTableInfo{
				schemaID: ptSchemaID,
				tblInfo:  pt,
			})
		}
		nt.ExchangePartitionInfo = &model.ExchangePartitionInfo{
			ExchangePartitionTableID: ptID,
			ExchangePartitionDefID:   defID,
		}
		// We need an interim schema version,
		// so there are no non-matching rows inserted
		// into the table using the schema version
		// before the exchange is made.
		job.SchemaState = model.StateWriteOnly
		pt.Partition.DDLState = job.SchemaState
		pt.Partition.DDLAction = job.Type
		return updateVersionAndTableInfoWithCheck(jobCtx, job, nt, true, ptInfo...)
	}
	// From now on, nt (the non-partitioned table) has
	// ExchangePartitionInfo set, meaning it is restricted
	// to only allow writes that would match the
	// partition to be exchange with.
	// So we need to rollback that change, instead of just cancelling.

	delayForAsyncCommit()

	if defID != partDef.ID {
		// Should never happen, should have been updated above, in previous state!
		logutil.DDLLogger().Error("Exchange partition id changed, updating to actual id",
			zap.Stringer("job", job), zap.Int64("defID", defID), zap.Int64("partDef.ID", partDef.ID))
		args.PartitionID = partDef.ID
		job.FillArgs(args)
		// might be used later, ignore the lint warning.
		//nolint: ineffassign
		defID = partDef.ID
		err = updateDDLJob2Table(jobCtx.stepCtx, w.sess, job, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	if args.WithValidation {
		ntbl, err := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, nt)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ptbl, err := getTable(jobCtx.getAutoIDRequirement(), ptSchemaID, pt)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = checkExchangePartitionRecordValidation(
			jobCtx.stepCtx,
			w,
			ptbl,
			ntbl,
			ptDbInfo.Name.L,
			ntDbInfo.Name.L,
			partName,
		)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
	}

	// partition table auto IDs.
	ptAutoIDs, err := metaMut.GetAutoIDAccessors(ptSchemaID, ptID).Get()
	if err != nil {
		return ver, errors.Trace(err)
	}
	// non-partition table auto IDs.
	ntAutoIDs, err := metaMut.GetAutoIDAccessors(job.SchemaID, nt.ID).Get()
	if err != nil {
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

	// Recreate non-partition table meta info,
	// by first delete it with the old table id
	err = metaMut.DropTableOrView(job.SchemaID, nt.ID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// exchange table meta id
	pt.ExchangePartitionInfo = nil
	// Used below to update the partitioned table's stats meta.
	originalPartitionDef := partDef.Clone()
	originalNt := nt.Clone()
	partDef.ID, nt.ID = nt.ID, partDef.ID
	pt.Partition.DDLState = model.StateNone
	pt.Partition.DDLAction = model.ActionNone

	err = metaMut.UpdateTable(ptSchemaID, pt)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = metaMut.CreateTableOrView(job.SchemaID, nt)
	if err != nil {
		return ver, errors.Trace(err)
	}

	failpoint.Inject("exchangePartitionErr", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("occur an error after updating partition id"))
		}
	})

	// Set both tables to the maximum auto IDs between normal table and partitioned table.
	// TODO: Fix the issue of big transactions during EXCHANGE PARTITION with AutoID.
	// Similar to https://github.com/pingcap/tidb/issues/46904
	newAutoIDs := model.AutoIDGroup{
		RowID:       max(ptAutoIDs.RowID, ntAutoIDs.RowID),
		IncrementID: max(ptAutoIDs.IncrementID, ntAutoIDs.IncrementID),
		RandomID:    max(ptAutoIDs.RandomID, ntAutoIDs.RandomID),
	}
	err = metaMut.GetAutoIDAccessors(ptSchemaID, pt.ID).Put(newAutoIDs)
	if err != nil {
		return ver, errors.Trace(err)
	}
	err = metaMut.GetAutoIDAccessors(job.SchemaID, nt.ID).Put(newAutoIDs)
	if err != nil {
		return ver, errors.Trace(err)
	}

	failpoint.Inject("exchangePartitionAutoID", func(val failpoint.Value) {
		if val.(bool) {
			seCtx, err := w.sessPool.Get()
			defer w.sessPool.Put(seCtx)
			if err != nil {
				failpoint.Return(ver, err)
			}
			se := sess.NewSession(seCtx)
			_, err = se.Execute(context.Background(), "insert ignore into test.pt values (40000000)", "exchange_partition_test")
			if err != nil {
				failpoint.Return(ver, err)
			}
		}
	})

	// the follow code is a swap function for rules of two partitions
	// though partitions has exchanged their ID, swap still take effect

	bundles, err := bundlesForExchangeTablePartition(metaMut, pt, partDef, nt)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles); err != nil {
		return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
	}

	ntrID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, job.SchemaName, nt.Name.L)
	ptrID := fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, job.SchemaName, pt.Name.L, partDef.Name.L)

	rules, err := infosync.GetLabelRules(context.TODO(), []string{ntrID, ptrID})
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get PD the label rules")
	}

	ntr := rules[ntrID]
	ptr := rules[ptrID]

	// This must be a bug, nt cannot be partitioned!
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
		return ver, errors.Wrapf(err, "failed to notify PD the label rules")
	}

	job.SchemaState = model.StatePublic
	nt.ExchangePartitionInfo = nil
	ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, nt, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	exchangePartitionEvent := notifier.NewExchangePartitionEvent(
		pt,
		&model.PartitionInfo{Definitions: []model.PartitionDefinition{originalPartitionDef}},
		originalNt,
	)
	err = asyncNotifyEvent(jobCtx, exchangePartitionEvent, job, noSubJob, w.sess)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StateNone, ver, pt)
	return ver, nil
}
func bundlesForExchangeTablePartition(t *meta.Mutator, pt *model.TableInfo, newPar *model.PartitionDefinition, nt *model.TableInfo) ([]*placement.Bundle, error) {
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

func checkExchangePartitionRecordValidation(
	ctx context.Context,
	w *worker,
	ptbl, ntbl table.Table,
	pschemaName, nschemaName, partitionName string,
) error {
	verifyFunc := func(sql string, params ...any) error {
		sctx, err := w.sessPool.Get()
		if err != nil {
			return errors.Trace(err)
		}
		defer w.sessPool.Put(sctx)

		rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(
			ctx,
			nil,
			sql,
			params...,
		)
		if err != nil {
			return errors.Trace(err)
		}
		rowCount := len(rows)
		if rowCount != 0 {
			return errors.Trace(dbterror.ErrRowDoesNotMatchPartition)
		}
		// Check warnings!
		// Is it possible to check how many rows where checked as well?
		return nil
	}
	genConstraintCondition := func(constraints []*table.Constraint) string {
		var buf strings.Builder
		buf.WriteString("not (")
		for i, cons := range constraints {
			if i != 0 {
				buf.WriteString(" and ")
			}
			buf.WriteString(fmt.Sprintf("(%s)", cons.ExprString))
		}
		buf.WriteString(")")
		return buf.String()
	}
	type CheckConstraintTable interface {
		WritableConstraint() []*table.Constraint
	}

	pt := ptbl.Meta()
	index, _, err := getPartitionDef(pt, partitionName)
	if err != nil {
		return errors.Trace(err)
	}

	var buf strings.Builder
	buf.WriteString("select 1 from %n.%n where ")
	paramList := []any{nschemaName, ntbl.Meta().Name.L}
	checkNt := true

	pi := pt.Partition
	switch pi.Type {
	case ast.PartitionTypeHash:
		if pi.Num == 1 {
			checkNt = false
		} else {
			buf.WriteString("mod(")
			buf.WriteString(pi.Expr)
			buf.WriteString(", %?) != %?")
			paramList = append(paramList, pi.Num, index)
			if index != 0 {
				// TODO: if hash result can't be NULL, we can remove the check part.
				// For example hash(id), but id is defined not NULL.
				buf.WriteString(" or mod(")
				buf.WriteString(pi.Expr)
				buf.WriteString(", %?) is null")
				paramList = append(paramList, pi.Num, index)
			}
		}
	case ast.PartitionTypeRange:
		// Table has only one partition and has the maximum value
		if len(pi.Definitions) == 1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
			checkNt = false
		} else {
			// For range expression and range columns
			if len(pi.Columns) == 0 {
				conds, params := buildCheckSQLConditionForRangeExprPartition(pi, index)
				buf.WriteString(conds)
				paramList = append(paramList, params...)
			} else {
				conds, params := buildCheckSQLConditionForRangeColumnsPartition(pi, index)
				buf.WriteString(conds)
				paramList = append(paramList, params...)
			}
		}
	case ast.PartitionTypeList:
		if len(pi.Columns) == 0 {
			conds := buildCheckSQLConditionForListPartition(pi, index)
			buf.WriteString(conds)
		} else {
			conds := buildCheckSQLConditionForListColumnsPartition(pi, index)
			buf.WriteString(conds)
		}
	default:
		return dbterror.ErrUnsupportedPartitionType.GenWithStackByArgs(pt.Name.O)
	}

	if vardef.EnableCheckConstraint.Load() {
		pcc, ok := ptbl.(CheckConstraintTable)
		if !ok {
			return errors.Errorf("exchange partition process assert table partition failed")
		}
		pCons := pcc.WritableConstraint()
		if len(pCons) > 0 {
			if !checkNt {
				checkNt = true
			} else {
				buf.WriteString(" or ")
			}
			buf.WriteString(genConstraintCondition(pCons))
		}
	}
	// Check non-partition table records.
	if checkNt {
		buf.WriteString(" limit 1")
		err = verifyFunc(buf.String(), paramList...)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Check partition table records.
	if vardef.EnableCheckConstraint.Load() {
		ncc, ok := ntbl.(CheckConstraintTable)
		if !ok {
			return errors.Errorf("exchange partition process assert table partition failed")
		}
		nCons := ncc.WritableConstraint()
		if len(nCons) > 0 {
			buf.Reset()
			buf.WriteString("select 1 from %n.%n partition(%n) where ")
			buf.WriteString(genConstraintCondition(nCons))
			buf.WriteString(" limit 1")
			err = verifyFunc(buf.String(), pschemaName, pt.Name.L, partitionName)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func checkExchangePartitionPlacementPolicy(t *meta.Mutator, ntPPRef, ptPPRef, partPPRef *model.PolicyRefInfo) error {
	partitionPPRef := partPPRef
	if partitionPPRef == nil {
		partitionPPRef = ptPPRef
	}

	if ntPPRef == nil && partitionPPRef == nil {
		return nil
	}
	if ntPPRef == nil || partitionPPRef == nil {
		return dbterror.ErrTablesDifferentMetadata
	}

	ptPlacementPolicyInfo, _ := getPolicyInfo(t, partitionPPRef.ID)
	ntPlacementPolicyInfo, _ := getPolicyInfo(t, ntPPRef.ID)
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

func buildCheckSQLConditionForRangeExprPartition(pi *model.PartitionInfo, index int) (string, []any) {
	var buf strings.Builder
	paramList := make([]any, 0, 2)
	// Since the pi.Expr string may contain the identifier, which couldn't be escaped in our ParseWithParams(...)
	// So we write it to the origin sql string here.
	if index == 0 {
		// TODO: Handle MAXVALUE in first partition
		buf.WriteString(pi.Expr)
		buf.WriteString(" >= %?")
		paramList = append(paramList, driver.UnwrapFromSingleQuotes(pi.Definitions[index].LessThan[0]))
	} else if index == len(pi.Definitions)-1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
		buf.WriteString(pi.Expr)
		buf.WriteString(" < %? or ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" is null")
		paramList = append(paramList, driver.UnwrapFromSingleQuotes(pi.Definitions[index-1].LessThan[0]))
	} else {
		buf.WriteString(pi.Expr)
		buf.WriteString(" < %? or ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" >= %? or ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" is null")
		paramList = append(paramList, driver.UnwrapFromSingleQuotes(pi.Definitions[index-1].LessThan[0]), driver.UnwrapFromSingleQuotes(pi.Definitions[index].LessThan[0]))
	}
	return buf.String(), paramList
}

func buildCheckSQLConditionForRangeColumnsPartition(pi *model.PartitionInfo, index int) (string, []any) {
	var buf strings.Builder
	paramList := make([]any, 0, len(pi.Columns)*2)

	hasLowerBound := index > 0
	needOR := false

	// Lower bound check (for all partitions except first)
	if hasLowerBound {
		currVals := pi.Definitions[index-1].LessThan
		for i := range pi.Columns {
			nextIsMax := false
			if i < (len(pi.Columns)-1) && strings.EqualFold(currVals[i+1], partitionMaxValue) {
				nextIsMax = true
			}
			if needOR {
				buf.WriteString(" OR ")
			}
			if i > 0 {
				buf.WriteString("(")
				// All previous columns must be equal and non-NULL
				for j := range i {
					if j > 0 {
						buf.WriteString(" AND ")
					}
					buf.WriteString("(%n = %?)")
					paramList = append(paramList, pi.Columns[j].L, driver.UnwrapFromSingleQuotes(currVals[j]))
				}
				buf.WriteString(" AND ")
			}
			paramList = append(paramList, pi.Columns[i].L, driver.UnwrapFromSingleQuotes(currVals[i]), pi.Columns[i].L)
			if nextIsMax {
				buf.WriteString("(%n <= %? OR %n IS NULL)")
			} else {
				buf.WriteString("(%n < %? OR %n IS NULL)")
			}
			if i > 0 {
				buf.WriteString(")")
			}
			needOR = true
			if nextIsMax {
				break
			}
		}
	}

	currVals := pi.Definitions[index].LessThan
	// Upper bound check (for all partitions)
	for i := range pi.Columns {
		if strings.EqualFold(currVals[i], partitionMaxValue) {
			break
		}
		if needOR {
			buf.WriteString(" OR ")
		}
		if i > 0 {
			buf.WriteString("(")
			// All previous columns must be equal
			for j := range i {
				if j > 0 {
					buf.WriteString(" AND ")
				}
				paramList = append(paramList, pi.Columns[j].L, driver.UnwrapFromSingleQuotes(currVals[j]))
				buf.WriteString("(%n = %?)")
			}
			buf.WriteString(" AND ")
		}
		isLast := i == len(pi.Columns)-1
		if isLast {
			buf.WriteString("(%n >= %?)")
		} else {
			buf.WriteString("(%n > %?)")
		}
		paramList = append(paramList, pi.Columns[i].L, driver.UnwrapFromSingleQuotes(currVals[i]))
		if i > 0 {
			buf.WriteString(")")
		}
		needOR = true
	}

	return buf.String(), paramList
}

func buildCheckSQLConditionForListPartition(pi *model.PartitionInfo, index int) string {
	var buf strings.Builder
	// TODO: Handle DEFAULT partition
	buf.WriteString("not (")
	for i, inValue := range pi.Definitions[index].InValues {
		if i != 0 {
			buf.WriteString(" OR ")
		}
		// AND has higher priority than OR, so no need for parentheses
		for j, val := range inValue {
			if j != 0 {
				// Should never happen, since there should be no multi-columns, only a single expression :)
				buf.WriteString(" AND ")
			}
			// null-safe compare '<=>'
			buf.WriteString(fmt.Sprintf("(%s) <=> %s", pi.Expr, val))
		}
	}
	buf.WriteString(")")
	return buf.String()
}

func buildCheckSQLConditionForListColumnsPartition(pi *model.PartitionInfo, index int) string {
	var buf strings.Builder
	// TODO: Verify if this is correct!!!
	// TODO: Handle DEFAULT partition!
	// TODO: use paramList with column names, instead of quoting.
	// How to find a match?
	// (row <=> vals1) OR (row <=> vals2)
	// How to find a non-matching row:
	// NOT ( (row <=> vals1) OR (row <=> vals2) ... )
	buf.WriteString("not (")
	colNames := make([]string, 0, len(pi.Columns))
	for i := range pi.Columns {
		// TODO: Add test for this!
		// TODO: check if there are no proper quoting function for this?
		// TODO: Maybe Sprintf("%#q", str) ?
		n := "`" + strings.ReplaceAll(pi.Columns[i].O, "`", "``") + "`"
		colNames = append(colNames, n)
	}
	for i, colValues := range pi.Definitions[index].InValues {
		if i != 0 {
			buf.WriteString(" OR ")
		}
		// AND has higher priority than OR, so no need for parentheses
		for j, val := range colValues {
			if j != 0 {
				buf.WriteString(" AND ")
			}
			// null-safe compare '<=>'
			buf.WriteString(fmt.Sprintf("%s <=> %s", colNames[j], val))
		}
	}
	buf.WriteString(")")
	return buf.String()
}
