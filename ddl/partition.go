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
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

const (
	partitionMaxValue = "MAXVALUE"
)

// buildTablePartitionInfo builds partition info and checks for some errors.
func buildTablePartitionInfo(ctx sessionctx.Context, d *ddl, s *ast.CreateTableStmt, cols []*table.Column) (*model.PartitionInfo, error) {
	if s.Partition == nil {
		return nil, nil
	}
	pi := &model.PartitionInfo{
		Type:   s.Partition.Tp,
		Enable: ctx.GetSessionVars().EnableTablePartition,
	}
	if s.Partition.Expr != nil {
		buf := new(bytes.Buffer)
		s.Partition.Expr.Format(buf)
		pi.Expr = buf.String()
		if s.Partition.Tp == model.PartitionTypeRange {
			for _, col := range cols {
				name := strings.Replace(col.Name.String(), ".", "`.`", -1)
				if _, ok := s.Partition.Expr.(*ast.ColumnNameExpr); ok {
					// TODO: check that the expression returns an integer.
				}
				if _, ok := s.Partition.Expr.(ast.ExprNode); ok {
					// Range partitioning key supported types: tinyint, smallint, mediumint, int and bigint.
					if !validRangePartitionType(col) && fmt.Sprintf("`%s`", name) == pi.Expr {
						return nil, errors.Trace(ErrNotAllowedTypeInPartition.GenByArgs(pi.Expr))
					}
				}
			}
		}
	} else if s.Partition.ColumnNames != nil {
		pi.Columns = make([]model.CIStr, 0, len(s.Partition.ColumnNames))
		for _, cn := range s.Partition.ColumnNames {
			pi.Columns = append(pi.Columns, cn.Name)
		}
	}
	for _, def := range s.Partition.Definitions {
		// TODO: generate multiple global ID for paritions, reduce the times of obtaining the global ID from the storage.
		pid, err := d.genGlobalID()
		if err != nil {
			return nil, errors.Trace(err)
		}
		piDef := model.PartitionDefinition{
			Name:    def.Name,
			ID:      pid,
			Comment: def.Comment,
		}

		if s.Partition.Tp == model.PartitionTypeRange {
			if s.Partition.ColumnNames == nil && len(def.LessThan) != 1 {
				return nil, ErrTooManyValues.GenByArgs(s.Partition.Tp.String())
			}
			buf := new(bytes.Buffer)
			// Range columns partitions support multi-column partitions.
			for _, expr := range def.LessThan {
				expr.Format(buf)
				piDef.LessThan = append(piDef.LessThan, buf.String())
				buf.Reset()
			}
			pi.Definitions = append(pi.Definitions, piDef)
		}
	}
	return pi, nil
}

func checkPartitionNameUnique(tbInfo *model.TableInfo, pi *model.PartitionInfo) error {
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
			return ErrSameNamePartition.GenByArgs(newPar.Name)
		}
		partNames[newPar.Name.L] = struct{}{}
	}
	return nil
}

// checkCreatePartitionValue checks whether `less than value` is strictly increasing for each partition.
func checkCreatePartitionValue(pi *model.PartitionInfo) error {
	defs := pi.Definitions
	if len(defs) <= 1 {
		return nil
	}

	if strings.EqualFold(defs[len(defs)-1].LessThan[0], partitionMaxValue) {
		defs = defs[:len(defs)-1]
	}
	var prevRangeValue int
	for i := 0; i < len(defs); i++ {
		if strings.EqualFold(defs[i].LessThan[0], partitionMaxValue) {
			return errors.Trace(ErrPartitionMaxvalue)
		}

		currentRangeValue, err := strconv.Atoi(defs[i].LessThan[0])
		if err != nil {
			return ErrNotAllowedTypeInPartition.GenByArgs(defs[i].LessThan[0])
		}

		if i == 0 {
			prevRangeValue = currentRangeValue
			continue
		}

		if currentRangeValue <= prevRangeValue {
			return errors.Trace(ErrRangeNotIncreasing)
		}
		prevRangeValue = currentRangeValue
	}
	return nil
}

// validRangePartitionType checks the type supported by the range partitioning key.
func validRangePartitionType(col *table.Column) bool {
	switch col.FieldType.EvalType() {
	case types.ETInt:
		return true
	default:
		return false
	}
}

// checkDropTablePartition checks if the partition exists and does not allow deleting the last existing partition in the table.
func checkDropTablePartition(meta *model.TableInfo, partName string) error {
	oldDefs := meta.Partition.Definitions
	for _, def := range oldDefs {
		if strings.EqualFold(def.Name.L, strings.ToLower(partName)) {
			if len(oldDefs) == 1 {
				return errors.Trace(ErrDropLastPartition)
			}
			return nil
		}
	}
	return errors.Trace(ErrDropPartitionNonExistent.GenByArgs(partName))
}

// removePartitionInfo each ddl job deletes a partition.
func removePartitionInfo(tblInfo *model.TableInfo, partName string) int64 {
	oldDefs := tblInfo.Partition.Definitions
	newDefs := make([]model.PartitionDefinition, 0, len(oldDefs)-1)
	var pid int64
	for i := 0; i < len(oldDefs); i++ {
		if !strings.EqualFold(oldDefs[i].Name.L, strings.ToLower(partName)) {
			continue
		}
		pid = oldDefs[i].ID
		newDefs = append(oldDefs[:i], oldDefs[i+1:]...)
		break
	}
	tblInfo.Partition.Definitions = newDefs
	return pid
}

// onDropTablePartition deletes old partition meta.
func onDropTablePartition(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var partName string
	if err := job.DecodeArgs(&partName); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// If an error occurs, it returns that it cannot delete all partitions or that the partition doesn't exist.
	err = checkDropTablePartition(tblInfo, partName)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	partitionID := removePartitionInfo(tblInfo, partName)
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
	// A background job will be created to delete old partition data.
	job.Args = []interface{}{partitionID}
	return ver, nil
}

func checkAddPartitionTooManyPartitions(piDefs int) error {
	if piDefs > PartitionCountLimit {
		return ErrTooManyPartitions
	}
	return nil
}
