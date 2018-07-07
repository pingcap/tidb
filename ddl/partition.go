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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
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
					if !checkSupportTypes(col) && fmt.Sprintf("`%s`", name) == pi.Expr {
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

func checkPartitionNameUnique(tbInfo *model.TableInfo, part *model.PartitionInfo) error {
	partNames := make(map[string]struct{})
	if tbInfo.Partition != nil {
		oldPars := tbInfo.Partition.Definitions
		for _, oldPar := range oldPars {
			partNames[strings.ToLower(oldPar.Name)] = struct{}{}
		}
	}
	newPars := part.Definitions
	for _, newPar := range newPars {
		if _, ok := partNames[strings.ToLower(newPar.Name)]; ok {
			return ErrSameNamePartition.GenByArgs(newPar.Name)
		}
		partNames[strings.ToLower(newPar.Name)] = struct{}{}
	}
	return nil
}

// checkCreatePartitionValue checks whether `less than value` is strictly increasing for each partition.
func checkCreatePartitionValue(part *model.PartitionInfo) error {
	defs := part.Definitions
	if len(defs) <= 1 {
		return nil
	}

	if strings.EqualFold(defs[len(defs)-1].LessThan[0], partitionMaxValue) {
		defs = defs[:len(defs)-1]
	}

	for i := 0; i < len(defs); i++ {
		if strings.EqualFold(defs[i].LessThan[0], partitionMaxValue) {
			return errors.Trace(ErrPartitionMaxvalue)
		}

		currentRangeValue, err := strconv.Atoi(defs[i].LessThan[0])
		if err != nil {
			return ErrNotAllowedTypeInPartition.GenByArgs(defs[i].LessThan[0])
		}

		if i == 0 {
			continue
		}
		prevRangeValue, err := strconv.Atoi(defs[i-1].LessThan[0])
		if err != nil {
			return ErrNotAllowedTypeInPartition.GenByArgs(defs[i-1].LessThan[0])
		}

		if currentRangeValue <= prevRangeValue {
			return errors.Trace(ErrRangeNotIncreasing)
		}
	}
	return nil
}

// checkSupportTypes checks the partitioning key types supported by the range and list partition.
func checkSupportTypes(col *table.Column) bool {
	switch col.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return true
	default:
		return false
	}
}
