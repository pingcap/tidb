// Copyright 2024 PingCAP, Inc.
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

package indexadvisor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	model2 "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
)

// QueryPlanCostHook is used to calculate the cost of the query plan on this sctx.
// This hook is used to avoid cyclic import.
var QueryPlanCostHook func(sctx sessionctx.Context, stmt ast.StmtNode) (float64, error)

// Optimizer is the interface of a what-if optimizer.
// This interface encapsulates all methods the Index Advisor needs to interact with the TiDB optimizer.
// This interface is not thread-safe.
type Optimizer interface {
	// ColumnType returns the column type of the specified column.
	ColumnType(c Column) (*types.FieldType, error)

	// PrefixContainIndex returns whether the specified index is a prefix of an existing index.
	PrefixContainIndex(idx Index) (bool, error)

	// PossibleColumns returns the possible columns that match the specified column name.
	PossibleColumns(schema, colName string) ([]Column, error)

	// TableColumns returns the columns of the specified table.
	TableColumns(schema, table string) ([]Column, error)

	// IndexNameExist returns whether the specified index name exists in the specified table.
	IndexNameExist(schema, table, indexName string) (bool, error)

	// EstIndexSize return the estimated index size of the specified table and columns
	EstIndexSize(db, table string, cols ...string) (indexSize float64, err error)

	// QueryPlanCost return the cost of the query plan.
	QueryPlanCost(sql string, hypoIndexes ...Index) (cost float64, err error)
}

// optimizerImpl is the implementation of Optimizer.
type optimizerImpl struct {
	sctx sessionctx.Context
}

// NewOptimizer creates a new Optimizer.
func NewOptimizer(sctx sessionctx.Context) Optimizer {
	return &optimizerImpl{sctx}
}

func (opt *optimizerImpl) is() infoschema.InfoSchema {
	return opt.sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
}

// IndexNameExist returns whether the specified index name exists in the specified table.
func (opt *optimizerImpl) IndexNameExist(schema, table, indexName string) (bool, error) {
	tbl, err := opt.is().TableByName(context.Background(), model2.NewCIStr(schema), model2.NewCIStr(table))
	if err != nil {
		return false, err
	}
	for _, idx := range tbl.Indices() {
		if idx.Meta().Name.L == indexName {
			return true, nil
		}
	}
	return false, nil
}

// TableColumns returns the columns of the specified table.
func (opt *optimizerImpl) TableColumns(schema, table string) ([]Column, error) {
	tbl, err := opt.is().TableByName(context.Background(), model2.NewCIStr(schema), model2.NewCIStr(table))
	if err != nil {
		return nil, err
	}
	cols := make([]Column, 0)
	for _, col := range tbl.Cols() {
		cols = append(cols, Column{
			SchemaName: schema,
			TableName:  table,
			ColumnName: col.Name.L,
		})
	}
	return cols, nil
}

// PossibleColumns returns the possible columns that match the specified column name.
func (opt *optimizerImpl) PossibleColumns(schema, colName string) ([]Column, error) {
	// filtering system schema
	schema = strings.ToLower(schema)
	if schema == "information_schema" || schema == "metrics_schema" ||
		schema == "performance_schema" || schema == "mysql" {
		return nil, nil
	}

	cols := make([]Column, 0)
	tbls, err := opt.is().SchemaTableInfos(context.Background(), model2.NewCIStr(schema))
	if err != nil {
		return nil, err
	}
	for _, tbl := range tbls {
		for _, col := range tbl.Cols() {
			if strings.ToLower(col.Name.L) == colName {
				cols = append(cols, Column{
					SchemaName: schema,
					TableName:  tbl.Name.L,
					ColumnName: col.Name.L,
				})
			}
		}
	}
	return cols, nil
}

// PrefixContainIndex returns whether the specified index is a prefix of an existing index.
func (opt *optimizerImpl) PrefixContainIndex(idx Index) (bool, error) {
	tbl, err := opt.is().TableByName(context.Background(), model2.NewCIStr(idx.SchemaName), model2.NewCIStr(idx.TableName))
	if err != nil {
		return false, err
	}
	for _, tblIndex := range tbl.Indices() {
		if len(tblIndex.Meta().Columns) < len(idx.Columns) {
			continue
		}
		prefixMatched := true
		for i, idxCol := range idx.Columns {
			if tblIndex.Meta().Columns[i].Name.L != strings.ToLower(idxCol.ColumnName) {
				prefixMatched = false
				break
			}
		}
		if prefixMatched {
			return true, nil
		}
	}
	return false, nil
}

// ColumnType returns the column type of the specified column.
func (opt *optimizerImpl) ColumnType(c Column) (*types.FieldType, error) {
	tbl, err := opt.is().TableByName(context.Background(), model2.NewCIStr(c.SchemaName), model2.NewCIStr(c.TableName))
	if err != nil {
		return nil, err
	}
	for _, col := range tbl.Cols() {
		if col.Name.L == strings.ToLower(c.ColumnName) {
			return &col.FieldType, nil
		}
	}
	return nil, fmt.Errorf("column %v not found in table %v.%v", c.ColumnName, c.SchemaName, c.TableName)
}

func (opt *optimizerImpl) addHypoIndex(hypoIndexes ...Index) error {
	for _, h := range hypoIndexes {
		tInfo, err := opt.is().TableByName(context.Background(), model2.NewCIStr(h.SchemaName), model2.NewCIStr(h.TableName))
		if err != nil {
			return err
		}

		var cols []*model.IndexColumn
		for _, col := range h.Columns {
			colOffset := -1
			for i, tCol := range tInfo.Cols() {
				if tCol.Name.L == strings.ToLower(col.ColumnName) {
					colOffset = i
					break
				}
			}
			if colOffset == -1 {
				return fmt.Errorf("column %v not found in table %v.%v", col.ColumnName, h.SchemaName, h.TableName)
			}
			cols = append(cols, &model.IndexColumn{
				Name:   model2.NewCIStr(col.ColumnName),
				Offset: colOffset,
				Length: types.UnspecifiedLength,
			})
		}
		idxInfo := &model.IndexInfo{
			Name:    model2.NewCIStr(h.IndexName),
			Columns: cols,
			State:   model.StatePublic,
			Tp:      model2.IndexTypeHypo,
		}

		if opt.sctx.GetSessionVars().HypoIndexes == nil {
			opt.sctx.GetSessionVars().HypoIndexes = make(map[string]map[string]map[string]*model.IndexInfo)
		}
		if opt.sctx.GetSessionVars().HypoIndexes[h.SchemaName] == nil {
			opt.sctx.GetSessionVars().HypoIndexes[h.SchemaName] = make(map[string]map[string]*model.IndexInfo)
		}
		if opt.sctx.GetSessionVars().HypoIndexes[h.SchemaName][h.TableName] == nil {
			opt.sctx.GetSessionVars().HypoIndexes[h.SchemaName][h.TableName] = make(map[string]*model.IndexInfo)
		}
		opt.sctx.GetSessionVars().HypoIndexes[h.SchemaName][h.TableName][h.IndexName] = idxInfo
	}
	return nil
}

// QueryPlanCost return the cost of the query plan.
func (opt *optimizerImpl) QueryPlanCost(sql string, hypoIndexes ...Index) (cost float64, err error) {
	stmt, err := ParseOneSQL(sql)
	if err != nil {
		return 0, err
	}

	originalFix43817 := opt.sctx.GetSessionVars().OptimizerFixControl[fixcontrol.Fix43817]
	originalWarns := opt.sctx.GetSessionVars().StmtCtx.GetWarnings()
	originalExtraWarns := opt.sctx.GetSessionVars().StmtCtx.GetExtraWarnings()
	originalHypoIndexes := opt.sctx.GetSessionVars().HypoIndexes
	defer func() {
		opt.sctx.GetSessionVars().OptimizerFixControl[fixcontrol.Fix43817] = originalFix43817
		opt.sctx.GetSessionVars().StmtCtx.SetWarnings(originalWarns)
		opt.sctx.GetSessionVars().StmtCtx.SetExtraWarnings(originalExtraWarns)
		opt.sctx.GetSessionVars().HypoIndexes = originalHypoIndexes
		opt.sctx.GetSessionVars().StmtCtx.InExplainStmt = false
	}()
	opt.sctx.GetSessionVars().OptimizerFixControl[fixcontrol.Fix43817] = "on"
	opt.sctx.GetSessionVars().StmtCtx.InExplainStmt = true
	opt.sctx.GetSessionVars().HypoIndexes = nil

	if err := opt.addHypoIndex(hypoIndexes...); err != nil {
		return 0, err
	}
	return QueryPlanCostHook(opt.sctx, stmt)
}

// EstIndexSize return the estimated index size of the specified table and columns
func (opt *optimizerImpl) EstIndexSize(db, table string, cols ...string) (indexSize float64, err error) {
	tbl, err := opt.is().TableByName(context.Background(), model2.NewCIStr(db), model2.NewCIStr(table))
	if err != nil {
		return 0, err
	}
	stats := domain.GetDomain(opt.sctx).StatsHandle()
	tblStats := stats.GetTableStats(tbl.Meta())
	for _, colName := range cols {
		colStats := tblStats.ColumnByName(colName)
		if colStats == nil { // might be not loaded
			indexSize += float64(8) * float64(tblStats.RealtimeCount)
		} else {
			indexSize += float64(colStats.TotColSize)
		}
	}
	return indexSize, nil
}
