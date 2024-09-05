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
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/logutil"
	parser2 "github.com/pingcap/tidb/pkg/util/parser"
	s "github.com/pingcap/tidb/pkg/util/set"
	"go.uber.org/zap"
)

// ParseOneSQL parses the given Query text and returns the AST.
func ParseOneSQL(sqlText string) (ast.StmtNode, error) {
	p := parser.New()
	return p.ParseOneStmt(sqlText, "", "")
}

// NormalizeDigest normalizes the given Query text and returns the normalized Query text and its digest.
func NormalizeDigest(sqlText string) (normalizedSQL, digest string) {
	norm, d := parser.NormalizeDigest(sqlText)
	return norm, d.String()
}

type nodeVisitor struct {
	enter func(n ast.Node) (skip bool)
	leave func(n ast.Node) (ok bool)
}

func (v *nodeVisitor) Enter(n ast.Node) (out ast.Node, skipChildren bool) {
	if v.enter != nil {
		return n, v.enter(n)
	}
	return n, false
}

func (v *nodeVisitor) Leave(n ast.Node) (out ast.Node, ok bool) {
	if v.leave != nil {
		return n, v.leave(n)
	}
	return n, true
}

func visitNode(n ast.Node, enter func(n ast.Node) (skip bool), leave func(n ast.Node) (ok bool)) {
	n.Accept(&nodeVisitor{enter, leave})
}

// CollectTableNamesFromQuery returns all referenced table names in the given Query text.
// The returned format is []string{"schema.table", "schema.table", ...}.
func CollectTableNamesFromQuery(defaultSchema, query string) ([]string, error) {
	node, err := ParseOneSQL(query)
	if err != nil {
		return nil, err
	}
	cteNames := make(map[string]struct{})
	var tableNames []string
	visitNode(node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.WithClause:
			for _, cte := range x.CTEs {
				cteNames[fmt.Sprintf("%v.%v", defaultSchema, cte.Name.String())] = struct{}{}
			}
		case *ast.TableName:
			var tableName string
			if x.Schema.L == "" {
				tableName = fmt.Sprintf("%v.%v", defaultSchema, x.Name.String())
			} else {
				tableName = fmt.Sprintf("%v.%v", x.Schema.L, x.Name.String())
			}
			if _, ok := cteNames[tableName]; !ok {
				tableNames = append(tableNames, tableName)
			}
		}
		return false
	}, nil)
	return tableNames, nil
}

// CollectSelectColumnsFromQuery parses the given Query text and returns the selected columns.
// For example, "select a, b, c from t" returns []string{"a", "b", "c"}.
func CollectSelectColumnsFromQuery(q Query) (s.Set[Column], error) {
	names, err := CollectTableNamesFromQuery(q.SchemaName, q.Text)
	if err != nil {
		return nil, err
	}
	if len(names) != 1 { // unsupported yet
		return nil, nil
	}
	tmp := strings.Split(names[0], ".")
	node, err := ParseOneSQL(q.Text)
	if err != nil {
		return nil, err
	}
	underSelectField := false
	selectCols := s.NewSet[Column]()
	visitNode(node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.SelectField:
			underSelectField = true
		case *ast.ColumnNameExpr:
			if underSelectField {
				selectCols.Add(Column{
					SchemaName: tmp[0],
					TableName:  tmp[1],
					ColumnName: x.Name.Name.O})
			}
		}
		return false
	}, func(n ast.Node) bool {
		if _, ok := n.(*ast.SelectField); ok {
			underSelectField = false
		}
		return true
	})
	return selectCols, nil
}

// CollectOrderByColumnsFromQuery parses the given Query text and returns the order-by columns.
// For example, "select a, b from t order by a, b" returns []string{"a", "b"}.
func CollectOrderByColumnsFromQuery(q Query) ([]Column, error) {
	names, err := CollectTableNamesFromQuery(q.SchemaName, q.Text)
	if err != nil {
		return nil, err
	}
	if len(names) != 1 { // unsupported yet
		return nil, nil
	}
	tmp := strings.Split(names[0], ".")
	node, err := ParseOneSQL(q.Text)
	if err != nil {
		return nil, err
	}
	var orderByCols []Column
	exit := false
	visitNode(node, func(n ast.Node) bool {
		if exit {
			return true
		}
		if x, ok := n.(*ast.OrderByClause); ok {
			for _, byItem := range x.Items {
				colExpr, ok := byItem.Expr.(*ast.ColumnNameExpr)
				if !ok {
					orderByCols = nil
					exit = true
					return true
				}
				orderByCols = append(orderByCols, Column{
					SchemaName: tmp[0],
					TableName:  tmp[1],
					ColumnName: colExpr.Name.Name.O})
			}
		}
		return false
	}, nil)
	return orderByCols, nil
}

// CollectDNFColumnsFromQuery parses the given Query text and returns the DNF columns.
// For a query `select ... where c1=1 or c2=2 or c3=3`, the DNF columns are `c1`, `c2` and `c3`.
func CollectDNFColumnsFromQuery(q Query) (s.Set[Column], error) {
	names, err := CollectTableNamesFromQuery(q.SchemaName, q.Text)
	if err != nil {
		return nil, err
	}
	if len(names) != 1 { // unsupported yet
		return nil, nil
	}
	tmp := strings.Split(names[0], ".")
	node, err := ParseOneSQL(q.Text)
	if err != nil {
		return nil, err
	}
	dnfColSet := s.NewSet[Column]()

	visitNode(node, func(n ast.Node) bool {
		if dnfColSet.Size() > 0 { // already collected
			return true
		}
		if x, ok := n.(*ast.SelectStmt); ok {
			cnf := flattenCNF(x.Where)
			for _, expr := range cnf {
				dnf := flattenDNF(expr)
				if len(dnf) <= 1 {
					continue
				}
				// c1=1 or c2=2 or c3=3
				var dnfCols []*ast.ColumnNameExpr
				fail := false
				for _, dnfExpr := range dnf {
					col, _ := flattenColEQConst(dnfExpr)
					if col == nil {
						fail = true
						break
					}
					dnfCols = append(dnfCols, col)
				}
				if fail {
					continue
				}
				for _, col := range dnfCols {
					dnfColSet.Add(Column{SchemaName: tmp[0], TableName: tmp[1], ColumnName: col.Name.Name.O})
				}
			}
		}
		return false
	}, nil)

	return dnfColSet, nil
}

func flattenColEQConst(expr ast.ExprNode) (*ast.ColumnNameExpr, *driver.ValueExpr) {
	if _, ok := expr.(*ast.ParenthesesExpr); ok {
		return flattenColEQConst(expr.(*ast.ParenthesesExpr).Expr)
	}

	if op, ok := expr.(*ast.BinaryOperationExpr); ok && op.Op == opcode.EQ {
		l, r := op.L, op.R
		_, lIsCol := l.(*ast.ColumnNameExpr)
		_, lIsCon := l.(*driver.ValueExpr)
		_, rIsCol := r.(*ast.ColumnNameExpr)
		_, rIsCon := r.(*driver.ValueExpr)
		if lIsCol && rIsCon {
			return l.(*ast.ColumnNameExpr), r.(*driver.ValueExpr)
		}
		if lIsCon && rIsCol {
			return r.(*ast.ColumnNameExpr), l.(*driver.ValueExpr)
		}
	}
	return nil, nil
}

func flattenCNF(expr ast.ExprNode) []ast.ExprNode {
	if _, ok := expr.(*ast.ParenthesesExpr); ok {
		return flattenCNF(expr.(*ast.ParenthesesExpr).Expr)
	}

	var cnf []ast.ExprNode
	if op, ok := expr.(*ast.BinaryOperationExpr); ok && op.Op == opcode.LogicAnd {
		cnf = append(cnf, flattenCNF(op.L)...)
		cnf = append(cnf, flattenCNF(op.R)...)
	} else {
		cnf = append(cnf, expr)
	}
	return cnf
}

func flattenDNF(expr ast.ExprNode) []ast.ExprNode {
	if _, ok := expr.(*ast.ParenthesesExpr); ok {
		return flattenDNF(expr.(*ast.ParenthesesExpr).Expr)
	}

	var cnf []ast.ExprNode
	if op, ok := expr.(*ast.BinaryOperationExpr); ok && op.Op == opcode.LogicOr {
		cnf = append(cnf, flattenDNF(op.L)...)
		cnf = append(cnf, flattenDNF(op.R)...)
	} else {
		cnf = append(cnf, expr)
	}
	return cnf
}

// RestoreSchemaName restores the schema name of the given Query set.
func RestoreSchemaName(defaultSchema string, sqls s.Set[Query], ignoreErr bool) (s.Set[Query], error) {
	s := s.NewSet[Query]()
	for _, sql := range sqls.ToList() {
		if sql.SchemaName == "" {
			sql.SchemaName = defaultSchema
		}
		stmt, err := ParseOneSQL(sql.Text)
		if err != nil {
			if ignoreErr {
				continue
			}
			return nil, fmt.Errorf("invalid query: %v, err: %v", sql.Text, err)
		}
		sql.Text = parser2.RestoreWithDefaultDB(stmt, sql.SchemaName, sql.Text)
		s.Add(sql)
	}
	return s, nil
}

// FilterInvalidQueries filters out invalid queries from the given query set.
// some queries might be forbidden by the fix-control 43817.
func FilterInvalidQueries(opt Optimizer, sqls s.Set[Query], ignoreErr bool) (s.Set[Query], error) {
	s := s.NewSet[Query]()
	for _, sql := range sqls.ToList() {
		_, err := opt.QueryPlanCost(sql.Text)
		if err != nil {
			if ignoreErr {
				continue
			}
			return nil, err
		}
		s.Add(sql)
	}
	return s, nil
}

// FilterSQLAccessingSystemTables filters out queries that access system tables.
func FilterSQLAccessingSystemTables(sqls s.Set[Query], ignoreErr bool) (s.Set[Query], error) {
	s := s.NewSet[Query]()
	for _, sql := range sqls.ToList() {
		accessSystemTable := false
		names, err := CollectTableNamesFromQuery(sql.SchemaName, sql.Text)
		if err != nil {
			if ignoreErr {
				continue
			}
			return nil, err
		}
		if len(names) == 0 {
			// `select @@some_var` or `select some_func()`
			continue
		}
		for _, name := range names {
			schemaName := strings.ToLower(strings.Split(name, ".")[0])
			if schemaName == "information_schema" || schemaName == "metrics_schema" ||
				schemaName == "performance_schema" || schemaName == "mysql" {
				accessSystemTable = true
				break
			}
		}
		if !accessSystemTable {
			s.Add(sql)
		}
	}
	return s, nil
}

// CollectIndexableColumnsForQuerySet finds all columns that appear in any range-filter, order-by, or group-by clause.
func CollectIndexableColumnsForQuerySet(opt Optimizer, querySet s.Set[Query]) (s.Set[Column], error) {
	indexableColumnSet := s.NewSet[Column]()
	queryList := querySet.ToList()
	for _, q := range queryList {
		cols, err := CollectIndexableColumnsFromQuery(q, opt)
		if err != nil {
			return nil, err
		}
		querySet.Add(q)
		indexableColumnSet.Add(cols.ToList()...)
	}
	return indexableColumnSet, nil
}

// CollectIndexableColumnsFromQuery parses the given Query text and returns the indexable columns.
func CollectIndexableColumnsFromQuery(q Query, opt Optimizer) (s.Set[Column], error) {
	tableNames, err := CollectTableNamesFromQuery(q.SchemaName, q.Text)
	if err != nil {
		return nil, err
	}
	possibleSchemas := make(map[string]bool)
	possibleSchemas[q.SchemaName] = true
	for _, name := range tableNames {
		schemaName := strings.Split(name, ".")[0]
		possibleSchemas[strings.ToLower(schemaName)] = true
	}

	stmt, err := ParseOneSQL(q.Text)
	if err != nil {
		return nil, err
	}
	cols := s.NewSet[Column]()
	var collectColumn func(n ast.Node)
	collectColumn = func(n ast.Node) {
		switch x := n.(type) {
		case *ast.ColumnNameExpr:
			collectColumn(x.Name)
		case *ast.ColumnName:
			var schemaNames []string
			if x.Schema.L != "" {
				schemaNames = append(schemaNames, x.Schema.L)
			} else {
				for schemaName := range possibleSchemas {
					schemaNames = append(schemaNames, schemaName)
				}
			}

			var possibleColumns []Column
			for _, schemaName := range schemaNames {
				cols, err := opt.PossibleColumns(schemaName, x.Name.L)
				if err != nil {
					advisorLogger().Warn("failed to get possible columns",
						zap.String("schema", schemaName),
						zap.String("column", x.Name.L))
					continue
				}
				possibleColumns = append(possibleColumns, cols...)
			}

			for _, c := range possibleColumns {
				colType, err := opt.ColumnType(c)
				if err != nil {
					advisorLogger().Warn("failed to get column type",
						zap.String("schema", c.SchemaName),
						zap.String("table", c.TableName),
						zap.String("column", c.ColumnName))
					continue
				}
				if !isIndexableColumnType(colType) {
					continue
				}
				cols.Add(c)
			}
		}
	}

	visitNode(stmt, func(n ast.Node) (skip bool) {
		switch x := n.(type) {
		case *ast.GroupByClause: // group by {col}
			for _, item := range x.Items {
				collectColumn(item.Expr)
			}
			return true
		case *ast.OrderByClause: // order by {col}
			for _, item := range x.Items {
				collectColumn(item.Expr)
			}
			return true
		case *ast.BetweenExpr: // {col} between ? and ?
			collectColumn(x.Expr)
		case *ast.PatternInExpr: // {col} in (?, ?, ...)
			collectColumn(x.Expr)
		case *ast.BinaryOperationExpr: // range predicates like `{col} > ?`
			switch x.Op {
			case opcode.EQ, opcode.LT, opcode.LE, opcode.GT, opcode.GE: // {col} = ?
				collectColumn(x.L)
				collectColumn(x.R)
			}
		default:
		}
		return false
	}, nil)
	return cols, nil
}

func isIndexableColumnType(tp *types.FieldType) bool {
	if tp == nil {
		return false
	}
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear,
		mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal,
		mysql.TypeDuration, mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return true
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		return tp.GetFlen() <= 512
	}
	return false
}

func advisorLogger() *zap.Logger {
	return logutil.BgLogger().With(zap.String("component", "index_advisor"))
}

// chooseBestIndexSet chooses the best index set from the given index sets.
func chooseBestIndexSet(
	querySet s.Set[Query], optimizer Optimizer,
	indexSets []s.Set[Index]) (bestSet s.Set[Index], bestCost IndexSetCost, err error) {
	for i, indexSet := range indexSets {
		cost, err := evaluateIndexSetCost(querySet, optimizer, indexSet)
		if err != nil {
			return nil, bestCost, err
		}
		if i == 0 || cost.Less(bestCost) {
			bestSet = indexSet
			bestCost = cost
		}
	}
	return bestSet, bestCost, nil
}

// evaluateIndexSetCost evaluates the workload cost under the given indexes.
func evaluateIndexSetCost(
	querySet s.Set[Query], optimizer Optimizer,
	indexSet s.Set[Index]) (IndexSetCost, error) {
	qs := querySet.ToList()
	sqls := make([]string, 0, len(qs))
	for _, q := range qs {
		sqls = append(sqls, q.Text)
	}
	costs := make([]float64, 0, len(sqls))
	for _, sql := range sqls {
		cost, err := optimizer.QueryPlanCost(sql, indexSet.ToList()...)
		if err != nil {
			return IndexSetCost{}, err
		}
		costs = append(costs, cost)
	}

	var workloadCost float64
	queries := querySet.ToList()
	for i, cost := range costs {
		query := queries[i]
		workloadCost += cost * float64(query.Frequency)
	}
	var totCols int
	keys := make([]string, 0, indexSet.Size())
	for _, index := range indexSet.ToList() {
		totCols += len(index.Columns)
		keys = append(keys, index.Key())
	}
	sort.Strings(keys)

	return IndexSetCost{workloadCost, totCols, strings.Join(keys, ",")}, nil
}
