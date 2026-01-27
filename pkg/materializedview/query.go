package materializedview

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type MVAggKind string

const (
	MVAggKindCountStar MVAggKind = "COUNT_STAR"
	MVAggKindSum       MVAggKind = "SUM"
	MVAggKindMin       MVAggKind = "MIN"
	MVAggKindMax       MVAggKind = "MAX"
)

type MVSelectItem struct {
	IsGroupBy bool

	// For group-by columns, BaseColumnID is set.
	BaseColumnID int64
	// For aggregates, AggKind is set. ArgBaseColumnID is set for SUM/MIN/MAX.
	AggKind         MVAggKind
	ArgBaseColumnID int64
}

type MVQuery struct {
	BaseSchema ast.CIStr
	BaseTable  ast.CIStr
	BaseAlias  ast.CIStr

	SelectItems []MVSelectItem

	GroupByBaseColumnIDs []int64
	UsedBaseColumnIDs    map[int64]struct{}
}

func GetSingleBaseTableFromSelect(sel *ast.SelectStmt) (*ast.TableName, ast.CIStr, error) {
	if sel.From == nil || sel.From.TableRefs == nil {
		return nil, ast.CIStr{}, errors.New("materialized view query must have FROM clause")
	}
	join := sel.From.TableRefs
	if join.Right != nil {
		return nil, ast.CIStr{}, errors.New("materialized view query only supports single table (no join)")
	}
	ts, ok := join.Left.(*ast.TableSource)
	if !ok {
		return nil, ast.CIStr{}, errors.New("materialized view query only supports single table source")
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, ast.CIStr{}, errors.New("materialized view query only supports base table as FROM source")
	}
	return tn, ts.AsName, nil
}

func columnNameToBaseColumnID(
	baseSchema, baseTable, baseAlias ast.CIStr,
	baseInfo *model.TableInfo,
	name *ast.ColumnName,
) (int64, error) {
	if name == nil {
		return 0, errors.New("nil column name")
	}
	if name.Schema.L != "" && name.Schema.L != baseSchema.L {
		return 0, errors.Errorf("column %s is not from base schema", name.OrigColName())
	}
	if name.Table.L != "" && name.Table.L != baseTable.L && (baseAlias.L == "" || name.Table.L != baseAlias.L) {
		return 0, errors.Errorf("column %s is not from base table", name.OrigColName())
	}
	colInfo := model.FindColumnInfo(baseInfo.Columns, name.Name.L)
	if colInfo == nil {
		return 0, infoschema.ErrColumnNotExists.GenWithStackByArgs(name.Name.O, baseInfo.Name.O)
	}
	return colInfo.ID, nil
}

func ParseMVQuery(is infoschema.InfoSchema, currentDB string, stmt ast.StmtNode) (*MVQuery, error) {
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return nil, errors.New("materialized view query must be a SELECT statement")
	}
	if sel.With != nil {
		return nil, errors.New("materialized view query does not support WITH clause")
	}
	if sel.Distinct {
		return nil, errors.New("materialized view query does not support DISTINCT")
	}
	if sel.Having != nil {
		return nil, errors.New("materialized view query does not support HAVING")
	}
	if sel.WindowSpecs != nil {
		return nil, errors.New("materialized view query does not support window functions")
	}
	if sel.OrderBy != nil {
		return nil, errors.New("materialized view query does not support ORDER BY")
	}
	if sel.Limit != nil {
		return nil, errors.New("materialized view query does not support LIMIT")
	}

	baseTN, baseAlias, err := GetSingleBaseTableFromSelect(sel)
	if err != nil {
		return nil, err
	}
	baseSchemaName := baseTN.Schema.L
	if baseSchemaName == "" {
		baseSchemaName = strings.ToLower(currentDB)
	}
	if baseSchemaName == "" {
		return nil, errors.New("no database selected")
	}
	baseSchema := ast.NewCIStr(baseSchemaName)

	baseTbl, err := is.TableByName(context.Background(), baseSchema, baseTN.Name)
	if err != nil {
		return nil, err
	}
	baseInfo := baseTbl.Meta()
	if baseInfo.IsView() || baseInfo.IsSequence() || baseInfo.IsMaterializedView() || baseInfo.IsMaterializedViewLog() {
		return nil, errors.New("materialized view base table must be a normal base table")
	}

	if sel.GroupBy == nil || len(sel.GroupBy.Items) == 0 {
		return nil, errors.New("materialized view query must have GROUP BY")
	}

	groupByIDs := make([]int64, 0, len(sel.GroupBy.Items))
	groupBySet := make(map[int64]struct{}, len(sel.GroupBy.Items))
	for _, item := range sel.GroupBy.Items {
		colExpr, ok := item.Expr.(*ast.ColumnNameExpr)
		if !ok {
			return nil, errors.New("materialized view GROUP BY only supports column names")
		}
		colID, err := columnNameToBaseColumnID(baseSchema, baseTN.Name, baseAlias, baseInfo, colExpr.Name)
		if err != nil {
			return nil, err
		}
		if _, ok := groupBySet[colID]; ok {
			return nil, errors.New("materialized view GROUP BY has duplicate columns")
		}
		groupBySet[colID] = struct{}{}
		groupByIDs = append(groupByIDs, colID)
	}

	selectItems := make([]MVSelectItem, 0, len(sel.Fields.Fields))
	usedIDs := make(map[int64]struct{})
	hasCountStar := false
	selectedGroupByIDs := make(map[int64]struct{})
	for _, field := range sel.Fields.Fields {
		switch expr := field.Expr.(type) {
		case *ast.ColumnNameExpr:
			colID, err := columnNameToBaseColumnID(baseSchema, baseTN.Name, baseAlias, baseInfo, expr.Name)
			if err != nil {
				return nil, err
			}
			if _, ok := groupBySet[colID]; !ok {
				return nil, errors.New("materialized view query requires all selected non-aggregate columns to be in GROUP BY")
			}
			selectItems = append(selectItems, MVSelectItem{IsGroupBy: true, BaseColumnID: colID})
			selectedGroupByIDs[colID] = struct{}{}
			usedIDs[colID] = struct{}{}
		case *ast.AggregateFuncExpr:
			fnName := strings.ToLower(expr.F)
			if expr.Distinct {
				return nil, errors.New("materialized view query does not support DISTINCT aggregation")
			}
			switch fnName {
			case "count":
				if len(expr.Args) != 1 {
					return nil, errors.New("materialized view query only supports count(*)/count(1)")
				}
				ve, ok := expr.Args[0].(ast.ValueExpr)
				if !ok || ve.GetValue() == nil {
					return nil, errors.New("materialized view query only supports count(*)/count(1)")
				}
				switch v := ve.GetValue().(type) {
				case int:
					if v != 1 {
						return nil, errors.New("materialized view query only supports count(*)/count(1)")
					}
				case int64:
					if v != 1 {
						return nil, errors.New("materialized view query only supports count(*)/count(1)")
					}
				case uint64:
					if v != 1 {
						return nil, errors.New("materialized view query only supports count(*)/count(1)")
					}
				default:
					return nil, errors.New("materialized view query only supports count(*)/count(1)")
				}
				hasCountStar = true
				selectItems = append(selectItems, MVSelectItem{AggKind: MVAggKindCountStar})
			case "sum", "min", "max":
				if len(expr.Args) != 1 {
					return nil, errors.Errorf("materialized view query only supports %s(col)", fnName)
				}
				argCol, ok := expr.Args[0].(*ast.ColumnNameExpr)
				if !ok {
					return nil, errors.Errorf("materialized view query only supports %s(col)", fnName)
				}
				argColID, err := columnNameToBaseColumnID(baseSchema, baseTN.Name, baseAlias, baseInfo, argCol.Name)
				if err != nil {
					return nil, err
				}
				usedIDs[argColID] = struct{}{}
				kind := MVAggKindSum
				if fnName == "min" {
					kind = MVAggKindMin
				} else if fnName == "max" {
					kind = MVAggKindMax
				}
				selectItems = append(selectItems, MVSelectItem{AggKind: kind, ArgBaseColumnID: argColID})
			default:
				return nil, errors.Errorf("materialized view query does not support aggregate function %s", fnName)
			}
		default:
			return nil, errors.New("materialized view query only supports column names and aggregation functions")
		}
	}
	if !hasCountStar {
		return nil, errors.New("materialized view query must explicitly include count(*)/count(1)")
	}
	if len(selectedGroupByIDs) != len(groupBySet) {
		return nil, errors.New("materialized view query requires all GROUP BY columns to be selected")
	}

	colCollector := &mvColumnCollector{
		baseSchema: baseSchema,
		baseTable:  baseTN.Name,
		baseAlias:  baseAlias,
		baseInfo:   baseInfo,
		usedIDs:    usedIDs,
	}
	if sel.Where != nil {
		sel.Where.Accept(colCollector)
	}
	if colCollector.err != nil {
		return nil, colCollector.err
	}

	return &MVQuery{
		BaseSchema:           baseSchema,
		BaseTable:            baseTN.Name,
		BaseAlias:            baseAlias,
		SelectItems:          selectItems,
		GroupByBaseColumnIDs: groupByIDs,
		UsedBaseColumnIDs:    usedIDs,
	}, nil
}

type mvColumnCollector struct {
	baseSchema ast.CIStr
	baseTable  ast.CIStr
	baseAlias  ast.CIStr
	baseInfo   *model.TableInfo

	usedIDs map[int64]struct{}
	err     error
}

func (v *mvColumnCollector) Enter(n ast.Node) (ast.Node, bool) {
	if v.err != nil {
		return n, true
	}
	colExpr, ok := n.(*ast.ColumnNameExpr)
	if !ok {
		return n, false
	}
	colID, err := columnNameToBaseColumnID(v.baseSchema, v.baseTable, v.baseAlias, v.baseInfo, colExpr.Name)
	if err != nil {
		v.err = err
		return n, true
	}
	v.usedIDs[colID] = struct{}{}
	return n, true
}

func (*mvColumnCollector) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

func DeriveMVColumnName(baseInfo *model.TableInfo, field *ast.SelectField, item MVSelectItem) (ast.CIStr, error) {
	if field != nil && field.AsName.L != "" {
		return field.AsName, nil
	}
	if item.IsGroupBy {
		colExpr, ok := field.Expr.(*ast.ColumnNameExpr)
		if !ok || colExpr.Name == nil {
			return ast.CIStr{}, errors.New("invalid group-by select field")
		}
		return colExpr.Name.Name, nil
	}
	switch item.AggKind {
	case MVAggKindCountStar:
		return ast.NewCIStr("cnt"), nil
	case MVAggKindSum, MVAggKindMin, MVAggKindMax:
		argCol := model.FindColumnInfoByID(baseInfo.Columns, item.ArgBaseColumnID)
		if argCol == nil {
			return ast.CIStr{}, errors.New("materialized view aggregate argument column not found")
		}
		prefix := "sum"
		if item.AggKind == MVAggKindMin {
			prefix = "min"
		} else if item.AggKind == MVAggKindMax {
			prefix = "max"
		}
		return ast.NewCIStr(fmt.Sprintf("%s_%s", prefix, argCol.Name.O)), nil
	default:
		return ast.CIStr{}, errors.New("invalid aggregate kind")
	}
}

func AllocUniqueColumnName(existing map[string]struct{}, suggested ast.CIStr) ast.CIStr {
	if existing == nil {
		existing = make(map[string]struct{})
	}
	if suggested.L == "" {
		suggested = ast.NewCIStr("c1")
	}
	if _, ok := existing[suggested.L]; !ok {
		existing[suggested.L] = struct{}{}
		return suggested
	}
	for i := 1; ; i++ {
		name := ast.NewCIStr(fmt.Sprintf("%s_%d", suggested.O, i))
		if _, ok := existing[name.L]; ok {
			continue
		}
		existing[name.L] = struct{}{}
		return name
	}
}
