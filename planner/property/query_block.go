package property

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/planner/util"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	atomic2 "go.uber.org/atomic"
)

// TODO: order by, union, union all, union scan, lock, cte

// QueryBlock can be transformed to a SELECT statement easily. It's a helper for restoring a plan tree back to a SQL.
// We place it here for now because it's placed in StatsInfo currently, which is also defined in this package.
type QueryBlock struct {
	JoinList    JoinList
	WhereConds  []string
	GroupByCols []string
	HavingConds []string
	Limit       int64
	Offset      int64

	TableCols     map[int64]*TableCol
	ProjectedCols map[int64]*ProjectedCol

	Stage Stage

	OutputCol map[int64]string

	TblNameAlloc *atomic2.Uint64
	ColNameAlloc *atomic2.Uint64
}

type Stage uint

const (
	StageJoin Stage = iota
	StageWhere
	StageProjection
	StageAgg
	StageWindow
	StageOrderBy
	StageFinalized
)

type ProjectedCol struct {
	Expr   string
	AsName string
	Needed bool
}

type TableCol struct {
	ColName string
	TblName string
}

func (q *QueryBlock) AllocTblName() string {
	tid := q.TblNameAlloc.Inc()
	name := fmt.Sprintf("tbl_%d", tid)
	return name
}

func (q *QueryBlock) AllocColName() string {
	tid := q.ColNameAlloc.Inc()
	name := fmt.Sprintf("col_%d", tid)
	return name
}

func (q *QueryBlock) Clone() *QueryBlock {
	if q == nil {
		return nil
	}
	res := &QueryBlock{
		JoinList:      make(JoinList, len(q.JoinList)),
		WhereConds:    make([]string, len(q.WhereConds)),
		GroupByCols:   make([]string, len(q.GroupByCols)),
		HavingConds:   make([]string, len(q.HavingConds)),
		Limit:         q.Limit,
		Offset:        q.Offset,
		TableCols:     make(map[int64]*TableCol, len(q.TableCols)),
		ProjectedCols: make(map[int64]*ProjectedCol, len(q.ProjectedCols)),
		Stage:         q.Stage,
		OutputCol:     make(map[int64]string, len(q.OutputCol)),
		TblNameAlloc:  q.TblNameAlloc,
		ColNameAlloc:  q.ColNameAlloc,
	}
	copy(res.JoinList, q.JoinList)
	copy(res.WhereConds, q.WhereConds)
	copy(res.GroupByCols, q.GroupByCols)
	copy(res.HavingConds, q.HavingConds)
	for k := range q.OutputCol {
		res.OutputCol[k] = q.OutputCol[k]
	}
	for k := range q.ProjectedCols {
		res.ProjectedCols[k] = q.ProjectedCols[k]
	}
	for k := range q.TableCols {
		res.TableCols[k] = q.TableCols[k]
	}
	return res
}

func (q *QueryBlock) GenQBNotAfter(s Stage) *QueryBlock {
	var query *QueryBlock
	if q.Stage <= s {
		query = q.Clone()
	} else {
		query = q.GenOuterQB()
	}
	return query
}

func (q *QueryBlock) GenOuterQB() *QueryBlock {
	ji, outputCols := q.Convert2JoinItem()
	res := &QueryBlock{
		Stage:        StageJoin,
		JoinList:     JoinList{ji},
		TblNameAlloc: q.TblNameAlloc,
		ColNameAlloc: q.ColNameAlloc,
	}
	res.AddTblColsFromOutputCols(ji.AsName, outputCols)
	return res
}

func (q *QueryBlock) Convert2JoinItem() (*JoinItem, map[int64]string) {
	j := JoinItem{SubQuery: q}
	j.AsName = q.AllocTblName()
	return &j, q.OutputCol
}

func (q *QueryBlock) AddTblColsFromQB(q2 *QueryBlock) {
	if q.TableCols == nil {
		q.TableCols = make(map[int64]*TableCol)
	}
	for k, v := range q2.TableCols {
		q.TableCols[k] = v
	}
}

func (q *QueryBlock) AddTblColsFromOutputCols(tblName string, cols map[int64]string) {
	if q.TableCols == nil {
		q.TableCols = make(map[int64]*TableCol)
	}
	for uid, name := range cols {
		q.TableCols[uid] = &TableCol{
			ColName: name,
			TblName: tblName,
		}
	}
}

func (q *QueryBlock) AddTblColsFromSlice(tblName string, colUIDs []int64, names []string) {
	if q.TableCols == nil {
		q.TableCols = make(map[int64]*TableCol)
	}
	for i, col := range colUIDs {
		q.TableCols[col] = &TableCol{
			ColName: names[i],
			TblName: tblName,
		}
	}
	return
}

func (q *QueryBlock) AddProjCol(colUID int64, expr string) {
	if q.ProjectedCols == nil {
		q.ProjectedCols = make(map[int64]*ProjectedCol, 0)
	}
	name := q.AllocColName()
	q.ProjectedCols[colUID] = &ProjectedCol{
		Expr:   expr,
		AsName: name,
		Needed: false,
	}
}

func (q *QueryBlock) AddOutputCol(colUID int64) {
	// Allocate a unique name for every output column to avoid duplicate names in the SELECT fields,
	// especially when we are joining tables and some of them have the same column names.
	name := ""
	if col, ok := q.ProjectedCols[colUID]; ok {
		name = col.AsName
	} else {
		name = q.AllocColName()
	}
	if q.OutputCol == nil {
		q.OutputCol = make(map[int64]string)
	}
	q.OutputCol[colUID] = name
}

func (q *QueryBlock) ResetOutputCol() {
	q.OutputCol = make(map[int64]string)
}

type JoinList []*JoinItem

type JoinItem struct {
	// case 1: it's a table
	Table string

	// case 2: it's a sub query block
	SubQuery *QueryBlock

	// case 3: it's a result of join(s)
	SubJoinList JoinList

	// Note: the first JoinItem in a JoinList doesn't have JoinCond and JoinType.
	JoinCond []string
	JoinType string

	// Each Table and SubQuery will be allocated a unique AsName (SubJoinList won't have AsName).
	AsName string
}

func (q *QueryBlock) FindAndMarkColName(uid int64, useProjected bool) (res string, ok bool) {
	if tblCol, ok := q.TableCols[uid]; ok {
		if len(tblCol.TblName) > 0 {
			res += "`" + tblCol.TblName + "`."
		}
		res += "`" + tblCol.ColName + "`"
		return res, true
	} else if col, ok := q.ProjectedCols[uid]; ok {
		if useProjected {
			col.Needed = true
			res += col.AsName
		} else {
			res += col.Expr
		}
		return res, true
	}
	return "", false
}

func (q *QueryBlock) String() string {
	res1 := "SELECT "
	res2 := ""
	first := true
	for uid, asname := range q.OutputCol {
		if !first {
			res2 += ", "
		}
		name, _ := q.FindAndMarkColName(uid, false)
		res2 += name
		if len(asname) == 0 || asname == name {
			continue
		}
		res2 += " AS `" + asname + "`"
		first = false
	}
	for uid, col := range q.ProjectedCols {
		if !col.Needed {
			continue
		}
		if _, ok := q.OutputCol[uid]; ok {
			continue
		}
		res2 += ", "
		res2 += col.Expr + " AS `" + col.AsName + "`"
	}
	res3 := " FROM " + q.JoinList.String()
	if len(q.WhereConds) > 0 {
		res3 += " WHERE "
		first := true
		for _, cond := range q.WhereConds {
			if !first {
				res3 += " AND "
			}
			res3 += cond
			first = false
		}
	}
	if len(q.GroupByCols) > 0 {
		res3 += " GROUP BY "
		first := true
		for _, groupBy := range q.GroupByCols {
			if !first {
				res3 += ", "
			}
			res3 += groupBy
			first = false
		}
	}
	if len(q.HavingConds) > 0 {
		res3 += " HAVING "
		first := true
		for _, cond := range q.HavingConds {
			if !first {
				res3 += " AND "
			}
			res3 += cond
			first = false
		}
	}
	return res1 + res2 + res3
}

func (jl JoinList) String() string {
	res := ""
	first := true
	for _, item := range jl {
		if !first {
			res += " " + item.JoinType + " "
		}
		if len(item.Table) > 0 {
			res += item.Table
		} else if len(item.SubJoinList) > 0 {
			res += "(" + item.SubJoinList.String() + ")"
		} else {
			res += "(" + item.SubQuery.String() + ")"
		}
		if len(item.AsName) > 0 {
			res += " AS `" + item.AsName + "`"
		}
		if !first && len(item.JoinCond) > 0 {
			res += " ON "
			first2 := true
			for _, cond := range item.JoinCond {
				if !first2 {
					res += " AND "
				}
				res += cond
				first2 = false
			}
		}
		first = false
	}
	return res
}

func (q *QueryBlock) ExprToString(e expression.Expression, useProjectedCol bool) (string, error) {
	switch expr := e.(type) {
	case *expression.ScalarFunction:
		var buffer bytes.Buffer
		buffer.WriteString("`" + expr.FuncName.L + "`(")
		for i, arg := range expr.GetArgs() {
			argStr, err := q.ExprToString(arg, useProjectedCol)
			if err != nil {
				return "", err
			}
			buffer.WriteString(argStr)
			if i+1 != len(expr.GetArgs()) {
				buffer.WriteString(", ")
			}
		}
		// TODO: cast
		buffer.WriteString(")")
		return buffer.String(), nil
	case *expression.Column:
		uid := expr.UniqueID
		name, ok := q.FindAndMarkColName(uid, useProjectedCol)
		if ok {
			return name, nil
		}
		return expr.String(), nil
	case *expression.CorrelatedColumn:
		return "", errors.New("tracing for correlated columns not supported now")
	case *expression.Constant:
		value, err := expr.Eval(chunk.Row{})
		if err != nil {
			return "", err
		}
		valueExpr := driver.ValueExpr{Datum: value}
		var buffer bytes.Buffer
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buffer)
		err = valueExpr.Restore(restoreCtx)
		if err != nil {
			return "", err
		}
		return buffer.String(), nil
	}
	return "", errors.New("unexpected type of Expression")
}

func (q *QueryBlock) ByItemToString(items []*util.ByItems, useProjectedCol bool) (string, error) {
	str := ""
	for i, item := range items {
		if i != 0 {
			str += ", "
		}
		s, err := q.ExprToString(item.Expr, useProjectedCol)
		if err != nil {
			return s, err
		}
		str += s
		if item.Desc {
			str += " DESC "
		}
	}
	return str, nil
}

func (q *QueryBlock) AggFuncToString(agg *aggregation.AggFuncDesc) (string, error) {
	if agg.Name == "firstrow" {
		s, err := q.ExprToString(agg.Args[0], false)
		if err != nil {
			return s, err
		}
		return s, nil
	}
	str := agg.Name + "("
	first := true
	for i := 0; i < len(agg.Args)-1; i++ {
		if !first {
			str += ", "
			first = false
		}
		s, err := q.ExprToString(agg.Args[i], false)
		if err != nil {
			return s, err
		}
		str += s
	}
	if agg.Name == "group_concat" {
		if agg.OrderByItems != nil {
			str += " ORDER BY "
			s, err := q.ByItemToString(agg.OrderByItems, false)
			if err != nil {
				return s, err
			}
			str += s
		}
		str += " SEPARATOR "
		s, err := q.ExprToString(agg.Args[len(agg.Args)-1], false)
		if err != nil {
			return s, err
		}
		str += s
	} else {
		if !first {
			str += ", "
		}
		s, err := q.ExprToString(agg.Args[len(agg.Args)-1], false)
		if err != nil {
			return s, err
		}
		str += s
	}
	str += ")"
	return str, nil
}
