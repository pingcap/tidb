package sql_restorer

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/expression"
	atomic2 "go.uber.org/atomic"
)

// QueryBlock can be transformed to a SELECT statement easily. It's a helper for restoring a plan tree back to a SQL.
// We place it in this package for now because it's placed in StatsInfo currently, which is also defined in this package.
type QueryBlock struct {
	joinList    joinList
	WhereConds  []string
	GroupByCols []string
	HavingConds []string
	Limit       uint64
	Offset      uint64

	tableCols     map[int64]*tableCol
	projectedCols map[int64]*projectedCol

	Stage Stage

	outputCol map[int64]string

	tblNameAlloc *atomic2.Uint64
	colNameAlloc *atomic2.Uint64
}

func newQB() *QueryBlock {
	q := &QueryBlock{}
	q.tableCols = make(map[int64]*tableCol, len(q.tableCols))
	q.projectedCols = make(map[int64]*projectedCol, len(q.projectedCols))
	q.outputCol = make(map[int64]string, len(q.outputCol))
	return q
}

type joinList []*joinItem

type joinItem struct {
	// case 1: it's a table
	Table string

	// case 2: it's a subquery
	SubQuery *QueryBlock

	// case 3: it's a result of join(s)
	SubJoinList joinList

	// Note: the first joinItem in a joinList doesn't have JoinCond and JoinType.
	JoinCond []string
	JoinType string

	// Each Table and SubQuery will be allocated a unique AsName (SubJoinList won't have AsName).
	AsName string
}

type Stage uint

const (
	StageJoin Stage = iota
	StageWhere
	StageProjection
	StageAgg
	StageWindow
	StageOrderBy
	StageLimit
)

type projectedCol struct {
	Expr   string
	AsName string
	Needed bool
}

type tableCol struct {
	ColName string
	TblName string
}

func (q *QueryBlock) allocTblName() string {
	tid := q.tblNameAlloc.Inc()
	name := fmt.Sprintf("tbl_%d", tid)
	return name
}

func (q *QueryBlock) allocColName() string {
	tid := q.colNameAlloc.Inc()
	name := fmt.Sprintf("col_%d", tid)
	return name
}

func (q *QueryBlock) Clone() *QueryBlock {
	if q == nil {
		return nil
	}
	res := &QueryBlock{
		joinList:      make(joinList, len(q.joinList)),
		WhereConds:    make([]string, len(q.WhereConds)),
		GroupByCols:   make([]string, len(q.GroupByCols)),
		HavingConds:   make([]string, len(q.HavingConds)),
		Limit:         q.Limit,
		Offset:        q.Offset,
		tableCols:     make(map[int64]*tableCol, len(q.tableCols)),
		projectedCols: make(map[int64]*projectedCol, len(q.projectedCols)),
		Stage:         q.Stage,
		outputCol:     make(map[int64]string, len(q.outputCol)),
		tblNameAlloc:  q.tblNameAlloc,
		colNameAlloc:  q.colNameAlloc,
	}
	copy(res.joinList, q.joinList)
	copy(res.WhereConds, q.WhereConds)
	copy(res.GroupByCols, q.GroupByCols)
	copy(res.HavingConds, q.HavingConds)
	for k := range q.outputCol {
		res.outputCol[k] = q.outputCol[k]
	}
	for k := range q.projectedCols {
		res.projectedCols[k] = q.projectedCols[k]
	}
	for k := range q.tableCols {
		res.tableCols[k] = q.tableCols[k]
	}
	return res
}

func NewQBFromTable(tblName string, colIDs []int64, colNames []string, tblNameAlloc, colNameAlloc *atomic2.Uint64) *QueryBlock {
	res := newQB()
	res.Stage = StageJoin
	res.tblNameAlloc = tblNameAlloc
	res.colNameAlloc = colNameAlloc
	tbl := &joinItem{Table: tblName}
	tbl.AsName = res.allocTblName()
	res.joinList = joinList{tbl}
	res.addTblColsFromSlice(tbl.AsName, colIDs, colNames)
	return res
}

// GenQBNotAfter returns an equivalent QueryBlock with its Stage not after s.
// Note: this method assumes that output columns of this QueryBlock has been set.
func (q *QueryBlock) GenQBNotAfter(s Stage) *QueryBlock {
	if q.Stage <= s {
		return q.Clone()
	}
	return q.wrapAsSubquery()
}

func (q *QueryBlock) wrapAsSubquery() *QueryBlock {
	ji := &joinItem{SubQuery: q}
	ji.AsName = q.allocTblName()
	res := newQB()
	res.Stage = StageJoin
	res.joinList = joinList{ji}
	res.tblNameAlloc = q.tblNameAlloc
	res.colNameAlloc = q.colNameAlloc
	res.addTblColsFromOutputCols(ji.AsName, q.outputCol)
	return res
}

// JoinQB join another QueryBlock with this QueryBlock
// Note: this method assumes both QueryBlocks are in StageJoin.
func (q *QueryBlock) JoinQB(right *QueryBlock, joinType string, joinConds []expression.Expression) {
	var r *joinItem
	if len(right.joinList) == 1 {
		r = right.joinList[0]
	} else {
		r = &joinItem{
			SubJoinList: right.joinList,
		}
	}
	q.joinList = append(q.joinList, r)
	q.addTblColsFromQB(right)
	ji := q.joinList[len(q.joinList)-1]
	ji.JoinType = joinType
	for _, cond := range joinConds {
		s, err := q.ExprToString(cond, false)
		if err != nil {
			panic(err)
		}
		ji.JoinCond = append(ji.JoinCond, s)
	}
}

func (q *QueryBlock) addTblColsFromQB(q2 *QueryBlock) {
	for k, v := range q2.tableCols {
		q.tableCols[k] = v
	}
}

func (q *QueryBlock) addTblColsFromOutputCols(tblName string, cols map[int64]string) {
	for uid, name := range cols {
		q.tableCols[uid] = &tableCol{
			ColName: name,
			TblName: tblName,
		}
	}
}

func (q *QueryBlock) addTblColsFromSlice(tblName string, colUIDs []int64, names []string) {
	for i, col := range colUIDs {
		q.tableCols[col] = &tableCol{
			ColName: names[i],
			TblName: tblName,
		}
	}
	return
}

func (q *QueryBlock) AddProjCol(colUID int64, expr string) {
	name := q.allocColName()
	q.projectedCols[colUID] = &projectedCol{
		Expr:   expr,
		AsName: name,
		Needed: false,
	}
}

func (q *QueryBlock) AddOutputCol(colUID int64) {
	// Allocate a unique name for every output column to avoid duplicate names in the SELECT fields,
	// especially when we are joining tables and some of them have the same column names.
	name := ""
	if col, ok := q.projectedCols[colUID]; ok {
		name = col.AsName
	} else {
		name = q.allocColName()
	}
	q.outputCol[colUID] = name
}

func (q *QueryBlock) ResetOutputCol() {
	q.outputCol = make(map[int64]string)
}

const unknownColumnPlaceholder = "%COLUMN%"

func (q *QueryBlock) ContainUnknownCol() bool {
	if q.joinList.containUnknownCol() {
		return true
	}
	for _, str := range q.WhereConds {
		if strings.Contains(str, unknownColumnPlaceholder) {
			return true
		}
	}
	for _, col := range q.projectedCols {
		if strings.Contains(col.Expr, unknownColumnPlaceholder) {
			return true
		}
	}
	for _, str := range q.HavingConds {
		if strings.Contains(str, unknownColumnPlaceholder) {
			return true
		}
	}
	for _, str := range q.GroupByCols {
		if strings.Contains(str, unknownColumnPlaceholder) {
			return true
		}
	}
	return false
}

func (jl joinList) containUnknownCol() bool {
	for _, item := range jl {
		for _, cond := range item.JoinCond {
			if strings.Contains(cond, unknownColumnPlaceholder) {
				return true
			}
		}
		if len(item.SubJoinList) > 0 {
			if item.SubJoinList.containUnknownCol() {
				return true
			}
		} else if item.SubQuery != nil {
			if item.SubQuery.ContainUnknownCol() {
				return true
			}
		}
	}
	return false
}

// Decorrelate generate the name of column of the uid from the left,
// then replace the unknown columns of the same uid in the right with this name.
func (q *QueryBlock) Decorrelate(uid int64, left *QueryBlock) {
	s, _ := left.findAndMarkColName(uid, false)
	q.replaceUnknownCol(uid, s)
}

func (q *QueryBlock) replaceUnknownCol(uid int64, name string) bool {
	q.joinList.replaceUnknownCol(uid, name)

	var newConds []string
	for _, cond := range q.WhereConds {
		newCond := strings.ReplaceAll(cond, unknownColumnPlaceholder+strconv.FormatInt(uid, 10), name)
		newConds = append(newConds, newCond)
	}
	q.WhereConds = newConds

	for _, col := range q.projectedCols {
		col.Expr = strings.ReplaceAll(col.Expr, unknownColumnPlaceholder+strconv.FormatInt(uid, 10), name)
	}

	newConds = make([]string, 0)
	for _, cond := range q.HavingConds {
		newCond := strings.ReplaceAll(cond, unknownColumnPlaceholder+strconv.FormatInt(uid, 10), name)
		newConds = append(newConds, newCond)
	}
	q.HavingConds = newConds

	newConds = make([]string, 0)
	for _, cond := range q.GroupByCols {
		newCond := strings.ReplaceAll(cond, unknownColumnPlaceholder+strconv.FormatInt(uid, 10), name)
		newConds = append(newConds, newCond)
	}
	q.GroupByCols = newConds
	return false
}

func (jl joinList) replaceUnknownCol(uid int64, name string) {
	for _, item := range jl {
		var newConds []string
		for _, cond := range item.JoinCond {
			newCond := strings.ReplaceAll(cond, unknownColumnPlaceholder+strconv.FormatInt(uid, 10), name)
			newConds = append(newConds, newCond)
		}
		item.JoinCond = newConds
		if len(item.SubJoinList) > 0 {
			item.SubJoinList.replaceUnknownCol(uid, name)
		} else if item.SubQuery != nil {
			item.SubQuery.replaceUnknownCol(uid, name)
		}
	}
}
