package core

import (
	context2 "context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics"
)

func dbName(fullName string) string {
	tmp := strings.Split(strings.ToLower(fullName), ".")
	return tmp[0]
}

func tblName(fullName string) string {
	tmp := strings.Split(strings.ToLower(fullName), ".")
	if len(tmp) < 2 {
		return ""
	}

	return tmp[0] + "." + tmp[1]
}

type UnityColumnInfo struct {
	NDV       int
	Nulls     int
	Min       string
	Max       string
	Histogram []UnityHistBucket
}

type UnityIndexInfo struct {
	NDV   int
	Nulls int
}

type UnityHistBucket struct {
	Lower string
	Upper string
	Count int64
}

type UnityTableInfo struct {
	AsName       string
	Columns      map[string]*UnityColumnInfo // db.table.col
	Indexes      map[string]*UnityIndexInfo  // db.table.index
	RealtimeRows int64
	ModifiedRows int64

	stats  *statistics.Table `json:"-"`
	col2id map[string]int64  `json:"-"`
	idx2id map[string]int64  `json:"-"`
}

type UnityOutput struct {
	Tables map[string]*UnityTableInfo
	Hints  []string

	joins map[string]string
}

func collectColumn(c *expression.Column, o *UnityOutput) {
	colName := strings.ToLower(c.OrigName)
	tName := tblName(colName)
	if tName == "" {
		return
	}
	o.Tables[tName].Columns[colName] = &UnityColumnInfo{}
}

func collectColumnFromExpr(expr expression.Expression, o *UnityOutput) {
	switch x := expr.(type) {
	case *expression.Column:
		collectColumn(x, o)
	case *expression.CorrelatedColumn:
		collectColumn(&x.Column, o)
	case *expression.ScalarFunction:
		for _, arg := range x.GetArgs() {
			collectColumnFromExpr(arg, o)
		}
	}
}

func tablesInExpr(expr expression.Expression) map[string]bool {
	m := make(map[string]bool)
	switch x := expr.(type) {
	case *expression.Column:
		m[tblName(x.OrigName)] = true
	case *expression.CorrelatedColumn:
		m[tblName(x.OrigName)] = true
	case *expression.ScalarFunction:
		for _, arg := range x.GetArgs() {
			am := tablesInExpr(arg)
			for k, v := range am {
				m[k] = v
			}
		}
	}
	return m
}

func collectJoins(expr expression.Expression, o *UnityOutput) {
	tbls := tablesInExpr(expr)
	for t1 := range tbls {
		for t2 := range tbls {
			if t1 == t2 {
				continue
			}
			o.joins[t1] = t2
			o.joins[t2] = t1
		}
	}
}

func collectUnityInfo(p base.LogicalPlan, o *UnityOutput) {
	for _, child := range p.Children() {
		collectUnityInfo(child, o)
	}
	switch x := p.(type) {
	case *DataSource:
		tableName := x.DBName.L + "." + x.tableInfo.Name.L
		if _, ok := o.Tables[tableName]; !ok {
			o.Tables[tableName] = &UnityTableInfo{
				AsName:  x.TableAsName.L,
				Columns: map[string]*UnityColumnInfo{},
				Indexes: map[string]*UnityIndexInfo{},
				stats:   x.statisticTable,
				col2id:  map[string]int64{},
				idx2id:  map[string]int64{},
			}
			for _, col := range x.tableInfo.Columns {
				colName := tableName + "." + col.Name.L
				o.Tables[tableName].col2id[colName] = col.ID
			}
			for _, idx := range x.tableInfo.Indices {
				idxName := tableName + "." + idx.Name.L
				o.Tables[tableName].Indexes[idxName] = &UnityIndexInfo{}
				o.Tables[tableName].idx2id[idxName] = idx.ID
			}
			//if x.tableInfo.PKIsHandle || x.tableInfo.IsCommonHandle {
			//	idxName := tableName+".primary"
			//	result[tableName].Indexes[idxName] = &UnityIndexInfo{}
			//}
		}
		for _, expr := range x.allConds {
			collectColumnFromExpr(expr, o)
		}
	case *LogicalSelection:
		for _, expr := range x.Conditions {
			collectColumnFromExpr(expr, o)
		}
	case *LogicalJoin:
		for _, expr := range x.EqualConditions {
			collectColumnFromExpr(expr, o)
			collectJoins(expr, o)
		}
		for _, expr := range x.NAEQConditions {
			collectColumnFromExpr(expr, o)
			collectJoins(expr, o)
		}
		for _, expr := range x.LeftConditions {
			collectColumnFromExpr(expr, o)
			collectJoins(expr, o)
		}
		for _, expr := range x.RightConditions {
			collectColumnFromExpr(expr, o)
			collectJoins(expr, o)
		}
		for _, expr := range x.OtherConditions {
			collectColumnFromExpr(expr, o)
			collectJoins(expr, o)
		}
	case *LogicalAggregation:
		for _, expr := range x.GroupByItems {
			collectColumnFromExpr(expr, o)
		}
		for _, agg := range x.AggFuncs {
			for _, expr := range agg.Args {
				collectColumnFromExpr(expr, o)
			}
		}
	case *LogicalSort:
		for _, item := range x.ByItems {
			collectColumnFromExpr(item.Expr, o)
		}
	default:
	}
}

func fillUpStats(o *UnityOutput) {
	for _, tblInfo := range o.Tables {
		tblStats := tblInfo.stats
		tblInfo.ModifiedRows = tblStats.ModifyCount
		tblInfo.RealtimeRows = tblStats.RealtimeCount
		for colName, col := range tblInfo.Columns {
			colStats := tblStats.Columns[tblInfo.col2id[colName]]
			col.NDV = int(colStats.NDV)
			col.Nulls = int(colStats.NullCount)

			hist := colStats.Histogram
			buckets := make([]UnityHistBucket, 0)
			lastCount := int64(0)
			for i := 0; i < colStats.Histogram.Len(); i++ {
				lower, err := hist.GetLower(i).ToString()
				must(err)
				upper, err := hist.GetUpper(i).ToString()
				must(err)
				count := hist.Buckets[i].Count + hist.Buckets[i].Repeat
				buckets = append(buckets, UnityHistBucket{
					Lower: lower,
					Upper: upper,
					Count: count - lastCount,
				})
				lastCount = count
			}
			col.Histogram = buckets
			if len(buckets) > 0 {
				col.Min = buckets[0].Lower
				col.Max = buckets[len(buckets)-1].Upper
			}
		}
		for idxName, idx := range tblInfo.Indexes {
			idxStats := tblStats.Indices[tblInfo.idx2id[idxName]]
			idx.NDV = int(idxStats.NDV)
			idx.Nulls = int(idxStats.NullCount)
		}
	}
}

func getPossibleHints(ctx context.PlanContext, o *UnityOutput) {
	possibleHints := make(map[string]bool)
	var hintTableNames []string
	for tableName, tblInfo := range o.Tables {
		hintTableName := tableName
		if tblInfo.AsName != "" {
			hintTableName = dbName(tableName) + "." + tblInfo.AsName
		}
		hintTableNames = append(hintTableNames, hintTableName)
		// use_index hint
		for idxName := range tblInfo.Indexes {
			possibleHints[fmt.Sprintf("use_index(%v, %v)", hintTableName, idxName)] = true
		}
	}
	if len(hintTableNames) >= 2 {
		// join hint
		for _, t := range hintTableNames {
			possibleHints[fmt.Sprintf("hash_join(%v)", t)] = true
			possibleHints[fmt.Sprintf("merge_join(%v)", t)] = true
			possibleHints[fmt.Sprintf("index_join(%v)", t)] = true
			possibleHints[fmt.Sprintf("no_hash_join(%v)", t)] = true
			possibleHints[fmt.Sprintf("no_merge_join(%v)", t)] = true
			possibleHints[fmt.Sprintf("no_index_join(%v)", t)] = true
		}
		// hash join all
		possibleHints[fmt.Sprintf("hash_join(%v)", strings.Join(hintTableNames, ", "))] = true
		possibleHints[fmt.Sprintf("index_join(%v)", strings.Join(hintTableNames, ", "))] = true
		possibleHints[fmt.Sprintf("merge_join(%v)", strings.Join(hintTableNames, ", "))] = true
		possibleHints[fmt.Sprintf("no_hash_join(%v)", strings.Join(hintTableNames, ", "))] = true
		possibleHints[fmt.Sprintf("no_index_join(%v)", strings.Join(hintTableNames, ", "))] = true
		possibleHints[fmt.Sprintf("no_merge_join(%v)", strings.Join(hintTableNames, ", "))] = true

		// leading hint
		for t1, t2 := range o.joins {
			if t1 == t2 {
				continue
			}
			possibleHints[fmt.Sprintf("leading(%v, %v)", t1, t2)] = true
		}
	}

	// explain format='unity' select * from t1 where a<1
	sctx, err := AsSctx(ctx)
	must(err)
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	selectIdx := -1
	for _, str := range []string{"select", "SELECT", "Select"} {
		selectIdx = strings.Index(sctx.GetSessionVars().StmtCtx.OriginalSQL, str)
		if selectIdx != -1 {
			break
		}
	}
	if selectIdx != -1 { // verify
		tmp := make(map[string]bool)
		for h := range possibleHints {
			q := fmt.Sprintf("select /*+ %v */ %v", h, sctx.GetSessionVars().StmtCtx.OriginalSQL[selectIdx+6:])
			stmt := parseSQL(q)
			sctx.GetSessionVars().StmtCtx.TruncateWarnings(0)
			_, _, err = OptimizeAstNode(context2.Background(), sctx, stmt, is)
			if sctx.GetSessionVars().StmtCtx.WarningCount() == 0 {
				tmp[h] = true
			} else {
				//fmt.Println(">>>> ", sctx.GetSessionVars().StmtCtx.GetWarnings(), q)
			}
		}
		possibleHints = tmp
	}

	o.Hints = make([]string, 0, len(possibleHints))
	for h := range possibleHints {
		o.Hints = append(o.Hints, h)
	}
}

func prepareForUnity(ctx context.PlanContext, p base.LogicalPlan) string {
	o := &UnityOutput{
		Tables: make(map[string]*UnityTableInfo),
		joins:  make(map[string]string),
	}
	collectUnityInfo(p, o)
	fillUpStats(o)
	getPossibleHints(ctx, o)

	v, err := json.Marshal(o)
	must(err)
	return string(v)
}

func parseSQL(q string) ast.StmtNode {
	p := parser.New()
	stmt, err := p.ParseOneStmt(q, "", "")
	must(err)
	return stmt
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
