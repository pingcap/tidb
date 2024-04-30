package core

import (
	context2 "context"
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/pkg/util/intest"
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
}

func collectColumn(c *expression.Column, result map[string]*UnityTableInfo) {
	colName := strings.ToLower(c.OrigName)
	result[tblName(colName)].Columns[colName] = &UnityColumnInfo{}
}

func collectColumnFromExpr(expr expression.Expression, result map[string]*UnityTableInfo) {
	switch x := expr.(type) {
	case *expression.Column:
		collectColumn(x, result)
	case *expression.CorrelatedColumn:
		collectColumn(&x.Column, result)
	case *expression.ScalarFunction:
		for _, arg := range x.GetArgs() {
			collectColumnFromExpr(arg, result)
		}
	}
}

func collectUnityInfo(p base.LogicalPlan, result map[string]*UnityTableInfo) {
	for _, child := range p.Children() {
		collectUnityInfo(child, result)
	}
	switch x := p.(type) {
	case *DataSource:
		tableName := x.DBName.L + "." + x.tableInfo.Name.L
		if _, ok := result[tableName]; !ok {
			result[tableName] = &UnityTableInfo{
				AsName:  x.TableAsName.L,
				Columns: map[string]*UnityColumnInfo{},
				Indexes: map[string]*UnityIndexInfo{},
				stats:   x.statisticTable,
				col2id:  map[string]int64{},
				idx2id:  map[string]int64{},
			}
			for _, col := range x.tableInfo.Columns {
				colName := tableName + "." + col.Name.L
				result[tableName].col2id[colName] = col.ID
			}
			for _, idx := range x.tableInfo.Indices {
				idxName := tableName + "." + idx.Name.L
				result[tableName].Indexes[idxName] = &UnityIndexInfo{}
				result[tableName].idx2id[idxName] = idx.ID
			}
			//if x.tableInfo.PKIsHandle || x.tableInfo.IsCommonHandle {
			//	idxName := tableName+".primary"
			//	result[tableName].Indexes[idxName] = &UnityIndexInfo{}
			//}
		}
		for _, expr := range x.allConds {
			collectColumnFromExpr(expr, result)
		}
	case *LogicalSelection:
		for _, expr := range x.Conditions {
			collectColumnFromExpr(expr, result)
		}
	case *LogicalJoin:
		for _, expr := range x.EqualConditions {
			collectColumnFromExpr(expr, result)
		}
		for _, expr := range x.NAEQConditions {
			collectColumnFromExpr(expr, result)
		}
		for _, expr := range x.LeftConditions {
			collectColumnFromExpr(expr, result)
		}
		for _, expr := range x.RightConditions {
			collectColumnFromExpr(expr, result)
		}
		for _, expr := range x.OtherConditions {
			collectColumnFromExpr(expr, result)
		}
	case *LogicalAggregation:
		for _, expr := range x.GroupByItems {
			collectColumnFromExpr(expr, result)
		}
	case *LogicalSort:
		for _, item := range x.ByItems {
			collectColumnFromExpr(item.Expr, result)
		}
	default:
	}
}

func fillUpStats(result map[string]*UnityTableInfo) {
	for _, tblInfo := range result {
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

			if intest.InTest {
				col.Histogram = []UnityHistBucket{}
			}
		}
		for idxName, idx := range tblInfo.Indexes {
			idxStats := tblStats.Indices[tblInfo.idx2id[idxName]]
			idx.NDV = int(idxStats.NDV)
			idx.Nulls = int(idxStats.NullCount)
		}
	}
}

func getPossibleHints(ctx context.PlanContext, result map[string]*UnityTableInfo) []string {
	possibleHints := make(map[string]bool)
	var hintTableNames []string
	for tableName, tblInfo := range result {
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

		// leading hint
		for _, t1 := range hintTableNames {
			for _, t2 := range hintTableNames {
				if t1 == t2 {
					continue
				}
				possibleHints[fmt.Sprintf("leading(%v, %v)", t1, t2)] = true
			}
		}
	}

	// TODO: verify
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

	hints := make([]string, 0, len(possibleHints))
	for h := range possibleHints {
		hints = append(hints, h)
	}
	return hints
}

func prepareForUnity(ctx context.PlanContext, p base.LogicalPlan) string {
	result := make(map[string]*UnityTableInfo)
	collectUnityInfo(p, result)
	fillUpStats(result)
	hints := getPossibleHints(ctx, result)

	v, err := json.Marshal(UnityOutput{
		Tables: result,
		Hints:  hints,
	})
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
