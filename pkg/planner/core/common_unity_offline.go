package core

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func (e *Explain) UnityOffline() string {
	p := parser.New()
	stmt, err := p.ParseOneStmt(e.SCtx().GetSessionVars().StmtCtx.OriginalSQL, "", "")
	must(err)
	tableNames := collectTableNames(e.SCtx().GetSessionVars().CurrentDB, stmt)
	leadingHints := e.unityOfflineIterateLeadingHints(tableNames)

	indexHints := make([][]string, len(tableNames))
	for i, t := range tableNames {
		indexHints[i] = e.unityOfflineIterateIndexHints(t)
	}

	allPossibleHintSets := e.unityOfflineIterateHints(leadingHints, indexHints)
	planDigestMap := make(map[string]struct{})
	sctx := e.SCtx()
	plans := make([]*UnityOfflinePlan, 0, len(allPossibleHintSets))
	for _, hs := range allPossibleHintSets {
		currentSQL := sctx.GetSessionVars().StmtCtx.OriginalSQL
		prefix := fmt.Sprintf("explain analyze format='%s' SELECT ", types.ExplainFormatUnityOffline)
		sql := fmt.Sprintf("explain analyze format='%s' select %s %s ", types.ExplainFormatUnityOffline_, hs, currentSQL[len(prefix):])
		sqlExec := sctx.GetRestrictedSQLExecutor()
		rows, _, err := sqlExec.ExecRestrictedSQL(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo),
			[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql)
		if err != nil {
			panic(err)
		}
		data := rows[0].GetString(0)
		plan := new(UnityOfflinePlan)
		err = json.Unmarshal([]byte(data), plan)
		if err != nil {
			panic(err)
		}
		if _, ok := planDigestMap[plan.PlanDigest]; ok {
			continue
		}
		planDigestMap[plan.PlanDigest] = struct{}{}
		plans = append(plans, plan)
	}

	sort.Slice(plans, func(i, j int) bool {
		return plans[i].TimeInMS < plans[j].TimeInMS
	})
	k := variable.UnityOfflineK.Load()
	if k > 0 && len(plans) > int(k) {
		plans = plans[:k]
	}

	data, err := json.Marshal(plans)
	must(err)
	return string(data)
}

func (e *Explain) unityOfflineIterateHints(leadingHints []string, indexHints [][]string) (hints []string) {
	currentIndexHints := make([]string, len(indexHints))
	var possibleIndexHints []string
	var f func(int)
	f = func(depth int) {
		if depth == len(indexHints) {
			possibleIndexHints = append(possibleIndexHints, strings.Join(currentIndexHints, " "))
			return
		}
		for _, hint := range indexHints[depth] {
			currentIndexHints[depth] = hint
			f(depth + 1)
		}
	}
	f(0)
	for _, leading := range leadingHints {
		for _, hint := range possibleIndexHints {
			hints = append(hints, fmt.Sprintf("/*+ %s %s */", leading, hint))
		}
	}
	return
}

func (e *Explain) unityOfflineIterateIndexHints(t *tableName) (hints []string) {
	hints = append(hints, "")                                         // empty hint
	hints = append(hints, fmt.Sprintf("use_index(%s)", t.HintName())) // don't use index
	idxNames := e.tableIndexNames(t)
	for _, idxName := range idxNames {
		hints = append(hints, fmt.Sprintf("use_index(%s, %s)", t.HintName(), idxName))
	}
	return
}

func (e *Explain) unityOfflineIterateLeadingHints(tableNames []*tableName) (hints []string) {
	hints = append(hints, "") // empty
	n := len(tableNames)
	switch n {
	case 0, 1, 2:
		return
	default: // leading-3
		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				if i == j {
					continue
				}
				for k := 0; k < n; k++ {
					if i == k || j == k {
						continue
					}
					hints = append(hints, fmt.Sprintf("leading(%s,%s,%s)",
						tableNames[i].HintName(), tableNames[j].HintName(), tableNames[k].HintName()))
				}
			}
		}
		return
	}
}

func (e *Explain) tableIndexNames(t *tableName) (idxNames []string) {
	is := domain.GetDomain(e.SCtx()).InfoSchema()
	tt, err := is.TableByName(context.Background(), ast.NewCIStr(t.schema), ast.NewCIStr(t.table))
	if err != nil {
		panic(err)
	}
	for _, idx := range tt.Indices() {
		idxNames = append(idxNames, idx.Meta().Name.O)
	}
	return
}

func (e *Explain) UnityOffline_() string {
	up := new(UnityOfflinePlan)
	up.PlanDigest = planDigest(e.TargetPlan)
	rootStats, _, memTracker, _ := getRuntimeInfo(e.SCtx(), e.TargetPlan, e.RuntimeStatsColl)
	basicStats, _ := rootStats.MergeStats()
	up.TimeInMS = float64(basicStats.GetTime()) / 1e6
	up.MemInByte = memTracker.MaxConsumed()
	up.SubPlans = e.unitySubPlan()
	data, err := json.Marshal(up)
	must(err)
	return string(data)
}

func (e *Explain) unitySubPlan() (subPlans []*UnityOfflinePlanNode) {
	flat := FlattenPhysicalPlan(e.TargetPlan, true)
	var iterSubPlanFunc func(op *FlatOperator)
	iterSubPlanFunc = func(op *FlatOperator) {
		if !op.IsRoot {
			return
		}
		explainNode := e.explainOpRecursivelyInJSONFormat(op, flat.Main)
		planNode := &UnityOfflinePlanNode{
			ExplainInfoForEncode: explainNode,
			PreSequence:          planPreSequences(op.Origin),
			Hints:                planHints(op.Origin),
			PlanStr:              ToString(op.Origin),
		}
		subPlans = append(subPlans, planNode)
		for _, childIdx := range op.ChildrenIdx {
			iterSubPlanFunc(flat.Main[childIdx])
		}
	}
	iterSubPlanFunc(flat.Main[0])
	return
}

type UnityOfflinePlanNode struct {
	*ExplainInfoForEncode
	PreSequence *UnityOfflinePreSequence `json:"preSequence"`
	Hints       string                   `json:"hints"`
	PlanStr     string                   `json:"planStr"`
}

type UnityOfflinePlan struct {
	PlanDigest string                  `json:"planDigest"`
	TimeInMS   float64                 `json:"TimeInMS"`
	MemInByte  int64                   `json:"memInByte"`
	SubPlans   []*UnityOfflinePlanNode `json:"subPlans"`
}

type UnityOfflinePreSequence struct {
	Tables           []string `json:"tables"`
	PredicateColumns []string `json:"predicateColumns"`
	JoinColumns      []string `json:"joinColumns"`
}

func planPreSequences(p base.Plan) *UnityOfflinePreSequence {
	var tables, predCols, joinCols []string
	m := make(map[string]struct{})

	collectTable := func(db, table string) {
		str := fmt.Sprintf("%s.%s", db, table)
		if _, ok := m[str]; ok {
			return
		}
		m[str] = struct{}{}
		tables = append(tables, str)
	}
	collectPredCols := func(preds []expression.Expression) {
		for _, pred := range preds {
			if sf, ok := pred.(*expression.ScalarFunction); ok {
				for _, arg := range sf.GetArgs() {
					if col, ok := arg.(*expression.Column); ok {
						str := "P:" + col.OrigName
						if _, ok := m[str]; ok {
							continue
						}
						m[str] = struct{}{}
						predCols = append(predCols, col.OrigName)
					}
				}
			}
		}
	}
	collectJoinCols := func(joinKeys []*expression.Column) {
		for _, col := range joinKeys {
			str := "J:" + col.OrigName
			if _, ok := m[str]; ok {
				continue
			}
			m[str] = struct{}{}
			joinCols = append(joinCols, col.OrigName)
		}
	}

	flat := FlattenPhysicalPlan(p, true)
	for _, op := range flat.Main {
		switch x := op.Origin.(type) {
		case *PhysicalTableScan:
			collectTable(x.DBName.L, x.Table.Name.L)
			collectPredCols(x.AccessCondition)
		case *PhysicalIndexScan:
			collectTable(x.DBName.L, x.Table.Name.L)
			collectPredCols(x.AccessCondition)
		case *PhysicalHashJoin:
			collectJoinCols(x.LeftJoinKeys)
			collectJoinCols(x.RightJoinKeys)
			collectJoinCols(x.InnerJoinKeys)
			collectJoinCols(x.RightJoinKeys)
		case *PhysicalMergeJoin:
			collectJoinCols(x.LeftJoinKeys)
			collectJoinCols(x.RightJoinKeys)
			collectJoinCols(x.InnerJoinKeys)
			collectJoinCols(x.RightJoinKeys)
		case *PhysicalIndexJoin:
			collectJoinCols(x.LeftJoinKeys)
			collectJoinCols(x.RightJoinKeys)
			collectJoinCols(x.InnerJoinKeys)
			collectJoinCols(x.RightJoinKeys)
		case *PhysicalIndexHashJoin:
			collectJoinCols(x.LeftJoinKeys)
			collectJoinCols(x.RightJoinKeys)
			collectJoinCols(x.InnerJoinKeys)
			collectJoinCols(x.RightJoinKeys)
		case *PhysicalIndexMergeJoin:
			collectJoinCols(x.LeftJoinKeys)
			collectJoinCols(x.RightJoinKeys)
			collectJoinCols(x.InnerJoinKeys)
			collectJoinCols(x.RightJoinKeys)
		default:
			continue
		}
	}

	sort.Strings(tables)
	sort.Strings(predCols)
	sort.Strings(joinCols)
	return &UnityOfflinePreSequence{
		Tables:           tables,
		PredicateColumns: predCols,
		JoinColumns:      joinCols,
	}
}

func planHints(p base.Plan) string {
	flat := FlattenPhysicalPlan(p, true)
	hints := GenHintsFromFlatPlan(flat)
	return hint.RestoreOptimizerHints(hints)
}

func planDigest(p base.Plan) string {
	flat := FlattenPhysicalPlan(p, true)
	_, digest := NormalizeFlatPlan(flat)
	return digest.String()
}

func collectTableNames(currentSchema string, stmt ast.StmtNode) []*tableName {
	collector := newTableNameCollector(currentSchema)
	stmt.Accept(collector)
	return collector.AllTables()
}

type tableName struct {
	schema string
	table  string
	alias  string
}

func (t *tableName) String() string {
	return fmt.Sprintf("%s.%s:%s", t.schema, t.table, t.alias)
}

func (t *tableName) HintName() string {
	if t.alias != "" {
		return fmt.Sprintf("%s.%s", t.schema, t.alias)
	}
	return fmt.Sprintf("%s.%s", t.schema, t.table)
}

type tableNameCollector struct {
	currentSchema string
	tables        map[string]*tableName
}

func newTableNameCollector(schema string) *tableNameCollector {
	return &tableNameCollector{
		currentSchema: schema,
		tables:        make(map[string]*tableName),
	}
}

func (c *tableNameCollector) AllTables() []*tableName {
	ret := make([]*tableName, 0, len(c.tables))
	for _, t := range c.tables {
		ret = append(ret, t)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].String() < ret[j].String() })
	return ret
}

// Enter implements Visitor interface.
func (c *tableNameCollector) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	if node, ok := in.(*ast.TableSource); ok {
		if table, ok := node.Source.(*ast.TableName); ok {
			t := new(tableName)
			if table.Schema.L == "" {
				t.schema = strings.ToLower(c.currentSchema)
			} else {
				t.schema = table.Schema.L
			}
			t.table = table.Name.L
			t.alias = node.AsName.L
			c.tables[t.String()] = t
		}
	}
	return in, false
}

// Leave implements Visitor interface.
func (*tableNameCollector) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}
