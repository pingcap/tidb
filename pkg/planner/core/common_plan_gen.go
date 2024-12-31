package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func (e *Explain) unityPlanAll() (string, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(e.SCtx().GetSessionVars().StmtCtx.OriginalSQL, "", "")
	if err != nil {
		return "", err
	}
	tableNames := collectTableNames(e.SCtx().GetSessionVars().CurrentDB, stmt)
	leadingHints := e.iterateLeadingHints(tableNames)

	indexHints := make([][]string, len(tableNames))
	for i, t := range tableNames {
		indexHints[i] = e.iterateIndexHints(t)
	}

	allPossibleHintSets := e.iterateHints(leadingHints, indexHints)
	planDigestMap := make(map[string]struct{})
	sctx := e.SCtx()
	plans := make([]*UnityPlan, 0, len(allPossibleHintSets))
	for _, hs := range allPossibleHintSets {
		currentSQL := sctx.GetSessionVars().StmtCtx.OriginalSQL
		prefix := "explain analyze format='unity_plan_all' SELECT "
		sql := fmt.Sprintf("explain analyze format='unity_plan_one' select %s %s ", hs, currentSQL[len(prefix):])
		sqlExec := sctx.GetRestrictedSQLExecutor()
		rows, _, err := sqlExec.ExecRestrictedSQL(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo),
			[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql)
		if err != nil {
			panic(err)
		}
		data := rows[0].GetString(0)
		plan := new(UnityPlan)
		err = json.Unmarshal([]byte(data), plan)
		if err != nil {
			panic(err)
		}
		if _, ok := planDigestMap[plan.PlanDigest]; ok {
			continue
		}
		planDigestMap[plan.PlanDigest] = struct{}{}
		plans = append(plans, plan)

		// TODO:
		if len(plans) >= 5 {
			break
		}
	}

	data, err := json.Marshal(plans)
	if err != nil {
		panic(err)
	}
	return string(data), nil
}

func (e *Explain) iterateHints(leadingHints []string, indexHints [][]string) (hints []string) {
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

func (e *Explain) iterateIndexHints(t *tableName) (hints []string) {
	hints = append(hints, "")                                         // empty hint
	hints = append(hints, fmt.Sprintf("use_index(%s)", t.HintName())) // don't use index
	idxNames := e.tableIndexNames(t)
	for _, idxName := range idxNames {
		hints = append(hints, fmt.Sprintf("use_index(%s, %s)", t.HintName(), idxName))
	}
	return
}

func (e *Explain) iterateLeadingHints(tableNames []*tableName) (hints []string) {
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
	tt, err := is.TableByName(context.Background(), model.NewCIStr(t.schema), model.NewCIStr(t.table))
	if err != nil {
		panic(err)
	}
	for _, idx := range tt.Indices() {
		idxNames = append(idxNames, idx.Meta().Name.O)
	}
	return
}

func (e *Explain) unityPlanOne() (string, error) {
	up := new(UnityPlan)
	up.PlanDigest = planDigest(e.TargetPlan)
	rootStats, _, memTracker, _ := getRuntimeInfo(e.SCtx(), e.TargetPlan, e.RuntimeStatsColl)
	basicStats, _ := rootStats.MergeStats()
	up.TimeInMS = float64(basicStats.GetTime()) / 1e6
	up.MemInByte = memTracker.MaxConsumed()
	up.SubPlans = e.unitySubPlan()
	data, err := json.Marshal(up)
	return string(data), err
}

func (e *Explain) unitySubPlan() (subPlans []*UnityPlanNode) {
	flat := FlattenPhysicalPlan(e.TargetPlan, true)
	var iterSubPlanFunc func(op *FlatOperator)
	iterSubPlanFunc = func(op *FlatOperator) {
		if !op.IsRoot {
			return
		}
		explainNode := e.explainOpRecursivelyInJSONFormat(op, flat.Main)
		planNode := &UnityPlanNode{
			ExplainInfoForEncode: explainNode,
			PreSequences:         planPreSequences(op.Origin),
			Hints:                planHints(op.Origin),
		}
		subPlans = append(subPlans, planNode)
		for _, childIdx := range op.ChildrenIdx {
			iterSubPlanFunc(flat.Main[childIdx])
		}
	}
	iterSubPlanFunc(flat.Main[0])
	return
}

type UnityPlanNode struct {
	*ExplainInfoForEncode
	PreSequences []string `json:"preSequences"`
	Hints        string   `json:"hints"`
}

type UnityPlan struct {
	PlanDigest string           `json:"planDigest"`
	TimeInMS   float64          `json:"TimeInMS"`
	MemInByte  int64            `json:"memInByte"`
	SubPlans   []*UnityPlanNode `json:"subPlans"`
}

func planPreSequences(p base.Plan) (preSeq []string) {
	flat := FlattenPhysicalPlan(p, true)
	for _, op := range flat.Main {
		if !op.IsRoot {
			continue
		}
		switch node := op.Origin.(type) {
		case *PhysicalTableReader:
			tableScan := node.TablePlans[len(node.TablePlans)-1].(*PhysicalTableScan)
			preSeq = append(preSeq, fmt.Sprintf("%s.%s", tableScan.DBName.L, tableScan.Table.Name.L))
		case *PhysicalIndexReader:
			indexScan := node.IndexPlans[len(node.IndexPlans)-1].(*PhysicalIndexScan)
			preSeq = append(preSeq, fmt.Sprintf("%s.%s", indexScan.DBName.L, indexScan.Table.Name.L))
		case *PhysicalIndexLookUpReader:
			tableScan := node.TablePlans[len(node.TablePlans)-1].(*PhysicalTableScan)
			preSeq = append(preSeq, fmt.Sprintf("%s.%s", tableScan.DBName.L, tableScan.Table.Name.L))
		case *PhysicalIndexMergeReader:
			tableScan := node.TablePlans[len(node.TablePlans)-1].(*PhysicalTableScan)
			preSeq = append(preSeq, fmt.Sprintf("%s.%s", tableScan.DBName.L, tableScan.Table.Name.L))
		default:
			continue
		}
	}
	return
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
