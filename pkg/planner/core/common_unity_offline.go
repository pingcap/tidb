package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func (e *Explain) UnityOffline() string {
	allPossibleHintSets := e.unityOfflinePossibleHints()
	planDigestMap := make(map[string]struct{})
	sctx := e.SCtx()
	plans := make([]*UnityOfflinePlan, 0, len(allPossibleHintSets))
	var currentBestInMS int
	for _, hs := range allPossibleHintSets {
		maxExecHint := ""
		if currentBestInMS > 0 {
			maxExecHint = fmt.Sprintf("max_execution_time(%d)", currentBestInMS*3)
		}
		currentSQL := sctx.GetSessionVars().StmtCtx.OriginalSQL
		prefix := fmt.Sprintf("explain analyze format='%s' SELECT ", types.ExplainFormatUnityOffline)
		sql := fmt.Sprintf("explain analyze format='%s' select /*+ %s %s */ %s ",
			types.ExplainFormatUnityOffline_, hs, maxExecHint, currentSQL[len(prefix):])
		sqlExec := sctx.GetRestrictedSQLExecutor()
		rows, _, err := sqlExec.ExecRestrictedSQL(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo),
			[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql)
		if err != nil { // may expected timeout
			continue
		}
		data := rows[0].GetString(0)
		plan := new(UnityOfflinePlan)
		must(json.Unmarshal([]byte(data), plan))
		if _, ok := planDigestMap[plan.PlanDigest]; ok {
			continue
		}
		planDigestMap[plan.PlanDigest] = struct{}{}
		plans = append(plans, plan)
		if currentBestInMS == 0 || plan.TimeInMS < float64(currentBestInMS) {
			currentBestInMS = int(plan.TimeInMS)
		}
	}

	sort.Slice(plans, func(i, j int) bool {
		return plans[i].TimeInMS < plans[j].TimeInMS
	})
	k := vardef.UnityOfflineK.Load()
	if k > 0 && len(plans) > int(k) {
		plans = plans[:k]
	}

	data, err := json.Marshal(plans)
	must(err)
	return string(data)
}

func (e *Explain) unityOfflinePossibleHints() (allPossibleHintSets []string) {
	if hints := e.unityOfflineTunedHints(); len(hints) > 0 {
		return hints
	}

	stmt, err := parser.New().ParseOneStmt(e.SCtx().GetSessionVars().StmtCtx.OriginalSQL, "", "")
	must(err)
	tableNames := collectTableNames(e.SCtx().GetSessionVars().CurrentDB, stmt)
	leadingHints := e.unityOfflineLeadingHints(tableNames)
	var indexHints [][]string
	if len(leadingHints) < 30 {
		indexHints = make([][]string, len(tableNames))
		for i, t := range tableNames {
			indexHints[i] = e.unityOfflineIndexHints(t)
		}
	}
	return e.unityOfflineCombineHints(leadingHints, indexHints)
}

func (e *Explain) unityOfflineCombineHints(leadingHints []string, indexHints [][]string) (hints []string) {
	if len(indexHints) == 0 { // only consider leading hints
		return leadingHints
	}

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
			hints = append(hints, fmt.Sprintf("%s %s", leading, hint))
		}
	}
	return
}

func (e *Explain) unityOfflineIndexHints(t *tableName) (hints []string) {
	hints = append(hints, "")                                         // empty hint
	hints = append(hints, fmt.Sprintf("use_index(%s)", t.HintName())) // don't use index
	idxNames := e.tableIndexNames(t)
	for _, idxName := range idxNames {
		hints = append(hints, fmt.Sprintf("use_index(%s, %s)", t.HintName(), idxName))
	}
	return
}

func (e *Explain) unityOfflineLeadingHints(tableNames []*tableName) (hints []string) {
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
	must(err)
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
	up.SubPlans = e.unityOfflineSubPlan()
	data, err := json.Marshal(up)
	must(err)
	return string(data)
}

func (e *Explain) unityOfflineSubPlan() (subPlans []*UnityOfflinePlanNode) {
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
	PreSequence *UnityPreSequence `json:"preSequence"`
	Hints       string            `json:"hints"`
	PlanStr     string            `json:"planStr"`
}

type UnityOfflinePlan struct {
	PlanDigest string                  `json:"planDigest"`
	TimeInMS   float64                 `json:"TimeInMS"`
	MemInByte  int64                   `json:"memInByte"`
	SubPlans   []*UnityOfflinePlanNode `json:"subPlans"`
}

type UnityPreSequence struct {
	Tables           []string `json:"tables"`
	PredicateColumns []string `json:"predicateColumns"`
	JoinColumns      []string `json:"joinColumns"`
}

func planPreSequences(p base.Plan) *UnityPreSequence {
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
	desireFuncName := map[string]struct{}{
		"eq": {}, "lt": {}, "le": {}, "gt": {}, "ge": {}, "ne": {},
		"in": {}, "isnull": {}, "between": {},
	}
	collectPredCols := func(preds []expression.Expression) {
		for _, pred := range preds {
			if sf, ok := pred.(*expression.ScalarFunction); ok {
				if _, ok := desireFuncName[sf.FuncName.L]; !ok {
					continue
				}
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
			collectPredCols(x.filterCondition)
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

	//sort.Strings(tables)
	sort.Strings(predCols)
	sort.Strings(joinCols)
	return &UnityPreSequence{
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

func (e *Explain) unityOfflineTunedHints() []string {
	return label2Hints[e.queryLabel()]
}

func (e *Explain) queryLabel() string {
	stmt := strings.TrimSpace(e.SCtx().GetSessionVars().StmtCtx.OriginalSQL)
	stmt = strings.TrimRight(stmt, ";")
	_, digest := parser.NormalizeDigest(stmt)
	return sqlDigestLabel[digest.String()]
}

var label2Hints = map[string][]string{
	"job-1a":  unityL("it-mi_idx mc-mi_idx ct-mc t-mc"),
	"job-1b":  unityL("it-mi_idx t-mi_idx t-mc ct-mc"),
	"job-1c":  unityL("it-mi_idx t-mi_idx t-mc ct-mc"),
	"job-1d":  unityL("it-mi_idx t-mi_idx t-mc ct-mc"),
	"job-2a":  unityL("mk-k mc-mk cn-mc mc-t"),
	"job-2b":  unityL("mk-k mc-mk cn-mc mc-t"),
	"job-2c":  unityL("cn-mc mc-mk mk-k mc-t"),
	"job-2d":  unityL("mk-k t-mk mc-t cn-mc"),
	"job-32a": unityL("lt-ml ml-t1 t1-mk mk-k ml-t2"),
	"job-32b": unityL("lt-ml ml-t1 t1-mk mk-k ml-t2"),
	"job-3a":  unityL("k-mk mk-mi t-mi"),
	"job-3b":  unityL("k-mk mk-mi t-mi"),
	"job-3c":  unityL("k-mk t-mk t-mi"),
	"job-4a":  unityL("it-mi_idx t-mi_idx t-mk k-mk"),
	"job-4b":  unityL("it-mi_idx t-mi_idx t-mk k-mk"),
	"job-4c":  unityL("it-mi_idx t-mi_idx t-mk k-mk"),
	"job-5a":  unityL("ct-mc mc-mi t-mi it-mi"),
	"job-5b":  unityL("ct-mc t-mc t-mi it-mi"),
	"job-5c":  unityL("ct-mc mc-mi t-mi it-mi"),
	"job-6a":  unityL("k-mk t-mk t-ci n-ci"),
	"job-6b":  unityL("k-mk t-mk t-ci n-ci"),
	"job-6c":  unityL("k-mk t-mk t-ci n-ci"),
	"job-6d":  unityL("k-mk t-mk t-ci n-ci"),
	"job-6e":  unityL("k-mk t-mk t-ci n-ci"),
	"job-6f":  unityL("k-mk t-mk t-ci n-ci"),
	"job-7a":  unityL("lt-ml ml-t t-ci ci-n n-pi it-pi n-an"),
	"job-7b":  unityL("lt-ml ml-t t-ci ci-n n-pi it-pi n-an"),
	"job-7c":  unityL("lt-ml it-pi n-pi ml-t ci-n t-ci n-an"),
	"job-8a":  unityL("mc-cn ci-mc ci-rt n-ci ci-t an-n"),
	"job-8b":  unityL("mc-cn t-mc ci-t ci-rt n-ci an-n"),
	"job-8c":  unityL("an-n ci-rt mc-cn t-mc ci-t n-ci"),
	"job-8d":  unityL("ci-rt an-ci an-n ci-t t-mc mc-cn"),
	"job-9a":  unityL("an-n n-ci ci-t t-mc mc-cn chn-ci ci-rt"),
	"job-9b":  unityL("mc-cn ci-mc ci-rt n-ci ci-t chn-ci an-n"),
	"job-9c":  unityL("an-n n-ci chn-ci ci-t ci-rt t-mc mc-cn"),
	"job-9d":  unityL("mc-cn ci-mc ci-rt chn-ci n-ci ci-t an-n"),
	"job-10a": unityL("cn-mc ci-mc rt-ci t-mc chn-ci ct-mc"),
	"job-10b": unityL("cn-mc t-mc ct-mc t-ci rt-ci chn-ci"),
	"job-10c": unityL("cn-mc t-mc t-ci chn-ci ct-mc rt-ci"),
	"job-11a": unityL("lt-ml ml-t t-mc mc-cn mc-ct t-mk mk-k"),
	"job-11b": unityL("lt-ml ml-t t-mk mk-k t-mc mc-cn mc-ct"),
	"job-11c": unityL("lt-ml ml-mk mk-k ml-t t-mc mc-cn mc-ct"),
	"job-11d": unityL("lt-ml ml-mc mc-ct mc-cn ml-mk mk-k ml-t"),
	"job-12a": unityL("mi_idx-it2 mi-mi_idx t-mi t-mc ct-mc cn-mc mi-it1"),
	"job-12b": unityL("mi_idx-it2 t-mi_idx mi-mi_idx mi-it1 t-mc cn-mc ct-mc"),
	"job-12c": unityL("mi_idx-it2 mi-mi_idx t-mi t-mc ct-mc cn-mc mi-it1"),
	"job-13a": unityL("cn-mc ct-mc mc-t kt-t mi_idx-mc it1-mi_idx mi-mi_idx it2-mi"),
	"job-13b": unityL("kt-t mi_idx-t it1-mi_idx mc-t ct-mc cn-mc mi-t it2-mi"),
	"job-13c": unityL("kt-t mi_idx-t it1-mi_idx mc-t ct-mc cn-mc mi-t it2-mi"),
	"job-13d": unityL("it1-mi_idx mi_idx-t kt-t mi_idx-mc cn-mc it2-mi ct-mc mi-t"),
	"job-14a": unityL("it2-mi_idx t-mi_idx kt-t t-mi t-mk k-mk it1-mi"),
	"job-14b": unityL("it2-mi_idx t-mi_idx t-mk k-mk kt-t t-mi it1-mi"),
	"job-14c": unityL("it2-mi_idx t-mi_idx t-mi it1-mi kt-t t-mk k-mk"),
	"job-15a": unityL("ct-mc mi-mc mi-att cn-mc t-att it-mi t-mk k-mk"),
	"job-15b": unityL("ct-mc cn-mc mc-att mi-mc t-att it-mi t-mk k-mk"),
	"job-15c": unityL("t-att t-mi it-mi t-mc cn-mc ct-mc t-mk k-mk"),
	"job-15d": unityL("t-att t-mi it-mi t-mc cn-mc ct-mc t-mk k-mk"),
	"job-16a": unityL("mk-k t-mk t-mc mc-cn ci-t n-ci an-n"),
	"job-16b": unityL("mk-k t-mk ci-t n-ci an-n mc-cn t-mc"),
	"job-16c": unityL("mk-k t-mk t-mc mc-cn ci-t n-ci an-n"),
	"job-16d": unityL("mk-k t-mk t-mc mc-cn ci-t n-ci an-n"),
	"job-17a": unityL("mk-k t-mk ci-t n-ci t-mc mc-cn"),
	"job-17b": unityL("mk-k ci-mk n-ci ci-t t-mc mc-cn"),
	"job-17c": unityL("mk-k ci-mk n-ci ci-t t-mc mc-cn"),
	"job-17d": unityL("mk-k ci-mk n-ci ci-t t-mc mc-cn"),
	"job-17e": unityL("mk-k t-mk ci-t n-ci mc-cn t-mc"),
	"job-17f": unityL("mk-k t-mk ci-t n-ci t-mc mc-cn"),
	"job-18a": unityL("it1-mi mi-mi_idx it2-mi_idx ci-mi n-ci t-mi"),
	"job-18b": unityL("it2-mi_idx mi-mi_idx ci-mi n-ci t-mi it1-mi"),
	"job-18c": unityL("it2-mi_idx mi-mi_idx ci-mi n-ci it1-mi t-mi"),
	"job-19a": unityL("n-an n-ci t-ci t-mc rt-ci cn-mc chn-ci t-mi it-mi"),
	"job-19b": unityL("cn-mc t-mc t-mi t-ci rt-ci n-ci it-mi chn-ci n-an"),
	"job-19c": unityL("n-an n-ci t-ci t-mc chn-ci cn-mc t-mi it-mi rt-ci"),
	"job-19d": unityL("n-an ci-an t-ci t-mi cn-mc it-mi rt-ci chn-ci t-mc"),
	"job-20a": unityL("cct2-cc t-cc kt-t t-ci chn-ci cct1-cc n-ci t-mk k-mk"),
	"job-20b": unityL("cct1-cc t-cc t-ci t-mk k-mk chn-ci n-ci kt-t cct2-cc"),
	"job-20c": unityL("cct2-cc t-cc t-mk k-mk kt-t cct1-cc t-ci chn-ci n-ci"),
	"job-21a": unityL("lt-ml ml-mc ml-mk mk-k mc-cn ml-t mc-ct mi-t"),
	"job-21b": unityL("lt-ml ml-t mi-t mk-mi mk-k t-mc mc-cn mc-ct"),
	"job-21c": unityL("lt-ml ml-mc mc-ct mc-cn ml-mk mk-k ml-t mi-t"),
	"job-22a": unityL("it2-mi_idx t-mi_idx t-mi t-mk t-mc cn-mc k-mk kt-t it1-mi ct-mc"),
	"job-22b": unityL("it2-mi_idx t-mi_idx t-mc t-mk k-mk kt-t t-mi it1-mi ct-mc cn-mc"),
	"job-22c": unityL("it2-mi_idx t-mi_idx t-mi t-mc t-mk cn-mc k-mk it1-mi kt-t ct-mc"),
	"job-22d": unityL("it2-mi_idx t-mi_idx t-mi t-mk k-mk kt-t it1-mi t-mc cn-mc ct-mc"),
	"job-23a": unityL("cct-cc mi-cc t-mi it-mi kt-t t-mc ct-mc cn-mc t-mk k-mk"),
	"job-23b": unityL("cct-cc mi-cc mi-mc mk-mi ct-mc k-mk it-mi t-mi kt-t cn-mc"),
	"job-23c": unityL("cct-cc mi-cc t-mi it-mi kt-t t-mc ct-mc cn-mc t-mk k-mk"),
	"job-24a": unityL("k-mk t-mk t-mi it-mi mi-ci rt-ci n-ci chn-ci t-mc cn-mc n-an"),
	"job-24b": unityL("cn-mc t-mc t-ci ci-mk ci-an rt-ci t-mi k-mk n-ci it-mi chn-ci"),
	"job-25a": unityL("k-mk mi-mk ci-mi it1-mi n-ci ci-mi_idx it2-mi_idx t-mi"),
	"job-25b": unityL("k-mk t-mk t-ci t-mi_idx it2-mi_idx t-mi n-ci it1-mi"),
	"job-25c": unityL("k-mk ci-mk ci-mi it1-mi n-ci ci-mi_idx it2-mi_idx t-mi"),
	"job-26a": unityL("it-mi_idx cc-mi_idx t-cc t-mk k-mk t-ci kt-t chn-ci cct1-cc n-ci cct2-cc"),
	"job-26b": unityL("cct1-cc cc-mi_idx it-mi_idx t-cc t-mk k-mk kt-t cct2-cc t-ci chn-ci n-ci"),
	"job-26c": unityL("cct1-cc t-cc t-mi_idx t-mk cct2-cc k-mk it-mi_idx kt-t t-ci chn-ci n-ci"),
	"job-27a": unityL("lt-ml ml-cc cct2-cc ml-t t-mk mk-k cct1-cc mc-cc mc-cn mc-ct mi-t"),
	"job-27b": unityL("lt-ml ml-t t-cc cct2-cc ml-mk mk-k cct1-cc mc-cc mc-cn mc-ct mi-t"),
	"job-27c": unityL("lt-ml ml-cc ml-mc ml-mk mk-k cct1-cc cct2-cc mc-cn mc-ct t-cc mi-t"),
	"job-28a": unityL("cct1-cc mi_idx-cc cct2-cc mi-mi_idx it2-mi_idx mk-mi k-mk t-mi it1-mi kt-t t-mc cn-mc ct-mc"),
	"job-28b": unityL("cct1-cc mc-cc ct-mc cct2-cc t-mc t-mk k-mk t-mi_idx it2-mi_idx cn-mc kt-t t-mi it1-mi"),
	"job-28c": unityL("it2-mi_idx mi_idx-cc cct1-cc t-mi_idx t-mi mk-cc k-mk cct2-cc t-mc cn-mc kt-t it1-mi ct-mc"),
	"job-30a": unityL("cct2-cc t-cc t-mi cct1-cc t-mi_idx t-mk k-mk it1-mi it2-mi_idx t-ci n-ci"),
	"job-30b": unityL("cct2-cc t-cc t-ci t-mi_idx it2-mi_idx t-mk t-mi k-mk it1-mi n-ci cct1-cc"),
	"job-30c": unityL("cct2-cc ci-cc ci-mi ci-mk ci-mi_idx it2-mi_idx n-ci k-mk cct1-cc it1-mi t-mi"),
	"job-31a": unityL("cn-mc mi-mc ci-mi it1-mi n-ci ci-mi_idx it2-mi_idx t-mc t-mk k-mk"),
	"job-31b": unityL("cn-mc t-mc t-ci t-mi_idx t-mk k-mk it2-mi_idx n-ci t-mi it1-mi"),
	"job-31c": unityL("cn-mc mi-mc ci-mi it1-mi ci-mk ci-mi_idx n-ci k-mk it2-mi_idx t-mi"),
	"job-33a": unityL("lt-ml t2-ml t2-mi_idx2 kt2-t2 it2-mi_idx2 ml-mi_idx1 ml-mc1 cn1-mc1 it1-mi_idx1 t1-ml kt1-t1 t2-mc2 cn2-mc2"),
	"job-33b": unityL("lt-ml t2-ml t2-mi_idx2 it2-mi_idx2 kt2-t2 t1-ml kt1-t1 t1-mc1 cn1-mc1 t1-mi_idx1 it1-mi_idx1 t2-mc2 cn2-mc2"),
	"job-33c": unityL("lt-ml t2-ml kt2-t2 ml-mc1 cn1-mc1 ml-mi_idx1 it1-mi_idx1 t1-ml kt1-t1 t2-mi_idx2 it2-mi_idx2 t2-mc2 cn2-mc2"),
	"job-29a": unityL("cct2-cc t-cc t-ci rt-ci n-ci t-mk k-mk chn-ci cct1-cc t-mc cn-mc n-pi it2-pi t-mi it1-mi n-an"),
	"job-29b": unityL("cct2-cc t-cc t-ci rt-ci n-ci t-mk k-mk chn-ci n-pi it2-pi mi-ci it1-mi cct1-cc t-mc cn-mc n-an"),
	"job-29c": unityL("cct2-cc t-cc t-ci rt-ci n-ci cct1-cc chn-ci t-mk k-mk t-mc cn-mc n-pi it2-pi t-mi it1-mi n-an"),
}

func unityL(plan string) (leadings []string) {
	// example: it-mi_idx mc-mi_idx ct-mc t-mc
	tmp := strings.Split(strings.TrimSpace(plan), " ")
	var xs []string
	tableM := make(map[string]struct{})
	for _, t := range tmp {
		table0, table1 := strings.Split(t, "-")[0], strings.Split(t, "-")[1]
		if _, ok := tableM[table0]; !ok {
			xs = append(xs, table0)
			tableM[table0] = struct{}{}
		}
		if _, ok := tableM[table1]; !ok {
			xs = append(xs, table1)
			tableM[table1] = struct{}{}
		}
	}

	for i := len(xs); i >= 1; i-- {
		leadings = append(leadings, fmt.Sprintf("leading(%s)", strings.Join(xs[:i], ",")))
		if len(leadings) >= 8 {
			break
		}
	}
	return leadings
}

var sqlDigestLabel = map[string]string{
	"9212dd7ece4be3840a738f8c7ad862add495de05ed613cacbb4e86739fce7399": "job-10a",
	"d4f0db93186fa0e7266a45a1176b0f92bc0639ba432a39b76ba82c6a1bcf3f0f": "job-10b",
	"f0ff7bf41a441c89186c7dc9361e997fe654837cb5604b396ab74da0e8d31f9f": "job-10c",
	"1dd9970d0e5bbaeaf0528b51dcc160effdb10ca39757af3d41f4b8e5127557c7": "job-11a",
	"074c42bcc7242a821139534560fab9c58dcc31d871a722affeeebbec6fd90283": "job-11b",
	"f8be502e2434579088788a9acf58e5a7e99fd71f2a6cc31319455850a6e557fd": "job-11c",
	"a959011d51a350c67a35a0e836267f17c2c9e32c9201c8fc5faf60f2c6f1364b": "job-11d",
	"ea763fa8357ba0093d360241d561d6178ed3db0fb3cf2d9737f148a54fea0ba3": "job-12a",
	"0b320775a5801886d00a97e1cbd41b46b7c82d6b757081e1068f8eb33c0d2f2c": "job-12b",
	"3f31c599dbe019e8356db8a32e0761d9f7a6c7d87d123990abdc2d31daea29ef": "job-12c",
	"c7b44835d86af69d14a00d75e602c2cf8d0b0b778f3eaa4cb0a7a353035cefb0": "job-13a",
	"82301910a521098983e00d2cb71f164faae9fa037f7a96f9ea2dcd1426b51c10": "job-13b",
	"093b2d27a92239c01212d7573a3b6822659d0859c39e2effbac545fd638a55e0": "job-13d",
	"3281f98f4417100cfb2b427942eb5de6754cb3b69a025dce48f13c20cb23678b": "job-14a",
	"9e22b10001b1a4e7c48110ee473f7c5999edb24ccf3ea50290f8a7f8c8253826": "job-14b",
	"0a02b39faf9ac900c3538bf383b02e65b74d5075eab08e4f545793104df274fe": "job-14c",
	"690a385554d562ead2c16892631d96e75a07dc5205ed60d7ae9db351a7bd1d8c": "job-15a",
	"8c9c301731c00daa2175c406037aa2eb6836406b8a46b04e4b55cf29c70416b0": "job-15b",
	"efa064c15e105e346d15547e471fc64e50ff1cac6fb9d8d9fa953b81a7c8ae51": "job-15c",
	"513977339f7899f721f0db8287229c8c8cef9f33bd4532ce9fdbcf680a3ab09d": "job-15d",
	"044a721287b37625fe757f10eb5abd4d323ff684ac57e95fc0ca1d37c7dc63c9": "job-16a",
	"c7edae530e556005dab35c25efe02038713b8f6cea84d7d6eb834f7d1789e9ad": "job-16b",
	"e6d3f023f09c82e99b71f879854c86d84c60a603faf488f47fd6bcfb54acf9cc": "job-16c",
	"acc7fca8ac564bd504cb72b7da58462f453498209bb1f1d85de167e6b740e283": "job-17a",
	"1657cf65015313a3822cfec2b5f95d517b2181e43c9dccc8a875dfa725dc62d3": "job-17b",
	"a3224884d0f341ac1c7e0dd5a8943d25884e57920118520c1a95c56bc99dc7d5": "job-17d",
	"065e9fcc5c38522c22907433db985ebd766171b0ba2fb06e94acad24807bb740": "job-17e",
	"545a3daa2e834d2382175c2d11e700ed7ac317dfe438ec1dd8ff419f53102149": "job-18a",
	"1834b090fdc37f251f1d47791a65231d4bed095fa76bda86afabfce2c6661700": "job-18b",
	"2b5668a675a8e62b18e7fbffd91a122a2a2b9f48c53a5ef0516881a7e5929f3b": "job-18c",
	"624e5b84216df1dc7afd8b24571c39ec56f9b721112b2660c888179656a86041": "job-19a",
	"5ab945e2e8aa0d113a471065a912453038221fe77950c760bddc0b0115076897": "job-19b",
	"2784d9c735695d88be1db9c73231a0d85da2dc06e5fb73ff24bd257c3f8d0c89": "job-19c",
	"2d41ceceecfaf92344d2c3f23618c0f3fd06468b3a0baed1574395bd834828bb": "job-19d",
	"f3a3ea5725922c273d4f6ec063ba6bca646953ea91720e91e59c9f9761009d4c": "job-1a",
	"c5b88a3ae49ca4c657af39b65a678c06f9b01419f1d267321d0c2a7c9d059ec2": "job-1b",
	"feed8f32b89431ce7b926752f7f9d0ca3adc475a13176f7c04fce3baa274d2de": "job-1c",
	"604ebd359ce62befaa2b7b79f4b995208c70bd2cea542ebd8532a0793c8b91fd": "job-1d",
	"78723406ebfae4f4c55d164d3da64d670286ebc21105bdaa5d97d997103ed22a": "job-20a",
	"2e3d39c9164b5e379784cf1ab5d21bdb8b079840f9b6d2b661375215bca5550e": "job-20b",
	"f27811939fcf874f3755f9d4bab677f8e4368f5efa49297c5eb3c28f020f94e1": "job-20c",
	"70c2aafc28532ae371b2c568ffa3e666bc568162aa3fa44084916ba47989d4a4": "job-21a",
	"02cb0754f9f122bec99a9cca78e5c8843c6612135a442701f111bcf009fb5e4a": "job-21b",
	"211d76f963f68a6bd7a988e4ae60a0e4eea343e1339e598183a66f6655138375": "job-22a",
	"1fddb00851636290fa29008d272646f7bdd6c066fb13267eff21e57cc370978f": "job-22d",
	"ef2cdca852dbc9a5ce9dded82ff5e572d554b66030a1fbc0e87e97b246794560": "job-23a",
	"87f2151b742b912e205f5f8e589ef5cf1406635e7f0e7e9dc5e131595fee734b": "job-23b",
	"b9f4cecdcf1ccfb0e527347944e9ea91d6c9455999cb1585fde4c7590f04ac1b": "job-23c",
	"73b4d21fd0ae81dd3b9e9c3a83cbb950da6d013ee799c4deb13abea79f31de02": "job-24a",
	"c8d6f393eaba86c61e0ca846e651665380913378f191648ef6a77198c77a401b": "job-24b",
	"383c12e39a7510e297792a7be7fa9dda20c95a4a87d64a505c2326e1a5ce6973": "job-25a",
	"e60a38b5dde0b8848c8d1bd3812a345de39c5c567ae89c2402cb87504fe44d12": "job-25b",
	"e74bf422c74184381a6a6df33e1495dfe6063c8bb7619a1c762365cf8f5b2840": "job-25c",
	"f9be140deb2ec929945d18b9426676ae8e6fa7608af782c2973e3ff12d5d228c": "job-26a",
	"7c576ee8b67311aa0aa78dca6b0b9c5438a511805dec8efd675049ecb24646d1": "job-26b",
	"986cc8d0186be392fcbc1e1a6944d4ff1f2db01f6bbc52dc1efac7e996e169a4": "job-26c",
	"8d5a8cc59e0e2e629da401da411d2e37d17db1d113170c20c62c1105d72129af": "job-27a",
	"693dea0141b37415119e32ef9e803f6aae888f096d8beb36a8e98cbebb8b561d": "job-27b",
	"fa68f5e86ab6cb734d3d1b87c4f88488b0a67722ea7f3e33c40a2fb54a080d1f": "job-27c",
	"75e4bb2d3d7fcf04d728eeb5de25a704bb2cec668f1343a1c65e4f7e7e1cdd85": "job-28a",
	"1cca64e4930fa6e9b397ab887b20d6dcb736658b310a6189a53bfb5de878883b": "job-28b",
	"bd5a2496b83d5c40cec38cc59a5a22a740303214e6d835c47b9875101a3173ac": "job-28c",
	"3ec4ea8ba7187fc9c8f8160607c7cbdaa8d4144eedc8c28a3c190e388bffb6cd": "job-29a",
	"0881641759cedce26418bc41fa80bf295f9f232e6d70c18772a4d34a6efc5364": "job-29b",
	"231115688ea313cb4f875e88df96ea6de0b36373640d61707ee223967f5dad06": "job-29c",
	"e9e406ef79576f5614250a60941b61ff169322024ba6e43fa8376bddc8cc94e1": "job-2a",
	"d5e75ea81c9acbf276b5ead0491a124503d1b31b686eeccf38a471b601e7627d": "job-30a",
	"300a4830f487c75571881895c86ee1fd7e7255affa329fe4830d3e7361c9b60a": "job-30b",
	"54b7c42939aa04534e5b443457c5a3f965627752fd36176a6807a9c53e542382": "job-30c",
	"db853c79d812e8693039fb117c9c5f2c7b7ec370b9cc4c0b1e7c7dfd7a98fe87": "job-31a",
	"eb299619c0e718c9ceb2ffa8af3700d5f9937a0f883d74a6c8c4c1434c18013f": "job-31b",
	"a59a9a3b4607e8df30cc28e96f757eddd0d3c7820eb0134a2e153619ca840904": "job-31c",
	"dc6e4fb2f8e4383f2ab0d95578e557f7bf87f58341053bfe9971bbae4958087d": "job-32a",
	"6c6ca1e0660a23a78bf2862c022f709336bc60dd92d6d44936d0dc2720d954eb": "job-33a",
	"b991ff06863195979a94194c8bf48557c1dbb08aefa374e36937597bcad8863b": "job-33b",
	"fecd4bcb2510317fd58dd35a5bfac5a677224d11e460855997eab827709161ce": "job-33c",
	"1bf77499e9156d8321a6392a80e539b120534cf55d1dd897896e61a8cd5861ac": "job-3a",
	"79d9f78d65cbba0e81330aa25f93ab88b2e259c19ad85253495e5fed7d19245b": "job-3b",
	"f46387f2dff1384ada52ef14f678b8fdd0bff17acc8fedd83133b4e6ac9557c5": "job-4a",
	"76f37791d5b4ad436c23cb073db280877b918276a13b37dd2e6d172eb8b1cde7": "job-5a",
	"dca5dbe5a3e3d07c3ab3996a2fb87c374fe0993b594098acbf6928f2c00209b4": "job-5b",
	"e156518fbe87a5b0db53b65b1fda92e8e1e6d81c5b36be275be26009a7d9538e": "job-5c",
	"d55f2d0206fd2ffa1bdfb6fb3b16fb45cd1d039bbb0350fbfb303a9051d4310d": "job-6a",
	"9ac1f3cc0f80caf4ccdb94013e28f602588849fe068c58e2b4905afcfb01dc70": "job-6b",
	"1076dcf843bd7411fd13a4a4bfb09d40f869f2c9eacce276fec9b53b58654204": "job-6f",
	"2004e9b27a151ad278142172c849f662154b14bc6a526eb21a881891b181e3c9": "job-7a",
	"59a9c1e2cb5146c910517b2f1f9f30aa513e3b2bfd03dd052c58402ca0ad5dea": "job-7b",
	"b67cce9a2742c90526ff522ed8f69efe85079eba742d66fc3e638be0827afb77": "job-7c",
	"6ddaef283fb8e286ad2ffbde7b114308a26a60348f789277d246d07ea79817fd": "job-8a",
	"13b723edd861e8b00b1a845807c8e304fa7022b735de1a7eb5004b5ff092ab46": "job-8b",
	"25126837565b68e9332253346db44c24451424c2f9850fee21b674291e358111": "job-8c",
	"0834d3ff224555753d81489c87ae212f4d287f1785553310b7278a7f207a4b39": "job-8d",
	"eb4216b97e8c6f3b18b0a66491286578afb1e30c6e87616a594f8fbad19f453a": "job-9a",
	"1aa371e48561707fcccdd8c6c7fde471aa74d09d35bfbda394ed47a5a3970563": "job-9b",
	"da007b07a9db23feb10bfbcac40ad529d8602cda997b0ac6fa64ca16f91cec02": "job-9c",
	"0d7c1b7ba29bebe1b91299f8f18e74e38a5d42c17e3669621f5b1690455d038c": "job-9d",
}
