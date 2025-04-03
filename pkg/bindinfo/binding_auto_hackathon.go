package bindinfo

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
)

func generateBindingPlans(sPool util.DestroyableSessionPool, currentDB, sql string) (plans []*BindingPlanInfo, rerr error) {
	rerr = callWithSCtx(sPool, false, func(sctx sessionctx.Context) error {
		sql = strings.TrimSpace(sql)
		sctx.GetSessionVars().CurrentDB = currentDB

		defaultPlan, _, err := explainPlan(sctx, sql)
		if err != nil {
			return err
		}
		relatedCostFactors := collectRelatedCostFactors(sctx, defaultPlan)

		// change these related cost factors randomly to walk in the plan space and sample some plans
		if len(relatedCostFactors) > 0 {
			memorizedPlan := make(map[string]struct{})
			for walkStep := 0; walkStep < 100; walkStep++ {
				// each step, randomly change one cost factor
				idx := rand.Intn(len(relatedCostFactors))
				factorValue := rand.Float64() * 1000 // scale range: [0, 1000]
				*relatedCostFactors[idx] = factorValue

				// generate a new plan based on the modified cost factors
				bindingPlan, err := generateBindingPlan(sctx, sql)
				if err != nil {
					return err
				}
				planHint, err := bindingPlan.Hint.Restore()
				if err != nil {
					return err
				}
				if _, ok := memorizedPlan[planHint]; ok {
					// skip the same plan
					continue
				}
				memorizedPlan[planHint] = struct{}{}
				plans = append(plans, bindingPlan)
			}
		}

		return nil
	})
	return
}

func generateBindingPlan(sctx sessionctx.Context, sql string) (*BindingPlanInfo, error) {
	plan, hint, err := explainPlan(sctx, sql)
	if err != nil {
		return nil, err
	}

	bindingSQL := fmt.Sprintf("%v /*+ %v */ %v", sql[:6], hint, sql[6:]) // "select" + hint + ...
	binding := &Binding{
		OriginalSQL: sql, // TODO: normalize
		BindSQL:     bindingSQL,
		Db:          sctx.GetSessionVars().CurrentDB,
		Source:      "generated vis cost factors",
	}
	if err = prepareHints(sctx, binding); err != nil {
		return nil, err
	}

	return &BindingPlanInfo{
		Binding: binding,
		Plan:    plan,
	}, nil
}

func explainPlan(sctx sessionctx.Context, sql string) (plan, hint string, err error) {
	rows, _, err := execRows(sctx, "EXPLAIN "+sql)
	if err != nil {
		return "", "", err
	}

	/*
		+----------------------------+----------+-----------+---------------------+---------------------------------+
		| id                         | estRows  | task      | access object       | operator info                   |
		+----------------------------+----------+-----------+---------------------+---------------------------------+
		| StreamAgg_20               | 1.00     | root      |                     | funcs:count(Column#9)->Column#4 |
		| └─IndexReader_21           | 1.00     | root      |                     | index:StreamAgg_8               |
		|   └─StreamAgg_8            | 1.00     | cop[tikv] |                     | funcs:count(1)->Column#9        |
		|     └─IndexFullScan_19     | 10000.00 | cop[tikv] | table:t, index:a(a) | keep order:false, stats:pseudo  |
		+----------------------------+----------+-----------+---------------------+---------------------------------+
	*/
	for _, r := range rows {
		op := r.GetString(0)
		estRows := r.GetString(1)
		task := r.GetString(2)
		accObj := r.GetString(3)
		opInfo := r.GetString(4)
		plan = plan + "\n" + fmt.Sprintf("%s\t%s\t%s\t%s\t%s", op, estRows, task, accObj, opInfo)
	}

	rows, _, err = execRows(sctx, "EXPLAIN FORMAT='hint' "+sql)
	if err != nil {
		return "", "", err
	}
	hint = rows[0].GetString(0)
	return
}

func collectRelatedCostFactors(sctx sessionctx.Context, plan string) []*float64 {
	factors := make([]*float64, 0, 4)
	if strings.Contains(plan, "Index") {
		factors = append(factors, &sctx.GetSessionVars().IndexScanCostFactor,
			&sctx.GetSessionVars().IndexLookupCostFactor,
			&sctx.GetSessionVars().IndexMergeCostFactor)
	}
	if strings.Contains(plan, "Table") {
		factors = append(factors, &sctx.GetSessionVars().TableFullScanCostFactor,
			&sctx.GetSessionVars().TableRangeScanCostFactor,
			&sctx.GetSessionVars().TableRowIDScanCostFactor)
	}
	if strings.Contains(plan, "Join") {
		factors = append(factors, &sctx.GetSessionVars().HashJoinCostFactor,
			&sctx.GetSessionVars().MergeJoinCostFactor,
			&sctx.GetSessionVars().IndexJoinCostFactor)
	}
	if strings.Contains(plan, "Sort") {
		factors = append(factors, &sctx.GetSessionVars().SortCostFactor,
			&sctx.GetSessionVars().TopNCostFactor)
	}
	if strings.Contains(plan, "Agg") {
		factors = append(factors, &sctx.GetSessionVars().HashAggCostFactor,
			&sctx.GetSessionVars().StreamAggCostFactor)
	}
	return factors
}
