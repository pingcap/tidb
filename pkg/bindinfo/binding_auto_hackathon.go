package bindinfo

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
)

func generateBindingPlans(sPool util.DestroyableSessionPool, currentDB, sql string) (plans []*BindingPlanInfo, rerr error) {
	rerr = callWithSCtx(sPool, false, func(sctx sessionctx.Context) error {
		sql = strings.TrimSpace(sql)
		sctx.GetSessionVars().CurrentDB = currentDB

		bindingPlan, err := generateBindingPlan(sctx, sql)
		if err != nil {
			return err
		}
		plans = append(plans, bindingPlan)
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
