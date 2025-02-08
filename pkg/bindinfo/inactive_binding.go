package bindinfo

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func (h *globalBindingHandle) RecordInactiveBindings() (err error) {
	execCountThreshold := 100
	stmtQuery := `select digest, query_sample_text, charset, collation, plan_hint
				from information_schema.statements_summary
				where stmt_type='Select' and exec_count > ?`
	var rows []chunk.Row
	err = h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		rows, _, err = execRows(sctx, stmtQuery, execCountThreshold)
		return err
	})
	if err != nil {
		return err
	}

	for _, row := range rows {
		digest := row.GetString(0)
		sql := row.GetString(1)
		charset := row.GetString(2)
		collation := row.GetString(3)
		hint := row.GetString(4)
		fmt.Println(digest, sql, charset, collation, hint)
	}

	return nil
}
