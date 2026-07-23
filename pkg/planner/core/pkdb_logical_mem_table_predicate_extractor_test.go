package core_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func getPKDBLogicalMemTable(t *testing.T, dom *domain.Domain, se types.Session, p *parser.Parser, sql string) (*logicalop.LogicalMemTable, bool) {
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	ctx := context.Background()
	builder, _ := core.NewPlanBuilder().Init(se.GetPlanCtx(), dom.InfoSchema(), hint.NewQBHintHandler(nil))
	plan, err := builder.Build(ctx, resolve.NewNodeW(stmt))
	require.NoError(t, err)

	logicalPlan, err := core.LogicalOptimizeTest(ctx, builder.GetOptFlag(), plan.(base.LogicalPlan))
	require.NoError(t, err)

	leafPlan := logicalPlan
	for len(leafPlan.Children()) > 0 {
		leafPlan = leafPlan.Children()[0]
	}

	memTable, ok := leafPlan.(*logicalop.LogicalMemTable)
	return memTable, ok
}

func TestLogReplWorkflowHistoryGlobalExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	p := parser.New()
	cases := []struct {
		sql             string
		startWorkflowID uint64
		endWorkflowID   uint64
		limit           uint64
		skipRequest     bool
	}{
		{
			sql:   "select * from information_schema.LR_WORKFLOW_HISTORY_GLOBAL limit 5",
			limit: 5,
		},
		{
			sql:             "select * from information_schema.LR_WORKFLOW_HISTORY_GLOBAL where WORKFLOW_ID >= 10 and WORKFLOW_ID < 20 limit 5",
			startWorkflowID: 10,
			endWorkflowID:   20,
			limit:           5,
		},
		{
			sql:             "select * from information_schema.LR_WORKFLOW_HISTORY_GLOBAL where WORKFLOW_ID >= 10 limit 5 offset 20",
			startWorkflowID: 10,
			limit:           25,
		},
		{
			sql:             "select * from information_schema.LR_WORKFLOW_HISTORY_GLOBAL where WORKFLOW_ID >= 10 and WORKFLOW_ID < 20 limit 5 offset 20",
			startWorkflowID: 10,
			endWorkflowID:   20,
			limit:           10,
		},
		{
			sql:             "select * from information_schema.LR_WORKFLOW_HISTORY_GLOBAL where WORKFLOW_ID <= 20 and START_TIME > '2026-01-01 00:00:00' limit 5",
			startWorkflowID: 0,
			endWorkflowID:   21,
		},
		{
			sql:             "select * from information_schema.LR_WORKFLOW_HISTORY_GLOBAL where WORKFLOW_ID > 1 and WORKFLOW_ID < 5 and WORKFLOW_ID = 3 limit 5",
			startWorkflowID: 3,
			endWorkflowID:   4,
			limit:           1,
		},
		{
			sql: "select * from information_schema.LR_WORKFLOW_HISTORY_GLOBAL where WORKFLOW_ID = 10 or WORKFLOW_ID = 20 limit 5",
		},
		{
			sql:             "select * from information_schema.LR_WORKFLOW_HISTORY_GLOBAL where (WORKFLOW_ID = 10 or WORKFLOW_ID = 20) and WORKFLOW_ID >= 5 limit 5",
			startWorkflowID: 5,
		},
		{
			sql: "select * from information_schema.LR_WORKFLOW_HISTORY_GLOBAL where WORKFLOW_ID in (1, 3, 5) limit 5",
		},
		{
			sql: "select * from information_schema.LR_WORKFLOW_HISTORY_GLOBAL where START_TIME > '2026-01-01 00:00:00' limit 5",
		},
		{
			sql:         "select * from information_schema.LR_WORKFLOW_HISTORY_GLOBAL where WORKFLOW_ID > 20 and WORKFLOW_ID <= 10",
			skipRequest: true,
		},
	}

	for _, tc := range cases {
		logicalMemTable, ok := getPKDBLogicalMemTable(t, dom, se, p, tc.sql)
		require.True(t, ok, "SQL: %v", tc.sql)
		require.NotNil(t, logicalMemTable.Extractor, "SQL: %v", tc.sql)

		extractor := logicalMemTable.Extractor.(*core.LogReplWorkflowHistoryGlobalExtractor)
		require.Equal(t, tc.skipRequest, extractor.SkipRequest, "SQL: %v", tc.sql)
		if tc.skipRequest {
			continue
		}

		require.Equal(t, tc.startWorkflowID, extractor.StartWorkflowID, "SQL: %v", tc.sql)
		require.Equal(t, tc.endWorkflowID, extractor.EndWorkflowID, "SQL: %v", tc.sql)
		require.Equal(t, tc.limit, extractor.Limit, "SQL: %v", tc.sql)
	}
}
