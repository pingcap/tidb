package core_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/set"
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

func TestSlowQueryDigestExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	const (
		digest1 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		digest2 = "1111111111111111111111111111111111111111111111111111111111111111"
	)
	cases := []struct {
		name      string
		condition string
		digests   set.StringSet
	}{
		{"equality", fmt.Sprintf("digest = '%s'", digest1), set.NewStringSet(digest1)},
		{"reversed equality", fmt.Sprintf("'%s' = digest", digest1), set.NewStringSet(digest1)},
		{"in", fmt.Sprintf("digest in ('%s', '%s')", digest1, digest2), set.NewStringSet(digest1, digest2)},
		{"or", fmt.Sprintf("digest = '%s' or digest = '%s'", digest1, digest2), set.NewStringSet(digest1, digest2)},
		{"intersection", fmt.Sprintf("digest in ('%s', '%s') and digest = '%s'", digest1, digest2, digest1), set.NewStringSet(digest1)},
		{"mixed column or", fmt.Sprintf("digest = '%s' or plan_digest = 'other-plan'", digest1), set.NewStringSet()},
		{"like", fmt.Sprintf("digest like '%s%%'", digest1), set.NewStringSet()},
	}
	p := parser.New()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			memTable, ok := getPKDBLogicalMemTable(t, dom, se, p,
				"select * from information_schema.slow_query where "+tc.condition)
			require.True(t, ok)
			extractor := memTable.Extractor.(*core.SlowQueryExtractor)
			require.False(t, extractor.SkipRequest)
			require.Equal(t, tc.digests, extractor.Digests)
		})
	}

	memTable, ok := getPKDBLogicalMemTable(t, dom, se, p, fmt.Sprintf(
		"select query from information_schema.slow_query where time >= '2026-07-01 00:00:00' and digest = '%s'", digest1))
	require.True(t, ok)
	extractor := memTable.Extractor.(*core.SlowQueryExtractor)
	require.True(t, extractor.Enable)
	require.Equal(t, set.NewStringSet(digest1), extractor.Digests)

	tk := testkit.NewTestKit(t, store)
	explainRows := tk.MustQuery(fmt.Sprintf(
		"explain format = 'brief' select query from information_schema.slow_query where digest = '%s'", digest1)).Rows()
	require.Contains(t, fmt.Sprint(explainRows), fmt.Sprintf("digests:[%q]", digest1))
	require.Contains(t, fmt.Sprint(explainRows), "Selection", "slow log predicates must remain for records without Digest")

	logPath := filepath.Join(t.TempDir(), "tidb-slow.log")
	require.NoError(t, os.WriteFile(logPath, []byte(
		"# Time: 2026-08-17T14:09:13.000000+08:00\n# Digest: "+digest1+"\n# Query_time: 1\nselect 1;\n"+
			"# Time: 2026-08-17T14:09:14.000000+08:00\n# Digest: "+digest2+"\n# Query_time: 2\nselect 2;\n"+
			"# Time: 2026-08-17T14:09:15.000000+08:00\n# Query_time: 3\nselect 3;\n"), 0600))
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file = '%s'", logPath))
	tk.MustQuery(fmt.Sprintf("select query from information_schema.slow_query where digest = '%s'", digest1)).Check(testkit.Rows("select 1;"))
	tk.MustQuery(fmt.Sprintf("select query from information_schema.slow_query where digest in ('%s', '%s') order by time", digest1, digest2)).Check(testkit.Rows("select 1;", "select 2;"))
}

func TestDigestExtractorsKnownLimitations(t *testing.T) {
	originalCollationEnabled := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(true)
	t.Cleanup(func() { collate.SetNewCollationEnabledForTest(originalCollationEnabled) })
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(conf *config.Config) { conf.Instance.StmtSummaryEnablePersistent = false })
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	previousEnabled := tk.MustQuery("select @@global.tidb_enable_stmt_summary").Rows()[0][0]
	tk.MustExec("set @@global.tidb_enable_stmt_summary = 1")
	t.Cleanup(func() { tk.MustExec(fmt.Sprintf("set @@global.tidb_enable_stmt_summary = %s", previousEnabled)) })
	tk.MustExec("create table digest_samples (v int)")
	tk.MustExec("insert into digest_samples values (1)")
	tk.MustQuery("select v from digest_samples").Check(testkit.Rows("1"))
	_, sqlDigest := parser.NormalizeDigest("select v from digest_samples")
	digest := sqlDigest.String()
	_, otherSQLDigest := parser.NormalizeDigest("select 2")
	otherDigest := otherSQLDigest.String()
	logPath := filepath.Join(t.TempDir(), "tidb-slow.log")
	require.NoError(t, os.WriteFile(logPath, []byte(
		"# Time: 2026-08-17T14:09:13.000000+08:00\n# Digest: "+digest+"\n# Query_time: 1\nselect v from digest_samples;\n"+
			"# Time: 2026-08-17T14:09:14.000000+08:00\n# Digest: "+otherDigest+"\n# Query_time: 1\nselect 2;\n"), 0600))

	cases := []struct {
		name             string
		condition        string
		slowQueryMatches bool
	}{
		{"upper", fmt.Sprintf("upper(digest) = '%s'", strings.ToUpper(digest)), false},
		{"trailing space", fmt.Sprintf("digest = '%s '", digest), false},
		{"case insensitive", fmt.Sprintf("digest = ('%s' collate utf8mb4_general_ci)", strings.ToUpper(digest)), false},
		{"compatible equalities", fmt.Sprintf("digest = '%s' and digest = '%s '", digest, digest), true},
		{"same case insensitive collation", fmt.Sprintf("digest = ('%s' collate utf8mb4_general_ci) and digest = ('%s' collate utf8mb4_general_ci)", digest, strings.ToUpper(digest)), true},
		{"mixed collations", fmt.Sprintf("digest = '%s' and digest = ('%s' collate utf8mb4_general_ci)", digest, strings.ToUpper(digest)), true},
	}
	for _, table := range []string{"statements_summary", "slow_query"} {
		t.Run(table, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					caseTK := testkit.NewTestKit(t, store)
					caseTK.MustExec(fmt.Sprintf("set @@tidb_slow_query_file = '%s'", logPath))
					// TODO: both readers lack function and collation handling.
					// StatementsSummaryExtractor also trusts raw-string intersection;
					// SlowQueryExtractor retains predicates when the intersection is empty.
					// Incorrect results below document the remaining limitations.
					// CONCAT prevents extractCol from extracting the Digest, so the
					// reference query evaluates the original expression normally.
					sql := "select distinct digest from information_schema." + table + " where " + tc.condition
					referenceSQL := strings.ReplaceAll(sql, "digest", "concat(digest, '')")
					caseTK.MustQuery(referenceSQL).Check(testkit.Rows(digest))
					if tc.slowQueryMatches && table == "slow_query" {
						caseTK.MustQuery(sql).Check(testkit.Rows(digest))
					} else {
						caseTK.MustQuery(sql).Check(testkit.Rows())
					}
				})
			}
		})
	}
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
