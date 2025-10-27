package audit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/extension/enterprise/audit"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

type entryFilterTest struct {
	entry audit.LogEntry
	match bool
}

func TestFilterSpec(t *testing.T) {
	audit.Register4Test()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	cases := []struct {
		spec        audit.FilterSpec
		invalid     bool
		entries     []entryFilterTest
		jsonContent string
	}{
		{
			spec: audit.FilterSpec{
				Classes: []string{"abc"},
			},
			invalid: true,
		},
		{
			spec: audit.FilterSpec{
				ClassesExclude: []string{"abc"},
			},
			invalid: true,
		},
		{
			spec: audit.FilterSpec{
				Tables: []string{"t1"},
			},
			invalid: true,
		},
		{
			spec: audit.FilterSpec{
				TablesExclude: []string{"t1"},
			},
			invalid: true,
		},
		{
			// empty filterSpec can pass all types of log
			spec:        audit.FilterSpec{},
			jsonContent: "{}",
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassQuery}},
					match: true,
				},
				{
					entry: audit.LogEntry{
						User:    "u1",
						Classes: []audit.EventClass{audit.ClassQuery},
						Tables: []stmtctx.TableEntry{
							{DB: "db1", Table: "t1"},
							{DB: "db2", Table: "t2"},
						},
					},
					match: true,
				},
				{
					entry: audit.LogEntry{
						Err: errors.New(""),
					},
					match: true,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				Classes: []string{"QUERY_DML", "QUERY_DDL"},
			},
			jsonContent: `{"class": ["QUERY_DML", "QUERY_DDL"]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassQuery}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassQuery, audit.ClassDML}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassQuery, audit.ClassDDL}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassQuery, audit.ClassSelect}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Err: errors.New(""), Classes: []audit.EventClass{audit.ClassQuery, audit.ClassDML}, Tables: []stmtctx.TableEntry{
						{DB: "db1", Table: "t1"},
						{DB: "db2", Table: "t2"},
					}},
					match: true,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				ClassesExclude: []string{"SELECT", "INSERT"},
			},
			jsonContent: `{"class_excl": ["SELECT", "INSERT"]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassQuery}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassQuery, audit.ClassSelect}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassQuery, audit.ClassInsert}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassQuery, audit.ClassUpdate}},
					match: true,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				Classes:        []string{"QUERY_DDL", "QUERY_DML"},
				ClassesExclude: []string{"SELECT", "INSERT"},
			},
			jsonContent: `{"class": ["QUERY_DDL", "QUERY_DML"], "class_excl": ["SELECT", "INSERT"]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDDL}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML, audit.ClassDDL}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassUpdate}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML, audit.ClassUpdate}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML, audit.ClassInsert}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML, audit.ClassSelect}},
					match: false,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				Classes:        []string{"SELECT", "QUERY_DML"},
				ClassesExclude: []string{"SELECT", "INSERT"},
			},
			jsonContent: `{"class": ["SELECT", "QUERY_DML"], "class_excl": ["SELECT", "INSERT"]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDDL}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML, audit.ClassDDL}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassUpdate}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML, audit.ClassUpdate}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML, audit.ClassInsert}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML, audit.ClassSelect}},
					match: false,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				Tables: []string{"*.*"},
			},
			jsonContent: `{"table": ["*.*"]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "test", Table: "t1"},
					}},
					match: true,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				Tables: []string{"db*.test*", "test.t1"},
			},
			jsonContent: `{"table": ["db*.test*", "test.t1"]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "db1", Table: "test1"},
					}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Err: errors.New(""), Tables: []stmtctx.TableEntry{
						{DB: "db2", Table: "test2"},
					}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "test", Table: "t1"},
					}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "db1", Table: "t1"},
					}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "d1", Table: "test1"},
					}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "test", Table: "test1"},
					}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "d2", Table: "t2"},
						{DB: "db2", Table: "test2"},
					}},
					match: true,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				TablesExclude: []string{"db*.test*", "test.t1"},
			},
			jsonContent: `{"table_excl": ["db*.test*", "test.t1"]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "d2", Table: "t2"},
						{DB: "db2", Table: "test2"},
					}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "d2", Table: "t2"},
						{DB: "test", Table: "t1"},
					}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "d2", Table: "t2"},
						{DB: "test", Table: "t2"},
					}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "d2", Table: "t2"},
						{DB: "db2", Table: "t2"},
					}},
					match: true,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				Tables:        []string{"db*.test*"},
				TablesExclude: []string{"db2.test*"},
			},
			jsonContent: `{"table": ["db*.test*"], "table_excl": ["db2.test*"]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "db2", Table: "test2"},
					}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "db3", Table: "test1"},
					}},
					match: true,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				Tables:        []string{"db*.test*"},
				TablesExclude: []string{"db*.test*"},
			},
			jsonContent: `{"table": ["db*.test*"], "table_excl": ["db*.test*"]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "db2", Table: "test2"},
					}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{
						{DB: "db3", Table: "test1"},
					}},
					match: false,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				StatusCodes: []int{0},
			},
			jsonContent: `{"status_code": [0]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Err: errors.New("err")},
					match: true,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				StatusCodes: []int{1},
			},
			jsonContent: `{"status_code": [1]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Err: errors.New("err")},
					match: false,
				},
			},
		},
		{
			spec: audit.FilterSpec{
				StatusCodes: []int{0, 1},
			},
			jsonContent: `{"status_code": [0, 1]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Err: errors.New("err")},
					match: true,
				},
			},
		},
	}

	for _, c := range cases {
		// test filter
		bs, err := json.Marshal(c.spec)
		require.NoError(t, err)
		specJSON := string(bs)
		if c.invalid {
			require.Error(t, c.spec.Validate(), specJSON)
			continue
		}
		require.NoError(t, c.spec.Validate(), specJSON)
		filter := audit.LogFilter{Name: "test", Filter: []audit.FilterSpec{c.spec}}
		fn := filter.CreateFilterFunc()
		for _, ent := range c.entries {
			require.Equal(t, ent.match, fn(&ent.entry), "%v %v", specJSON, ent.entry)
		}

		// test creating filter by sql
		tk.MustQuery(fmt.Sprintf(`SELECT audit_log_create_filter('test', '{"filter":[%s]}')`, c.jsonContent)).Check(testkit.Rows("OK"))
		tk.MustQuery(`SELECT audit_log_create_rule('%@%','test')`).Check(testkit.Rows("OK"))
		tk.MustQuery("SELECT count(*) FROM mysql.audit_log_filters").Check(testkit.Rows("1"))
		tk.MustQuery("SELECT COALESCE(content->>'$.filter[0]', '{}') FROM mysql.audit_log_filters WHERE filter_name = 'test'").Check(testkit.Rows(c.jsonContent))
		rules, err := audit.ListFilterRules(context.Background(), tk.Session())
		require.NoError(t, err, specJSON)
		require.Equal(t, 1, len(rules), specJSON)
		require.Equal(t, c.spec, rules[0].Filter.Filter[0], specJSON)
		require.Equal(t, "test", rules[0].FilterName, specJSON)
		tk.MustQuery("SELECT audit_log_remove_rule('%@%','test')").Check(testkit.Rows("OK"))
		tk.MustQuery("SELECT audit_log_remove_filter('test')").Check(testkit.Rows("OK"))
	}
	_, err := deleteAllAuditLogs(workDir, "tidb-audit", ".log")
	require.NoError(t, err)
}

func TestFilterRule(t *testing.T) {
	audit.Register4Test()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	cases := []struct {
		rule        audit.LogFilterRule
		entries     []entryFilterTest
		jsonContent string
	}{
		{
			rule: audit.LogFilterRule{
				User:       "root",
				FilterName: "f1",
				Enabled:    true,
				Filter: &audit.LogFilter{
					Name: "f1",
				},
			},
			jsonContent: `{}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "u1"},
					match: false,
				},
			},
		},
		{
			rule: audit.LogFilterRule{
				User:       "root",
				FilterName: "f1",
				Enabled:    false,
				Filter: &audit.LogFilter{
					Name: "f1",
				},
			},
			jsonContent: `{}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "u1"},
					match: false,
				},
			},
		},
		{
			rule: audit.LogFilterRule{
				User:       "root@127.0.0._",
				FilterName: "f1",
				Enabled:    true,
				Filter: &audit.LogFilter{
					Name: "f1",
				},
			},
			jsonContent: `{}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "u1"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Host: "localhost"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Host: "127.0.0.1"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Host: "127.0.0.11"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Host: "%"},
					match: false,
				},
			},
		},
		{
			rule: audit.LogFilterRule{
				User:       "%@%",
				FilterName: "f1",
				Enabled:    true,
				Filter: &audit.LogFilter{
					Name: "f1",
				},
			},
			jsonContent: `{}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "u1"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Host: "localhost"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Host: "127.0.0.1"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Host: "127.0.0.11"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Host: "%"},
					match: true,
				},
			},
		},
		{
			rule: audit.LogFilterRule{
				User:       "root",
				FilterName: "f1",
				Enabled:    true,
				Filter: &audit.LogFilter{
					Name:   "f1",
					Filter: []audit.FilterSpec{},
				},
			},
			jsonContent: `{}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "u1"},
					match: false,
				},
			},
		},
		{
			rule: audit.LogFilterRule{
				User:       "root",
				FilterName: "f1",
				Enabled:    true,
				Filter: &audit.LogFilter{
					Name: "f1",
					Filter: []audit.FilterSpec{
						// An audit.LogEntry matches the Filter if only any filterSpec matches
						{
							Classes: []string{"QUERY_DML"},
						},
						{
							Classes: []string{"QUERY_DDL"},
						},
					},
				},
			},
			jsonContent: `{"filter":[{"class":["QUERY_DML"]},{"class":["QUERY_DDL"]}]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDDL}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassConnection}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "user1", Classes: []audit.EventClass{audit.ClassDML}},
					match: false,
				},
			},
		},
		{
			rule: audit.LogFilterRule{
				User:       "root",
				FilterName: "f1",
				Enabled:    true,
				Filter: &audit.LogFilter{
					Name: "f1",
					Filter: []audit.FilterSpec{
						{
							Classes: []string{"QUERY_DML"},
						},
						{
							ClassesExclude: []string{"QUERY_DML"},
						},
					},
				},
			},
			jsonContent: `{"filter":[{"class":["QUERY_DML"]},{"class_excl":["QUERY_DML"]}]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDDL}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML, audit.ClassDDL}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "user1", Classes: []audit.EventClass{audit.ClassDML}},
					match: false,
				},
			},
		},
		{
			rule: audit.LogFilterRule{
				User:       "root",
				FilterName: "f1",
				Enabled:    true,
				Filter: &audit.LogFilter{
					Name: "f1",
					Filter: []audit.FilterSpec{
						{
							Tables: []string{"test*.t*"},
						},
						{
							TablesExclude: []string{"test*.t*"},
						},
					},
				},
			},
			jsonContent: `{"filter":[{"table":["test*.t*"]},{"table_excl":["test*.t*"]}]}`,
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root"},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{{DB: "test", Table: "t"}}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Tables: []stmtctx.TableEntry{{DB: "test", Table: "c"}}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "user1"},
					match: false,
				},
			},
		},
	}

	for i, c := range cases {
		// test rule
		fn := c.rule.CreateFilterFunc()
		for j, ent := range c.entries {
			require.Equal(t, ent.match, fn(&ent.entry), "case index: %d entry index: %d", i, j)
		}

		// test creating rule by sql
		fmt.Println(c.jsonContent)
		tk.MustQuery(fmt.Sprintf(`SELECT audit_log_create_filter('%s', '%s')`, c.rule.FilterName, c.jsonContent)).Check(testkit.Rows("OK"))
		tk.MustQuery(fmt.Sprintf(`SELECT audit_log_create_rule('%s','%s')`, c.rule.User, c.rule.FilterName)).Check(testkit.Rows("OK"))
		tk.MustQuery("SELECT count(*) FROM mysql.audit_log_filters").Check(testkit.Rows("1"))
		tk.MustQuery("SELECT count(*) FROM mysql.audit_log_filter_rules").Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf("SELECT content FROM mysql.audit_log_filters WHERE filter_name = '%s'", c.rule.FilterName)).Check(testkit.Rows(c.jsonContent))
		if !c.rule.Enabled {
			tk.MustQuery(fmt.Sprintf(`SELECT audit_log_disable_rule('%s','%s')`, c.rule.User, c.rule.FilterName)).Check(testkit.Rows("OK"))
		}
		rules, err := audit.ListFilterRules(context.Background(), tk.Session())
		require.NoError(t, err, c.jsonContent)
		require.Equal(t, 1, len(rules), c.jsonContent)
		if rules[0].Filter != nil && len(rules[0].Filter.Filter) > 0 {
			require.Equal(t, c.rule, *rules[0], c.jsonContent)
		}
		tk.MustQuery(fmt.Sprintf(`SELECT audit_log_remove_rule('%s','%s')`, c.rule.User, c.rule.FilterName)).Check(testkit.Rows("OK"))
		tk.MustQuery(fmt.Sprintf(`SELECT audit_log_remove_filter('%s')`, c.rule.FilterName)).Check(testkit.Rows("OK"))
	}
}

func TestFilterRuleBundle(t *testing.T) {
	cases := []struct {
		bundle  *audit.LogFilterRuleBundle
		entries []entryFilterTest
	}{
		{
			bundle: audit.NewLogFilterRuleBundle([]*audit.LogFilterRule{
				{
					User:       "root",
					FilterName: "f1",
					Enabled:    true,
					Filter: &audit.LogFilter{
						Name: "f1",
						Filter: []audit.FilterSpec{
							{Classes: []string{"query_ddl"}},
						},
					},
				},
				{
					User:       "root",
					FilterName: "f2",
					Enabled:    true,
					Filter: &audit.LogFilter{
						Name: "f2",
						Filter: []audit.FilterSpec{
							{Classes: []string{"query_dml"}, ClassesExclude: []string{"query_ddl"}},
						},
					},
				},
				{
					User:       "u1",
					FilterName: "f2",
					Enabled:    true,
					Filter: &audit.LogFilter{
						Name: "f2",
						Filter: []audit.FilterSpec{
							{Classes: []string{"query_dml"}},
						},
					},
				},
			}),
			entries: []entryFilterTest{
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDDL}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassDML, audit.ClassDDL}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "root", Classes: []audit.EventClass{audit.ClassConnection}},
					match: false,
				},
				{
					entry: audit.LogEntry{User: "u1", Classes: []audit.EventClass{audit.ClassDML}},
					match: true,
				},
				{
					entry: audit.LogEntry{User: "u1", Classes: []audit.EventClass{audit.ClassDDL}},
					match: false,
				},
			},
		},
	}

	for i, c := range cases {
		for j, ent := range c.entries {
			filtered := c.bundle.Filter(&ent.entry)
			if filtered != nil {
				require.Same(t, &ent.entry, filtered, "case index: %d entry index: %d", i, j)
			}
			require.Equal(t, ent.match, filtered != nil, "case index: %d entry index: %d", i, j)
		}
	}
}
