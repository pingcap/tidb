// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func maskingDiagnostic(level string, err error) string {
	code, state, msg := uint16(mysql.ErrUnknown), mysql.DefaultMySQLState, ""
	switch x := errors.Cause(err).(type) {
	case *terror.Error:
		sqlErr := terror.ToSQLError(x)
		code, state, msg = sqlErr.Code, sqlErr.State, sqlErr.Message
	default:
		msg = x.Error()
	}
	return fmt.Sprintf("%s %d (%s): %s", level, code, state, msg)
}

// maskingDiagnostics runs a statement and returns the error that a client
// receives, followed by the warnings of the statement.
func maskingDiagnostics(tk *testkit.TestKit, sql string) []string {
	var diags []string
	rs, err := tk.Exec(sql)
	if err == nil && rs != nil {
		_, err = session.GetRows4Test(context.Background(), tk.Session(), rs)
		terror.Log(rs.Close())
	}
	if err != nil {
		diags = append(diags, maskingDiagnostic("Error", err))
	}
	for _, w := range tk.Session().GetSessionVars().StmtCtx.GetWarnings() {
		if w.Level != "Error" {
			diags = append(diags, maskingDiagnostic(w.Level, w.Err))
		}
	}
	return diags
}

func prepareMaskingDiagnostics(t *testing.T) (root, user *testkit.TestKit) {
	store := testkit.CreateMockStore(t)
	root = testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create table t(id int primary key, c varchar(100), n int, b varchar(100), key(c))")
	root.MustExec("insert into t values (1, 'secret', 1, 'plain')")
	root.MustExec("create table plain(id int primary key, b varchar(100))")
	root.MustExec("insert into plain values (1, 'plain')")
	root.MustExec("create table dst(id int primary key, v int)")
	root.MustExec("insert into dst values (1, 0)")
	root.MustExec("create masking policy p on t(c) as case when current_user() = 'root@%' then c else mask_full(c) end enable")
	root.MustExec("create user u1")
	root.MustExec("grant select, insert, update, delete on test.* to u1")
	user = testkit.NewTestKit(t, store)
	require.NoError(t, user.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	user.MustExec("use test")
	return root, user
}

// TestMaskingPolicyDiagnostics tests that a statement evaluating the original
// values of a column masked for the session does not disclose them in its
// warnings and errors, and that the other statements keep their diagnostics.
func TestMaskingPolicyDiagnostics(t *testing.T) {
	root, user := prepareMaskingDiagnostics(t)
	root.MustExec("create table tg(id int primary key, c varchar(100), g int as (cast(c as signed)) virtual)")
	root.MustExec("set sql_mode = ''")
	root.MustExec("insert into tg(id, c) values (1, 'secret')")
	root.MustExec("set sql_mode = default")
	root.MustExec("create masking policy pg on tg(c) as case when current_user() = 'root@%' then c else mask_full(c) end enable")

	truncated := func(tp, value string) string {
		return fmt.Sprintf("Warning 1292 (22007): Truncated incorrect %s value: '%s'", tp, value)
	}
	truncatedErr := func(tp, value string) string {
		return fmt.Sprintf("Error 1292 (22007): Truncated incorrect %s value: '%s'", tp, value)
	}
	cases := []struct {
		sql string
		// original is the diagnostics of a session that reads the original
		// values, and masked those of a session the policy applies to.
		original, masked []string
	}{
		{"select 1 from t where cast(c as signed) > 0", []string{truncated("INTEGER", "secret")}, []string{truncated("INTEGER", "?")}},
		{"select 1 from t where cast(c as decimal(10,2)) > 0", []string{truncated("DECIMAL", "secret")}, []string{truncated("DECIMAL", "?")}},
		{"select 1 from t where c + 0 > 0", []string{truncated("DOUBLE", "secret")}, []string{truncated("DOUBLE", "?")}},
		{"select 1 from t where cast(c as time) is null", []string{truncated("time", "secret")}, []string{truncated("time", "?")}},
		{"select 1 from t where cast(c as date) is null",
			[]string{"Warning 1292 (22007): Incorrect datetime value: 'secret'"},
			[]string{"Warning 1292 (22007): Incorrect datetime value: '?'"}},
		{"select 1 from t where format(1, 2, c) = 'x'",
			[]string{"Warning 1649 (HY000): Unknown locale: 'secret'"},
			[]string{"Warning 1649 (HY000): Unknown locale: '?'"}},
		{"select 1 from t where cast(c as json) is null",
			[]string{"Error 1105 (HY000): Invalid JSON text: The document root must not be followed by other values."},
			[]string{"Error 1105 (HY000): Invalid JSON text: ?"}},
		{"select 1 from t where 'x' regexp concat(c, '(')",
			[]string{"Error 1105 (HY000): Got error 'error parsing regexp: missing closing ): `secret(`' from regexp"},
			[]string{"Error 1105 (HY000): Got error '?' from regexp"}},
		{"select 1 from t where bin_to_uuid(c) is null",
			[]string{"Error 1411 (HY000): Incorrect string value: 'secret' for function bin_to_uuid"},
			[]string{"Error 1411 (HY000): Incorrect string value: '?' for function bin_to_uuid"}},
		// The store returns the two errors above as text, and they are reported
		// with the code of an unknown error. Evaluated by TiDB, they keep their
		// code and SQLSTATE.
		{"select 1 from (select c from t limit 1) d where 'x' regexp concat(c, '(')",
			[]string{"Error 1139 (42000): Got error 'error parsing regexp: missing closing ): `secret(`' from regexp"},
			[]string{"Error 1139 (42000): Got error '?' from regexp"}},
		// An error without an error code of its own is replaced as a whole.
		{"select 1 from t where cast(c as vector) is null",
			[]string{"Error 1105 (HY000): Invalid vector text: secret"},
			[]string{"Error 1105 (HY000): the error message is hidden because the statement reads a column masked by a masking policy"}},
		// Views, CTEs, derived tables, joins, grouping and sorting.
		{"select 1 from (select c x from t limit 1) d where cast(x as signed) > 0", []string{truncated("INTEGER", "secret")}, []string{truncated("INTEGER", "?")}},
		{"with w as (select c from t) select 1 from w where cast(c as signed) > 0", []string{truncated("INTEGER", "secret")}, []string{truncated("INTEGER", "?")}},
		{"select 1 from v where cast(c as signed) > 0", []string{truncated("INTEGER", "secret")}, []string{truncated("INTEGER", "?")}},
		{"select 1 from dst join t on dst.v = t.c", []string{truncated("DOUBLE", "secret")}, []string{truncated("DOUBLE", "?")}},
		{"select 1 from t group by cast(c as signed)", []string{truncated("INTEGER", "secret")}, []string{truncated("INTEGER", "?")}},
		{"select 1 from t order by cast(c as signed)", []string{truncated("INTEGER", "secret")}, []string{truncated("INTEGER", "?")}},
		// A subquery evaluated while the statement is planned.
		{"select 1 from dst where v = (select 1 from t where cast(c as signed) > 0 limit 1)", []string{truncated("INTEGER", "secret")}, []string{truncated("INTEGER", "?")}},
		{"select 1 from dst where v = (select 1 from t where bin_to_uuid(c) is null)",
			[]string{"Error 1411 (HY000): Incorrect string value: 'secret' for function bin_to_uuid"},
			[]string{"Error 1411 (HY000): Incorrect string value: '?' for function bin_to_uuid"}},
		// The select list evaluates the masked value, which is redacted with
		// the other values of the statement.
		{"select cast(c as signed) from t", []string{truncated("INTEGER", "secret")}, []string{truncated("INTEGER", "?")}},
		// A virtual generated column is evaluated from the masked column.
		{"select 1 from tg where g > 0", []string{truncated("INTEGER", "secret")}, []string{truncated("INTEGER", "?")}},
		// Point get and batch point get followed by a filter.
		{"select 1 from t where id = 1 and cast(c as signed) > 0", []string{truncated("INTEGER", "secret")}, []string{truncated("INTEGER", "?")}},
		{"select 1 from t where id in (1, 2) and cast(c as signed) > 0", []string{truncated("INTEGER", "secret")}, []string{truncated("INTEGER", "?")}},
		// DML. An UPDATE and INSERT ... ON DUPLICATE KEY UPDATE read whole rows.
		{"update t set n = cast(c as signed) where id = 1", []string{truncatedErr("INTEGER", "secret")}, []string{truncatedErr("INTEGER", "?")}},
		{"update t set n = c", []string{truncatedErr("DOUBLE", "secret")}, []string{truncatedErr("DOUBLE", "?")}},
		{"update dst, t set dst.v = t.c where dst.id = t.id", []string{truncatedErr("DOUBLE", "secret")}, []string{truncatedErr("DOUBLE", "?")}},
		{"delete from t where cast(c as signed) > 5", []string{truncatedErr("INTEGER", "secret")}, []string{truncatedErr("INTEGER", "?")}},
		{"insert into dst select id + 10, 1 from t where cast(c as signed) > 0", []string{truncatedErr("INTEGER", "secret")}, []string{truncatedErr("INTEGER", "?")}},
		{"insert into t(id, n) values (1, 1) on duplicate key update n = cast(c as signed)", []string{truncatedErr("INTEGER", "secret")}, []string{truncatedErr("INTEGER", "?")}},
		// Diagnostics without data values are kept.
		{"select 1 from t where c = (select 'a' union all select 'b')",
			[]string{"Error 1242 (21000): Subquery returns more than 1 row"},
			[]string{"Error 1242 (21000): Subquery returns more than 1 row"}},
		{"select 1 from t where n / 0 > 0 and c > ''", []string{"Warning 1365 (22012): Division by 0"}, []string{"Warning 1365 (22012): Division by 0"}},
		// Statements that read no masked column are unchanged for every session.
		{"select 1 from t where cast(b as signed) > 0", []string{truncated("INTEGER", "plain")}, []string{truncated("INTEGER", "plain")}},
		{"select cast(b as signed) from t where id = 1", []string{truncated("INTEGER", "plain")}, []string{truncated("INTEGER", "plain")}},
		{"select 1 from t t1 join t t2 on t1.id = t2.id where cast(t2.b as signed) > 0", []string{truncated("INTEGER", "plain")}, []string{truncated("INTEGER", "plain")}},
		{"select 1 from plain where cast(b as signed) > 0", []string{truncated("INTEGER", "plain")}, []string{truncated("INTEGER", "plain")}},
		{"select 1 from plain where 'x' regexp concat(b, '(')",
			[]string{"Error 1105 (HY000): Got error 'error parsing regexp: missing closing ): `plain(`' from regexp"},
			[]string{"Error 1105 (HY000): Got error 'error parsing regexp: missing closing ): `plain(`' from regexp"}},
		{"update plain set id = cast(b as signed)", []string{truncatedErr("INTEGER", "plain")}, []string{truncatedErr("INTEGER", "plain")}},
	}
	root.MustExec("create view v as select c from t")
	for _, tc := range cases {
		require.Equal(t, tc.masked, maskingDiagnostics(user, tc.sql), tc.sql)
		require.Equal(t, tc.original, maskingDiagnostics(root, tc.sql), tc.sql)
	}
	// A disabled policy masks nothing.
	root.MustExec("alter table t disable masking policy p")
	root.MustExec("alter table tg disable masking policy pg")
	for _, tc := range cases {
		require.Equal(t, tc.original, maskingDiagnostics(user, tc.sql), tc.sql)
	}
	root.MustExec("alter table t enable masking policy p")

	// The diagnostics are redacted before a later statement shows them, and
	// their number, level and code are kept.
	user.MustQuery("select 1 from t where cast(c as signed) > 0").Check(testkit.Rows())
	user.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: '?'"))
	user.MustQuery("select 1 from t where cast(c as signed) > 0").Check(testkit.Rows())
	user.MustQuery("select @@warning_count, @@error_count").Check(testkit.Rows("1 0"))
	user.MustGetErrMsg("update t set n = cast(c as signed)", "[types:1292]Truncated incorrect INTEGER value: '?'")
	user.MustQuery("show errors").Check(testkit.Rows("Error 1292 Truncated incorrect INTEGER value: '?'"))
	root.MustQuery("select 1 from t where cast(c as signed) > 0").Check(testkit.Rows())
	root.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect INTEGER value: 'secret'"))

	// The filter of the first statement is evaluated by the store and that of
	// the second one by TiDB.
	pushed := "select 1 from t where cast(c as signed) > 0"
	notPushed := "select 1 from (select c from t limit 1) d where cast(c as signed) > 0"
	for sql, task := range map[string]string{pushed: "cop[tikv]", notPushed: "root"} {
		found := false
		for _, row := range user.MustQuery("explain " + sql).Rows() {
			if strings.Contains(row[4].(string), "cast(") {
				require.Equal(t, task, row[2], sql)
				found = true
			}
		}
		require.True(t, found, sql)
		require.Equal(t, []string{truncated("INTEGER", "?")}, maskingDiagnostics(user, sql), sql)
	}
}

// TestMaskingPolicyDiagnosticsLockName tests the errors of the functions that
// report an invalid user-level lock name.
func TestMaskingPolicyDiagnosticsLockName(t *testing.T) {
	root, user := prepareMaskingDiagnostics(t)
	secret, plain := strings.Repeat("s3cret", 12), strings.Repeat("plain", 14)
	root.MustExec("update t set c = ?, b = ?", secret, plain)
	root.MustExec("update plain set b = ?", plain)
	wrongName := func(name string) []string {
		return []string{fmt.Sprintf("Error 3057 (HY000): Incorrect user-level lock name '%s'.", name)}
	}
	for _, fn := range []string{"is_free_lock(%s)", "is_used_lock(%s)", "release_lock(%s)"} {
		for _, sql := range []string{
			"select 1 from t where " + fn + " = 1",
			"select 1 from t order by " + fn,
			"select " + fn + " from t",
		} {
			masked := fmt.Sprintf(sql, "c")
			if strings.HasPrefix(sql, "select "+fn) {
				// A lock function has to be evaluated in its own place: the
				// result field cannot keep the policy of its argument.
				for _, tk := range []*testkit.TestKit{user, root} {
					require.Equal(t, []string{"Error 1105 (HY000): masking policy cannot be preserved through mutable expression"}, maskingDiagnostics(tk, masked), masked)
				}
			} else {
				require.Equal(t, wrongName("?"), maskingDiagnostics(user, masked), masked)
				require.Equal(t, wrongName(secret), maskingDiagnostics(root, masked), masked)
			}
			unprotected := fmt.Sprintf(sql, "b")
			require.Equal(t, wrongName(plain), maskingDiagnostics(user, unprotected), unprotected)
			unprotected = strings.Replace(unprotected, "from t", "from plain", 1)
			require.Equal(t, wrongName(plain), maskingDiagnostics(user, unprotected), unprotected)
		}
	}
	for _, sql := range []string{"select 1 from t where get_lock(c, 0) = 1", "select get_lock(c, 0) from t"} {
		diags := maskingDiagnostics(user, sql)
		require.Len(t, diags, 1, sql)
		require.NotContains(t, diags[0], "s3cret", sql)
	}
	root.MustExec("alter table t disable masking policy p")
	require.Equal(t, wrongName(secret), maskingDiagnostics(user, "select 1 from t where is_free_lock(c) = 1"))
}

// TestMaskingPolicyDiagnosticsPlanCache tests that a reused plan decides for
// each execution whether the diagnostics are redacted.
func TestMaskingPolicyDiagnosticsPlanCache(t *testing.T) {
	root, user := prepareMaskingDiagnostics(t)
	execute := func(tk *testkit.TestKit, sql, value string, fromCache bool) {
		require.Equal(t, []string{"Warning 1292 (22007): Truncated incorrect INTEGER value: '" + value + "'"}, maskingDiagnostics(tk, sql), sql)
		require.Equal(t, fromCache, tk.Session().GetSessionVars().FoundInPlanCache, sql)
	}
	for _, tk := range []*testkit.TestKit{root, user} {
		tk.MustExec("prepare scan from 'select 1 from t where cast(c as signed) > ?'")
		tk.MustExec("prepare point from 'select 1 from t where id = ? and cast(c as signed) > 0'")
		tk.MustExec("set @zero = 0, @one = 1")
	}
	for _, sql := range []string{"execute scan using @zero", "execute point using @one"} {
		execute(user, sql, "?", false)
		execute(user, sql, "?", true)
		execute(root, sql, "secret", false)
		execute(root, sql, "secret", true)

		// A policy that is enabled after the plan was cached applies to it.
		root.MustExec("alter table t disable masking policy p")
		execute(user, sql, "secret", false)
		execute(user, sql, "secret", true)
		root.MustExec("alter table t enable masking policy p")
		execute(user, sql, "?", false)
		execute(user, sql, "?", true)
	}

	// The fast plans of point queries.
	user.MustExec("prepare upd from 'update t set n = cast(c as signed) where id = ?'")
	for range 2 {
		require.Equal(t, []string{"Error 1292 (22007): Truncated incorrect INTEGER value: '?'"}, maskingDiagnostics(user, "execute upd using @one"))
	}
	require.Equal(t, []string{"Error 1292 (22007): Truncated incorrect INTEGER value: '?'"},
		maskingDiagnostics(user, "update t set n = cast(c as signed) where id in (1, 2)"))
	require.Equal(t, []string{"Error 1292 (22007): Truncated incorrect INTEGER value: 'secret'"},
		maskingDiagnostics(root, "update t set n = cast(c as signed) where id = 1"))
	require.Equal(t, []string{"Error 1292 (22007): Truncated incorrect INTEGER value: 'plain'"},
		maskingDiagnostics(user, "update plain set id = cast(b as signed) where id = 1"))
	user.MustQuery("select c, b from t where id = 1").Check(testkit.Rows("XXXXXX plain"))
	require.Empty(t, maskingDiagnostics(user, "select c from t where id = 1"))
}

// TestMaskingPolicyDiagnosticsUnclassified tests the diagnostics that a
// masked value reaches although nothing marks their arguments as data values:
// they are redacted when they are raised by evaluating the value, and the
// advice of the planner is kept.
func TestMaskingPolicyDiagnosticsUnclassified(t *testing.T) {
	root, user := prepareMaskingDiagnostics(t)
	restrictOn := " restrict on (insert_into_select, update_select, delete_select, ctas) enable"
	root.MustExec("create table ti(c int)")
	root.MustExec("insert into ti values (123456789)")
	root.MustExec("create masking policy pi on ti(c) as case when current_user() = 'root@%' then c else mask_null(c) end" + restrictOn)
	root.MustExec("create table ts(c varchar(20))")
	root.MustExec("insert into ts values ('secret')")
	root.MustExec("create masking policy ps on ts(c) as case when current_user() = 'root@%' then c else mask_full(c) end" + restrictOn)
	root.MustExec("create user u2")
	root.MustExec("grant select on test.* to u2")
	reader := testkit.NewTestKit(t, root.Session().GetStore())
	require.NoError(t, reader.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "%"}, nil, nil, nil))
	reader.MustExec("use test")
	reader.MustExec("set session tidb_enable_non_prepared_plan_cache = off")
	root.MustExec("set session tidb_enable_non_prepared_plan_cache = off")

	now := "select 1 from (select c from ti limit 1) d where now(d.c) > '2000-01-01'"
	for _, vectorized := range []string{"off", "on"} {
		for _, tk := range []*testkit.TestKit{reader, root} {
			tk.MustExec("set session tidb_enable_vectorized_expression = " + vectorized)
		}
		require.Equal(t, []string{"Error 1426 (42000): Too-big precision ? specified for 'now'. Maximum is 6."}, maskingDiagnostics(reader, now), vectorized)
		require.Equal(t, []string{"Error 1426 (42000): Too-big precision 123456789 specified for 'now'. Maximum is 6."}, maskingDiagnostics(root, now), vectorized)
	}

	hidden := "Warning 1105 (HY000): the error message is hidden because the statement reads a column masked by a masking policy"
	decodeKey := "select count(*) from ts where length(tidb_decode_key(hex(concat('!', c)))) > 0"
	require.Equal(t, []string{hidden}, maskingDiagnostics(reader, decodeKey))
	reader.MustQuery(decodeKey).Check(testkit.Rows("1"))
	reader.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 the error message is hidden because the statement reads a column masked by a masking policy"))
	require.Equal(t, []string{"Warning 1105 (HY000): invalid key: 21736563726574"}, maskingDiagnostics(root, decodeKey))
	// The same warning about an unprotected value is kept.
	require.Equal(t, []string{"Warning 1105 (HY000): invalid key: 21706C61696E"},
		maskingDiagnostics(user, "select count(*) from t where length(tidb_decode_key(hex(concat('!', b)))) > 0"))

	// The advice of the planner is kept.
	for _, sql := range []string{
		"select /*+ use_index(ts, nosuch) */ count(*) from ts where length(c) > 0",
		"select /*+ nosuch_hint() */ count(*) from ts where length(c) > 0",
	} {
		require.Equal(t, maskingDiagnostics(root, sql), maskingDiagnostics(reader, sql), sql)
		require.NotEmpty(t, maskingDiagnostics(reader, sql), sql)
	}
}

// TestMaskingPolicyDiagnosticsCommit tests that the diagnostics of a
// transaction that read a masked column stay redacted until it ends, as a
// later statement such as COMMIT may report an error about the rows it read.
func TestMaskingPolicyDiagnosticsCommit(t *testing.T) {
	root, user := prepareMaskingDiagnostics(t)
	root.MustExec("create table tu(id int primary key, c varchar(20), k int, unique key uk(c, k))")
	root.MustExec("insert into tu values (1, 'secret', 0), (2, 'secret', 1)")
	root.MustExec("create masking policy pu on tu(c) as case when current_user() = 'root@%' then c else mask_full(c) end" +
		" restrict on (insert_into_select, update_select, delete_select, ctas) enable")
	root.MustExec("grant create on test.* to u1")
	for _, tk := range []*testkit.TestKit{user, root} {
		tk.MustExec("set session tidb_constraint_check_in_place_pessimistic = off")
	}
	run := func(tk *testkit.TestKit, end string) []string {
		require.Empty(t, maskingDiagnostics(tk, "begin pessimistic"))
		// Only the unprotected component of the unique key is assigned, and
		// the duplicate is found when the transaction commits.
		require.Empty(t, maskingDiagnostics(tk, "update tu set k = 1 where id + 0 = 1"))
		// The other statements of the transaction are redacted for the session.
		value := map[*testkit.TestKit]string{user: "?", root: "plain"}[tk]
		require.Equal(t, []string{truncatedWarning(value)}, maskingDiagnostics(tk, "select 1 from plain where cast(b as signed) > 0"))
		return maskingDiagnostics(tk, end)
	}
	for _, end := range []string{"commit", "begin", "create table tu2(a int)"} {
		root.MustExec("set session tidb_txn_assertion_level = off")
		user.MustExec("set session tidb_txn_assertion_level = off")
		require.Equal(t, []string{"Error 1062 (23000): Duplicate entry '?' for key 'tu.uk'"}, run(user, end), end)
		require.Equal(t, []string{"Error 1062 (23000): Duplicate entry 'secret-1' for key 'tu.uk'"}, run(root, end), end)
		user.MustExec("rollback")
		root.MustExec("rollback")
	}
	// The assertion on the new index key fails first, and it reports the key.
	user.MustExec("set session tidb_txn_assertion_level = default")
	diags := run(user, "commit")
	require.Len(t, diags, 1)
	require.Contains(t, diags[0], "Error 8141 (HY000): assertion failed: key: ?,")
	require.NotContains(t, diags[0], "736563726574")

	// The next transaction starts without redaction.
	require.Equal(t, []string{truncatedWarning("plain")}, maskingDiagnostics(user, "select 1 from plain where cast(b as signed) > 0"))
	user.MustQuery("select k from tu where id = 1").Check(testkit.Rows("0"))
}

func truncatedWarning(value string) string {
	return fmt.Sprintf("Warning 1292 (22007): Truncated incorrect INTEGER value: '%s'", value)
}

// TestMaskingPolicyDiagnosticsCompile tests that an error raised while the
// statement is compiled is redacted once a masked value has been evaluated,
// such as the result of a scalar subquery.
func TestMaskingPolicyDiagnosticsCompile(t *testing.T) {
	root, _ := prepareMaskingDiagnostics(t)
	root.MustExec("create table ts(c varchar(20))")
	root.MustExec("insert into ts values ('secret')")
	root.MustExec("create masking policy ps on ts(c) as case when current_user() = 'root@%' then c else mask_full(c) end" +
		" restrict on (insert_into_select, update_select, delete_select, ctas) enable")
	root.MustExec("create user u2")
	root.MustExec("grant select on test.* to u2")
	reader := testkit.NewTestKit(t, root.Session().GetStore())
	require.NoError(t, reader.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "%"}, nil, nil, nil))
	reader.MustExec("use test")
	for _, tk := range []*testkit.TestKit{reader, root} {
		tk.MustExec("set session tidb_enable_non_prepared_plan_cache = off")
	}
	for _, fn := range []string{"min", "max"} {
		sql := fmt.Sprintf("select 1 where 'x' in (uuid_to_bin((select %s(c) from ts)), 'y')", fn)
		require.Equal(t, []string{"Error 1411 (HY000): Incorrect string value: '?' for function uuid_to_bin"}, maskingDiagnostics(reader, sql), sql)
		require.Equal(t, []string{"Error 1411 (HY000): Incorrect string value: 'secret' for function uuid_to_bin"}, maskingDiagnostics(root, sql), sql)
	}
	// An error raised before a masked value is read is kept.
	require.Equal(t, []string{"Error 1054 (42S22): Unknown column 'nosuch' in 'where clause'"},
		maskingDiagnostics(reader, "select 1 from ts where nosuch = (select min(c) from ts)"))
}
