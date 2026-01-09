// Copyright 2022 PingCAP, Inc.
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

package core

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestParameterize(t *testing.T) {
	cases := []struct {
		sql      string
		paramSQL string
		params   []any
	}{
		{
			"select * from t where a<10",
			"SELECT * FROM `t` WHERE `a`<?",
			[]any{int64(10)},
		},
		{
			"select * from t",
			"SELECT * FROM `t`",
			[]any{},
		},
		{
			"select * from t where a<10 and b<20 and c=30 and d>40",
			"SELECT * FROM `t` WHERE `a`<? AND `b`<? AND `c`=? AND `d`>?",
			[]any{int64(10), int64(20), int64(30), int64(40)},
		},
		{
			"select * from t where a='a' and b='bbbbbbbbbbbbbbbbbbbbbbbb'",
			"SELECT * FROM `t` WHERE `a`=? AND `b`=?",
			[]any{"a", "bbbbbbbbbbbbbbbbbbbbbbbb"},
		},
		{
			"select 1, 2, 3 from t where a<10",
			"SELECT 1,2,3 FROM `t` WHERE `a`<?",
			[]any{int64(10)},
		},
		{
			"select a+1 from t where a<10",
			"SELECT a+1 FROM `t` WHERE `a`<?",
			[]any{int64(10)},
		},
		{
			`select a+ "a b c" from t`,
			"SELECT a+ \"a b c\" FROM `t`",
			[]any{},
		},
		{
			`select a + 'a b c'+"x" from t`,
			"SELECT a + 'a b c'+\"x\" FROM `t`", // keep the original format for select fields
			[]any{},
		},
		{
			`select a + 'a b c'+"x" as 'xxx' from t`,
			"SELECT a + 'a b c'+\"x\" as 'xxx' FROM `t`", // keep the original format for select fields
			[]any{},
		},
		{
			`insert into t (a, B, c) values (1, 2, 3), (4, 5, 6)`,
			"INSERT INTO `t` (`a`,`B`,`c`) VALUES (?,?,?),(?,?,?)",
			[]any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)},
		},
		{
			`select * from t where a < date_format('2020-02-02', '%Y-%m-%d')`,
			"SELECT * FROM `t` WHERE `a`<date_format(?, '%Y-%m-%d')",
			[]any{"2020-02-02"},
		},
		{
			"select * from `txu#p#p1`",
			"SELECT * FROM `txu#p#p1`",
			[]any{},
		},

		// keep the original format for limit clauses
		{
			`select * from t limit 10`,
			"SELECT * FROM `t` LIMIT 10",
			[]any{},
		},
		{
			`select * from t limit 10, 20`,
			"SELECT * FROM `t` LIMIT 10,20",
			[]any{},
		},
		// TODO: more test cases
	}

	for _, c := range cases {
		stmt, err := parser.New().ParseOneStmt(c.sql, "", "")
		require.Nil(t, err)
		paramSQL, params, err := ParameterizeAST(stmt)
		require.Nil(t, err)
		require.Equal(t, c.paramSQL, paramSQL)
		require.Equal(t, len(c.params), len(params))
		for i := range params {
			require.Equal(t, c.params[i], params[i].Datum.GetValue())
		}
	}
}

func TestGetParamSQLFromASTWithoutMutationConcurrently(t *testing.T) {
	n := 50
	sqls := make([]string, 0, n)
	for i := range n {
		sqls = append(sqls, fmt.Sprintf(`insert into t values (%d, %d, %d)`, i*3+0, i*3+1, i*3+2))
	}
	stmts := make([]ast.StmtNode, 0, n)
	for _, sql := range sqls {
		stmt, err := parser.New().ParseOneStmt(sql, "", "")
		require.Nil(t, err)
		stmts = append(stmts, stmt)
	}

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(id int) {
			for range 100 {
				_, vals, err := GetParamSQLFromASTWithoutMutation(stmts[id])
				require.Nil(t, err)
				require.Equal(t, len(vals), 3)
				require.Equal(t, vals[0].GetValue(), int64(id*3+0))
				require.Equal(t, vals[1].GetValue(), int64(id*3+1))
				require.Equal(t, vals[2].GetValue(), int64(id*3+2))
				time.Sleep(time.Millisecond + time.Duration(rand.Intn(int(time.Millisecond))))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func BenchmarkParameterizeSelect(b *testing.B) {
	paymentSelectCustomerForUpdate := `SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,
c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ?AND c_id = ? FOR UPDATE`
	stmt, err := parser.New().ParseOneStmt(paymentSelectCustomerForUpdate, "", "")
	require.Nil(b, err)
	_, _, err = ParameterizeAST(stmt)
	require.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParameterizeAST(stmt)
	}
}

func BenchmarkParameterizeInsert(b *testing.B) {
	paymentInsertHistory := `INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (1, 2, 3, 4, 5, 6, 7, 8)`
	stmt, err := parser.New().ParseOneStmt(paymentInsertHistory, "", "")
	require.Nil(b, err)
	_, _, err = ParameterizeAST(stmt)
	require.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParameterizeAST(stmt)
	}
}

func BenchmarkGetParamSQL(b *testing.B) {
	paymentInsertHistory := `INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (1, 2, 3, 4, 5, 6, 7, 8)`
	stmt, err := parser.New().ParseOneStmt(paymentInsertHistory, "", "")
	require.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetParamSQLFromASTWithoutMutation(stmt)
	}
}

// TestGetParamSQLFromASTWithoutMutation tests that the optimized function
// produces the same results as the original function.
func TestGetParamSQLFromASTWithoutMutation(t *testing.T) {
	cases := []struct {
		sql      string
		paramSQL string
		params   []any
	}{
		{
			"select * from t where a<10",
			"SELECT * FROM `t` WHERE `a`<?",
			[]any{int64(10)},
		},
		{
			"select * from t",
			"SELECT * FROM `t`",
			[]any{},
		},
		{
			"select * from t where a<10 and b<20 and c=30 and d>40",
			"SELECT * FROM `t` WHERE `a`<? AND `b`<? AND `c`=? AND `d`>?",
			[]any{int64(10), int64(20), int64(30), int64(40)},
		},
		{
			"select * from t where a='a' and b='bbbbbbbbbbbbbbbbbbbbbbbb'",
			"SELECT * FROM `t` WHERE `a`=? AND `b`=?",
			[]any{"a", "bbbbbbbbbbbbbbbbbbbbbbbb"},
		},
		{
			"select 1, 2, 3 from t where a<10",
			"SELECT 1,2,3 FROM `t` WHERE `a`<?",
			[]any{int64(10)},
		},
		{
			"select a+1 from t where a<10",
			"SELECT a+1 FROM `t` WHERE `a`<?",
			[]any{int64(10)},
		},
		{
			`insert into t (a, B, c) values (1, 2, 3), (4, 5, 6)`,
			"INSERT INTO `t` (`a`,`B`,`c`) VALUES (?,?,?),(?,?,?)",
			[]any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)},
		},
		{
			`select * from t where a < date_format('2020-02-02', '%Y-%m-%d')`,
			"SELECT * FROM `t` WHERE `a`<date_format(?, '%Y-%m-%d')",
			[]any{"2020-02-02"},
		},
		// keep the original format for limit clauses
		{
			`select * from t limit 10`,
			"SELECT * FROM `t` LIMIT 10",
			[]any{},
		},
		{
			`select * from t limit 10, 20`,
			"SELECT * FROM `t` LIMIT 10,20",
			[]any{},
		},
		// group by and order by should not be parameterized
		{
			`select * from t group by 1 order by 2`,
			"SELECT * FROM `t` GROUP BY 1 ORDER BY 2",
			[]any{},
		},
	}

	for _, c := range cases {
		stmt, err := parser.New().ParseOneStmt(c.sql, "", "")
		require.Nil(t, err)
		paramSQL, params, err := GetParamSQLFromASTWithoutMutation(stmt)
		require.Nil(t, err)
		require.Equal(t, c.paramSQL, paramSQL, "sql: %s", c.sql)
		require.Equal(t, len(c.params), len(params), "sql: %s", c.sql)
		for i := range params {
			require.Equal(t, c.params[i], params[i].GetValue(), "sql: %s, param %d", c.sql, i)
		}
	}
}

// TestGetParamSQLWithoutMutationConsistency verifies that the optimized function
// produces results consistent with the original function.
func TestGetParamSQLWithoutMutationConsistency(t *testing.T) {
	sqls := []string{
		"select * from t where a < 10 and b = 'hello'",
		"insert into t values (1, 2, 3)",
		"update t set a = 10 where b = 20",
		"delete from t where a > 5",
		"select * from t where a < date_format('2020-02-02', '%Y-%m-%d')",
	}

	for _, sql := range sqls {
		stmt, err := parser.New().ParseOneStmt(sql, "", "")
		require.Nil(t, err)

		// Get results from optimized function
		paramSQL1, params1, err := GetParamSQLFromASTWithoutMutation(stmt)
		require.Nil(t, err)

		// Get results from original function
		paramSQL2, params2, err := GetParamSQLFromAST(stmt)
		require.Nil(t, err)

		// Compare
		require.Equal(t, paramSQL2, paramSQL1, "sql: %s", sql)
		require.Equal(t, len(params2), len(params1), "sql: %s", sql)
		for i := range params1 {
			require.Equal(t, params2[i].GetValue(), params1[i].GetValue(), "sql: %s, param %d", sql, i)
		}
	}
}

// BenchmarkGetParamSQLComparison provides side-by-side benchmark comparison.
func BenchmarkGetParamSQLComparison(b *testing.B) {
	// Short SELECT query
	shortSelect := `SELECT * FROM t WHERE a = 1 AND b = 2`
	shortStmt, err := parser.New().ParseOneStmt(shortSelect, "", "")
	require.Nil(b, err)

	// Long SELECT query with multiple conditions and columns
	longSelect := `SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = 10 AND c_d_id = 20 AND c_id = 30 AND c_balance > 100 AND c_discount < 0.5 AND c_credit_lim >= 5000`
	longStmt, err := parser.New().ParseOneStmt(longSelect, "", "")
	require.Nil(b, err)

	b.Run("Short-Original", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			GetParamSQLFromAST(shortStmt)
		}
	})

	b.Run("Short-Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			GetParamSQLFromASTWithoutMutation(shortStmt)
		}
	})

	b.Run("Long-Original", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			GetParamSQLFromAST(longStmt)
		}
	})

	b.Run("Long-Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			GetParamSQLFromASTWithoutMutation(longStmt)
		}
	})
}
