package core_test

import (
	"testing"

	"github.com/pingcap/tidb/planner/core"
	"github.com/stretchr/testify/require"
)

func TestFastLexer(t *testing.T) {
	// Just support some simple pattern for demo.
	testCaseNum := 5

	querys := []string{
		"select * from t where a > 1 and b = 'abc';",
		"select /*+ hint or comment */ a from t1 where expr(a1) = '123' or b1 < 123;",
		"select 123;",
		"select 12",
		"select 'a.....",
	}

	resSQLText := []string{
		"select * from t where a > ? and b = ?;",
		"select /*+ hint or comment */ a from t1 where expr(a1) = ? or b1 < ?;",
		"select ?;",
		"",
		"",
	}

	resConstParams := [][]string{
		{"1", "'abc'"},
		{"'123'", "123"},
		{"123"},
		nil,
		nil,
	}
	canSucc := []bool{true, true, true, false, false}

	for i := 0; i < testCaseNum; i++ {
		sqlText, constParams, ok := core.FastLexer(querys[i])
		require.Equal(t, canSucc[i], ok)
		require.Equal(t, resSQLText[i], sqlText)
		require.Equal(t, len(resConstParams[i]), len(constParams))
		for j := range constParams {
			require.Equal(t, resConstParams[i][j], constParams[j])
		}
	}
}
