package aggregation

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestDistinct(t *testing.T) {
	t.Parallel()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	dc := createDistinctChecker(sc)
	testCases := []struct {
		vals   []interface{}
		expect bool
	}{
		{[]interface{}{1, 1}, true},
		{[]interface{}{1, 1}, false},
		{[]interface{}{1, 2}, true},
		{[]interface{}{1, 2}, false},
		{[]interface{}{1, nil}, true},
		{[]interface{}{1, nil}, false},
	}
	for _, tc := range testCases {
		d, err := dc.Check(types.MakeDatums(tc.vals...))
		require.NoError(t, err)
		require.Equal(t, tc.expect, d)
	}
}
