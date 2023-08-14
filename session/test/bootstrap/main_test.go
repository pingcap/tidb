package bootstrap

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func match(t *testing.T, row []types.Datum, expected ...interface{}) {
	require.Len(t, row, len(expected))
	for i := range row {
		if _, ok := expected[i].(time.Time); ok {
			// Since password_last_changed is set to default current_timestamp, we pass this check.
			continue
		}
		got := fmt.Sprintf("%v", row[i].GetValue())
		need := fmt.Sprintf("%v", expected[i])
		require.Equal(t, need, got, i)
	}
}
