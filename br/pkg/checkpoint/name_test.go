package checkpoint

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableIdentifier(t *testing.T) {
	identifier := TableIdentifier{
		StartTS:           1704067200000, // 2024-01-01 00:00:00
		EndTS:             1704153600000, // 2024-01-02 00:00:00
		Filter:            []string{"db1.table1", "db2.*", "*.table3", "db4.table4"},
		UpstreamClusterID: 987654321,
		TaskType:          "incremental-restore",
	}

	suffix := identifier.GenerateTableNameSuffix()

	// length check, "_" + 16 hex chars
	require.Equal(t, 17, len(suffix))

	// first item check
	require.Equal(t, byte('_'), suffix[0])

	// generate again to verify deterministic behavior
	secondSuffix := identifier.GenerateTableNameSuffix()
	require.Equal(t, secondSuffix, suffix)

	// verify that changing any field produces a different suffix
	modifiedIdentifier := identifier
	modifiedIdentifier.Filter = []string{}
	differentSuffix := modifiedIdentifier.GenerateTableNameSuffix()
	require.NotEqual(t, suffix, differentSuffix)
}
