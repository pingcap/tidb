package cardinality

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestExplainFormatBriefTruncated(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)

	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, index idx(a))")

	// Insert some data to generate statistics
	for i := 0; i < 100; i++ {
		testKit.MustExec("insert into t values (?, ?)", i, i*2)
	}
	testKit.MustExec("analyze table t")

	// Test the new brief_truncated format
	rows := testKit.MustQuery("explain format='brief_truncated' select * from t where a = 5").Rows()

	// Verify that the format is recognized and produces output
	require.Greater(t, len(rows), 0)

	// Check that the format is working (the exact output may vary based on statistics)
	// but we should have at least the basic structure
	require.Greater(t, len(rows[0]), 4) // Should have at least id, estRows, task, access object, operator info

	// Verify that estRows (index 1) doesn't contain decimal points when using brief_truncated format
	if len(rows) > 0 && len(rows[0]) > 1 {
		estRows := rows[0][1].(string)
		require.NotContains(t, estRows, ".", "Estimated rows should not contain decimal points in brief_truncated format")
	}
}

func TestExplainFormatBriefTruncatedConstant(t *testing.T) {
	// Test that the new format constant is properly defined
	require.Equal(t, "brief_truncated", types.ExplainFormatBriefTruncated)

	// Test that it's included in the valid formats list
	formats := types.ExplainFormats
	found := false
	for _, format := range formats {
		if format == types.ExplainFormatBriefTruncated {
			found = true
			break
		}
	}
	require.True(t, found, "ExplainFormatBriefTruncated should be included in ExplainFormats")
}
