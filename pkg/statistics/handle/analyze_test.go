package handle

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestAutoAnalyzeWithInvalidHealthButSignificantModifications(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	// Set lower auto-analyze threshold for testing
	originVal := tk.MustQuery("select @@tidb_auto_analyze_ratio").Rows()[0][0].(string)
	tk.MustExec("set global tidb_auto_analyze_ratio=0.3")
	defer func() {
		tk.MustExec("set global tidb_auto_analyze_ratio=" + originVal)
	}()

	// Create table
	tk.MustExec("create table t(a int, b int)")

	// Insert data directly without querying
	for i := 0; i < 2000; i++ {
		tk.MustExec("insert into t values(?, ?)", i, i)
	}

	// Get table stats
	h := dom.StatsHandle()
	require.NotNil(t, h)

	// Force stats collection
	is := dom.InfoSchema()
	h.Update(is)

	// Wait for auto-analyze to trigger
	time.Sleep(3 * time.Second)

	// Verify stats were collected despite invalid health
	tbl, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()

	statsTbl := h.GetTableStats(tableInfo)
	require.NotNil(t, statsTbl)
	require.Greater(t, statsTbl.Count, int64(0),
		"Stats should be collected even with invalid health when modifications are significant")
}
