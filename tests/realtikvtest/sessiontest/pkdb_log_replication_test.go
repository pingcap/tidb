package sessiontest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
)

func TestLogReplicationInfoSchemaDefaultStatus(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("log replication status tables require real TiKV PD")
	}

	// TODO(lance6716): fix CI
	t.Skip("CI is not ready for log replication")

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	// check ROLE is STANDARD
	tk.MustQuery(`
		select
			ROLE,
			HAS_REPLICA,
			REPLICATION_NAME,
			SOURCE_CLUSTER_ID,
			SOURCE_PD_ADDRS,
			PROTECTION_MODE,
			DEGRADE_TIMEOUT,
			REPLICATION_STATE,
			REPLICATION_MODE,
			CHECKPOINT_TS,
			CHECKPOINT_TIME,
			CHECKPOINT_LAG,
			SWITCHOVER_READY,
			FAILOVER_READY,
			INITIALIZING_PROGRESS
		from information_schema.LR_STATUS_LOCAL`).Check(testkit.Rows(
		"STANDARD 0 <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil>",
	))
}
