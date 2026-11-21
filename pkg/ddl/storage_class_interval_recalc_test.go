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

package ddl_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

const storageClassIntervalTable = `CREATE TABLE t (id INT, dt DATE)
ENGINE_ATTRIBUTE = '{"storage_class": {"tier": "IA", "less_than": "INTERVAL 3 YEAR"}}'
PARTITION BY RANGE COLUMNS (dt) (
	PARTITION p202310 VALUES LESS THAN ('2023-10-01'),
	PARTITION p202311 VALUES LESS THAN ('2023-11-01'),
	PARTITION p202312 VALUES LESS THAN ('2023-12-01'),
	PARTITION pmax VALUES LESS THAN (MAXVALUE))`

func storageClassPartitionTiers(t *testing.T, tk *testkit.TestKit, table string) map[string]string {
	tiers := make(map[string]string)
	for _, def := range external.GetTableByName(t, tk, "test", table).Meta().Partition.Definitions {
		tiers[def.Name.L] = def.StorageClassTier
	}
	return tiers
}

func mockStorageClassIntervalNow(t *testing.T) *atomic.Pointer[time.Time] {
	var now atomic.Pointer[time.Time]
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockStorageClassIntervalNow", func(current *time.Time) {
		if mocked := now.Load(); mocked != nil {
			*current = *mocked
		}
	})
	return &now
}

func setMockTime(now *atomic.Pointer[time.Time], s string) {
	tm, err := time.Parse(time.DateTime, s)
	if err != nil {
		panic(err)
	}
	now.Store(&tm)
}

func TestStorageClassIntervalSelector(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableStorageClass = true
	})
	now := mockStorageClassIntervalNow(t)
	setMockTime(now, "2026-10-31 18:00:00")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")

	tk.MustExec(storageClassIntervalTable)
	require.Equal(t, map[string]string{"p202310": "IA", "p202311": "STANDARD", "p202312": "STANDARD", "pmax": "STANDARD"},
		storageClassPartitionTiers(t, tk, "t"))

	// ADD PARTITION evaluates new partitions at the current time.
	setMockTime(now, "2026-12-01 00:00:00")
	tk.MustExec("ALTER TABLE t REORGANIZE PARTITION pmax INTO (PARTITION p202401 VALUES LESS THAN ('2024-01-01'), PARTITION pmax VALUES LESS THAN (MAXVALUE))")
	tiers := storageClassPartitionTiers(t, tk, "t")
	require.Equal(t, "STANDARD", tiers["p202401"])
	require.Equal(t, "STANDARD", tiers["p202311"], "existing partitions keep their tier until recalculated")

	// ALTER evaluates the whole table at the current time.
	tk.MustExec(`ALTER TABLE t ENGINE_ATTRIBUTE = '{"storage_class": {"tier": "IA", "less_than": "INTERVAL 3 YEAR"}}'`)
	require.Equal(t, map[string]string{"p202310": "IA", "p202311": "IA", "p202312": "IA", "p202401": "STANDARD", "pmax": "STANDARD"},
		storageClassPartitionTiers(t, tk, "t"))

	tk.MustExec(`CREATE TABLE days (dt DATE) PARTITION BY RANGE (TO_DAYS(dt)) (
	PARTITION p0 VALUES LESS THAN (TO_DAYS('2023-11-01')), PARTITION p1 VALUES LESS THAN (TO_DAYS('2023-12-02')))`)
	tk.MustExec(`ALTER TABLE days ENGINE_ATTRIBUTE = '{"storage_class": {"tier": "IA", "less_than": "INTERVAL 3 YEAR"}}'`)
	require.Equal(t, map[string]string{"p0": "IA", "p1": "STANDARD"}, storageClassPartitionTiers(t, tk, "days"))

	// UNIX_TIMESTAMP boundaries are fixed in the session time zone at CREATE time
	// and compared with the cutoff as absolute seconds.
	tk.MustExec("SET time_zone = '+00:00'")
	tk.MustExec(`CREATE TABLE seconds (ts TIMESTAMP) PARTITION BY RANGE (UNIX_TIMESTAMP(ts)) (
	PARTITION p0 VALUES LESS THAN (UNIX_TIMESTAMP('2023-12-01 00:00:00')), PARTITION p1 VALUES LESS THAN (UNIX_TIMESTAMP('2023-12-01 00:00:01')))`)
	tk.MustExec(`ALTER TABLE seconds ENGINE_ATTRIBUTE = '{"storage_class": {"tier": "IA", "less_than": "INTERVAL 36 MONTH"}}'`)
	require.Equal(t, map[string]string{"p0": "IA", "p1": "STANDARD"}, storageClassPartitionTiers(t, tk, "seconds"))

	tk.MustExec("CREATE TABLE ints (id INT) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))")
	tk.MustExec("CREATE TABLE strs (s VARCHAR(10)) PARTITION BY RANGE COLUMNS (s) (PARTITION p0 VALUES LESS THAN ('2023-01-01'))")
	tk.MustExec("CREATE TABLE plain (dt DATE)")
	invalid := []struct {
		table, storageClass, msg string
	}{
		{"t", `{"tier": "STANDARD", "less_than": "INTERVAL 3 YEAR"}`, "only supports tier 'IA'"},
		{"t", `{"tier": "IA", "less_than": "INTERVAL 3 YEAR", "transitions": [{"tier": "IA", "after_days": 1}]}`, "transition"},
		{"t", `{"tier": "IA", "less_than": "INTERVAL 3 HOUR"}`, "invalid 'less_than' interval"},
		{"t", `{"tier": "IA", "less_than": "INTERVAL 0 DAY"}`, "invalid 'less_than' interval"},
		{"t", `{"tier": "IA", "less_than": "INTERVAL 1 DAY + INTERVAL 1 DAY"}`, "invalid 'less_than' interval"},
		{"t", `{"tier": "IA", "less_than": "INTERVAL @n DAY"}`, "invalid 'less_than' interval"},
		{"ints", `{"tier": "IA", "less_than": "INTERVAL 3 YEAR"}`, "only supports RANGE COLUMNS"},
		{"strs", `{"tier": "IA", "less_than": "INTERVAL 3 YEAR"}`, "only supports RANGE COLUMNS"},
		{"plain", `{"tier": "IA", "less_than": "INTERVAL 3 YEAR"}`, "only supports RANGE COLUMNS"},
		{"days", `{"tier": "IA", "less_than": "2023-02-01"}`, "is a date"},
	}
	tiersBefore := storageClassPartitionTiers(t, tk, "t")
	for _, c := range invalid {
		err := tk.ExecToErr("ALTER TABLE " + c.table + ` ENGINE_ATTRIBUTE = '{"storage_class": ` + c.storageClass + `}'`)
		require.Error(t, err, c.storageClass)
		require.ErrorContains(t, err, c.msg, c.storageClass)
	}
	require.Equal(t, tiersBefore, storageClassPartitionTiers(t, tk, "t"))
	err := tk.ExecToErr(`CREATE TABLE bad (dt DATE) ENGINE_ATTRIBUTE = '{"storage_class": {"tier": "IA", "less_than": "INTERVAL 3 YEAR"}}'`)
	require.ErrorContains(t, err, "only supports RANGE COLUMNS")
	tk.MustExec(`ALTER TABLE days ENGINE_ATTRIBUTE = '{"storage_class": {"tier": "IA", "less_than": "739215"}}'`)

	// CREATE TABLE ... LIKE copies the interval rule.
	tk.MustExec("CREATE TABLE t_like LIKE t")
	require.Equal(t, tiersBefore["p202310"], storageClassPartitionTiers(t, tk, "t_like")["p202310"])
}

func TestStorageClassIntervalRecalcPeriodVariable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("SELECT @@global.tidb_storage_class_interval_recalc_period").Check(testkit.Rows("24h0m0s"))
	tk.MustExec("SET GLOBAL tidb_storage_class_interval_recalc_period = '0s'")
	tk.MustQuery("SELECT @@global.tidb_storage_class_interval_recalc_period").Check(testkit.Rows("0s"))
	tk.MustExec("SET GLOBAL tidb_storage_class_interval_recalc_period = '1m'")
	tk.MustQuery("SHOW WARNINGS").CheckContain("Truncated incorrect tidb_storage_class_interval_recalc_period")
	tk.MustQuery("SELECT @@global.tidb_storage_class_interval_recalc_period").Check(testkit.Rows("10m0s"))
	tk.MustExec("SET GLOBAL tidb_storage_class_interval_recalc_period = '400h'")
	tk.MustQuery("SELECT @@global.tidb_storage_class_interval_recalc_period").Check(testkit.Rows("400h0m0s"))
	tk.MustExec("SET GLOBAL tidb_storage_class_interval_recalc_period = '9000h'")
	tk.MustQuery("SELECT @@global.tidb_storage_class_interval_recalc_period").Check(testkit.Rows("8760h0m0s"))
	tk.MustGetErrCode("SET SESSION tidb_storage_class_interval_recalc_period = '1h'", 1229)
	tk.MustGetErrCode("SET GLOBAL tidb_storage_class_interval_recalc_period = 'abc'", 1232)
	tk.MustGetErrCode("SET GLOBAL tidb_storage_class_interval_recalc_period = '-1h'", 1231)
	tk.MustQuery("SELECT @@global.tidb_storage_class_interval_recalc_period").Check(testkit.Rows("8760h0m0s"))
}

type storageClassIntervalRecalcLoop struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func startStorageClassIntervalRecalcLoop(d ddl.DDL) *storageClassIntervalRecalcLoop {
	ctx, cancel := context.WithCancel(context.Background())
	loop := &storageClassIntervalRecalcLoop{cancel: cancel}
	loop.wg.Add(1)
	go func() {
		defer loop.wg.Done()
		ddl.RunStorageClassIntervalRecalcLoopForTest(ctx, d)
	}()
	return loop
}

// stop simulates the DDL owner retiring: the loop must return promptly.
func (l *storageClassIntervalRecalcLoop) stop() {
	l.cancel()
	l.wg.Wait()
}

func TestStorageClassIntervalRecalc(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("the DDL owner already runs the recalculation loop in the next-gen kernel")
	}
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableStorageClass = true
	})
	now := mockStorageClassIntervalNow(t)
	setMockTime(now, "2026-10-31 18:00:00")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec(storageClassIntervalTable)

	var ticks atomic.Int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockStorageClassIntervalRecalcCheckInterval", func(interval *time.Duration) {
		ticks.Add(1)
		*interval = 10 * time.Millisecond
	})
	var period atomic.Int64
	period.Store(int64(time.Millisecond))
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockStorageClassIntervalRecalcPeriod", func(p *time.Duration) {
		if overridden := time.Duration(period.Load()); overridden >= 0 {
			*p = overridden
		}
	})
	rounds := make(chan struct{}, 1000)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterStorageClassIntervalRecalcRound", func() {
		rounds <- struct{}{}
	})
	var submitted atomic.Int64
	var beforeSubmit atomic.Pointer[func()]
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeStorageClassIntervalRecalcSubmit", func(int64) {
		submitted.Add(1)
		if fn := beforeSubmit.Load(); fn != nil {
			(*fn)()
		}
	})
	waitRounds := func(n int) {
		for range n {
			select {
			case <-rounds:
			case <-time.After(10 * time.Second):
				require.FailNow(t, "storage class interval recalculation round did not run")
			}
		}
	}
	waitJobsDone := func() {
		require.Eventually(t, func() bool {
			return tk.MustQuery("SELECT COUNT(*) FROM mysql.tidb_ddl_job").Rows()[0][0] == "0"
		}, 10*time.Second, 10*time.Millisecond)
	}
	drainRounds := func() {
		for {
			select {
			case <-rounds:
			default:
				return
			}
		}
	}

	// Nothing crossed the cutoff, so nothing is submitted.
	loop := startStorageClassIntervalRecalcLoop(dom.DDL())
	waitRounds(2)
	require.Zero(t, submitted.Load())
	require.Equal(t, "STANDARD", storageClassPartitionTiers(t, tk, "t")["p202311"])

	// p202311 crosses the cutoff at 2026-11-01 UTC.
	setMockTime(now, "2026-11-01 00:00:00")
	require.Eventually(t, func() bool {
		return storageClassPartitionTiers(t, tk, "t")["p202311"] == "IA"
	}, 10*time.Second, 10*time.Millisecond)
	waitJobsDone()
	require.Equal(t, map[string]string{"p202310": "IA", "p202311": "IA", "p202312": "STANDARD", "pmax": "STANDARD"},
		storageClassPartitionTiers(t, tk, "t"))

	// The clock moves backwards: partitions would leave IA, so nothing is submitted.
	submitted.Store(0)
	setMockTime(now, "2026-01-01 00:00:00")
	drainRounds()
	waitRounds(3)
	require.Zero(t, submitted.Load())
	require.Equal(t, "IA", storageClassPartitionTiers(t, tk, "t")["p202311"])

	// The user changes the rule after the round read it: the queued job is a no-op.
	setMockTime(now, "2026-12-01 00:00:00")
	newRule := `{"storage_class": {"tier": "IA", "names_in": ["p202310"]}}`
	var userAlterErr atomic.Pointer[error]
	hook := func() {
		beforeSubmit.Store(nil)
		tk2 := testkit.NewTestKit(t, store)
		err := tk2.ExecToErr("ALTER TABLE test.t ENGINE_ATTRIBUTE = '" + newRule + "'")
		userAlterErr.Store(&err)
	}
	period.Store(int64(time.Hour))
	loop.stop()
	beforeSubmit.Store(&hook)
	schemaVerBefore := dom.InfoSchema().SchemaMetaVersion()
	// Owner change: the new owner runs a round right away despite the 1h period.
	drainRounds()
	loop = startStorageClassIntervalRecalcLoop(dom.DDL())
	waitRounds(1)
	waitJobsDone()
	require.NotNil(t, userAlterErr.Load(), "the user ALTER must run before the job is submitted")
	require.NoError(t, *userAlterErr.Load())
	require.Equal(t, newRule, external.GetTableByName(t, tk, "test", "t").Meta().EngineAttribute)
	require.Equal(t, map[string]string{"p202310": "IA", "p202311": "STANDARD", "p202312": "STANDARD", "pmax": "STANDARD"},
		storageClassPartitionTiers(t, tk, "t"))
	// Only the user's ALTER bumped the schema version.
	require.Equal(t, schemaVerBefore+1, dom.InfoSchema().SchemaMetaVersion())

	// A period of 0 disables recalculation.
	loop.stop()
	setMockTime(now, "2026-11-15 00:00:00")
	tk.MustExec(`ALTER TABLE t ENGINE_ATTRIBUTE = '{"storage_class": {"tier": "IA", "less_than": "INTERVAL 3 YEAR"}}'`)
	period.Store(-1)
	tk.MustExec("SET GLOBAL tidb_storage_class_interval_recalc_period = 0")
	setMockTime(now, "2027-01-01 00:00:00")
	submitted.Store(0)
	drainRounds()
	loop = startStorageClassIntervalRecalcLoop(dom.DDL())
	// Wait for several checks of the period variable; none may start a round.
	ticksBefore := ticks.Load()
	require.Eventually(t, func() bool { return ticks.Load() >= ticksBefore+5 }, 10*time.Second, time.Millisecond)
	loop.stop()
	require.Empty(t, rounds)
	require.Zero(t, submitted.Load())
	require.Equal(t, "STANDARD", storageClassPartitionTiers(t, tk, "t")["p202312"])
}
