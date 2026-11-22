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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta/model"
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

func setMockStorageClassIntervalNow(now *atomic.Pointer[time.Time], s string) {
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
	setMockStorageClassIntervalNow(now, "2026-10-31 18:00:00")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	// The DDL owner's recalculation would otherwise move partitions under the test.
	tk.MustExec("SET GLOBAL tidb_storage_class_interval_recalc_period = 0")

	tk.MustExec(storageClassIntervalTable)
	require.Equal(t, map[string]string{"p202310": "IA", "p202311": "STANDARD", "p202312": "STANDARD", "pmax": "STANDARD"},
		storageClassPartitionTiers(t, tk, "t"))

	// ADD PARTITION evaluates new partitions at the current time.
	setMockStorageClassIntervalNow(now, "2026-12-01 00:00:00")
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

// storageClassIntervalRecalcHooks drives the DDL owner's recalculation loop:
// checks run every 10ms, and the period defaults to 1ms so each check runs a
// round. They must be enabled before the store starts, as the owner reads the
// check interval when its loop starts.
type storageClassIntervalRecalcHooks struct {
	now       *atomic.Pointer[time.Time]
	ticks     atomic.Int64
	period    atomic.Int64
	submitted atomic.Int64
	rounds    chan struct{}
	// beforeSubmit runs once, before the next job is submitted.
	beforeSubmit atomic.Pointer[func()]
}

func enableStorageClassIntervalRecalcHooks(t *testing.T) *storageClassIntervalRecalcHooks {
	h := &storageClassIntervalRecalcHooks{now: mockStorageClassIntervalNow(t), rounds: make(chan struct{}, 100000)}
	setMockStorageClassIntervalNow(h.now, "2026-10-31 18:00:00")
	h.period.Store(int64(time.Millisecond))
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockStorageClassIntervalRecalcCheckInterval", func(interval *time.Duration) {
		h.ticks.Add(1)
		*interval = 10 * time.Millisecond
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockStorageClassIntervalRecalcPeriod", func(p *time.Duration) {
		if overridden := time.Duration(h.period.Load()); overridden >= 0 {
			*p = overridden
		}
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterStorageClassIntervalRecalcRound", func() {
		h.rounds <- struct{}{}
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeStorageClassIntervalRecalcSubmit", func(int64) {
		h.submitted.Add(1)
		if fn := h.beforeSubmit.Swap(nil); fn != nil {
			(*fn)()
		}
	})
	return h
}

// waitFreshRounds waits for n rounds that start after the call.
func (h *storageClassIntervalRecalcHooks) waitFreshRounds(t *testing.T, n int) {
	for len(h.rounds) > 0 {
		<-h.rounds
	}
	// A round may be in progress; it does not count.
	n++
	for range n {
		select {
		case <-h.rounds:
		case <-time.After(10 * time.Second):
			require.FailNow(t, "storage class interval recalculation round did not run")
		}
	}
}

// waitTicks waits for n checks of the period. A round in progress when it is
// called has finished when it returns.
func (h *storageClassIntervalRecalcHooks) waitTicks(t *testing.T, n int64) {
	ticksBefore := h.ticks.Load()
	require.Eventually(t, func() bool { return h.ticks.Load() >= ticksBefore+n }, 10*time.Second, time.Millisecond)
}

// waitTicksWithoutRounds waits for several checks of the period and requires
// that none of them started a round.
func (h *storageClassIntervalRecalcHooks) waitTicksWithoutRounds(t *testing.T) {
	h.waitTicks(t, 1)
	for len(h.rounds) > 0 {
		<-h.rounds
	}
	submittedBefore := h.submitted.Load()
	h.waitTicks(t, 5)
	require.Empty(t, h.rounds)
	require.Equal(t, submittedBefore, h.submitted.Load())
}

func waitStorageClassJobsDone(t *testing.T, tk *testkit.TestKit) {
	require.Eventually(t, func() bool {
		return tk.MustQuery("SELECT COUNT(*) FROM mysql.tidb_ddl_job").Rows()[0][0] == "0"
	}, 10*time.Second, 10*time.Millisecond)
}

// storageClassTransitionCount counts staged TiKV transitions, which exist only
// in the next-gen kernel.
func storageClassTransitionCount(t *testing.T, tk *testkit.TestKit) int {
	if !kerneltype.IsNextGen() {
		return 0
	}
	count, err := strconv.Atoi(tk.MustQuery("SELECT COUNT(*) FROM mysql.tidb_storage_class_transition_history").Rows()[0][0].(string))
	require.NoError(t, err)
	return count
}

func TestStorageClassIntervalRecalc(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableStorageClass = true
	})
	h := enableStorageClassIntervalRecalcHooks(t)
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec(storageClassIntervalTable)

	// Nothing crossed the cutoff, so nothing is submitted.
	h.waitFreshRounds(t, 2)
	require.Zero(t, h.submitted.Load())
	require.Equal(t, "STANDARD", storageClassPartitionTiers(t, tk, "t")["p202311"])

	// p202311 crosses the cutoff at 2026-11-01 UTC.
	transitionsBefore := storageClassTransitionCount(t, tk)
	setMockStorageClassIntervalNow(h.now, "2026-11-01 00:00:00")
	require.Eventually(t, func() bool {
		return storageClassPartitionTiers(t, tk, "t")["p202311"] == "IA"
	}, 10*time.Second, 10*time.Millisecond)
	waitStorageClassJobsDone(t, tk)
	require.Equal(t, map[string]string{"p202310": "IA", "p202311": "IA", "p202312": "STANDARD", "pmax": "STANDARD"},
		storageClassPartitionTiers(t, tk, "t"))
	if kerneltype.IsNextGen() {
		require.NotEqual(t, transitionsBefore, storageClassTransitionCount(t, tk), "the job must stage a TiKV transition")
	}

	// The clock moves backwards: partitions would leave IA, so nothing is submitted.
	h.submitted.Store(0)
	setMockStorageClassIntervalNow(h.now, "2026-01-01 00:00:00")
	h.waitFreshRounds(t, 3)
	require.Zero(t, h.submitted.Load())
	require.Equal(t, "IA", storageClassPartitionTiers(t, tk, "t")["p202311"])

	// Jobs pile up while DDL execution is blocked. Only the first one applies;
	// the duplicates finish without a schema version or a transition.
	releaseJobs := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(*model.Job) {
		<-releaseJobs
	})
	h.submitted.Store(0)
	schemaVerBefore := dom.InfoSchema().SchemaMetaVersion()
	transitionsBefore = storageClassTransitionCount(t, tk)
	setMockStorageClassIntervalNow(h.now, "2026-12-01 00:00:00")
	require.Eventually(t, func() bool { return h.submitted.Load() >= 3 }, 10*time.Second, time.Millisecond)
	close(releaseJobs)
	waitStorageClassJobsDone(t, tk)
	require.Equal(t, "IA", storageClassPartitionTiers(t, tk, "t")["p202312"])
	require.Equal(t, schemaVerBefore+1, dom.InfoSchema().SchemaMetaVersion())
	if kerneltype.IsNextGen() {
		require.Equal(t, transitionsBefore+1, storageClassTransitionCount(t, tk))
	}

	// The user changes the rule after the round read it: the queued job is a
	// no-op. The new owner runs a round right away despite the 1h period.
	tk.MustExec(`ALTER TABLE t REORGANIZE PARTITION pmax INTO (PARTITION p202401 VALUES LESS THAN ('2024-01-01'), PARTITION pmax VALUES LESS THAN (MAXVALUE))`)
	h.period.Store(int64(time.Hour))
	h.waitTicks(t, 2)
	setMockStorageClassIntervalNow(h.now, "2027-01-01 00:00:00")
	newRule := `{"storage_class": {"tier": "IA", "names_in": ["p202310"]}}`
	var userAlterErr atomic.Pointer[error]
	hook := func() {
		err := testkit.NewTestKit(t, store).ExecToErr("ALTER TABLE test.t ENGINE_ATTRIBUTE = '" + newRule + "'")
		userAlterErr.Store(&err)
	}
	h.beforeSubmit.Store(&hook)
	schemaVerBefore = dom.InfoSchema().SchemaMetaVersion()
	require.NoError(t, dom.DDL().OwnerManager().ResignOwner(context.Background()))
	require.Eventually(t, func() bool { return userAlterErr.Load() != nil }, 10*time.Second, 10*time.Millisecond)
	waitStorageClassJobsDone(t, tk)
	require.NoError(t, *userAlterErr.Load())
	require.Equal(t, newRule, external.GetTableByName(t, tk, "test", "t").Meta().EngineAttribute)
	require.Equal(t, "STANDARD", storageClassPartitionTiers(t, tk, "t")["p202401"])
	// Only the user's ALTER bumped the schema version.
	require.Equal(t, schemaVerBefore+1, dom.InfoSchema().SchemaMetaVersion())
}

func TestStorageClassIntervalRecalcDisabled(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableStorageClass = true
	})
	h := enableStorageClassIntervalRecalcHooks(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec(storageClassIntervalTable)

	// With storage class disabled, no round runs although p202311 crossed.
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableStorageClass = false
	})
	h.waitTicks(t, 1)
	setMockStorageClassIntervalNow(h.now, "2026-11-01 00:00:00")
	h.waitTicksWithoutRounds(t)

	// A period of 0 disables recalculation.
	h.period.Store(-1)
	tk.MustExec("SET GLOBAL tidb_storage_class_interval_recalc_period = 0")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableStorageClass = true
	})
	h.waitTicksWithoutRounds(t)
	require.Equal(t, "STANDARD", storageClassPartitionTiers(t, tk, "t")["p202311"])
}
