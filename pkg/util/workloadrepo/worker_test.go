// Copyright 2024 PingCAP, Inc.
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

package workloadrepo

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func setupWorkerForTest(ctx context.Context, etcdCli *clientv3.Client, dom *domain.Domain, id string, testWorker bool) *worker {
	wrk := &worker{}
	if !testWorker {
		wrk = &workerCtx
	}

	workloadTables2 := make([]repositoryTable, len(workloadTables))
	copy(workloadTables2, workloadTables)

	owner.ManagerSessionTTL = 3
	initializeWorker(wrk,
		etcdCli, func(s1, s2 string) owner.Manager {
			return owner.NewOwnerManager(ctx, etcdCli, s1, id, s2)
		},
		dom.SysSessionPool(),
		workloadTables2)
	wrk.samplingInterval = 1
	wrk.snapshotInterval = 1
	wrk.instanceID = id
	return wrk
}

func setupDomainAndContext(t *testing.T) (context.Context, kv.Storage, *domain.Domain, string) {
	ctx := context.Background()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	var cancel context.CancelFunc = nil
	if ddl, ok := t.Deadline(); ok {
		ctx, cancel = context.WithDeadline(ctx, ddl)
	}
	t.Cleanup(func() {
		if cancel != nil {
			cancel()
		}
	})

	store, dom := testkit.CreateMockStoreAndDomain(t)

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()

	lcurl, err := url.Parse("http://127.0.0.1:0")
	require.NoError(t, err)
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{*lcurl}, []url.URL{*lcurl}
	lpurl, err := url.Parse("http://127.0.0.1:0")
	require.NoError(t, err)
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{*lpurl}, []url.URL{*lpurl}

	cfg.InitialCluster = "default=" + lpurl.String()
	cfg.Logger = "zap"
	embedEtcd, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(embedEtcd.Close)

	select {
	case <-embedEtcd.Server.ReadyNotify():
	case <-time.After(5 * time.Second):
		embedEtcd.Server.Stop() // trigger a shutdown
		require.False(t, true, "server took too long to start")
	}

	return ctx, store, dom, embedEtcd.Clients[0].Addr().String()
}

func setupWorker(ctx context.Context, t *testing.T, addr string, dom *domain.Domain, id string, testWorker bool) *worker {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{addr},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = etcdCli.Close()
	})

	wrk := setupWorkerForTest(ctx, etcdCli, dom, id, testWorker)

	t.Cleanup(func() {
		wrk.stop()
	})

	return wrk
}

func eventuallyWithLock(t *testing.T, wrk *worker, fn func() bool) {
	require.Eventually(t, func() bool {
		wrk.Lock()
		defer wrk.Unlock()
		return fn()
	}, time.Minute, time.Second)
}

func trueWithLock(t *testing.T, wrk *worker, fn func() bool) {
	wrk.Lock()
	defer wrk.Unlock()
	require.True(t, fn())
}

func waitForTables(ctx context.Context, t *testing.T, wrk *worker, now time.Time) {
	require.Eventually(t, func() bool {
		return wrk.checkTablesExists(ctx, now)
	}, time.Minute, time.Second)
}

func TestRaceToCreateTablesWorker(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)

	_, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("workload_schema"))
	require.False(t, ok)

	wrk1 := setupWorker(ctx, t, addr, dom, "worker1", true)
	wrk2 := setupWorker(ctx, t, addr, dom, "worker2", true)
	wrk1.changeSnapshotInterval(nil, "3600")
	wrk2.changeSnapshotInterval(nil, "3600")
	now := time.Now()
	require.NoError(t, wrk1.setRepositoryDest(ctx, "table"))
	require.NoError(t, wrk2.setRepositoryDest(ctx, "table"))

	require.Eventually(t, func() bool {
		return wrk1.checkTablesExists(ctx, now) && wrk2.checkTablesExists(ctx, now)
	}, time.Minute, time.Second)

	tk := testkit.NewTestKit(t, store)

	// sampling succeeded
	require.Eventually(t, func() bool {
		res := tk.MustQuery("select instance_id, count(*) from workload_schema.hist_memory_usage group by instance_id").Rows()
		return len(res) >= 2
	}, time.Minute, time.Second)

	// no snapshot for now
	res := tk.MustQuery("select snap_id, count(*) from workload_schema.hist_snapshots group by snap_id").Rows()
	require.Len(t, res, 0)

	// manually trigger snapshot by sending a tick to all workers
	wrk1.snapshotChan <- struct{}{}
	require.Eventually(t, func() bool {
		res := tk.MustQuery("select snap_id, count(*) from workload_schema.hist_snapshots group by snap_id").Rows()
		return len(res) == 1
	}, time.Minute, time.Second)

	wrk2.snapshotChan <- struct{}{}
	require.Eventually(t, func() bool {
		res := tk.MustQuery("select snap_id, count(*) from workload_schema.hist_snapshots group by snap_id").Rows()
		return len(res) == 2
	}, time.Minute, time.Second)
}

func getMultipleWorkerCount(tk *testkit.TestKit, worker string) int {
	res := tk.MustQuery("select count(*) from workload_schema.hist_memory_usage group by instance_id having instance_id = '" + worker + "'").Rows()
	return len(res)
}

func TestMultipleWorker(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)
	tk := testkit.NewTestKit(t, store)

	wrk1 := setupWorker(ctx, t, addr, dom, "worker1", true)
	wrk1.changeSamplingInterval(ctx, "1")
	wrk2 := setupWorker(ctx, t, addr, dom, "worker2", true)
	wrk2.changeSamplingInterval(ctx, "1")

	// start worker 1
	now := time.Now()
	require.NoError(t, wrk1.setRepositoryDest(ctx, "table"))
	waitForTables(ctx, t, wrk1, now)
	require.True(t, wrk1.owner.IsOwner())

	require.Eventually(t, func() bool {
		return getMultipleWorkerCount(tk, "worker1") >= 1
	}, time.Minute, time.Second)

	// start worker 2
	require.NoError(t, wrk2.setRepositoryDest(ctx, "table"))
	eventuallyWithLock(t, wrk2, func() bool { return wrk2.owner != nil })
	require.True(t, !wrk2.owner.IsOwner())

	require.Eventually(t, func() bool {
		return getMultipleWorkerCount(tk, "worker2") >= 1
	}, time.Minute, time.Second)

	// stop worker 1, worker 2 should become owner
	require.NoError(t, wrk1.setRepositoryDest(ctx, ""))
	require.Eventually(t, func() bool {
		return wrk2.owner.IsOwner()
	}, time.Minute, time.Second)

	// start worker 1 again
	require.NoError(t, wrk1.setRepositoryDest(ctx, "table"))
	eventuallyWithLock(t, wrk1, func() bool { return wrk1.owner != nil })
	require.True(t, !wrk1.owner.IsOwner())

	// get counts from tables for both workers
	cnt1 := getMultipleWorkerCount(tk, "worker1")
	cnt2 := getMultipleWorkerCount(tk, "worker2")

	require.Eventually(t, func() bool {
		cnt := getMultipleWorkerCount(tk, "worker1")
		return cnt >= cnt1
	}, time.Minute, time.Second)

	// stop worker 2, worker 1 should become owner
	require.NoError(t, wrk2.setRepositoryDest(ctx, ""))
	require.Eventually(t, func() bool {
		return wrk1.owner.IsOwner()
	}, time.Minute, time.Second)

	// start worker 2 again
	require.NoError(t, wrk2.setRepositoryDest(ctx, "table"))
	eventuallyWithLock(t, wrk2, func() bool { return wrk2.owner != nil })
	require.True(t, !wrk2.owner.IsOwner())

	require.Eventually(t, func() bool {
		cnt := getMultipleWorkerCount(tk, "worker2")
		return cnt >= cnt2
	}, time.Minute, time.Second)
}

func TestGlobalWorker(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)
	tk := testkit.NewTestKit(t, store)

	_, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("workload_schema"))
	require.False(t, ok)

	wrk := setupWorker(ctx, t, addr, dom, "worker", false)
	now := time.Now()
	tk.MustExec("set @@global.tidb_workload_repository_dest='table'")

	waitForTables(ctx, t, wrk, now)

	// sampling succeeded
	require.Eventually(t, func() bool {
		res := tk.MustQuery("select instance_id from workload_schema.hist_memory_usage").Rows()
		return len(res) >= 1
	}, time.Minute, time.Second)
}

func TestAdminWorkloadRepo(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)
	tk := testkit.NewTestKit(t, store)

	_, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("workload_schema"))
	require.False(t, ok)

	wrk := setupWorker(ctx, t, addr, dom, "worker", false)
	now := time.Now()
	tk.MustExec("set @@global.tidb_workload_repository_snapshot_interval='5000'")
	tk.MustExec("set @@global.tidb_workload_repository_active_sampling_interval='600'")
	tk.MustExec("set @@global.tidb_workload_repository_dest='table'")

	require.Eventually(t, func() bool {
		return wrk.checkTablesExists(ctx, now)
	}, time.Minute, time.Second)

	// able to snapshot manually
	tk.MustExec("admin create workload snapshot")
	require.Eventually(t, func() bool {
		res := tk.MustQuery("select snap_id, count(*) from workload_schema.hist_snapshots group by snap_id").Rows()
		return len(res) >= 1
	}, time.Minute, time.Second)

	// disable the worker and it will fail
	tk.MustExec("set @@global.tidb_workload_repository_dest=''")
	tk.MustExecToErr("admin create workload snapshot")
}

func getRows(t *testing.T, tk *testkit.TestKit, cnt int, maxSecs int, query string) [][]any {
	var rows [][]any
	require.Eventually(t, func() bool {
		rows = tk.MustQuery(query).Rows()
		return len(rows) == cnt
	}, time.Second*time.Duration(maxSecs*cnt), time.Millisecond*100)
	return rows
}

func validateDate(t *testing.T, row []any, idx int, lastRowTs time.Time, maxSecs int) time.Time {
	loc := lastRowTs.Location()
	actualTs, err := time.ParseInLocation("2006-01-02 15:04:05", row[idx].(string), loc)
	require.NoError(t, err)
	require.Greater(t, actualTs.Unix(), lastRowTs.Unix())                    // after last row
	require.LessOrEqual(t, actualTs.Unix(), lastRowTs.Unix()+int64(maxSecs)) // within last maxSecs seconds of last row
	return actualTs
}

func SamplingTimingWorker(t *testing.T, tk *testkit.TestKit, lastRowTs time.Time, cnt int, maxSecs int) time.Time {
	rows := getRows(t, tk, cnt, maxSecs, "select instance_id, ts from "+WorkloadSchema+".hist_memory_usage where ts > '"+lastRowTs.Format("2006-01-02 15:04:05")+"' order by ts asc")

	for _, row := range rows {
		// check that the instance_id is correct
		require.Equal(t, "worker", row[0]) // instance_id should match worker name

		// check that each rows are the correct interval apart
		lastRowTs = validateDate(t, row, 1, lastRowTs, maxSecs)
	}

	return lastRowTs
}

func TestSamplingTimingWorker(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)
	tk := testkit.NewTestKit(t, store)

	wrk := setupWorker(ctx, t, addr, dom, "worker", true)
	wrk.changeSamplingInterval(ctx, "2")
	now := time.Now()
	wrk.setRepositoryDest(ctx, "table")

	waitForTables(ctx, t, wrk, now)

	lastRowTs := time.Now()
	lastRowTs = SamplingTimingWorker(t, tk, lastRowTs, 3, 3)

	// Change interval and verify new samples are taken at new interval
	wrk.changeSamplingInterval(ctx, "5")

	_ = SamplingTimingWorker(t, tk, lastRowTs, 3, 6)
}

func findMatchingRowForSnapshot(t *testing.T, rowidx int, snapRows [][]any, row []any, lastRowTs time.Time, maxSecs int) {
	require.Less(t, rowidx, len(snapRows))
	row2 := snapRows[rowidx]
	require.Equal(t, row2[0], row[0])
	require.Equal(t, row2[2], "worker")
	validateDate(t, row2, 1, lastRowTs, maxSecs)
}

func SnapshotTimingWorker(t *testing.T, tk *testkit.TestKit, lastRowTs time.Time, lastSnapID int, cnt int, maxSecs int) (time.Time, int) {
	rows := getRows(t, tk, cnt, maxSecs, "select snap_id, begin_time from "+WorkloadSchema+"."+histSnapshotsTable+" where begin_time > '"+lastRowTs.Format("2006-01-02 15:04:05")+"' order by begin_time asc")

	// We want to get all rows if we are starting from 0.
	snapWhere := ""
	if lastSnapID > 0 {
		snapWhere = " where snap_id > " + strconv.Itoa(lastSnapID)
	}

	rows2 := getRows(t, tk, cnt, maxSecs, "select snap_id, ts, instance_id from WORKLOAD_SCHEMA.HIST_MEMORY_USAGE2"+snapWhere+" order by snap_id asc")
	rows3 := getRows(t, tk, cnt, maxSecs, "select snap_id, ts, instance_id from WORKLOAD_SCHEMA.HIST_MEMORY_USAGE3"+snapWhere+" order by snap_id asc")
	rowidx := 0

	for _, row := range rows {
		snapID, err := strconv.Atoi(row[0].(string))
		require.NoError(t, err)
		require.Equal(t, lastSnapID+1, snapID)

		actualTs := validateDate(t, row, 1, lastRowTs, maxSecs)

		findMatchingRowForSnapshot(t, rowidx, rows2, row, lastRowTs, maxSecs)
		findMatchingRowForSnapshot(t, rowidx, rows3, row, lastRowTs, maxSecs)
		rowidx++

		lastSnapID = snapID
		lastRowTs = actualTs
	}

	return lastRowTs, lastSnapID
}

func TestSnapshotTimingWorker(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)
	tk := testkit.NewTestKit(t, store)

	wrk := setupWorker(ctx, t, addr, dom, "worker", true)

	// MEMORY_USAGE will contain a single row.  Ideal for testing here.
	wrk.workloadTables[1] = repositoryTable{
		"INFORMATION_SCHEMA", "MEMORY_USAGE", snapshotTable, "HIST_MEMORY_USAGE2", "", "", "",
	}
	wrk.workloadTables = append(wrk.workloadTables, repositoryTable{
		"INFORMATION_SCHEMA", "MEMORY_USAGE", snapshotTable, "HIST_MEMORY_USAGE3", "", "", "",
	})

	wrk.changeSnapshotInterval(ctx, "2")
	now := time.Now()
	wrk.setRepositoryDest(ctx, "table")

	waitForTables(ctx, t, wrk, now)

	// Check that snapshots are taken at 2 second intervals
	lastRowTs := time.Now()
	lastSnapID := 0
	lastRowTs, lastSnapID = SnapshotTimingWorker(t, tk, lastRowTs, lastSnapID, 3, 3)

	// Change interval and verify new snapshots are taken at new interval
	wrk.changeSnapshotInterval(ctx, "6")
	_, _ = SnapshotTimingWorker(t, tk, lastRowTs, lastSnapID, 1, 7)
}

func TestStoppingAndRestartingWorker(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)
	tk := testkit.NewTestKit(t, store)

	wrk := setupWorker(ctx, t, addr, dom, "worker", true)
	wrk.changeSamplingInterval(ctx, "1")
	wrk.changeSnapshotInterval(ctx, "1")
	now := time.Now()
	wrk.setRepositoryDest(ctx, "table")

	waitForTables(ctx, t, wrk, now)

	// There should be one row every second.
	require.Eventually(t, func() bool {
		return len(tk.MustQuery("select instance_id from workload_schema.hist_memory_usage").Rows()) > 0
	}, time.Minute, time.Second)
	require.Eventually(t, func() bool {
		return len(tk.MustQuery("select snap_id from workload_schema."+histSnapshotsTable).Rows()) > 0
	}, time.Minute, time.Second)

	// Stop worker and verify no new samples are taken
	wrk.setRepositoryDest(ctx, "")
	eventuallyWithLock(t, wrk, func() bool { return wrk.cancel == nil })
	samplingCnt := len(tk.MustQuery("select instance_id from workload_schema.hist_memory_usage").Rows())
	snapshotCnt := len(tk.MustQuery("select snap_id from workload_schema." + histSnapshotsTable).Rows())

	// Wait for 5 seconds to make sure no new samples are taken
	time.Sleep(time.Second * 5)
	require.True(t, len(tk.MustQuery("select instance_id from workload_schema.hist_memory_usage").Rows()) == samplingCnt)
	require.True(t, len(tk.MustQuery("select snap_id from workload_schema."+histSnapshotsTable).Rows()) == snapshotCnt)

	// Restart worker and verify new samples are taken
	wrk.setRepositoryDest(ctx, "table")

	require.Eventually(t, func() bool {
		return len(tk.MustQuery("select instance_id from workload_schema.hist_memory_usage").Rows()) >= samplingCnt
	}, time.Minute, time.Second)
	require.Eventually(t, func() bool {
		return len(tk.MustQuery("select snap_id from workload_schema."+histSnapshotsTable).Rows()) >= snapshotCnt
	}, time.Minute, time.Second)
}

func TestSettingSQLVariables(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)
	tk := testkit.NewTestKit(t, store)

	wrk := setupWorker(ctx, t, addr, dom, "worker", false)

	/* Order of less than minimum, maximum, minimum, and greater than maximum
	tests are not random. Because we need to know if the invalid values are
	converted to the minimum or maximum. */

	// Test values less than minimum
	tk.MustExec("set @@global." + repositorySamplingInterval + " = -1")
	tk.MustExec("set @@global." + repositorySnapshotInterval + " = 899")
	tk.MustExec("set @@global." + repositoryRetentionDays + " = -1")
	eventuallyWithLock(t, wrk, func() bool { return int32(0) == wrk.samplingInterval })
	eventuallyWithLock(t, wrk, func() bool { return int32(900) == wrk.snapshotInterval })
	eventuallyWithLock(t, wrk, func() bool { return int32(0) == wrk.retentionDays })

	// Test maximum values
	tk.MustExec("set @@global." + repositorySamplingInterval + " = 600")
	tk.MustExec("set @@global." + repositorySnapshotInterval + " = 7200")
	tk.MustExec("set @@global." + repositoryRetentionDays + " = 365")
	eventuallyWithLock(t, wrk, func() bool { return int32(600) == wrk.samplingInterval })
	eventuallyWithLock(t, wrk, func() bool { return int32(7200) == wrk.snapshotInterval })
	eventuallyWithLock(t, wrk, func() bool { return int32(365) == wrk.retentionDays })

	// Test minimum values
	tk.MustExec("set @@global." + repositorySamplingInterval + " = 0")
	tk.MustExec("set @@global." + repositorySnapshotInterval + " = 900")
	tk.MustExec("set @@global." + repositoryRetentionDays + " = 0")
	eventuallyWithLock(t, wrk, func() bool { return int32(0) == wrk.samplingInterval })
	eventuallyWithLock(t, wrk, func() bool { return int32(900) == wrk.snapshotInterval })
	eventuallyWithLock(t, wrk, func() bool { return int32(0) == wrk.retentionDays })

	// Test values greater than maximum
	tk.MustExec("set @@global." + repositorySamplingInterval + " = 601")
	tk.MustExec("set @@global." + repositorySnapshotInterval + " = 7201")
	tk.MustExec("set @@global." + repositoryRetentionDays + " = 366")
	eventuallyWithLock(t, wrk, func() bool { return int32(600) == wrk.samplingInterval })
	eventuallyWithLock(t, wrk, func() bool { return int32(7200) == wrk.snapshotInterval })
	eventuallyWithLock(t, wrk, func() bool { return int32(365) == wrk.retentionDays })

	// Test invalid values for intervals
	tk.MustGetDBError("set @@global."+repositorySamplingInterval+" = 'invalid'", variable.ErrWrongTypeForVar)
	tk.MustGetDBError("set @@global."+repositorySnapshotInterval+" = 'invalid'", variable.ErrWrongTypeForVar)
	tk.MustGetDBError("set @@global."+repositoryRetentionDays+" = 'invalid'", variable.ErrWrongTypeForVar)

	// Test that if the strconv.Atoi call fails that the error is correctly handled.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/workloadrepo/FastRunawayGC", `return(true)`))
	tk.MustGetDBError("set @@global."+repositorySamplingInterval+" = 10", errWrongValueForVar)
	tk.MustGetDBError("set @@global."+repositorySnapshotInterval+" = 901", errWrongValueForVar)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/workloadrepo/FastRunawayGC"))

	trueWithLock(t, wrk, func() bool { return int32(600) == wrk.samplingInterval })
	trueWithLock(t, wrk, func() bool { return int32(7200) == wrk.snapshotInterval })
	trueWithLock(t, wrk, func() bool { return int32(365) == wrk.retentionDays })

	// Test setting repository destination
	tk.MustExec("set @@global." + repositoryDest + " = 'table'")
	eventuallyWithLock(t, wrk, func() bool { return wrk.enabled })

	// Test disabling repository
	tk.MustExec("set @@global." + repositoryDest + " = ''")
	eventuallyWithLock(t, wrk, func() bool { return !wrk.enabled })

	// Test invalid value for repository destination
	tk.MustGetDBError("set @@global."+repositoryDest+" = 'invalid'", errWrongValueForVar)
}

func getTable(t *testing.T, tableName string, wrk *worker) *repositoryTable {
	var tbl *repositoryTable = nil
	for _, nt := range wrk.workloadTables {
		if nt.table == tableName {
			tbl = &nt
		}
	}
	require.NotNil(t, tbl)
	return tbl
}

func validatePartitionsMatchExpected(ctx context.Context, t *testing.T,
	sess sessionctx.Context, tbl *repositoryTable, partitions []time.Time) bool {
	// validate that the partitions exactly match as expected
	ep := make(map[string]struct{})
	for _, p := range partitions {
		ep[generatePartitionName(p.AddDate(0, 0, 1))] = struct{}{}
	}

	is := sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	tbSchema, err := is.TableByName(ctx, workloadSchemaCIStr, ast.NewCIStr(tbl.destTable))
	require.NoError(t, err)
	tbInfo := tbSchema.Meta()
	require.NotNil(t, tbInfo)
	pi := tbInfo.GetPartitionInfo()
	tp := make(map[string]struct{})
	if pi != nil && pi.Definitions != nil {
		for _, p := range pi.Definitions {
			tp[p.Name.L] = struct{}{}
		}
	}

	if len(tp) != len(ep) {
		return false
	}

	for p := range ep {
		_, ok := tp[p]
		if !ok {
			return false
		}
	}

	return true
}

func buildPartitionRow(now time.Time) string {
	newPtTime := now.AddDate(0, 0, 1)
	newPtName := "p" + newPtTime.Format("20060102")
	parttemp := `PARTITION %s VALUES LESS THAN (TO_DAYS('%s'))`
	return fmt.Sprintf(parttemp, newPtName, newPtTime.Format("2006-01-02"))
}

func buildPartitionString(partitions []time.Time) string {
	sb := &strings.Builder{}
	if len(partitions) > 0 {
		fmt.Fprint(sb, ` PARTITION BY RANGE (TO_DAYS(TS)) (`)
		first := true
		for _, p := range partitions {
			if !first {
				fmt.Fprint(sb, ", ")
			}
			fmt.Fprint(sb, buildPartitionRow(p))
			first = false
		}
		fmt.Fprint(sb, `)`)
	}
	return sb.String()
}

func createTableWithParts(ctx context.Context, t *testing.T, tk *testkit.TestKit, tbl *repositoryTable,
	sess sessionctx.Context, partitions []time.Time) {
	createSQL, err := buildCreateQuery(ctx, sess, tbl)
	require.NoError(t, err)
	createSQL += buildPartitionString(partitions)
	tk.MustExec(createSQL)
	require.True(t, validatePartitionsMatchExpected(ctx, t, sess, tbl, partitions))
}

func validatePartitionCreation(ctx context.Context, now time.Time, t *testing.T,
	sess sessionctx.Context, tk *testkit.TestKit, wrk *worker,
	firstTestFails bool, tableName string, partitions []time.Time, expectedParts []time.Time) {
	tbl := getTable(t, tableName, wrk)
	createTableWithParts(ctx, t, tk, tbl, sess, partitions)

	is := sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	require.False(t, firstTestFails == checkTableExistsByIS(ctx, is, tbl.destTable, now))

	is = sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	require.NoError(t, createPartition(ctx, is, tbl, sess, now))

	require.True(t, validatePartitionsMatchExpected(ctx, t, sess, tbl, expectedParts))

	is = sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	require.True(t, checkTableExistsByIS(ctx, is, tbl.destTable, now))
}

func TestCreatePartition(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)
	wrk := setupWorker(ctx, t, addr, dom, "worker", true)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database WORKLOAD_SCHEMA")
	tk.MustExec("use WORKLOAD_SCHEMA")

	_sessctx := wrk.getSessionWithRetry()
	sess := _sessctx.(sessionctx.Context)

	wrk.fillInTableNames()

	now := time.Now()

	/* Tables without partitions are not currently supported. */

	// Should create one partition for today and tomorrow on a table with only old partitions before today.
	partitions := []time.Time{now.AddDate(0, 0, -1)}
	expectedParts := []time.Time{now.AddDate(0, 0, -1), now, now.AddDate(0, 0, 1)}
	validatePartitionCreation(ctx, now, t, sess, tk, wrk, true, "PROCESSLIST", partitions, expectedParts)

	// Should create one partition for tomorrow on a table with only a partition for today.
	partitions = []time.Time{now}
	expectedParts = []time.Time{now, now.AddDate(0, 0, 1)}
	validatePartitionCreation(ctx, now, t, sess, tk, wrk, true, "DATA_LOCK_WAITS", partitions, expectedParts)

	// Should not create any partitions on a table with only a partition for tomorrow.
	partitions = []time.Time{now.AddDate(0, 0, 1)}
	expectedParts = []time.Time{now.AddDate(0, 0, 1)}
	validatePartitionCreation(ctx, now, t, sess, tk, wrk, false, "TIDB_TRX", partitions, expectedParts)

	// Should not create any partitions on a table with only partitions for both today and tomorrow.
	partitions = []time.Time{now, now.AddDate(0, 0, 1)}
	expectedParts = []time.Time{now, now.AddDate(0, 0, 1)}
	validatePartitionCreation(ctx, now, t, sess, tk, wrk, false, "MEMORY_USAGE", partitions, expectedParts)

	// Should not create any partitions on a table with a partition for the day after tomorrow.
	partitions = []time.Time{now.AddDate(0, 0, 2)}
	expectedParts = []time.Time{now.AddDate(0, 0, 2)}
	validatePartitionCreation(ctx, now, t, sess, tk, wrk, false, "CLUSTER_LOAD", partitions, expectedParts)

	// Should not fill in missing partitions on a table with a partition for dates beyond tomorrow.
	partitions = []time.Time{now, now.AddDate(0, 0, 3)}
	expectedParts = []time.Time{now, now.AddDate(0, 0, 3)}
	validatePartitionCreation(ctx, now, t, sess, tk, wrk, false, "TIDB_HOT_REGIONS", partitions, expectedParts)

	// this table should be updated when the repository is enabled
	partitions = []time.Time{now}
	createTableWithParts(ctx, t, tk, getTable(t, "DEADLOCKS", wrk), sess, partitions)

	// turn on the repository and see if it creates the remaining tables
	now = time.Now()
	wrk.setRepositoryDest(ctx, "table")
	waitForTables(ctx, t, wrk, now)
}

func validatePartitionDrop(ctx context.Context, now time.Time, t *testing.T,
	sess sessionctx.Context, tk *testkit.TestKit, wrk *worker,
	tableName string, partitions []time.Time, retention int, shouldErr bool, expectedParts []time.Time) {
	tbl := getTable(t, tableName, wrk)
	createTableWithParts(ctx, t, tk, tbl, sess, partitions)

	require.True(t, validatePartitionsMatchExpected(ctx, t, sess, tbl, partitions))

	is := sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	err := dropOldPartition(ctx, is, tbl, now, retention, sess)
	if shouldErr {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}

	require.True(t, validatePartitionsMatchExpected(ctx, t, sess, tbl, expectedParts))
}

func TestDropOldPartitions(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)
	wrk := setupWorker(ctx, t, addr, dom, "worker", true)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database WORKLOAD_SCHEMA")
	tk.MustExec("use WORKLOAD_SCHEMA")

	_sessctx := wrk.getSessionWithRetry()
	sess := _sessctx.(sessionctx.Context)

	wrk.fillInTableNames()

	now := time.Now()

	var partitions []time.Time
	var expectedParts []time.Time
	// Zero retention days disabled the partition deletion code.  So we will not test it here.

	// Should not trim any partitions
	partitions = []time.Time{now.AddDate(0, 0, -1), now, now.AddDate(0, 0, 1)}
	expectedParts = []time.Time{now.AddDate(0, 0, -1), now, now.AddDate(0, 0, 1)}
	validatePartitionDrop(ctx, now, t, sess, tk, wrk, "PROCESSLIST", partitions, 1, false, expectedParts)

	// Test trimming a single partition more than one date before retention date.
	partitions = []time.Time{now.AddDate(0, 0, -2), now, now.AddDate(0, 0, 1)}
	expectedParts = []time.Time{now, now.AddDate(0, 0, 1)}
	validatePartitionDrop(ctx, now, t, sess, tk, wrk, "DATA_LOCK_WAITS", partitions, 1, false, expectedParts)

	// Check that multiple partitions can be removed.
	partitions = []time.Time{now.AddDate(0, 0, -3), now.AddDate(0, 0, -2), now.AddDate(0, 0, -1), now, now.AddDate(0, 0, 1)}
	expectedParts = []time.Time{now.AddDate(0, 0, -1), now, now.AddDate(0, 0, 1)}
	validatePartitionDrop(ctx, now, t, sess, tk, wrk, "TIDB_TRX", partitions, 1, false, expectedParts)

	// should trim nothing
	partitions = []time.Time{now.AddDate(0, 0, -2), now.AddDate(0, 0, -1), now, now.AddDate(0, 0, 1)}
	expectedParts = []time.Time{now.AddDate(0, 0, -2), now.AddDate(0, 0, -1), now, now.AddDate(0, 0, 1)}
	validatePartitionDrop(ctx, now, t, sess, tk, wrk, "MEMORY_USAGE", partitions, 2, false, expectedParts)

	// should trim one partition
	partitions = []time.Time{now.AddDate(0, 0, -3), now.AddDate(0, 0, -2), now.AddDate(0, 0, 1)}
	expectedParts = []time.Time{now.AddDate(0, 0, -2), now.AddDate(0, 0, 1)}
	validatePartitionDrop(ctx, now, t, sess, tk, wrk, "CLUSTER_LOAD", partitions, 2, false, expectedParts)

	// validate that it works when not dropping any partitions
	partitions = []time.Time{now.AddDate(0, 0, -1)}
	expectedParts = []time.Time{now.AddDate(0, 0, -1)}
	validatePartitionDrop(ctx, now, t, sess, tk, wrk, "TIDB_HOT_REGIONS", partitions, 2, false, expectedParts)

	// there must be partitions, so this should error
	partitions = []time.Time{now.AddDate(0, 0, -2)}
	expectedParts = []time.Time{now.AddDate(0, 0, -2)}
	validatePartitionDrop(ctx, now, t, sess, tk, wrk, "TIKV_STORE_STATUS", partitions, 1, true, expectedParts)
}

func TestAddNewPartitionsOnStart(t *testing.T) {
	ctx, _, dom, addr := setupDomainAndContext(t)

	wrk := setupWorker(ctx, t, addr, dom, "worker", true)
	wrk.fillInTableNames()
	now := time.Now()
	require.NoError(t, wrk.createAllTables(ctx, now))
	require.True(t, wrk.checkTablesExists(ctx, now))

	_sessctx := wrk.getSessionWithRetry()
	sess := _sessctx.(sessionctx.Context)
	expectedParts := []time.Time{now, now.AddDate(0, 0, 1)}
	for _, tbl := range wrk.workloadTables {
		// check for now and now + 1 partitions
		require.True(t, validatePartitionsMatchExpected(ctx, t, sess, &tbl, expectedParts))
	}
}

func getNextTick(now time.Time) time.Duration {
	return time.Second
}

func TestHouseKeeperThread(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)
	wrk := setupWorker(ctx, t, addr, dom, "worker", true)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database WORKLOAD_SCHEMA")
	tk.MustExec("use WORKLOAD_SCHEMA")

	_sessctx := wrk.getSessionWithRetry()
	sess := _sessctx.(sessionctx.Context)

	wrk.workloadTables = []repositoryTable{
		{"INFORMATION_SCHEMA", "PROCESSLIST", samplingTable, "", "", "", ""},
		{"INFORMATION_SCHEMA", "DATA_LOCK_WAITS", samplingTable, "", "", "", ""},
	}
	wrk.fillInTableNames()

	now := time.Now()
	var parts []time.Time

	// This will have a partition added for tomorrow.
	parts = []time.Time{now.AddDate(0, 0, -1), now}
	plTbl := getTable(t, "PROCESSLIST", wrk)
	createTableWithParts(ctx, t, tk, plTbl, sess, parts)
	require.True(t, validatePartitionsMatchExpected(ctx, t, sess, plTbl, parts))

	// This will have partitions removed.   -3 will be removed when retention is 2, and -2 will be removed at 1.
	parts = []time.Time{now.AddDate(0, 0, -3), now.AddDate(0, 0, -2), now.AddDate(0, 0, -1), now}
	dlwTbl := getTable(t, "DATA_LOCK_WAITS", wrk)
	createTableWithParts(ctx, t, tk, dlwTbl, sess, parts)
	require.True(t, validatePartitionsMatchExpected(ctx, t, sess, dlwTbl, parts))

	// start the thread
	wrk.owner = wrk.newOwner(ownerKey, promptKey)
	require.NoError(t, wrk.owner.CampaignOwner())
	require.Eventually(t, func() bool {
		return wrk.owner.IsOwner()
	}, time.Second*10, time.Second)

	// build remaining tables and start housekeeper
	wrk.retentionDays = 2
	fn := wrk.getHouseKeeper(ctx, getNextTick)
	go fn()

	// check paritions
	require.Eventually(t, func() bool {
		return validatePartitionsMatchExpected(ctx, t, sess, plTbl, []time.Time{now.AddDate(0, 0, -1), now, now.AddDate(0, 0, 1)})
	}, time.Second*10, time.Second)
	require.Eventually(t, func() bool {
		return validatePartitionsMatchExpected(ctx, t, sess, dlwTbl, []time.Time{now.AddDate(0, 0, -2), now.AddDate(0, 0, -1), now, now.AddDate(0, 0, 1)})
	}, time.Second*10, time.Second)

	// Change the retention value.  We just want to see that it is still running.
	wrk.Lock()
	wrk.retentionDays = 1
	wrk.Unlock()

	// check partitions
	time.Sleep(time.Second * 10)
	require.Eventually(t, func() bool {
		return validatePartitionsMatchExpected(ctx, t, sess, plTbl, []time.Time{now.AddDate(0, 0, -1), now, now.AddDate(0, 0, 1)})
	}, time.Second*10, time.Second)
	require.Eventually(t, func() bool {
		return validatePartitionsMatchExpected(ctx, t, sess, dlwTbl, []time.Time{now.AddDate(0, 0, -1), now, now.AddDate(0, 0, 1)})
	}, time.Second*10, time.Second)
}

func TestCalcNextTick(t *testing.T) {
	loc := time.Now().Location()
	require.True(t, calcNextTick(time.Date(2024, 12, 6, 23, 59, 59, 999999999, loc)) == time.Hour*2+time.Nanosecond)
	require.True(t, calcNextTick(time.Date(2024, 12, 7, 0, 0, 0, 0, loc)) == time.Hour*2)
	require.True(t, calcNextTick(time.Date(2024, 12, 7, 2, 0, 0, 1, loc)) == time.Hour*24-time.Nanosecond)
	require.True(t, calcNextTick(time.Date(2024, 12, 7, 1, 59, 59, 999999999, loc)) == time.Nanosecond)
}
