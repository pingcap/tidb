// Copyright 2023 PingCAP, Inc.
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

package ttlworker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/timer/tablestore"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	mockutil "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestTTLManualTriggerOneTimer(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(tablestore.CreateTimerTableSQL("test", "test_timers"))
	timerStore := tablestore.NewTableTimerStore(1, do.SysSessionPool(), "test", "test_timers", nil)
	defer timerStore.Close()
	var zeroWatermark time.Time
	cli := timerapi.NewDefaultTimerClient(timerStore)
	sync := ttlworker.NewTTLTimerSyncer(do.SysSessionPool(), cli)

	tk.MustExec("set @@global.tidb_ttl_job_enable=0")
	tk.MustExec("create table tp1(a int, t timestamp) TTL=`t`+interval 1 HOUR ttl_job_interval='3h' partition by range(a) (" +
		"partition p0 values less than (10)," +
		"partition p1 values less than (100)," +
		"partition p2 values less than (1000)" +
		")")

	key, physical := getPhysicalTableInfo(t, do, "test", "tp1", "p0")
	_, err := cli.GetTimerByKey(context.TODO(), key)
	require.True(t, errors.ErrorEqual(err, timerapi.ErrTimerNotExist))

	startTrigger := func(ctx context.Context, expectErr string) (func() (string, bool, error), timerapi.ManualRequest) {
		timer, err := cli.GetTimerByKey(context.TODO(), key)
		if !errors.ErrorEqual(timerapi.ErrTimerNotExist, err) {
			require.NoError(t, err)
			require.False(t, timer.IsManualRequesting())
		}

		_, physical = getPhysicalTableInfo(t, do, "test", "tp1", "p0")
		check, err := sync.ManualTriggerTTLTimer(ctx, physical)
		timer = checkTimerWithTableMeta(t, do, cli, "test", "tp1", "p0", zeroWatermark)
		if expectErr != "" {
			require.EqualError(t, err, expectErr)
			require.Nil(t, check)
			require.False(t, timer.IsManualRequesting())
			return nil, timer.ManualRequest
		}

		require.NoError(t, err)
		require.True(t, timer.IsManualRequesting())
		return check, timer.ManualRequest
	}

	testCheckFunc := func(check func() (string, bool, error), expectJobID string, expectErr string) {
		jobID, ok, err := check()
		if expectErr != "" {
			require.Empty(t, expectJobID)
			require.Empty(t, jobID)
			require.False(t, ok)
			require.EqualError(t, err, expectErr)
		} else {
			require.NoError(t, err)
			require.Equal(t, ok, jobID != "")
			require.Equal(t, expectJobID, jobID)
		}
	}

	createJobHistory := func(jobID string) {
		tk.MustExec(fmt.Sprintf(`INSERT INTO mysql.tidb_ttl_job_history (
				job_id,
				table_id,
				parent_table_id,
				table_schema,
				table_name,
				partition_name,
				create_time,
				finish_time,
				ttl_expire,
				expired_rows,
				deleted_rows,
				error_delete_rows,
                status
			)
		VALUES
			(
			 	'%s', %d, %d, 'test', '%s', '',
			 	from_unixtime(%d),
			 	from_unixtime(%d),
			 	from_unixtime(%d),
			 	100, 100, 0, 'running'
		)`,
			jobID, physical.TableInfo.ID, physical.ID, physical.TableInfo.Name.O,
			time.Now().Unix(),
			time.Now().Unix()+int64(time.Minute.Seconds()),
			time.Now().Unix()-int64(time.Hour.Seconds()),
		))
	}

	// start trigger -> not finished -> finished
	check, manual := startTrigger(context.TODO(), "")
	testCheckFunc(check, "", "")
	timer, err := cli.GetTimerByKey(context.TODO(), key)
	require.NoError(t, err)
	manual.ManualEventID = "event123"
	manual.ManualProcessed = true
	require.NoError(t, timerStore.Update(context.TODO(), timer.ID, &timerapi.TimerUpdate{
		ManualRequest: timerapi.NewOptionalVal(manual),
	}))
	testCheckFunc(check, "", "")
	createJobHistory("event123")
	testCheckFunc(check, "event123", "")

	// start trigger -> trigger done but no event id
	check, manual = startTrigger(context.TODO(), "")
	manual.ManualProcessed = true
	require.NoError(t, timerStore.Update(context.TODO(), timer.ID, &timerapi.TimerUpdate{
		ManualRequest: timerapi.NewOptionalVal(manual),
	}))
	testCheckFunc(check, "", "manual request failed to trigger, request cancelled")

	// start trigger -> manual requestID not match
	check, manual = startTrigger(context.TODO(), "")
	manual.ManualRequestID = "anotherreqid"
	require.NoError(t, timerStore.Update(context.TODO(), timer.ID, &timerapi.TimerUpdate{
		ManualRequest: timerapi.NewOptionalVal(manual),
	}))
	testCheckFunc(check, "", "manual request failed to trigger, request not found")
	manual.ManualRequestID = "anotherreqid"
	manual.ManualProcessed = true
	require.NoError(t, timerStore.Update(context.TODO(), timer.ID, &timerapi.TimerUpdate{
		ManualRequest: timerapi.NewOptionalVal(manual),
	}))
	testCheckFunc(check, "", "manual request failed to trigger, request not found")
	require.NoError(t, timerStore.Update(context.TODO(), timer.ID, &timerapi.TimerUpdate{
		ManualRequest: timerapi.NewOptionalVal(timerapi.ManualRequest{}),
	}))
	testCheckFunc(check, "", "manual request failed to trigger, request not found")

	// start trigger -> trigger not done but timeout
	check, manual = startTrigger(context.TODO(), "")
	manual.ManualRequestTime = time.Now().Add(-time.Minute)
	manual.ManualTimeout = 50 * time.Second
	require.NoError(t, timerStore.Update(context.TODO(), timer.ID, &timerapi.TimerUpdate{
		ManualRequest: timerapi.NewOptionalVal(manual),
	}))
	testCheckFunc(check, "", "manual request timeout")

	// disable ttl
	require.NoError(t, timerStore.Update(context.TODO(), timer.ID, &timerapi.TimerUpdate{
		ManualRequest: timerapi.NewOptionalVal(timerapi.ManualRequest{}),
	}))
	tk.MustExec("alter table tp1 ttl_enable='OFF'")
	_, physical = getPhysicalTableInfo(t, do, "test", "tp1", "p0")
	startTrigger(context.TODO(), "manual trigger is not allowed when timer is disabled")
	tk.MustExec("alter table tp1 ttl_enable='ON'")

	// start trigger -> timer deleted
	check, _ = startTrigger(context.TODO(), "")
	_, err = cli.DeleteTimer(context.TODO(), timer.ID)
	require.NoError(t, err)
	testCheckFunc(check, "", "timer not exist")

	// ctx timeout
	ctx, cancel := context.WithCancel(context.TODO())
	check, _ = startTrigger(ctx, "")
	cancel()
	testCheckFunc(check, "", ctx.Err().Error())
}

func TestTTLTimerSync(t *testing.T) {
	origFullRefreshTimerCounter := metrics.TTLFullRefreshTimersCounter
	origSyncTimerCounter := metrics.TTLSyncTimerCounter
	defer func() {
		metrics.TTLFullRefreshTimersCounter = origFullRefreshTimerCounter
		metrics.TTLSyncTimerCounter = origSyncTimerCounter
	}()

	store, do := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, do)

	fullRefreshTimersCounter := &mockutil.MetricsCounter{}
	metrics.TTLFullRefreshTimersCounter = fullRefreshTimersCounter
	syncTimerCounter := &mockutil.MetricsCounter{}
	metrics.TTLSyncTimerCounter = syncTimerCounter

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(tablestore.CreateTimerTableSQL("test", "test_timers"))
	timerStore := tablestore.NewTableTimerStore(1, do.SysSessionPool(), "test", "test_timers", nil)
	defer timerStore.Close()

	tk.MustExec("set @@global.tidb_ttl_job_enable=0")
	tk.MustExec("create table t0(t timestamp)")
	tk.MustExec("create table t1(t timestamp) TTL=`t`+interval 1 HOUR")
	tk.MustExec("create table t2(t timestamp) TTL=`t`+interval 1 HOUR ttl_job_interval='1m' ttl_enable='OFF'")
	tk.MustExec("create table t3(t timestamp, t2 timestamp) TTL=`t`+interval 1 HOUR ttl_job_interval='1h'")
	tk.MustExec("create table tp1(a int, t timestamp) TTL=`t`+interval 1 HOUR ttl_job_interval='3h' partition by range(a) (" +
		"partition p0 values less than (10)," +
		"partition p1 values less than (100)," +
		"partition p2 values less than (1000)" +
		")")
	var zeroTime time.Time
	wm1 := time.Unix(3600*24*12, 0)
	wm2 := time.Unix(3600*24*24, 0)
	insertTTLTableStatusWatermark(t, do, tk, "test", "t1", "", zeroTime, false)
	insertTTLTableStatusWatermark(t, do, tk, "test", "t2", "", wm1, false)
	insertTTLTableStatusWatermark(t, do, tk, "test", "tp1", "p0", zeroTime, false)
	insertTTLTableStatusWatermark(t, do, tk, "test", "tp1", "p1", wm2, true)

	cli := timerapi.NewDefaultTimerClient(timerStore)
	sync := ttlworker.NewTTLTimerSyncer(do.SysSessionPool(), cli)

	lastSyncTime, lastSyncVer := sync.GetLastSyncInfo()
	require.True(t, lastSyncTime.IsZero())
	require.Zero(t, lastSyncVer)

	// first sync
	now := time.Now()
	require.Equal(t, float64(0), fullRefreshTimersCounter.Val())
	require.Equal(t, float64(0), syncTimerCounter.Val())
	sync.SyncTimers(context.TODO(), do.InfoSchema())
	require.Equal(t, float64(1), fullRefreshTimersCounter.Val())
	require.Equal(t, float64(6), syncTimerCounter.Val())
	syncCnt := syncTimerCounter.Val()
	lastSyncTime, lastSyncVer = sync.GetLastSyncInfo()
	require.Equal(t, do.InfoSchema().SchemaMetaVersion(), lastSyncVer)
	require.GreaterOrEqual(t, lastSyncTime.Unix(), now.Unix())
	checkTimerCnt(t, cli, 6)
	timer1 := checkTimerWithTableMeta(t, do, cli, "test", "t1", "", zeroTime)
	timer2 := checkTimerWithTableMeta(t, do, cli, "test", "t2", "", wm1)
	timer3 := checkTimerWithTableMeta(t, do, cli, "test", "t3", "", zeroTime)
	timerP10 := checkTimerWithTableMeta(t, do, cli, "test", "tp1", "p0", zeroTime)
	timerP11 := checkTimerWithTableMeta(t, do, cli, "test", "tp1", "p1", wm2)
	timerP12 := checkTimerWithTableMeta(t, do, cli, "test", "tp1", "p2", zeroTime)

	// create table/partition
	tk.MustExec("create table t4(t timestamp) TTL=`t`+interval 1 HOUR ttl_job_interval='2h'")
	tk.MustExec("create table t5(t timestamp)")
	tk.MustExec("alter table tp1 add partition (partition p3 values less than(10000))")
	tk.MustExec("create table tp2(a int, t timestamp) TTL=`t`+interval 1 HOUR ttl_job_interval='6h' partition by range(a) (" +
		"partition p0 values less than (10)," +
		"partition p1 values less than (100)" +
		")")
	now = time.Now()
	sync.SyncTimers(context.TODO(), do.InfoSchema())
	require.Equal(t, syncCnt+4, syncTimerCounter.Val())
	syncCnt = syncTimerCounter.Val()
	lastSyncTime, lastSyncVer = sync.GetLastSyncInfo()
	require.Equal(t, do.InfoSchema().SchemaMetaVersion(), lastSyncVer)
	require.GreaterOrEqual(t, lastSyncTime.Unix(), now.Unix())
	checkTimerCnt(t, cli, 10)
	timer4 := checkTimerWithTableMeta(t, do, cli, "test", "t4", "", zeroTime)
	checkTimerWithTableMeta(t, do, cli, "test", "tp1", "p3", zeroTime)
	timerP20 := checkTimerWithTableMeta(t, do, cli, "test", "tp2", "p0", zeroTime)
	timerP21 := checkTimerWithTableMeta(t, do, cli, "test", "tp2", "p1", zeroTime)
	checkTimersNotChange(t, cli, timer1, timer2, timer3, timerP10, timerP11, timerP12)

	// update table
	tk.MustExec("alter table t1 ttl_enable='OFF'")
	tk.MustExec("alter table t2 ttl_job_interval='6m'")
	tk.MustExec("alter table t3 TTL=`t2`+interval 2 HOUR")
	tk.MustExec("alter table t5 TTL=`t`+interval 10 HOUR ttl_enable='OFF'")
	tk.MustExec("alter table tp1 ttl_job_interval='3m'")
	sync.SyncTimers(context.TODO(), do.InfoSchema())
	require.Equal(t, syncCnt+7, syncTimerCounter.Val())
	syncCnt = syncTimerCounter.Val()
	checkTimerCnt(t, cli, 11)
	checkTimerWithTableMeta(t, do, cli, "test", "t1", "", zeroTime)
	timer2 = checkTimerWithTableMeta(t, do, cli, "test", "t2", "", wm1)
	timer5 := checkTimerWithTableMeta(t, do, cli, "test", "t5", "", zeroTime)
	timerP10 = checkTimerWithTableMeta(t, do, cli, "test", "tp1", "p0", zeroTime)
	timerP11 = checkTimerWithTableMeta(t, do, cli, "test", "tp1", "p1", wm2)
	timerP12 = checkTimerWithTableMeta(t, do, cli, "test", "tp1", "p2", zeroTime)
	timerP13 := checkTimerWithTableMeta(t, do, cli, "test", "tp1", "p3", zeroTime)
	checkTimersNotChange(t, cli, timer3, timer4, timerP20, timerP21)

	// rename table
	tk.MustExec("rename table t1 to t1a")
	sync.SyncTimers(context.TODO(), do.InfoSchema())
	require.Equal(t, syncCnt+1, syncTimerCounter.Val())
	syncCnt = syncTimerCounter.Val()
	checkTimerCnt(t, cli, 11)
	timer1 = checkTimerWithTableMeta(t, do, cli, "test", "t1a", "", zeroTime)
	checkTimersNotChange(t, cli, timer1, timer2, timer3, timer4, timer5, timerP10, timerP11, timerP12, timerP13, timerP20, timerP21)

	// truncate table/partition
	oldTimer2 := timer2
	oldTimerP11 := timerP11
	oldTimerP20 := timerP20
	oldTimerP21 := timerP21
	tk.MustExec("truncate table t2")
	tk.MustExec("alter table tp1 truncate partition p1")
	tk.MustExec("truncate table tp2")
	sync.SyncTimers(context.TODO(), do.InfoSchema())
	require.Equal(t, syncCnt+7, syncTimerCounter.Val())
	syncCnt = syncTimerCounter.Val()
	checkTimerCnt(t, cli, 15)
	timer2 = checkTimerWithTableMeta(t, do, cli, "test", "t2", "", zeroTime)
	require.NotEqual(t, oldTimer2.ID, timer2.ID)
	timerP11 = checkTimerWithTableMeta(t, do, cli, "test", "tp1", "p1", zeroTime)
	require.NotEqual(t, oldTimerP11.ID, timerP11.ID)
	timerP20 = checkTimerWithTableMeta(t, do, cli, "test", "tp2", "p0", zeroTime)
	require.NotEqual(t, oldTimerP20.ID, timerP20.ID)
	timerP21 = checkTimerWithTableMeta(t, do, cli, "test", "tp2", "p1", zeroTime)
	require.NotEqual(t, oldTimerP21.ID, timerP21.ID)
	oldTimer2 = checkTimerOnlyDisabled(t, cli, oldTimer2)
	oldTimerP11 = checkTimerOnlyDisabled(t, cli, oldTimerP11)
	oldTimerP20 = checkTimerOnlyDisabled(t, cli, oldTimerP20)
	oldTimerP21 = checkTimerOnlyDisabled(t, cli, oldTimerP21)
	checkTimersNotChange(t, cli, timer1, timer3, timer4, timer5, timerP10, timerP12, timerP13)

	// drop table/partition
	tk.MustExec("drop table t1a")
	tk.MustExec("alter table tp1 drop partition p3")
	tk.MustExec("drop table tp2")
	sync.SyncTimers(context.TODO(), do.InfoSchema())
	require.Equal(t, syncCnt+3, syncTimerCounter.Val())
	syncCnt = syncTimerCounter.Val()
	checkTimerCnt(t, cli, 15)
	checkTimerOnlyDisabled(t, cli, timer1)
	checkTimerOnlyDisabled(t, cli, timerP13)
	checkTimerOnlyDisabled(t, cli, timerP20)
	checkTimerOnlyDisabled(t, cli, timerP21)
	checkTimersNotChange(t, cli, oldTimer2, oldTimerP11, oldTimerP20, oldTimerP21)
	checkTimersNotChange(t, cli, timer2, timer3, timer4, timer5, timerP10, timerP11, timerP12)

	// clear deleted tables
	sync.SetDelayDeleteInterval(time.Millisecond)
	time.Sleep(time.Second)
	sync.SyncTimers(context.TODO(), do.InfoSchema())
	require.Equal(t, syncCnt+8, syncTimerCounter.Val())
	syncCnt = syncTimerCounter.Val()
	checkTimerCnt(t, cli, 7)
	checkTimersNotChange(t, cli, timer2, timer3, timer4, timer5, timerP10, timerP11, timerP12)

	// reset timers
	sync.Reset()
	lastSyncTime, lastSyncVer = sync.GetLastSyncInfo()
	require.True(t, lastSyncTime.IsZero())
	require.Zero(t, lastSyncVer)

	// sync after reset
	now = time.Now()
	require.Equal(t, float64(1), fullRefreshTimersCounter.Val())
	sync.SyncTimers(context.TODO(), do.InfoSchema())
	require.Equal(t, float64(2), fullRefreshTimersCounter.Val())
	require.Equal(t, syncCnt, syncTimerCounter.Val())
	lastSyncTime, lastSyncVer = sync.GetLastSyncInfo()
	require.Equal(t, do.InfoSchema().SchemaMetaVersion(), lastSyncVer)
	require.GreaterOrEqual(t, lastSyncTime.Unix(), now.Unix())
	checkTimerCnt(t, cli, 7)
	checkTimersNotChange(t, cli, timer2, timer3, timer4, timer5, timerP10, timerP11, timerP12)
}

func insertTTLTableStatusWatermark(t *testing.T, do *domain.Domain, tk *testkit.TestKit, db, table, partition string, watermark time.Time, jobRunning bool) {
	_, physical := getPhysicalTableInfo(t, do, db, table, partition)
	if watermark.IsZero() {
		tk.MustExec("insert into mysql.tidb_ttl_table_status (table_id, parent_table_id) values (?, ?)", physical.ID, physical.TableInfo.ID)
		return
	}

	if jobRunning {
		tk.MustExec(
			"insert into mysql.tidb_ttl_table_status (table_id, parent_table_id, last_job_id, last_job_start_time, last_job_finish_time, last_job_ttl_expire, current_job_id, current_job_start_time) values(?, ?, ?, FROM_UNIXTIME(?), FROM_UNIXTIME(?), FROM_UNIXTIME(?), ?, FROM_UNIXTIME(?))",
			physical.ID, physical.TableInfo.ID, uuid.NewString(), watermark.Add(-10*time.Minute).Unix(), watermark.Add(-time.Minute).Unix(), watermark.Add(-20*time.Minute).Unix(),
			uuid.NewString(),
			watermark.Unix(),
		)
	} else {
		tk.MustExec(
			"insert into mysql.tidb_ttl_table_status (table_id, parent_table_id, last_job_id, last_job_start_time, last_job_finish_time, last_job_ttl_expire) values(?, ?, ?, FROM_UNIXTIME(?), FROM_UNIXTIME(?), FROM_UNIXTIME(?))",
			physical.ID, physical.TableInfo.ID, uuid.NewString(), watermark.Unix(), watermark.Add(time.Minute).Unix(), watermark.Add(-time.Minute).Unix(),
		)
	}
}

func checkTimerCnt(t *testing.T, cli timerapi.TimerClient, cnt int) {
	timers, err := cli.GetTimers(context.TODO())
	require.NoError(t, err)
	require.Equal(t, cnt, len(timers))
}

func checkTimerOnlyDisabled(t *testing.T, cli timerapi.TimerClient, timer *timerapi.TimerRecord) *timerapi.TimerRecord {
	tm, err := cli.GetTimerByID(context.TODO(), timer.ID)
	require.NoError(t, err)
	if !timer.Enable {
		require.Equal(t, *timer, *tm)
		return timer
	}

	require.False(t, tm.Enable)
	require.Greater(t, tm.Version, timer.Version)
	tm2 := timer.Clone()
	tm2.Enable = tm.Enable
	tm2.Version = tm.Version
	require.Equal(t, *tm, *tm2)
	return tm
}

func checkTimersNotChange(t *testing.T, cli timerapi.TimerClient, timers ...*timerapi.TimerRecord) {
	for i, timer := range timers {
		tm, err := cli.GetTimerByID(context.TODO(), timer.ID)
		require.NoError(t, err)
		require.Equal(t, *timer, *tm, fmt.Sprintf("index: %d", i))
	}
}

func getPhysicalTableInfo(t *testing.T, do *domain.Domain, db, table, partition string) (string, *cache.PhysicalTable) {
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr(db), model.NewCIStr(table))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	physical, err := cache.NewPhysicalTable(model.NewCIStr(db), tblInfo, model.NewCIStr(partition))
	require.NoError(t, err)
	return fmt.Sprintf("/tidb/ttl/physical_table/%d/%d", tblInfo.ID, physical.ID), physical
}

func checkTimerWithTableMeta(t *testing.T, do *domain.Domain, cli timerapi.TimerClient, db, table, partition string, watermark time.Time) *timerapi.TimerRecord {
	is := do.InfoSchema()
	dbInfo, ok := is.SchemaByName(model.NewCIStr(db))
	require.True(t, ok)

	key, physical := getPhysicalTableInfo(t, do, db, table, partition)
	timer, err := cli.GetTimerByKey(context.TODO(), key)
	require.NoError(t, err)

	require.Equal(t, physical.TTLInfo.Enable, timer.Enable)
	require.Equal(t, timerapi.SchedEventInterval, timer.SchedPolicyType)
	require.Equal(t, physical.TTLInfo.JobInterval, timer.SchedPolicyExpr)
	if partition == "" {
		require.Equal(t, []string{
			fmt.Sprintf("db=%s", dbInfo.Name.O),
			fmt.Sprintf("table=%s", physical.Name.O),
		}, timer.Tags)
	} else {
		require.Equal(t, []string{
			fmt.Sprintf("db=%s", dbInfo.Name.O),
			fmt.Sprintf("table=%s", physical.Name.O),
			fmt.Sprintf("partition=%s", physical.Partition.O),
		}, timer.Tags)
	}

	require.NotNil(t, timer.Data)
	var timerData ttlworker.TTLTimerData
	require.NoError(t, json.Unmarshal(timer.Data, &timerData))
	require.Equal(t, physical.TableInfo.ID, timerData.TableID)
	require.Equal(t, physical.ID, timerData.PhysicalID)
	require.Equal(t, watermark.Unix(), timer.Watermark.Unix())
	return timer
}
