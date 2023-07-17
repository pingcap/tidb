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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	timerapi "github.com/pingcap/tidb/timer/api"
	"github.com/pingcap/tidb/timer/tablestore"
	"github.com/pingcap/tidb/ttl/ttlworker"
	"github.com/stretchr/testify/require"
)

func TestTTLTimerSync(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)

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
	insertTTLTableStatusWatermark(t, do, tk, "test", "t1", "", zeroTime)
	insertTTLTableStatusWatermark(t, do, tk, "test", "t2", "", wm1)
	insertTTLTableStatusWatermark(t, do, tk, "test", "tp1", "p0", zeroTime)
	insertTTLTableStatusWatermark(t, do, tk, "test", "tp1", "p1", wm2)

	cli := timerapi.NewDefaultTimerClient(timerStore)
	sync := ttlworker.NewTTLTimerSyncer(do.SysSessionPool(), cli)

	// first sync
	sync.SyncTimers(context.TODO(), do.InfoSchema())
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
	sync.SyncTimers(context.TODO(), do.InfoSchema())
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
	checkTimerCnt(t, cli, 15)
	timer2 = checkTimerWithTableMeta(t, do, cli, "test", "t2", "", zeroTime)
	require.NotEqual(t, oldTimer2.ID, timer2.ID)
	timerP11 = checkTimerWithTableMeta(t, do, cli, "test", "tp1", "p1", zeroTime)
	require.NotEqual(t, oldTimerP11.ID, timerP11.ID)
	timerP20 = checkTimerWithTableMeta(t, do, cli, "test", "tp2", "p0", zeroTime)
	require.NotEqual(t, oldTimerP20.ID, timerP20.ID)
	timerP21 = checkTimerWithTableMeta(t, do, cli, "test", "tp2", "p1", zeroTime)
	require.NotEqual(t, oldTimerP21.ID, timerP21.ID)
	checkTimersNotChange(t, cli, timer1, oldTimer2, timer3, timer4, timer5, timerP10, oldTimerP11, timerP12, timerP13, oldTimerP20, oldTimerP21)

	// drop table/partition
	tk.MustExec("drop table t1a")
	tk.MustExec("alter table tp1 drop partition p3")
	tk.MustExec("drop table tp2")
	sync.SyncTimers(context.TODO(), do.InfoSchema())
	checkTimerCnt(t, cli, 15)
	checkTimersNotChange(t, cli, oldTimer2, oldTimerP11, oldTimerP20, oldTimerP21)
	checkTimersNotChange(t, cli, timer1, timer2, timer3, timer4, timer5, timerP10, timerP11, timerP12, timerP13, timerP20, timerP21)

	// clear deleted tables
	sync.SetDelayDeleteInterval(time.Millisecond)
	time.Sleep(time.Second)
	sync.SyncTimers(context.TODO(), do.InfoSchema())
	checkTimerCnt(t, cli, 7)
	checkTimersNotChange(t, cli, timer2, timer3, timer4, timer5, timerP10, timerP11, timerP12)
}

func insertTTLTableStatusWatermark(t *testing.T, do *domain.Domain, tk *testkit.TestKit, db, table, partition string, watermark time.Time) {
	tbl, err := do.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	physicalID := tblInfo.ID
	var par model.PartitionDefinition
	if partition != "" {
		for _, def := range tblInfo.Partition.Definitions {
			if def.Name.L == model.NewCIStr(partition).L {
				par = def
			}
		}
		require.NotNil(t, par)
		physicalID = par.ID
	}

	if watermark.IsZero() {
		tk.MustExec("insert into mysql.tidb_ttl_table_status (table_id, parent_table_id) values (?, ?)", physicalID, tblInfo.ID)
		return
	}

	tk.MustExec(
		"insert into mysql.tidb_ttl_table_status (table_id, parent_table_id, last_job_id, last_job_start_time, last_job_finish_time, last_job_ttl_expire) values(?, ?, ?, FROM_UNIXTIME(?), FROM_UNIXTIME(?), FROM_UNIXTIME(?))",
		physicalID, tblInfo.ID, uuid.NewString(), watermark.Unix(), watermark.Add(time.Minute).Unix(), watermark.Add(-time.Minute).Unix(),
	)
}

func checkTimerCnt(t *testing.T, cli timerapi.TimerClient, cnt int) {
	timers, err := cli.GetTimers(context.TODO())
	require.NoError(t, err)
	require.Equal(t, cnt, len(timers))
}

func checkTimersNotChange(t *testing.T, cli timerapi.TimerClient, timers ...*timerapi.TimerRecord) {
	for i, timer := range timers {
		tm, err := cli.GetTimerByID(context.TODO(), timer.ID)
		require.NoError(t, err)
		require.Equal(t, *timer, *tm, fmt.Sprintf("index: %d", i))
	}
}

func checkTimerWithTableMeta(t *testing.T, do *domain.Domain, cli timerapi.TimerClient, db, table, partition string, watermark time.Time) *timerapi.TimerRecord {
	is := do.InfoSchema()
	dbInfo, ok := is.SchemaByName(model.NewCIStr(db))
	require.True(t, ok)
	tbl, err := is.TableByName(model.NewCIStr(db), model.NewCIStr(table))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	physicalID := tblInfo.ID
	var par model.PartitionDefinition
	if partition != "" {
		for _, def := range tblInfo.Partition.Definitions {
			if def.Name.L == model.NewCIStr(partition).L {
				par = def
			}
		}
		require.NotNil(t, par)
		physicalID = par.ID
	}

	key := fmt.Sprintf("/tidb/ttl/physical_table/%d/%d", tblInfo.ID, physicalID)
	timer, err := cli.GetTimerByKey(context.TODO(), key)
	require.NoError(t, err)

	require.Equal(t, tblInfo.TTLInfo.Enable, timer.Enable)
	require.Equal(t, timerapi.SchedEventInterval, timer.SchedPolicyType)
	require.Equal(t, tblInfo.TTLInfo.JobInterval, timer.SchedPolicyExpr)
	if partition == "" {
		require.Equal(t, []string{
			fmt.Sprintf("db=%s", dbInfo.Name.O),
			fmt.Sprintf("table=%s", tblInfo.Name.O),
		}, timer.Tags)
	} else {
		require.Equal(t, []string{
			fmt.Sprintf("db=%s", dbInfo.Name.O),
			fmt.Sprintf("table=%s", tblInfo.Name.O),
			fmt.Sprintf("partition=%s", par.Name.O),
		}, timer.Tags)
	}

	require.NotNil(t, timer.Data)
	var timerData ttlworker.TTLTimerData
	require.NoError(t, json.Unmarshal(timer.Data, &timerData))
	require.Equal(t, tblInfo.ID, timerData.TableID)
	require.Equal(t, physicalID, timerData.PhysicalID)
	require.Equal(t, watermark.Unix(), timer.Watermark.Unix())
	return timer
}
