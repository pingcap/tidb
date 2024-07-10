// Copyright 2022 PingCAP, Inc.
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

package cache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/stretchr/testify/assert"
)

func TestTTLStatusCache(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	defer sv.Close()

	conn := server.CreateMockConn(t, sv)
	sctx := conn.Context().Session
	tk := testkit.NewTestKitWithSession(t, store, sctx)
	ttlSession := session.NewSession(sctx, tk.Session(), func(_ session.Session) {})

	isc := cache.NewTableStatusCache(time.Hour)

	// test should update
	assert.True(t, isc.ShouldUpdate())
	assert.NoError(t, isc.Update(context.Background(), ttlSession))
	assert.False(t, isc.ShouldUpdate())

	// test new entries are synced
	tk.MustExec("insert into mysql.tidb_ttl_table_status(table_id, parent_table_id) values (1, 2)")
	assert.NoError(t, isc.Update(context.Background(), ttlSession))
	assert.Equal(t, 1, len(isc.Tables))
	tk.MustExec("delete from mysql.tidb_ttl_table_status where table_id = 1")
	assert.NoError(t, isc.Update(context.Background(), ttlSession))
	assert.Equal(t, 0, len(isc.Tables))

	timeZone := tk.Session().GetSessionVars().TimeZone

	// test every field of tidb_ttl_table_status can be extracted well
	testCases := []struct {
		columnName string
		sqlLiteral string
		assert     func(table *cache.TableStatus)
	}{
		{
			"parent_table_id",
			"2",
			func(table *cache.TableStatus) { assert.Equal(t, int64(2), table.ParentTableID) },
		},
		{
			"table_statistics",
			"'test str'",
			func(table *cache.TableStatus) { assert.Equal(t, "test str", table.TableStatistics) },
		},
		{
			"last_job_id",
			"'test job id'",
			func(table *cache.TableStatus) { assert.Equal(t, "test job id", table.LastJobID) },
		},
		{
			"last_job_start_time",
			"'2022-12-01 16:49:01'",
			func(table *cache.TableStatus) {
				expectedTime, err := time.ParseInLocation(time.DateTime, "2022-12-01 16:49:01", timeZone)
				assert.NoError(t, err)
				assert.Equal(t, expectedTime, table.LastJobStartTime)
			},
		},
		{
			"last_job_finish_time",
			"'2022-12-01 16:50:01'",
			func(table *cache.TableStatus) {
				expectedTime, err := time.ParseInLocation(time.DateTime, "2022-12-01 16:50:01", timeZone)
				assert.NoError(t, err)
				assert.Equal(t, expectedTime, table.LastJobFinishTime)
			},
		},
		{
			"last_job_ttl_expire",
			"'2022-12-01 16:51:01'",
			func(table *cache.TableStatus) {
				expectedTime, err := time.ParseInLocation(time.DateTime, "2022-12-01 16:51:01", timeZone)
				assert.NoError(t, err)
				assert.Equal(t, expectedTime, table.LastJobTTLExpire)
			},
		},
		{
			"last_job_summary",
			"'test summary'",
			func(table *cache.TableStatus) { assert.Equal(t, "test summary", table.LastJobSummary) },
		},
		{
			"current_job_id",
			"'test current job id'",
			func(table *cache.TableStatus) { assert.Equal(t, "test current job id", table.CurrentJobID) },
		},
		{
			"current_job_owner_id",
			"'test current job owner id'",
			func(table *cache.TableStatus) { assert.Equal(t, "test current job owner id", table.CurrentJobOwnerID) },
		},
		{
			"current_job_owner_hb_time",
			"'2022-12-01 16:52:01'",
			func(table *cache.TableStatus) {
				expectedTime, err := time.ParseInLocation(time.DateTime, "2022-12-01 16:52:01", timeZone)
				assert.NoError(t, err)
				assert.Equal(t, expectedTime, table.CurrentJobOwnerHBTime)
			},
		},
		{
			"current_job_start_time",
			"'2022-12-01 16:53:01'",
			func(table *cache.TableStatus) {
				expectedTime, err := time.ParseInLocation(time.DateTime, "2022-12-01 16:53:01", timeZone)
				assert.NoError(t, err)
				assert.Equal(t, expectedTime, table.CurrentJobStartTime)
			},
		},
		{
			"current_job_ttl_expire",
			"'2022-12-01 16:54:01'",
			func(table *cache.TableStatus) {
				expectedTime, err := time.ParseInLocation(time.DateTime, "2022-12-01 16:54:01", timeZone)
				assert.NoError(t, err)
				assert.Equal(t, expectedTime, table.CurrentJobTTLExpire)
			},
		},
		{
			"current_job_state",
			"'test state'",
			func(table *cache.TableStatus) { assert.Equal(t, "test state", table.CurrentJobState) },
		},
		{
			"current_job_status",
			"'test status'",
			func(table *cache.TableStatus) {
				assert.Equal(t, cache.JobStatus("test status"), table.CurrentJobStatus)
			},
		},
		{
			"current_job_status_update_time",
			"'2022-12-01 16:55:01'",
			func(table *cache.TableStatus) {
				expectedTime, err := time.ParseInLocation(time.DateTime, "2022-12-01 16:55:01", timeZone)
				assert.NoError(t, err)
				assert.Equal(t, expectedTime, table.CurrentJobStatusUpdateTime)
			},
		},
	}
	for index, testCase := range testCases {
		t.Run(testCase.columnName, func(t *testing.T) {
			sql := fmt.Sprintf(`insert into mysql.tidb_ttl_table_status (table_id, %s) values (%d, %s)`,
				testCase.columnName, index, testCase.sqlLiteral)

			tk.MustExec(sql)
			assert.NoError(t, isc.Update(context.Background(), ttlSession))
			assert.Equal(t, index+1, len(isc.Tables))
			testCase.assert(isc.Tables[int64(index)])
		})
	}
}
