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

package ttlworker_test

import (
	"context"
	"testing"

	dbsession "github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/ttl/ttlworker"
	"github.com/stretchr/testify/require"
)

func TestChangeStatus(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	dbSession, err := dbsession.CreateSession4Test(store)
	require.NoError(t, err)
	se := session.NewSession(dbSession, dbSession, nil)

	job := ttlworker.NewTTLJob(&cache.PhysicalTable{ID: 0}, "0", cache.JobStatusWaiting)
	tk.MustExec("insert into mysql.tidb_ttl_table_status(table_id,current_job_id,current_job_status) VALUES(0, '0', 'waiting')")
	require.NoError(t, job.ChangeStatus(context.Background(), se, cache.JobStatusRunning))
	tk.MustQuery("select current_job_status from mysql.tidb_ttl_table_status").Check(testkit.Rows("running"))
}
