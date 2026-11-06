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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestDDLJobElapsedTimeCollection(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	dom.DDL().OwnerManager().CampaignCancel()
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	update := ddl.NewJobElapsedTimeUpdaterForTest(session.NewSessionPool(dom.SysSessionPool()))
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(metrics.DDLUnfinishedJobMaxElapsed))
	t.Cleanup(metrics.DDLUnfinishedJobMaxElapsed.Reset)
	values := func() map[string]float64 {
		families, err := registry.Gather()
		require.NoError(t, err)
		res := make(map[string]float64)
		for _, family := range families {
			for _, metric := range family.Metric {
				require.Len(t, metric.Label, 1)
				res[metric.Label[0].GetValue()] = metric.GetGauge().GetValue()
			}
		}
		return res
	}
	now := time.Unix(1700000000, 0)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockDDLJobElapsedTimeNow", func(current *time.Time) {
		*current = now
	})
	// Force a second page. Older, untracked jobs must not dominate the maximum.
	for id := int64(1); id <= 100; id++ {
		insertMockJob2Table(tk, &model.Job{ID: id, Type: model.ActionCreateTable, State: model.JobStateRunning,
			StartTS: oracle.GoTimeToTS(now.Add(-time.Hour))})
	}
	job := &model.Job{ID: 101, Type: model.ActionAddIndex, State: model.JobStateRunning,
		StartTS:     oracle.GoTimeToTS(now.Add(-4 * time.Minute)),
		ElapsedTime: &model.JobElapsedTime{PausedDurationMS: (3 * time.Minute).Milliseconds()}}
	insertMockJob2Table(tk, job)
	pausing := &model.Job{ID: 102, Type: model.ActionAddIndex, State: model.JobStatePausing,
		StartTS: oracle.GoTimeToTS(now.Add(-2 * time.Minute)), ElapsedTime: &model.JobElapsedTime{}}
	insertMockJob2Table(tk, pausing)
	insertMockJob2Table(tk, &model.Job{ID: 103, Type: model.ActionDropTable, State: model.JobStateDone,
		StartTS: oracle.GoTimeToTS(now.Add(-3 * time.Minute)), ElapsedTime: &model.JobElapsedTime{}})
	require.NoError(t, update(ctx))
	got := values()
	require.Len(t, got, 2)
	require.Equal(t, 120.0, got[model.ActionAddIndex.String()])
	require.Equal(t, 180.0, got[model.ActionDropTable.String()])

	// A failed later page must not publish the successfully read first page.
	// Advance beyond the former five-second tolerance without waiting on wall time.
	now = now.Add(6 * time.Second)
	tk.MustExec("insert into mysql.tidb_ddl_job(job_id, job_meta) values (104, 'invalid json')")
	require.Error(t, update(ctx))
	require.Equal(t, got, values())
	deleteJobMetaByID(tk, 104)
	deleteJobMetaByID(tk, 102)
	require.NoError(t, update(ctx))
	require.Equal(t, 66.0, values()[model.ActionAddIndex.String()])

	// A replacement owner reconstructs pause deductions from durable metadata.
	update = ddl.NewJobElapsedTimeUpdaterForTest(session.NewSessionPool(dom.SysSessionPool()))
	require.NoError(t, update(ctx))
	require.Equal(t, 66.0, values()[model.ActionAddIndex.String()])
	tk.MustExec("delete from mysql.tidb_ddl_job where job_id = 101")
	job.State = model.JobStatePaused
	insertMockJob2Table(tk, job)
	require.NoError(t, update(ctx))
	require.NotContains(t, values(), model.ActionAddIndex.String())
	deleteJobMetaByID(tk, 103)
	require.NoError(t, update(ctx))
	require.Empty(t, values())

	ctx, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, update(ctx))
}

func TestDDLJobPauseTimePersistence(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	dom.DDL().OwnerManager().CampaignCancel()
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	for _, action := range []string{"resume", "cancel", "resume all", "legacy"} {
		t.Run(action, func(t *testing.T) {
			job := &model.Job{ID: 1, Type: model.ActionAddIndex, State: model.JobStatePaused,
				AdminOperator: model.AdminCommandByEndUser,
				ElapsedTime:   &model.JobElapsedTime{PauseStartTS: oracle.GoTimeToTS(time.Now().Add(-time.Minute))}}
			if action == "resume all" {
				job.AdminOperator = model.AdminCommandBySystem
			}
			if action == "legacy" {
				job.ElapsedTime = nil
			}
			insertMockJob2Table(tk, job)
			defer deleteJobMetaByID(tk, job.ID)
			if action == "resume" {
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockCommitFailedOnDDLCommand", `return(true)`)
				_, err := ddl.ResumeJobs(ctx, tk.Session(), []int64{job.ID})
				require.Error(t, err)
				require.Equal(t, job.ElapsedTime, getJobMetaByID(t, tk, job.ID).ElapsedTime)
				testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockCommitFailedOnDDLCommand")
			}
			switch action {
			case "cancel":
				errs, err := ddl.CancelJobs(ctx, tk.Session(), []int64{job.ID})
				require.NoError(t, err)
				require.NoError(t, errs[0])
			case "resume all":
				errs, err := ddl.ResumeAllJobsBySystem(tk.Session())
				require.NoError(t, err)
				require.Empty(t, errs)
			default:
				errs, err := ddl.ResumeJobs(ctx, tk.Session(), []int64{job.ID})
				require.NoError(t, err)
				require.NoError(t, errs[0])
			}
			stored := getJobMetaByID(t, tk, job.ID)
			if action == "legacy" {
				require.Nil(t, stored.ElapsedTime)
			} else {
				require.Zero(t, stored.ElapsedTime.PauseStartTS)
				require.InDelta(t, time.Minute.Milliseconds(), stored.ElapsedTime.PausedDurationMS, 5000)
			}
		})
	}
}
