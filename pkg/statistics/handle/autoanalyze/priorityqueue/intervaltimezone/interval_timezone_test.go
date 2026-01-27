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

package interval_timezone_test

import (
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestLastFailedAnalysisDurationUseCorrectTimezone verifies that
// GetLastFailedAnalysisDuration uses the correct time zone when calculating durations.
// Do not add other test cases to this file; it exists solely to cover time zone contamination.
// Additional tests here could be affected by the global system time zone.
func TestLastFailedAnalysisDurationUseCorrectTimezone(t *testing.T) {
	// Force the system time zone to America/New_York (UTC-4) to simulate contamination.
	// This must happen before bootstrap because SetSystemTZ is guarded by sync.Once.
	timeutil.SetSystemTZ("America/New_York")
	// Make sure golang's time.Local also uses the contaminated time zone.
	time.Local, _ = time.LoadLocation("America/New_York")

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	// Set the global time zone so sessions should follow it.
	tk.MustExec("set @@global.time_zone='Europe/Berlin';")

	h := dom.StatsHandle()
	pool := h.SPool()

	// Step 1: Insert a pending job using the StatsHandle API.
	job := &statistics.AnalyzeJob{
		DBName:    "db_reset",
		TableName: "tbl_reset",
		JobInfo:   "test job",
	}
	require.NoError(t, h.InsertAnalyzeJob(job, "test-instance", 1))
	require.NotNil(t, job.ID)

	// Step 2: Start the job.
	h.StartAnalyzeJob(job)

	time.Sleep(2 * time.Second) // Ensure some time passes.

	// Step 3: Mark the job as failed.
	h.FinishAnalyzeJob(job, errors.New("test error"), statistics.TableAnalysisJob)

	// Step 4: Query via the stats session pool.
	// The session should reset its time zone to the global value (Europe/Berlin).
	// If it keeps the contaminated system time zone, the duration can be skewed.
	var dur time.Duration
	err := statsutil.CallWithSCtx(pool, func(sctx sessionctx.Context) error {
		var err error
		dur, err = priorityqueue.GetLastFailedAnalysisDuration(sctx, "db_reset", "tbl_reset")
		return err
	})
	require.NoError(t, err)
	// When the time zone is correctly reset, the duration should be positive and small
	// (we just inserted the job).
	require.Greater(t, dur, time.Duration(0), "duration should be positive; negative means timezone was not reset")
	require.Less(t, dur, time.Minute, "duration should be less than an hour")
}
