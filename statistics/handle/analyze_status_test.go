// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package handle_test

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
)

func equalJobStatus(j1, j2 *statistics.AnalyzeJob) bool {
	return j1.UUID == j2.UUID && j1.DBName == j2.DBName && j1.TableName == j2.TableName &&
		j1.PartitionName == j2.PartitionName && j1.JobInfo == j2.JobInfo &&
		j1.RowCount == j2.RowCount && j1.State == j2.State &&
		j1.StartTime.Format(types.TimeFSPFormat) == j2.StartTime.Format(types.TimeFSPFormat)
}

func (s *testStatsSuite) TestAnalyzeJobsStatus(c *C) {
	defer cleanEnv(c, s.store, s.do)

	h := s.do.StatsHandle()
	st1 := time.Unix(time.Now().Unix(), 0)
	st2 := st1.Add(-time.Second * 10)
	oriJobs := []*statistics.AnalyzeJob{
		{
			UUID:          "1",
			DBName:        "db",
			TableName:     "t",
			PartitionName: "par1",
			JobInfo:       "info1",
			RowCount:      1,
			StartTime:     st1,
			State:         "s1",
		},
		{
			UUID:          "2",
			DBName:        "db",
			TableName:     "t",
			PartitionName: "par2",
			JobInfo:       "info2",
			RowCount:      2,
			StartTime:     st2,
			State:         "s2",
		},
	}
	c.Assert(h.UpdateAnalyzeJobsStatus(oriJobs, 100), IsNil)

	jobs, err := h.GetAnalyzeJobsStatus()
	c.Assert(err, IsNil)
	c.Assert(len(jobs), Equals, 2)
	c.Assert(equalJobStatus(jobs[0], oriJobs[0]), IsTrue)
	c.Assert(equalJobStatus(jobs[1], oriJobs[1]), IsTrue)

	// update the first job
	oriJobs[0].JobInfo = "123123"
	c.Assert(h.UpdateAnalyzeJobsStatus([]*statistics.AnalyzeJob{oriJobs[0]}, 100), IsNil)
	jobs, err = h.GetAnalyzeJobsStatus()
	c.Assert(err, IsNil)
	c.Assert(len(jobs), Equals, 2)
	c.Assert(equalJobStatus(jobs[0], oriJobs[0]), IsTrue)
	c.Assert(equalJobStatus(jobs[1], oriJobs[1]), IsTrue)

	// remove stale jobs
	c.Assert(h.UpdateAnalyzeJobsStatus(nil, 5), IsNil)
	jobs, err = h.GetAnalyzeJobsStatus()
	c.Assert(err, IsNil)
	c.Assert(len(jobs), Equals, 1)
	c.Assert(equalJobStatus(jobs[0], oriJobs[0]), IsTrue)
}
