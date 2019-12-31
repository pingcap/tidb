// Copyright 2019 PingCAP, Inc.
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

package statistics

import (
	. "github.com/pingcap/check"
)

var _ = SerialSuites(&testStatisticsSerialSuite{})

type testStatisticsSerialSuite struct{}

func (s *testStatisticsSerialSuite) TestMoveToHistory(c *C) {
	ClearHistoryJobs()
	numJobs := numMaxHistoryJobs*2 + 1
	jobs := make([]*AnalyzeJob, 0, numJobs)
	for i := 0; i < numJobs; i++ {
		job := &AnalyzeJob{}
		AddNewAnalyzeJob(job)
		jobs = append(jobs, job)
	}
	MoveToHistory(jobs[0])
	c.Assert(len(GetAllAnalyzeJobs()), Equals, numJobs)
	for i := 1; i < numJobs; i++ {
		MoveToHistory(jobs[i])
	}
	c.Assert(len(GetAllAnalyzeJobs()), Equals, numMaxHistoryJobs)
	ClearHistoryJobs()
	c.Assert(len(GetAllAnalyzeJobs()), Equals, 0)
}
