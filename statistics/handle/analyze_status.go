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

package handle

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// UpdateAnalyzeJobsStatus updates analyze jobs status into mysql.analyze_jobs_status
func (h *Handle) UpdateAnalyzeJobsStatus(changedJobs []statistics.AnalyzeJob, staleIntervalInSeconds int) (err error) {
	ctx := context.Background()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)

	// remove stale jobs' status
	sql := fmt.Sprintf("delete from mysql.analyze_jobs_status where start_time < NOW() - interval %v second or updated_at < NOW() - interval %v second", staleIntervalInSeconds, staleIntervalInSeconds)
	if _, err := exec.Execute(ctx, sql); err != nil {
		return err
	}

	// update analyze jobs' status
	if len(changedJobs) == 0 {
		return nil
	}
	sqls := make([]string, 0, len(changedJobs))
	updatedAt := time.Now().Format(types.TimeFSPFormat)
	for _, job := range changedJobs {
		sqls = append(sqls, fmt.Sprintf("replace into mysql.analyze_jobs_status values ('%v', '%v', '%v', '%v', '%v', %v, '%v', '%v', '%v')",
			job.UUID, job.DBName, job.TableName, job.PartitionName, job.JobInfo, job.RowCount, job.StartTime.Format(types.TimeFSPFormat), job.State, updatedAt))
	}
	if _, err := exec.Execute(ctx, "begin"); err != nil {
		return err
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	err = execSQLs(ctx, exec, sqls)
	return
}

// GetAnalyzeJobsStatus returns all analyze jobs.
func (h *Handle) GetAnalyzeJobsStatus() ([]statistics.AnalyzeJob, error) {
	ctx := context.Background()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)

	sql := fmt.Sprintf("select * from mysql.analyze_jobs_status order by updated_at, start_time desc")
	rc, err := exec.Execute(ctx, sql)
	if err != nil {
		return nil, err
	}
	res := rc[0]
	defer terror.Call(res.Close)

	jobs := make([]statistics.AnalyzeJob, 0, 64)
	for {
		chk := res.NewChunk()
		iter := chunk.NewIterator4Chunk(chk)
		if err := res.Next(ctx, chk); err != nil {
			return nil, errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			tm := row.GetTime(6)
			gotime, err := tm.GoTime(time.Local)
			if err != nil {
				return nil, errors.Trace(err)
			}
			jobs = append(jobs, statistics.AnalyzeJob{
				UUID:          row.GetString(0),
				DBName:        row.GetString(1),
				TableName:     row.GetString(2),
				PartitionName: row.GetString(3),
				JobInfo:       row.GetString(4),
				RowCount:      row.GetInt64(5),
				StartTime:     gotime,
				State:         row.GetString(7),
			})
		}
	}

	return jobs, nil
}
