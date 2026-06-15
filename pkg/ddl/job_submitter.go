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

package ddl

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/jobsubmit"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// JobSubmitter collects the DDL jobs and submits them to job tables in batch, it's
// also responsible allocating IDs for the jobs. when fast-create is enabled, it
// will merge the create-table jobs to a single batch create-table job.
// export for testing.
type JobSubmitter struct {
	ctx               context.Context
	etcdCli           *clientv3.Client
	ownerManager      owner.Manager
	store             kv.Storage
	serverStateSyncer serverstate.Syncer
	ddlJobDoneChMap   *generic.SyncMap[int64, chan struct{}]

	// init at ddl start.
	sessPool          *sess.Pool
	sysTblMgr         systable.Manager
	minJobIDRefresher *systable.MinJobIDRefresher

	limitJobCh chan *JobWrapper
	// get notification if any DDL job submitted or finished.
	ddlJobNotifyCh chan struct{}
}

func (s *JobSubmitter) submitLoop() {
	defer util.Recover(metrics.LabelDDL, "submitLoop", nil, true)

	jobWs := make([]*JobWrapper, 0, batchAddingJobs)
	ch := s.limitJobCh
	for {
		select {
		// the channel is never closed
		case jobW := <-ch:
			jobWs = jobWs[:0]
			failpoint.InjectCall("afterGetJobFromLimitCh", ch)
			jobLen := len(ch)
			jobWs = append(jobWs, jobW)
			for range jobLen {
				jobWs = append(jobWs, <-ch)
			}
			s.addBatchDDLJobs(jobWs)
		case <-s.ctx.Done():
			return
		}
	}
}

// addBatchDDLJobs gets global job IDs and puts the DDL jobs in the DDL queue.
func (s *JobSubmitter) addBatchDDLJobs(jobWs []*JobWrapper) {
	startTime := time.Now()
	var (
		err   error
		newWs []*JobWrapper
	)
	fastCreate := vardef.EnableFastCreateTable.Load()
	if fastCreate {
		newWs, err = mergeCreateTableJobs(jobWs)
		if err != nil {
			logutil.DDLLogger().Warn("failed to merge create table jobs", zap.Error(err))
		} else {
			jobWs = newWs
		}
	}
	err = s.addBatchDDLJobs2Table(jobWs)
	var jobs string
	for _, jobW := range jobWs {
		jobW.NotifyResult(err)
		jobs += jobW.Job.String() + "; "
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerAddDDLJob, jobW.Job.Type.String(),
			metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}
	if err != nil {
		logutil.DDLLogger().Warn("add DDL jobs failed", zap.String("jobs", jobs), zap.Error(err))
		return
	}
	// Notice worker that we push a new job and wait the job done.
	s.notifyNewJobSubmitted()
	logutil.DDLLogger().Info("add DDL jobs",
		zap.Int("batch count", len(jobWs)),
		zap.String("jobs", jobs),
		zap.Bool("fast_create", fastCreate))
}

// mergeCreateTableJobs merges CreateTable jobs to CreateTables.
func mergeCreateTableJobs(jobWs []*JobWrapper) ([]*JobWrapper, error) {
	if len(jobWs) <= 1 {
		return jobWs, nil
	}
	resJobWs := make([]*JobWrapper, 0, len(jobWs))
	mergeableJobWs := make(map[string][]*JobWrapper, len(jobWs))
	for _, jobW := range jobWs {
		// we don't merge jobs with ID pre-allocated.
		if jobW.Type != model.ActionCreateTable || jobW.IDAllocated {
			resJobWs = append(resJobWs, jobW)
			continue
		}
		// ActionCreateTables doesn't support foreign key now.
		args := jobW.JobArgs.(*model.CreateTableArgs)
		if len(args.TableInfo.ForeignKeys) > 0 {
			resJobWs = append(resJobWs, jobW)
			continue
		}
		// CreateTables only support tables of same schema now.
		mergeableJobWs[jobW.Job.SchemaName] = append(mergeableJobWs[jobW.Job.SchemaName], jobW)
	}

	for schema, jobs := range mergeableJobWs {
		total := len(jobs)
		if total <= 1 {
			resJobWs = append(resJobWs, jobs...)
			continue
		}
		const maxBatchSize = 8
		batchCount := (total + maxBatchSize - 1) / maxBatchSize
		start := 0
		for _, batchSize := range mathutil.Divide2Batches(total, batchCount) {
			batch := jobs[start : start+batchSize]
			newJobW, err := mergeCreateTableJobsOfSameSchema(batch)
			if err != nil {
				return nil, err
			}
			start += batchSize
			logutil.DDLLogger().Info("merge create table jobs", zap.String("schema", schema),
				zap.Int("total", total), zap.Int("batch_size", batchSize))
			resJobWs = append(resJobWs, newJobW)
		}
	}
	return resJobWs, nil
}

// buildQueryStringFromJobs takes a slice of Jobs and concatenates their
// queries into a single query string.
// Each query is separated by a semicolon and a space.
// Trailing spaces are removed from each query, and a semicolon is appended
// if it's not already present.
func buildQueryStringFromJobs(jobs []*JobWrapper) string {
	var queryBuilder strings.Builder
	for i, job := range jobs {
		q := strings.TrimSpace(job.Query)
		if !strings.HasSuffix(q, ";") {
			q += ";"
		}
		queryBuilder.WriteString(q)

		if i < len(jobs)-1 {
			queryBuilder.WriteString(" ")
		}
	}
	return queryBuilder.String()
}

// mergeCreateTableJobsOfSameSchema combine CreateTableJobs to BatchCreateTableJob.
func mergeCreateTableJobsOfSameSchema(jobWs []*JobWrapper) (*JobWrapper, error) {
	if len(jobWs) == 0 {
		return nil, errors.Trace(fmt.Errorf("expect non-empty jobs"))
	}

	var (
		combinedJob *model.Job
		args        = &model.BatchCreateTableArgs{
			Tables: make([]*model.CreateTableArgs, 0, len(jobWs)),
		}
		involvingSchemaInfo = make([]model.InvolvingSchemaInfo, 0, len(jobWs))
	)

	// if there is any duplicated table name
	duplication := make(map[string]struct{})
	for _, job := range jobWs {
		if combinedJob == nil {
			combinedJob = job.Clone()
			combinedJob.Type = model.ActionCreateTables
		}
		jobArgs := job.JobArgs.(*model.CreateTableArgs)
		args.Tables = append(args.Tables, jobArgs)

		info := jobArgs.TableInfo
		if _, ok := duplication[info.Name.L]; ok {
			// return err even if create table if not exists
			return nil, infoschema.ErrTableExists.FastGenByArgs("can not batch create tables with same name")
		}

		duplication[info.Name.L] = struct{}{}

		involvingSchemaInfo = append(involvingSchemaInfo,
			model.InvolvingSchemaInfo{
				Database: job.SchemaName,
				Table:    info.Name.L,
			})
	}

	combinedJob.InvolvingSchemaInfo = involvingSchemaInfo
	combinedJob.Query = buildQueryStringFromJobs(jobWs)

	newJobW := &JobWrapper{
		Job:      combinedJob,
		JobArgs:  args,
		ResultCh: make([]chan jobSubmitResult, 0, len(jobWs)),
	}
	// merge the result channels.
	for _, j := range jobWs {
		newJobW.ResultCh = append(newJobW.ResultCh, j.ResultCh...)
	}

	return newJobW, nil
}

// addBatchDDLJobs2Table gets global job IDs and puts the DDL jobs in the DDL job table.
func (s *JobSubmitter) addBatchDDLJobs2Table(jobWs []*JobWrapper) error {
	if len(jobWs) == 0 {
		return nil
	}
	err := jobsubmit.SubmitBatch(s.ctx, jobsubmit.SubmitOptions{
		Store:                       s.store,
		SessPool:                    s.sessPool,
		SysTblMgr:                   s.sysTblMgr,
		MinJobIDRefresher:           s.minJobIDRefresher,
		ServerStateSyncer:           s.serverStateSyncer,
		BeforeInsertWithAssignedIDs: s.registerJobDoneChannels,
	}, jobWrappersToSpecs(jobWs))
	return errors.Trace(err)
}

func (s *JobSubmitter) registerJobDoneChannels(specs []*jobsubmit.JobSpec) func() {
	currentJobIDs := make([]int64, 0, len(specs))
	// Job scheduler will start run them after txn commit, we want to make sure
	// the channel exists before the jobs are submitted.
	for _, spec := range specs {
		s.ddlJobDoneChMap.Store(spec.Job.ID, make(chan struct{}, 1))
		currentJobIDs = append(currentJobIDs, spec.Job.ID)
	}
	return func() {
		for _, jobID := range currentJobIDs {
			s.ddlJobDoneChMap.Delete(jobID)
		}
	}
}

func jobWrappersToSpecs(jobWs []*JobWrapper) []*jobsubmit.JobSpec {
	specs := make([]*jobsubmit.JobSpec, 0, len(jobWs))
	for _, jobW := range jobWs {
		specs = append(specs, &jobsubmit.JobSpec{
			Job:         jobW.Job,
			Args:        jobW.JobArgs,
			IDAllocated: jobW.IDAllocated,
		})
	}
	return specs
}

func (s *JobSubmitter) notifyNewJobSubmitted() {
	if s.ownerManager.IsOwner() {
		asyncNotify(s.ddlJobNotifyCh)
		return
	}
	s.notifyNewJobByEtcd()
}

func (s *JobSubmitter) notifyNewJobByEtcd() {
	if s.etcdCli == nil {
		return
	}

	err := ddlutil.PutKVToEtcd(s.ctx, s.etcdCli, 1, ddlutil.AddingDDLJobNotifyKey, "0")
	if err != nil {
		logutil.DDLLogger().Info("notify new DDL job failed", zap.Error(err))
	}
}
