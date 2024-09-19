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

// Package systable contains all constants/methods related accessing system tables
// related to DDL job execution
package systable

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta/model"
)

var (
	// ErrNotFound is the error when we can't found the info we are querying related
	// to some jobID. might happen when there are multiple owners and 1 of them run and delete it.
	ErrNotFound = errors.New("not found")
)

// Manager is the interface for DDL job/MDL storage layer, it provides the methods
// to access the job/MDL related tables.
type Manager interface {
	// GetJobByID gets the job by ID, returns ErrNotFound if the job does not exist.
	GetJobByID(ctx context.Context, jobID int64) (*model.Job, error)
	// GetMDLVer gets the MDL version by job ID, returns ErrNotFound if the MDL info does not exist.
	GetMDLVer(ctx context.Context, jobID int64) (int64, error)
	// GetMinJobID gets current minimum job ID in the job table for job_id >= prevMinJobID,
	// if no jobs, returns 0. prevMinJobID is used to avoid full table scan, see
	// https://github.com/pingcap/tidb/issues/52905
	GetMinJobID(ctx context.Context, prevMinJobID int64) (int64, error)
	// HasFlashbackClusterJob checks if there is any flashback cluster job.
	// minJobID has the same meaning as in GetMinJobID.
	HasFlashbackClusterJob(ctx context.Context, minJobID int64) (bool, error)
}

type manager struct {
	sePool *session.Pool
}

var _ Manager = (*manager)(nil)

// NewManager creates a new Manager.
func NewManager(pool *session.Pool) Manager {
	return &manager{
		sePool: pool,
	}
}

func (mgr *manager) withNewSession(fn func(se *session.Session) error) error {
	se, err := mgr.sePool.Get()
	if err != nil {
		return err
	}
	defer mgr.sePool.Put(se)

	ddlse := session.NewSession(se)
	return fn(ddlse)
}

func (mgr *manager) GetJobByID(ctx context.Context, jobID int64) (*model.Job, error) {
	job := model.Job{}
	if err := mgr.withNewSession(func(se *session.Session) error {
		sql := fmt.Sprintf(`select job_meta from mysql.tidb_ddl_job where job_id = %d`, jobID)
		rows, err := se.Execute(ctx, sql, "get-job-by-id")
		if err != nil {
			return errors.Trace(err)
		}
		if len(rows) == 0 {
			return ErrNotFound
		}
		jobBinary := rows[0].GetBytes(0)
		err = job.Decode(jobBinary)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &job, nil
}

func (mgr *manager) GetMDLVer(ctx context.Context, jobID int64) (int64, error) {
	var ver int64
	if err := mgr.withNewSession(func(se *session.Session) error {
		sql := fmt.Sprintf("select version from mysql.tidb_mdl_info where job_id = %d", jobID)
		rows, err := se.Execute(ctx, sql, "check-mdl-info")
		if err != nil {
			return errors.Trace(err)
		}
		if len(rows) == 0 {
			return ErrNotFound
		}
		ver = rows[0].GetInt64(0)
		return nil
	}); err != nil {
		return 0, err
	}
	return ver, nil
}

func (mgr *manager) GetMinJobID(ctx context.Context, prevMinJobID int64) (int64, error) {
	var minID int64
	if err := mgr.withNewSession(func(se *session.Session) error {
		sql := fmt.Sprintf(`select min(job_id) from mysql.tidb_ddl_job where job_id >= %d`, prevMinJobID)
		rows, err := se.Execute(ctx, sql, "get-min-job-id")
		if err != nil {
			return errors.Trace(err)
		}
		if len(rows) == 0 {
			return nil
		}
		minID = rows[0].GetInt64(0)
		return nil
	}); err != nil {
		return 0, err
	}
	return minID, nil
}

func (mgr *manager) HasFlashbackClusterJob(ctx context.Context, minJobID int64) (bool, error) {
	var hasFlashbackClusterJob bool
	if err := mgr.withNewSession(func(se *session.Session) error {
		sql := fmt.Sprintf(`select count(1) from mysql.tidb_ddl_job where job_id >= %d and type = %d`,
			minJobID, model.ActionFlashbackCluster)
		rows, err := se.Execute(ctx, sql, "has-flashback-cluster-job")
		if err != nil {
			return errors.Trace(err)
		}
		if len(rows) == 0 {
			return nil
		}
		hasFlashbackClusterJob = rows[0].GetInt64(0) > 0
		return nil
	}); err != nil {
		return false, err
	}
	return hasFlashbackClusterJob, nil
}
