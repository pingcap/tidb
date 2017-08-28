// Copyright 2017 PingCAP, Inc.
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
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/contrib/recipes"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/util/sqlexec"
	goctx "golang.org/x/net/context"
)

// JobManager is used to manage auto analyze jobs.
type JobManager interface {
	// ID returns the ID of the manager.
	ID() string
	// IsOwner returns whether the jobManager is the owner.
	IsOwner() bool
	// PendingSize returns the size of pending jobs.
	PendingSize() (int, error)
	// Enqueue pushes jobs into the pending queue.
	Enqueue(jobs []string) error
	// DequeueAndAnalyze gets a job from pending queue and analyze it.
	DequeueAndAnalyze(ctx context.Context, h *Handle, is infoschema.InfoSchema) error
}

// jobManager represents the structure which is used for manage auto analyze jobs.
type jobManager struct {
	owner        owner.Manager
	etcdCli      *clientv3.Client
	pendingQueue *recipe.Queue
	session      *concurrency.Session
}

const (
	StatsPendingPrefix = "/tidb/stats/pending/"
	StatsWorkingPrefix = "/tidb/stats/working/"
)

// NewJobManager creates a new JobManager.
func NewJobManager(owner owner.Manager, etcdCli *clientv3.Client, session *concurrency.Session) JobManager {
	return &jobManager{
		owner:        owner,
		etcdCli:      etcdCli,
		pendingQueue: recipe.NewQueue(etcdCli, StatsPendingPrefix),
		session:      session,
	}
}

// ID implements JobManager.ID interface.
func (m *jobManager) ID() string {
	return m.owner.ID()
}

// IsOwner implements JobManager.IsOwner interface.
func (m *jobManager) IsOwner() bool {
	return m.owner.IsOwner()
}

// PendingSize implements JobManager.PendingSize interface.
func (m *jobManager) PendingSize() (int, error) {
	resp, err := m.etcdCli.Get(goctx.TODO(), StatsPendingPrefix, clientv3.WithPrefix())
	if err != nil {
		return 0, errors.Trace(err)
	}
	return len(resp.Kvs), nil
}

// Enqueue implements JobManager.Enqueue interface.
func (m *jobManager) Enqueue(jobs []string) error {
	workingJob, err := m.getWorkingJob()
	if err != nil {
		return errors.Trace(err)
	}
	for _, job := range jobs {
		if !workingJob[job] {
			err := m.pendingQueue.Enqueue(job)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (m *jobManager) getWorkingJob() (map[string]bool, error) {
	resp, err := m.etcdCli.Get(goctx.TODO(), StatsWorkingPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make(map[string]bool)
	for _, kv := range resp.Kvs {
		job := kv.Key[len(StatsWorkingPrefix):]
		jobs[string(job)] = true
	}
	return jobs, nil
}

// DequeueAndAnalyze implements JobManager.DequeueAndAnalyze interface.
func (m *jobManager) DequeueAndAnalyze(ctx context.Context, h *Handle, is infoschema.InfoSchema) error {
	if m.session == nil {
		return nil
	}
	job, err := m.pendingQueue.Dequeue()
	if err != nil {
		return errors.Trace(err)
	}
	mutex := NewMutex(m.session, StatsWorkingPrefix+job)
	// Why use TryToLock but not Lock? Suppose that there are two worker A, B, the owner pushes a table `t` into the queue,
	// A gets the job, but before A locks the job, the owner finds that the queue is empty, and the table `t` is not
	// analyzed, so it pushes table `t` into queue again, this time B also gets the job. So if we use `Lock` here,
	// one of the worker will be forced to wait another one finish.
	ok, err := mutex.TryToLock(goctx.TODO())
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		return nil
	}
	defer func() {
		if err := mutex.Unlock(goctx.TODO()); err != nil {
			log.Error("[stats] unlock mutex failed: ", errors.ErrorStack(err))
		}
	}()
	dbName, tblName, idxName := decode(job)
	// We need to check again here in case that the table or index has been dropped.
	if !h.needAnalyze(is, dbName, tblName, idxName) {
		return nil
	}
	sql := "analyze table `" + dbName + "`.`" + tblName + "`"
	if idxName != "" {
		sql = sql + " index `" + idxName + "`"
	}
	log.Infof("[stats] auto %s now", sql)
	_, _, err = ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// mockManager represents the structure which is used for manage auto analyze jobs.
// It's used for local store and testing.
type mockManager struct {
	jobs []string
}

// NewMockJobManager creates a new mock job manager.
func NewMockJobManager() JobManager {
	return &mockManager{}
}

// ID implements JobManager.ID interface.
func (m *mockManager) ID() string {
	return ""
}

// IsOwner implements JobManager.IsOwner interface.
func (m *mockManager) IsOwner() bool {
	return true
}

// PendingSize implements JobManager.PendingSize interface.
func (m *mockManager) PendingSize() (int, error) {
	return len(m.jobs), nil
}

// Enqueue implements JobManager.Enqueue interface.
func (m *mockManager) Enqueue(jobs []string) error {
	if len(m.jobs) == 0 {
		m.jobs = jobs
	}
	return nil
}

// DequeueAndAnalyze implements JobManager.DequeueAndAnalyze interface.
func (m *mockManager) DequeueAndAnalyze(ctx context.Context, h *Handle, is infoschema.InfoSchema) error {
	if len(m.jobs) == 0 {
		return nil
	}
	job := m.jobs[0]
	m.jobs = m.jobs[1:]
	dbName, tblName, idxName := decode(job)
	if !h.needAnalyze(is, dbName, tblName, idxName) {
		return nil
	}
	sql := "analyze table `" + dbName + "`.`" + tblName + "`"
	if idxName != "" {
		sql = sql + " index `" + idxName + "`"
	}
	log.Infof("[stats] auto %s now", sql)
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
