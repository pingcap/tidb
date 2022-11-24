// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttlworker

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/ttl"
)

type JobManager struct {
	baseWorker

	id          string
	delCh       chan *delTask
	scanWorkers []worker
	delWorkers  []worker
	sessPool    sessionPool
}

func NewJobManager(id string, sessPool sessionPool) (manager *JobManager) {
	manager = &JobManager{}
	manager.init(manager.jobLoop)
	manager.id = id
	manager.delCh = make(chan *delTask)
	manager.sessPool = sessPool
	return
}

func (m *JobManager) jobLoop() error {
	se, err := getSession(m.sessPool)
	if err != nil {
		return err
	}

	defer func() {
		m.resizeScanWorkers(0)
		m.resizeDelWorkers(0)
		se.Close()
		close(m.delCh)
	}()

	m.resizeScanWorkers(4)
	m.resizeDelWorkers(4)

	tables := newTTLTables(m.id)
	terror.Log(tables.FullUpdate(m.ctx, se))
	ticker := time.Tick(10 * time.Second)
	for {
		select {
		case <-m.ctx.Done():
			return nil
		case <-ticker:
			if time.Since(tables.TablesTryUpdateTime) > time.Minute {
				terror.Log(tables.UpdateSchemaTables(se))
			}
			if time.Since(tables.StatesTryUpdateTime) > time.Minute*10 {
				terror.Log(tables.UpdateTableStates(m.ctx, se))
			}
			m.rescheduleJobs(se, tables)
		}
	}
}

func (m *JobManager) idleScanWorkers() []*scanWorker {
	m.Lock()
	defer m.Unlock()
	workers := make([]*scanWorker, 0, len(m.scanWorkers))
	for _, w := range m.scanWorkers {
		if w.(*scanWorker).Idle() {
			workers = append(workers, w.(*scanWorker))
		}
	}
	return workers
}

func (m *JobManager) resizeScanWorkers(n int) {
	m.Lock()
	defer m.Unlock()
	m.scanWorkers = m.resizeWorkers(m.scanWorkers, n, func() worker {
		return newScanWorker(m.delCh, m.sessPool)
	})
}

func (m *JobManager) resizeDelWorkers(n int) {
	m.Lock()
	defer m.Unlock()
	m.delWorkers = m.resizeWorkers(m.delWorkers, n, func() worker {
		return newDelWorker(m.delCh, m.sessPool)
	})
}

func (m *JobManager) resizeWorkers(workers []worker, n int, factory func() worker) []worker {
	currentCnt := len(workers)
	switch {
	case n > currentCnt:
		for i := currentCnt; i < n; i++ {
			w := factory()
			w.Start()
			workers = append(workers, w)
		}
	case n < currentCnt:
		for i := n; i < currentCnt; i++ {
			workers[i].Stop()
		}
		workers = workers[0:n]
	}
	return workers
}

func (m *JobManager) rescheduleJobs(se *ttl.Session, tables *ttlTables) {
	defer func() {
		for _, job := range tables.RunningJobs {
			if job.Done() {
				terror.Log(tables.FinishJob(m.ctx, se, job))
			}
		}
	}()

	idleScanWorkers := m.idleScanWorkers()
	if len(idleScanWorkers) == 0 {
		return
	}

	tbls := tables.ReadyForNewJobTables(se.Sctx.GetSessionVars())
	jobs := tables.ReadyForResumeJobs(se.Sctx.GetSessionVars())
	for len(idleScanWorkers) > 0 && (len(tbls) > 0 || len(jobs) > 0) {
		var job *ttlJob
		var err error
		switch {
		case len(tbls) > 0:
			tbl := tbls[0]
			tbls = tbls[1:]
			job, err = tables.CreateNewJob(m.ctx, se, tbl.ID)
		case len(jobs) > 0:
			jobInfo := jobs[0]
			jobs = jobs[1:]
			job, err = tables.ResumeJob(m.ctx, se, jobInfo.TableID, jobInfo.ID)
		}

		if err != nil || job == nil {
			terror.Log(err)
			continue
		}

		for len(idleScanWorkers) > 0 {
			task, ok := job.PollNextScanTask()
			if !ok {
				break
			}
			idleWorker := idleScanWorkers[0]
			idleScanWorkers = idleScanWorkers[1:]
			idleWorker.ScheduleTask(task)
		}
	}
}

func (m *JobManager) ttlTableFromMeta(schema model.CIStr, tbl *model.TableInfo) []*ttl.PhysicalTable {
	if tbl.Partition == nil {
		ttlTbl, err := ttl.NewPhysicalTable(schema, tbl, nil)
		if err != nil {
			terror.Log(err)
			return nil
		}
		return []*ttl.PhysicalTable{ttlTbl}
	}
	// TODO: partition
	return nil
}

type ttlJob struct {
	sync.Mutex
	tbl      *ttl.PhysicalTable
	tasks    []*scanTask
	nextPoll int
}

func (t *ttlJob) Done() bool {
	t.Lock()
	defer t.Unlock()
	return t.nextPoll < 0 || t.nextPoll >= len(t.tasks)
}

func (t *ttlJob) PollNextScanTask() (*scanTask, bool) {
	t.Lock()
	defer t.Unlock()
	if t.nextPoll < 0 || t.nextPoll >= len(t.tasks) {
		return nil, false
	}

	task := t.tasks[t.nextPoll]
	t.nextPoll++
	return task, true
}

type ttlTables struct {
	instanceID          string
	RunningJobs         map[int]*ttlJob
	Tables              *ttl.InfoSchemaTables
	TablesTryUpdateTime time.Time
	TablesUpdateTime    time.Time

	States              *ttl.TableStatesCache
	StatesTryUpdateTime time.Time
	StatesUpdateTime    time.Time
}

func newTTLTables(instanceID string) *ttlTables {
	return &ttlTables{
		instanceID:          instanceID,
		Tables:              ttl.NewInfoSchemaTables(),
		TablesTryUpdateTime: time.UnixMilli(0),
		TablesUpdateTime:    time.UnixMilli(0),
		States:              ttl.NewTableStatesCache(),
		StatesTryUpdateTime: time.UnixMilli(0),
		StatesUpdateTime:    time.UnixMilli(0),
	}
}

func (ts *ttlTables) FullUpdate(ctx context.Context, se *ttl.Session) error {
	err1 := ts.UpdateSchemaTables(se)
	err2 := ts.UpdateTableStates(ctx, se)
	if err1 != nil {
		return err1
	}
	return err2
}

func (ts *ttlTables) UpdateSchemaTables(se *ttl.Session) error {
	ts.TablesTryUpdateTime = time.Now()
	is := se.GetDomainInfoSchema()
	if err := ts.Tables.Update(is); err != nil {
		return err
	}
	ts.TablesUpdateTime = time.Now()
	return nil
}

func (ts *ttlTables) UpdateTableStates(ctx context.Context, se *ttl.Session) error {
	ts.StatesTryUpdateTime = time.Now()
	if err := ts.States.Update(ctx, se); err != nil {
		return err
	}
	ts.StatesUpdateTime = time.Now()
	return nil
}

func (ts *ttlTables) CreateNewJob(ctx context.Context, se *ttl.Session, tblID int64) (*ttlJob, error) {
	// TODO:
	return nil, nil
}

func (ts *ttlTables) ResumeJob(ctx context.Context, se *ttl.Session, tblID int64, jobID string) (*ttlJob, error) {
	// TODO:
	return nil, nil
}

func (ts *ttlTables) FinishJob(ctx context.Context, se *ttl.Session, job *ttlJob) error {
	// TODO:
	return nil
}

func (ts *ttlTables) ReadyForNewJobTables(sessVars *variable.SessionVars) []*ttl.PhysicalTable {
	// TODO:
	return nil
}

func (ts *ttlTables) ReadyForResumeJobs(sessVars *variable.SessionVars) []*ttl.JobInfo {
	// TODO:
	return nil
}
