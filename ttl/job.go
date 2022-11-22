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

package ttl

import (
	"sync"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

type JobManager struct {
	baseWorker
	jobs        sync.Map
	delCh       chan *delTask
	scanWorkers []worker
	delWorkers  []worker
	sessPool    sessionPool
}

func NewJobManager(sessPool sessionPool) (manager *JobManager) {
	manager = &JobManager{}
	manager.init(manager.jobLoop)
	manager.delCh = make(chan *delTask)
	manager.sessPool = sessPool
	return
}

func (m *JobManager) jobLoop() error {
	se, err := getWorkerSession(m.sessPool)
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
	ticker := time.Tick(time.Second * 10)
	for {
		select {
		case <-m.ctx.Done():
			return nil
		case <-ticker:
			m.rescheduleJobs(se)
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

func (m *JobManager) rescheduleJobs(se *session) {
	is := se.GetDomainInfoSchema().(infoschema.InfoSchema)
	if extended, ok := is.(*infoschema.SessionExtendedInfoSchema); ok {
		is = extended
	}

	for _, db := range is.AllSchemas() {
		for _, tbl := range is.SchemaTables(db.Name) {
			tblInfo := tbl.Meta()
			if !isTTLTable(tblInfo) {
				continue
			}

			ttlTbls := m.ttlTableFromMeta(db.Name, tblInfo)
			for _, ttlTbl := range ttlTbls {
				if _, ok := m.jobs.Load(ttlTbl.GetPhysicalTableID()); ok {
					continue
				}
				job := newTTLJob(ttlTbl)
				logutil.BgLogger().Info("new TTL job", zap.String("tbl", ttlTbl.Name.O))
				m.jobs.Store(job.GetPhysicalTableID(), job)
			}
		}
	}

	idleScanWorkers := m.idleScanWorkers()
	if len(idleScanWorkers) > 0 {
		pos := 0
		done := make([]int64, 0, len(idleScanWorkers))
		m.jobs.Range(func(_, value any) bool {
			job := value.(*ttlJob)
			if task, ok := job.PollNextScanTask(); ok {
				logutil.BgLogger().Info("new Scan task", zap.String("tbl", task.tbl.Name.L))
				if !idleScanWorkers[pos].ScheduleTask(task) {
					logutil.BgLogger().Info("schedule task failed", zap.String("tbl", task.tbl.Name.L))
				}

				if job.Done() {
					done = append(done, job.GetPhysicalTableID())
				}
				pos++
			}
			return pos < len(idleScanWorkers)
		})

		for _, id := range done {
			logutil.BgLogger().Info("job done", zap.Int64("id", id))
			m.jobs.Delete(id)
		}
	}
}

func (m *JobManager) ttlTableFromMeta(schema model.CIStr, tbl *model.TableInfo) []*ttlTable {
	if tbl.Partition == nil {
		ttlTbl, err := newTTLTable(schema, tbl, nil)
		if err != nil {
			terror.Log(err)
			return nil
		}
		return []*ttlTable{ttlTbl}
	}
	// TODO: partition
	return nil
}

type ttlJob struct {
	sync.Mutex
	tbl      *ttlTable
	tasks    []*scanTask
	nextPoll int
}

func newTTLJob(tbl *ttlTable) *ttlJob {
	return &ttlJob{tbl: tbl, tasks: []*scanTask{
		{tbl: tbl, expire: time.Now().Add(-time.Minute)},
	}}
}

func (t *ttlJob) GetPhysicalTableID() int64 {
	return t.tbl.GetPhysicalTableID()
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
