package utils

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// process:
// 1. periodically fetch all available TiDB nodes from infosync
// 2. rebuild the consistent hash ring if there is any change
// 3. periodically fetch all tasks from the task manager
// 4. for each task, check if the current node can execute it according to the consistent hash ring
//	if yes, try to acquire the task lock and execute the task
// 		if yes, execute the task
// 		if not, skip the task
//	if not, if the task exec node is not available, try to acquire the task lock and execute the task
//		if yes, execute the task
//		if not, skip the task

type ServerConsistentHash struct {
	servers map[string]*infosync.ServerInfo
	chash   ConsistentHash // use consistent hash to reduce task movement when nodes change
	mu      sync.RWMutex
	id      string // current server id
}

func NewServerConsistentHash(replicas int) *ServerConsistentHash {
	return &ServerConsistentHash{
		servers: make(map[string]*infosync.ServerInfo),
		chash:   *NewConsistentHash(replicas),
	}
}

func (sch *ServerConsistentHash) AddServer(srv *infosync.ServerInfo) {
	sch.servers[srv.ID] = srv
	sch.chash.AddNode(srv.ID)
}

func (sch *ServerConsistentHash) RemoveServer(srvID string) {
	delete(sch.servers, srvID)
	sch.chash.RemoveNode(srvID)
}

func (sch *ServerConsistentHash) Refresh(ctx context.Context) error {
	newServerInfos, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		logutil.BgLogger().Warn("get available TiDB nodes failed", zap.Error(err))
		return err
	}

	{ // if no change, return directly
		sch.mu.RLock()

		nochanged := len(sch.servers) == len(newServerInfos)
		if nochanged {
			for _, srv := range newServerInfos {
				if _, ok := sch.servers[srv.ID]; !ok {
					nochanged = false
					break
				}
			}
		}

		sch.mu.RUnlock()

		if nochanged {
			return nil
		}
	}

	sch.mu.Lock()
	defer sch.mu.Unlock()

	RebuildFromMap(&sch.chash, sch.servers)

	return nil
}

func (sch *ServerConsistentHash) CanExecuteTask(taskID string) bool {
	return sch.GetTaskExecNode(taskID) == sch.id
}

func (sch *ServerConsistentHash) GetTaskExecNode(taskID string) string {
	sch.mu.RLock()
	defer sch.mu.RUnlock()
	return sch.chash.GetNode(taskID)
}

/*
create table task_locks (
	task_id varchar(128) primary key,
	holder_id varchar(128) not null,
	updated_at timestamp not null default current_timestamp on update current_timestamp
) engine=InnoDB
*/

func (sch *ServerConsistentHash) AcquireTaskLock(taskID string) bool {
	sql := ""
	return true
}

func (sch *ServerConsistentHash) GetTaskHolder(taskID string) string {
	return "?"
}

func (sch *ServerConsistentHash) ExecuteTask(taskID string) bool {
	return true
}

func (sch *ServerConsistentHash) TryExecuteTask(taskID string) bool {
	if !sch.CanExecuteTask(taskID) {
		return false
	}
	if !sch.AcquireTaskLock(taskID) {
		holder := sch.GetTaskHolder(taskID)
		if sch.servers[holder] == nil {
			logutil.BgLogger().Info("task holder is not available, try to execute the task",
				zap.String("task-id", taskID),
				zap.String("holder", holder))
			
		}
	}
	return sch.ExecuteTask(taskID)
}
