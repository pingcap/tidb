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
	chash   ConsistentHash // use consistent hash to reduce task movements after nodes changed
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

func (sch *ServerConsistentHash) ToServerID(key string) string {
	sch.mu.RLock()
	defer sch.mu.RUnlock()
	return sch.chash.GetNode(key)
}

func (sch *ServerConsistentHash) Available(key string) bool {
	sch.mu.RLock()
	defer sch.mu.RUnlock()
	return sch.chash.GetNode(key) == sch.id
}
