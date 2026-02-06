package utils

import (
	"context"
	"errors"
	"sync"
	"time"

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
	ID      string // current server ID
}

func NewServerConsistentHash(replicas int) *ServerConsistentHash {
	return &ServerConsistentHash{
		servers: make(map[string]*infosync.ServerInfo),
		chash:   *NewConsistentHash(replicas),
	}
}

// Init initializes current server ID. It keeps retrying until successful.
func (sch *ServerConsistentHash) Init() {
	if sch == nil {
		return
	}
	backoff := time.Second
	for {
		info, err := infosync.GetServerInfo()
		if err == nil && info != nil && info.ID != "" {
			sch.mu.Lock()
			sch.ID = info.ID
			sch.mu.Unlock()
			return
		}
		if err != nil {
			logutil.BgLogger().Warn("get local TiDB server info failed", zap.Error(err))
		} else {
			logutil.BgLogger().Warn("get local TiDB server info empty")
		}
		if backoff > time.Minute {
			backoff = time.Minute
		}
		time.Sleep(backoff)
		if backoff < time.Minute {
			backoff *= 2
			if backoff > time.Minute {
				backoff = time.Minute
			}
		}
	}
}

func (sch *ServerConsistentHash) AddServer(srv *infosync.ServerInfo) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	sch.servers[srv.ID] = srv
	sch.chash.AddNode(srv.ID)
}

func (sch *ServerConsistentHash) RemoveServer(srvID string) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	delete(sch.servers, srvID)
	sch.chash.RemoveNode(srvID)
}

func (sch *ServerConsistentHash) Refresh(ctx context.Context, filter func(*infosync.ServerInfo) bool) error {
	if sch == nil {
		return errors.New("server consistent hash is nil")
	}
	newServerInfos, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		logutil.BgLogger().Warn("get available TiDB nodes failed", zap.Error(err))
		return err
	}
	// filter servers by the given filter function
	if filter != nil {
		for k, v := range newServerInfos {
			if !filter(v) {
				delete(newServerInfos, k)
			}
		}
	}

	{ // if no change, return directly
		sch.mu.RLock()

		noChanged := len(sch.servers) == len(newServerInfos)
		if noChanged {
			for id := range newServerInfos {
				if _, ok := sch.servers[id]; !ok {
					noChanged = false
					break
				}
			}
		}

		sch.mu.RUnlock()

		if noChanged {
			return nil
		}
	}
	{
		sch.mu.Lock() // guard server map and hash ring rebuild

		sch.servers = newServerInfos
		RebuildFromMap(&sch.chash, sch.servers)

		sch.mu.Unlock() // release guard after rebuild
	}
	return nil
}

func (sch *ServerConsistentHash) ToServerID(key string) string {
	sch.mu.RLock()
	defer sch.mu.RUnlock()
	return sch.chash.GetNode(key)
}

func (sch *ServerConsistentHash) Available(key string) bool {
	return sch.ToServerID(key) == sch.ID
}
