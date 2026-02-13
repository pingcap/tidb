package mvs

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type serverInfo struct {
	ID string
}

func int64KeyToBinaryBytes(key int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(key))
	return buf[:]
}

// ServerHelper provides server discovery for building the consistent-hash view.
type ServerHelper interface {
	serverFilter(serverInfo) bool
	getServerInfo() (serverInfo, error)
	getAllServerInfo(ctx context.Context) (map[string]serverInfo, error)
}

// ServerConsistentHash maintains TiDB server membership and ownership mapping.
type ServerConsistentHash struct {
	ctx     context.Context
	servers map[string]serverInfo
	chash   ConsistentHash // use consistent hash to reduce task movements after nodes changed
	mu      sync.RWMutex
	ID      string // current server ID
	helper  ServerHelper
}

// NewServerConsistentHash creates a server ownership helper backed by consistent hash.
func NewServerConsistentHash(ctx context.Context, replicas int, helper ServerHelper) *ServerConsistentHash {
	if ctx == nil || helper == nil {
		panic("context and helper cannot be nil")
	}
	return &ServerConsistentHash{
		ctx:     ctx,
		servers: make(map[string]serverInfo),
		chash:   *NewConsistentHash(replicas),
		helper:  helper,
	}
}

// init loads local server info with retry and initializes local server ID.
func (sch *ServerConsistentHash) init() bool {
	backoff := time.Second
	for {
		// Retry until local server identity is available; otherwise this node
		// cannot be part of the hash ring and will receive no tasks.
		info, err := sch.helper.getServerInfo()
		if err == nil {
			sch.mu.Lock()

			sch.ID = info.ID

			sch.mu.Unlock()
			return true
		}

		waitBackoff := min(backoff, time.Second*5)
		logutil.BgLogger().Warn("get local TiDB server info failed, retrying after backoff", zap.Error(err), zap.Duration("backoff", waitBackoff))
		select {
		case <-sch.ctx.Done():
			return false
		case <-mvsAfter(waitBackoff):
			backoff = waitBackoff * 2
		}
	}
}

// addServer inserts one server into the member map and hash ring.
func (sch *ServerConsistentHash) addServer(srv serverInfo) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	sch.servers[srv.ID] = srv
	sch.chash.AddNode(srv.ID)
}

// removeServer removes one server from the member map and hash ring.
func (sch *ServerConsistentHash) removeServer(srvID string) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	delete(sch.servers, srvID)
	sch.chash.RemoveNode(srvID)
}

// refresh reloads server membership and rebuilds the hash ring when changed.
func (sch *ServerConsistentHash) refresh() error {
	newServerInfos, err := sch.helper.getAllServerInfo(sch.ctx)
	if err != nil {
		logutil.BgLogger().Warn("get available TiDB nodes failed", zap.Error(err))
		return err
	}
	// Filter servers by helper policy.
	for k, v := range newServerInfos {
		if !sch.helper.serverFilter(v) {
			delete(newServerInfos, k)
		}
	}

	{ // Return early when there is no membership change.
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
		sch.mu.Lock() // Guard server map and hash-ring rebuild.

		sch.servers = newServerInfos
		sch.chash.Rebuild(sch.servers)

		sch.mu.Unlock() // Release guard after rebuild.
	}
	return nil
}

// Available checks if the server responsible for key is available (i.e. matches current server ID).
// Supported key types are string and int64.
// int64 keys are hashed by their fixed-width binary encoding.
func (sch *ServerConsistentHash) Available(key any) bool {
	sch.mu.RLock()
	defer sch.mu.RUnlock()
	switch v := key.(type) {
	case string:
		return sch.chash.GetNode([]byte(v)) == sch.ID
	case int64:
		return sch.chash.GetNode(int64KeyToBinaryBytes(v)) == sch.ID
	default:
		return false
	}
}
