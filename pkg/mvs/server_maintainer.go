// Copyright 2026 PingCAP, Inc.
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

// AddServer inserts one server into the member map and hash ring.
func (sch *ServerConsistentHash) AddServer(srv serverInfo) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	sch.servers[srv.ID] = srv
	sch.chash.AddNode(srv.ID)
}

// RemoveServer removes one server from the member map and hash ring.
func (sch *ServerConsistentHash) RemoveServer(srvID string) {
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
// Supported key types are string, []byte and int64.
// int64 keys are hashed by their fixed-width binary encoding.
func (sch *ServerConsistentHash) Available(key any) bool {
	switch v := key.(type) {
	case string:
		return sch.AvailableString(v)
	case []byte:
		return sch.AvailableBytes(v)
	case int64:
		return sch.AvailableInt64(v)
	default:
		return false
	}
}

// AvailableString checks if the owner of the string key is the current server.
func (sch *ServerConsistentHash) AvailableString(key string) bool {
	sch.mu.RLock()
	defer sch.mu.RUnlock()
	return sch.chash.GetNode([]byte(key)) == sch.ID
}

// AvailableBytes checks if the owner of the byte key is the current server.
func (sch *ServerConsistentHash) AvailableBytes(key []byte) bool {
	sch.mu.RLock()
	defer sch.mu.RUnlock()
	return sch.chash.GetNode(key) == sch.ID
}

// AvailableInt64 checks if the owner of the int64 key is the current server.
func (sch *ServerConsistentHash) AvailableInt64(key int64) bool {
	sch.mu.RLock()
	defer sch.mu.RUnlock()
	return sch.chash.GetNode(int64KeyToBinaryBytes(key)) == sch.ID
}
