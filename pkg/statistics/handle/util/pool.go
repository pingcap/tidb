// Copyright 2023 PingCAP, Inc.
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

package util

import (
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/resourcemanager/gpool"
)

// SessionPool is used to recycle sessionctx.
type SessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

// Pool is used to reuse goroutine and session.
type Pool interface {
	// GPool returns the goroutine pool.
	GPool() gpool.Pool

	// SPool returns the session pool.
	SPool() SessionPool

	// Close closes the goroutine pool.
	Close()
}

var _ Pool = (*pool)(nil)

type pool struct {
	// This gpool is used to reuse goroutine in the mergeGlobalStatsTopN.
	gpool gpool.Pool
	pool  SessionPool
}

// NewPool creates a new Pool.
func NewPool(gPool gpool.Pool, sessionPool SessionPool) Pool {
	return &pool{
		gpool: gPool,
		pool:  sessionPool,
	}
}

// GPool returns the goroutine pool.
func (p *pool) GPool() gpool.Pool {
	return p.gpool
}

// SPool returns the session pool.
func (p *pool) SPool() SessionPool {
	return p.pool
}

// Close close the goroutine pool.
func (p *pool) Close() {
	p.gpool.Close()
}
