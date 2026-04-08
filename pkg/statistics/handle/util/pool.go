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
	"math"
	"time"

	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/tiancaiamao/gp"
)

// Pool is used to reuse goroutine and session.
type Pool interface {
	// GPool returns the goroutine pool.
	GPool() *gp.Pool

	// SPool returns the session pool.
	SPool() syssession.Pool

	// Close closes the goroutine pool.
	Close()
}

var _ Pool = (*pool)(nil)

type pool struct {
	// This gpool is used to reuse goroutine in the mergeGlobalStatsTopN.
	gpool *gp.Pool
	pool  syssession.Pool
}

// NewPool creates a new Pool.
func NewPool(p syssession.Pool) Pool {
	return &pool{
		gpool: gp.New(math.MaxInt16, time.Minute),
		pool:  p,
	}
}

// GPool returns the goroutine pool.
func (p *pool) GPool() *gp.Pool {
	return p.gpool
}

// SPool returns the advanced session pool.
func (p *pool) SPool() syssession.Pool {
	return p.pool
}

// Close close the goroutine pool.
func (p *pool) Close() {
	p.gpool.Close()
}
