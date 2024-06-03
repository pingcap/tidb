// Copyright 2023 PingCAP, Inc.
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

package session

import (
	"fmt"
	"sync"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Pool is used to new Session.
type Pool struct {
	mu struct {
		sync.Mutex
		closed bool
	}
	resPool *pools.ResourcePool
	store   kv.Storage
}

// NewSessionPool creates a new Session pool.
func NewSessionPool(resPool *pools.ResourcePool, store kv.Storage) *Pool {
	intest.AssertNotNil(resPool)
	intest.AssertNotNil(store)
	return &Pool{resPool: resPool, store: store}
}

// Get gets sessionCtx from context resource pool.
// Please remember to call Put after you finished using sessionCtx.
func (sg *Pool) Get() (sessionctx.Context, error) {
	sg.mu.Lock()
	if sg.mu.closed {
		sg.mu.Unlock()
		return nil, errors.Errorf("session pool is closed")
	}
	sg.mu.Unlock()

	// no need to protect sg.resPool
	resource, err := sg.resPool.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, ok := resource.(sessionctx.Context)
	if !ok {
		return nil, errors.Trace(fmt.Errorf("need sessionctx.Context, but got %T", ctx))
	}
	ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusAutocommit, true)
	ctx.GetSessionVars().InRestrictedSQL = true
	ctx.GetSessionVars().StmtCtx.SetTimeZone(ctx.GetSessionVars().Location())
	infosync.StoreInternalSession(ctx)
	return ctx, nil
}

// Put returns sessionCtx to context resource pool.
func (sg *Pool) Put(ctx sessionctx.Context) {
	// no need to protect sg.resPool, even the sg.resPool is closed, the ctx still need to
	// Put into resPool, because when resPool is closing, it will wait all the ctx returns, then resPool finish closing.
	sg.resPool.Put(ctx.(pools.Resource))
	infosync.DeleteInternalSession(ctx)
}

// Close clean up the Pool.
func (sg *Pool) Close() {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	// prevent closing resPool twice.
	if sg.mu.closed {
		return
	}
	logutil.DDLLogger().Info("closing session pool")
	sg.resPool.Close()
	sg.mu.closed = true
}
