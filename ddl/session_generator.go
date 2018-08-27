// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"github.com/juju/errors"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
)

// sessionGenerator is used to new session.
type sessionGenerator struct {
	resPool *pools.ResourcePool
}

// getSessionCtx gets sessionctx from context resource pool.
// Please remember to call putSessionCtx after you getting the sessionctx.
func (sg *sessionGenerator) getSessionCtx() (sessionctx.Context, error) {
	if sg.resPool == nil {
		return mock.NewContext(), nil
	}

	resource, err := sg.resPool.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx := resource.(sessionctx.Context)
	ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusAutocommit, true)
	ctx.GetSessionVars().InRestrictedSQL = true
	return ctx, nil
}

// putSessionCtx returns sessionctx to context resource pool.
func (sg *sessionGenerator) putSessionCtx(ctx sessionctx.Context) {
	if sg.resPool == nil {
		return
	}

	sg.resPool.Put(ctx.(pools.Resource))
}

// close clean up the sessionGenerator.
func (sg *sessionGenerator) close() {
	if sg.resPool == nil {
		return
	}

	sg.resPool.Close()
}
