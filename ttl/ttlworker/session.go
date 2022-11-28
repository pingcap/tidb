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

package ttlworker

import (
	"context"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/ttl"
	"github.com/pingcap/tidb/util/sqlexec"
)

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

func getSession(pool sessionPool) (*ttl.Session, error) {
	resource, err := pool.Get()
	if err != nil {
		return nil, err
	}

	sctx, ok := resource.(sessionctx.Context)
	if !ok {
		pool.Put(resource)
		return nil, errors.Errorf("%T cannot be casted to sessionctx.Context", sctx)
	}

	exec, ok := resource.(sqlexec.SQLExecutor)
	if !ok {
		pool.Put(resource)
		return nil, errors.Errorf("%T cannot be casted to sqlexec.SQLExecutor", sctx)
	}

	se := &ttl.Session{
		Sctx:     sctx,
		Executor: exec,
		CloseFn: func() {
			pool.Put(resource)
		},
	}

	if _, err = se.ExecuteSQL(context.Background(), "commit"); err != nil {
		se.Close()
		return nil, err
	}

	return se, nil
}
