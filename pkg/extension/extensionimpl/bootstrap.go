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

package extensionimpl

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type bootstrapContext struct {
	context.Context

	sqlExecutor sqlexec.SQLExecutor
	etcdCli     *clientv3.Client
	sessionPool extension.SessionPool
}

func (c *bootstrapContext) ExecuteSQL(ctx context.Context, sql string) (rows []chunk.Row, err error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	rs, err := c.sqlExecutor.ExecuteInternal(ctx, sql)
	if err != nil {
		return nil, err
	}

	if rs == nil {
		return nil, nil
	}

	defer func() {
		closeErr := rs.Close()
		if err == nil {
			err = closeErr
		}
	}()

	return sqlexec.DrainRecordSet(ctx, rs, 8)
}

func (c *bootstrapContext) EtcdClient() *clientv3.Client {
	return c.etcdCli
}

func (c *bootstrapContext) SessionPool() extension.SessionPool {
	return c.sessionPool
}

// Bootstrap bootstraps all extensions
func Bootstrap(ctx context.Context, do *domain.Domain) error {
	extensions, err := extension.GetExtensions()
	if err != nil {
		return err
	}

	if extensions == nil {
		return nil
	}

	pool := do.SysSessionPool()
	r, err := pool.Get()
	if err != nil {
		return err
	}
	defer pool.Put(r)

	sctx, ok := r.(sessionctx.Context)
	if !ok {
		return errors.Errorf("type '%T' cannot be casted to 'sessionctx.Context'", sctx)
	}

	executor := sctx.GetSQLExecutor()
	return extensions.Bootstrap(&bootstrapContext{
		Context:     ctx,
		sessionPool: pool,
		sqlExecutor: executor,
		etcdCli:     do.GetEtcdClient(),
	})
}
