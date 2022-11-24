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

package ttl

import (
	"context"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

type Session struct {
	Sctx     sessionctx.Context
	Executor sqlexec.SQLExecutor
	CloseFn  func()
}

func (s *Session) GetDomainInfoSchema() infoschema.InfoSchema {
	is, ok := s.Sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	if !ok {
		return nil
	}

	if ext, ok := is.(*infoschema.SessionExtendedInfoSchema); ok {
		return ext.InfoSchema
	}

	return is
}

func (s *Session) ExecuteSQL(ctx context.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnTTL)
	rs, err := s.Executor.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	if rs == nil {
		return nil, nil
	}

	defer func() {
		terror.Log(rs.Close())
	}()

	return sqlexec.DrainRecordSet(ctx, rs, 8)
}

func (s *Session) Close() {
	if s.CloseFn != nil {
		s.CloseFn()
	}
}
