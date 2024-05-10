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

package bindinfo

import (
	"context"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// exec is a helper function to execute sql and return RecordSet.
func exec(sctx sessionctx.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	sqlExec := sctx.GetSQLExecutor()
	return sqlExec.ExecuteInternal(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo), sql, args...)
}

// execRows is a helper function to execute sql and return rows and fields.
func execRows(sctx sessionctx.Context, sql string, args ...any) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	sqlExec := sctx.GetRestrictedSQLExecutor()
	return sqlExec.ExecRestrictedSQL(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo),
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql, args...)
}

// SessionPool is used to recycle sessionctx.
type SessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}
