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

package util

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// StatsCtx is used to mark the request is from stats module.
func StatsCtx(ctx context.Context) context.Context {
	return kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
}

// FinishTransaction will execute `commit` when error is nil, otherwise `rollback`.
func FinishTransaction(ctx context.Context, exec interface{}, err error) error {
	sqlExec, ok := exec.(sqlexec.SQLExecutor)
	if !ok {
		return errors.Errorf("invalid sql executor")
	}
	if err == nil {
		_, err = sqlExec.ExecuteInternal(ctx, "commit")
	} else {
		_, err1 := sqlExec.ExecuteInternal(ctx, "rollback")
		terror.Log(errors.Trace(err1))
	}
	return errors.Trace(err)
}

// GetStartTS gets the start ts from current transaction.
func GetStartTS(sctx sessionctx.Context) (uint64, error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return 0, err
	}
	return txn.StartTS(), nil
}

// Read is a helper function to execute sql and return rows and fields.
func Read(exec interface{}, sql string, args ...interface{}) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	sqlExec, ok := exec.(sqlexec.RestrictedSQLExecutor)
	if !ok {
		return nil, nil, errors.Errorf("invalid sql executor")
	}
	return sqlExec.ExecRestrictedSQL(StatsCtx(context.Background()), []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql, args...)
}
