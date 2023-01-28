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

package statistics

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// StatsReader is used for simplify code that needs to read system tables in different sqls
// but requires the same transactions.
type StatsReader struct {
	ctx      sqlexec.RestrictedSQLExecutor
	snapshot uint64
}

// Read is a thin wrapper reading statistics from storage by sql command.
func (sr *StatsReader) Read(sql string, args ...interface{}) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	if sr.snapshot > 0 {
		return sr.ctx.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseSessionPool, sqlexec.ExecOptionWithSnapshot(sr.snapshot)}, sql, args...)
	}
	return sr.ctx.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql, args...)
}

// IsHistory indicates whether to read history statistics.
func (sr *StatsReader) IsHistory() bool {
	return sr.snapshot > 0
}

// GetStatsReader returns a StatsReader.
func GetStatsReader(snapshot uint64, exec sqlexec.RestrictedSQLExecutor) (reader *StatsReader, err error) {
	failpoint.Inject("mockGetStatsReaderFail", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("gofail genStatsReader error"))
		}
	})
	if snapshot > 0 {
		return &StatsReader{ctx: exec, snapshot: snapshot}, nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("getStatsReader panic %v", r)
		}
	}()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	failpoint.Inject("mockGetStatsReaderPanic", nil)
	_, err = exec.(sqlexec.SQLExecutor).ExecuteInternal(ctx, "begin")
	if err != nil {
		return nil, err
	}
	return &StatsReader{ctx: exec}, nil
}

// ReleaseStatsReader releases the reader.
func ReleaseStatsReader(reader *StatsReader) error {
	if reader.IsHistory() || reader.ctx == nil {
		return nil
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	_, err := reader.ctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, "commit")
	return err
}
