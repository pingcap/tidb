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
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

type scanTask struct {
	ident     ast.Ident
	delTaskCh chan<- interface{}
}

func (t *scanTask) execute(ctx context.Context, sctx sessionctx.Context, idx int) error {
	return runInTxn(ctx, sctx, func(ctx context.Context, exec *sqlExecutor) error {
		tbl, err := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema().TableByName(t.ident.Schema, t.ident.Name)
		if err != nil {
			return err
		}

		ttl, err := newTTLTable(t.ident.Schema, tbl.Meta())
		if err != nil {
			return err
		}

		expire := types.NewTimeDatum(types.NewTime(types.FromGoTime(time.Now().Add(-1*time.Minute)), ttl.GetTTLColumn().GetType(), 0))
		var pos []types.Datum
		var fieldTypes []*types.FieldType
		limit := 128
		for {
			sql, err := ttl.FormatQuerySQL(pos, nil, expire, limit)
			if err != nil {
				return err
			}

			logutil.BgLogger().Info("TTL scan", zap.String("SQL", sql), zap.Int("workerIdx", idx))
			rs, fields, err := exec.Execute(ctx, sql)
			if err != nil {
				return err
			}

			if len(rs) == 0 {
				break
			}

			if fieldTypes == nil {
				fieldTypes = make([]*types.FieldType, 0, len(fields))
				for _, field := range fields {
					fieldTypes = append(fieldTypes, &field.Column.FieldType)
				}
			}

			deletePKs := make([][]types.Datum, 0, len(rs))
			for i, row := range rs {
				datums := row.GetDatumRow(fieldTypes)
				deletePKs = append(deletePKs, datums)
				if i == len(rs)-1 {
					pos = datums
				}
			}

			t.delTaskCh <- &delTask{
				tbl:       ttl,
				deletePKs: deletePKs,
				expire:    expire,
			}

			if len(rs) < limit {
				break
			}
		}
		return nil
	})
}

type delTask struct {
	tbl       ttlTable
	deletePKs [][]types.Datum
	expire    types.Datum
}

func (t *delTask) execute(ctx context.Context, sctx sessionctx.Context, idx int) error {
	return runInTxnWithTableUnchanged(ctx, sctx, t.tbl, func(ctx context.Context, exec *sqlExecutor) error {
		deleteSQL, err := t.tbl.FormatDeleteSQL(t.deletePKs, t.expire)
		if err != nil {
			return err
		}

		logutil.BgLogger().Info("TTL delete", zap.String("SQL", deleteSQL), zap.Int("workerIdx", idx))
		if _, _, err = exec.Execute(ctx, deleteSQL); err != nil {
			return err
		}

		return nil
	})
}

type taskWorker struct {
	idx  int
	ctx  context.Context
	sctx sessionctx.Context
	recv <-chan interface{}
	stop func()
}

func (w *taskWorker) loop() {
	for {
		var err error
		select {
		case recv, ok := <-w.recv:
			if !ok {
				return
			}
			switch task := recv.(type) {
			case *scanTask:
				err = task.execute(w.ctx, w.sctx, w.idx)
			case *delTask:
				err = task.execute(w.ctx, w.sctx, w.idx)
			default:
			}
		case <-w.ctx.Done():
			return
		}

		if err != nil {
			terror.Log(err)
		}
	}
}

type sqlExecutor struct {
	restrictedExec sqlexec.RestrictedSQLExecutor
}

func (e *sqlExecutor) Execute(ctx context.Context, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	return e.restrictedExec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql)
}

func runInTxn(ctx context.Context, sctx sessionctx.Context, fn func(context.Context, *sqlExecutor) error) (err error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	restrictedExec, ok := sctx.(sqlexec.RestrictedSQLExecutor)
	if !ok {
		return errors.Errorf("'%T' is not a type of 'sqlexec.RestrictedSQLExecutor'", sctx)
	}

	exec := &sqlExecutor{restrictedExec: restrictedExec}
	if _, _, err = exec.Execute(ctx, "BEGIN"); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_, _, rollbackErr := exec.Execute(ctx, "ROLLBACK")
			terror.Log(rollbackErr)
			return
		}

		_, _, err = exec.Execute(ctx, "COMMIT")
	}()

	return fn(ctx, exec)
}

func runInTxnWithTableUnchanged(ctx context.Context, sctx sessionctx.Context, tbl ttlTable, fn func(context.Context, *sqlExecutor) error) error {
	return runInTxn(ctx, sctx, func(ctx context.Context, exec *sqlExecutor) error {
		txnTbl, err := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema().TableByName(tbl.Schema, tbl.Name)
		if err != nil {
			return err
		}

		if txnTbl.Meta().ID != tbl.ID {
			return errors.New("table changed")
		}

		return fn(ctx, exec)
	})
}
