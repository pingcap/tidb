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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type commitWorker struct {
	*InsertValues
	controller *importer.LoadDataController
}

// commitWork commit batch sequentially. When returns nil, it means the job is
// finished.
func (w *commitWorker) commitWork(ctx context.Context, inCh <-chan commitTask) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("commitWork panicked",
				zap.Any("r", r),
				zap.Stack("stack"))
			err = util.GetRecoverError(r)
		}
	}()

	var (
		taskCnt uint64
	)
	for {
		failpoint.Inject("CommitWorkError", func(_ failpoint.Value) {
			failpoint.Return(errors.New("mock commit work error"))
		})

		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-inCh:
			if !ok {
				return nil
			}
			start := time.Now()
			if err = w.commitOneTask(ctx, task); err != nil {
				return err
			}
			taskCnt++
			logutil.Logger(ctx).Info("commit one task success",
				zap.Duration("commit time usage", time.Since(start)),
				zap.Uint64("keys processed", task.cnt),
				zap.Uint64("taskCnt processed", taskCnt),
			)
		}
	}
}

// commitOneTask insert Data from LoadDataWorker.rows, then commit the modification
// like a statement.
func (w *commitWorker) commitOneTask(ctx context.Context, task commitTask) error {
	err := w.checkAndInsertOneBatch(ctx, task.rows, task.cnt)
	if err != nil {
		logutil.Logger(ctx).Error("commit error CheckAndInsert", zap.Error(err))
		return err
	}
	failpoint.Inject("commitOneTaskErr", func() {
		failpoint.Return(errors.New("mock commit one task error"))
	})
	return nil
}

func (w *commitWorker) checkAndInsertOneBatch(ctx context.Context, rows [][]types.Datum, cnt uint64) error {
	if w.stats != nil && w.stats.BasicRuntimeStats != nil {
		// Since this method will not call by executor Next,
		// so we need record the basic executor runtime stats by ourselves.
		start := time.Now()
		defer func() {
			w.stats.BasicRuntimeStats.Record(time.Since(start), 0)
		}()
	}
	var err error
	if cnt == 0 {
		return err
	}
	w.Ctx().GetSessionVars().StmtCtx.AddRecordRows(cnt)

	switch w.controller.OnDuplicate {
	case ast.OnDuplicateKeyHandlingReplace:
		return w.batchCheckAndInsert(ctx, rows[0:cnt], w.addRecordLD, true)
	case ast.OnDuplicateKeyHandlingIgnore:
		return w.batchCheckAndInsert(ctx, rows[0:cnt], w.addRecordLD, false)
	case ast.OnDuplicateKeyHandlingError:
		txn, err := w.Ctx().Txn(true)
		if err != nil {
			return err
		}
		dupKeyCheck := optimizeDupKeyCheckForNormalInsert(w.Ctx().GetSessionVars(), txn)
		for i, row := range rows[0:cnt] {
			sizeHintStep := int(w.Ctx().GetSessionVars().ShardAllocateStep)
			if sizeHintStep > 0 && i%sizeHintStep == 0 {
				sizeHint := sizeHintStep
				remain := len(rows[0:cnt]) - i
				if sizeHint > remain {
					sizeHint = remain
				}
				err = w.addRecordWithAutoIDHint(ctx, row, sizeHint, dupKeyCheck)
			} else {
				err = w.addRecord(ctx, row, dupKeyCheck)
			}
			if err != nil {
				return err
			}
			w.Ctx().GetSessionVars().StmtCtx.AddCopiedRows(1)
		}
		return nil
	default:
		return errors.Errorf("unknown on duplicate key handling: %v", w.controller.OnDuplicate)
	}
}

func (w *commitWorker) addRecordLD(ctx context.Context, row []types.Datum, dupKeyCheck table.DupKeyCheckMode) error {
	if row == nil {
		return nil
	}
	return w.addRecord(ctx, row, dupKeyCheck)
}

// GetInfilePath get infile path.
func (e *LoadDataWorker) GetInfilePath() string {
	return e.controller.Path
}

// GetController get load data controller.
// used in unit test.
func (e *LoadDataWorker) GetController() *importer.LoadDataController {
	return e.controller
}

// TestLoadLocal is a helper function for unit test.
func (e *LoadDataWorker) TestLoadLocal(parser mydump.Parser) error {
	if err := ResetContextOfStmt(e.UserSctx, &ast.LoadDataStmt{}); err != nil {
		return err
	}
	setNonRestrictiveFlags(e.UserSctx.GetSessionVars().StmtCtx)
	encoder, committer, err := initEncodeCommitWorkers(e)
	if err != nil {
		return err
	}

	ctx := context.Background()
	err = sessiontxn.NewTxn(ctx, e.UserSctx)
	if err != nil {
		return err
	}

	for range e.controller.IgnoreLines {
		//nolint: errcheck
		_ = parser.ReadRow()
	}

	err = encoder.readOneBatchRows(ctx, parser)
	if err != nil {
		return err
	}

	err = committer.checkAndInsertOneBatch(
		ctx,
		encoder.rows,
		encoder.curBatchCnt)
	if err != nil {
		return err
	}
	encoder.resetBatch()
	committer.Ctx().StmtCommit(ctx)
	err = committer.Ctx().CommitTxn(ctx)
	if err != nil {
		return err
	}
	e.setResult(encoder.exprWarnings)
	return nil
}

var _ io.ReadSeekCloser = (*SimpleSeekerOnReadCloser)(nil)

// SimpleSeekerOnReadCloser provides Seek(0, SeekCurrent) on ReadCloser.
type SimpleSeekerOnReadCloser struct {
	r   io.ReadCloser
	pos int
}

// NewSimpleSeekerOnReadCloser creates a SimpleSeekerOnReadCloser.
func NewSimpleSeekerOnReadCloser(r io.ReadCloser) *SimpleSeekerOnReadCloser {
	return &SimpleSeekerOnReadCloser{r: r}
}

// Read implements io.Reader.
func (s *SimpleSeekerOnReadCloser) Read(p []byte) (n int, err error) {
	n, err = s.r.Read(p)
	s.pos += n
	return
}

// Seek implements io.Seeker.
func (s *SimpleSeekerOnReadCloser) Seek(offset int64, whence int) (int64, error) {
	// only support get reader's current offset
	if offset == 0 && whence == io.SeekCurrent {
		return int64(s.pos), nil
	}
	return 0, errors.Errorf("unsupported seek on SimpleSeekerOnReadCloser, offset: %d whence: %d", offset, whence)
}

// Close implements io.Closer.
func (s *SimpleSeekerOnReadCloser) Close() error {
	return s.r.Close()
}

// GetFileSize implements objectio.Reader.
func (*SimpleSeekerOnReadCloser) GetFileSize() (int64, error) {
	return 0, errors.Errorf("unsupported GetFileSize on SimpleSeekerOnReadCloser")
}

// loadDataVarKeyType is a dummy type to avoid naming collision in context.
type loadDataVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (loadDataVarKeyType) String() string {
	return "load_data_var"
}
