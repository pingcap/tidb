// Copyright 2025 PingCAP, Inc.
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

package importinto

import (
	"context"
	"encoding/json"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	dxfhandle "github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type conflictResolutionStepExecutor struct {
	taskexecutor.EmptyStepExecutor
	taskID   int64
	store    tidbkv.Storage
	taskMeta *TaskMeta
	logger   *zap.Logger

	tableImporter *importer.TableImporter
}

var _ execute.StepExecutor = &conflictResolutionStepExecutor{}

func (e *conflictResolutionStepExecutor) Init(ctx context.Context) error {
	tableImporter, err := getTableImporter(ctx, e.taskID, e.taskMeta, e.store)
	if err != nil {
		return err
	}
	e.tableImporter = tableImporter
	return nil
}

func (e *conflictResolutionStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := e.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()
	stepMeta := &ConflictResolutionStepMeta{}
	if err = json.Unmarshal(subtask.Meta, stepMeta); err != nil {
		return errors.Trace(err)
	}
	if stepMeta.ExternalPath != "" {
		if err := stepMeta.ReadJSONFromExternalStorage(ctx, e.tableImporter.GlobalSortStore, stepMeta); err != nil {
			return errors.Trace(err)
		}
	}
	for kvGroup, ci := range stepMeta.Infos.ConflictInfos {
		err = e.resolveConflictsOfKVGroup(ctx, subtask.Concurrency, kvGroup, ci)
		failpoint.InjectCall("afterResolveOneKVGroup", &err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *conflictResolutionStepExecutor) resolveConflictsOfKVGroup(
	ctx context.Context,
	concurrency int,
	kvGroup string,
	ci *common.ConflictInfo,
) (err error) {
	failpoint.Inject("forceHandleConflictsBySingleThread", func() {
		concurrency = 1
	})
	task := log.BeginTask(e.logger.With(
		zap.String("kvGroup", kvGroup), zap.Uint64("duplicates", ci.Count),
		zap.Int("file-count", len(ci.Files)), zap.Int("concurrency", concurrency),
	), "resolve conflicts of kv group")

	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	eg, egCtx := tidbutil.NewErrorGroupWithRecoverWithCtx(ctx)

	pairCh := startReadFiles(egCtx, eg, e.tableImporter.GlobalSortStore, ci.Files)

	keysToDeleteCh := make(chan []tidbkv.Key)
	handlers, deleters, err := e.buildHandlersAndDeleters(egCtx, concurrency, kvGroup, keysToDeleteCh)
	if err != nil {
		return errors.Trace(err)
	}
	var finishedHandlers atomic.Int32
	for i := 0; i < concurrency; i++ {
		handler, deleter := handlers[i], deleters[i]
		eg.Go(func() error {
			defer func() {
				count := finishedHandlers.Add(1)
				if int(count) == concurrency {
					close(keysToDeleteCh)
				}
			}()
			if err := handler.run(egCtx, pairCh); err != nil {
				_ = handler.close(egCtx)
				return err
			}
			return handler.close(egCtx)
		})
		eg.Go(func() error {
			return deleter.run(egCtx)
		})
	}
	return eg.Wait()
}

func (e *conflictResolutionStepExecutor) buildHandlersAndDeleters(
	ctx context.Context, concurrency int, kvGroup string, keysToDeleteCh chan []tidbkv.Key,
) (handlers []conflictKVHandler, deleters []*conflictKVDeleter, err error) {
	handlers = make([]conflictKVHandler, 0, concurrency)
	deleters = make([]*conflictKVDeleter, 0, concurrency)
	defer func() {
		if err != nil {
			for _, hdl := range handlers {
				_ = hdl.close(ctx)
			}
		}
	}()
	for range concurrency {
		handler, deleter := e.getHandlerAndDeleter(kvGroup, keysToDeleteCh)
		// when create encoder, if the table have generated column, when calling
		// backend/kv.CollectGeneratedColumns(), buildSimpleExpr will rewrite the
		// AST node, and data race. and the data race might happen during encoding,
		// in EvalGeneratedColumns, so we have to finish initialize all handlers
		// before running them.
		if err = handler.init(); err != nil {
			return nil, nil, err
		}
		handlers = append(handlers, handler)
		deleters = append(deleters, deleter)
	}
	return handlers, deleters, nil
}

func (e *conflictResolutionStepExecutor) getHandlerAndDeleter(kvGroup string, keysToDelCh chan []tidbkv.Key) (
	conflictKVHandler, *conflictKVDeleter) {
	deleter := &conflictKVDeleter{
		keysCh: keysToDelCh,
		store:  e.store,
		logger: e.logger,
	}
	baseHandler := &baseConflictKVHandler{
		tableImporter: e.tableImporter,
		store:         e.store,
		logger:        e.logger,
		kvGroup:       kvGroup,
		deleter:       deleter,
	}
	dataKVHandler := &conflictDataKVHandler{baseConflictKVHandler: baseHandler}
	baseHandler.handleFn = dataKVHandler.handle
	var handler conflictKVHandler = dataKVHandler
	if kvGroup != dataKVGroup {
		indexKVHandler := &conflictIndexKVHandler{baseConflictKVHandler: baseHandler}
		baseHandler.handleFn = indexKVHandler.handle
		handler = indexKVHandler
	}
	return handler, deleter
}

func (e *conflictResolutionStepExecutor) Cleanup(_ context.Context) (err error) {
	e.logger.Info("cleanup subtask env")
	return e.tableImporter.Close()
}

type conflictKVDeleter struct {
	keysCh chan []tidbkv.Key
	store  tidbkv.Storage
	logger *zap.Logger
}

func (d *conflictKVDeleter) getCh() chan []tidbkv.Key {
	return d.keysCh
}

func (d *conflictKVDeleter) run(ctx context.Context) error {
	for keys := range d.keysCh {
		if err := d.deleteKeysWithRetry(ctx, keys); err != nil {
			return err
		}
	}
	return nil
}

func (d *conflictKVDeleter) deleteKeysWithRetry(ctx context.Context, keys []tidbkv.Key) error {
	if len(keys) == 0 {
		return nil
	}
	backoffer := backoff.NewExponential(storeOpMinBackoff, 2, storeOpMaxBackoff)
	return dxfhandle.RunWithRetry(ctx, storeOpMaxRetryCnt, backoffer, d.logger, func(ctx context.Context) (bool, error) {
		err := d.deleteBufferedKeys(ctx, keys)
		if err != nil {
			return common.IsRetryableError(err), err
		}
		return true, nil
	})
}

func (d *conflictKVDeleter) deleteBufferedKeys(ctx context.Context, keys []tidbkv.Key) error {
	txn, err := d.store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err == nil {
			err = txn.Commit(ctx)
		} else {
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				d.logger.Warn("failed to rollback transaction", zap.Error(rollbackErr))
			}
		}
	}()

	for _, k := range keys {
		if err = txn.Delete(k); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
