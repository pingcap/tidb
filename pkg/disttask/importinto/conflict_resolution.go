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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	dxfhandle "github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/importinto/conflictedkv"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/log"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type conflictResolutionStepExecutor struct {
	taskexecutor.BaseStepExecutor
	taskID   int64
	store    tidbkv.Storage
	taskMeta *TaskMeta
	logger   *zap.Logger

	tableImporter *importer.TableImporter
	summary       execute.SubtaskSummary
}

var _ execute.StepExecutor = &conflictResolutionStepExecutor{}

// NewConflictResolutionStepExecutor creates a new StepExecutor for conflict
// resolution step, exported for test.
func NewConflictResolutionStepExecutor(
	taskID int64,
	store tidbkv.Storage,
	taskMeta *TaskMeta,
	logger *zap.Logger,
) execute.StepExecutor {
	return &conflictResolutionStepExecutor{
		taskID:   taskID,
		taskMeta: taskMeta,
		logger:   logger,
		store:    store,
	}
}

func (e *conflictResolutionStepExecutor) Init(ctx context.Context) error {
	tableImporter, err := getTableImporter(ctx, e.taskID, e.taskMeta, e.store, e.logger)
	if err != nil {
		return err
	}
	e.tableImporter = tableImporter
	return nil
}

func (e *conflictResolutionStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	accessRec, objStore, err := dxfhandle.NewObjStoreWithRecording(ctx, e.taskMeta.Plan.CloudStorageURI)
	if err != nil {
		return err
	}
	defer func() {
		objStore.Close()
		e.summary.MergeObjStoreRequests(&accessRec.Requests)
		e.GetMeterRecorder().MergeObjStoreAccess(accessRec)
	}()
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
		if err := stepMeta.ReadJSONFromExternalStorage(ctx, objStore, stepMeta); err != nil {
			return errors.Trace(err)
		}
	}
	// since we have separate the collection of conflict info from the resolution,
	// it's possible to resolve different kv groups in parallel, we can enhance
	// it later.
	for kvGroup, ci := range stepMeta.Infos.ConflictInfos {
		err = e.resolveConflictsOfKVGroup(ctx, objStore, subtask.Concurrency, kvGroup, ci)
		failpoint.InjectCall("afterResolveOneKVGroup", &err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *conflictResolutionStepExecutor) resolveConflictsOfKVGroup(
	ctx context.Context,
	objStore storage.ExternalStorage,
	concurrency int,
	kvGroup string,
	ci *engineapi.ConflictInfo,
) (err error) {
	failpoint.Inject("forceHandleConflictsBySingleThread", func() {
		concurrency = 1
	})
	task := log.BeginTask(e.logger.With(
		zap.String("kvGroup", kvGroup), zap.Uint64("duplicates", ci.Count),
		zap.Int("fileCount", len(ci.Files)), zap.Int("concurrency", concurrency),
	), "resolve conflicts of kv group")

	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	encoders, err := createEncoders(concurrency, e.tableImporter)
	if err != nil {
		return err
	}

	eg, egCtx := tidbutil.NewErrorGroupWithRecoverWithCtx(ctx)
	pairCh := external.ReadKVFilesAsync(egCtx, eg, objStore, ci.Files)
	for i := range concurrency {
		encoder := encoders[i]
		deleter := conflictedkv.NewDeleter(e.tableImporter.Table, e.logger, e.store, kvGroup, encoder)
		eg.Go(func() error {
			return deleter.Run(egCtx, pairCh)
		})
	}

	return eg.Wait()
}

func (e *conflictResolutionStepExecutor) Cleanup(_ context.Context) (err error) {
	e.logger.Info("cleanup subtask env")
	return e.tableImporter.Close()
}

func (e *conflictResolutionStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	e.summary.Update()
	return &e.summary
}

func (e *conflictResolutionStepExecutor) ResetSummary() {
	e.summary.Reset()
}

// when create encoder, if the table have generated column, when calling
// backend/kv.CollectGeneratedColumns(), buildSimpleExpr will rewrite the AST node,
// and data race. and the data race might happen during encoding, in
// EvalGeneratedColumns, so we have to finish initialize all encoders before
// running them.
func createEncoders(concurrency int, tableImporter *importer.TableImporter) (encoders []*importer.TableKVEncoder, err error) {
	encoders = make([]*importer.TableKVEncoder, 0, concurrency)
	defer func() {
		if err != nil {
			for _, encoder := range encoders {
				_ = encoder.Close()
			}
		}
	}()
	for range concurrency {
		encoder, err := tableImporter.GetKVEncoderForDupResolve()
		if err != nil {
			return nil, err
		}
		encoders = append(encoders, encoder)
	}
	return encoders, nil
}
