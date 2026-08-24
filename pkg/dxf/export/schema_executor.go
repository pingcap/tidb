// Copyright 2026 PingCAP, Inc.
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

package export

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// schemaConcurrencyMultiplier sizes schema-file concurrency relative to node
// CPU. Unlike Dump, every schema file is uniformly small (render text off an
// already-fetched TableInfo, then a small PUT), so there's no bandwidth-bound
// case to protect against, and this can go well past Dump's countCapMultiplier.
const schemaConcurrencyMultiplier = 8

// ShowCreateTableFunc and ShowCreateDatabaseFunc render CREATE TABLE/DATABASE
// text byte-identical to SHOW CREATE TABLE/DATABASE. pkg/dxf/export can't
// import pkg/executor directly for pkg/executor.ConstructResultOfShowCreate*
// (pkg/executor's EXPORT TABLE/SCHEMA statement executor imports
// pkg/dxf/export, so the reverse would cycle), so pkg/executor registers
// these at init time instead, the same indirection DXF's own scheduler/
// executor factories already use.
var (
	ShowCreateTableFunc    func(ctx sessionctx.Context, tableInfo *model.TableInfo, allocators autoid.Allocators, buf *bytes.Buffer) error
	ShowCreateDatabaseFunc func(ctx sessionctx.Context, dbInfo *model.DBInfo, ifNotExists bool, buf *bytes.Buffer) error
)

type schemaStepExecutor struct {
	taskexecutor.BaseStepExecutor
	taskMeta *TaskMeta
	store    kv.Storage
	taskTbl  taskexecutor.TaskTable
	logger   *zap.Logger

	objStore   storeapi.Storage
	tableRefs  []tableRef
	dbInfos    map[int64]*model.DBInfo
	dbFirstIdx map[int]struct{}
	summary    execute.SubtaskSummary
}

var _ execute.StepExecutor = (*schemaStepExecutor)(nil)

// Init implements execute.StepExecutor.
func (e *schemaStepExecutor) Init(ctx context.Context) error {
	objStore, err := objstore.NewFromURL(ctx, e.taskMeta.DestURI)
	if err != nil {
		return errors.Trace(err)
	}
	e.objStore = objStore

	tableInfos, dbInfos, err := snapshotTableInfos(e.store, e.taskMeta)
	if err != nil {
		return err
	}
	refs, err := e.taskMeta.tableRefs(tableInfos)
	if err != nil {
		return err
	}
	e.tableRefs = refs
	e.dbInfos = dbInfos
	e.dbFirstIdx = e.taskMeta.dbFirstTableIdxs()
	return nil
}

// RunSubtask implements execute.StepExecutor. Schema files are uniformly
// small and latency- rather than bandwidth-bound (see the PUT-latency
// benchmark in the design discussion), so a flat concurrency cap is enough;
// unlike Dump there's no large-chunk case to weight against.
func (e *schemaStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	stMeta := &SchemaSubtaskMeta{}
	if err := json.Unmarshal(subtask.Meta, stMeta); err != nil {
		return errors.Annotate(err, "unmarshal export schema subtask meta failed")
	}
	if stMeta.ExternalPath != "" {
		metaStore, err := extstore.GetGlobalExtStorage(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if err := stMeta.ReadJSONFromExternalStorage(ctx, metaStore, stMeta); err != nil {
			return errors.Trace(err)
		}
	}

	nodeCPU := max(1, int(e.GetResource().CPU.Capacity()))
	concurrency := schemaConcurrencyMultiplier * nodeCPU
	e.logger.Info("run export schema subtask",
		zap.Int64("subtask-id", subtask.ID),
		zap.Int("table-cnt", len(stMeta.TableIdxs)),
		zap.Int("concurrency", concurrency))

	sem := make(chan struct{}, concurrency)
	eg, egCtx := errgroup.WithContext(ctx)
	for _, idx := range stMeta.TableIdxs {
		select {
		case sem <- struct{}{}:
		case <-egCtx.Done():
			return eg.Wait()
		}
		eg.Go(func() error {
			defer func() { <-sem }()
			return e.exportTableSchema(egCtx, idx)
		})
	}
	return eg.Wait()
}

// exportTableSchema writes one table's CREATE TABLE file, plus its
// database's CREATE DATABASE file if idx is that database's first table.
// Rendering needs a sessionctx.Context (ShowCreateTableFunc reads session
// vars like charset/sql_mode); pkg/dxf/export can't create its own session
// (pkg/session imports pkg/dxf/export, so the reverse would cycle), so this
// borrows one from the DXF framework's session pool instead, the same way
// import-into's postProcessStepExecutor does.
func (e *schemaStepExecutor) exportTableSchema(ctx context.Context, idx int) error {
	ref := e.tableRefs[idx]

	var buf bytes.Buffer
	err := e.taskTbl.WithNewSession(func(se sessionctx.Context) error {
		return ShowCreateTableFunc(se, ref.tableInfo, autoid.Allocators{}, &buf)
	})
	if err != nil {
		return errors.Trace(err)
	}
	if err := e.objStore.WriteFile(ctx, schemaFileName(ref.dbName, ref.tableInfo.Name.O), buf.Bytes()); err != nil {
		return errors.Trace(err)
	}
	e.summary.Processed.Add(int64(buf.Len()))

	if _, ok := e.dbFirstIdx[idx]; !ok {
		return nil
	}
	dbInfo, ok := e.dbInfos[ref.dbID]
	if !ok {
		return errors.Errorf("export: database %d not found for schema rendering", ref.dbID)
	}
	var dbBuf bytes.Buffer
	err = e.taskTbl.WithNewSession(func(se sessionctx.Context) error {
		return ShowCreateDatabaseFunc(se, dbInfo, true, &dbBuf)
	})
	if err != nil {
		return errors.Trace(err)
	}
	if err := e.objStore.WriteFile(ctx, dbCreateFileName(ref.dbName), dbBuf.Bytes()); err != nil {
		return errors.Trace(err)
	}
	e.summary.Processed.Add(int64(dbBuf.Len()))
	return nil
}

// RealtimeSummary implements execute.StepExecutor.
func (e *schemaStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return &e.summary
}

// Cleanup implements execute.StepExecutor.
func (e *schemaStepExecutor) Cleanup(context.Context) error {
	if e.objStore != nil {
		e.objStore.Close()
	}
	return nil
}
