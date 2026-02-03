// Copyright 2025 PingCAP, Inc.
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

package importinto_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	dxfhandle "github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func writeConflictKVFile(t *testing.T, kvGroup string, objStore storeapi.Storage, kvs []*external.KVPair) *engineapi.ConflictInfo {
	t.Helper()
	ctx := context.Background()
	var summary *external.WriterSummary
	w := external.NewWriterBuilder().
		SetOnCloseFunc(func(s *external.WriterSummary) { summary = s }).
		Build(objStore, "/test", kvGroup)
	for _, kv := range kvs {
		require.NoError(t, w.WriteRow(ctx, kv.Key, kv.Value, nil))
	}
	require.NoError(t, w.Close(ctx))
	require.Len(t, summary.MultipleFilesStats, 1)
	return &engineapi.ConflictInfo{
		Count: uint64(len(kvs)),
		Files: []string{summary.MultipleFilesStats[0].Filenames[0][0]},
	}
}

func generateConflictKVFiles(t *testing.T, tempDir string, tbl table.Table) importinto.KVGroupConflictInfos {
	t.Helper()
	encodeCfg := &encode.EncodingConfig{
		Table:                tbl,
		UseIdentityAutoRowID: true,
	}
	controller := &importer.LoadDataController{
		ASTArgs: &importer.ASTArgs{},
		Plan:    &importer.Plan{},
		Table:   tbl,
	}
	localEncoder, err := importer.NewTableKVEncoderForDupResolve(encodeCfg, controller)
	require.NoError(t, err)

	dupDataKVs := make([]*external.KVPair, 0, 6)
	dupIndexKVs := make([]*external.KVPair, 0, 6)
	for i := range 3 {
		dupID := i + 1
		row := []types.Datum{types.NewDatum(dupID), types.NewDatum(dupID), types.NewDatum(dupID)}
		dupPairs, err2 := localEncoder.Encode(row, int64(dupID))
		require.NoError(t, err2)
		for _, pair := range dupPairs.Pairs {
			if tablecodec.IsRecordKey(pair.Key) {
				kv := &external.KVPair{Key: pair.Key, Value: pair.Val}
				dupDataKVs = append(dupDataKVs, kv, kv)
			} else {
				indexID, err := tablecodec.DecodeIndexID(pair.Key)
				require.NoError(t, err)
				if indexID == 2 {
					kv := &external.KVPair{Key: pair.Key, Value: pair.Val}
					dupIndexKVs = append(dupIndexKVs, kv, kv)
				}
			}
		}
	}
	ctx := context.Background()
	objStore, err := dxfhandle.NewObjStore(ctx, tempDir)
	require.NoError(t, err)

	return importinto.KVGroupConflictInfos{
		ConflictInfos: map[string]*engineapi.ConflictInfo{
			external.DataKVGroup:        writeConflictKVFile(t, external.DataKVGroup, objStore, dupDataKVs),
			external.IndexID2KVGroup(2): writeConflictKVFile(t, "2", objStore, dupIndexKVs),
		},
	}
}

type conflictedKVHandleContext struct {
	tempDir          string
	store            tidbkv.Storage
	logger           *zap.Logger
	taskMeta         *importinto.TaskMeta
	tk               *testkit.TestKit
	conflictedKVInfo importinto.KVGroupConflictInfos
}

func prepareConflictedKVHandleContext(t *testing.T) *conflictedKVHandleContext {
	t.Helper()
	tempDir := t.TempDir()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	ctx := context.Background()
	logger := zap.Must(zap.NewDevelopment())
	tk.MustExec("create table tc(a bigint primary key clustered, b int, c int, index(b), unique(c))")
	tk.MustExec("insert into tc values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")
	tk.MustQuery("select * from tc").Sort().Check(testkit.Rows("1 1 1", "2 2 2", "3 3 3", "4 4 4", "5 5 5"))
	tbl, err := do.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("tc"))
	require.NoError(t, err)

	// Note: this conflicted KVs doesn't exist in real world condition, we just
	// need them to generate conflict KV files for testing.
	conflictedKVInfo := generateConflictKVFiles(t, tempDir, tbl)

	taskMeta := &importinto.TaskMeta{
		Plan: importer.Plan{
			CloudStorageURI: tempDir,
			TableInfo:       tbl.Meta(),
			InImportInto:    true,
			Format:          importer.DataFormatCSV,
		},
		// we just need a valid SQL here to create TableImporter
		Stmt: "import into tc from '/local/file.txt'",
	}

	return &conflictedKVHandleContext{
		store:            store,
		logger:           logger,
		taskMeta:         taskMeta,
		tk:               tk,
		conflictedKVInfo: conflictedKVInfo,
	}
}

func runConflictedKVHandleStep(t *testing.T, subtask *proto.Subtask, stepExe execute.StepExecutor) {
	t.Helper()
	resource := &proto.StepResource{CPU: proto.NewAllocatable(1), Mem: proto.NewAllocatable(units.GiB)}
	execute.SetFrameworkInfo(stepExe, &proto.Task{TaskBase: proto.TaskBase{ID: 1}}, resource, nil, nil)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/createTableImporterForTest", `return(true)`)
	ctx := context.Background()
	require.NoError(t, stepExe.Init(ctx))
	require.NoError(t, stepExe.RunSubtask(ctx, subtask))
}

func TestConflictResolutionStepExecutor(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("skip test for next-gen kernel temporarily, we need to adapt the test later")
	}
	hdlCtx := prepareConflictedKVHandleContext(t)
	stMeta := importinto.ConflictResolutionStepMeta{Infos: hdlCtx.conflictedKVInfo}
	bytes, err := json.Marshal(stMeta)
	require.NoError(t, err)
	st := &proto.Subtask{SubtaskBase: proto.SubtaskBase{}, Meta: bytes}
	stepExe := importinto.NewConflictResolutionStepExecutor(&proto.TaskBase{RequiredSlots: 1}, hdlCtx.store, hdlCtx.taskMeta, hdlCtx.logger)
	runConflictedKVHandleStep(t, st, stepExe)
	hdlCtx.tk.MustQuery("select * from tc").Sort().Check(testkit.Rows("4 4 4", "5 5 5"))
}
