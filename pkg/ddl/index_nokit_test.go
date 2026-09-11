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

package ddl

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	ddlmock "github.com/pingcap/tidb/pkg/ddl/mock"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBuildFullTextIndexInfo(t *testing.T) {
	tblInfo := &model.TableInfo{
		Name: pmodel.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("title"), Offset: 0, FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{Name: pmodel.NewCIStr("body"), Offset: 1, FieldType: *types.NewFieldType(mysql.TypeBlob)},
			{Name: pmodel.NewCIStr("id"), Offset: 2, FieldType: *types.NewFieldType(mysql.TypeLonglong)},
		},
	}
	parts := []*ast.IndexPartSpecification{
		{Column: &ast.ColumnName{Name: pmodel.NewCIStr("title")}, Length: types.UnspecifiedLength},
		{Column: &ast.ColumnName{Name: pmodel.NewCIStr("body")}, Length: types.UnspecifiedLength},
	}

	idx, err := buildFullTextIndexInfo(tblInfo, pmodel.NewCIStr("fts"), parts, nil, model.StateNone)
	require.NoError(t, err)
	require.Equal(t, pmodel.NewCIStr("fts"), idx.Name)
	require.Len(t, idx.Columns, 2)
	require.Equal(t, model.FullTextParserTypeStandardV1, idx.FullTextInfo.ParserType)
	require.True(t, idx.IsNonKVIndex())

	_, err = buildFullTextIndexInfo(tblInfo, pmodel.NewCIStr("bad"), []*ast.IndexPartSpecification{
		{Column: &ast.ColumnName{Name: pmodel.NewCIStr("id")}, Length: types.UnspecifiedLength},
	}, nil, model.StateNone)
	require.ErrorContains(t, err, "FULLTEXT index only supports string columns")
}

func TestModifyTaskParamLoop(t *testing.T) {
	type env struct {
		ctrl          *gomock.Controller
		ctx           context.Context
		sysTblMgr     *ddlmock.MockManager
		taskMgr       *mock.MockManager
		done          chan struct{}
		jobID, taskID int64

		currentJob  *model.JobW
		modifiedJob *model.JobW
	}
	newEnv := func(t *testing.T) *env {
		ctrl := gomock.NewController(t)
		bak := UpdateDDLJobReorgCfgInterval
		t.Cleanup(func() {
			ctrl.Finish()
			UpdateDDLJobReorgCfgInterval = bak
		})
		UpdateDDLJobReorgCfgInterval = 10 * time.Millisecond
		currentJob := &model.Job{ReorgMeta: &model.DDLReorgMeta{}}
		currentJob.ReorgMeta.SetConcurrency(1)
		currentJob.ReorgMeta.SetBatchSize(2)
		currentJob.ReorgMeta.SetMaxWriteSpeed(3)

		modifiedJob := &model.Job{ReorgMeta: &model.DDLReorgMeta{}}
		modifiedJob.ReorgMeta.SetConcurrency(4)
		modifiedJob.ReorgMeta.SetBatchSize(5)
		modifiedJob.ReorgMeta.SetMaxWriteSpeed(6)
		return &env{
			ctrl:      ctrl,
			ctx:       context.Background(),
			sysTblMgr: ddlmock.NewMockManager(ctrl),
			taskMgr:   mock.NewMockManager(ctrl),
			done:      make(chan struct{}),
			jobID:     int64(1),
			taskID:    int64(1),

			currentJob:  &model.JobW{Job: currentJob},
			modifiedJob: &model.JobW{Job: modifiedJob},
		}
	}
	t.Run("return on done", func(t *testing.T) {
		e := newEnv(t)
		close(e.done)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("retry on get job error; return on job not found", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, errors.New("some error"))
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, systable.ErrNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("adjust concurrency failed, retry", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.currentJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(0, errors.New("some error"))
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, systable.ErrNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("nothing modified, retry", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.currentJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, systable.ErrNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("detect modify, but the task has done", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(nil, storage.ErrTaskNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("detect modify, fail to get task, after retry, found task state is un-modifiable", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(nil, errors.New("some error"))
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(&proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateCancelling}}, nil)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(nil, storage.ErrTaskNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("detect modify, success after retry, and we update internal variable to avoid modify twice", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(&proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateRunning}}, nil)
		modifyParam := &proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyConcurrency, To: 4},
				{Type: proto.ModifyBatchSize, To: 5},
				{Type: proto.ModifyMaxWriteSpeed, To: 6},
			},
		}
		e.taskMgr.EXPECT().ModifyTaskByID(e.ctx, e.taskID, modifyParam).Return(errors.New("some error"))
		// retry and success
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(&proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateRunning}}, nil)
		e.taskMgr.EXPECT().ModifyTaskByID(e.ctx, e.taskID, modifyParam).Return(nil)
		// same param, but will continue this time, as nothing modified
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		// exit loop
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, systable.ErrNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("modify twice, both success", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(&proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateRunning}}, nil)
		modifyParam := &proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyConcurrency, To: 4},
				{Type: proto.ModifyBatchSize, To: 5},
				{Type: proto.ModifyMaxWriteSpeed, To: 6},
			},
		}
		e.taskMgr.EXPECT().ModifyTaskByID(e.ctx, e.taskID, modifyParam).Return(nil)
		// same param, but will continue this time, as nothing modified
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)

		modifiedJob2 := &model.JobW{Job: &model.Job{ReorgMeta: &model.DDLReorgMeta{}}}
		modifiedJob2.ReorgMeta.SetConcurrency(7)
		modifiedJob2.ReorgMeta.SetBatchSize(8)
		modifiedJob2.ReorgMeta.SetMaxWriteSpeed(9)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(modifiedJob2, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(&proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateRunning}}, nil)
		modifyParam2 := &proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyConcurrency, To: 7},
				{Type: proto.ModifyBatchSize, To: 8},
				{Type: proto.ModifyMaxWriteSpeed, To: 9},
			},
		}
		e.taskMgr.EXPECT().ModifyTaskByID(e.ctx, e.taskID, modifyParam2).Return(nil)
		// same param, but will continue this time, as nothing modified
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(modifiedJob2, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		// exit loop
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, systable.ErrNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})
}

func TestBuildFullTextInfoWithCheckParser(t *testing.T) {
	newIdxPart := func(name string) *ast.IndexPartSpecification {
		return &ast.IndexPartSpecification{
			Column: &ast.ColumnName{Name: pmodel.NewCIStr(name)},
			Length: types.UnspecifiedLength,
		}
	}
	tblInfo := &model.TableInfo{
		Name: pmodel.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("c1"), Offset: 0, FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{Name: pmodel.NewCIStr("c2"), Offset: 1, FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{Name: pmodel.NewCIStr("c3"), Offset: 2, FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{Name: pmodel.NewCIStr("c4"), Offset: 3, FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{Name: pmodel.NewCIStr("c5"), Offset: 4, FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
	}
	idxParts := []*ast.IndexPartSpecification{
		newIdxPart("c1"), newIdxPart("c2"), newIdxPart("c3"), newIdxPart("c4"), newIdxPart("c5"),
	}

	idx, err := buildFullTextIndexInfo(tblInfo, pmodel.NewCIStr("fts"), idxParts, nil, model.StateNone)
	require.NoError(t, err)
	require.Equal(t, model.FullTextParserTypeStandardV1, idx.FullTextInfo.ParserType)

	idx, err = buildFullTextIndexInfo(tblInfo, pmodel.NewCIStr("fts"), idxParts, &ast.IndexOption{ParserName: pmodel.NewCIStr("standard")}, model.StateNone)
	require.NoError(t, err)
	require.Equal(t, model.FullTextParserTypeStandardV1, idx.FullTextInfo.ParserType)

	idx, err = buildFullTextIndexInfo(tblInfo, pmodel.NewCIStr("fts"), idxParts, &ast.IndexOption{ParserName: pmodel.NewCIStr("ngram")}, model.StateNone)
	require.NoError(t, err)
	require.Equal(t, model.FullTextParserTypeNgramV1, idx.FullTextInfo.ParserType)
}

func TestFullTextParserConfigFromJob(t *testing.T) {
	job := &model.Job{SessionVars: make(map[string]string)}
	job.AddSessionVars("innodb_ft_min_token_size", "4")
	job.AddSessionVars("innodb_ft_max_token_size", "80")
	job.AddSessionVars("ngram_token_size", "3")
	job.AddSessionVars("innodb_ft_enable_stopword", "OFF")
	config, err := fullTextParserConfigFromJob(job)
	require.NoError(t, err)
	require.Equal(t, &model.FullTextParserConfig{
		InnodbFtMinTokenSize: 4, InnodbFtMaxTokenSize: 80,
		NgramTokenSize: 3, InnodbFtEnableStopword: false,
	}, config)
	// The TiCI request must continue to use the job, even if index metadata
	// carries a different snapshot (for example, from CREATE TABLE LIKE).
	index := &model.IndexInfo{FullTextInfo: &model.FullTextIndexInfo{
		ParserType:   model.FullTextParserTypeStandardV1,
		ParserConfig: &model.FullTextParserConfig{InnodbFtMinTokenSize: 9},
	}}
	info, err := (&worker{}).buildTiCIFulltextParserInfo(nil, job, index)
	require.NoError(t, err)
	require.Equal(t, "4", info.ParserParams["innodb_ft_min_token_size"])
	require.Equal(t, "80", info.ParserParams["innodb_ft_max_token_size"])
	require.Equal(t, "OFF", info.ParserParams["innodb_ft_enable_stopword"])
	require.Equal(t, 9, index.FullTextInfo.ParserConfig.InnodbFtMinTokenSize)
	defaults, err := fullTextParserConfigFromJob(&model.Job{})
	require.NoError(t, err)
	require.Equal(t, &model.FullTextParserConfig{
		InnodbFtMinTokenSize: 3, InnodbFtMaxTokenSize: 84,
		NgramTokenSize: 2, InnodbFtEnableStopword: true,
	}, defaults)
}

func TestTiCIAddPartitionParserSnapshot(t *testing.T) {
	job := &model.Job{Type: model.ActionAddTablePartition, SessionVars: map[string]string{
		"innodb_ft_min_token_size": "2", "innodb_ft_max_token_size": "84",
		"ngram_token_size": "2", "innodb_ft_enable_stopword": "ON",
	}}
	tbl := &model.TableInfo{}
	for i, minSize := range []int{5, 7} {
		tbl.Indices = append(tbl.Indices, &model.IndexInfo{ID: int64(i + 1), FullTextInfo: &model.FullTextIndexInfo{
			ParserType: model.FullTextParserTypeStandardV1,
			ParserConfig: &model.FullTextParserConfig{
				InnodbFtMinTokenSize: minSize, InnodbFtMaxTokenSize: 70,
				NgramTokenSize: 3, InnodbFtEnableStopword: false,
			},
		}})
	}
	tbl.Indices = append(tbl.Indices, &model.IndexInfo{ID: 3, FullTextInfo: &model.FullTextIndexInfo{
		ParserType: model.FullTextParserTypeNgramV1,
		ParserConfig: &model.FullTextParserConfig{
			InnodbFtMinTokenSize: 5, InnodbFtMaxTokenSize: 70,
			NgramTokenSize: 3, InnodbFtEnableStopword: false,
		},
	}})
	groups, err := (&worker{}).buildTiCIAddPartitionGroups(nil, job, tbl)
	require.NoError(t, err)
	require.Len(t, groups, 3)
	for _, group := range groups {
		require.Len(t, group.indexIDs, 1)
		params := group.parserInfo.ParserParams
		require.Equal(t, "OFF", params["innodb_ft_enable_stopword"])
		switch group.indexIDs[0] {
		case 1:
			require.Equal(t, "5", params["innodb_ft_min_token_size"])
			require.Equal(t, "70", params["innodb_ft_max_token_size"])
		case 2:
			require.Equal(t, "7", params["innodb_ft_min_token_size"])
		case 3:
			require.Equal(t, "3", params["ngram_token_size"])
		}
	}
	require.Equal(t, "2", job.SessionVars["innodb_ft_min_token_size"])
	require.Equal(t, "ON", job.SessionVars["innodb_ft_enable_stopword"])
	groups, err = (&worker{}).buildTiCIAddPartitionGroups(nil, &model.Job{}, tbl)
	require.NoError(t, err)
	require.Len(t, groups, 3)
	// Legacy metadata without a snapshot retains the existing job-based behavior.
	tbl.Indices = tbl.Indices[:1]
	tbl.Indices[0].FullTextInfo.ParserConfig = nil
	groups, err = (&worker{}).buildTiCIAddPartitionGroups(nil, job, tbl)
	require.NoError(t, err)
	require.Equal(t, "2", groups[0].parserInfo.ParserParams["innodb_ft_min_token_size"])
}
