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

package loaddata

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestProcessNormalFlow(t *testing.T) {
	flowHandle := &FlowHandle{}
	mockDispatcherHandle := &dispatcher.MockHandle{}

	dir := t.TempDir()
	path1 := "test1.csv"
	path2 := "test2.csv"
	content1 := []byte("1,1\r\n2,2\r\n3,3")
	content2 := []byte("4,4\r\n5,5\r\n6,6")
	require.NoError(t, os.WriteFile(filepath.Join(dir, path1), content1, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, path2), content2, 0o644))
	fileInfo1, err := os.Stat(filepath.Join(dir, path1))
	require.NoError(t, err)
	fileInfo2, err := os.Stat(filepath.Join(dir, path2))
	require.NoError(t, err)

	taskMeta := TaskMeta{
		Table: Table{
			DBName:        "db",
			Info:          &model.TableInfo{},
			TargetColumns: []string{"a", "b"},
			IsRowOrdered:  true,
		},
		Format: Format{
			Type: importer.LoadDataFormatDelimitedData,
		},
		Dir: dir,
		FileInfos: []FileInfo{
			{
				Path:     path1,
				Size:     fileInfo1.Size(),
				RealSize: fileInfo1.Size(),
			},
			{
				Path:     path2,
				Size:     fileInfo1.Size(),
				RealSize: fileInfo2.Size(),
			},
		},
	}
	bs, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	task := &proto.Task{
		Meta: bs,
	}

	mockDispatcherHandle.On("GetAllSchedulerIDs", mock.Anything, mock.Anything).Return([]string{"tidb1", "tidb2"}, nil).Once()
	subtaskMetas, err := flowHandle.ProcessNormalFlow(context.Background(), mockDispatcherHandle, task)
	require.NoError(t, err)
	require.Equal(t, task.Step, Import)
	require.Len(t, subtaskMetas, 1)
	subtaskMeta := &SubtaskMeta{}
	require.NoError(t, json.Unmarshal(subtaskMetas[0], subtaskMeta))
	require.Equal(t, subtaskMeta.Table, taskMeta.Table)
	require.Equal(t, subtaskMeta.Format, taskMeta.Format)
	require.Equal(t, subtaskMeta.Dir, dir)
	require.Len(t, subtaskMeta.Chunks, 2)
	require.Equal(t, subtaskMeta.Chunks[0], Chunk{
		Path:         path1,
		Offset:       0,
		EndOffset:    13,
		RealOffset:   0,
		PrevRowIDMax: 0,
		RowIDMax:     6,
	})
	require.Equal(t, subtaskMeta.Chunks[1], Chunk{
		Path:         path2,
		Offset:       0,
		EndOffset:    13,
		RealOffset:   0,
		PrevRowIDMax: 6,
		RowIDMax:     12,
	})

	subtaskMetas, err = flowHandle.ProcessNormalFlow(context.Background(), mockDispatcherHandle, task)
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 0)
	require.Equal(t, task.State, proto.TaskStateSucceed)
}

func TestProcessErrFlow(t *testing.T) {
	flowHandle := &FlowHandle{}
	mockDispatcherHandle := &dispatcher.MockHandle{}
	// add test if needed
	bs, err := flowHandle.ProcessErrFlow(context.Background(), mockDispatcherHandle, &proto.Task{}, "")
	require.NoError(t, err)
	require.Nil(t, bs)
}
