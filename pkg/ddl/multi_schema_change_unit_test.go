// Copyright 2024 PingCAP, Inc.
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
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func cloneSubJobs(subJobs []*model.SubJob) []*model.SubJob {
	ret := make([]*model.SubJob, len(subJobs))
	for i, j := range subJobs {
		cloned := *j
		if len(j.Args) > 0 {
			cloned.Args = make([]any, len(j.Args))
			copy(cloned.Args, j.Args)
		}
		ret[i] = &cloned
	}
	return ret
}

func TestMergeAddIndex(t *testing.T) {
	subJobs := []*model.SubJob{
		{Type: model.ActionAddIndex, Args: []any{
			false, model.NewCIStr("job1"), []*ast.IndexPartSpecification{}, &ast.IndexOption{}, []*model.ColumnInfo{}, false,
		}},
		{Type: model.ActionAddColumn},
		{Type: model.ActionAddIndex, Args: []any{
			false, model.NewCIStr("job2"), []*ast.IndexPartSpecification{}, &ast.IndexOption{}, []*model.ColumnInfo{}, false,
		}},
		{Type: model.ActionAddIndex, Args: []any{
			false, model.NewCIStr("job3"), []*ast.IndexPartSpecification{}, &ast.IndexOption{}, []*model.ColumnInfo{}, false,
		}},
		{Type: model.ActionAddIndex, Args: []any{
			false, model.NewCIStr("job4"), []*ast.IndexPartSpecification{}, &ast.IndexOption{}, []*model.ColumnInfo{}, false,
		}},
	}

	info := &model.MultiSchemaInfo{SubJobs: cloneSubJobs(subJobs)}
	mergeAddIndex(info, model.ReorgTypeTxnMerge, 0, 0)
	require.Len(t, info.SubJobs, 2)
	require.Equal(t, model.ActionAddColumn, info.SubJobs[0].Type)
	require.Equal(t, model.ActionAddIndex, info.SubJobs[1].Type)
	require.Equal(t, []model.CIStr{
		model.NewCIStr("job1"), model.NewCIStr("job2"), model.NewCIStr("job3"), model.NewCIStr("job4"),
	}, info.SubJobs[1].Args[1])

	info = &model.MultiSchemaInfo{SubJobs: cloneSubJobs(subJobs)}
	mergeAddIndex(info, model.ReorgTypeLitMerge, math.MaxInt64, 1)
	require.Len(t, info.SubJobs, 2)
	require.Equal(t, model.ActionAddColumn, info.SubJobs[0].Type)
	require.Equal(t, model.ActionAddIndex, info.SubJobs[1].Type)
	require.Equal(t, []model.CIStr{
		model.NewCIStr("job1"), model.NewCIStr("job2"), model.NewCIStr("job3"), model.NewCIStr("job4"),
	}, info.SubJobs[1].Args[1])

	info = &model.MultiSchemaInfo{SubJobs: cloneSubJobs(subJobs)}
	mergeAddIndex(info, model.ReorgTypeLitMerge, 1, 100)
	require.Equal(t, subJobs, info.SubJobs)

	// merge every 2 subjobs
	info = &model.MultiSchemaInfo{SubJobs: cloneSubJobs(subJobs)}
	mergeAddIndex(info, model.ReorgTypeLitMerge, 4*config.DefaultLocalWriterMemCacheSize, 2)
	require.Len(t, info.SubJobs, 3)
	require.Equal(t, model.ActionAddColumn, info.SubJobs[0].Type)
	require.Equal(t, model.ActionAddIndex, info.SubJobs[1].Type)
	require.Equal(t, []model.CIStr{
		model.NewCIStr("job1"), model.NewCIStr("job2"),
	}, info.SubJobs[1].Args[1])
	require.Equal(t, []model.CIStr{
		model.NewCIStr("job3"), model.NewCIStr("job4"),
	}, info.SubJobs[2].Args[1])

	// merge every 3 subjobs
	info = &model.MultiSchemaInfo{SubJobs: cloneSubJobs(subJobs)}
	mergeAddIndex(info, model.ReorgTypeLitMerge, 7*config.DefaultLocalWriterMemCacheSize, 2)
	require.Len(t, info.SubJobs, 3)
	require.Equal(t, model.ActionAddColumn, info.SubJobs[0].Type)
	require.Equal(t, model.ActionAddIndex, info.SubJobs[1].Type)
	require.Equal(t, []model.CIStr{
		model.NewCIStr("job1"), model.NewCIStr("job2"), model.NewCIStr("job3"),
	}, info.SubJobs[1].Args[1])
	require.Equal(t, []model.CIStr{
		model.NewCIStr("job4"),
	}, info.SubJobs[2].Args[1])
}
