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

package importinto

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/importinto/mock"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestEncodeAndSortOperator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	executor := mock.NewMockMiniTaskExecutor(ctrl)
	backup := newImportMinimalTaskExecutor
	t.Cleanup(func() {
		newImportMinimalTaskExecutor = backup
	})
	newImportMinimalTaskExecutor = func(t *importStepMinimalTask) MiniTaskExecutor {
		return executor
	}

	executorForParam := &importStepExecutor{
		taskID: 1,
		taskMeta: &TaskMeta{
			Plan: importer.Plan{
				ThreadCnt: 2,
			},
		},
		tableImporter: &importer.TableImporter{
			LoadDataController: &importer.LoadDataController{
				Plan: &importer.Plan{
					CloudStorageURI: "",
				},
			},
		},
	}

	wctx := workerpool.NewContext(context.Background())
	source := operator.NewSimpleDataChannel(make(chan *importStepMinimalTask))
	op := newEncodeAndSortOperator(wctx, executorForParam, nil, 3, 1)
	op.SetSource(source)
	require.NoError(t, op.Open())
	require.Greater(t, len(op.String()), 0)

	// cancel on error
	mockErr := errors.New("mock err")
	executor.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockErr)
	source.Channel() <- &importStepMinimalTask{}
	require.Eventually(t, func() bool {
		return wctx.OperatorErr() != nil
	}, 3*time.Second, 300*time.Millisecond)
	require.Equal(t, mockErr, wctx.OperatorErr())
	// should not block
	<-wctx.Done()
	require.NoError(t, op.Close())
	require.ErrorIs(t, wctx.OperatorErr(), mockErr)

	// cancel on error and log other errors
	mockErr2 := errors.New("mock err 2")
	wctx = workerpool.NewContext(context.Background())
	source = operator.NewSimpleDataChannel(make(chan *importStepMinimalTask))
	op = newEncodeAndSortOperator(wctx, executorForParam, nil, 2, 2)
	op.SetSource(source)
	executor1 := mock.NewMockMiniTaskExecutor(ctrl)
	executor2 := mock.NewMockMiniTaskExecutor(ctrl)
	var id atomic.Int32
	newImportMinimalTaskExecutor = func(t *importStepMinimalTask) MiniTaskExecutor {
		if id.Add(1) == 1 {
			return executor1
		}
		return executor2
	}
	var wg sync.WaitGroup
	wg.Add(2)
	// wait until 2 executor start running, else workerpool will be cancelled.
	executor1.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, backend.EngineWriter, backend.EngineWriter) error {
			wg.Done()
			wg.Wait()
			return mockErr2
		})
	var err2 error
	executor2.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, backend.EngineWriter, backend.EngineWriter) error {
			wg.Done()
			wg.Wait()
			// wait error in executor1 has been processed
			require.Eventually(t, func() bool {
				return wctx.OperatorErr() != nil
			}, 3*time.Second, 300*time.Millisecond)
			err2 = errors.New("mock error from executor2")
			return err2
		})
	require.NoError(t, op.Open())
	// send 2 tasks
	source.Channel() <- &importStepMinimalTask{}
	source.Channel() <- &importStepMinimalTask{}
	// should not block
	<-wctx.Done()
	require.NoError(t, op.Close())
	require.ErrorIs(t, wctx.OperatorErr(), mockErr2)
	require.Equal(t, err2.Error(), "mock error from executor2")
}

func TestGetWriterMemorySizeLimit(t *testing.T) {
	cases := []struct {
		createSQL               string
		numOfIndexGenKV         int
		dataKVMemSizePerCon     uint64
		perIndexKVMemSizePerCon uint64
	}{
		{
			createSQL:           "create table t (a int)",
			numOfIndexGenKV:     0,
			dataKVMemSizePerCon: units.GiB,
		},
		{
			createSQL:           "create table t (a int primary key clustered)",
			numOfIndexGenKV:     0,
			dataKVMemSizePerCon: units.GiB,
		},
		{
			createSQL:               "create table t (a int primary key nonclustered)",
			numOfIndexGenKV:         1,
			dataKVMemSizePerCon:     768 * units.MiB,
			perIndexKVMemSizePerCon: 256 * units.MiB,
		},
		{
			createSQL:               "create table t (a int primary key clustered, b int, key(b))",
			numOfIndexGenKV:         1,
			dataKVMemSizePerCon:     768 * units.MiB,
			perIndexKVMemSizePerCon: 256 * units.MiB,
		},
		{
			createSQL:               "create table t (a int primary key clustered, b int, key(b), key(a,b))",
			numOfIndexGenKV:         2,
			dataKVMemSizePerCon:     644245094,
			perIndexKVMemSizePerCon: 214748364,
		},
		{
			createSQL:               "create table t (a int primary key clustered, b int, c int, key(b,c), unique(b), unique(c), key(a,b))",
			numOfIndexGenKV:         4,
			dataKVMemSizePerCon:     460175067,
			perIndexKVMemSizePerCon: 153391689,
		},
		{
			createSQL:               "create table t (a int, b int, c int, primary key(a,b,c) clustered, key(b,c), unique(b), unique(c), key(a,b))",
			numOfIndexGenKV:         4,
			dataKVMemSizePerCon:     460175067,
			perIndexKVMemSizePerCon: 153391689,
		},
		{
			createSQL:               "create table t (a int, b int, c int, primary key(a,b,c) nonclustered, key(b,c), unique(b), unique(c), key(a,b))",
			numOfIndexGenKV:         5,
			dataKVMemSizePerCon:     402653184,
			perIndexKVMemSizePerCon: 134217728,
		},
	}

	for _, c := range cases {
		p := parser.New()
		t.Run(c.createSQL, func(t *testing.T) {
			node, err := p.ParseOneStmt(c.createSQL, "", "")
			require.NoError(t, err)
			sctx := utilmock.NewContext()
			info, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), 1)
			require.NoError(t, err)
			info.State = model.StatePublic

			require.Equal(t, c.numOfIndexGenKV, getNumOfIndexGenKV(info), c.createSQL)
			dataKVMemSizePerCon, perIndexKVMemSizePerCon := getWriterMemorySizeLimit(&proto.StepResource{
				Mem: proto.NewAllocatable(2 * units.GiB),
			}, &importer.Plan{
				DesiredTableInfo: info,
				ThreadCnt:        1,
			})
			require.Equal(t, c.dataKVMemSizePerCon, dataKVMemSizePerCon, c.createSQL)
			if c.numOfIndexGenKV > 0 {
				require.Equal(t, c.perIndexKVMemSizePerCon, perIndexKVMemSizePerCon, c.createSQL)
			}
			require.LessOrEqual(t, c.dataKVMemSizePerCon+c.perIndexKVMemSizePerCon*uint64(c.numOfIndexGenKV), uint64(units.GiB))
		})
	}
}

func TestGetKVGroupBlockSize(t *testing.T) {
	require.Equal(t, 32*units.MiB, getKVGroupBlockSize(dataKVGroup))
	require.Equal(t, 16*units.MiB, getKVGroupBlockSize(""))
	require.Equal(t, 16*units.MiB, getKVGroupBlockSize("1"))
}
