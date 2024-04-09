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

package importinto_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPostProcessStepExecutor(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	asInt := func(s string) int {
		v, err := strconv.Atoi(s)
		require.NoError(t, err)
		return v
	}

	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 2), (3, 4)")
	res := tk.MustQuery("admin checksum table t").Rows()
	stepMeta := &importinto.PostProcessStepMeta{
		Checksum: map[int64]importinto.Checksum{
			-1: {
				Sum:  uint64(asInt(res[0][2].(string))),
				KVs:  uint64(asInt(res[0][3].(string))),
				Size: uint64(asInt(res[0][4].(string))),
			},
		},
	}

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	taskMeta := &importinto.TaskMeta{
		Plan: importer.Plan{
			Checksum:         config.OpLevelRequired,
			TableInfo:        table.Meta(),
			DesiredTableInfo: table.Meta(),
			DBName:           "test",
		},
	}

	bytes, err := json.Marshal(stepMeta)
	require.NoError(t, err)
	executor := importinto.NewPostProcessStepExecutor(1, store, taskMeta, zap.NewExample())
	err = executor.RunSubtask(context.Background(), &proto.Subtask{Meta: bytes})
	require.NoError(t, err)

	tmp := stepMeta.Checksum[-1]
	tmp.Sum += 1
	stepMeta.Checksum[-1] = tmp
	bytes, err = json.Marshal(stepMeta)
	require.NoError(t, err)
	executor = importinto.NewPostProcessStepExecutor(1, store, taskMeta, zap.NewExample())
	err = executor.RunSubtask(context.Background(), &proto.Subtask{Meta: bytes})
	require.ErrorContains(t, err, "checksum mismatched remote vs local")

	taskMeta.Plan.Checksum = config.OpLevelOptional
	executor = importinto.NewPostProcessStepExecutor(1, store, taskMeta, zap.NewExample())
	err = executor.RunSubtask(context.Background(), &proto.Subtask{Meta: bytes})
	require.NoError(t, err)

	taskMeta.Plan.Checksum = config.OpLevelOff
	executor = importinto.NewPostProcessStepExecutor(1, store, taskMeta, zap.NewExample())
	err = executor.RunSubtask(context.Background(), &proto.Subtask{Meta: bytes})
	require.NoError(t, err)
}
