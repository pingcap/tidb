// Copyright 2022 PingCAP, Inc.
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

package local_test

import (
	"context"
	"testing"

	lkv "github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/keyspace"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestBuildDupTask(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table t (a int, b int, index idx(a), index idx(b));")
	require.NoError(t, err)
	info, err := ddl.MockTableInfo(mock.NewContext(), node[0].(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	info.State = model.StatePublic
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(0), info)
	require.NoError(t, err)

	// Test build duplicate detecting task.
	testCases := []struct {
		sessOpt       *lkv.SessionOptions
		hasTableRange bool
	}{
		{&lkv.SessionOptions{}, true},
		{&lkv.SessionOptions{IndexID: info.Indices[0].ID}, false},
		{&lkv.SessionOptions{IndexID: info.Indices[1].ID}, false},
	}
	for _, tc := range testCases {
		dupMgr, err := local.NewDuplicateManager(tbl, "t", nil, nil, keyspace.CodecV1, nil,
			tc.sessOpt, 4, atomic.NewBool(false), log.FromContext(context.Background()))
		require.NoError(t, err)
		tasks, err := local.BuildDuplicateTaskForTest(dupMgr)
		require.NoError(t, err)
		var hasRecordKey bool
		for _, task := range tasks {
			tableID, _, isRecordKey, err := tablecodec.DecodeKeyHead(task.StartKey)
			require.NoError(t, err)
			require.Equal(t, info.ID, tableID)
			if isRecordKey {
				hasRecordKey = true
			}
		}
		require.Equal(t, tc.hasTableRange, hasRecordKey)
	}
}
