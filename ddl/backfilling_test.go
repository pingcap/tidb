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

package ddl

import (
	"bytes"
	"context"
	"testing"

	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestDoneTaskKeeper(t *testing.T) {
	n := newDoneTaskKeeper(kv.Key("a"))
	n.updateNextKey(0, kv.Key("b"))
	n.updateNextKey(1, kv.Key("c"))
	require.True(t, bytes.Equal(n.nextKey, kv.Key("c")))
	require.Len(t, n.doneTaskNextKey, 0)

	n.updateNextKey(4, kv.Key("f"))
	require.True(t, bytes.Equal(n.nextKey, kv.Key("c")))
	require.Len(t, n.doneTaskNextKey, 1)
	n.updateNextKey(3, kv.Key("e"))
	n.updateNextKey(5, kv.Key("g"))
	require.True(t, bytes.Equal(n.nextKey, kv.Key("c")))
	require.Len(t, n.doneTaskNextKey, 3)
	n.updateNextKey(2, kv.Key("d"))
	require.True(t, bytes.Equal(n.nextKey, kv.Key("g")))
	require.Len(t, n.doneTaskNextKey, 0)

	n.updateNextKey(6, kv.Key("h"))
	require.True(t, bytes.Equal(n.nextKey, kv.Key("h")))
}

func TestPickBackfillType(t *testing.T) {
	originMgr := ingest.LitBackCtxMgr
	originInit := ingest.LitInitialized
	originFastReorg := variable.EnableFastReorg.Load()
	defer func() {
		ingest.LitBackCtxMgr = originMgr
		ingest.LitInitialized = originInit
		variable.EnableFastReorg.Store(originFastReorg)
	}()
	mockMgr := ingest.NewMockBackendCtxMgr(
		func() sessionctx.Context {
			return nil
		})
	ingest.LitBackCtxMgr = mockMgr
	mockCtx := context.Background()
	const uk = false
	mockJob := &model.Job{
		ID: 1,
		ReorgMeta: &model.DDLReorgMeta{
			ReorgTp: model.ReorgTypeTxn,
		},
	}
	variable.EnableFastReorg.Store(true)
	tp, err := pickBackfillType(mockCtx, mockJob, uk)
	require.NoError(t, err)
	require.Equal(t, tp, model.ReorgTypeTxn)

	mockJob.ReorgMeta.ReorgTp = model.ReorgTypeNone
	ingest.LitInitialized = false
	tp, err = pickBackfillType(mockCtx, mockJob, uk)
	require.NoError(t, err)
	require.Equal(t, tp, model.ReorgTypeTxnMerge)

	mockJob.ReorgMeta.ReorgTp = model.ReorgTypeNone
	ingest.LitInitialized = true
	tp, err = pickBackfillType(mockCtx, mockJob, uk)
	require.NoError(t, err)
	require.Equal(t, tp, model.ReorgTypeLitMerge)
}
