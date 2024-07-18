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

	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
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
	defer func() {
		ingest.LitBackCtxMgr = originMgr
		ingest.LitInitialized = originInit
	}()
	mockMgr := ingest.NewMockBackendCtxMgr(
		func() sessionctx.Context {
			return nil
		})
	ingest.LitBackCtxMgr = mockMgr
	mockCtx := context.Background()
	mockJob := &model.Job{
		ID: 1,
		ReorgMeta: &model.DDLReorgMeta{
			ReorgTp: model.ReorgTypeTxn,
		},
	}
	mockJob.ReorgMeta.IsFastReorg = true
	tp, err := pickBackfillType(mockCtx, mockJob)
	require.NoError(t, err)
	require.Equal(t, tp, model.ReorgTypeTxn)

	mockJob.ReorgMeta.ReorgTp = model.ReorgTypeNone
	ingest.LitInitialized = false
	tp, err = pickBackfillType(mockCtx, mockJob)
	require.NoError(t, err)
	require.Equal(t, tp, model.ReorgTypeTxnMerge)

	mockJob.ReorgMeta.ReorgTp = model.ReorgTypeNone
	ingest.LitInitialized = true
	tp, err = pickBackfillType(mockCtx, mockJob)
	require.NoError(t, err)
	require.Equal(t, tp, model.ReorgTypeLitMerge)
}

func TestValidateAndFillRanges(t *testing.T) {
	mkRange := func(start, end string) kv.KeyRange {
		return kv.KeyRange{StartKey: []byte(start), EndKey: []byte(end)}
	}
	ranges := []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "d"),
		mkRange("d", "e"),
	}
	err := validateAndFillRanges(ranges, []byte("a"), []byte("e"))
	require.NoError(t, err)
	require.EqualValues(t, []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "d"),
		mkRange("d", "e"),
	}, ranges)

	// adjust first and last range.
	ranges = []kv.KeyRange{
		mkRange("a", "c"),
		mkRange("c", "e"),
		mkRange("e", "g"),
	}
	err = validateAndFillRanges(ranges, []byte("b"), []byte("f"))
	require.NoError(t, err)
	require.EqualValues(t, []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "e"),
		mkRange("e", "f"),
	}, ranges)

	// first range startKey and last range endKey are empty.
	ranges = []kv.KeyRange{
		mkRange("", "c"),
		mkRange("c", "e"),
		mkRange("e", ""),
	}
	err = validateAndFillRanges(ranges, []byte("b"), []byte("f"))
	require.NoError(t, err)
	require.EqualValues(t, []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "e"),
		mkRange("e", "f"),
	}, ranges)
	ranges = []kv.KeyRange{
		mkRange("", "c"),
		mkRange("c", ""),
	}
	err = validateAndFillRanges(ranges, []byte("b"), []byte("f"))
	require.NoError(t, err)
	require.EqualValues(t, []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "f"),
	}, ranges)

	// invalid range.
	ranges = []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", ""),
		mkRange("e", "f"),
	}
	err = validateAndFillRanges(ranges, []byte("b"), []byte("f"))
	require.Error(t, err)

	ranges = []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "d"),
		mkRange("e", "f"),
	}
	err = validateAndFillRanges(ranges, []byte("b"), []byte("f"))
	require.Error(t, err)
}
