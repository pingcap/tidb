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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/mock"
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

// TestReorgExprContext is used in refactor stage to make sure the newReorgExprCtx() is
// compatible with newReorgSessCtx(nil).GetExprCtx() to make it safe to replace `mock.Context` usage.
// After refactor, the TestReorgExprContext can be removed.
func TestReorgExprContext(t *testing.T) {
	sctx := newReorgSessCtx(nil)
	sessCtx := sctx.GetExprCtx()
	exprCtx := newReorgExprCtx()
	cs1, col1 := sessCtx.GetCharsetInfo()
	cs2, col2 := exprCtx.GetCharsetInfo()
	require.Equal(t, cs1, cs2)
	require.Equal(t, col1, col2)
	require.Equal(t, sessCtx.GetDefaultCollationForUTF8MB4(), exprCtx.GetDefaultCollationForUTF8MB4())
	if sessCtx.GetBlockEncryptionMode() == "" {
		// The newReorgSessCtx returns a block encryption mode as an empty string.
		// Though it is not a valid value, it does not matter because `GetBlockEncryptionMode` is never used in DDL.
		// So we do not want to modify the behavior of `newReorgSessCtx` or `newReorgExprCtx`, and just to
		// place the test code here to check:
		// If `GetBlockEncryptionMode` still returns empty string in `newReorgSessCtx`, that means the behavior is
		// not changed, and we just need to return a default value for `newReorgExprCtx`.
		// If `GetBlockEncryptionMode` returns some other values, that means `GetBlockEncryptionMode` may have been
		// used in somewhere and two return values should be the same.
		require.Equal(t, "aes-128-ecb", exprCtx.GetBlockEncryptionMode())
	} else {
		require.Equal(t, sessCtx.GetBlockEncryptionMode(), exprCtx.GetBlockEncryptionMode())
	}
	require.Equal(t, sessCtx.GetSysdateIsNow(), exprCtx.GetSysdateIsNow())
	require.Equal(t, sessCtx.GetNoopFuncsMode(), exprCtx.GetNoopFuncsMode())
	require.Equal(t, sessCtx.IsUseCache(), exprCtx.IsUseCache())
	require.Equal(t, sessCtx.IsInNullRejectCheck(), exprCtx.IsInNullRejectCheck())
	require.Equal(t, sessCtx.ConnectionID(), exprCtx.ConnectionID())
	require.Equal(t, sessCtx.AllocPlanColumnID(), exprCtx.AllocPlanColumnID())
	require.Equal(t, sessCtx.GetWindowingUseHighPrecision(), exprCtx.GetWindowingUseHighPrecision())
	require.Equal(t, sessCtx.GetGroupConcatMaxLen(), exprCtx.GetGroupConcatMaxLen())

	evalCtx1 := sessCtx.GetEvalCtx()
	evalCtx := exprCtx.GetEvalCtx()
	require.Equal(t, evalCtx1.SQLMode(), evalCtx.SQLMode())
	tc1 := evalCtx1.TypeCtx()
	tc2 := evalCtx.TypeCtx()
	require.Equal(t, tc1.Flags(), tc2.Flags())
	require.Equal(t, tc1.Location().String(), tc2.Location().String())
	ec1 := evalCtx1.ErrCtx()
	ec2 := evalCtx.ErrCtx()
	require.Equal(t, ec1.LevelMap(), ec2.LevelMap())
	require.Equal(t, time.UTC, sctx.GetSessionVars().Location())
	require.Equal(t, time.UTC, sctx.GetSessionVars().StmtCtx.TimeZone())
	require.Equal(t, time.UTC, evalCtx1.Location())
	require.Equal(t, time.UTC, evalCtx.Location())
	require.Equal(t, evalCtx1.CurrentDB(), evalCtx.CurrentDB())
	tm1, err := evalCtx1.CurrentTime()
	require.NoError(t, err)
	tm2, err := evalCtx.CurrentTime()
	require.NoError(t, err)
	require.InDelta(t, tm1.Unix(), tm2.Unix(), 2)
	require.Equal(t, evalCtx1.GetMaxAllowedPacket(), evalCtx.GetMaxAllowedPacket())
	require.Equal(t, evalCtx1.GetDefaultWeekFormatMode(), evalCtx.GetDefaultWeekFormatMode())
	require.Equal(t, evalCtx1.GetDivPrecisionIncrement(), evalCtx.GetDivPrecisionIncrement())
}

type mockStorage struct {
	kv.Storage
	client kv.Client
}

func (s *mockStorage) GetClient() kv.Client {
	return s.client
}

// TestReorgExprContext is used in refactor stage to make sure the newDefaultReorgDistSQLCtx() is
// compatible with newReorgSessCtx(nil).GetDistSQLCtx() to make it safe to replace `mock.Context` usage.
// After refactor, the TestReorgExprContext can be removed.
func TestReorgDistSQLCtx(t *testing.T) {
	store := &mockStorage{client: &mock.Client{}}
	ctx1 := newReorgSessCtx(store).GetDistSQLCtx()
	ctx2 := newDefaultReorgDistSQLCtx(store.client)

	// set the same warnHandler to make two contexts equal
	ctx1.WarnHandler = ctx2.WarnHandler

	// set the same KVVars to make two contexts equal
	require.Equal(t, uint32(0), *ctx1.KVVars.Killed)
	require.Equal(t, uint32(0), *ctx2.KVVars.Killed)
	ctx1.KVVars.Killed = ctx2.KVVars.Killed

	// set the same SessionMemTracker to make two contexts equal
	require.Equal(t, ctx1.SessionMemTracker.Label(), ctx2.SessionMemTracker.Label())
	require.Equal(t, ctx1.SessionMemTracker.GetBytesLimit(), ctx2.SessionMemTracker.GetBytesLimit())
	ctx1.SessionMemTracker = ctx2.SessionMemTracker

	// set the same ErrCtx to make two contexts equal
	require.Equal(t, ctx1.ErrCtx.LevelMap(), ctx2.ErrCtx.LevelMap())
	require.Equal(t, 0, ctx2.WarnHandler.(contextutil.WarnHandler).WarningCount())
	ctx2.ErrCtx.AppendWarning(errors.New("warn"))
	require.Equal(t, 1, ctx2.WarnHandler.(contextutil.WarnHandler).WarningCount())
	ctx1.ErrCtx = ctx2.ErrCtx

	// set the same ExecDetails to make two contexts equal
	require.NotNil(t, ctx1.ExecDetails)
	require.NotNil(t, ctx2.ExecDetails)
	ctx1.ExecDetails = ctx2.ExecDetails

	require.Equal(t, ctx1, ctx2)
}
