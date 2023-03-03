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

package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
)

type taskGetter struct {
	ctx context.Context
	t   *testing.T
	tk  *testkit.TestKit
}

func newTaskGetter(ctx context.Context, t *testing.T, tk *testkit.TestKit) *taskGetter {
	return &taskGetter{
		ctx, t, tk,
	}
}

func (tg *taskGetter) mustGetTestTask() *cache.TTLTask {
	sql, args := cache.SelectFromTTLTaskWithJobID("test-job")
	rs, err := tg.tk.Session().ExecuteInternal(tg.ctx, sql, args...)
	require.NoError(tg.t, err)
	rows, err := session.GetRows4Test(context.Background(), tg.tk.Session(), rs)
	require.NoError(tg.t, err)
	task, err := cache.RowToTTLTask(tg.tk.Session(), rows[0])
	require.NoError(tg.t, err)
	return task
}

func TestRowToTTLTask(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().TimeZone = time.Local

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)
	tg := newTaskGetter(ctx, t, tk)

	now := time.Now()
	now = now.Round(time.Second)

	sql, args, err := cache.InsertIntoTTLTask(tk.Session(), "test-job", 1, 1, nil, nil, now, now)
	require.NoError(t, err)
	// tk.MustExec cannot handle the NULL parameter, use the `tk.Session().ExecuteInternal` instead here.
	_, err = tk.Session().ExecuteInternal(ctx, sql, args...)
	require.NoError(t, err)
	task := tg.mustGetTestTask()
	require.Equal(t, "test-job", task.JobID)
	require.Equal(t, int64(1), task.TableID)
	require.Equal(t, int64(1), task.ScanID)
	require.Nil(t, task.ScanRangeStart)
	require.Nil(t, task.ScanRangeEnd)
	require.Equal(t, now, task.ExpireTime)
	require.Equal(t, now, task.CreatedTime)

	rangeStart, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx, []byte{}, []types.Datum{types.NewDatum(1)}...)
	require.NoError(t, err)
	rangeEnd, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx, []byte{}, []types.Datum{types.NewDatum(2)}...)
	require.NoError(t, err)
	tk.MustExec("UPDATE mysql.tidb_ttl_task SET scan_range_start = ?, scan_range_end = ? WHERE job_id = 'test-job'", rangeStart, rangeEnd)

	task = tg.mustGetTestTask()
	require.Equal(t, []types.Datum{types.NewDatum(1)}, task.ScanRangeStart)
	require.Equal(t, []types.Datum{types.NewDatum(2)}, task.ScanRangeEnd)
}

func TestInsertIntoTTLTask(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().TimeZone = time.Local

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)
	tg := newTaskGetter(ctx, t, tk)

	rangeStart := []types.Datum{types.NewDatum(1)}
	rangeEnd := []types.Datum{types.NewDatum(2)}

	now := time.Now()
	now = now.Round(time.Second)

	sql, args, err := cache.InsertIntoTTLTask(tk.Session(), "test-job", 1, 1, rangeStart, rangeEnd, now, now)
	require.NoError(t, err)
	// tk.MustExec cannot handle the NULL parameter, use the `tk.Session().ExecuteInternal` instead here.
	_, err = tk.Session().ExecuteInternal(ctx, sql, args...)
	require.NoError(t, err)
	task := tg.mustGetTestTask()
	require.Equal(t, "test-job", task.JobID)
	require.Equal(t, int64(1), task.TableID)
	require.Equal(t, int64(1), task.ScanID)
	require.Equal(t, []types.Datum{types.NewDatum(1)}, task.ScanRangeStart)
	require.Equal(t, []types.Datum{types.NewDatum(2)}, task.ScanRangeEnd)
	require.Equal(t, now, task.ExpireTime)
	require.Equal(t, now, task.CreatedTime)
}
