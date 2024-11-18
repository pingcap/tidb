// Copyright 2019 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestFormatSQL(t *testing.T) {
	val := executor.FormatSQL("aaaa")
	require.Equal(t, "aaaa", val.String())
	variable.QueryLogMaxLen.Store(0)
	val = executor.FormatSQL("aaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaa", val.String())
	variable.QueryLogMaxLen.Store(5)
	val = executor.FormatSQL("aaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "aaaaa(len:20)", val.String())
}

func TestContextCancelWhenReadFromCopIterator(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")

	syncCh := make(chan struct{})
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/store/copr/CtxCancelBeforeReceive",
		func(ctx context.Context) {
			if ctx.Value("TestContextCancel") == "test" {
				syncCh <- struct{}{}
				<-syncCh
			}
		},
	))
	ctx := context.WithValue(context.Background(), "TestContextCancel", "test")
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx = util.WithInternalSourceType(ctx, "scheduler")
		rs, err := tk.Session().ExecuteInternal(ctx, "select * from test.t")
		require.NoError(t, err)
		_, err2 := session.ResultSetToStringSlice(ctx, tk.Session(), rs)
		require.ErrorIs(t, err2, context.Canceled)
	}()
	<-syncCh
	cancelFunc()
	syncCh <- struct{}{}
	wg.Wait()
}
