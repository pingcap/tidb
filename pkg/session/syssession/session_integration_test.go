// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syssession_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestDomainAdvancedSessionPoolInternalSessionRegistry(t *testing.T) {
	_, do := testkit.CreateMockStoreAndDomain(t)
	p := do.AdvancedSysSessionPool()
	require.NotNil(t, p)

	sessManager := do.InfoSyncer().GetSessionManager()

	// test session manager registry when put back
	// We test for more than one times to cover the case that the session is in the pool.
	var sctx syssession.SessionContext
	var se *syssession.Session
	for i := 0; i < 2; i++ {
		sctx = nil
		se = nil
		require.NoError(t, p.WithSession(func(session *syssession.Session) error {
			require.Nil(t, se)
			se = session
			require.True(t, session.IsOwner())
			return session.WithSessionContext(func(ctx syssession.SessionContext) error {
				require.Nil(t, sctx)
				sctx = ctx
				require.True(t, sessManager.ContainsInternalSession(ctx))
				return nil
			})
		}))
		require.NotNil(t, se)
		require.False(t, se.IsInternalClosed())
		require.False(t, se.IsOwner())
		require.NotNil(t, sctx)
		require.False(t, sessManager.ContainsInternalSession(sctx))
	}

	// test session manager registry when close session
	sctx = nil
	se, err := p.Get()
	require.NoError(t, err)
	require.NoError(t, se.WithSessionContext(func(ctx syssession.SessionContext) error {
		sctx = ctx
		return nil
	}))
	require.NotNil(t, sctx)
	require.True(t, sessManager.ContainsInternalSession(sctx))
	se.Close()
	require.False(t, sessManager.ContainsInternalSession(sctx))
}

func TestDomainAdvancedSessionPoolPutBackDirtySession(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	p := do.AdvancedSysSessionPool()
	require.NotNil(t, p)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("insert into t1 values(1), (2), (3), (4), (5)")

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	cases := []struct {
		name               string
		withSession        func(*syssession.Session)
		withSessionContext func(sessionContext syssession.SessionContext)
	}{
		{
			name: "resultSetNotClose",
			withSession: func(session *syssession.Session) {
				_, err := session.ExecuteInternal(ctx, "select * from test.t1")
				require.NoError(t, err)
			},
		},
		{
			name: "optimisticTxnNotClose",
			withSession: func(session *syssession.Session) {
				_, err := session.ExecuteInternal(ctx, "begin optimistic")
				require.NoError(t, err)
			},
		},
		{
			name: "pessimisticTxnNotClose",
			withSession: func(session *syssession.Session) {
				_, err := session.ExecuteInternal(ctx, "begin pessimistic")
				require.NoError(t, err)
			},
		},
		{
			name: "tsFuturePrepared",
			withSessionContext: func(sctx syssession.SessionContext) {
				require.NoError(t, sctx.PrepareTSFuture(ctx, sessiontxn.ConstantFuture(1), kv.GlobalTxnScope))
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var se *syssession.Session
			syssession.WithSuppressAssert(func() {
				require.NoError(t, p.WithSession(func(session *syssession.Session) error {
					require.Nil(t, se)
					se = session
					require.True(t, session.IsOwner())
					require.False(t, se.IsInternalClosed())
					if c.withSession != nil {
						c.withSession(session)
					}

					if c.withSessionContext != nil {
						require.NoError(t, session.WithSessionContext(func(sessionctx syssession.SessionContext) error {
							c.withSessionContext(sessionctx)
							return nil
						}))
					}

					return nil
				}))
			})
			require.NotNil(t, se)
			require.True(t, se.IsInternalClosed())
			require.False(t, se.IsOwner())
		})
	}
}
