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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestDomainAdvancedSessionPoolInternalSessionRegistry(t *testing.T) {
	_, do := testkit.CreateMockStoreAndDomain(t)
	p := do.AdvancedSysSessionPool()
	require.NotNil(t, p)

	sessManager := do.InfoSyncer().GetSessionManager()

	// test session manager registry when put back
	// We test for more than one times to cover the case that the session is in the pool.
	var sctx sessionctx.Context
	var se *syssession.Session
	for range 2 {
		sctx = nil
		se = nil
		require.NoError(t, p.WithSession(func(session *syssession.Session) error {
			require.Nil(t, se)
			se = session
			require.True(t, session.IsOwner())
			return session.WithSessionContext(func(ctx sessionctx.Context) error {
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
	require.NoError(t, se.WithSessionContext(func(ctx sessionctx.Context) error {
		sctx = ctx
		return nil
	}))
	require.NotNil(t, sctx)
	require.True(t, sessManager.ContainsInternalSession(sctx))
	se.Close()
	require.False(t, sessManager.ContainsInternalSession(sctx))
}

func TestDomainAdvancedSessionPoolPutBackDirtySession(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/statistics/handle/SkipSystemTableCheck", `return(true)`)
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
		withSession        func(*syssession.Session) error
		withSessionContext func(sessionctx.Context) error
	}{
		{
			name: "put back closed one",
			withSession: func(session *syssession.Session) error {
				session.Close()
				return nil
			},
		},
		{
			name: "return error for withSession",
			withSession: func(session *syssession.Session) error {
				return errors.New("err1")
			},
		},
		{
			name: "return error for withSessionContext",
			withSessionContext: func(sctx sessionctx.Context) error {
				return errors.New("err2")
			},
		},
		{
			name: "resultSetNotClose",
			withSession: func(session *syssession.Session) error {
				_, err := session.ExecuteInternal(ctx, "select * from test.t1")
				require.NoError(t, err)
				return nil
			},
		},
		{
			name: "optimisticTxnNotClose",
			withSession: func(session *syssession.Session) error {
				_, err := session.ExecuteInternal(ctx, "begin optimistic")
				require.NoError(t, err)
				return nil
			},
		},
		{
			name: "pessimisticTxnNotClose",
			withSession: func(session *syssession.Session) error {
				_, err := session.ExecuteInternal(ctx, "begin pessimistic")
				require.NoError(t, err)
				return nil
			},
		},
		{
			name: "tsFuturePrepared",
			withSessionContext: func(sctx sessionctx.Context) error {
				require.NoError(t, sctx.PrepareTSFuture(ctx, sessiontxn.ConstantFuture(1), kv.GlobalTxnScope))
				return nil
			},
		},
		{
			name: "avoid reuse in withSession",
			withSession: func(session *syssession.Session) error {
				session.AvoidReuse()
				return nil
			},
		},
		{
			name: "avoid reuse in withSessionContext",
			withSession: func(session *syssession.Session) error {
				return session.WithSessionContext(func(sessionctx.Context) error {
					session.AvoidReuse()
					return nil
				})
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var se *syssession.Session
			var expectedErr error
			syssession.WithSuppressAssert(func() {
				err := p.WithSession(func(session *syssession.Session) error {
					require.Nil(t, se)
					se = session
					require.True(t, session.IsOwner())
					require.False(t, se.IsInternalClosed())
					if c.withSession != nil {
						expectedErr = c.withSession(session)
						return expectedErr
					}

					if c.withSessionContext != nil {
						err := session.WithSessionContext(func(sessionctx sessionctx.Context) error {
							expectedErr = c.withSessionContext(sessionctx)
							return expectedErr
						})
						if expectedErr != nil {
							require.EqualError(t, err, expectedErr.Error())
						} else {
							require.NoError(t, err)
						}
						return err
					}

					return nil
				})

				if expectedErr != nil {
					require.EqualError(t, err, expectedErr.Error())
				} else {
					require.NoError(t, err)
				}
			})
			require.NotNil(t, se)
			require.True(t, se.IsInternalClosed())
			require.False(t, se.IsOwner())
			require.Zero(t, p.(*syssession.AdvancedSessionPool).Size())
		})
	}

	t.Run("success case", func(t *testing.T) {
		var se *syssession.Session
		require.NoError(t, p.WithSession(func(s *syssession.Session) error {
			se = s
			return s.WithSessionContext(func(sessionctx.Context) error { return nil })
		}))
		require.NotNil(t, se)
		require.False(t, se.IsInternalClosed())
		require.False(t, se.IsOwner())
		require.Equal(t, 1, p.(*syssession.AdvancedSessionPool).Size())
	})

	t.Run("put back a put back case", func(t *testing.T) {
		var se *syssession.Session
		require.NoError(t, p.WithSession(func(s *syssession.Session) error {
			se = s
			p.Put(s)
			return nil
		}))
		require.NotNil(t, se)
		require.False(t, se.IsInternalClosed())
		require.False(t, se.IsOwner())
		require.Equal(t, 1, p.(*syssession.AdvancedSessionPool).Size())
	})
}

func TestAdvancedSessionPoolResetTimeZone(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	p := do.AdvancedSysSessionPool()
	require.NotNil(t, p)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.time_zone='UTC'")

	require.NoError(t, p.WithSession(func(session *syssession.Session) error {
		return session.WithSessionContext(func(sctx sessionctx.Context) error {
			return sctx.GetSessionVars().SetSystemVar(vardef.TimeZone, "Asia/Shanghai")
		})
	}))

	require.NoError(t, p.WithSession(func(session *syssession.Session) error {
		return session.WithSessionContext(func(sctx sessionctx.Context) error {
			val, ok := sctx.GetSessionVars().GetSystemVar(vardef.TimeZone)
			require.True(t, ok)
			require.Equal(t, "UTC", val)
			require.Equal(t, "UTC", sctx.GetSessionVars().Location().String())
			require.Equal(t, sctx.GetSessionVars().Location().String(), sctx.GetSessionVars().StmtCtx.TimeZone().String())
			return nil
		})
	}))
}
