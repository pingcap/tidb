// Copyright 2024 PingCAP, Inc.
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

package contextopt

import (
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/context"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

type mockSessionVarsProvider struct {
	vars *variable.SessionVars
}

func (p mockSessionVarsProvider) GetSessionVars() *variable.SessionVars {
	return p.vars
}

type mockEvalCtx struct {
	context.EvalContext
	props OptionalEvalPropProviders
}

func (ctx *mockEvalCtx) Location() *time.Location {
	return time.UTC
}

func (ctx *mockEvalCtx) GetOptionalPropProvider(
	key context.OptionalEvalPropKey,
) (context.OptionalEvalPropProvider, bool) {
	return ctx.props.Get(key)
}

func assertReaderFuncReturnErr[T any](
	t *testing.T, ctx context.EvalContext, fn func(ctx context.EvalContext) (T, error),
) {
	_, err := fn(ctx)
	require.Contains(t, err.Error(), "not exists in EvalContext")
}

func assertReaderFuncValue[T any](
	t *testing.T, ctx context.EvalContext, fn func(ctx context.EvalContext) (T, error),
) T {
	v, err := fn(ctx)
	require.NoError(t, err)
	return v
}

func TestOptionalEvalPropProviders(t *testing.T) {
	mockCtx := &mockEvalCtx{}
	require.True(t, mockCtx.props.PropKeySet().IsEmpty())

	var p context.OptionalEvalPropProvider
	var reader RequireOptionalEvalProps
	var verifyNoProvider func(ctx context.EvalContext)
	var verifyProvider func(ctx context.EvalContext, val context.OptionalEvalPropProvider)

	for i := 0; i < context.OptPropsCnt; i++ {
		key := context.OptionalEvalPropKey(i)
		switch key {
		case context.OptPropCurrentUser:
			user := &auth.UserIdentity{Username: "u1", Hostname: "h1"}
			roles := []*auth.RoleIdentity{{Username: "u2", Hostname: "h2"}, {Username: "u3", Hostname: "h3"}}
			p = CurrentUserPropProvider(func() (*auth.UserIdentity, []*auth.RoleIdentity) { return user, roles })
			r := CurrentUserPropReader{}
			reader = r
			verifyNoProvider = func(ctx context.EvalContext) {
				assertReaderFuncReturnErr(t, ctx, r.CurrentUser)
				assertReaderFuncReturnErr(t, ctx, r.ActiveRoles)
			}
			verifyProvider = func(ctx context.EvalContext, val context.OptionalEvalPropProvider) {
				user2, roles2 := val.(CurrentUserPropProvider)()
				require.Equal(t, user, user2)
				require.Equal(t, roles, roles2)

				require.Equal(t, user, assertReaderFuncValue(t, ctx, r.CurrentUser))
				require.Equal(t, roles, assertReaderFuncValue(t, ctx, r.ActiveRoles))
			}
		case context.OptPropSessionVars:
			vars := variable.NewSessionVars(nil)
			vars.TimeZone = time.UTC
			vars.StmtCtx.SetTimeZone(time.UTC)
			p = NewSessionVarsProvider(mockSessionVarsProvider{vars: vars})
			r := SessionVarsPropReader{}
			reader = r
			verifyNoProvider = func(ctx context.EvalContext) {
				assertReaderFuncReturnErr(t, ctx, r.GetSessionVars)
			}
			verifyProvider = func(ctx context.EvalContext, val context.OptionalEvalPropProvider) {
				require.Same(t, vars, assertReaderFuncValue(t, ctx, r.GetSessionVars))
			}
		case context.OptPropInfoSchema:
			type mockIsType struct {
				infoschema.MetaOnlyInfoSchema
			}
			var is1, is2 mockIsType
			p = InfoSchemaPropProvider(func(isDomain bool) infoschema.MetaOnlyInfoSchema {
				if isDomain {
					return &is1
				}
				return &is2
			})
			r := InfoSchemaPropReader{}
			reader = r
			verifyNoProvider = func(ctx context.EvalContext) {
				assertReaderFuncReturnErr(t, ctx, r.GetSessionInfoSchema)
				assertReaderFuncReturnErr(t, ctx, r.GetDomainInfoSchema)
			}
			verifyProvider = func(ctx context.EvalContext, val context.OptionalEvalPropProvider) {
				is, ok := val.(InfoSchemaPropProvider)(true).(*mockIsType)
				require.True(t, ok)
				require.Same(t, &is1, is)

				is, ok = val.(InfoSchemaPropProvider)(false).(*mockIsType)
				require.True(t, ok)
				require.Same(t, &is2, is)

				require.Same(t, &is1, assertReaderFuncValue(t, ctx, r.GetDomainInfoSchema))
				require.Same(t, &is2, assertReaderFuncValue(t, ctx, r.GetSessionInfoSchema))
			}
		case context.OptPropKVStore:
			type mockKVStoreType struct {
				kv.Storage
			}
			mockKVStore := &mockKVStoreType{}
			p = KVStorePropProvider(func() kv.Storage {
				return mockKVStore
			})
			r := KVStorePropReader{}
			reader = r
			verifyNoProvider = func(ctx context.EvalContext) {
				assertReaderFuncReturnErr(t, ctx, r.GetKVStore)
			}
			verifyProvider = func(ctx context.EvalContext, val context.OptionalEvalPropProvider) {
				store, ok := val.(KVStorePropProvider)().(*mockKVStoreType)
				require.True(t, ok)
				require.Same(t, mockKVStore, store)
				require.Same(t, mockKVStore, assertReaderFuncValue(t, ctx, r.GetKVStore))
			}
		case context.OptPropSQLExecutor:
			type mockSQLExecutorType struct {
				SQLExecutor
			}
			mockExecutor := &mockSQLExecutorType{}
			var mockErr error
			p = SQLExecutorPropProvider(func() (SQLExecutor, error) {
				if mockErr != nil {
					return nil, mockErr
				}
				return mockExecutor, nil
			})
			r := SQLExecutorPropReader{}
			reader = r
			verifyNoProvider = func(ctx context.EvalContext) {
				assertReaderFuncReturnErr(t, ctx, r.GetSQLExecutor)
			}
			verifyProvider = func(ctx context.EvalContext, val context.OptionalEvalPropProvider) {
				exec, err := val.(SQLExecutorPropProvider)()
				require.Nil(t, err)
				require.Same(t, mockExecutor, exec.(*mockSQLExecutorType))

				mockErr = errors.New("mockErr1")
				exec, err = val.(SQLExecutorPropProvider)()
				require.EqualError(t, err, "mockErr1")
				require.Nil(t, exec)

				mockErr = nil
				require.Same(t, mockExecutor, assertReaderFuncValue(t, ctx, r.GetSQLExecutor))

				mockErr = errors.New("mockErr2")
				exec, err = r.GetSQLExecutor(ctx)
				require.EqualError(t, err, "mockErr2")
				require.Nil(t, exec)
			}
		case context.OptPropSequenceOperator:
			type mockSeqOpType struct {
				SequenceOperator
			}
			mockOp := &mockSeqOpType{}
			var mockErr error
			p = SequenceOperatorProvider(func(db, name string) (SequenceOperator, error) {
				require.Equal(t, "db1", db)
				require.Equal(t, "name1", name)
				if mockErr != nil {
					return nil, mockErr
				}
				return mockOp, nil
			})
			r := SequenceOperatorPropReader{}
			reader = r
			verifyNoProvider = func(ctx context.EvalContext) {
				assertReaderFuncReturnErr(t, ctx, func(ctx context.EvalContext) (SequenceOperator, error) {
					return r.GetSequenceOperator(ctx, "db1", "name1")
				})
			}
			verifyProvider = func(ctx context.EvalContext, val context.OptionalEvalPropProvider) {
				op, err := val.(SequenceOperatorProvider)("db1", "name1")
				require.NoError(t, err)
				require.Same(t, mockOp, op.(*mockSeqOpType))

				mockErr = errors.New("mockErr1")
				op, err = val.(SequenceOperatorProvider)("db1", "name1")
				require.EqualError(t, err, "mockErr1")
				require.Nil(t, op)

				mockErr = nil
				assertReaderFuncValue(t, ctx, func(ctx context.EvalContext) (SequenceOperator, error) {
					return r.GetSequenceOperator(ctx, "db1", "name1")
				})

				mockErr = errors.New("mockErr2")
				op, err = r.GetSequenceOperator(ctx, "db1", "name1")
				require.EqualError(t, err, "mockErr2")
				require.Nil(t, op)
			}
		case context.OptPropAdvisoryLock:
			type mockLockCtxType struct {
				AdvisoryLockContext
			}
			mockLockCtx := &mockLockCtxType{}
			p = NewAdvisoryLockPropProvider(mockLockCtx)
			r := AdvisoryLockPropReader{}
			reader = r
			verifyNoProvider = func(ctx context.EvalContext) {
				assertReaderFuncReturnErr(t, ctx, r.AdvisoryLockCtx)
			}
			verifyProvider = func(ctx context.EvalContext, val context.OptionalEvalPropProvider) {
				got := val.(*AdvisoryLockPropProvider).AdvisoryLockContext
				require.Same(t, mockLockCtx, got)
				require.Same(t, p, assertReaderFuncValue(t, ctx, r.AdvisoryLockCtx))
			}
		case context.OptPropDDLOwnerInfo:
			var isOwner bool
			p = DDLOwnerInfoProvider(func() bool { return isOwner })
			r := DDLOwnerPropReader{}
			reader = r
			verifyNoProvider = func(ctx context.EvalContext) {
				assertReaderFuncReturnErr(t, ctx, r.IsDDLOwner)
			}
			verifyProvider = func(ctx context.EvalContext, val context.OptionalEvalPropProvider) {
				isOwner = true
				require.True(t, val.(DDLOwnerInfoProvider)())
				isOwner = false
				require.False(t, val.(DDLOwnerInfoProvider)())

				isOwner = true
				require.True(t, assertReaderFuncValue(t, ctx, r.IsDDLOwner))
				isOwner = false
				require.False(t, assertReaderFuncValue(t, ctx, r.IsDDLOwner))
			}
		default:
			require.Fail(t, "unexpected optional property key")
		}

		// before add
		beforeKeySet := mockCtx.props.PropKeySet()
		require.Equal(t, context.OptionalEvalPropKeySet(1<<key)-1, beforeKeySet)
		require.False(t, beforeKeySet.Contains(key))
		require.False(t, mockCtx.props.Contains(key))
		require.Equal(t, p.Desc().Key().AsPropKeySet(), reader.RequiredOptionalEvalProps())
		verifyNoProvider(mockCtx)

		// after add
		mockCtx.props.Add(p)
		afterKeySet := mockCtx.props.PropKeySet()
		require.Equal(t, context.OptionalEvalPropKeySet(1<<(key+1))-1, afterKeySet)
		require.True(t, afterKeySet.Contains(key))
		require.True(t, mockCtx.props.Contains(key))

		val, ok := mockCtx.props.Get(key)
		require.True(t, ok)
		verifyProvider(mockCtx, val)
	}
}
