// Copyright 2023-2023 PingCAP, Inc.

package session_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/extension/enterprise/audit"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestBootStrapWithoutDutySeparation(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	ctx := context.Background()
	r, err := se.Execute(ctx, "select * from mysql.user")
	require.Nil(t, err)
	require.NotNil(t, r)

	req := r[0].NewChunk(nil)
	err = r[0].Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
}

func TestBootStrapWithDutySeparation(t *testing.T) {
	restoreFunc := executor.MockSetDutySeparationModeEnable()
	defer restoreFunc()

	// register extensions
	audit.Register4Test()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	ctx := context.Background()
	r, err := se.Execute(ctx, "select * from mysql.user")
	require.Nil(t, err)
	require.NotNil(t, r)

	req := r[0].NewChunk(nil)
	err = r[0].Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 5, req.NumRows())
}
