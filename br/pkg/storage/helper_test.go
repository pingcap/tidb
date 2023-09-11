// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestSetTiDBCloudStorageURI(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	mock := variable.NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	cloudStorageURI := variable.GetSysVar(variable.TiDBCloudStorageURI)
	require.Len(t, variable.CloudStorageURI.Load(), 0)
	defer func() {
		variable.CloudStorageURI.Store("")
	}()

	// Default empty
	require.Len(t, cloudStorageURI.Value, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Set to noop
	noopURI := "noop://blackhole?access-key=hello&secret-access-key=world"
	err := mock.SetGlobalSysVar(ctx, variable.TiDBCloudStorageURI, noopURI)
	require.NoError(t, err)
	val, err1 := mock.SessionVars.GetSessionOrGlobalSystemVar(ctx, variable.TiDBCloudStorageURI)
	require.NoError(t, err1)
	require.Equal(t, noopURI, val)
	require.Equal(t, noopURI, variable.CloudStorageURI.Load())

	// Set to s3, should fail
	err = mock.SetGlobalSysVar(ctx, variable.TiDBCloudStorageURI, "s3://blackhole")
	require.ErrorContains(t, err, "bucket blackhole")

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer s.Close()

	// Set to s3, should return uri without variable
	s3URI := "s3://tiflow-test/?access-key=testid&secret-access-key=testkey8&session-token=testtoken&endpoint=" + s.URL
	err = mock.SetGlobalSysVar(ctx, variable.TiDBCloudStorageURI, s3URI)
	require.NoError(t, err)
	val, err1 = mock.SessionVars.GetSessionOrGlobalSystemVar(ctx, variable.TiDBCloudStorageURI)
	require.NoError(t, err1)
	require.True(t, strings.HasPrefix(val, "s3://tiflow-test/"))
	require.Contains(t, val, "access-key=redacted")
	require.Contains(t, val, "secret-access-key=redacted")
	require.Contains(t, val, "session-token=redacted")
	require.Equal(t, s3URI, variable.CloudStorageURI.Load())
	cancel()
}
