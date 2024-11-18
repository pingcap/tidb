// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package storage_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDefaultHttpTransport(t *testing.T) {
	transport, ok := storage.CloneDefaultHttpTransport()
	require.True(t, ok)
	require.True(t, transport.MaxConnsPerHost == 0)
	require.True(t, transport.MaxIdleConns > 0)
}

func TestDefaultHttpClient(t *testing.T) {
	var concurrency uint = 128
	transport, ok := storage.GetDefaultHttpClient(concurrency).Transport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, int(concurrency), transport.MaxIdleConnsPerHost)
	require.Equal(t, int(concurrency), transport.MaxIdleConns)
}

func TestNewMemStorage(t *testing.T) {
	url := "memstore://"
	s, err := storage.NewFromURL(context.Background(), url)
	require.NoError(t, err)
	require.IsType(t, (*storage.MemStorage)(nil), s)
}
