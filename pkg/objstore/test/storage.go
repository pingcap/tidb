// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"context"
	"net/http"
	"testing"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/stretchr/testify/require"
)

func RunDefaultHttpTransport(t *testing.T) {
	transport, ok := objstore.CloneDefaultHTTPTransport()
	require.True(t, ok)
	require.True(t, transport.MaxConnsPerHost == 0)
	require.True(t, transport.MaxIdleConns > 0)
}

func RunDefaultHttpClient(t *testing.T) {
	var concurrency uint = 128
	transport, ok := objstore.GetDefaultHTTPClient(concurrency).Transport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, int(concurrency), transport.MaxIdleConnsPerHost)
	require.Equal(t, int(concurrency), transport.MaxIdleConns)
}

func RunNewMemStorage(t *testing.T) {
	url := "memstore://"
	s, err := objstore.NewFromURL(context.Background(), url)
	require.NoError(t, err)
	require.IsType(t, (*objstore.MemStorage)(nil), s)
}
