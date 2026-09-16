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

package objstore_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

type unmarshalDirTestStorage struct {
	storeapi.Storage
	walk func(context.Context, *storeapi.WalkOption, func(string, int64) error) error
	read func(context.Context, string) ([]byte, error)
}

func (s *unmarshalDirTestStorage) WalkDir(ctx context.Context, opt *storeapi.WalkOption, f func(string, int64) error) error {
	return s.walk(ctx, opt, f)
}

func (s *unmarshalDirTestStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return s.read(ctx, name)
}

func TestUnmarshalDirWaitsForWorkersOnWalkError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	walkErr := errors.New("injected listing failure")
	s := &unmarshalDirTestStorage{
		walk: func(_ context.Context, _ *storeapi.WalkOption, f func(string, int64) error) error {
			if err := f("meta", 4); err != nil {
				return err
			}
			<-started
			return walkErr
		},
		read: func(ctx context.Context, _ string) ([]byte, error) {
			defer close(finished)
			close(started)
			select {
			case <-release:
				return []byte("data"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	defer func() {
		cancel()
		<-finished
	}()
	items := objstore.UnmarshalDir(ctx, nil, s, func(target *string, _ string, content []byte) error {
		*target = string(content)
		return nil
	})
	first := make(chan iter.IterResult[*string], 1)
	go func() { first <- items.TryNext(ctx) }()
	<-started
	select {
	case result := <-first:
		t.Fatalf("iterator terminated before its worker completed: %v", result)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	result := <-first
	require.NoError(t, result.Err)
	require.False(t, result.Finished)
	require.Equal(t, "data", *result.Item)
	require.ErrorIs(t, items.TryNext(ctx).Err, walkErr)
}

func TestUnmarshalDirReturnsWalkError(t *testing.T) {
	walkErr := errors.New("injected listing failure")
	s := &unmarshalDirTestStorage{
		walk: func(context.Context, *storeapi.WalkOption, func(string, int64) error) error {
			return walkErr
		},
	}
	for range 100 {
		items := objstore.UnmarshalDir(context.Background(), nil, s, func(*string, string, []byte) error { return nil })
		// Exercise consumption after the producer has had time to finish listing.
		time.Sleep(time.Millisecond)
		require.ErrorIs(t, items.TryNext(context.Background()).Err, walkErr)
	}
}

func TestDefaultHttpTransport(t *testing.T) {
	transport, ok := objstore.CloneDefaultHTTPTransport()
	require.True(t, ok)
	require.True(t, transport.MaxConnsPerHost == 0)
	require.True(t, transport.MaxIdleConns > 0)
}

func TestDefaultHttpClient(t *testing.T) {
	var concurrency uint = 128
	transport, ok := objstore.GetDefaultHTTPClient(concurrency).Transport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, int(concurrency), transport.MaxIdleConnsPerHost)
	require.Equal(t, int(concurrency), transport.MaxIdleConns)
}

func TestNewMemStorage(t *testing.T) {
	url := "memstore://"
	s, err := objstore.NewFromURL(context.Background(), url)
	require.NoError(t, err)
	require.IsType(t, (*objstore.MemStorage)(nil), s)
}
