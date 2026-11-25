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
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/objstore/tosstore"
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

func TestUnmarshalDirReturnsWorkerError(t *testing.T) {
	workerErr := errors.New("unsupported metadata version")
	s := &unmarshalDirTestStorage{
		walk: func(_ context.Context, _ *storeapi.WalkOption, f func(string, int64) error) error {
			return f("meta", 4)
		},
		read: func(context.Context, string) ([]byte, error) { return []byte("data"), nil },
	}
	for range 100 {
		returned := make(chan struct{})
		items := objstore.UnmarshalDir(context.Background(), nil, s, func(*string, string, []byte) error {
			defer close(returned)
			return workerErr
		})
		<-returned
		// Also exercise a consumer that resumes after error publication and channel closure.
		time.Sleep(time.Millisecond)
		require.ErrorIs(t, items.TryNext(context.Background()).Err, workerErr)
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

func TestNewTOSStorage(t *testing.T) {
	backend, err := objstore.ParseBackend("tos://bucket/prefix?region=cn-beijing", &objstore.BackendOptions{
		S3: s3like.S3BackendOptions{AccessKey: "test-id", SecretAccessKey: "sk"},
	})
	require.NoError(t, err)
	s, err := objstore.New(context.Background(), backend, nil)
	require.NoError(t, err)
	defer s.Close()
	require.IsType(t, (*tosstore.TOSStore)(nil), s)
	require.Equal(t, "tos://bucket/prefix/", s.URI())
}

func TestCopyFromBetweenOSSAndS3CompatibleOSS(t *testing.T) {
	var (
		mu         sync.Mutex
		copySource string
		copyPath   string
		copyCalls  int
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.Method == http.MethodGet && r.URL.Query().Has("location") {
			_, _ = w.Write([]byte(`<LocationConstraint>oss-cn-hangzhou</LocationConstraint>`))
			return
		}
		if r.Method == http.MethodPut && r.Header.Get("x-oss-copy-source") != "" {
			mu.Lock()
			copyCalls++
			copySource = r.Header.Get("x-oss-copy-source")
			copyPath = r.URL.Path
			mu.Unlock()
			_, _ = w.Write([]byte(`<CopyObjectResult><ETag>"etag"</ETag><LastModified>2026-01-01T00:00:00.000Z</LastModified></CopyObjectResult>`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	ctx := context.Background()
	srcBackend, err := objstore.ParseBackend(
		"s3://src-bucket/src?provider=alibaba&region=cn-hangzhou&access-key=ak&secret-access-key=sk&force-path-style=true&endpoint="+server.URL,
		nil,
	)
	require.NoError(t, err)
	srcStore, err := objstore.New(ctx, srcBackend, &storeapi.Options{HTTPClient: server.Client()})
	require.NoError(t, err)
	defer srcStore.Close()

	dstBackend, err := objstore.ParseBackend(
		"oss://dst-bucket/dst?region=cn-hangzhou&access-key=ak&secret-access-key=sk&endpoint="+server.URL,
		nil,
	)
	require.NoError(t, err)
	dstStore, err := objstore.New(ctx, dstBackend, &storeapi.Options{HTTPClient: server.Client()})
	require.NoError(t, err)
	defer dstStore.Close()

	copier, ok := dstStore.(storeapi.Copier)
	require.True(t, ok)
	err = copier.CopyFrom(ctx, srcStore, storeapi.CopySpec{
		From: "file.sst",
		To:   "file.sst",
	})
	require.NoError(t, err)
	mu.Lock()
	require.Equal(t, 1, copyCalls)
	require.Equal(t, "/src-bucket/src/file.sst", copySource)
	require.Equal(t, "/dst-bucket/dst/file.sst", copyPath)
	mu.Unlock()

	// TOSStore.CopyFrom requires a *TOSStore source and rejects non-TOS sources before any request.
	tosBackend, err := objstore.ParseBackend(
		"tos://src-bucket/src?region=cn-beijing&access-key=ak&secret-access-key=sk&endpoint="+server.URL,
		nil,
	)
	require.NoError(t, err)
	tosStore, err := objstore.New(ctx, tosBackend, &storeapi.Options{HTTPClient: server.Client()})
	require.NoError(t, err)
	defer tosStore.Close()

	tosCopier, ok := tosStore.(storeapi.Copier)
	require.True(t, ok)
	require.ErrorIs(t, tosCopier.CopyFrom(ctx, dstStore, storeapi.CopySpec{
		From: "file.sst",
		To:   "file.sst",
	}), berrors.ErrStorageInvalidConfig)

	mu.Lock()
	require.Equal(t, 1, copyCalls)
	mu.Unlock()
}
