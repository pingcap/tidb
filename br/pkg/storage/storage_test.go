// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package storage_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/stretchr/testify/require"
)

type unmarshalDirTestStorage struct {
	storage.ExternalStorage
	walk func(context.Context, *storage.WalkOption, func(string, int64) error) error
	read func(context.Context, string) ([]byte, error)
}

func (s *unmarshalDirTestStorage) WalkDir(ctx context.Context, opt *storage.WalkOption, f func(string, int64) error) error {
	return s.walk(ctx, opt, f)
}

func (s *unmarshalDirTestStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return s.read(ctx, name)
}

func TestUnmarshalDir(t *testing.T) {
	t.Run("WaitsForWorkersOnWalkError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
		walkErr := errors.New("injected listing failure")
		s := &unmarshalDirTestStorage{
			walk: func(_ context.Context, _ *storage.WalkOption, f func(string, int64) error) error {
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
		items := storage.UnmarshalDir(ctx, nil, s, func(target *string, _ string, content []byte) error {
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
	})

	t.Run("ReturnsWalkError", func(t *testing.T) {
		walkErr := errors.New("injected listing failure")
		s := &unmarshalDirTestStorage{
			walk: func(context.Context, *storage.WalkOption, func(string, int64) error) error {
				return walkErr
			},
		}
		for range 100 {
			items := storage.UnmarshalDir(context.Background(), nil, s, func(*string, string, []byte) error { return nil })
			// Exercise consumption after the producer has had time to finish listing.
			time.Sleep(time.Millisecond)
			require.ErrorIs(t, items.TryNext(context.Background()).Err, walkErr)
		}
	})

	t.Run("ReturnsWorkerError", func(t *testing.T) {
		workerErr := errors.New("unsupported metadata version")
		s := &unmarshalDirTestStorage{
			walk: func(_ context.Context, _ *storage.WalkOption, f func(string, int64) error) error {
				return f("meta", 4)
			},
			read: func(context.Context, string) ([]byte, error) { return []byte("data"), nil },
		}
		for range 100 {
			returned := make(chan struct{})
			items := storage.UnmarshalDir(context.Background(), nil, s, func(*string, string, []byte) error {
				defer close(returned)
				return workerErr
			})
			<-returned
			// Also exercise a consumer that resumes after error publication and channel closure.
			time.Sleep(time.Millisecond)
			require.ErrorIs(t, items.TryNext(context.Background()).Err, workerErr)
		}
	})
}

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
