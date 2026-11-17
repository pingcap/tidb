// Copyright 2026 PingCAP, Inc.
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

package importer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type querySourceTestStore struct {
	kv.Storage
	snapshot kv.Snapshot
}

func (s *querySourceTestStore) GetSnapshot(kv.Version) kv.Snapshot { return s.snapshot }

type querySourceTestSnapshot struct {
	kv.Snapshot
	read func(context.Context, bool) error
}

func (*querySourceTestSnapshot) SetOption(int, any) {}

func (s *querySourceTestSnapshot) Get(ctx context.Context, _ kv.Key, _ ...kv.GetOption) (kv.ValueEntry, error) {
	if err := s.read(ctx, false); err != nil {
		return kv.ValueEntry{}, err
	}
	// Schema version is an integer; database existence only needs a nonempty value.
	return kv.ValueEntry{Value: []byte("1")}, nil
}

func (s *querySourceTestSnapshot) BatchGet(ctx context.Context, keys []kv.Key, _ ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	if err := s.read(ctx, true); err != nil {
		return nil, err
	}
	return nil, nil
}

func TestQuerySourceMetadataCancellation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		blockAt int
	}{
		{"schema version", 1}, {"database existence", 2}, {"source tables", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, abort := make(chan struct{}), make(chan struct{})
			defer close(abort)
			reads, batchReads := 0, 0
			snapshot := &querySourceTestSnapshot{read: func(readCtx context.Context, batch bool) error {
				reads++
				if batch {
					batchReads++
				}
				if reads != tc.blockAt {
					return nil
				}
				close(entered)
				select {
				case <-readCtx.Done():
					return readCtx.Err()
				case <-abort:
					return errors.New("blocked metadata read did not observe cancellation")
				}
			}}
			sctx := mock.NewContext()
			sctx.Store = &querySourceTestStore{snapshot: snapshot}
			done := make(chan error, 1)
			go func() {
				_, err := newQuerySourceAtSnapshot(ctx, sctx, 100, 1, []int64{11, 12})
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("metadata read did not start")
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, tc.blockAt, reads)
				if tc.blockAt == 3 {
					require.Equal(t, 1, batchReads)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("metadata read ignored preparation cancellation")
			}
		})
	}
	t.Run("cancelled between reads", func(t *testing.T) {
		for _, cancelAfter := range []int{1, 2} {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reads := 0
			snapshot := &querySourceTestSnapshot{read: func(context.Context, bool) error {
				reads++
				if reads == cancelAfter {
					cancel()
				}
				return nil
			}}
			sctx := mock.NewContext()
			sctx.Store = &querySourceTestStore{snapshot: snapshot}
			_, err := newQuerySourceAtSnapshot(ctx, sctx, 100, 1, []int64{11})
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, cancelAfter, reads)
		}
	})
	t.Run("already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		reads := 0
		snapshot := &querySourceTestSnapshot{read: func(context.Context, bool) error {
			reads++
			return errors.New("unexpected read after cancellation")
		}}
		sctx := mock.NewContext()
		sctx.Store = &querySourceTestStore{snapshot: snapshot}
		_, err := newQuerySourceAtSnapshot(ctx, sctx, 100, 1, []int64{11})
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, reads)
	})
}
