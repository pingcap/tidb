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

package modelruntime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yalue/onnxruntime_go"
)

type stubSession struct {
	destroyed int
}

func (s *stubSession) Run([]onnxruntime_go.Value, []onnxruntime_go.Value) error {
	return nil
}

func (s *stubSession) RunWithOptions([]onnxruntime_go.Value, []onnxruntime_go.Value, *onnxruntime_go.RunOptions) error {
	return nil
}

func (s *stubSession) Destroy() error {
	s.destroyed++
	return nil
}

func TestSessionCacheReusesSession(t *testing.T) {
	cache := NewSessionCache(SessionCacheOptions{Capacity: 1})
	createCount := 0
	create := func() (dynamicSession, error) {
		createCount++
		return &stubSession{}, nil
	}

	first, err := cache.GetOrCreate(SessionKey("m1"), create)
	require.NoError(t, err)
	second, err := cache.GetOrCreate(SessionKey("m1"), create)
	require.NoError(t, err)

	require.Equal(t, 1, createCount)
	require.Same(t, first, second)
}

func TestSessionCacheEvictsSession(t *testing.T) {
	cache := NewSessionCache(SessionCacheOptions{Capacity: 1})
	firstSession := &stubSession{}
	_, err := cache.GetOrCreate(SessionKey("m1"), func() (dynamicSession, error) {
		return firstSession, nil
	})
	require.NoError(t, err)

	_, err = cache.GetOrCreate(SessionKey("m2"), func() (dynamicSession, error) {
		return &stubSession{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, firstSession.destroyed)
}

func TestSessionCacheTTLExpires(t *testing.T) {
	now := time.Date(2026, 2, 21, 0, 0, 0, 0, time.UTC)
	cache := NewSessionCache(SessionCacheOptions{
		Capacity: 1,
		TTL:      time.Second,
		Now: func() time.Time {
			return now
		},
	})
	firstSession := &stubSession{}
	_, err := cache.GetOrCreate(SessionKey("m1"), func() (dynamicSession, error) {
		return firstSession, nil
	})
	require.NoError(t, err)

	now = now.Add(2 * time.Second)
	_, err = cache.GetOrCreate(SessionKey("m1"), func() (dynamicSession, error) {
		return &stubSession{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, firstSession.destroyed)
}
