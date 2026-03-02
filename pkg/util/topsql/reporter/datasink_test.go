// Copyright 2021 PingCAP, Inc.
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

package reporter

import (
	"context"
	"sync"
	"testing"
	"time"

	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultDataSinkRegisterer(t *testing.T) {
	var err error
	r := NewDefaultDataSinkRegisterer(context.Background())
	m1 := newMockDataSink2()
	m2 := newMockDataSink2()
	err = r.Register(m1)
	assert.NoError(t, err)
	err = r.Register(m2)
	assert.NoError(t, err)
	assert.Len(t, r.dataSinks, 2)
	r.Deregister(m1)
	r.Deregister(m2)
	assert.Empty(t, r.dataSinks)
}

// TestDefaultDataSinkRegistererTopRUTwoSinksRefCountAndReset verifies TopRU
// enablement is ref-counted across sinks and the interval resets when the count hits zero.
// It uses two sinks with different intervals to ensure last-write wins while enabled.
func TestDefaultDataSinkRegistererTopRUTwoSinksRefCountAndReset(t *testing.T) {
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.ResetTopRUItemInterval()
	t.Cleanup(func() {
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
		topsqlstate.ResetTopRUItemInterval()
	})

	r := NewDefaultDataSinkRegisterer(context.Background())
	ds1 := &pubSubDataSink{
		enableTopRU:  true,
		itemInterval: tipb.ItemInterval_ITEM_INTERVAL_30S,
	}
	ds2 := &pubSubDataSink{
		enableTopRU:  true,
		itemInterval: tipb.ItemInterval_ITEM_INTERVAL_15S,
	}

	require.NoError(t, r.Register(ds1))
	require.NoError(t, r.Register(ds2))
	require.True(t, topsqlstate.TopRUEnabled())
	require.Equal(t, int64(15), topsqlstate.GetTopRUItemInterval())

	r.Deregister(ds2)
	require.True(t, topsqlstate.TopRUEnabled())
	require.Equal(t, int64(15), topsqlstate.GetTopRUItemInterval())

	r.Deregister(ds1)
	require.False(t, topsqlstate.TopRUEnabled())
	require.Equal(t, int64(topsqlstate.DefTiDBTopRUItemIntervalSeconds), topsqlstate.GetTopRUItemInterval())
}

// TestDefaultDataSinkRegistererTopRUDuplicateRegisterIsIdempotent verifies duplicate
// registrations do not inflate the TopRU ref count or alter the interval.
func TestDefaultDataSinkRegistererTopRUDuplicateRegisterIsIdempotent(t *testing.T) {
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.ResetTopRUItemInterval()
	t.Cleanup(func() {
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
		topsqlstate.ResetTopRUItemInterval()
	})

	r := NewDefaultDataSinkRegisterer(context.Background())
	ds := &pubSubDataSink{
		enableTopRU:  true,
		itemInterval: tipb.ItemInterval_ITEM_INTERVAL_15S,
	}

	require.NoError(t, r.Register(ds))
	require.NoError(t, r.Register(ds))
	require.Len(t, r.dataSinks, 1)
	require.True(t, topsqlstate.TopRUEnabled())

	r.Deregister(ds)
	require.False(t, topsqlstate.TopRUEnabled())
	require.Equal(t, int64(topsqlstate.DefTiDBTopRUItemIntervalSeconds), topsqlstate.GetTopRUItemInterval())
}

// TestDefaultDataSinkRegistererTopRURefCountConcurrentRegisterDeregister verifies
// concurrent register/deregister cycles do not leak TopRU/TopSQL global state.
// It uses fixed loop counts with a waitgroup, so there are no timing-based assertions.
func TestDefaultDataSinkRegistererTopRURefCountConcurrentRegisterDeregister(t *testing.T) {
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.DisableTopSQL()
	topsqlstate.ResetTopRUItemInterval()
	t.Cleanup(func() {
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
		topsqlstate.DisableTopSQL()
		topsqlstate.ResetTopRUItemInterval()
	})

	r := NewDefaultDataSinkRegisterer(context.Background())
	sinks := []*pubSubDataSink{
		{enableTopRU: true, itemInterval: tipb.ItemInterval_ITEM_INTERVAL_15S},
		{enableTopRU: true, itemInterval: tipb.ItemInterval_ITEM_INTERVAL_30S},
		{enableTopRU: true, itemInterval: tipb.ItemInterval_ITEM_INTERVAL_60S},
	}

	const (
		goroutines = 4
		loops      = 8
	)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		sink := sinks[g%len(sinks)]
		go func(ds *pubSubDataSink) {
			defer wg.Done()
			for i := 0; i < loops; i++ {
				if err := r.Register(ds); err != nil {
					t.Errorf("register failed: %v", err)
					return
				}
				r.Deregister(ds)
			}
		}(sink)
	}
	wg.Wait()

	// Ensure all known sinks are removed before checking global state.
	for _, sink := range sinks {
		r.Deregister(sink)
	}

	require.False(t, topsqlstate.TopRUEnabled())
	require.Equal(t, int64(topsqlstate.DefTiDBTopRUItemIntervalSeconds), topsqlstate.GetTopRUItemInterval())
	require.False(t, topsqlstate.TopSQLEnabled())
	require.Empty(t, r.dataSinks)
}

type mockDataSink2 struct {
	data   []*ReportData
	closed bool
}

func newMockDataSink2() *mockDataSink2 {
	return &mockDataSink2{
		data: []*ReportData{},
	}
}

func (m *mockDataSink2) TrySend(data *ReportData, deadline time.Time) error {
	m.data = append(m.data, data)
	return nil
}

func (m *mockDataSink2) OnReporterClosing() {
	m.closed = true
}
