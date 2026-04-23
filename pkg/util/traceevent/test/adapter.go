// Copyright 2025 PingCAP, Inc.
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

package traceevent_test

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/trace"
)

func RunTraceControlExtractor(t *testing.T) {
	var conf traceevent.ExportedFlightRecorderConfig
	conf.Initialize()
	require.NoError(t, traceevent.ExportedStartLogFlightRecorder(&conf))
	fr := traceevent.ExportedGetFlightRecorder()
	defer fr.Close()

	// Test with nil context (no sink)
	t.Run("NoSink", func(t *testing.T) {
		oldCategories := tracing.GetEnabledCategories()
		defer tracing.SetCategories(oldCategories)
		tracing.SetCategories(tracing.TiKVRequest)

		ctx := context.Background()
		flags := traceevent.ExportedHandleTraceControlExtractor(ctx)
		require.True(t, flags.Has(trace.FlagTiKVCategoryRequest))
		require.False(t, flags.Has(trace.FlagImmediateLog))
	})

	// Test with keep=false
	t.Run("KeepFalse", func(t *testing.T) {
		tr := traceevent.ExportedNewTrace()
		ctx := tracing.WithFlightRecorder(context.Background(), tr)

		// Save old categories and restore after test
		oldCategories := tracing.GetEnabledCategories()
		defer tracing.SetCategories(oldCategories)

		// Enable only TiKVRequest
		tracing.SetCategories(tracing.TiKVRequest)

		flags := traceevent.ExportedHandleTraceControlExtractor(ctx)
		require.False(t, flags.Has(trace.FlagImmediateLog), "immediate log should not be set when keep=false")
		require.True(t, flags.Has(trace.FlagTiKVCategoryRequest), "request category should be set")
	})

	// Test with keep=true
	t.Run("KeepTrue", func(t *testing.T) {
		tr := traceevent.ExportedNewTrace()
		// This sets keep=true
		traceevent.ExportedSetTraceBits(tr, traceevent.ExportedGetTruthTable(traceevent.ExportedGetFlightRecorder())[0])
		ctx := tracing.WithFlightRecorder(context.Background(), tr)

		// Save old categories and restore after test
		oldCategories := tracing.GetEnabledCategories()
		defer tracing.SetCategories(oldCategories)

		// Enable only TiKVRequest
		tracing.SetCategories(tracing.TiKVRequest)

		flags := traceevent.ExportedHandleTraceControlExtractor(ctx)
		require.True(t, flags.Has(trace.FlagImmediateLog), "immediate log should be set when keep=true")
		require.True(t, flags.Has(trace.FlagTiKVCategoryRequest), "request category should be set")
	})

	// Test category mapping: TiKVRequest
	t.Run("CategoryTiKVRequest", func(t *testing.T) {
		tr := traceevent.ExportedNewTrace()
		ctx := tracing.WithFlightRecorder(context.Background(), tr)

		// Save old categories and restore after test
		oldCategories := tracing.GetEnabledCategories()
		defer tracing.SetCategories(oldCategories)

		tracing.SetCategories(tracing.TiKVRequest)

		flags := traceevent.ExportedHandleTraceControlExtractor(ctx)
		require.True(t, flags.Has(trace.FlagTiKVCategoryRequest))
		require.False(t, flags.Has(trace.FlagTiKVCategoryWriteDetails))
		require.False(t, flags.Has(trace.FlagTiKVCategoryReadDetails))
	})

	// Test category mapping: TiKVWriteDetails
	t.Run("CategoryTiKVWriteDetails", func(t *testing.T) {
		tr := traceevent.ExportedNewTrace()
		ctx := tracing.WithFlightRecorder(context.Background(), tr)

		// Save old categories and restore after test
		oldCategories := tracing.GetEnabledCategories()
		defer tracing.SetCategories(oldCategories)

		tracing.SetCategories(tracing.TiKVWriteDetails)

		flags := traceevent.ExportedHandleTraceControlExtractor(ctx)
		require.False(t, flags.Has(trace.FlagTiKVCategoryRequest))
		require.True(t, flags.Has(trace.FlagTiKVCategoryWriteDetails))
		require.False(t, flags.Has(trace.FlagTiKVCategoryReadDetails))
	})

	// Test category mapping: TiKVReadDetails
	t.Run("CategoryTiKVReadDetails", func(t *testing.T) {
		tr := traceevent.ExportedNewTrace()
		ctx := tracing.WithFlightRecorder(context.Background(), tr)

		// Save old categories and restore after test
		oldCategories := tracing.GetEnabledCategories()
		defer tracing.SetCategories(oldCategories)

		tracing.SetCategories(tracing.TiKVReadDetails)

		flags := traceevent.ExportedHandleTraceControlExtractor(ctx)
		require.False(t, flags.Has(trace.FlagTiKVCategoryRequest))
		require.False(t, flags.Has(trace.FlagTiKVCategoryWriteDetails))
		require.True(t, flags.Has(trace.FlagTiKVCategoryReadDetails))
	})

	// Test multiple categories
	t.Run("MultipleCategoriesAndKeep", func(t *testing.T) {
		tr := traceevent.ExportedNewTrace()
		// Set keep=true
		traceevent.ExportedSetTraceBits(tr, traceevent.ExportedGetTruthTable(traceevent.ExportedGetFlightRecorder())[0])
		ctx := tracing.WithFlightRecorder(context.Background(), tr)

		// Save old categories and restore after test
		oldCategories := tracing.GetEnabledCategories()
		defer tracing.SetCategories(oldCategories)

		// Enable all three TiKV categories
		tracing.SetCategories(tracing.TiKVRequest | tracing.TiKVWriteDetails | tracing.TiKVReadDetails)

		flags := traceevent.ExportedHandleTraceControlExtractor(ctx)
		require.True(t, flags.Has(trace.FlagImmediateLog), "immediate log should be set")
		require.True(t, flags.Has(trace.FlagTiKVCategoryRequest), "request category should be set")
		require.True(t, flags.Has(trace.FlagTiKVCategoryWriteDetails), "write details should be set")
		require.True(t, flags.Has(trace.FlagTiKVCategoryReadDetails), "read details should be set")
	})

	// Test concurrent access (should not race)
	t.Run("ConcurrentAccess", func(t *testing.T) {
		tr := traceevent.ExportedNewTrace()
		ctx := tracing.WithFlightRecorder(context.Background(), tr)

		// Save old categories and restore after test
		oldCategories := tracing.GetEnabledCategories()
		defer tracing.SetCategories(oldCategories)

		tracing.SetCategories(tracing.TiKVRequest)

		var wg sync.WaitGroup
		// Run multiple concurrent extractors
		for range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = traceevent.ExportedHandleTraceControlExtractor(ctx)
			}()
		}

		// Concurrently mark dump
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				traceevent.ExportedTraceMarkBits(tr, 1)
			}()
		}

		wg.Wait()
		// If there's a race, the test will fail with -race flag
	})
}

func RunCategoryParsing(t *testing.T) {
	// Test that new categories can be parsed
	t.Run("ParseTiKVRequest", func(t *testing.T) {
		cat := tracing.ParseTraceCategory("tikv_request")
		require.Equal(t, tracing.TiKVRequest, cat)
		require.Equal(t, "tikv_request", cat.String())
	})

	t.Run("ParseTiKVWriteDetails", func(t *testing.T) {
		cat := tracing.ParseTraceCategory("tikv_write_details")
		require.Equal(t, tracing.TiKVWriteDetails, cat)
		require.Equal(t, "tikv_write_details", cat.String())
	})

	t.Run("ParseTiKVReadDetails", func(t *testing.T) {
		cat := tracing.ParseTraceCategory("tikv_read_details")
		require.Equal(t, tracing.TiKVReadDetails, cat)
		require.Equal(t, "tikv_read_details", cat.String())
	})
}

func RunDefaultConfiguration(t *testing.T) {
	// Test that default configuration excludes write/read details
	t.Run("DefaultExcludesDetails", func(t *testing.T) {
		config := &traceevent.ExportedFlightRecorderConfig{}
		config.Initialize()

		categories := traceevent.ExportedParseCategories(config.EnabledCategories)

		// Should include TiKVRequest
		require.True(t, categories&tracing.TiKVRequest != 0, "default should include tikv_request")

		// Should exclude write and read details
		require.False(t, categories&tracing.TiKVWriteDetails != 0, "default should exclude tikv_write_details")
		require.False(t, categories&tracing.TiKVReadDetails != 0, "default should exclude tikv_read_details")

		// Should include other categories
		require.True(t, categories&tracing.TxnLifecycle != 0, "default should include txn_lifecycle")
		require.True(t, categories&tracing.General != 0, "default should include general")
	})
}
