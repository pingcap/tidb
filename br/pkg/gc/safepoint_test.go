// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc_test

import (
	"regexp"
	"sync"
	"testing"

	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestMakeSafePointID(t *testing.T) {
	t.Run("Format", func(t *testing.T) {
		id := gc.MakeSafePointID()

		// Should match "br-{uuid}" pattern
		// UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
		pattern := `^br-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
		matched, err := regexp.MatchString(pattern, id)
		require.NoError(t, err)
		require.True(t, matched, "ID %q should match pattern %q", id, pattern)
	})

	t.Run("Uniqueness", func(t *testing.T) {
		ids := make(map[string]bool)
		count := 100

		for i := 0; i < count; i++ {
			id := gc.MakeSafePointID()
			require.False(t, ids[id], "duplicate ID generated: %s", id)
			ids[id] = true
		}
	})

	t.Run("Concurrent_Uniqueness", func(t *testing.T) {
		var mu sync.Mutex
		ids := make(map[string]bool)
		count := 100

		var wg sync.WaitGroup
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id := gc.MakeSafePointID()
				mu.Lock()
				defer mu.Unlock()
				require.False(t, ids[id], "duplicate ID generated: %s", id)
				ids[id] = true
			}()
		}
		wg.Wait()
	})
}

func TestBRServiceSafePoint_MarshalLogObject(t *testing.T) {
	t.Run("NormalValues", func(t *testing.T) {
		sp := gc.BRServiceSafePoint{
			ID:       "br-test-id",
			TTL:      300,
			BackupTS: 1000,
		}

		core, logs := observer.New(zapcore.InfoLevel)
		logger := zap.New(core)

		logger.Info("test", zap.Object("safepoint", sp))

		require.Equal(t, 1, logs.Len())
		entry := logs.All()[0]

		// Check the context fields
		fields := entry.ContextMap()
		spFields, ok := fields["safepoint"].(map[string]interface{})
		require.True(t, ok, "safepoint field should be a map")

		require.Equal(t, "br-test-id", spFields["ID"])
		require.Equal(t, "5m0s", spFields["TTL"]) // 300 seconds = 5m0s
		require.Equal(t, uint64(1000), spFields["BackupTS"])
	})

	t.Run("ZeroValues", func(t *testing.T) {
		sp := gc.BRServiceSafePoint{
			ID:       "",
			TTL:      0,
			BackupTS: 0,
		}

		core, logs := observer.New(zapcore.InfoLevel)
		logger := zap.New(core)

		// Should not panic
		logger.Info("test", zap.Object("safepoint", sp))

		require.Equal(t, 1, logs.Len())
		entry := logs.All()[0]

		fields := entry.ContextMap()
		spFields, ok := fields["safepoint"].(map[string]interface{})
		require.True(t, ok, "safepoint field should be a map")

		require.Equal(t, "", spFields["ID"])
		require.Equal(t, "0s", spFields["TTL"])
		require.Equal(t, uint64(0), spFields["BackupTS"])
	})

	t.Run("LargeTTL", func(t *testing.T) {
		sp := gc.BRServiceSafePoint{
			ID:       "br-test",
			TTL:      86400, // 24 hours
			BackupTS: 1000,
		}

		core, logs := observer.New(zapcore.InfoLevel)
		logger := zap.New(core)

		logger.Info("test", zap.Object("safepoint", sp))

		require.Equal(t, 1, logs.Len())
		entry := logs.All()[0]

		fields := entry.ContextMap()
		spFields, ok := fields["safepoint"].(map[string]interface{})
		require.True(t, ok)

		require.Equal(t, "24h0m0s", spFields["TTL"])
	})
}
