package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPiTRTableTracker(t *testing.T) {
	t.Run("test new tracker", func(t *testing.T) {
		tracker := NewPiTRTableTracker()
		require.NotNil(t, tracker)
		require.NotNil(t, tracker.DBIdToPhysicalId)
		require.Empty(t, tracker.DBIdToPhysicalId)
	})

	t.Run("test update and contains table", func(t *testing.T) {
		tracker := NewPiTRTableTracker()

		tracker.AddDB(1)
		tracker.AddPhysicalId(1, 100)
		tracker.AddDB(2)
		require.True(t, tracker.ContainsDB(1))
		require.True(t, tracker.ContainsDB(2))
		require.True(t, tracker.ContainsPhysicalId(1, 100))
		require.False(t, tracker.ContainsPhysicalId(1, 101))
		require.False(t, tracker.ContainsPhysicalId(2, 100))

		tracker.AddPhysicalId(1, 101)
		tracker.AddPhysicalId(2, 200)
		require.True(t, tracker.ContainsPhysicalId(1, 100))
		require.True(t, tracker.ContainsPhysicalId(1, 101))
		require.True(t, tracker.ContainsPhysicalId(2, 200))

		tracker.AddPhysicalId(3, 300)
		require.True(t, tracker.ContainsDB(3))
		require.True(t, tracker.ContainsPhysicalId(3, 300))
	})

	t.Run("test remove table", func(t *testing.T) {
		tracker := NewPiTRTableTracker()

		tracker.AddPhysicalId(1, 100)
		tracker.AddPhysicalId(1, 101)

		require.True(t, tracker.Remove(1, 100))
		require.False(t, tracker.ContainsPhysicalId(1, 100))
		require.True(t, tracker.ContainsPhysicalId(1, 101))

		require.False(t, tracker.Remove(1, 102))
		require.False(t, tracker.Remove(2, 100))
	})
}
