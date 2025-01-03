package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPiTRTableTracker(t *testing.T) {
	t.Run("test new tracker", func(t *testing.T) {
		tracker := NewPiTRTableTracker()
		require.NotNil(t, tracker)
		require.NotNil(t, tracker.DBIdToTable)
		require.Empty(t, tracker.DBIdToTable)
	})

	t.Run("test update and contains table", func(t *testing.T) {
		tracker := NewPiTRTableTracker()

		tracker.AddDB(1)
		tracker.AddTable(1, 100)
		tracker.AddDB(2)
		require.True(t, tracker.ContainsDB(1))
		require.True(t, tracker.ContainsDB(2))
		require.True(t, tracker.ContainsTable(1, 100))
		require.False(t, tracker.ContainsTable(1, 101))
		require.False(t, tracker.ContainsTable(2, 100))

		tracker.AddTable(1, 101)
		tracker.AddTable(2, 200)
		require.True(t, tracker.ContainsTable(1, 100))
		require.True(t, tracker.ContainsTable(1, 101))
		require.True(t, tracker.ContainsTable(2, 200))

		tracker.AddTable(3, 300)
		require.True(t, tracker.ContainsDB(3))
		require.True(t, tracker.ContainsTable(3, 300))
	})

	t.Run("test remove table", func(t *testing.T) {
		tracker := NewPiTRTableTracker()

		tracker.AddTable(1, 100)
		tracker.AddTable(1, 101)

		require.True(t, tracker.Remove(1, 100))
		require.False(t, tracker.ContainsTable(1, 100))
		require.True(t, tracker.ContainsTable(1, 101))

		require.False(t, tracker.Remove(1, 102))
		require.False(t, tracker.Remove(2, 100))
	})
}
