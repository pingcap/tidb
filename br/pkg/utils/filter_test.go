package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPiTRTableTracker(t *testing.T) {
	t.Run("test new tracker", func(t *testing.T) {
		tracker := NewPiTRIdTracker()
		require.NotNil(t, tracker)
		require.NotNil(t, tracker.DBIds)
		require.NotNil(t, tracker.TableIdToDBId)
		require.Empty(t, tracker.DBIds)
		require.Empty(t, tracker.TableIdToDBId)
	})

	t.Run("test update and contains table", func(t *testing.T) {
		tracker := NewPiTRIdTracker()

		tracker.AddDB(1)
		tracker.TrackTableId(1, 100)
		tracker.AddDB(2)
		require.True(t, tracker.ContainsDB(1))
		require.True(t, tracker.ContainsDB(2))
		require.True(t, tracker.ContainsTableId(1, 100))
		require.False(t, tracker.ContainsTableId(1, 101))
		require.False(t, tracker.ContainsTableId(2, 100))

		tracker.TrackTableId(1, 101)
		tracker.TrackTableId(2, 200)
		require.True(t, tracker.ContainsTableId(1, 100))
		require.True(t, tracker.ContainsTableId(1, 101))
		require.True(t, tracker.ContainsTableId(2, 200))

		tracker.TrackTableId(3, 300)
		require.True(t, tracker.ContainsDB(3))
		require.True(t, tracker.ContainsTableId(3, 300))

		// Test RemoveTableId
		tracker.RemoveTableId(100)
		require.False(t, tracker.ContainsTableId(1, 100))
		require.True(t, tracker.ContainsDB(1))           // DB should still exist
		require.True(t, tracker.ContainsTableId(1, 101)) // Other table should still exist
	})
}
