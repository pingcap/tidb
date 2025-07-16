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
		require.NotNil(t, tracker.TableIdToDBIds)
		require.Empty(t, tracker.DBIds)
		require.Empty(t, tracker.TableIdToDBIds)
	})

	t.Run("test update and contains table", func(t *testing.T) {
		tracker := NewPiTRIdTracker()

		tracker.AddDB(1)
		tracker.TrackTableId(1, 100)
		tracker.AddDB(2)
		require.True(t, tracker.ContainsDB(1))
		require.True(t, tracker.ContainsDB(2))
		require.True(t, tracker.ContainsDBAndTableId(1, 100))
		require.False(t, tracker.ContainsDBAndTableId(1, 101))
		require.False(t, tracker.ContainsDBAndTableId(2, 100))

		tracker.TrackTableId(1, 101)
		tracker.TrackTableId(2, 200)
		require.True(t, tracker.ContainsDBAndTableId(1, 100))
		require.True(t, tracker.ContainsDBAndTableId(1, 101))
		require.True(t, tracker.ContainsDBAndTableId(2, 200))

		tracker.TrackTableId(3, 300)
		require.True(t, tracker.ContainsDB(3))
		require.True(t, tracker.ContainsDBAndTableId(3, 300))
	})
}
