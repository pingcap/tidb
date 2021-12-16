package paging

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGrowPagingSize(t *testing.T) {
	t.Parallel()

	require.Equal(t, GrowPagingSize(MinPagingSize), MinPagingSize*pagingSizeGrow)
	require.Equal(t, GrowPagingSize(MaxPagingSize), MaxPagingSize)
	require.Equal(t, GrowPagingSize(MaxPagingSize/pagingSizeGrow+1), MaxPagingSize)
}

func TestCalculateSeekCnt(t *testing.T) {
	t.Parallel()

	require.InDelta(t, CalculateSeekCnt(0), 0, 0.1)
	require.InDelta(t, CalculateSeekCnt(1), 1, 0.1)
	require.InDelta(t, CalculateSeekCnt(MinPagingSize), 1, 0.1)
	require.InDelta(t, CalculateSeekCnt(pagingGrowingSum), maxPagingSizeShift+1, 0.1)
	require.InDelta(t, CalculateSeekCnt(pagingGrowingSum+1), maxPagingSizeShift+2, 0.1)
	require.InDelta(t, CalculateSeekCnt(pagingGrowingSum+MaxPagingSize), maxPagingSizeShift+2, 0.1)
}
