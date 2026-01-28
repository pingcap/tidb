package copr

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/stretchr/testify/require"
)

func TestCopTaskToPBBatchTasksRangeVersionsLenMismatch(t *testing.T) {
	parent := &copTask{
		batchTaskList: map[uint64]*batchedCopTask{},
	}
	sub := &copTask{
		rangeVersions: []uint64{1, 2},
	}
	parent.batchTaskList[1] = &batchedCopTask{
		task: sub,
		region: coprocessor.RegionInfo{
			Ranges: []*coprocessor.KeyRange{
				{Start: []byte("a"), End: []byte("b")},
			},
		},
	}

	_, err := parent.ToPBBatchTasks()
	require.Error(t, err)
}
