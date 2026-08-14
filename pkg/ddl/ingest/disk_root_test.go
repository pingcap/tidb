// Copyright 2022 PingCAP, Inc.
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

package ingest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
)

func TestRiskOfDiskFull(t *testing.T) {
	require.Equal(t, uint64(10), reservedCapacityBytes(100))
	require.Equal(t, uint64(11), reservedCapacityBytes(101))
	require.False(t, RiskOfDiskFull(11, 100))
	require.False(t, RiskOfDiskFull(10, 100))
	require.True(t, RiskOfDiskFull(9, 100))
	require.False(t, RiskOfDiskFull(11, 101))
	require.True(t, RiskOfDiskFull(10, 101))
}

func TestCheckLocalSortFreeDisk(t *testing.T) {
	const execID = "10.0.1.8:4000"
	tests := []struct {
		name    string
		check   localSortFreeDiskCheck
		wantErr bool
		errMsgs []string
		notMsgs []string
	}{
		{
			name: "enough space",
			check: localSortFreeDiskCheck{
				execID:                   execID,
				sortPath:                 "/tmp/local-sort",
				availableBytes:           20 * size.GB,
				totalCapacityBytes:       20 * size.GB,
				otherRunningJobCount:     1,
				otherRunningRuntimeSlots: 4,
				otherRunningUsedBytes:    size.GB,
				currentJobRuntimeSlots:   2,
			},
		},
		{
			name: "not enough for aggregate slots",
			check: localSortFreeDiskCheck{
				execID:                   execID,
				sortPath:                 "/tmp/local-sort",
				availableBytes:           12 * size.GB,
				totalCapacityBytes:       20 * size.GB,
				otherRunningJobCount:     1,
				otherRunningRuntimeSlots: 3,
				currentJobRuntimeSlots:   2,
			},
			wantErr: true,
		},
		{
			name: "below reserved capacity",
			check: localSortFreeDiskCheck{
				execID:                   execID,
				sortPath:                 "/tmp/local-sort",
				availableBytes:           9 * size.GB,
				totalCapacityBytes:       10 * size.GB,
				otherRunningJobCount:     2,
				otherRunningRuntimeSlots: 4,
				otherRunningUsedBytes:    2 * size.GB,
				currentJobRuntimeSlots:   1,
			},
			wantErr: true,
		},
		{
			name: "quota cap leaves enough space",
			check: localSortFreeDiskCheck{
				execID:                   execID,
				sortPath:                 "/tmp/local-sort",
				availableBytes:           81 * size.GB,
				totalCapacityBytes:       200 * size.GB,
				otherRunningJobCount:     1,
				otherRunningRuntimeSlots: 40,
				otherRunningUsedBytes:    40 * size.GB,
				currentJobRuntimeSlots:   20,
			},
		},
		{
			name: "quota cap still short",
			check: localSortFreeDiskCheck{
				execID:                   execID,
				sortPath:                 "/tmp/local-sort",
				availableBytes:           80 * size.GB,
				totalCapacityBytes:       200 * size.GB,
				otherRunningJobCount:     1,
				otherRunningRuntimeSlots: 40,
				otherRunningUsedBytes:    40 * size.GB,
				currentJobRuntimeSlots:   20,
			},
			wantErr: true,
		},
		{
			name: "usage already above quota",
			check: localSortFreeDiskCheck{
				execID:                   execID,
				sortPath:                 "/tmp/local-sort",
				availableBytes:           21 * size.GB,
				totalCapacityBytes:       200 * size.GB,
				otherRunningJobCount:     1,
				otherRunningRuntimeSlots: 40,
				otherRunningUsedBytes:    101 * size.GB,
				currentJobRuntimeSlots:   20,
			},
		},
		{
			name: "user-facing error",
			check: localSortFreeDiskCheck{
				execID:                   execID,
				sortPath:                 "/tmp/local-sort",
				availableBytes:           size.GB,
				totalCapacityBytes:       size.GB,
				otherRunningJobCount:     1,
				otherRunningRuntimeSlots: 1,
				currentJobRuntimeSlots:   1,
			},
			wantErr: true,
			errMsgs: []string{
				"insufficient free disk space on TiDB node 10.0.1.8:4000 at /tmp/local-sort: 1073741824 bytes available",
				"the add-index job cannot start because low disk space would degrade SST ingestion",
				"Free disk space on this TiDB node by removing unnecessary logs or files",
			},
			notMsgs: []string{
				"running local-sort job count",
				"runtime slots",
				"bytes per slot",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkLocalSortFreeDisk(tt.check)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, msg := range tt.errMsgs {
				require.ErrorContains(t, err, msg)
			}
			for _, msg := range tt.notMsgs {
				require.NotContains(t, err.Error(), msg)
			}
		})
	}
	require.EqualValues(t, 2*size.GB, localSortBytesPerSlot)
}
