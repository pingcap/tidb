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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
)

func TestRiskOfDiskFull(t *testing.T) {
	require.Equal(t, uint64(10), minFreeDiskBytes(100))
	require.Equal(t, uint64(11), minFreeDiskBytes(101))
	require.False(t, riskOfDiskFull(11, 100))
	require.False(t, riskOfDiskFull(10, 100))
	require.True(t, riskOfDiskFull(9, 100))
	require.False(t, riskOfDiskFull(11, 101))
	require.True(t, riskOfDiskFull(10, 101))
}

func TestCheckLocalSortDiskSpace(t *testing.T) {
	const execID = "10.0.1.8:4000"
	tests := []struct {
		name    string
		check   localSortDiskSpaceCheck
		wantErr bool
		errMsgs []string
		notMsgs []string
	}{
		{
			name: "enough space",
			check: localSortDiskSpaceCheck{
				execID:                  execID,
				sortPath:                "/tmp/local-sort",
				availableBytes:          7 * size.GB,
				totalCapacityBytes:      20 * size.GB,
				currentTaskRuntimeSlots: 2,
			},
		},
		{
			name: "available space equal to required space",
			check: localSortDiskSpaceCheck{
				execID:                  execID,
				sortPath:                "/tmp/local-sort",
				availableBytes:          6 * size.GB,
				totalCapacityBytes:      20 * size.GB,
				currentTaskRuntimeSlots: 2,
			},
			wantErr: true,
			errMsgs: []string{
				"6442450944 bytes available; available free disk space must be greater than 6442450944 bytes",
			},
			notMsgs: []string{
				"6442450944 bytes required",
			},
		},
		{
			name: "above 10 percent but insufficient for current task",
			check: localSortDiskSpaceCheck{
				execID:                  execID,
				sortPath:                "/tmp/local-sort",
				availableBytes:          2 * size.GB,
				totalCapacityBytes:      10 * size.GB,
				currentTaskRuntimeSlots: 1,
			},
			wantErr: true,
		},
		{
			name: "quota cap leaves enough space",
			check: localSortDiskSpaceCheck{
				execID:                  execID,
				sortPath:                "/tmp/local-sort",
				availableBytes:          121 * size.GB,
				totalCapacityBytes:      200 * size.GB,
				currentTaskRuntimeSlots: 60,
			},
		},
		{
			name: "quota cap still short",
			check: localSortDiskSpaceCheck{
				execID:                  execID,
				sortPath:                "/tmp/local-sort",
				availableBytes:          120 * size.GB,
				totalCapacityBytes:      200 * size.GB,
				currentTaskRuntimeSlots: 60,
			},
			wantErr: true,
		},
		{
			name: "user-facing error",
			check: localSortDiskSpaceCheck{
				execID:                  execID,
				sortPath:                "/tmp/local-sort",
				availableBytes:          size.GB,
				totalCapacityBytes:      size.GB,
				currentTaskRuntimeSlots: 1,
			},
			wantErr: true,
			errMsgs: []string{
				"insufficient free disk space on TiDB node 10.0.1.8:4000 at /tmp/local-sort: 1073741824 bytes available; available free disk space must be greater than 2254857831 bytes",
				"the add-index job cannot start because low disk space would degrade SST ingestion",
				"Free disk space on this TiDB node by removing unnecessary logs or files",
			},
			notMsgs: []string{
				"runtime slots",
				"bytes per slot",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkLocalSortDiskSpace(tt.check)
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
	require.EqualValues(t, 2*size.GB, localSortHeadroomBytesPerSlot)
}

func TestCheckLocalSortDiskSpaceErrorClassification(t *testing.T) {
	t.Run("probe failure is not ErrIngestCheckEnvFailed", func(t *testing.T) {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/ingest/mockLocalSortDiskSpaceProbeFailed", "return"))
		t.Cleanup(func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/ingest/mockLocalSortDiskSpaceProbeFailed"))
		})
		err := CheckLocalSortDiskSpace("10.0.1.8:4000", 1)
		require.ErrorContains(t, err, "mock local sort disk probe failed")
		require.False(t, dbterror.ErrIngestCheckEnvFailed.Equal(err))
	})
	t.Run("confirmed insufficient space is ErrIngestCheckEnvFailed", func(t *testing.T) {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/ingest/mockLocalSortDiskSpaceInsufficient", "return"))
		t.Cleanup(func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/ingest/mockLocalSortDiskSpaceInsufficient"))
		})
		err := CheckLocalSortDiskSpace("10.0.1.8:4000", 1)
		require.True(t, dbterror.ErrIngestCheckEnvFailed.Equal(err))
		require.ErrorContains(t, err, "mock insufficient local sort disk space")
	})
}
