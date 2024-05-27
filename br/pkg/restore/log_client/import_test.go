// Copyright 2024 PingCAP, Inc.
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

package logclient_test

import (
	"context"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func TestImportKVFiles(t *testing.T) {
	var (
		importer            = logclient.LogFileImporter{}
		ctx                 = context.Background()
		shiftStartTS uint64 = 100
		startTS      uint64 = 200
		restoreTS    uint64 = 300
	)

	err := importer.ImportKVFiles(
		ctx,
		[]*logclient.LogDataFileInfo{
			{
				DataFileInfo: &backuppb.DataFileInfo{
					Path: "log3",
				},
			},
			{
				DataFileInfo: &backuppb.DataFileInfo{
					Path: "log1",
				},
			},
		},
		nil,
		shiftStartTS,
		startTS,
		restoreTS,
		false,
	)
	require.True(t, berrors.ErrInvalidArgument.Equal(err))
}

func TestFilterFilesByRegion(t *testing.T) {
	files := []*logclient.LogDataFileInfo{
		{
			DataFileInfo: &backuppb.DataFileInfo{
				Path: "log3",
			},
		},
		{
			DataFileInfo: &backuppb.DataFileInfo{
				Path: "log1",
			},
		},
	}
	ranges := []kv.KeyRange{
		{
			StartKey: []byte("1111"),
			EndKey:   []byte("2222"),
		}, {
			StartKey: []byte("3333"),
			EndKey:   []byte("4444"),
		},
	}

	testCases := []struct {
		r        split.RegionInfo
		subfiles []*logclient.LogDataFileInfo
		err      error
	}{
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("0000"),
					EndKey:   []byte("1110"),
				},
			},
			subfiles: []*logclient.LogDataFileInfo{},
			err:      nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("0000"),
					EndKey:   []byte("1111"),
				},
			},
			subfiles: []*logclient.LogDataFileInfo{
				files[0],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("0000"),
					EndKey:   []byte("2222"),
				},
			},
			subfiles: []*logclient.LogDataFileInfo{
				files[0],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("2222"),
					EndKey:   []byte("3332"),
				},
			},
			subfiles: []*logclient.LogDataFileInfo{
				files[0],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("2223"),
					EndKey:   []byte("3332"),
				},
			},
			subfiles: []*logclient.LogDataFileInfo{},
			err:      nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("3332"),
					EndKey:   []byte("3333"),
				},
			},
			subfiles: []*logclient.LogDataFileInfo{
				files[1],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("4444"),
					EndKey:   []byte("5555"),
				},
			},
			subfiles: []*logclient.LogDataFileInfo{
				files[1],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("4444"),
					EndKey:   nil,
				},
			},
			subfiles: []*logclient.LogDataFileInfo{
				files[1],
			},
			err: nil,
		},
		{
			r: split.RegionInfo{
				Region: &metapb.Region{
					StartKey: []byte("0000"),
					EndKey:   nil,
				},
			},
			subfiles: files,
			err:      nil,
		},
	}

	for _, c := range testCases {
		subfile, err := logclient.FilterFilesByRegion(files, ranges, &c.r)
		require.Equal(t, err, c.err)
		require.Equal(t, subfile, c.subfiles)
	}
}
