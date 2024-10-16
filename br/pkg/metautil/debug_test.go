// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metautil_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"testing"

	"github.com/gogo/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
)

func flushMetaFile(
	ctx context.Context,
	t *testing.T,
	fname string,
	metaFile *backuppb.MetaFile,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
) *backuppb.File {
	content, err := metaFile.Marshal()
	require.NoError(t, err)

	encyptedContent, iv, err := metautil.Encrypt(content, cipher)
	require.NoError(t, err)

	err = storage.WriteFile(ctx, fname, encyptedContent)
	require.NoError(t, err)

	checksum := sha256.Sum256(content)
	file := &backuppb.File{
		Name:     fname,
		Sha256:   checksum[:],
		Size_:    uint64(len(content)),
		CipherIv: iv,
	}

	return file
}

func flushStatsFile(
	ctx context.Context,
	t *testing.T,
	fname string,
	statsFile *backuppb.StatsFile,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
) *backuppb.StatsFileIndex {
	content, err := proto.Marshal(statsFile)
	require.NoError(t, err)

	checksum := sha256.Sum256(content)
	sizeOri := uint64(len(content))
	encryptedContent, iv, err := metautil.Encrypt(content, cipher)
	require.NoError(t, err)

	err = storage.WriteFile(ctx, fname, encryptedContent)
	require.NoError(t, err)

	return &backuppb.StatsFileIndex{
		Name:       fname,
		Sha256:     checksum[:],
		SizeEnc:    uint64(len(encryptedContent)),
		SizeOri:    sizeOri,
		CipherIv:   iv,
		InlineData: []byte(fmt.Sprintf("%d", rand.Int())),
	}
}

func TestDecodeMetaFile(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	s, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	cipher := &backuppb.CipherInfo{CipherType: 1}
	file1 := flushMetaFile(ctx, t, "data", &backuppb.MetaFile{
		DataFiles: []*backuppb.File{
			{
				Name:       "1.sst",
				Sha256:     []byte("1.sst"),
				StartKey:   []byte("start"),
				EndKey:     []byte("end"),
				EndVersion: 1,
				Crc64Xor:   1,
				TotalKvs:   2,
				TotalBytes: 3,
				Cf:         "write",
				CipherIv:   []byte("1.sst"),
			},
		},
	}, s, cipher)
	stats := flushStatsFile(ctx, t, "stats", &backuppb.StatsFile{Blocks: []*backuppb.StatsBlock{
		{
			PhysicalId: 1,
			JsonTable:  []byte("1"),
		},
		{
			PhysicalId: 2,
			JsonTable:  []byte("2"),
		},
	}}, s, cipher)
	metaFile2 := &backuppb.MetaFile{
		Schemas: []*backuppb.Schema{
			{
				Db:              []byte(`{"db_name":{"L":"test","O":"test"},"id":1,"state":5}`),
				Table:           []byte(`{"id":2,"state":5}`),
				Crc64Xor:        1,
				TotalKvs:        2,
				TotalBytes:      3,
				TiflashReplicas: 4,
				Stats:           []byte(`{"a":1}`),
				StatsIndex:      []*backuppb.StatsFileIndex{stats},
			},
		},
	}
	file2 := flushMetaFile(ctx, t, "schema", metaFile2, s, cipher)

	{
		err = metautil.DecodeMetaFile(ctx, s, cipher, &backuppb.MetaFile{MetaFiles: []*backuppb.File{file1}})
		require.NoError(t, err)
		content, err := s.ReadFile(ctx, "jsons/data.json")
		require.NoError(t, err)
		metaFile, err := utils.UnmarshalMetaFile(content)
		require.NoError(t, err)
		require.Equal(t, 1, len(metaFile.DataFiles))
		require.Equal(t, "1.sst", metaFile.DataFiles[0].Name)
		require.Equal(t, []byte("1.sst"), metaFile.DataFiles[0].Sha256)
		require.Equal(t, []byte("start"), metaFile.DataFiles[0].StartKey)
		require.Equal(t, []byte("end"), metaFile.DataFiles[0].EndKey)
		require.Equal(t, uint64(1), metaFile.DataFiles[0].EndVersion)
		require.Equal(t, uint64(1), metaFile.DataFiles[0].Crc64Xor)
		require.Equal(t, uint64(2), metaFile.DataFiles[0].TotalKvs)
		require.Equal(t, uint64(3), metaFile.DataFiles[0].TotalBytes)
		require.Equal(t, "write", metaFile.DataFiles[0].Cf)
		require.Equal(t, []byte("1.sst"), metaFile.DataFiles[0].CipherIv)
	}

	{
		err = metautil.DecodeMetaFile(ctx, s, cipher, &backuppb.MetaFile{MetaFiles: []*backuppb.File{file2}})
		require.NoError(t, err)
		{
			content, err := s.ReadFile(ctx, "jsons/schema.json")
			require.NoError(t, err)
			metaFile, err := utils.UnmarshalMetaFile(content)
			require.NoError(t, err)
			require.Equal(t, 1, len(metaFile.Schemas))
			require.Equal(t, metaFile2.Schemas[0].Db, metaFile.Schemas[0].Db)
			require.Equal(t, metaFile2.Schemas[0].Table, metaFile.Schemas[0].Table)
			require.Equal(t, uint64(1), metaFile.Schemas[0].Crc64Xor)
			require.Equal(t, uint64(2), metaFile.Schemas[0].TotalKvs)
			require.Equal(t, uint64(3), metaFile.Schemas[0].TotalBytes)
			require.Equal(t, uint32(4), metaFile.Schemas[0].TiflashReplicas)
			require.Equal(t, metaFile2.Schemas[0].Stats, metaFile.Schemas[0].Stats)
			statsIndex := metaFile.Schemas[0].StatsIndex
			require.Equal(t, 1, len(statsIndex))
			require.Equal(t, stats.Name, statsIndex[0].Name)
			require.Equal(t, stats.Sha256, statsIndex[0].Sha256)
			require.Equal(t, stats.SizeEnc, statsIndex[0].SizeEnc)
			require.Equal(t, stats.SizeOri, statsIndex[0].SizeOri)
			require.Equal(t, stats.CipherIv, statsIndex[0].CipherIv)
			require.Equal(t, stats.InlineData, statsIndex[0].InlineData)
		}
		{
			content, err := s.ReadFile(ctx, "jsons/stats.json")
			require.NoError(t, err)
			statsFileBlocks, err := utils.UnmarshalStatsFile(content)
			require.NoError(t, err)
			require.Equal(t, 2, len(statsFileBlocks.Blocks))
			require.Equal(t, int64(1), statsFileBlocks.Blocks[0].PhysicalId)
			require.Equal(t, []byte("1"), statsFileBlocks.Blocks[0].JsonTable)
			require.Equal(t, int64(2), statsFileBlocks.Blocks[1].PhysicalId)
			require.Equal(t, []byte("2"), statsFileBlocks.Blocks[1].JsonTable)
		}
	}
}
