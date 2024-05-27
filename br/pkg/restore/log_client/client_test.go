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
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/mock"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/tablecodec"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/keepalive"
)

var mc *mock.Cluster

var deleteRangeQueryList = []*stream.PreDelRangeQuery{
	{
		Sql: "INSERT IGNORE INTO mysql.gc_delete_range VALUES (%?, %?, %?, %?, %?), (%?, %?, %?, %?, %?)",
		ParamsList: []stream.DelRangeParams{
			{
				JobID:    1,
				ElemID:   1,
				StartKey: "a",
				EndKey:   "b",
			},
			{
				JobID:    1,
				ElemID:   2,
				StartKey: "b",
				EndKey:   "c",
			},
		},
	},
	{
		// When the last table id has no rewrite rule
		Sql: "INSERT IGNORE INTO mysql.gc_delete_range VALUES (%?, %?, %?, %?, %?),",
		ParamsList: []stream.DelRangeParams{
			{
				JobID:    2,
				ElemID:   1,
				StartKey: "a",
				EndKey:   "b",
			},
		},
	},
	{
		// When all the tables have no rewrite rule
		Sql:        "INSERT IGNORE INTO mysql.gc_delete_range VALUES ",
		ParamsList: nil,
	},
}

func TestDeleteRangeQueryExec(t *testing.T) {
	ctx := context.Background()
	m := mc
	g := gluetidb.New()
	client := logclient.NewRestoreClient(
		utiltest.NewFakePDClient(nil, false, nil), nil, nil, keepalive.ClientParameters{})
	err := client.Init(g, m.Storage)
	require.NoError(t, err)

	client.RunGCRowsLoader(ctx)

	for _, query := range deleteRangeQueryList {
		client.RecordDeleteRange(query)
	}

	require.NoError(t, client.InsertGCRows(ctx))
}

func TestDeleteRangeQuery(t *testing.T) {
	ctx := context.Background()
	m := mc

	g := gluetidb.New()
	client := logclient.NewRestoreClient(
		utiltest.NewFakePDClient(nil, false, nil), nil, nil, keepalive.ClientParameters{})
	err := client.Init(g, m.Storage)
	require.NoError(t, err)

	client.RunGCRowsLoader(ctx)

	for _, query := range deleteRangeQueryList {
		client.RecordDeleteRange(query)
	}
	querys := client.GetGCRows()
	require.Equal(t, len(querys), len(deleteRangeQueryList))
	for i, query := range querys {
		expected_query := deleteRangeQueryList[i]
		require.Equal(t, expected_query.Sql, query.Sql)
		require.Equal(t, len(expected_query.ParamsList), len(query.ParamsList))
		for j := range expected_query.ParamsList {
			require.Equal(t, expected_query.ParamsList[j], query.ParamsList[j])
		}
	}
}

func MockEmptySchemasReplace() *stream.SchemasReplace {
	dbMap := make(map[stream.UpstreamID]*stream.DBReplace)
	return stream.NewSchemasReplace(
		dbMap,
		true,
		nil,
		1,
		filter.All(),
		nil,
		nil,
		nil,
	)
}

func TestRestoreBatchMetaKVFiles(t *testing.T) {
	client := logclient.NewRestoreClient(nil, nil, nil, keepalive.ClientParameters{})
	files := []*backuppb.DataFileInfo{}
	// test empty files and entries
	next, err := client.RestoreBatchMetaKVFiles(context.Background(), files[0:], nil, make([]*logclient.KvEntryWithTS, 0), math.MaxUint64, nil, nil, "")
	require.NoError(t, err)
	require.Equal(t, 0, len(next))
}

func TestRestoreMetaKVFilesWithBatchMethod1(t *testing.T) {
	files_default := []*backuppb.DataFileInfo{}
	files_write := []*backuppb.DataFileInfo{}
	batchCount := 0

	sr := MockEmptySchemasReplace()
	err := logclient.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		files_default,
		files_write,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			files []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*logclient.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*logclient.KvEntryWithTS, error) {
			require.Equal(t, 0, len(entries))
			require.Equal(t, 0, len(files))
			batchCount++
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, batchCount, 2)
}

func TestRestoreMetaKVFilesWithBatchMethod2_default_empty(t *testing.T) {
	files_default := []*backuppb.DataFileInfo{}
	files_write := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 120,
		},
	}
	batchCount := 0

	sr := MockEmptySchemasReplace()
	err := logclient.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		files_default,
		files_write,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			files []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*logclient.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*logclient.KvEntryWithTS, error) {
			if len(entries) == 0 && len(files) == 0 {
				require.Equal(t, stream.DefaultCF, cf)
				batchCount++
			} else {
				require.Equal(t, 0, len(entries))
				require.Equal(t, 1, len(files))
				require.Equal(t, uint64(100), files[0].MinTs)
				require.Equal(t, stream.WriteCF, cf)
			}
			require.Equal(t, uint64(math.MaxUint64), filterTS)
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, batchCount, 1)
}

func TestRestoreMetaKVFilesWithBatchMethod2_write_empty_1(t *testing.T) {
	files_default := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 120,
		},
	}
	files_write := []*backuppb.DataFileInfo{}
	batchCount := 0

	sr := MockEmptySchemasReplace()
	err := logclient.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		files_default,
		files_write,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			files []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*logclient.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*logclient.KvEntryWithTS, error) {
			if len(entries) == 0 && len(files) == 0 {
				require.Equal(t, stream.WriteCF, cf)
				batchCount++
			} else {
				require.Equal(t, 0, len(entries))
				require.Equal(t, 1, len(files))
				require.Equal(t, uint64(100), files[0].MinTs)
				require.Equal(t, stream.DefaultCF, cf)
			}
			require.Equal(t, uint64(math.MaxUint64), filterTS)
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, batchCount, 1)
}

func TestRestoreMetaKVFilesWithBatchMethod2_write_empty_2(t *testing.T) {
	files_default := []*backuppb.DataFileInfo{
		{
			Path:   "f1",
			MinTs:  100,
			MaxTs:  120,
			Length: logclient.MetaKVBatchSize - 1000,
		},
		{
			Path:   "f2",
			MinTs:  110,
			MaxTs:  1100,
			Length: logclient.MetaKVBatchSize,
		},
	}
	files_write := []*backuppb.DataFileInfo{}
	emptyCount := 0
	batchCount := 0

	sr := MockEmptySchemasReplace()
	err := logclient.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		files_default,
		files_write,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			files []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*logclient.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*logclient.KvEntryWithTS, error) {
			if len(entries) == 0 && len(files) == 0 {
				// write - write
				require.Equal(t, stream.WriteCF, cf)
				emptyCount++
				if emptyCount == 1 {
					require.Equal(t, uint64(110), filterTS)
				} else {
					require.Equal(t, uint64(math.MaxUint64), filterTS)
				}
			} else {
				// default - default
				batchCount++
				require.Equal(t, 1, len(files))
				require.Equal(t, stream.DefaultCF, cf)
				if batchCount == 1 {
					require.Equal(t, uint64(100), files[0].MinTs)
					require.Equal(t, uint64(110), filterTS)
					return nil, nil
				}
				require.Equal(t, 0, len(entries))
			}
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, batchCount, 2)
	require.Equal(t, emptyCount, 2)
}

func TestRestoreMetaKVFilesWithBatchMethod_with_entries(t *testing.T) {
	files_default := []*backuppb.DataFileInfo{
		{
			Path:   "f1",
			MinTs:  100,
			MaxTs:  120,
			Length: logclient.MetaKVBatchSize - 1000,
		},
		{
			Path:   "f2",
			MinTs:  110,
			MaxTs:  1100,
			Length: logclient.MetaKVBatchSize,
		},
	}
	files_write := []*backuppb.DataFileInfo{}
	emptyCount := 0
	batchCount := 0

	sr := MockEmptySchemasReplace()
	err := logclient.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		files_default,
		files_write,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			files []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*logclient.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*logclient.KvEntryWithTS, error) {
			if len(entries) == 0 && len(files) == 0 {
				// write - write
				require.Equal(t, stream.WriteCF, cf)
				emptyCount++
				if emptyCount == 1 {
					require.Equal(t, uint64(110), filterTS)
				} else {
					require.Equal(t, uint64(math.MaxUint64), filterTS)
				}
			} else {
				// default - default
				batchCount++
				require.Equal(t, 1, len(files))
				require.Equal(t, stream.DefaultCF, cf)
				if batchCount == 1 {
					require.Equal(t, uint64(100), files[0].MinTs)
					require.Equal(t, uint64(110), filterTS)
					return nil, nil
				}
				require.Equal(t, 0, len(entries))
			}
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, batchCount, 2)
	require.Equal(t, emptyCount, 2)
}

func TestRestoreMetaKVFilesWithBatchMethod3(t *testing.T) {
	defaultFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 140,
			MaxTs: 150,
		},
		{
			Path:  "f5",
			MinTs: 150,
			MaxTs: 160,
		},
	}
	writeFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 135,
			MaxTs: 150,
		},
		{
			Path:  "f5",
			MinTs: 150,
			MaxTs: 160,
		},
	}

	batchCount := 0
	result := make(map[int][]*backuppb.DataFileInfo)
	resultKV := make(map[int]int)

	sr := MockEmptySchemasReplace()
	err := logclient.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		defaultFiles,
		writeFiles,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			fs []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*logclient.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*logclient.KvEntryWithTS, error) {
			result[batchCount] = fs
			t.Log(filterTS)
			resultKV[batchCount] = len(entries)
			batchCount++
			return make([]*logclient.KvEntryWithTS, batchCount), nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, len(result), 4)
	require.Equal(t, result[0], defaultFiles[0:3])
	require.Equal(t, resultKV[0], 0)
	require.Equal(t, result[1], writeFiles[0:4])
	require.Equal(t, resultKV[1], 0)
	require.Equal(t, result[2], defaultFiles[3:])
	require.Equal(t, resultKV[2], 1)
	require.Equal(t, result[3], writeFiles[4:])
	require.Equal(t, resultKV[3], 2)
}

func TestRestoreMetaKVFilesWithBatchMethod4(t *testing.T) {
	defaultFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 110,
			MaxTs: 150,
		},
	}

	writeFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 110,
			MaxTs: 150,
		},
	}
	batchCount := 0
	result := make(map[int][]*backuppb.DataFileInfo)

	sr := MockEmptySchemasReplace()
	err := logclient.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		defaultFiles,
		writeFiles,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			fs []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*logclient.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*logclient.KvEntryWithTS, error) {
			result[batchCount] = fs
			batchCount++
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, len(result), 4)
	require.Equal(t, result[0], defaultFiles[0:2])
	require.Equal(t, result[1], writeFiles[0:2])
	require.Equal(t, result[2], defaultFiles[2:])
	require.Equal(t, result[3], writeFiles[2:])
}

func TestRestoreMetaKVFilesWithBatchMethod5(t *testing.T) {
	defaultFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 110,
			MaxTs: 150,
		},
	}

	writeFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f3",
			MinTs: 100,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 100,
			MaxTs: 150,
		},
	}
	batchCount := 0
	result := make(map[int][]*backuppb.DataFileInfo)

	sr := MockEmptySchemasReplace()
	err := logclient.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		defaultFiles,
		writeFiles,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			fs []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*logclient.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*logclient.KvEntryWithTS, error) {
			result[batchCount] = fs
			batchCount++
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, len(result), 4)
	require.Equal(t, result[0], defaultFiles[0:2])
	require.Equal(t, result[1], writeFiles[0:])
	require.Equal(t, result[2], defaultFiles[2:])
	require.Equal(t, len(result[3]), 0)
}

func TestRestoreMetaKVFilesWithBatchMethod6(t *testing.T) {
	defaultFiles := []*backuppb.DataFileInfo{
		{
			Path:   "f1",
			MinTs:  100,
			MaxTs:  120,
			Length: 100,
		},
		{
			Path:   "f2",
			MinTs:  100,
			MaxTs:  120,
			Length: logclient.MetaKVBatchSize - 100,
		},
		{
			Path:   "f3",
			MinTs:  110,
			MaxTs:  130,
			Length: 1,
		},
		{
			Path:   "f4",
			MinTs:  140,
			MaxTs:  150,
			Length: 1,
		},
		{
			Path:   "f5",
			MinTs:  150,
			MaxTs:  160,
			Length: 1,
		},
	}

	writeFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 140,
		},
		{
			Path:  "f4",
			MinTs: 120,
			MaxTs: 150,
		},
		{
			Path:  "f5",
			MinTs: 140,
			MaxTs: 160,
		},
	}

	batchCount := 0
	result := make(map[int][]*backuppb.DataFileInfo)
	resultKV := make(map[int]int)

	sr := MockEmptySchemasReplace()
	err := logclient.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		defaultFiles,
		writeFiles,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			fs []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*logclient.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*logclient.KvEntryWithTS, error) {
			result[batchCount] = fs
			t.Log(filterTS)
			resultKV[batchCount] = len(entries)
			batchCount++
			return make([]*logclient.KvEntryWithTS, batchCount), nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, len(result), 6)
	require.Equal(t, result[0], defaultFiles[0:2])
	require.Equal(t, resultKV[0], 0)
	require.Equal(t, result[1], writeFiles[0:2])
	require.Equal(t, resultKV[1], 0)
	require.Equal(t, result[2], defaultFiles[2:3])
	require.Equal(t, resultKV[2], 1)
	require.Equal(t, result[3], writeFiles[2:4])
	require.Equal(t, resultKV[3], 2)
	require.Equal(t, result[4], defaultFiles[3:])
	require.Equal(t, resultKV[4], 3)
	require.Equal(t, result[5], writeFiles[4:])
	require.Equal(t, resultKV[5], 4)
}

func TestSortMetaKVFiles(t *testing.T) {
	files := []*backuppb.DataFileInfo{
		{
			Path:       "f5",
			MinTs:      110,
			MaxTs:      150,
			ResolvedTs: 120,
		},
		{
			Path:       "f1",
			MinTs:      100,
			MaxTs:      100,
			ResolvedTs: 80,
		},
		{
			Path:       "f2",
			MinTs:      100,
			MaxTs:      100,
			ResolvedTs: 90,
		},
		{
			Path:       "f4",
			MinTs:      110,
			MaxTs:      130,
			ResolvedTs: 120,
		},
		{
			Path:       "f3",
			MinTs:      105,
			MaxTs:      130,
			ResolvedTs: 100,
		},
	}

	files = logclient.SortMetaKVFiles(files)
	require.Equal(t, len(files), 5)
	require.Equal(t, files[0].Path, "f1")
	require.Equal(t, files[1].Path, "f2")
	require.Equal(t, files[2].Path, "f3")
	require.Equal(t, files[3].Path, "f4")
	require.Equal(t, files[4].Path, "f5")
}

func toLogDataFileInfoIter(logIter iter.TryNextor[*backuppb.DataFileInfo]) logclient.LogIter {
	return iter.Map(logIter, func(d *backuppb.DataFileInfo) *logclient.LogDataFileInfo {
		return &logclient.LogDataFileInfo{
			DataFileInfo: d,
		}
	})
}

func TestApplyKVFilesWithSingelMethod(t *testing.T) {
	var (
		totalKVCount int64  = 0
		totalSize    uint64 = 0
		logs                = make([]string, 0)
	)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
		},
		{
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
		}, {
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*logclient.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		totalKVCount += kvCount
		totalSize += size
		for _, f := range files {
			logs = append(logs, f.GetPath())
		}
	}

	logclient.ApplyKVFilesWithSingelMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		applyFunc,
		&applyWg,
	)

	require.Equal(t, totalKVCount, int64(15))
	require.Equal(t, totalSize, uint64(300))
	require.Equal(t, logs, []string{"log1", "log2", "log3"})
}

func TestApplyKVFilesWithBatchMethod1(t *testing.T) {
	var (
		runCount            = 0
		batchCount   int    = 3
		batchSize    uint64 = 1000
		totalKVCount int64  = 0
		logs                = make([][]string, 0)
	)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log5",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
			RegionId:        1,
		}, {
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log4",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          800,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		},
		{
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          200,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*logclient.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		runCount += 1
		totalKVCount += kvCount
		log := make([]string, 0, len(files))
		for _, f := range files {
			log = append(log, f.GetPath())
		}
		logs = append(logs, log)
	}

	logclient.ApplyKVFilesWithBatchMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		batchCount,
		batchSize,
		applyFunc,
		&applyWg,
	)

	require.Equal(t, runCount, 3)
	require.Equal(t, totalKVCount, int64(25))
	require.Equal(t,
		logs,
		[][]string{
			{"log1", "log2"},
			{"log3", "log4"},
			{"log5"},
		},
	)
}

func TestApplyKVFilesWithBatchMethod2(t *testing.T) {
	var (
		runCount            = 0
		batchCount   int    = 2
		batchSize    uint64 = 1500
		totalKVCount int64  = 0
		logs                = make([][]string, 0)
	)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
			RegionId:        1,
		}, {
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log4",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log5",
			NumberOfEntries: 5,
			Length:          800,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		},
		{
			Path:            "log6",
			NumberOfEntries: 5,
			Length:          200,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*logclient.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		runCount += 1
		totalKVCount += kvCount
		log := make([]string, 0, len(files))
		for _, f := range files {
			log = append(log, f.GetPath())
		}
		logs = append(logs, log)
	}

	logclient.ApplyKVFilesWithBatchMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		batchCount,
		batchSize,
		applyFunc,
		&applyWg,
	)

	require.Equal(t, runCount, 4)
	require.Equal(t, totalKVCount, int64(30))
	require.Equal(t,
		logs,
		[][]string{
			{"log2", "log3"},
			{"log5", "log6"},
			{"log4"},
			{"log1"},
		},
	)
}

func TestApplyKVFilesWithBatchMethod3(t *testing.T) {
	var (
		runCount            = 0
		batchCount   int    = 2
		batchSize    uint64 = 1500
		totalKVCount int64  = 0
		logs                = make([][]string, 0)
	)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          2000,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
			RegionId:        1,
		}, {
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          2000,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log5",
			NumberOfEntries: 5,
			Length:          800,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        3,
		},
		{
			Path:            "log6",
			NumberOfEntries: 5,
			Length:          200,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        3,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*logclient.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		runCount += 1
		totalKVCount += kvCount
		log := make([]string, 0, len(files))
		for _, f := range files {
			log = append(log, f.GetPath())
		}
		logs = append(logs, log)
	}

	logclient.ApplyKVFilesWithBatchMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		batchCount,
		batchSize,
		applyFunc,
		&applyWg,
	)

	require.Equal(t, totalKVCount, int64(25))
	require.Equal(t,
		logs,
		[][]string{
			{"log2"},
			{"log5", "log6"},
			{"log3"},
			{"log1"},
		},
	)
}

func TestApplyKVFilesWithBatchMethod4(t *testing.T) {
	var (
		runCount            = 0
		batchCount   int    = 2
		batchSize    uint64 = 1500
		totalKVCount int64  = 0
		logs                = make([][]string, 0)
	)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          2000,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
			TableId:         1,
		}, {
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         1,
		}, {
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         2,
		}, {
			Path:            "log4",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         1,
		}, {
			Path:            "log5",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			TableId:         2,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*logclient.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		runCount += 1
		totalKVCount += kvCount
		log := make([]string, 0, len(files))
		for _, f := range files {
			log = append(log, f.GetPath())
		}
		logs = append(logs, log)
	}

	logclient.ApplyKVFilesWithBatchMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		batchCount,
		batchSize,
		applyFunc,
		&applyWg,
	)

	require.Equal(t, runCount, 4)
	require.Equal(t, totalKVCount, int64(25))
	require.Equal(t,
		logs,
		[][]string{
			{"log2", "log4"},
			{"log5"},
			{"log3"},
			{"log1"},
		},
	)
}

func TestApplyKVFilesWithBatchMethod5(t *testing.T) {
	var lock sync.Mutex
	types := make([]backuppb.FileType, 0)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          2000,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
			TableId:         1,
		}, {
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         1,
		}, {
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         2,
		}, {
			Path:            "log4",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         1,
		}, {
			Path:            "log5",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			TableId:         2,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*logclient.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		if len(files) == 0 {
			return
		}
		applyWg.Add(1)
		go func() {
			defer applyWg.Done()
			if files[0].Type == backuppb.FileType_Put {
				time.Sleep(time.Second)
			}
			lock.Lock()
			types = append(types, files[0].Type)
			lock.Unlock()
		}()
	}

	logclient.ApplyKVFilesWithBatchMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		2,
		1500,
		applyFunc,
		&applyWg,
	)

	applyWg.Wait()
	require.Equal(t, backuppb.FileType_Delete, types[len(types)-1])

	types = make([]backuppb.FileType, 0)
	logclient.ApplyKVFilesWithSingelMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		applyFunc,
		&applyWg,
	)

	applyWg.Wait()
	require.Equal(t, backuppb.FileType_Delete, types[len(types)-1])
}

type mockLogIter struct {
	next int
}

func (m *mockLogIter) TryNext(ctx context.Context) iter.IterResult[*logclient.LogDataFileInfo] {
	if m.next > 10000 {
		return iter.Done[*logclient.LogDataFileInfo]()
	}
	m.next += 1
	return iter.Emit(&logclient.LogDataFileInfo{
		DataFileInfo: &backuppb.DataFileInfo{
			StartKey: []byte(fmt.Sprintf("a%d", m.next)),
			EndKey:   []byte("b"),
			Length:   1024, // 1 KB
		},
	})
}

func TestLogFilesIterWithSplitHelper(t *testing.T) {
	var tableID int64 = 76
	var oldTableID int64 = 80
	rewriteRules := &utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
			},
		},
	}
	rewriteRulesMap := map[int64]*utils.RewriteRules{
		oldTableID: rewriteRules,
	}
	mockIter := &mockLogIter{}
	ctx := context.Background()
	logIter := logclient.NewLogFilesIterWithSplitHelper(mockIter, rewriteRulesMap, utiltest.NewFakeSplitClient(), 144*1024*1024, 1440000)
	next := 0
	for r := logIter.TryNext(ctx); !r.Finished; r = logIter.TryNext(ctx) {
		require.NoError(t, r.Err)
		next += 1
		require.Equal(t, []byte(fmt.Sprintf("a%d", next)), r.Item.StartKey)
	}
}
