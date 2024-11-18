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

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
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
		split.NewFakePDClient(nil, false, nil), nil, nil, keepalive.ClientParameters{})
	err := client.Init(ctx, g, m.Storage)
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
		split.NewFakePDClient(nil, false, nil), nil, nil, keepalive.ClientParameters{})
	err := client.Init(ctx, g, m.Storage)
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

	logclient.ApplyKVFilesWithSingleMethod(
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
	logclient.ApplyKVFilesWithSingleMethod(
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
	w := restore.PipelineRestorerWrapper[*logclient.LogDataFileInfo]{
		PipelineRegionsSplitter: split.NewPipelineRegionsSplitter(split.NewFakeSplitClient(), 144*1024*1024, 1440000),
	}
	s, err := logclient.NewLogSplitStrategy(ctx, false, nil, rewriteRulesMap, func(uint64, uint64) {})
	require.NoError(t, err)
	logIter := w.WithSplit(context.Background(), mockIter, s)
	next := 0
	for r := logIter.TryNext(ctx); !r.Finished; r = logIter.TryNext(ctx) {
		require.NoError(t, r.Err)
		next += 1
		require.Equal(t, []byte(fmt.Sprintf("a%d", next)), r.Item.StartKey)
	}
}

type fakeSession struct {
	glue.Session
}

func (fs fakeSession) GetSessionCtx() sessionctx.Context {
	return fakeSessionContext{}
}

type fakeSessionContext struct {
	sessionctx.Context
}

func (fsc fakeSessionContext) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return fakeSQLExecutor{}
}

type fakeSQLExecutor struct {
	sqlexec.RestrictedSQLExecutor
}

func (fse fakeSQLExecutor) ExecRestrictedSQL(_ context.Context, _ []sqlexec.OptionFuncAlias, query string, args ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	return nil, nil, errors.Errorf("name: %s, %v", query, args)
}

func TestInitSchemasReplaceForDDL(t *testing.T) {
	ctx := context.Background()

	{
		client := logclient.TEST_NewLogClient(123, 1, 2, 1, domain.NewMockDomain(), fakeSession{})
		cfg := &logclient.InitSchemaConfig{IsNewTask: false}
		_, err := client.InitSchemasReplaceForDDL(ctx, cfg, nil)
		require.Error(t, err)
		require.Regexp(t, "failed to get pitr id map from mysql.tidb_pitr_id_map.* [2, 1]", err.Error())
	}

	{
		client := logclient.TEST_NewLogClient(123, 1, 2, 1, domain.NewMockDomain(), fakeSession{})
		cfg := &logclient.InitSchemaConfig{IsNewTask: true}
		_, err := client.InitSchemasReplaceForDDL(ctx, cfg, nil)
		require.Error(t, err)
		require.Regexp(t, "failed to get pitr id map from mysql.tidb_pitr_id_map.* [1, 1]", err.Error())
	}

	{
		s := utiltest.CreateRestoreSchemaSuite(t)
		tk := testkit.NewTestKit(t, s.Mock.Storage)
		tk.Exec(session.CreatePITRIDMap)
		g := gluetidb.New()
		se, err := g.CreateSession(s.Mock.Storage)
		require.NoError(t, err)
		client := logclient.TEST_NewLogClient(123, 1, 2, 1, domain.NewMockDomain(), se)
		cfg := &logclient.InitSchemaConfig{IsNewTask: true}
		_, err = client.InitSchemasReplaceForDDL(ctx, cfg, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "miss upstream table information at `start-ts`(1) but the full backup path is not specified")
	}
}

func downstreamID(upstreamID int64) int64 {
	return upstreamID + 10000000
}

func emptyDB(startupID, endupID int64, replaces map[int64]*stream.DBReplace) {
	for id := startupID; id < endupID; id += 1 {
		replaces[id] = &stream.DBReplace{
			Name: fmt.Sprintf("db_%d", id),
			DbID: downstreamID(id),
		}
	}
}

func emptyTables(dbupID, startupID, endupID int64, replaces map[int64]*stream.DBReplace) {
	tableMap := make(map[int64]*stream.TableReplace)
	for id := startupID; id < endupID; id += 1 {
		tableMap[id] = &stream.TableReplace{
			Name:    fmt.Sprintf("table_%d", id),
			TableID: downstreamID(id),
		}
	}
	replaces[dbupID] = &stream.DBReplace{
		Name:     fmt.Sprintf("db_%d", dbupID),
		DbID:     downstreamID(dbupID),
		TableMap: tableMap,
	}
}

func partitions(dbupID, tableupID, startupID, endupID int64, replaces map[int64]*stream.DBReplace) {
	partitionMap := make(map[int64]int64)
	for id := startupID; id < endupID; id += 1 {
		partitionMap[id] = downstreamID(id)
	}
	replaces[dbupID] = &stream.DBReplace{
		Name: fmt.Sprintf("db_%d", dbupID),
		DbID: downstreamID(dbupID),
		TableMap: map[int64]*stream.TableReplace{
			tableupID: {
				Name:         fmt.Sprintf("table_%d", tableupID),
				TableID:      downstreamID(tableupID),
				PartitionMap: partitionMap,
			},
		},
	}
}

func getDBMap() map[int64]*stream.DBReplace {
	replaces := make(map[int64]*stream.DBReplace)
	emptyDB(1, 3000, replaces)
	emptyTables(3000, 3001, 8000, replaces)
	partitions(8000, 8001, 8002, 12000, replaces)
	emptyTables(12000, 12001, 30000, replaces)
	return replaces
}

func TestPITRIDMap(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)
	tk.Exec(session.CreatePITRIDMap)
	g := gluetidb.New()
	se, err := g.CreateSession(s.Mock.Storage)
	require.NoError(t, err)
	client := logclient.TEST_NewLogClient(123, 1, 2, 3, nil, se)
	baseSchemaReplaces := &stream.SchemasReplace{
		DbMap: getDBMap(),
	}
	err = client.TEST_saveIDMap(ctx, baseSchemaReplaces)
	require.NoError(t, err)
	newSchemaReplaces, err := client.TEST_initSchemasMap(ctx, 1)
	require.NoError(t, err)
	require.Nil(t, newSchemaReplaces)
	client2 := logclient.TEST_NewLogClient(123, 1, 2, 4, nil, se)
	newSchemaReplaces, err = client2.TEST_initSchemasMap(ctx, 2)
	require.NoError(t, err)
	require.Nil(t, newSchemaReplaces)
	newSchemaReplaces, err = client.TEST_initSchemasMap(ctx, 2)
	require.NoError(t, err)

	require.Equal(t, len(baseSchemaReplaces.DbMap), len(newSchemaReplaces))
	for _, dbMap := range newSchemaReplaces {
		baseDbMap := baseSchemaReplaces.DbMap[dbMap.IdMap.UpstreamId]
		require.NotNil(t, baseDbMap)
		require.Equal(t, baseDbMap.DbID, dbMap.IdMap.DownstreamId)
		require.Equal(t, baseDbMap.Name, dbMap.Name)
		require.Equal(t, len(baseDbMap.TableMap), len(dbMap.Tables))
		for _, tableMap := range dbMap.Tables {
			baseTableMap := baseDbMap.TableMap[tableMap.IdMap.UpstreamId]
			require.NotNil(t, baseTableMap)
			require.Equal(t, baseTableMap.TableID, tableMap.IdMap.DownstreamId)
			require.Equal(t, baseTableMap.Name, tableMap.Name)
			require.Equal(t, len(baseTableMap.PartitionMap), len(tableMap.Partitions))
			for _, partitionMap := range tableMap.Partitions {
				basePartitionMap, exist := baseTableMap.PartitionMap[partitionMap.UpstreamId]
				require.True(t, exist)
				require.Equal(t, basePartitionMap, partitionMap.DownstreamId)
			}
		}
	}
}

type mockLogStrategy struct {
	*logclient.LogSplitStrategy
	expectSplitCount int
}

func (m *mockLogStrategy) ShouldSplit() bool {
	return m.AccumulateCount == m.expectSplitCount
}

func TestLogSplitStrategy(t *testing.T) {
	ctx := context.Background()

	// Define rewrite rules for table ID transformations.
	rules := map[int64]*utils.RewriteRules{
		1: {
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.GenTableRecordPrefix(1),
					NewKeyPrefix: tablecodec.GenTableRecordPrefix(100),
				},
			},
		},
		2: {
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.GenTableRecordPrefix(2),
					NewKeyPrefix: tablecodec.GenTableRecordPrefix(200),
				},
			},
		},
	}

	// Define initial regions for the mock PD client.
	oriRegions := [][]byte{
		{},
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
	}

	// Set up a mock PD client with predefined regions.
	storesMap := make(map[uint64]*metapb.Store)
	storesMap[1] = &metapb.Store{Id: 1}
	mockPDCli := split.NewMockPDClientForSplit()
	mockPDCli.SetStores(storesMap)
	mockPDCli.SetRegions(oriRegions)

	// Create a split client with the mock PD client.
	client := split.NewClient(mockPDCli, nil, nil, 100, 4)

	// Define a mock iterator with sample data files.
	mockIter := iter.FromSlice([]*backuppb.DataFileInfo{
		fakeFile(1, 100, 100, 100),
		fakeFile(1, 200, 2*units.MiB, 200),
		fakeFile(2, 100, 3*units.MiB, 300),
		fakeFile(3, 100, 10*units.MiB, 100000),
		fakeFile(1, 300, 3*units.MiB, 10),
		fakeFile(1, 400, 4*units.MiB, 10),
	})
	logIter := toLogDataFileInfoIter(mockIter)

	// Initialize a wrapper for the file restorer with a region splitter.
	wrapper := restore.PipelineRestorerWrapper[*logclient.LogDataFileInfo]{
		PipelineRegionsSplitter: split.NewPipelineRegionsSplitter(client, 4*units.MB, 400),
	}

	// Create a log split strategy with the given rewrite rules.
	strategy, err := logclient.NewLogSplitStrategy(ctx, false, nil, rules, func(u1, u2 uint64) {})
	require.NoError(t, err)

	// Set up a mock strategy to control split behavior.
	expectSplitCount := 2
	mockStrategy := &mockLogStrategy{
		LogSplitStrategy: strategy,
		// fakeFile(3, 100, 10*units.MiB, 100000) will skipped due to no rewrite rule found.
		expectSplitCount: expectSplitCount,
	}

	// Use the wrapper to apply the split strategy to the log iterator.
	helper := wrapper.WithSplit(ctx, logIter, mockStrategy)

	// Iterate over the log items and verify the split behavior.
	count := 0
	for i := helper.TryNext(ctx); !i.Finished; i = helper.TryNext(ctx) {
		require.NoError(t, i.Err)
		if count == expectSplitCount {
			// Verify that no split occurs initially due to insufficient data.
			regions, err := mockPDCli.ScanRegions(ctx, []byte{}, []byte{}, 0)
			require.NoError(t, err)
			require.Len(t, regions, 3)
			require.Equal(t, []byte{}, regions[0].Meta.StartKey)
			require.Equal(t, codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)), regions[1].Meta.StartKey)
			require.Equal(t, codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)), regions[2].Meta.StartKey)
			require.Equal(t, codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)), regions[2].Meta.EndKey)
		}
		// iter.Filterout execute first
		count += 1
	}

	// Verify that a split occurs on the second region due to excess data.
	regions, err := mockPDCli.ScanRegions(ctx, []byte{}, []byte{}, 0)
	require.NoError(t, err)
	require.Len(t, regions, 4)
	require.Equal(t, fakeRowKey(100, 400), kv.Key(regions[1].Meta.EndKey))
}

type mockCompactedStrategy struct {
	*logclient.CompactedFileSplitStrategy
	expectSplitCount int
}

func (m *mockCompactedStrategy) ShouldSplit() bool {
	return m.AccumulateCount%m.expectSplitCount == 0
}

func TestCompactedSplitStrategy(t *testing.T) {
	ctx := context.Background()

	rules := map[int64]*utils.RewriteRules{
		1: {
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.GenTableRecordPrefix(1),
					NewKeyPrefix: tablecodec.GenTableRecordPrefix(100),
				},
			},
		},
		2: {
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.GenTableRecordPrefix(2),
					NewKeyPrefix: tablecodec.GenTableRecordPrefix(200),
				},
			},
		},
	}

	oriRegions := [][]byte{
		{},
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
	}

	cases := []struct {
		MockSubcompationIter iter.TryNextor[*backuppb.LogFileSubcompaction]
		ExpectRegionEndKeys  [][]byte
	}{
		{
			iter.FromSlice([]*backuppb.LogFileSubcompaction{
				fakeSubCompactionWithOneSst(1, 100, 16*units.MiB, 100),
				fakeSubCompactionWithOneSst(1, 200, 32*units.MiB, 200),
				fakeSubCompactionWithOneSst(2, 100, 48*units.MiB, 300),
				fakeSubCompactionWithOneSst(3, 100, 100*units.MiB, 100000),
			}),
			// no split
			// table 1 has not accumlate enough 400 keys or 4MB
			[][]byte{
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
			},
		},
		{
			iter.FromSlice([]*backuppb.LogFileSubcompaction{
				fakeSubCompactionWithOneSst(1, 100, 16*units.MiB, 100),
				fakeSubCompactionWithOneSst(1, 200, 32*units.MiB, 200),
				fakeSubCompactionWithOneSst(1, 100, 32*units.MiB, 10),
				fakeSubCompactionWithOneSst(2, 100, 48*units.MiB, 300),
			}),
			// split on table 1
			// table 1 has accumlate enough keys
			[][]byte{
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
				[]byte(fakeRowKey(100, 200)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
			},
		},
		{
			iter.FromSlice([]*backuppb.LogFileSubcompaction{
				fakeSubCompactionWithOneSst(1, 100, 16*units.MiB, 100),
				fakeSubCompactionWithOneSst(1, 200, 32*units.MiB, 200),
				fakeSubCompactionWithOneSst(2, 100, 32*units.MiB, 300),
				fakeSubCompactionWithOneSst(3, 100, 10*units.MiB, 100000),
				fakeSubCompactionWithOneSst(1, 300, 48*units.MiB, 13),
				fakeSubCompactionWithOneSst(1, 400, 64*units.MiB, 14),
				fakeSubCompactionWithOneSst(1, 100, 1*units.MiB, 15),
			}),
			[][]byte{
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
				[]byte(fakeRowKey(100, 400)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
			},
		},
	}
	for _, ca := range cases {
		storesMap := make(map[uint64]*metapb.Store)
		storesMap[1] = &metapb.Store{Id: 1}
		mockPDCli := split.NewMockPDClientForSplit()
		mockPDCli.SetStores(storesMap)
		mockPDCli.SetRegions(oriRegions)

		client := split.NewClient(mockPDCli, nil, nil, 100, 4)
		wrapper := restore.PipelineRestorerWrapper[*backuppb.LogFileSubcompaction]{
			PipelineRegionsSplitter: split.NewPipelineRegionsSplitter(client, 4*units.MB, 400),
		}

		strategy := logclient.NewCompactedFileSplitStrategy(rules, nil, nil)
		mockStrategy := &mockCompactedStrategy{
			CompactedFileSplitStrategy: strategy,
			expectSplitCount:           3,
		}

		helper := wrapper.WithSplit(ctx, ca.MockSubcompationIter, mockStrategy)

		for i := helper.TryNext(ctx); !i.Finished; i = helper.TryNext(ctx) {
			require.NoError(t, i.Err)
		}

		regions, err := mockPDCli.ScanRegions(ctx, []byte{}, []byte{}, 0)
		require.NoError(t, err)
		require.Len(t, regions, len(ca.ExpectRegionEndKeys))
		for i, endKey := range ca.ExpectRegionEndKeys {
			require.Equal(t, endKey, regions[i].Meta.EndKey)
		}
	}
}

func TestCompactedSplitStrategyWithCheckpoint(t *testing.T) {
	ctx := context.Background()

	rules := map[int64]*utils.RewriteRules{
		1: {
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.GenTableRecordPrefix(1),
					NewKeyPrefix: tablecodec.GenTableRecordPrefix(100),
				},
			},
		},
		2: {
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.GenTableRecordPrefix(2),
					NewKeyPrefix: tablecodec.GenTableRecordPrefix(200),
				},
			},
		},
	}

	oriRegions := [][]byte{
		{},
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
	}

	cases := []struct {
		MockSubcompationIter iter.TryNextor[*backuppb.LogFileSubcompaction]
		CheckpointSet        map[string]struct{}
		ProcessedKVCount     int
		ProcessedSize        int
		ExpectRegionEndKeys  [][]byte
	}{
		{
			iter.FromSlice([]*backuppb.LogFileSubcompaction{
				fakeSubCompactionWithOneSst(1, 100, 16*units.MiB, 100),
				fakeSubCompactionWithOneSst(1, 200, 32*units.MiB, 200),
				fakeSubCompactionWithOneSst(2, 100, 48*units.MiB, 300),
				fakeSubCompactionWithOneSst(3, 100, 100*units.MiB, 100000),
			}),
			map[string]struct{}{
				"1:100": {},
				"1:200": {},
			},
			300,
			48 * units.MiB,
			// no split, checkpoint files came in order
			[][]byte{
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
			},
		},
		{
			iter.FromSlice([]*backuppb.LogFileSubcompaction{
				fakeSubCompactionWithOneSst(1, 100, 16*units.MiB, 100),
				fakeSubCompactionWithOneSst(1, 200, 32*units.MiB, 200),
				fakeSubCompactionWithOneSst(1, 100, 32*units.MiB, 10),
				fakeSubCompactionWithOneSst(2, 100, 48*units.MiB, 300),
			}),
			map[string]struct{}{
				"1:100": {},
			},
			110,
			48 * units.MiB,
			// no split, checkpoint files came in different order
			[][]byte{
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
			},
		},
		{
			iter.FromSlice([]*backuppb.LogFileSubcompaction{
				fakeSubCompactionWithOneSst(1, 100, 16*units.MiB, 100),
				fakeSubCompactionWithOneSst(1, 200, 32*units.MiB, 200),
				fakeSubCompactionWithOneSst(2, 100, 32*units.MiB, 300),
				fakeSubCompactionWithOneSst(3, 100, 10*units.MiB, 100000),
				fakeSubCompactionWithOneSst(1, 300, 48*units.MiB, 13),
				fakeSubCompactionWithOneSst(1, 400, 64*units.MiB, 14),
				fakeSubCompactionWithOneSst(1, 100, 1*units.MiB, 15),
			}),
			map[string]struct{}{
				"1:300": {},
				"1:400": {},
			},
			27,
			112 * units.MiB,
			// no split, the main file has skipped due to checkpoint.
			[][]byte{
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
			},
		},
		{
			iter.FromSlice([]*backuppb.LogFileSubcompaction{
				fakeSubCompactionWithOneSst(1, 100, 16*units.MiB, 100),
				fakeSubCompactionWithOneSst(1, 200, 32*units.MiB, 200),
				fakeSubCompactionWithOneSst(2, 100, 32*units.MiB, 300),
				fakeSubCompactionWithOneSst(3, 100, 10*units.MiB, 100000),
				fakeSubCompactionWithOneSst(1, 300, 48*units.MiB, 13),
				fakeSubCompactionWithOneSst(1, 400, 64*units.MiB, 14),
				fakeSubCompactionWithOneSst(1, 100, 1*units.MiB, 15),
			}),
			map[string]struct{}{
				"1:100": {},
				"1:200": {},
			},
			315,
			49 * units.MiB,
			[][]byte{
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
				[]byte(fakeRowKey(100, 400)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
			},
		},
		{
			iter.FromSlice([]*backuppb.LogFileSubcompaction{
				fakeSubCompactionWithOneSst(1, 100, 16*units.MiB, 100),
				fakeSubCompactionWithMultiSsts(1, 200, 32*units.MiB, 200),
				fakeSubCompactionWithOneSst(2, 100, 32*units.MiB, 300),
				fakeSubCompactionWithOneSst(3, 100, 10*units.MiB, 100000),
				fakeSubCompactionWithOneSst(1, 300, 48*units.MiB, 13),
				fakeSubCompactionWithOneSst(1, 400, 64*units.MiB, 14),
				fakeSubCompactionWithOneSst(1, 100, 1*units.MiB, 15),
			}),
			map[string]struct{}{
				"1:100": {},
				"1:200": {},
			},
			315,
			49 * units.MiB,
			[][]byte{
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
				[]byte(fakeRowKey(100, 300)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
				codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
			},
		},
	}
	for _, ca := range cases {
		storesMap := make(map[uint64]*metapb.Store)
		storesMap[1] = &metapb.Store{Id: 1}
		mockPDCli := split.NewMockPDClientForSplit()
		mockPDCli.SetStores(storesMap)
		mockPDCli.SetRegions(oriRegions)

		client := split.NewClient(mockPDCli, nil, nil, 100, 4)
		wrapper := restore.PipelineRestorerWrapper[*backuppb.LogFileSubcompaction]{
			PipelineRegionsSplitter: split.NewPipelineRegionsSplitter(client, 4*units.MB, 400),
		}
		totalSize := 0
		totalKvCount := 0

		strategy := logclient.NewCompactedFileSplitStrategy(rules, ca.CheckpointSet, func(u1, u2 uint64) {
			totalKvCount += int(u1)
			totalSize += int(u2)
		})
		mockStrategy := &mockCompactedStrategy{
			CompactedFileSplitStrategy: strategy,
			expectSplitCount:           3,
		}

		helper := wrapper.WithSplit(ctx, ca.MockSubcompationIter, mockStrategy)

		for i := helper.TryNext(ctx); !i.Finished; i = helper.TryNext(ctx) {
			require.NoError(t, i.Err)
		}

		regions, err := mockPDCli.ScanRegions(ctx, []byte{}, []byte{}, 0)
		require.NoError(t, err)
		require.Len(t, regions, len(ca.ExpectRegionEndKeys))
		for i, endKey := range ca.ExpectRegionEndKeys {
			require.Equal(t, endKey, regions[i].Meta.EndKey)
		}
		require.Equal(t, totalKvCount, ca.ProcessedKVCount)
		require.Equal(t, totalSize, ca.ProcessedSize)
	}
}

func fakeSubCompactionWithMultiSsts(tableID, rowID int64, length uint64, num uint64) *backuppb.LogFileSubcompaction {
	return &backuppb.LogFileSubcompaction{
		Meta: &backuppb.LogFileSubcompactionMeta{
			TableId: tableID,
		},
		SstOutputs: []*backuppb.File{
			{
				Name:     fmt.Sprintf("%d:%d", tableID, rowID),
				StartKey: fakeRowRawKey(tableID, rowID),
				EndKey:   fakeRowRawKey(tableID, rowID+1),
				Size_:    length,
				TotalKvs: num,
			},
			{
				Name:     fmt.Sprintf("%d:%d", tableID, rowID+1),
				StartKey: fakeRowRawKey(tableID, rowID+1),
				EndKey:   fakeRowRawKey(tableID, rowID+2),
				Size_:    length,
				TotalKvs: num,
			},
		},
	}
}
func fakeSubCompactionWithOneSst(tableID, rowID int64, length uint64, num uint64) *backuppb.LogFileSubcompaction {
	return &backuppb.LogFileSubcompaction{
		Meta: &backuppb.LogFileSubcompactionMeta{
			TableId: tableID,
		},
		SstOutputs: []*backuppb.File{
			{
				Name:     fmt.Sprintf("%d:%d", tableID, rowID),
				StartKey: fakeRowRawKey(tableID, rowID),
				EndKey:   fakeRowRawKey(tableID, rowID+1),
				Size_:    length,
				TotalKvs: num,
			},
		},
	}
}

func fakeFile(tableID, rowID int64, length uint64, num int64) *backuppb.DataFileInfo {
	return &backuppb.DataFileInfo{
		StartKey:        fakeRowKey(tableID, rowID),
		EndKey:          fakeRowKey(tableID, rowID+1),
		TableId:         tableID,
		Length:          length,
		NumberOfEntries: num,
	}
}

func fakeRowKey(tableID, rowID int64) kv.Key {
	return codec.EncodeBytes(nil, fakeRowRawKey(tableID, rowID))
}

func fakeRowRawKey(tableID, rowID int64) kv.Key {
	return tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(tableID), kv.IntHandle(rowID))
}
