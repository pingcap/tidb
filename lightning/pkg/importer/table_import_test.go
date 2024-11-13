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

package importer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	restoremock "github.com/pingcap/tidb/lightning/pkg/importer/mock"
	ropts "github.com/pingcap/tidb/lightning/pkg/importer/opts"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/lightning/pkg/web"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/backend/tidb"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/lightning/worker"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	tmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/promutil"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/mock/gomock"
)

const (
	tiflashReplica1 = 1
	tiflashReplica2 = 2
	tblSize         = 222
)

type tableRestoreSuiteBase struct {
	tr  *TableImporter
	cfg *config.Config

	tableInfo  *checkpoints.TidbTableInfo
	dbInfo     *checkpoints.TidbDBInfo
	tableMeta  *mydump.MDTableMeta
	tableMeta2 *mydump.MDTableMeta

	store storage.ExternalStorage
}

func mockTiflashTableInfo(t *testing.T, sql string, replica uint64) *model.TableInfo {
	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	se := tmock.NewContext()
	node, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	core, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 0xabcdef)
	require.NoError(t, err)
	core.State = model.StatePublic
	core.TiFlashReplica = &model.TiFlashReplicaInfo{
		Count:     replica,
		Available: true,
	}

	return core
}

func (s *tableRestoreSuiteBase) setupSuite(t *testing.T) {
	web.EnableCurrentProgress()

	core := mockTiflashTableInfo(t, `CREATE TABLE "table" (
		a INT,
		b INT,
		c INT,
		KEY (b)
	)
`, tiflashReplica1)

	core2 := mockTiflashTableInfo(t, `CREATE TABLE "table" (
	a INT,
	b INT,
	c INT,
	KEY (b)
)
`, tiflashReplica2)

	s.tableInfo = &checkpoints.TidbTableInfo{Name: "table", DB: "db", Core: core}
	s.dbInfo = &checkpoints.TidbDBInfo{
		Name: "db",
		Tables: map[string]*checkpoints.TidbTableInfo{
			"table":  s.tableInfo,
			"table2": {Name: "table2", DB: "db", Core: core2},
		},
	}

	// Write some sample SQL dump
	fakeDataDir := t.TempDir()
	store, err := storage.NewLocalStorage(fakeDataDir)
	require.NoError(t, err)
	s.store = store

	fakeDataFilesCount := 6
	fakeDataFilesContent := []byte("INSERT INTO `table` VALUES (1, 2, 3);")
	require.Equal(t, 37, len(fakeDataFilesContent))
	fakeDataFiles := make([]mydump.FileInfo, 0, fakeDataFilesCount)
	for i := 1; i <= fakeDataFilesCount; i++ {
		fakeFileName := fmt.Sprintf("db.table.%d.sql", i)
		fakeDataPath := filepath.Join(fakeDataDir, fakeFileName)
		err = os.WriteFile(fakeDataPath, fakeDataFilesContent, 0o644)
		require.NoError(t, err)
		fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{
			TableName: filter.Table{Schema: "db", Name: "table"},
			FileMeta: mydump.SourceFileMeta{
				Path:     fakeFileName,
				Type:     mydump.SourceTypeSQL,
				SortKey:  strconv.Itoa(i),
				FileSize: 37,
				RealSize: 37,
			},
		})
	}

	fakeCsvContent := []byte("1,2,3\r\n4,5,6\r\n")
	csvName := "db.table.99.csv"
	err = os.WriteFile(filepath.Join(fakeDataDir, csvName), fakeCsvContent, 0o644)
	require.NoError(t, err)
	fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{
		TableName: filter.Table{Schema: "db", Name: "table"},
		FileMeta: mydump.SourceFileMeta{
			Path:     csvName,
			Type:     mydump.SourceTypeCSV,
			SortKey:  "99",
			FileSize: 14,
			RealSize: 14,
		},
	})

	s.tableMeta = &mydump.MDTableMeta{
		DB:        "db",
		Name:      "table",
		TotalSize: tblSize,
		SchemaFile: mydump.FileInfo{
			TableName: filter.Table{Schema: "db", Name: "table"},
			FileMeta: mydump.SourceFileMeta{
				Path: "db.table-schema.sql",
				Type: mydump.SourceTypeTableSchema,
			},
		},
		DataFiles: fakeDataFiles,
	}

	s.tableMeta2 = &mydump.MDTableMeta{
		DB:        "db",
		Name:      "table2",
		TotalSize: tblSize,
		SchemaFile: mydump.FileInfo{
			TableName: filter.Table{Schema: "db", Name: "table"},
			FileMeta: mydump.SourceFileMeta{
				Path: "db.table-schema.sql",
				Type: mydump.SourceTypeTableSchema,
			},
		},
		DataFiles: fakeDataFiles,
	}
}

func (s *tableRestoreSuiteBase) setupTest(t *testing.T) {
	// Collect into the test TableImporter structure
	var err error
	s.tr, err = NewTableImporter("`db`.`table`", s.tableMeta, s.dbInfo, s.tableInfo, &checkpoints.TableCheckpoint{}, nil, nil, nil, log.L())
	require.NoError(t, err)

	s.cfg = config.NewConfig()
	s.cfg.Mydumper.BatchSize = 111
	s.cfg.App.TableConcurrency = 2
}

type tableRestoreSuite struct {
	suite.Suite
	tableRestoreSuiteBase
}

func TestTableRestoreSuite(t *testing.T) {
	suite.Run(t, new(tableRestoreSuite))
}

func (s *tableRestoreSuite) SetupSuite() {
	s.setupSuite(s.T())
}

func (s *tableRestoreSuite) SetupTest() {
	s.setupTest(s.T())
}

func (s *tableRestoreSuite) TestPopulateChunks() {
	_ = failpoint.Enable("github.com/pingcap/tidb/lightning/pkg/importer/PopulateChunkTimestamp", "return(1234567897)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/lightning/pkg/importer/PopulateChunkTimestamp")
	}()

	cp := &checkpoints.TableCheckpoint{
		Engines: make(map[int32]*checkpoints.EngineCheckpoint),
	}

	rc := &Controller{cfg: s.cfg, ioWorkers: worker.NewPool(context.Background(), 1, "io"), store: s.store}
	err := s.tr.populateChunks(context.Background(), rc, cp)
	require.NoError(s.T(), err)
	//nolint:dupl // false positive.
	require.Equal(s.T(), map[int32]*checkpoints.EngineCheckpoint{
		-1: {
			Status: checkpoints.CheckpointStatusLoaded,
		},
		0: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[0].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[0].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 0,
						RowIDMax:     7, // 37 bytes with 3 columns can store at most 7 rows.
					},
					Timestamp: 1234567897,
				},
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[1].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[1].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 7,
						RowIDMax:     14,
					},
					Timestamp: 1234567897,
				},
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[2].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[2].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 14,
						RowIDMax:     21,
					},
					Timestamp: 1234567897,
				},
			},
		},
		1: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[3].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[3].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 21,
						RowIDMax:     28,
					},
					Timestamp: 1234567897,
				},
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[4].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[4].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 28,
						RowIDMax:     35,
					},
					Timestamp: 1234567897,
				},
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[5].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[5].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 35,
						RowIDMax:     42,
					},
					Timestamp: 1234567897,
				},
			},
		},
		2: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[6].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[6].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    14,
						PrevRowIDMax: 42,
						RowIDMax:     46,
					},
					Timestamp: 1234567897,
				},
			},
		},
	}, cp.Engines)

	// set csv header to true, this will cause check columns fail
	s.cfg.Mydumper.CSV.Header = true
	s.cfg.Mydumper.CSV.HeaderSchemaMatch = true
	s.cfg.Mydumper.StrictFormat = true
	regionSize := s.cfg.Mydumper.MaxRegionSize
	s.cfg.Mydumper.MaxRegionSize = 5
	err = s.tr.populateChunks(context.Background(), rc, cp)
	require.Error(s.T(), err)
	require.Regexp(s.T(), `.*unknown columns in header \(1,2,3\)`, err.Error())
	s.cfg.Mydumper.MaxRegionSize = regionSize
	s.cfg.Mydumper.CSV.Header = false
}

type errorLocalWriter struct{}

var _ backend.EngineWriter = (*errorLocalWriter)(nil)

func (w errorLocalWriter) AppendRows(context.Context, []string, encode.Rows) error {
	return errors.New("mock write rows failed")
}

func (w errorLocalWriter) IsSynced() bool {
	return true
}

func (w errorLocalWriter) Close(context.Context) (backend.ChunkFlushStatus, error) {
	return nil, nil
}

func (s *tableRestoreSuite) TestRestoreEngineFailed() {
	ctx := context.Background()
	ctrl := gomock.NewController(s.T())
	mockBackend := mock.NewMockBackend(ctrl)
	mockEncBuilder := mock.NewMockEncodingBuilder(ctrl)
	rc := &Controller{
		cfg:            s.cfg,
		pauser:         DeliverPauser,
		ioWorkers:      worker.NewPool(ctx, 1, "io"),
		regionWorkers:  worker.NewPool(ctx, 10, "region"),
		store:          s.store,
		engineMgr:      backend.MakeEngineManager(mockBackend),
		backend:        mockBackend,
		errorSummaries: makeErrorSummaries(log.L()),
		saveCpCh:       make(chan saveCp, 1),
		encBuilder:     mockEncBuilder,
	}
	defer close(rc.saveCpCh)
	go func() {
		for cp := range rc.saveCpCh {
			cp.waitCh <- nil
		}
	}()

	cp := &checkpoints.TableCheckpoint{
		Engines: make(map[int32]*checkpoints.EngineCheckpoint),
	}
	err := s.tr.populateChunks(ctx, rc, cp)
	require.NoError(s.T(), err)

	mockChunkFlushStatus := mock.NewMockChunkFlushStatus(ctrl)
	mockChunkFlushStatus.EXPECT().Flushed().Return(true).AnyTimes()
	mockEngineWriter := mock.NewMockEngineWriter(ctrl)
	mockEngineWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockEngineWriter.EXPECT().IsSynced().Return(true).AnyTimes()
	mockEngineWriter.EXPECT().Close(gomock.Any()).Return(mockChunkFlushStatus, nil).AnyTimes()

	tbl, err := tables.TableFromMeta(kv.NewPanickingAllocators(s.tableInfo.Core.SepAutoInc(), 0), s.tableInfo.Core)
	require.NoError(s.T(), err)
	_, indexUUID := backend.MakeUUID("`db`.`table`", -1)
	_, dataUUID := backend.MakeUUID("`db`.`table`", 0)
	realEncBuilder := tidb.NewEncodingBuilder()
	mockBackend.EXPECT().OpenEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockBackend.EXPECT().OpenEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockBackend.EXPECT().CloseEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockEncBuilder.EXPECT().NewEncoder(gomock.Any(), gomock.Any()).
		Return(realEncBuilder.NewEncoder(ctx, &encode.EncodingConfig{Table: tbl})).
		AnyTimes()
	mockEncBuilder.EXPECT().MakeEmptyRows().Return(realEncBuilder.MakeEmptyRows()).AnyTimes()
	mockBackend.EXPECT().LocalWriter(gomock.Any(), gomock.Any(), dataUUID).Return(mockEngineWriter, nil)
	mockBackend.EXPECT().LocalWriter(gomock.Any(), gomock.Any(), indexUUID).
		Return(nil, errors.New("mock open index local writer failed"))
	openedIdxEngine, err := rc.engineMgr.OpenEngine(ctx, nil, "`db`.`table`", -1)
	require.NoError(s.T(), err)

	// open the first engine meet error, should directly return the error
	_, err = s.tr.preprocessEngine(ctx, rc, openedIdxEngine, 0, cp.Engines[0])
	require.Equal(s.T(), "mock open index local writer failed", err.Error())

	localWriter := func(ctx context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
		time.Sleep(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			return nil, errors.New("mock open index local writer failed after ctx.Done")
		default:
			return mockEngineWriter, nil
		}
	}
	mockBackend.EXPECT().OpenEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockBackend.EXPECT().OpenEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockBackend.EXPECT().LocalWriter(gomock.Any(), gomock.Any(), dataUUID).Return(errorLocalWriter{}, nil).AnyTimes()
	mockBackend.EXPECT().LocalWriter(gomock.Any(), gomock.Any(), indexUUID).
		DoAndReturn(localWriter).AnyTimes()

	openedIdxEngine, err = rc.engineMgr.OpenEngine(ctx, nil, "`db`.`table`", -1)
	require.NoError(s.T(), err)

	// open engine failed after write rows failed, should return write rows error
	_, err = s.tr.preprocessEngine(ctx, rc, openedIdxEngine, 0, cp.Engines[0])
	require.Equal(s.T(), "mock write rows failed", err.Error())
}

func (s *tableRestoreSuite) TestPopulateChunksCSVHeader() {
	fakeDataDir := s.T().TempDir()

	store, err := storage.NewLocalStorage(fakeDataDir)
	require.NoError(s.T(), err)

	fakeDataFiles := make([]mydump.FileInfo, 0)

	fakeCsvContents := []string{
		// small full header
		"a,b,c\r\n1,2,3\r\n",
		// small partial header
		"b,c\r\n2,3\r\n",
		// big full header
		"a,b,c\r\n90000,80000,700000\r\n1000,2000,3000\r\n11,22,33\r\n3,4,5\r\n",
		// big full header unordered
		"c,a,b\r\n,1000,2000,3000\r\n11,22,33\r\n1000,2000,404\r\n3,4,5\r\n90000,80000,700000\r\n7999999,89999999,9999999\r\n",
		// big partial header
		"b,c\r\n2000001,30000001\r\n35231616,462424626\r\n62432,434898934\r\n",
	}
	total := 0
	for i, str := range fakeCsvContents {
		csvName := fmt.Sprintf("db.table.%02d.csv", i)
		err := os.WriteFile(filepath.Join(fakeDataDir, csvName), []byte(str), 0o644)
		require.NoError(s.T(), err)
		fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{
			TableName: filter.Table{Schema: "db", Name: "table"},
			FileMeta:  mydump.SourceFileMeta{Path: csvName, Type: mydump.SourceTypeCSV, SortKey: fmt.Sprintf("%02d", i), FileSize: int64(len(str)), RealSize: int64(len(str))},
		})
		total += len(str)
	}
	tableMeta := &mydump.MDTableMeta{
		DB:         "db",
		Name:       "table",
		TotalSize:  int64(total),
		SchemaFile: mydump.FileInfo{TableName: filter.Table{Schema: "db", Name: "table"}, FileMeta: mydump.SourceFileMeta{Path: "db.table-schema.sql", Type: mydump.SourceTypeTableSchema}},
		DataFiles:  fakeDataFiles,
	}

	_ = failpoint.Enable("github.com/pingcap/tidb/lightning/pkg/importer/PopulateChunkTimestamp", "return(1234567897)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/lightning/pkg/importer/PopulateChunkTimestamp")
	}()

	cp := &checkpoints.TableCheckpoint{
		Engines: make(map[int32]*checkpoints.EngineCheckpoint),
	}

	cfg := config.NewConfig()
	cfg.Mydumper.BatchSize = 100
	cfg.Mydumper.MaxRegionSize = 40

	cfg.Mydumper.CSV.Header = true
	cfg.Mydumper.CSV.HeaderSchemaMatch = true
	cfg.Mydumper.StrictFormat = true
	rc := &Controller{cfg: cfg, ioWorkers: worker.NewPool(context.Background(), 1, "io"), store: store}

	tr, err := NewTableImporter("`db`.`table`", tableMeta, s.dbInfo, s.tableInfo, &checkpoints.TableCheckpoint{}, nil, nil, nil, log.L())
	require.NoError(s.T(), err)
	require.NoError(s.T(), tr.populateChunks(context.Background(), rc, cp))

	require.Equal(s.T(), map[int32]*checkpoints.EngineCheckpoint{
		-1: {
			Status: checkpoints.CheckpointStatusLoaded,
		},
		0: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[0].FileMeta.Path, Offset: 0},
					FileMeta: tableMeta.DataFiles[0].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    14,
						PrevRowIDMax: 0,
						RowIDMax:     4, // 37 bytes with 3 columns can store at most 7 rows.
					},
					Timestamp: 1234567897,
				},
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[1].FileMeta.Path, Offset: 0},
					FileMeta: tableMeta.DataFiles[1].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    10,
						PrevRowIDMax: 4,
						RowIDMax:     7,
					},
					Timestamp: 1234567897,
				},
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[2].FileMeta.Path, Offset: 6},
					FileMeta:          tableMeta.DataFiles[2].FileMeta,
					ColumnPermutation: []int{0, 1, 2, -1},
					Chunk: mydump.Chunk{
						Offset:       6,
						EndOffset:    52,
						PrevRowIDMax: 7,
						RowIDMax:     20,
						Columns:      []string{"a", "b", "c"},
					},

					Timestamp: 1234567897,
				},
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[2].FileMeta.Path, Offset: 52},
					FileMeta:          tableMeta.DataFiles[2].FileMeta,
					ColumnPermutation: []int{0, 1, 2, -1},
					Chunk: mydump.Chunk{
						Offset:       52,
						EndOffset:    60,
						PrevRowIDMax: 20,
						RowIDMax:     22,
						Columns:      []string{"a", "b", "c"},
					},
					Timestamp: 1234567897,
				},
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[3].FileMeta.Path, Offset: 6},
					FileMeta:          tableMeta.DataFiles[3].FileMeta,
					ColumnPermutation: []int{1, 2, 0, -1},
					Chunk: mydump.Chunk{
						Offset:       6,
						EndOffset:    48,
						PrevRowIDMax: 22,
						RowIDMax:     35,
						Columns:      []string{"c", "a", "b"},
					},
					Timestamp: 1234567897,
				},
			},
		},
		1: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[3].FileMeta.Path, Offset: 48},
					FileMeta:          tableMeta.DataFiles[3].FileMeta,
					ColumnPermutation: []int{1, 2, 0, -1},
					Chunk: mydump.Chunk{
						Offset:       48,
						EndOffset:    101,
						PrevRowIDMax: 35,
						RowIDMax:     48,
						Columns:      []string{"c", "a", "b"},
					},
					Timestamp: 1234567897,
				},
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[3].FileMeta.Path, Offset: 101},
					FileMeta:          tableMeta.DataFiles[3].FileMeta,
					ColumnPermutation: []int{1, 2, 0, -1},
					Chunk: mydump.Chunk{
						Offset:       101,
						EndOffset:    102,
						PrevRowIDMax: 48,
						RowIDMax:     48,
						Columns:      []string{"c", "a", "b"},
					},
					Timestamp: 1234567897,
				},
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[4].FileMeta.Path, Offset: 4},
					FileMeta:          tableMeta.DataFiles[4].FileMeta,
					ColumnPermutation: []int{-1, 0, 1, -1},
					Chunk: mydump.Chunk{
						Offset:       4,
						EndOffset:    59,
						PrevRowIDMax: 48,
						RowIDMax:     61,
						Columns:      []string{"b", "c"},
					},
					Timestamp: 1234567897,
				},
			},
		},
		2: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[4].FileMeta.Path, Offset: 59},
					FileMeta:          tableMeta.DataFiles[4].FileMeta,
					ColumnPermutation: []int{-1, 0, 1, -1},
					Chunk: mydump.Chunk{
						Offset:       59,
						EndOffset:    60,
						PrevRowIDMax: 61,
						RowIDMax:     61,
						Columns:      []string{"b", "c"},
					},
					Timestamp: 1234567897,
				},
			},
		},
	}, cp.Engines)
}

func (s *tableRestoreSuite) TestInitializeColumns() {
	ccp := &checkpoints.ChunkCheckpoint{}

	defer func() {
		s.tr.ignoreColumns = nil
	}()

	cases := []struct {
		columns             []string
		ignoreColumns       map[string]struct{}
		expectedPermutation []int
		errPat              string
	}{
		{
			nil,
			nil,
			[]int{0, 1, 2, -1},
			"",
		},
		{
			nil,
			map[string]struct{}{"b": {}},
			[]int{0, -1, 2, -1},
			"",
		},
		{
			[]string{"b", "c", "a"},
			nil,
			[]int{2, 0, 1, -1},
			"",
		},
		{
			[]string{"b", "c", "a"},
			map[string]struct{}{"b": {}},
			[]int{2, -1, 1, -1},
			"",
		},
		{
			[]string{"b"},
			nil,
			[]int{-1, 0, -1, -1},
			"",
		},
		{
			[]string{"_tidb_rowid", "b", "a", "c"},
			nil,
			[]int{2, 1, 3, 0},
			"",
		},
		{
			[]string{"_tidb_rowid", "b", "a", "c"},
			map[string]struct{}{"b": {}, "_tidb_rowid": {}},
			[]int{2, -1, 3, -1},
			"",
		},
		{
			[]string{"_tidb_rowid", "b", "a", "c", "d"},
			nil,
			nil,
			`\[Lightning:Restore:ErrUnknownColumns\]unknown columns in header \(d\) for table table`,
		},
		{
			[]string{"e", "b", "c", "d"},
			nil,
			nil,
			`\[Lightning:Restore:ErrUnknownColumns\]unknown columns in header \(e,d\) for table table`,
		},
	}

	for _, testCase := range cases {
		ccp.ColumnPermutation = nil
		s.tr.ignoreColumns = testCase.ignoreColumns
		err := s.tr.initializeColumns(testCase.columns, ccp)
		if len(testCase.errPat) > 0 {
			require.Error(s.T(), err)
			require.Regexp(s.T(), testCase.errPat, err.Error())
		} else {
			require.Equal(s.T(), testCase.expectedPermutation, ccp.ColumnPermutation)
		}
	}
}
func (s *tableRestoreSuite) TestInitializeColumnsGenerated() {
	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	se := tmock.NewContext()

	cases := []struct {
		schema              string
		columns             []string
		expectedPermutation []int
	}{
		{
			"CREATE TABLE `table` (a INT, b INT, C INT, d INT AS (a * 2))",
			[]string{"b", "c", "a"},
			[]int{2, 0, 1, -1, -1},
		},
		// all generated columns and none input columns
		{
			"CREATE TABLE `table` (a bigint as (1 + 2) stored, b text as (sha1(repeat('x', a))) stored)",
			[]string{},
			[]int{-1, -1, -1},
		},
	}

	for _, testCase := range cases {
		node, err := p.ParseOneStmt(testCase.schema, "", "")
		require.NoError(s.T(), err)
		core, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 0xabcdef)
		require.NoError(s.T(), err)
		core.State = model.StatePublic
		tableInfo := &checkpoints.TidbTableInfo{Name: "table", DB: "db", Core: core}
		s.tr, err = NewTableImporter("`db`.`table`", s.tableMeta, s.dbInfo, tableInfo, &checkpoints.TableCheckpoint{}, nil, nil, nil, log.L())
		require.NoError(s.T(), err)
		ccp := &checkpoints.ChunkCheckpoint{}

		err = s.tr.initializeColumns(testCase.columns, ccp)
		require.NoError(s.T(), err)
		require.Equal(s.T(), testCase.expectedPermutation, ccp.ColumnPermutation)
	}
}

func MockDoChecksumCtx(db *sql.DB) context.Context {
	ctx := context.Background()
	manager := local.NewTiDBChecksumExecutor(db)
	return context.WithValue(ctx, &checksumManagerKey, manager)
}

func (s *tableRestoreSuite) TestCompareChecksumSuccess() {
	db, mock, err := sqlmock.New()
	require.NoError(s.T(), err)
	defer func() {
		require.NoError(s.T(), db.Close())
		require.NoError(s.T(), mock.ExpectationsWereMet())
	}()

	mock.ExpectQuery("SELECT.*tikv_gc_life_time.*").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("100h0m0s").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("ADMIN CHECKSUM.*").
		WillReturnRows(
			sqlmock.NewRows([]string{"Db_name", "Table_name", "Checksum_crc64_xor", "Total_kvs", "Total_bytes"}).
				AddRow("db", "table", 1234567890, 12345, 1234567),
		)
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)
	remoteChecksum, err := DoChecksum(ctx, s.tr.tableInfo)
	require.NoError(s.T(), err)
	err = s.tr.compareChecksum(remoteChecksum, verification.MakeKVChecksum(1234567, 12345, 1234567890))
	require.NoError(s.T(), err)
}

func (s *tableRestoreSuite) TestCompareChecksumFailure() {
	db, mock, err := sqlmock.New()
	require.NoError(s.T(), err)
	defer func() {
		require.NoError(s.T(), db.Close())
		require.NoError(s.T(), mock.ExpectationsWereMet())
	}()

	mock.ExpectQuery("SELECT.*tikv_gc_life_time.*").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("100h0m0s").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("ADMIN CHECKSUM TABLE `db`\\.`table`").
		WillReturnRows(
			sqlmock.NewRows([]string{"Db_name", "Table_name", "Checksum_crc64_xor", "Total_kvs", "Total_bytes"}).
				AddRow("db", "table", 1234567890, 12345, 1234567),
		)
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()
	mock.ExpectClose()
	ctx := MockDoChecksumCtx(db)
	remoteChecksum, err := DoChecksum(ctx, s.tr.tableInfo)
	require.NoError(s.T(), err)
	err = s.tr.compareChecksum(remoteChecksum, verification.MakeKVChecksum(9876543, 54321, 1357924680))
	require.Regexp(s.T(), "checksum mismatched.*", err.Error())
}

func (s *tableRestoreSuite) TestAnalyzeTable() {
	db, mock, err := sqlmock.New()
	require.NoError(s.T(), err)
	defer func() {
		require.NoError(s.T(), db.Close())
		require.NoError(s.T(), mock.ExpectationsWereMet())
	}()

	mock.ExpectExec("ANALYZE TABLE `db`\\.`table`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose()

	ctx := context.Background()
	require.NoError(s.T(), err)
	err = s.tr.analyzeTable(ctx, db)
	require.NoError(s.T(), err)
}

func (s *tableRestoreSuite) TestImportKVSuccess() {
	controller := gomock.NewController(s.T())
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := backend.MakeEngineManager(mockBackend)
	chptCh := make(chan saveCp)
	defer close(chptCh)
	rc := &Controller{saveCpCh: chptCh, cfg: config.NewConfig()}
	go func() {
		for scp := range chptCh {
			if scp.waitCh != nil {
				scp.waitCh <- nil
			}
		}
	}()

	ctx := context.Background()
	engineUUID := uuid.New()

	mockBackend.EXPECT().
		CloseEngine(ctx, nil, engineUUID).
		Return(nil)
	mockBackend.EXPECT().
		ImportEngine(ctx, engineUUID, gomock.Any(), gomock.Any()).
		Return(nil)
	mockBackend.EXPECT().
		CleanupEngine(ctx, engineUUID).
		Return(nil)

	closedEngine, err := importer.UnsafeCloseEngineWithUUID(ctx, nil, "tag", engineUUID, 0)
	require.NoError(s.T(), err)
	err = s.tr.importKV(ctx, closedEngine, rc)
	require.NoError(s.T(), err)
}

func (s *tableRestoreSuite) TestImportKVFailure() {
	controller := gomock.NewController(s.T())
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := backend.MakeEngineManager(mockBackend)
	chptCh := make(chan saveCp)
	defer close(chptCh)
	rc := &Controller{saveCpCh: chptCh, cfg: config.NewConfig()}
	go func() {
		for scp := range chptCh {
			if scp.waitCh != nil {
				scp.waitCh <- nil
			}
		}
	}()

	ctx := context.Background()
	engineUUID := uuid.New()

	mockBackend.EXPECT().
		CloseEngine(ctx, nil, engineUUID).
		Return(nil)
	mockBackend.EXPECT().
		ImportEngine(ctx, engineUUID, gomock.Any(), gomock.Any()).
		Return(errors.Annotate(context.Canceled, "fake import error"))

	closedEngine, err := importer.UnsafeCloseEngineWithUUID(ctx, nil, "tag", engineUUID, 0)
	require.NoError(s.T(), err)
	err = s.tr.importKV(ctx, closedEngine, rc)
	require.Regexp(s.T(), "fake import error.*", err.Error())
}

func (s *tableRestoreSuite) TestTableRestoreMetrics() {
	controller := gomock.NewController(s.T())
	defer controller.Finish()

	metrics := metric.NewMetrics(promutil.NewDefaultFactory())
	chunkPendingBase := metric.ReadCounter(metrics.ChunkCounter.WithLabelValues(metric.ChunkStatePending))
	chunkFinishedBase := metric.ReadCounter(metrics.ChunkCounter.WithLabelValues(metric.ChunkStatePending))
	engineFinishedBase := metric.ReadCounter(metrics.ProcessedEngineCounter.WithLabelValues("imported", metric.TableResultSuccess))
	tableFinishedBase := metric.ReadCounter(metrics.TableCounter.WithLabelValues("index_imported", metric.TableResultSuccess))

	ctx := metric.WithMetric(context.Background(), metrics)
	chptCh := make(chan saveCp)
	defer close(chptCh)
	cfg := config.NewConfig()
	cfg.Mydumper.BatchSize = 1

	cfg.Checkpoint.Enable = false
	cfg.TiDB.Host = "127.0.0.1"
	cfg.TiDB.StatusPort = 10080
	cfg.TiDB.Port = 4000
	cfg.TiDB.PdAddr = "127.0.0.1:2379"

	cfg.Mydumper.SourceDir = "."
	cfg.Mydumper.CSV.Header = false
	cfg.TikvImporter.Backend = config.BackendTiDB
	tls, err := cfg.ToTLS()
	require.NoError(s.T(), err)

	err = cfg.Adjust(ctx)
	require.NoError(s.T(), err)

	cpDB := checkpoints.NewNullCheckpointsDB()
	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Name:   s.tableInfo.DB,
			Tables: []*mydump.MDTableMeta{s.tableMeta},
		},
	}
	ioWorkers := worker.NewPool(ctx, 5, "io")
	targetInfoGetter := &TargetInfoGetterImpl{
		cfg: cfg,
	}
	preInfoGetter := &PreImportInfoGetterImpl{
		cfg:              cfg,
		dbMetas:          dbMetas,
		targetInfoGetter: targetInfoGetter,
		srcStorage:       s.store,
		ioWorkers:        ioWorkers,
	}
	preInfoGetter.Init()
	dbInfos := map[string]*checkpoints.TidbDBInfo{
		s.tableInfo.DB: s.dbInfo,
	}
	mockChunkFlushStatus := mock.NewMockChunkFlushStatus(controller)
	mockChunkFlushStatus.EXPECT().Flushed().Return(true).AnyTimes()
	mockEngineWriter := mock.NewMockEngineWriter(controller)
	mockEngineWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockEngineWriter.EXPECT().IsSynced().Return(true).AnyTimes()
	mockEngineWriter.EXPECT().Close(gomock.Any()).Return(mockChunkFlushStatus, nil).AnyTimes()
	backendObj := mock.NewMockBackend(controller)
	backendObj.EXPECT().OpenEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	backendObj.EXPECT().CloseEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	backendObj.EXPECT().ImportEngine(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	backendObj.EXPECT().CleanupEngine(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	backendObj.EXPECT().ShouldPostProcess().Return(false).AnyTimes()
	backendObj.EXPECT().LocalWriter(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockEngineWriter, nil).AnyTimes()
	db, sqlMock, err := sqlmock.New()
	require.NoError(s.T(), err)

	rc := &Controller{
		cfg:               cfg,
		dbMetas:           dbMetas,
		dbInfos:           dbInfos,
		tableWorkers:      worker.NewPool(ctx, 6, "table"),
		ioWorkers:         ioWorkers,
		indexWorkers:      worker.NewPool(ctx, 2, "index"),
		regionWorkers:     worker.NewPool(ctx, 10, "region"),
		checksumWorks:     worker.NewPool(ctx, 2, "region"),
		saveCpCh:          chptCh,
		pauser:            DeliverPauser,
		engineMgr:         backend.MakeEngineManager(backendObj),
		backend:           backendObj,
		db:                db,
		errorSummaries:    makeErrorSummaries(log.L()),
		tls:               tls,
		checkpointsDB:     cpDB,
		closedEngineLimit: worker.NewPool(ctx, 1, "closed_engine"),
		store:             s.store,
		metaMgrBuilder:    noopMetaMgrBuilder{},
		errorMgr:          errormanager.New(nil, cfg, log.L()),
		taskMgr:           noopTaskMetaMgr{},
		preInfoGetter:     preInfoGetter,
		encBuilder:        tidb.NewEncodingBuilder(),
	}
	go func() {
		for scp := range chptCh {
			if scp.waitCh != nil {
				scp.waitCh <- nil
			}
		}
	}()

	sqlMock.ExpectQuery("SELECT tidb_version\\(\\);").WillReturnRows(sqlmock.NewRows([]string{"tidb_version()"}).
		AddRow("Release Version: v5.2.1\nEdition: Community\n"))

	web.BroadcastInitProgress(rc.dbMetas)

	err = rc.importTables(ctx)
	require.NoError(s.T(), err)

	chunkPending := metric.ReadCounter(metrics.ChunkCounter.WithLabelValues(metric.ChunkStatePending))
	chunkFinished := metric.ReadCounter(metrics.ChunkCounter.WithLabelValues(metric.ChunkStatePending))
	require.Equal(s.T(), float64(7), chunkPending-chunkPendingBase)
	require.Equal(s.T(), chunkPending-chunkPendingBase, chunkFinished-chunkFinishedBase)

	engineFinished := metric.ReadCounter(metrics.ProcessedEngineCounter.WithLabelValues("imported", metric.TableResultSuccess))
	require.Equal(s.T(), float64(8), engineFinished-engineFinishedBase)

	tableFinished := metric.ReadCounter(metrics.TableCounter.WithLabelValues("index_imported", metric.TableResultSuccess))
	require.Equal(s.T(), float64(1), tableFinished-tableFinishedBase)
}

func (s *tableRestoreSuite) TestSaveStatusCheckpoint() {
	_ = failpoint.Enable("github.com/pingcap/tidb/lightning/pkg/importer/SlowDownCheckpointUpdate", "sleep(100)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/lightning/pkg/importer/SlowDownCheckpointUpdate")
	}()

	web.BroadcastInitProgress([]*mydump.MDDatabaseMeta{{
		Name:   "test",
		Tables: []*mydump.MDTableMeta{{DB: "test", Name: "tbl"}},
	}})
	web.BroadcastTableCheckpoint(common.UniqueTable("test", "tbl"), &checkpoints.TableCheckpoint{})

	saveCpCh := make(chan saveCp)

	rc := &Controller{
		saveCpCh:      saveCpCh,
		checkpointsDB: checkpoints.NewNullCheckpointsDB(),
	}
	rc.checkpointsWg.Add(1)
	go rc.listenCheckpointUpdates(log.L())

	rc.errorSummaries = makeErrorSummaries(log.L())

	err := rc.saveStatusCheckpoint(context.Background(), common.UniqueTable("test", "tbl"), common.IndexEngineID, errors.New("connection refused"), checkpoints.CheckpointStatusImported)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, len(rc.errorSummaries.summary))

	err = rc.saveStatusCheckpoint(
		context.Background(),
		common.UniqueTable("test", "tbl"), common.IndexEngineID,
		common.ErrChecksumMismatch.GenWithStackByArgs(0, 0, 0, 0, 0, 0),
		checkpoints.CheckpointStatusImported,
	)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, len(rc.errorSummaries.summary))

	start := time.Now()
	err = rc.saveStatusCheckpoint(context.Background(), common.UniqueTable("test", "tbl"), common.IndexEngineID, nil, checkpoints.CheckpointStatusImported)
	require.NoError(s.T(), err)
	elapsed := time.Since(start)
	require.GreaterOrEqual(s.T(), elapsed, time.Millisecond*100)

	close(saveCpCh)
	rc.checkpointsWg.Wait()
}

type mockPDHTTPCli struct {
	pdhttp.Client
	storesInfo   *pdhttp.StoresInfo
	replicaCfg   map[string]any
	emptyRegions *pdhttp.RegionsInfo
}

func (c mockPDHTTPCli) GetStores(context.Context) (*pdhttp.StoresInfo, error) {
	return c.storesInfo, nil
}

func (c mockPDHTTPCli) GetReplicateConfig(context.Context) (map[string]any, error) {
	return c.replicaCfg, nil
}

func (c mockPDHTTPCli) GetEmptyRegions(context.Context) (*pdhttp.RegionsInfo, error) {
	return c.emptyRegions, nil
}

func (s *tableRestoreSuite) TestCheckClusterResource() {
	cases := []struct {
		mockStoreResponse   *pdhttp.StoresInfo
		mockReplicaResponse map[string]any
		expectMsg           string
		expectResult        bool
		expectErrorCount    int
	}{
		{
			&pdhttp.StoresInfo{
				Count: 1,
				Stores: []pdhttp.StoreInfo{
					{
						Store: pdhttp.MetaStore{
							ID: 2,
						},
						Status: pdhttp.StoreStatus{
							Available: "24",
						},
					},
				},
			},
			map[string]any{
				"max-replicas": 1.0,
			},
			"(.*)The storage space is rich(.*)",
			true,
			0,
		},
		{
			&pdhttp.StoresInfo{
				Count: 1,
				Stores: []pdhttp.StoreInfo{
					{
						Store: pdhttp.MetaStore{
							ID: 2,
						},
						Status: pdhttp.StoreStatus{
							Available: "15",
						},
					},
				},
			},
			map[string]any{
				"max-replicas": 1.0,
			},
			"(.*)Please increase storage(.*)",
			true,
			0,
		},
	}

	ctx := context.Background()
	dir := s.T().TempDir()

	file := filepath.Join(dir, "tmp")
	f, err := os.Create(file)
	require.NoError(s.T(), err)
	buf := make([]byte, 16)
	// write 16 bytes file into local storage
	for i := range buf {
		buf[i] = byte('A' + i)
	}
	_, err = f.Write(buf)
	require.NoError(s.T(), err)
	mockStore, err := storage.NewLocalStorage(dir)
	require.NoError(s.T(), err)
	for _, ca := range cases {
		template := NewSimpleTemplate()

		cfg := &config.Config{}
		cli := &mockPDHTTPCli{
			storesInfo: ca.mockStoreResponse,
			replicaCfg: ca.mockReplicaResponse,
		}
		targetInfoGetter := &TargetInfoGetterImpl{
			cfg:       cfg,
			pdHTTPCli: cli,
		}
		preInfoGetter := &PreImportInfoGetterImpl{
			cfg:              cfg,
			targetInfoGetter: targetInfoGetter,
			srcStorage:       mockStore,
		}
		theCheckBuilder := NewPrecheckItemBuilder(cfg, []*mydump.MDDatabaseMeta{}, preInfoGetter, nil, nil)
		rc := &Controller{
			cfg:                 cfg,
			store:               mockStore,
			checkTemplate:       template,
			preInfoGetter:       preInfoGetter,
			precheckItemBuilder: theCheckBuilder,
			pdHTTPCli:           cli,
		}
		var sourceSize int64
		err = rc.store.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
			sourceSize += size
			return nil
		})
		require.NoError(s.T(), err)
		preInfoGetter.estimatedSizeCache = &EstimateSourceDataSizeResult{
			SizeWithIndex:    sourceSize,
			SizeWithoutIndex: sourceSize,
		}
		err = rc.clusterResource(ctx)
		require.NoError(s.T(), err)

		require.Equal(s.T(), ca.expectErrorCount, template.FailedCount(precheck.Critical))
		require.Equal(s.T(), ca.expectResult, template.Success())
		require.Regexp(s.T(), ca.expectMsg, strings.ReplaceAll(template.Output(), "\n", ""))
	}
}

type mockTaskMetaMgr struct {
	taskMetaMgr
}

func (mockTaskMetaMgr) CheckTasksExclusively(ctx context.Context, action func(tasks []taskMeta) ([]taskMeta, error)) error {
	_, err := action([]taskMeta{{
		taskID: 1,
		pdCfgs: "",
		status: taskMetaStatusInitial,
		state:  taskStateNormal,
	}})
	return err
}

type mockPDClient struct {
	pd.Client
	leaderAddr string
}

func (m *mockPDClient) GetClusterID(_ context.Context) uint64 {
	return 1
}

func (m *mockPDClient) GetLeaderAddr() string {
	return m.leaderAddr
}

func (s *tableRestoreSuite) TestCheckClusterRegion() {
	type testCase struct {
		stores         pdhttp.StoresInfo
		emptyRegions   pdhttp.RegionsInfo
		expectMsgs     []string
		expectErrorCnt int
	}

	makeRegions := func(regionCnt int, storeID int64) []pdhttp.RegionInfo {
		var regions []pdhttp.RegionInfo
		for i := 0; i < regionCnt; i++ {
			regions = append(regions, pdhttp.RegionInfo{Peers: []pdhttp.RegionPeer{{StoreID: storeID}}})
		}
		return regions
	}

	testCases := []testCase{
		{
			stores: pdhttp.StoresInfo{Stores: []pdhttp.StoreInfo{
				{Store: pdhttp.MetaStore{ID: 1}, Status: pdhttp.StoreStatus{RegionCount: 200}},
			}},
			emptyRegions: pdhttp.RegionsInfo{
				Regions: append([]pdhttp.RegionInfo(nil), makeRegions(100, 1)...),
			},
			expectMsgs:     []string{".*Cluster doesn't have too many empty regions.*", ".*Cluster region distribution is balanced.*"},
			expectErrorCnt: 0,
		},
		{
			stores: pdhttp.StoresInfo{Stores: []pdhttp.StoreInfo{
				{Store: pdhttp.MetaStore{ID: 1}, Status: pdhttp.StoreStatus{RegionCount: 2000}},
				{Store: pdhttp.MetaStore{ID: 2}, Status: pdhttp.StoreStatus{RegionCount: 3100}},
				{Store: pdhttp.MetaStore{ID: 3}, Status: pdhttp.StoreStatus{RegionCount: 2500}},
			}},
			emptyRegions: pdhttp.RegionsInfo{
				Regions: append(append(append([]pdhttp.RegionInfo(nil),
					makeRegions(600, 1)...),
					makeRegions(300, 2)...),
					makeRegions(1200, 3)...),
			},
			expectMsgs: []string{
				".*TiKV stores \\(3\\) contains more than 1000 empty regions respectively.*",
				".*TiKV stores \\(1\\) contains more than 500 empty regions respectively.*",
				".*Region distribution is unbalanced.*but we expect it should not be less than 0.75.*",
			},
			expectErrorCnt: 1, // empty region too large
		},
		{
			stores: pdhttp.StoresInfo{Stores: []pdhttp.StoreInfo{
				{Store: pdhttp.MetaStore{ID: 1}, Status: pdhttp.StoreStatus{RegionCount: 1200}},
				{Store: pdhttp.MetaStore{ID: 2}, Status: pdhttp.StoreStatus{RegionCount: 3000}},
				{Store: pdhttp.MetaStore{ID: 3}, Status: pdhttp.StoreStatus{RegionCount: 2500}},
			}},
			expectMsgs:     []string{".*Region distribution is unbalanced.*but we expect it must not be less than 0.5.*"},
			expectErrorCnt: 1,
		},
		{
			stores: pdhttp.StoresInfo{Stores: []pdhttp.StoreInfo{
				{Store: pdhttp.MetaStore{ID: 1}, Status: pdhttp.StoreStatus{RegionCount: 0}},
				{Store: pdhttp.MetaStore{ID: 2}, Status: pdhttp.StoreStatus{RegionCount: 2800}},
				{Store: pdhttp.MetaStore{ID: 3}, Status: pdhttp.StoreStatus{RegionCount: 2500}},
			}},
			expectMsgs:     []string{".*Region distribution is unbalanced.*but we expect it must not be less than 0.5.*"},
			expectErrorCnt: 1,
		},
	}

	for i, ca := range testCases {
		template := NewSimpleTemplate()

		cfg := &config.Config{}
		cli := &mockPDHTTPCli{storesInfo: &ca.stores, emptyRegions: &ca.emptyRegions}

		targetInfoGetter := &TargetInfoGetterImpl{
			cfg:       cfg,
			pdHTTPCli: cli,
		}
		dbMetas := []*mydump.MDDatabaseMeta{}
		preInfoGetter := &PreImportInfoGetterImpl{
			cfg:              cfg,
			targetInfoGetter: targetInfoGetter,
			dbMetas:          dbMetas,
		}
		theCheckBuilder := NewPrecheckItemBuilder(cfg, dbMetas, preInfoGetter, checkpoints.NewNullCheckpointsDB(), nil)
		rc := &Controller{
			cfg:                 cfg,
			taskMgr:             mockTaskMetaMgr{},
			checkTemplate:       template,
			preInfoGetter:       preInfoGetter,
			dbInfos:             make(map[string]*checkpoints.TidbDBInfo),
			precheckItemBuilder: theCheckBuilder,
			pdHTTPCli:           cli,
		}

		preInfoGetter.dbInfosCache = rc.dbInfos
		ctx := context.Background()
		err := rc.checkClusterRegion(ctx)
		require.NoError(s.T(), err)
		require.Equal(s.T(), ca.expectErrorCnt, template.FailedCount(precheck.Warn), fmt.Sprintf("case %d", i))

		for _, expectMsg := range ca.expectMsgs {
			require.Regexp(s.T(), expectMsg, strings.ReplaceAll(template.Output(), "\n", ""))
		}
	}
}

func (s *tableRestoreSuite) TestCheckHasLargeCSV() {
	cases := []struct {
		strictFormat    bool
		expectMsg       string
		expectResult    bool
		expectWarnCount int
		dbMetas         []*mydump.MDDatabaseMeta
	}{
		{
			true,
			"(.*)Skip the data file size check, because config.StrictFormat is true(.*)",
			true,
			0,
			nil,
		},
		{
			false,
			"(.*)Source data files size is proper(.*)",
			true,
			0,
			[]*mydump.MDDatabaseMeta{
				{
					Tables: []*mydump.MDTableMeta{
						{
							DataFiles: []mydump.FileInfo{
								{
									FileMeta: mydump.SourceFileMeta{
										FileSize: 1 * units.KiB,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			false,
			"(.*)large data file: /testPath file exists(.*)",
			true,
			1,
			[]*mydump.MDDatabaseMeta{
				{
					Tables: []*mydump.MDTableMeta{
						{
							DataFiles: []mydump.FileInfo{
								{
									FileMeta: mydump.SourceFileMeta{
										FileSize: 1 * units.TiB,
										RealSize: 1 * units.TiB,
										Path:     "/testPath",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	dir := s.T().TempDir()

	mockStore, err := storage.NewLocalStorage(dir)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, ca := range cases {
		template := NewSimpleTemplate()
		cfg := &config.Config{Mydumper: config.MydumperRuntime{StrictFormat: ca.strictFormat}}
		theCheckBuilder := NewPrecheckItemBuilder(cfg, ca.dbMetas, nil, nil, nil)
		rc := &Controller{
			cfg:                 cfg,
			checkTemplate:       template,
			store:               mockStore,
			dbMetas:             ca.dbMetas,
			precheckItemBuilder: theCheckBuilder,
		}
		rc.HasLargeCSV(ctx)
		require.Equal(s.T(), ca.expectWarnCount, template.FailedCount(precheck.Warn))
		require.Equal(s.T(), ca.expectResult, template.Success())
		require.Regexp(s.T(), ca.expectMsg, strings.ReplaceAll(template.Output(), "\n", ""))
	}
}

func (s *tableRestoreSuite) TestEstimate() {
	ctx := context.Background()
	controller := gomock.NewController(s.T())
	defer controller.Finish()
	mockEncBuilder := mock.NewMockEncodingBuilder(controller)
	idAlloc := kv.NewPanickingAllocators(s.tableInfo.Core.SepAutoInc(), 0)
	tbl, err := tables.TableFromMeta(idAlloc, s.tableInfo.Core)
	require.NoError(s.T(), err)

	mockEncBuilder.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).AnyTimes()
	mockEncBuilder.EXPECT().NewEncoder(gomock.Any(), gomock.Any()).Return(kv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: tbl,
		SessionOptions: encode.SessionOptions{
			SQLMode:        s.cfg.TiDB.SQLMode,
			Timestamp:      0,
			AutoRandomSeed: 0,
		},
		Logger: log.L(),
	}, nil)).AnyTimes()

	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Name:   "db1",
			Tables: []*mydump.MDTableMeta{s.tableMeta, s.tableMeta2},
		},
	}
	dbInfos := map[string]*checkpoints.TidbDBInfo{
		"db1": s.dbInfo,
	}
	ioWorkers := worker.NewPool(context.Background(), 1, "io")
	mockTarget := restoremock.NewTargetInfo()

	preInfoGetter := &PreImportInfoGetterImpl{
		cfg:              s.cfg,
		srcStorage:       s.store,
		encBuilder:       mockEncBuilder,
		ioWorkers:        ioWorkers,
		dbMetas:          dbMetas,
		targetInfoGetter: mockTarget,
	}
	preInfoGetter.Init()

	preInfoGetter.cfg.TikvImporter.Backend = config.BackendLocal
	preInfoGetter.dbInfosCache = dbInfos
	estimateResult, err := preInfoGetter.EstimateSourceDataSize(ctx)
	s.Require().NoError(err)

	// Because this file is small than region split size so we does not sample it.
	tikvExpected := 2 * int64(compressionRatio*float64(tblSize))
	s.Require().Equal(tikvExpected, estimateResult.SizeWithIndex)
	tiflashExpected := int64(compressionRatio * float64(tblSize) * float64(tiflashReplica1+tiflashReplica2))
	s.Require().Equal(tiflashExpected, estimateResult.TiFlashSize)

	s.tableMeta.TotalSize = int64(config.SplitRegionSize)
	tikvExpected = int64(compressionRatio * float64(config.SplitRegionSize+tblSize))
	estimateResult, err = preInfoGetter.EstimateSourceDataSize(ctx, ropts.ForceReloadCache(true))
	s.Require().NoError(err)
	s.Require().Greater(estimateResult.SizeWithIndex, tikvExpected)
	tiflashExpected = int64(compressionRatio * (float64(config.SplitRegionSize*tiflashReplica1) + float64(tblSize*tiflashReplica2)))
	s.Require().Greater(estimateResult.TiFlashSize, tiflashExpected)

	// tidb backend don't compress
	preInfoGetter.cfg.TikvImporter.Backend = config.BackendTiDB
	estimateResult, err = preInfoGetter.EstimateSourceDataSize(ctx, ropts.ForceReloadCache(true))
	s.Require().NoError(err)
	tikvExpected = int64((int(config.SplitRegionSize) + tblSize))
	s.Require().Equal(tikvExpected, estimateResult.SizeWithIndex)
	tiflashExpected = int64(config.SplitRegionSize*tiflashReplica1 + tblSize*tiflashReplica2)
	s.Require().Equal(tiflashExpected, estimateResult.TiFlashSize)
}

func (s *tableRestoreSuite) TestSchemaIsValid() {
	dir := s.T().TempDir()

	ctx := context.Background()

	case1File := "db1.table1.csv"
	mockStore, err := storage.NewLocalStorage(dir)
	require.NoError(s.T(), err)
	err = mockStore.WriteFile(ctx, case1File, []byte(`"a"`))
	require.NoError(s.T(), err)

	case2File := "db1.table2.csv"
	err = mockStore.WriteFile(ctx, case2File, []byte("\"colA\",\"colB\"\n\"a\",\"b\""))
	require.NoError(s.T(), err)

	case3File := "db1.table3.csv"
	err = mockStore.WriteFile(ctx, case3File, []byte("\"a\",\"b\""))
	require.NoError(s.T(), err)

	cases := []struct {
		ignoreColumns []*config.IgnoreColumns
		expectMsg     string
		// MsgNum == 0 means the check passed.
		MsgNum    int
		hasHeader bool
		dbInfos   map[string]*checkpoints.TidbDBInfo
		tableMeta *mydump.MDTableMeta
	}{
		// Case 1:
		// csv has one column without header.
		// tidb has the two columns but the second column doesn't have the default value.
		// we expect the check failed.
		{
			nil,
			"TiDB schema `db1`.`table1` has 2 columns,and data file has 1 columns, but column colb are missing(.*)",
			1,
			false,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table1": {
							ID:   1,
							DB:   "db1",
							Name: "table1",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colA has the default value
										Name:          model.NewCIStr("colA"),
										DefaultIsExpr: true,
									},
									{
										// colB doesn't have the default value
										Name:      model.NewCIStr("colB"),
										FieldType: types.NewFieldTypeBuilder().SetType(0).SetFlag(1).Build(),
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table1",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case1File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 2.1:
		// csv has two columns(colA, colB) with the header.
		// tidb only has one column(colB).
		// we expect the check failed.
		{
			nil,
			"TiDB schema `db1`.`table2` doesn't have column cola,(.*)use tables.ignoreColumns to ignore(.*)",
			1,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table2": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colB has the default value
										Name:          model.NewCIStr("colB"),
										DefaultIsExpr: true,
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table2",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 2.2:
		// csv has two columns(colA, colB) with the header.
		// tidb only has one column(colB).
		// we ignore colA by set config tables.IgnoreColumns
		// we expect the check success.
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db1",
					Table:   "table2",
					Columns: []string{"cola"},
				},
			},
			"",
			0,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table2": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colB has the default value
										Name:          model.NewCIStr("colB"),
										DefaultIsExpr: true,
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table2",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 2.3:
		// csv has two columns(colA, colB) with the header.
		// tidb has two columns(colB, colC).
		// we ignore colA by set config tables.IgnoreColumns
		// colC doesn't have the default value.
		// we expect the check failed.
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db1",
					Table:   "table2",
					Columns: []string{"cola"},
				},
			},
			"TiDB schema `db1`.`table2` doesn't have the default value for colc(.*)",
			1,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table2": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colB has the default value
										Name:          model.NewCIStr("colB"),
										DefaultIsExpr: true,
									},
									{
										// colC doesn't have the default value
										Name:      model.NewCIStr("colC"),
										FieldType: types.NewFieldTypeBuilder().SetType(0).SetFlag(1).Build(),
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table2",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 2.4:
		// csv has two columns(colA, colB) with the header.
		// tidb has two columns(colB, colC).
		// we ignore colB by set config tables.IgnoreColumns
		// colB doesn't have the default value.
		// we expect the check failed.
		{
			[]*config.IgnoreColumns{
				{
					TableFilter: []string{"`db1`.`table2`"},
					Columns:     []string{"colb"},
				},
			},
			"TiDB schema `db1`.`table2`'s column colb cannot be ignored(.*)",
			2,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table2": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colB doesn't have the default value
										Name:      model.NewCIStr("colB"),
										FieldType: types.NewFieldTypeBuilder().SetType(0).SetFlag(1).Build(),
									},
									{
										// colC has the default value
										Name:          model.NewCIStr("colC"),
										DefaultIsExpr: true,
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table2",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 3:
		// table3's schema file not found.
		// tidb has no table3.
		// we expect the check failed.
		{
			[]*config.IgnoreColumns{
				{
					TableFilter: []string{"`db1`.`table2`"},
					Columns:     []string{"colb"},
				},
			},
			"TiDB schema `db1`.`table3` doesn't exists(.*)",
			1,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"": {},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table3",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 4:
		// table4 has two datafiles for table. we only check the first file.
		// we expect the check success.
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db1",
					Table:   "table2",
					Columns: []string{"cola"},
				},
			},
			"",
			0,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table2": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colB has the default value
										Name:          model.NewCIStr("colB"),
										DefaultIsExpr: true,
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table2",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							// This type will make the check failed.
							// but it's the second file for table.
							// so it's unreachable so this case will success.
							Type: mydump.SourceTypeIgnore,
						},
					},
				},
			},
		},
		// Case 5:
		// table has two datafiles for table.
		// ignore column and extended column are overlapped,
		// we expect the check failed.
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db",
					Table:   "table",
					Columns: []string{"colA"},
				},
			},
			"extend column colA is also assigned in ignore-column(.*)",
			1,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db": {
					Name: "db",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										Name: model.NewCIStr("colA"),
									},
									{
										Name: model.NewCIStr("colB"),
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db",
				Name: "table",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
							ExtendData: mydump.ExtendColumnData{
								Columns: []string{"colA"},
								Values:  []string{"a"},
							},
						},
					},
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
							ExtendData: mydump.ExtendColumnData{
								Columns: []string{},
								Values:  []string{},
							},
						},
					},
				},
			},
		},
		// Case 6：
		// table has one datafile for table.
		// we expect the check failed because csv header contains extend column.
		{
			nil,
			"extend column colA is contained in table(.*)",
			1,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db": {
					Name: "db",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										Name: model.NewCIStr("colA"),
									},
									{
										Name: model.NewCIStr("colB"),
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db",
				Name: "table",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
							ExtendData: mydump.ExtendColumnData{
								Columns: []string{"colA"},
								Values:  []string{"a"},
							},
						},
					},
				},
			},
		},
		// Case 7：
		// table has one datafile for table.
		// we expect the check failed because csv data columns plus extend columns is greater than target schema's columns.
		{
			nil,
			"row count 2 adding with extend column length 1 is larger than columnCount 2 plus ignore column count 0 for(.*)",
			1,
			false,
			map[string]*checkpoints.TidbDBInfo{
				"db": {
					Name: "db",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										Name: model.NewCIStr("colA"),
									},
									{
										Name: model.NewCIStr("colB"),
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db",
				Name: "table",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case3File,
							Type:     mydump.SourceTypeCSV,
							ExtendData: mydump.ExtendColumnData{
								Columns: []string{"colA"},
								Values:  []string{"a"},
							},
						},
					},
				},
			},
		},
		// Case 8：
		// table has two datafiles for table.
		// we expect the check failed because target schema doesn't contain extend column.
		{
			nil,
			"extend column \\[colC\\] don't exist in target table(.*)",
			1,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db": {
					Name: "db",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										Name: model.NewCIStr("colA"),
									},
									{
										Name: model.NewCIStr("colB"),
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db",
				Name: "table",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
							ExtendData: mydump.ExtendColumnData{
								Columns: []string{"colC"},
								Values:  []string{"a"},
							},
						},
					},
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
							ExtendData: mydump.ExtendColumnData{
								Columns: []string{"colC"},
								Values:  []string{"b"},
							},
						},
					},
				},
			},
		},
		// Case 9：
		// table has two datafiles and extend data for table.
		// we expect the check succeed.
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db",
					Table:   "table",
					Columns: []string{"colb"},
				},
			},
			"",
			0,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db": {
					Name: "db",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										Name: model.NewCIStr("colA"),
									},
									{
										Name:          model.NewCIStr("colB"),
										DefaultIsExpr: true,
									},
									{
										Name: model.NewCIStr("colC"),
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db",
				Name: "table",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
							ExtendData: mydump.ExtendColumnData{
								Columns: []string{"colC"},
								Values:  []string{"a"},
							},
						},
					},
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
							ExtendData: mydump.ExtendColumnData{
								Columns: []string{"colC"},
								Values:  []string{"b"},
							},
						},
					},
				},
			},
		},
	}

	for i, ca := range cases {
		s.T().Logf("running testCase: #%d", i+1)
		cfg := &config.Config{
			Mydumper: config.MydumperRuntime{
				ReadBlockSize: config.ReadBlockSize,
				CSV: config.CSVConfig{
					Separator:         ",",
					Delimiter:         `"`,
					Header:            ca.hasHeader,
					HeaderSchemaMatch: true,
					NotNull:           false,
					Null:              []string{`\N`},
					EscapedBy:         `\`,
					TrimLastSep:       false,
				},
				IgnoreColumns: ca.ignoreColumns,
			},
		}
		ioWorkers := worker.NewPool(context.Background(), 1, "io")
		preInfoGetter := &PreImportInfoGetterImpl{
			cfg:        cfg,
			srcStorage: mockStore,
			ioWorkers:  ioWorkers,
		}
		ci := NewSchemaCheckItem(cfg, preInfoGetter, nil, nil).(*schemaCheckItem)
		preInfoGetter.dbInfosCache = ca.dbInfos
		msgs, err := ci.SchemaIsValid(ctx, ca.tableMeta, ca.dbInfos)
		require.NoError(s.T(), err)
		require.Len(s.T(), msgs, ca.MsgNum)
		if len(msgs) > 0 {
			require.Regexp(s.T(), ca.expectMsg, msgs[0])
		}
	}
}

func (s *tableRestoreSuite) TestGBKEncodedSchemaIsValid() {
	cfg := &config.Config{
		Mydumper: config.MydumperRuntime{
			ReadBlockSize:          config.ReadBlockSize,
			DataCharacterSet:       "gb18030",
			DataInvalidCharReplace: string(utf8.RuneError),
			CSV: config.CSVConfig{
				Separator:         "，",
				Delimiter:         `"`,
				Header:            true,
				HeaderSchemaMatch: true,
				NotNull:           false,
				Null:              []string{`\N`},
				EscapedBy:         `\`,
				TrimLastSep:       false,
			},
			IgnoreColumns: nil,
		},
	}
	charsetConvertor, err := mydump.NewCharsetConvertor(cfg.Mydumper.DataCharacterSet, cfg.Mydumper.DataInvalidCharReplace)
	require.NoError(s.T(), err)
	dir := s.T().TempDir()
	mockStore, err := storage.NewLocalStorage(dir)
	require.NoError(s.T(), err)
	csvContent, err := charsetConvertor.Encode(string([]byte("\"colA\"，\"colB\"\n\"a\"，\"b\"")))
	require.NoError(s.T(), err)
	ctx := context.Background()
	csvFile := "db1.gbk_table.csv"
	err = mockStore.WriteFile(ctx, csvFile, []byte(csvContent))
	require.NoError(s.T(), err)

	dbInfos := map[string]*checkpoints.TidbDBInfo{
		"db1": {
			Name: "db1",
			Tables: map[string]*checkpoints.TidbTableInfo{
				"gbk_table": {
					ID:   1,
					DB:   "db1",
					Name: "gbk_table",
					Core: &model.TableInfo{
						Columns: []*model.ColumnInfo{
							{
								Name:      model.NewCIStr("colA"),
								FieldType: types.NewFieldTypeBuilder().SetType(0).SetFlag(1).Build(),
							},
							{
								Name:      model.NewCIStr("colB"),
								FieldType: types.NewFieldTypeBuilder().SetType(0).SetFlag(1).Build(),
							},
						},
					},
				},
			},
		},
	}
	ioWorkers := worker.NewPool(ctx, 1, "io")
	preInfoGetter := &PreImportInfoGetterImpl{
		cfg:        cfg,
		srcStorage: mockStore,
		ioWorkers:  ioWorkers,
	}
	ci := NewSchemaCheckItem(cfg, preInfoGetter, nil, nil).(*schemaCheckItem)
	preInfoGetter.dbInfosCache = dbInfos
	msgs, err := ci.SchemaIsValid(ctx, &mydump.MDTableMeta{
		DB:   "db1",
		Name: "gbk_table",
		DataFiles: []mydump.FileInfo{
			{
				FileMeta: mydump.SourceFileMeta{
					FileSize: 1 * units.TiB,
					Path:     csvFile,
					Type:     mydump.SourceTypeCSV,
				},
			},
		},
	}, dbInfos)
	require.NoError(s.T(), err)
	require.Len(s.T(), msgs, 0)
}

func TestGetDDLStatus(t *testing.T) {
	const adminShowDDLJobQueries = "ADMIN SHOW DDL JOB QUERIES LIMIT 30"

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	// test 1
	mock.ExpectQuery(adminShowDDLJobQueries).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).
		AddRow(61, "ALTER TABLE many_tables_test.t6 ADD x timestamp DEFAULT current_timestamp").
		AddRow(60, "ALTER TABLE many_tables_test.t5 ADD x timestamp DEFAULT current_timestamp").
		AddRow(59, "ALTER TABLE many_tables_test.t4 ADD x timestamp DEFAULT current_timestamp").
		AddRow(58, "ALTER TABLE many_tables_test.t3 ADD x timestamp DEFAULT current_timestamp").
		AddRow(57, "ALTER TABLE many_tables_test.t2 ADD x timestamp DEFAULT current_timestamp").
		AddRow(56, "ALTER TABLE many_tables_test.t1 ADD x timestamp DEFAULT current_timestamp").
		AddRow(55, "CREATE TABLE IF NOT EXISTS many_tables_test.t6(i TINYINT, j INT UNIQUE KEY)").
		AddRow(54, "CREATE TABLE IF NOT EXISTS many_tables_test.t5(i TINYINT, j INT UNIQUE KEY)").
		AddRow(53, "CREATE TABLE IF NOT EXISTS many_tables_test.t4(i TINYINT, j INT UNIQUE KEY)").
		AddRow(52, "CREATE TABLE IF NOT EXISTS many_tables_test.t3(i TINYINT, j INT UNIQUE KEY)"))

	mock.ExpectQuery("ADMIN SHOW DDL JOBS 30 WHERE job_id = 61").
		WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "DB_NAME", "TABLE_NAME", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID", "ROW_COUNT", "CREATE_TIME", "START_TIME", "END_TIME", "STATE"}).
			AddRow(61, "many_tables_test", "t6", "alter table", "public", 1, 61, 123, "2022-08-02 2:51:39", "2022-08-02 2:51:39", nil, "running"))

	createTime, err := time.Parse(time.DateTime, "2022-08-02 2:51:38")
	require.NoError(t, err)
	status, err := getDDLStatus(context.Background(), db, "ALTER TABLE many_tables_test.t6 ADD x timestamp DEFAULT current_timestamp", createTime)
	require.NoError(t, err)
	require.Equal(t, model.JobStateRunning, status.state)
	require.Equal(t, int64(123), status.rowCount)

	// test 2
	// ddl query is matched, but job is created before the ddl query
	mock.ExpectQuery(adminShowDDLJobQueries).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).
		AddRow(61, "ALTER TABLE many_tables_test.t6 ADD x timestamp DEFAULT current_timestamp").
		AddRow(60, "ALTER TABLE many_tables_test.t5 ADD x timestamp DEFAULT current_timestamp").
		AddRow(59, "ALTER TABLE many_tables_test.t4 ADD x timestamp DEFAULT current_timestamp").
		AddRow(58, "ALTER TABLE many_tables_test.t3 ADD x timestamp DEFAULT current_timestamp").
		AddRow(57, "ALTER TABLE many_tables_test.t2 ADD x timestamp DEFAULT current_timestamp").
		AddRow(56, "ALTER TABLE many_tables_test.t1 ADD x timestamp DEFAULT current_timestamp").
		AddRow(55, "CREATE TABLE IF NOT EXISTS many_tables_test.t6(i TINYINT, j INT UNIQUE KEY)").
		AddRow(54, "CREATE TABLE IF NOT EXISTS many_tables_test.t5(i TINYINT, j INT UNIQUE KEY)").
		AddRow(53, "CREATE TABLE IF NOT EXISTS many_tables_test.t4(i TINYINT, j INT UNIQUE KEY)").
		AddRow(52, "CREATE TABLE IF NOT EXISTS many_tables_test.t3(i TINYINT, j INT UNIQUE KEY)"))

	mock.ExpectQuery("ADMIN SHOW DDL JOBS 30 WHERE job_id = 59").
		WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "DB_NAME", "TABLE_NAME", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID", "ROW_COUNT", "CREATE_TIME", "START_TIME", "END_TIME", "STATE"}).
			AddRow(59, "many_tables_test", "t4", "alter table", "public", 1, 59, 0, "2022-08-02 2:50:37", "2022-08-02 2:50:37", nil, "none"))

	createTime, err = time.Parse(time.DateTime, "2022-08-02 2:50:38")
	require.NoError(t, err)
	status, err = getDDLStatus(context.Background(), db, "ALTER TABLE many_tables_test.t4 ADD x timestamp DEFAULT current_timestamp", createTime)
	require.NoError(t, err)
	require.Nil(t, status)

	// test 3
	// ddl query is not matched
	mock.ExpectQuery(adminShowDDLJobQueries).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).
		AddRow(61, "ALTER TABLE many_tables_test.t6 ADD x timestamp DEFAULT current_timestamp").
		AddRow(60, "ALTER TABLE many_tables_test.t5 ADD x timestamp DEFAULT current_timestamp").
		AddRow(59, "ALTER TABLE many_tables_test.t4 ADD x timestamp DEFAULT current_timestamp").
		AddRow(58, "ALTER TABLE many_tables_test.t3 ADD x timestamp DEFAULT current_timestamp").
		AddRow(57, "ALTER TABLE many_tables_test.t2 ADD x timestamp DEFAULT current_timestamp").
		AddRow(56, "ALTER TABLE many_tables_test.t1 ADD x timestamp DEFAULT current_timestamp").
		AddRow(55, "CREATE TABLE IF NOT EXISTS many_tables_test.t6(i TINYINT, j INT UNIQUE KEY)").
		AddRow(54, "CREATE TABLE IF NOT EXISTS many_tables_test.t5(i TINYINT, j INT UNIQUE KEY)").
		AddRow(53, "CREATE TABLE IF NOT EXISTS many_tables_test.t4(i TINYINT, j INT UNIQUE KEY)").
		AddRow(52, "CREATE TABLE IF NOT EXISTS many_tables_test.t3(i TINYINT, j INT UNIQUE KEY)"))

	createTime, err = time.Parse(time.DateTime, "2022-08-03 12:35:00")
	require.NoError(t, err)
	status, err = getDDLStatus(context.Background(), db, "CREATE TABLE IF NOT EXISTS many_tables_test.t7(i TINYINT, j INT UNIQUE KEY)", createTime)
	require.NoError(t, err)
	require.Nil(t, status) // DDL does not exist

	// test 5
	// multi-schema change tests
	mock.ExpectQuery(adminShowDDLJobQueries).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).
		AddRow(59, "ALTER TABLE many_tables_test.t4 ADD y INT, ADD z INT").
		AddRow(58, "ALTER TABLE many_tables_test.t3 ADD x timestamp DEFAULT current_timestamp").
		AddRow(57, "ALTER TABLE many_tables_test.t2 ADD x timestamp DEFAULT current_timestamp").
		AddRow(56, "ALTER TABLE many_tables_test.t1 ADD x timestamp DEFAULT current_timestamp").
		AddRow(55, "CREATE TABLE IF NOT EXISTS many_tables_test.t6(i TINYINT, j INT UNIQUE KEY)").
		AddRow(54, "CREATE TABLE IF NOT EXISTS many_tables_test.t5(i TINYINT, j INT UNIQUE KEY)").
		AddRow(53, "CREATE TABLE IF NOT EXISTS many_tables_test.t4(i TINYINT, j INT UNIQUE KEY)").
		AddRow(52, "CREATE TABLE IF NOT EXISTS many_tables_test.t3(i TINYINT, j INT UNIQUE KEY)").
		AddRow(51, "CREATE TABLE IF NOT EXISTS many_tables_test.t2(i TINYINT, j INT UNIQUE KEY)").
		AddRow(50, "CREATE TABLE IF NOT EXISTS many_tables_test.t1(i TINYINT, j INT UNIQUE KEY)"))

	mock.ExpectQuery("ADMIN SHOW DDL JOBS 30 WHERE job_id = 59").WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "DB_NAME", "TABLE_NAME", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID", "ROW_COUNT", "CREATE_TIME", "START_TIME", "END_TIME", "STATE"}).
		AddRow(59, "many_tables_test", "t4", "alter table multi-schema change", "public", 1, 59, 0, "2022-08-02 2:51:39", "2022-08-02 2:51:39", nil, "running").
		AddRow(59, "many_tables_test", "t4", "add column /* subjob */", "public", 1, 59, 123, nil, nil, nil, "done").
		AddRow(59, "many_tables_test", "t4", "add column /* subjob */", "public", 1, 59, 456, nil, nil, nil, "done"))

	createTime, err = time.Parse(time.DateTime, "2022-08-02 2:50:36")
	require.NoError(t, err)
	status, err = getDDLStatus(context.Background(), db, "ALTER TABLE many_tables_test.t4 ADD y INT, ADD z INT", createTime)
	require.NoError(t, err)
	require.Equal(t, model.JobStateRunning, status.state)
	require.Equal(t, int64(123)+int64(456), status.rowCount)
}

func TestGetChunkCompressedSizeForParquet(t *testing.T) {
	dir := "./testdata/"
	fileName := "000000_0.parquet"
	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)

	dataFiles := make([]mydump.FileInfo, 0)
	dataFiles = append(dataFiles, mydump.FileInfo{
		TableName: filter.Table{Schema: "db", Name: "table"},
		FileMeta: mydump.SourceFileMeta{
			Path:        fileName,
			Type:        mydump.SourceTypeParquet,
			Compression: mydump.CompressionNone,
			SortKey:     "99",
			FileSize:    192,
		},
	})

	chunk := checkpoints.ChunkCheckpoint{
		Key:      checkpoints.ChunkCheckpointKey{Path: dataFiles[0].FileMeta.Path, Offset: 0},
		FileMeta: dataFiles[0].FileMeta,
		Chunk: mydump.Chunk{
			Offset:       0,
			EndOffset:    192,
			PrevRowIDMax: 0,
			RowIDMax:     100,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	compressedSize, err := getChunkCompressedSizeForParquet(ctx, &chunk, store)
	require.NoError(t, err)
	require.Equal(t, compressedSize, int64(192))
}
