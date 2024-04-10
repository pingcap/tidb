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
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/tidb"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/worker"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	tmock "github.com/pingcap/tidb/pkg/util/mock"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type chunkRestoreSuite struct {
	suite.Suite
	tableRestoreSuiteBase
	cr *chunkProcessor
}

func TestChunkRestoreSuite(t *testing.T) {
	suite.Run(t, new(chunkRestoreSuite))
}

func (s *chunkRestoreSuite) SetupSuite() {
	s.setupSuite(s.T())
}

func (s *chunkRestoreSuite) SetupTest() {
	s.setupTest(s.T())

	ctx := context.Background()
	w := worker.NewPool(ctx, 5, "io")

	chunk := checkpoints.ChunkCheckpoint{
		Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[1].FileMeta.Path, Offset: 0},
		FileMeta: s.tr.tableMeta.DataFiles[1].FileMeta,
		Chunk: mydump.Chunk{
			Offset:       0,
			EndOffset:    37,
			PrevRowIDMax: 18,
			RowIDMax:     36,
		},
	}

	var err error
	s.cr, err = newChunkProcessor(context.Background(), 1, s.cfg, &chunk, w, s.store, nil)
	require.NoError(s.T(), err)
}

func (s *chunkRestoreSuite) TearDownTest() {
	s.cr.close()
}

func (s *chunkRestoreSuite) TestDeliverLoopCancel() {
	controller := gomock.NewController(s.T())
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	mockEncBuilder := mock.NewMockEncodingBuilder(controller)
	mockEncBuilder.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).AnyTimes()

	rc := &Controller{engineMgr: backend.MakeEngineManager(mockBackend), backend: mockBackend, encBuilder: mockEncBuilder}
	ctx, cancel := context.WithCancel(context.Background())
	kvsCh := make(chan []deliveredKVs)
	go cancel()
	_, err := s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, nil, nil, rc)
	require.Equal(s.T(), context.Canceled, errors.Cause(err))
}

func (s *chunkRestoreSuite) TestDeliverLoopEmptyData() {
	ctx := context.Background()

	// Open two mock engines.

	controller := gomock.NewController(s.T())
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	mockEncBuilder := mock.NewMockEncodingBuilder(controller)
	importer := backend.MakeEngineManager(mockBackend)

	mockBackend.EXPECT().OpenEngine(ctx, gomock.Any(), gomock.Any()).Return(nil).Times(2)
	mockEncBuilder.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).AnyTimes()
	mockWriter := mock.NewMockEngineWriter(controller)
	mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().
		AppendRows(ctx, gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
	mockWriter.EXPECT().IsSynced().Return(true).AnyTimes()

	dataEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, 0)
	require.NoError(s.T(), err)
	dataWriter, err := dataEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(s.T(), err)
	indexEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, -1)
	require.NoError(s.T(), err)
	indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(s.T(), err)

	// Deliver nothing.

	cfg := &config.Config{}
	saveCpCh := make(chan saveCp, 16)
	rc := &Controller{cfg: cfg, engineMgr: backend.MakeEngineManager(mockBackend), backend: mockBackend, saveCpCh: saveCpCh, encBuilder: mockEncBuilder}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for scp := range saveCpCh {
			if scp.waitCh != nil {
				scp.waitCh <- nil
			}
		}
	}()

	kvsCh := make(chan []deliveredKVs, 1)
	kvsCh <- []deliveredKVs{}
	_, err = s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, dataWriter, indexWriter, rc)
	require.NoError(s.T(), err)
	close(saveCpCh)
	wg.Wait()
}

func (s *chunkRestoreSuite) TestDeliverLoop() {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs)
	mockCols := []string{"c1", "c2"}

	// Open two mock engines.

	controller := gomock.NewController(s.T())
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := backend.MakeEngineManager(mockBackend)
	mockEncBuilder := mock.NewMockEncodingBuilder(controller)

	mockBackend.EXPECT().OpenEngine(ctx, gomock.Any(), gomock.Any()).Return(nil).Times(2)
	// avoid return the same object at each call
	mockEncBuilder.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).Times(1)
	mockEncBuilder.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).Times(1)
	mockWriter := mock.NewMockEngineWriter(controller)
	mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().IsSynced().Return(true).AnyTimes()

	dataEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, 0)
	require.NoError(s.T(), err)
	indexEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, -1)
	require.NoError(s.T(), err)

	dataWriter, err := dataEngine.LocalWriter(ctx, &backend.LocalWriterConfig{TableName: s.tr.tableName})
	require.NoError(s.T(), err)
	indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(s.T(), err)

	// Set up the expected API calls to the data engine...

	mockWriter.EXPECT().
		AppendRows(ctx, mockCols, kv.MakeRowsFromKvPairs([]common.KvPair{
			{
				Key: []byte("txxxxxxxx_ryyyyyyyy"),
				Val: []byte("value1"),
			},
			{
				Key: []byte("txxxxxxxx_rwwwwwwww"),
				Val: []byte("value2"),
			},
		})).
		Return(nil)

	// ... and the index engine.
	//
	// Note: This test assumes data engine is written before the index engine.

	mockWriter.EXPECT().
		AppendRows(ctx, mockCols, kv.MakeRowsFromKvPairs([]common.KvPair{
			{
				Key: []byte("txxxxxxxx_izzzzzzzz"),
				Val: []byte("index1"),
			},
		})).
		Return(nil)

	// Now actually start the delivery loop.

	saveCpCh := make(chan saveCp, 2)
	go func() {
		kvsCh <- []deliveredKVs{
			{
				kvs: kv.MakeRowFromKvPairs([]common.KvPair{
					{
						Key: []byte("txxxxxxxx_ryyyyyyyy"),
						Val: []byte("value1"),
					},
					{
						Key: []byte("txxxxxxxx_rwwwwwwww"),
						Val: []byte("value2"),
					},
					{
						Key: []byte("txxxxxxxx_izzzzzzzz"),
						Val: []byte("index1"),
					},
				}),
				columns: mockCols,
				offset:  12,
				rowID:   76,
			},
		}
		kvsCh <- []deliveredKVs{{offset: 37}}
		close(kvsCh)
	}()

	cfg := &config.Config{}
	rc := &Controller{cfg: cfg, saveCpCh: saveCpCh, engineMgr: backend.MakeEngineManager(mockBackend), backend: mockBackend, encBuilder: mockEncBuilder}

	_, err = s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, dataWriter, indexWriter, rc)
	require.NoError(s.T(), err)
	require.Len(s.T(), saveCpCh, 2)
	require.Equal(s.T(), s.cr.chunk.Chunk.EndOffset, s.cr.chunk.Chunk.Offset)
	require.Equal(s.T(), int64(76), s.cr.chunk.Chunk.PrevRowIDMax)
	require.Equal(s.T(), uint64(3), s.cr.chunk.Checksum.SumKVS())
}

func (s *chunkRestoreSuite) TestEncodeLoop() {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: s.tr.encTable,
		SessionOptions: encode.SessionOptions{
			SQLMode:   s.cfg.TiDB.SQLMode,
			Timestamp: 1234567895,
		},
		Logger: log.L(),
	}, nil)
	require.NoError(s.T(), err)
	cfg := config.NewConfig()
	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	require.NoError(s.T(), err)
	require.Len(s.T(), kvsCh, 2)

	kvs := <-kvsCh
	require.Len(s.T(), kvs, 1)
	require.Equal(s.T(), int64(19), kvs[0].rowID)
	require.Equal(s.T(), int64(36), kvs[0].offset)
	require.Equal(s.T(), []string(nil), kvs[0].columns)

	kvs = <-kvsCh
	require.Equal(s.T(), 1, len(kvs))
	require.Nil(s.T(), kvs[0].kvs)
	require.Equal(s.T(), s.cr.chunk.Chunk.EndOffset, kvs[0].offset)
}

func (s *chunkRestoreSuite) TestEncodeLoopWithExtendData() {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)

	p := parser.New()
	se := tmock.NewContext()

	lastTi := s.tr.tableInfo
	defer func() {
		s.tr.tableInfo = lastTi
	}()

	node, err := p.ParseOneStmt("CREATE TABLE `t1` (`c1` varchar(5) NOT NULL, `c_table` varchar(5), `c_schema` varchar(5), `c_source` varchar(5))", "utf8mb4", "utf8mb4_bin")
	require.NoError(s.T(), err)
	tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), int64(1))
	require.NoError(s.T(), err)
	tableInfo.State = model.StatePublic

	schema := "test_1"
	tb := "t1"
	ti := &checkpoints.TidbTableInfo{
		ID:   tableInfo.ID,
		DB:   schema,
		Name: tb,
		Core: tableInfo,
	}
	s.tr.tableInfo = ti
	s.cr.chunk.FileMeta.ExtendData = mydump.ExtendColumnData{
		Columns: []string{"c_table", "c_schema", "c_source"},
		Values:  []string{"1", "1", "01"},
	}
	defer func() {
		s.cr.chunk.FileMeta.ExtendData = mydump.ExtendColumnData{}
	}()

	kvEncoder, err := kv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: s.tr.encTable,
		SessionOptions: encode.SessionOptions{
			SQLMode:   s.cfg.TiDB.SQLMode,
			Timestamp: 1234567895,
		},
		Logger: log.L(),
	}, nil)
	require.NoError(s.T(), err)
	cfg := config.NewConfig()
	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	require.NoError(s.T(), err)
	require.Len(s.T(), kvsCh, 2)

	kvs := <-kvsCh
	require.Len(s.T(), kvs, 1)
	require.Equal(s.T(), int64(19), kvs[0].rowID)
	require.Equal(s.T(), int64(36), kvs[0].offset)
	require.Equal(s.T(), []string{"c1", "c_table", "c_schema", "c_source"}, kvs[0].columns)

	kvs = <-kvsCh
	require.Equal(s.T(), 1, len(kvs))
	require.Nil(s.T(), kvs[0].kvs)
	require.Equal(s.T(), s.cr.chunk.Chunk.EndOffset, kvs[0].offset)
}

func (s *chunkRestoreSuite) TestEncodeLoopCanceled() {
	ctx, cancel := context.WithCancel(context.Background())
	kvsCh := make(chan []deliveredKVs)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: s.tr.encTable,
		SessionOptions: encode.SessionOptions{
			SQLMode:   s.cfg.TiDB.SQLMode,
			Timestamp: 1234567896,
		},
		Logger: log.L(),
	}, nil)
	require.NoError(s.T(), err)

	go cancel()
	cfg := config.NewConfig()
	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	require.Equal(s.T(), context.Canceled, errors.Cause(err))
	require.Len(s.T(), kvsCh, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopForcedError() {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: s.tr.encTable,
		SessionOptions: encode.SessionOptions{
			SQLMode:   s.cfg.TiDB.SQLMode,
			Timestamp: 1234567897,
		},
		Logger: log.L(),
	}, nil)
	require.NoError(s.T(), err)

	// close the chunk so reading it will result in the "file already closed" error.
	s.cr.parser.Close()

	cfg := config.NewConfig()
	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	require.Regexp(s.T(), `in file .*[/\\]?db\.table\.2\.sql:0 at offset 0:.*file already closed`, err.Error())
	require.Len(s.T(), kvsCh, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopDeliverLimit() {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs, 4)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: s.tr.encTable,
		SessionOptions: encode.SessionOptions{
			SQLMode:   s.cfg.TiDB.SQLMode,
			Timestamp: 1234567898,
		},
		Logger: log.L(),
	}, nil)
	require.NoError(s.T(), err)

	dir := s.T().TempDir()

	fileName := "db.limit.000.csv"
	err = os.WriteFile(filepath.Join(dir, fileName), []byte("1,2,3\r\n4,5,6\r\n7,8,9\r"), 0o644)
	require.NoError(s.T(), err)

	store, err := storage.NewLocalStorage(dir)
	require.NoError(s.T(), err)
	cfg := config.NewConfig()

	reader, err := store.Open(ctx, fileName, nil)
	require.NoError(s.T(), err)
	w := worker.NewPool(ctx, 1, "io")
	p, err := mydump.NewCSVParser(ctx, &cfg.Mydumper.CSV, reader, 111, w, false, nil)
	require.NoError(s.T(), err)
	s.cr.parser = p

	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	require.NoError(s.T(), failpoint.Enable(
		"github.com/pingcap/tidb/lightning/pkg/importer/mock-kv-size", "return(110000000)"))
	defer failpoint.Disable("github.com/pingcap/tidb/lightning/pkg/importer/mock-kv-size")
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	require.NoError(s.T(), err)

	// we have 3 kvs total. after the failpoint injected.
	// we will send one kv each time.
	count := 0
	for {
		kvs, ok := <-kvsCh
		if !ok {
			break
		}
		count++
		if count <= 3 {
			require.Len(s.T(), kvs, 1)
		}
		if count == 4 {
			// we will send empty kvs before encodeLoop exists
			// so, we can receive 4 batch and 1 is empty
			require.Len(s.T(), kvs, 1)
			require.Nil(s.T(), kvs[0].kvs)
			require.Equal(s.T(), s.cr.chunk.Chunk.EndOffset, kvs[0].offset)
			break
		}
	}
}

func (s *chunkRestoreSuite) TestEncodeLoopDeliverErrored() {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(&encode.EncodingConfig{
		Table: s.tr.encTable,
		SessionOptions: encode.SessionOptions{
			SQLMode:   s.cfg.TiDB.SQLMode,
			Timestamp: 1234567898,
		},
		Logger: log.L(),
	}, nil)
	require.NoError(s.T(), err)

	go func() {
		deliverCompleteCh <- deliverResult{
			err: errors.New("fake deliver error"),
		}
	}()
	cfg := config.NewConfig()
	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	require.Equal(s.T(), "fake deliver error", err.Error())
	require.Len(s.T(), kvsCh, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopColumnsMismatch() {
	dir := s.T().TempDir()

	fileName := "db.table.000.csv"
	err := os.WriteFile(filepath.Join(dir, fileName), []byte("1,2\r\n4,5,6,7\r\n"), 0o644)
	require.NoError(s.T(), err)

	store, err := storage.NewLocalStorage(dir)
	require.NoError(s.T(), err)

	ctx := context.Background()
	cfg := config.NewConfig()
	logger := log.L()
	errorMgr := errormanager.New(nil, cfg, logger)
	rc := &Controller{pauser: DeliverPauser, cfg: cfg, errorMgr: errorMgr}

	reader, err := store.Open(ctx, fileName, nil)
	require.NoError(s.T(), err)
	w := worker.NewPool(ctx, 5, "io")
	p, err := mydump.NewCSVParser(ctx, &cfg.Mydumper.CSV, reader, 111, w, false, nil)
	require.NoError(s.T(), err)

	err = s.cr.parser.Close()
	require.NoError(s.T(), err)
	s.cr.parser = p

	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	encodingBuilder := tidb.NewEncodingBuilder()
	kvEncoder, err := encodingBuilder.NewEncoder(
		ctx,
		&encode.EncodingConfig{
			SessionOptions: encode.SessionOptions{
				SQLMode:   s.cfg.TiDB.SQLMode,
				Timestamp: 1234567895,
			},
			Table:  s.tr.encTable,
			Logger: s.tr.logger,
		})
	require.NoError(s.T(), err)
	defer kvEncoder.Close()

	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	require.Equal(s.T(), "[Lightning:Restore:ErrEncodeKV]encode kv error in file db.table.2.sql:0 at offset 4: column count mismatch, expected 3, got 2", err.Error())
	require.Len(s.T(), kvsCh, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopIgnoreColumnsCSV() {
	cases := []struct {
		s             string
		ignoreColumns []*config.IgnoreColumns
		kvs           deliveredKVs
		header        bool
	}{
		{
			"1,2,3\r\n4,5,6\r\n",
			[]*config.IgnoreColumns{
				{
					DB:      "db",
					Table:   "table",
					Columns: []string{"a"},
				},
			},
			deliveredKVs{
				rowID:   1,
				offset:  6,
				columns: []string{"b", "c"},
			},
			false,
		},
		{
			"b,c\r\n2,3\r\n5,6\r\n",
			[]*config.IgnoreColumns{
				{
					TableFilter: []string{"db*.tab*"},
					Columns:     []string{"b"},
				},
			},
			deliveredKVs{
				rowID:   1,
				offset:  9,
				columns: []string{"c"},
			},
			true,
		},
	}

	for _, cs := range cases {
		// reset test
		s.SetupTest()
		s.testEncodeLoopIgnoreColumnsCSV(cs.s, cs.ignoreColumns, cs.kvs, cs.header)
	}
}

func (s *chunkRestoreSuite) testEncodeLoopIgnoreColumnsCSV(
	f string,
	ignoreColumns []*config.IgnoreColumns,
	deliverKV deliveredKVs,
	header bool,
) {
	dir := s.T().TempDir()

	fileName := "db.table.000.csv"
	err := os.WriteFile(filepath.Join(dir, fileName), []byte(f), 0o644)
	require.NoError(s.T(), err)

	store, err := storage.NewLocalStorage(dir)
	require.NoError(s.T(), err)

	ctx := context.Background()
	cfg := config.NewConfig()
	cfg.Mydumper.IgnoreColumns = ignoreColumns
	cfg.Mydumper.CSV.Header = header
	rc := &Controller{pauser: DeliverPauser, cfg: cfg}

	reader, err := store.Open(ctx, fileName, nil)
	require.NoError(s.T(), err)
	w := worker.NewPool(ctx, 5, "io")
	p, err := mydump.NewCSVParser(ctx, &cfg.Mydumper.CSV, reader, 111, w, cfg.Mydumper.CSV.Header, nil)
	require.NoError(s.T(), err)

	err = s.cr.parser.Close()
	require.NoError(s.T(), err)
	s.cr.parser = p

	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	encodingBuilder := tidb.NewEncodingBuilder()
	kvEncoder, err := encodingBuilder.NewEncoder(
		ctx,
		&encode.EncodingConfig{
			SessionOptions: encode.SessionOptions{
				SQLMode:   s.cfg.TiDB.SQLMode,
				Timestamp: 1234567895,
			},
			Table:  s.tr.encTable,
			Logger: s.tr.logger,
		})
	require.NoError(s.T(), err)
	defer kvEncoder.Close()

	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	require.NoError(s.T(), err)
	require.Len(s.T(), kvsCh, 2)

	kvs := <-kvsCh
	require.Len(s.T(), kvs, 2)
	require.Equal(s.T(), deliverKV.rowID, kvs[0].rowID)
	require.Equal(s.T(), deliverKV.offset, kvs[0].offset)
	require.Equal(s.T(), deliverKV.columns, kvs[0].columns)

	kvs = <-kvsCh
	require.Equal(s.T(), 1, len(kvs))
	require.Nil(s.T(), kvs[0].kvs)
	require.Equal(s.T(), s.cr.chunk.Chunk.EndOffset, kvs[0].offset)
}

type mockEncoder struct{}

func (mockEncoder) Encode(row []types.Datum, rowID int64, columnPermutation []int, offset int64) (encode.Row, error) {
	return &kv.Pairs{}, nil
}

func (mockEncoder) Close() {}

func (s *chunkRestoreSuite) TestRestore() {
	ctx := context.Background()

	controller := gomock.NewController(s.T())
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := backend.MakeEngineManager(mockBackend)
	mockEncBuilder := mock.NewMockEncodingBuilder(controller)

	mockBackend.EXPECT().OpenEngine(ctx, gomock.Any(), gomock.Any()).Return(nil).Times(2)
	// avoid return the same object at each call
	mockEncBuilder.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).Times(1)
	mockEncBuilder.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).Times(1)
	mockWriter := mock.NewMockEngineWriter(controller)
	mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).Return(mockWriter, nil).AnyTimes()
	mockEncBuilder.EXPECT().NewEncoder(gomock.Any(), gomock.Any()).Return(mockEncoder{}, nil).Times(1)
	mockWriter.EXPECT().IsSynced().Return(true).AnyTimes()
	mockWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	dataEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, 0)
	require.NoError(s.T(), err)
	indexEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, -1)
	require.NoError(s.T(), err)

	dataWriter, err := dataEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(s.T(), err)
	indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(s.T(), err)

	saveCpCh := make(chan saveCp, 16)
	err = s.cr.process(ctx, s.tr, 0, dataWriter, indexWriter, &Controller{
		cfg:        s.cfg,
		saveCpCh:   saveCpCh,
		engineMgr:  backend.MakeEngineManager(mockBackend),
		backend:    mockBackend,
		pauser:     DeliverPauser,
		encBuilder: mockEncBuilder,
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), saveCpCh, 2)
}

func TestCompressChunkRestore(t *testing.T) {
	// Produce a mock table info
	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	node, err := p.ParseOneStmt(`
	CREATE TABLE "table" (
		a INT,
		b INT,
		c INT,
		KEY (b)
	)
`, "", "")
	require.NoError(t, err)
	core, err := ddl.BuildTableInfoFromAST(node.(*ast.CreateTableStmt))
	require.NoError(t, err)
	core.State = model.StatePublic

	// Write some sample CSV dump
	fakeDataDir := t.TempDir()
	store, err := storage.NewLocalStorage(fakeDataDir)
	require.NoError(t, err)

	fakeDataFiles := make([]mydump.FileInfo, 0)

	csvName := "db.table.1.csv.gz"
	file, err := os.Create(filepath.Join(fakeDataDir, csvName))
	require.NoError(t, err)
	gzWriter := gzip.NewWriter(file)

	var totalBytes int64
	for i := 0; i < 300; i += 3 {
		n, err := gzWriter.Write([]byte(fmt.Sprintf("%d,%d,%d\r\n", i, i+1, i+2)))
		require.NoError(t, err)
		totalBytes += int64(n)
	}

	err = gzWriter.Close()
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{
		TableName: filter.Table{Schema: "db", Name: "table"},
		FileMeta: mydump.SourceFileMeta{
			Path:        csvName,
			Type:        mydump.SourceTypeCSV,
			Compression: mydump.CompressionGZ,
			SortKey:     "99",
			FileSize:    totalBytes,
		},
	})

	chunk := checkpoints.ChunkCheckpoint{
		Key:      checkpoints.ChunkCheckpointKey{Path: fakeDataFiles[0].FileMeta.Path, Offset: 0},
		FileMeta: fakeDataFiles[0].FileMeta,
		Chunk: mydump.Chunk{
			Offset:       0,
			EndOffset:    totalBytes,
			PrevRowIDMax: 0,
			RowIDMax:     100,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := worker.NewPool(ctx, 5, "io")
	cfg := config.NewConfig()
	cfg.Mydumper.BatchSize = 111
	cfg.App.TableConcurrency = 2
	cfg.Mydumper.CSV.Header = false

	cr, err := newChunkProcessor(ctx, 1, cfg, &chunk, w, store, nil)
	require.NoError(t, err)
	var (
		id, lastID int
		offset     int64 = 0
		rowID      int64 = 0
	)
	for id < 100 {
		offset, rowID = cr.parser.Pos()
		err = cr.parser.ReadRow()
		require.NoError(t, err)
		rowData := cr.parser.LastRow().Row
		require.Len(t, rowData, 3)
		lastID = id
		for i := 0; id < 100 && i < 3; i++ {
			require.Equal(t, strconv.Itoa(id), rowData[i].GetString())
			id++
		}
	}
	require.Equal(t, int64(33), rowID)

	// test read starting from compress files' middle
	chunk = checkpoints.ChunkCheckpoint{
		Key:      checkpoints.ChunkCheckpointKey{Path: fakeDataFiles[0].FileMeta.Path, Offset: offset},
		FileMeta: fakeDataFiles[0].FileMeta,
		Chunk: mydump.Chunk{
			Offset:       offset,
			EndOffset:    totalBytes,
			PrevRowIDMax: rowID,
			RowIDMax:     100,
		},
	}
	cr, err = newChunkProcessor(ctx, 1, cfg, &chunk, w, store, nil)
	require.NoError(t, err)
	for id = lastID; id < 300; {
		err = cr.parser.ReadRow()
		require.NoError(t, err)
		rowData := cr.parser.LastRow().Row
		require.Len(t, rowData, 3)
		for i := 0; id < 300 && i < 3; i++ {
			require.Equal(t, strconv.Itoa(id), rowData[i].GetString())
			id++
		}
	}
	_, rowID = cr.parser.Pos()
	require.Equal(t, int64(100), rowID)
	err = cr.parser.ReadRow()
	require.Equal(t, io.EOF, errors.Cause(err))
}

func TestGetColumnsNames(t *testing.T) {
	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	se := tmock.NewContext()
	node, err := p.ParseOneStmt(`
	CREATE TABLE "table" (
		a INT,
		b INT,
		c INT,
		KEY (b)
	)`, "", "")
	require.NoError(t, err)
	tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 0xabcdef)
	require.NoError(t, err)
	tableInfo.State = model.StatePublic

	require.Equal(t, []string{"a", "b", "c"}, getColumnNames(tableInfo, []int{0, 1, 2, -1}))
	require.Equal(t, []string{"b", "a", "c"}, getColumnNames(tableInfo, []int{1, 0, 2, -1}))
	require.Equal(t, []string{"b", "c"}, getColumnNames(tableInfo, []int{-1, 0, 1, -1}))
	require.Equal(t, []string{"a", "b"}, getColumnNames(tableInfo, []int{0, 1, -1, -1}))
	require.Equal(t, []string{"c", "a"}, getColumnNames(tableInfo, []int{1, -1, 0, -1}))
	require.Equal(t, []string{"b"}, getColumnNames(tableInfo, []int{-1, 0, -1, -1}))
	require.Equal(t, []string{"_tidb_rowid", "a", "b", "c"}, getColumnNames(tableInfo, []int{1, 2, 3, 0}))
	require.Equal(t, []string{"b", "a", "c", "_tidb_rowid"}, getColumnNames(tableInfo, []int{1, 0, 2, 3}))
	require.Equal(t, []string{"b", "_tidb_rowid", "c"}, getColumnNames(tableInfo, []int{-1, 0, 2, 1}))
	require.Equal(t, []string{"c", "_tidb_rowid", "a"}, getColumnNames(tableInfo, []int{2, -1, 0, 1}))
	require.Equal(t, []string{"_tidb_rowid", "b"}, getColumnNames(tableInfo, []int{-1, 1, -1, 0}))
}
