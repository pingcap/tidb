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

package restore

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/tidb"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type chunkRestoreSuite struct {
	suite.Suite
	tableRestoreSuiteBase
	cr *chunkRestore
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
	s.cr, err = newChunkRestore(context.Background(), 1, s.cfg, &chunk, w, s.store, nil)
	require.NoError(s.T(), err)
}

func (s *chunkRestoreSuite) TearDownTest() {
	s.cr.close()
}

func (s *chunkRestoreSuite) TestDeliverLoopCancel() {
	controller := gomock.NewController(s.T())
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).AnyTimes()

	rc := &Controller{backend: backend.MakeBackend(mockBackend)}
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
	importer := backend.MakeBackend(mockBackend)

	mockBackend.EXPECT().OpenEngine(ctx, gomock.Any(), gomock.Any()).Return(nil).Times(2)
	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).AnyTimes()
	mockWriter := mock.NewMockEngineWriter(controller)
	mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().
		AppendRows(ctx, gomock.Any(), gomock.Any(), gomock.Any()).
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
	rc := &Controller{cfg: cfg, backend: importer, saveCpCh: saveCpCh}

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
	importer := backend.MakeBackend(mockBackend)

	mockBackend.EXPECT().OpenEngine(ctx, gomock.Any(), gomock.Any()).Return(nil).Times(2)
	// avoid return the same object at each call
	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).Times(1)
	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).Times(1)
	mockWriter := mock.NewMockEngineWriter(controller)
	mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().IsSynced().Return(true).AnyTimes()

	dataEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, 0)
	require.NoError(s.T(), err)
	indexEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, -1)
	require.NoError(s.T(), err)

	dataWriter, err := dataEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(s.T(), err)
	indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(s.T(), err)

	// Set up the expected API calls to the data engine...

	mockWriter.EXPECT().
		AppendRows(ctx, s.tr.tableName, mockCols, kv.MakeRowsFromKvPairs([]common.KvPair{
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
		AppendRows(ctx, s.tr.tableName, mockCols, kv.MakeRowsFromKvPairs([]common.KvPair{
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
	rc := &Controller{cfg: cfg, saveCpCh: saveCpCh, backend: importer}

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
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567895,
	})
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

func (s *chunkRestoreSuite) TestEncodeLoopCanceled() {
	ctx, cancel := context.WithCancel(context.Background())
	kvsCh := make(chan []deliveredKVs)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567896,
	})
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
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567897,
	})
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
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567898,
	})
	require.NoError(s.T(), err)

	dir := s.T().TempDir()

	fileName := "db.limit.000.csv"
	err = os.WriteFile(filepath.Join(dir, fileName), []byte("1,2,3\r\n4,5,6\r\n7,8,9\r"), 0o644)
	require.NoError(s.T(), err)

	store, err := storage.NewLocalStorage(dir)
	require.NoError(s.T(), err)
	cfg := config.NewConfig()

	reader, err := store.Open(ctx, fileName)
	require.NoError(s.T(), err)
	w := worker.NewPool(ctx, 1, "io")
	p, err := mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, 111, w, false, nil)
	require.NoError(s.T(), err)
	s.cr.parser = p

	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	require.NoError(s.T(), failpoint.Enable(
		"github.com/pingcap/tidb/br/pkg/lightning/restore/mock-kv-size", "return(110000000)"))
	defer failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/restore/mock-kv-size")
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
		count += 1
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
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567898,
	})
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
	errorMgr := errormanager.New(nil, cfg)
	rc := &Controller{pauser: DeliverPauser, cfg: cfg, errorMgr: errorMgr}

	reader, err := store.Open(ctx, fileName)
	require.NoError(s.T(), err)
	w := worker.NewPool(ctx, 5, "io")
	p, err := mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, 111, w, false, nil)
	require.NoError(s.T(), err)

	err = s.cr.parser.Close()
	require.NoError(s.T(), err)
	s.cr.parser = p

	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := tidb.NewTiDBBackend(nil, config.ReplaceOnDup, errorMgr).NewEncoder(
		s.tr.encTable,
		&kv.SessionOptions{
			SQLMode:   s.cfg.TiDB.SQLMode,
			Timestamp: 1234567895,
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

	reader, err := store.Open(ctx, fileName)
	require.NoError(s.T(), err)
	w := worker.NewPool(ctx, 5, "io")
	p, err := mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, 111, w, cfg.Mydumper.CSV.Header, nil)
	require.NoError(s.T(), err)

	err = s.cr.parser.Close()
	require.NoError(s.T(), err)
	s.cr.parser = p

	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := tidb.NewTiDBBackend(nil, config.ReplaceOnDup, errormanager.New(nil, config.NewConfig())).NewEncoder(
		s.tr.encTable,
		&kv.SessionOptions{
			SQLMode:   s.cfg.TiDB.SQLMode,
			Timestamp: 1234567895,
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

func (mockEncoder) Encode(_ log.Logger, _ []types.Datum, rowID int64, _ []int, _ string, _ int64) (kv.Row, error) {
	return &kv.KvPairs{}, nil
}

func (mockEncoder) Close() {}

func (s *chunkRestoreSuite) TestRestore() {
	ctx := context.Background()

	controller := gomock.NewController(s.T())
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := backend.MakeBackend(mockBackend)

	mockBackend.EXPECT().OpenEngine(ctx, gomock.Any(), gomock.Any()).Return(nil).Times(2)
	// avoid return the same object at each call
	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).Times(1)
	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).Times(1)
	mockWriter := mock.NewMockEngineWriter(controller)
	mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).Return(mockWriter, nil).AnyTimes()
	mockBackend.EXPECT().NewEncoder(gomock.Any(), gomock.Any()).Return(mockEncoder{}, nil).Times(1)
	mockWriter.EXPECT().IsSynced().Return(true).AnyTimes()
	mockWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	dataEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, 0)
	require.NoError(s.T(), err)
	indexEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, -1)
	require.NoError(s.T(), err)

	dataWriter, err := dataEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(s.T(), err)
	indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(s.T(), err)

	saveCpCh := make(chan saveCp, 16)
	err = s.cr.restore(ctx, s.tr, 0, dataWriter, indexWriter, &Controller{
		cfg:      s.cfg,
		saveCpCh: saveCpCh,
		backend:  importer,
		pauser:   DeliverPauser,
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), saveCpCh, 2)
}
