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

package local

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
)

func TestIngestSSTWithClosedEngine(t *testing.T) {
	dir := t.TempDir()
	opt := &pebble.Options{
		MemTableSize:             1024 * 1024,
		MaxConcurrentCompactions: 16,
		L0CompactionThreshold:    math.MaxInt32, // set to max try to disable compaction
		L0StopWritesThreshold:    math.MaxInt32, // set to max try to disable compaction
		DisableWAL:               true,
		ReadOnly:                 false,
	}
	db, err := pebble.Open(filepath.Join(dir, "test"), opt)
	require.NoError(t, err)
	tmpPath := filepath.Join(dir, "test.sst")
	err = os.Mkdir(tmpPath, 0o755)
	require.NoError(t, err)

	_, engineUUID := backend.MakeUUID("ww", 0)
	engineCtx, cancel := context.WithCancel(context.Background())
	f := &Engine{
		db:           db,
		UUID:         engineUUID,
		sstDir:       tmpPath,
		ctx:          engineCtx,
		cancel:       cancel,
		sstMetasChan: make(chan metaOrFlush, 64),
		keyAdapter:   noopKeyAdapter{},
	}
	f.sstIngester = dbSSTIngester{e: f}
	sstPath := path.Join(tmpPath, uuid.New().String()+".sst")
	file, err := os.Create(sstPath)
	require.NoError(t, err)
	w := sstable.NewWriter(file, sstable.WriterOptions{})
	for i := 0; i < 10; i++ {
		require.NoError(t, w.Add(sstable.InternalKey{
			Trailer: uint64(sstable.InternalKeyKindSet),
			UserKey: []byte(fmt.Sprintf("key%d", i)),
		}, nil))
	}
	require.NoError(t, w.Close())

	require.NoError(t, f.ingestSSTs([]*sstMeta{
		{
			path: sstPath,
		},
	}))
	require.NoError(t, f.Close())
	require.ErrorIs(t, f.ingestSSTs([]*sstMeta{
		{
			path: sstPath,
		},
	}), errorEngineClosed)
}

func TestAutoSplitSST(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/MockFlushWriter", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/MockFlushWriter"))
	}()
	var err error
	dir := os.TempDir()
	w := &Writer{
		engine: &Engine{
			sstDir:     dir,
			keyAdapter: noopKeyAdapter{},
		},
		isKVSorted:         true,
		isWriteBatchSorted: true,
	}
	w.engine.closed.Store(false)
	w.writer, err = w.createSSTWriter()
	require.Nil(t, err)
	kvs := []common.KvPair{
		{
			Key:   []byte("1"),
			Val:   []byte("val1"),
			RowID: 1,
		},
		{
			Key:   []byte("2"),
			Val:   []byte("val1"),
			RowID: 2,
		},
	}
	prevWriter := w.writer
	err = w.appendRowsSorted(kvs)
	require.Nil(t, err)
	require.True(t, prevWriter == w.writer)
	kvs = []common.KvPair{
		{
			Key:   []byte("10"),
			Val:   []byte("val10"),
			RowID: 10,
		},
		{
			Key:   []byte("11"),
			Val:   []byte("val11"),
			RowID: 11,
		},
	}
	err = w.appendRowsSorted(kvs)
	require.Nil(t, err)
	require.False(t, prevWriter == w.writer) // id leap, should flush and create
	prevWriter = w.writer
	kvs = []common.KvPair{
		{
			Key:   []byte("12"),
			Val:   []byte("val12"),
			RowID: 10,
		},
		{
			Key:   []byte("13"),
			Val:   []byte("val13"),
			RowID: 11,
		},
		{
			Key:   []byte("15"),
			Val:   []byte("val15"),
			RowID: 15,
		},
	}
	err = w.appendRowsSorted(kvs)
	require.Nil(t, err)
	require.False(t, prevWriter == w.writer) // id leap, should flush and create
	prevWriter = w.writer
	kvs = []common.KvPair{
		{
			Key:   []byte("16"),
			Val:   []byte("val16"),
			RowID: 16,
		},
		{
			Key:   []byte("17"),
			Val:   []byte("val17"),
			RowID: 17,
		},
		{
			Key:   []byte("19"),
			Val:   []byte("val19"),
			RowID: 19,
		},
		{
			Key:   []byte("20"),
			Val:   []byte("val20"),
			RowID: 20,
		},
		{
			Key:   []byte("22"),
			Val:   []byte("val22"),
			RowID: 22,
		},
		{
			Key:   []byte("23"),
			Val:   []byte("val23"),
			RowID: 22,
		},
	}
	err = w.appendRowsSorted(kvs)
	require.Nil(t, err)
	require.False(t, prevWriter == w.writer) // id leap, should flush and create
}
