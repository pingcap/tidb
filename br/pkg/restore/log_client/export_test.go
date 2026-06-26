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

package logclient

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/domain"
)

var FilterFilesByRegion = filterFilesByRegion

func (metaname *MetaName) Meta() Meta {
	return metaname.meta
}

func NewMetaName(meta Meta, name string) *MetaName {
	return &MetaName{meta: meta, name: name}
}

func NewMigrationBuilder(shiftStartTS, startTS, restoredTS uint64) *WithMigrationsBuilder {
	return &WithMigrationsBuilder{
		shiftStartTS: shiftStartTS,
		startTS:      startTS,
		restoredTS:   restoredTS,
	}
}

func (m *MetaWithMigrations) StoreId() int64 {
	return m.meta.StoreId
}

func (m *MetaWithMigrations) Meta() *backuppb.Metadata {
	return m.meta
}

func (m *PhysicalWithMigrations) PhysicalLength() uint64 {
	return m.physical.Item.Length
}

func (m *PhysicalWithMigrations) Physical() *backuppb.DataFileGroup {
	return m.physical.Item
}

func (rc *LogClient) TEST_saveIDMap(
	ctx context.Context,
	sr *stream.SchemasReplace,
) error {
	return rc.saveIDMap(ctx, sr)
}

func (rc *LogClient) TEST_initSchemasMap(
	ctx context.Context,
	restoreTS uint64,
) ([]*backuppb.PitrDBMap, error) {
	return rc.initSchemasMap(ctx, restoreTS)
}

// readStreamMetaByTS is used for streaming task. collect all meta file by TS, it is for test usage.
func (rc *LogFileManager) ReadStreamMeta(ctx context.Context) ([]*MetaName, error) {
	metas, err := rc.streamingMeta(ctx)
	if err != nil {
		return nil, err
	}
	r := iter.CollectAll(ctx, metas)
	if r.Err != nil {
		return nil, errors.Trace(r.Err)
	}
	return r.Item, nil
}

func TEST_NewLogClient(clusterID, startTS, restoreTS, upstreamClusterID uint64, dom *domain.Domain, se glue.Session) *LogClient {
	return &LogClient{
		dom:               dom,
		unsafeSession:     se,
		upstreamClusterID: upstreamClusterID,
		LogFileManager: &LogFileManager{
			startTS:   startTS,
			restoreTS: restoreTS,
		},
		clusterID: clusterID,
	}
}

// TEST_NewLogClientWithStorage returns a minimal LogClient whose only
// dependency is the storage. It is intended for tests that exercise
// storage-level behavior (lock acquisition, migration loading) and do
// not need the full domain / session / checkpoint wiring.
func TEST_NewLogClientWithStorage(s storage.ExternalStorage) *LogClient {
	return &LogClient{storage: s}
}

func (rc *LogClient) SetUseCheckpoint() {
	rc.useCheckpoint = true
}

func TEST_NewLogFileManager(startTS, restoreTS, shiftStartTS uint64, helper streamMetadataHelper) *LogFileManager {
	return &LogFileManager{
		startTS:      startTS,
		restoreTS:    restoreTS,
		shiftStartTS: shiftStartTS,
		helper:       helper,
	}
}

func TEST_CountReadableMetaKVFiles(files []*backuppb.DataFileInfo) int {
	return countReadableMetaKVFiles(files)
}

type FakeStreamMetadataHelper struct {
	streamMetadataHelper

	Data      []byte
	ReadGate  <-chan struct{}
	active    atomic.Int32
	maxActive atomic.Int32
}

func (helper *FakeStreamMetadataHelper) ActiveReadCount() int32 {
	return helper.active.Load()
}

func (helper *FakeStreamMetadataHelper) MaxActiveReadCount() int32 {
	return helper.maxActive.Load()
}

func (helper *FakeStreamMetadataHelper) ReadFile(
	ctx context.Context,
	path string,
	offset uint64,
	length uint64,
	rawLength uint64,
	compressionType backuppb.CompressionType,
	storage storage.ExternalStorage,
	encryptionInfo *encryptionpb.FileEncryptionInfo,
) ([]byte, error) {
	active := helper.active.Add(1)
	for {
		maxActive := helper.maxActive.Load()
		if active <= maxActive || helper.maxActive.CompareAndSwap(maxActive, active) {
			break
		}
	}
	defer helper.active.Add(-1)
	if helper.ReadGate != nil {
		<-helper.ReadGate
	}
	return helper.Data[offset : offset+length], nil
}

func (m WithMigrations) CompactionDirs() []string {
	return m.compactionDirs
}
