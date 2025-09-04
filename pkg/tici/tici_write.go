// Copyright 2025 PingCAP, Inc.
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

package tici

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// IndexEngineID is temp redefinition to avoid import cycle; will revert to common.IndexEngineID
	// after moving tici-dependent code out of infosync.
	IndexEngineID = -1
)

// GetFulltextIndexes returns all IndexInfo in the table that are fulltext indexes.
func GetFulltextIndexes(tbl *model.TableInfo) []*model.IndexInfo {
	var result []*model.IndexInfo
	for _, idx := range tbl.Indices {
		if idx.FullTextInfo != nil {
			result = append(result, idx)
		}
	}
	return result
}

// IndexMeta keep the metadata interacting with TiCI for DataWriterGroup
type IndexMeta struct {
	tidbTaskID string           // TiDB task ID for this writer
	tblInfo    *model.TableInfo // table info
	schema     string           // database name
	ticiJobID  uint64           // TiCI job ID for this writer. For better traceability.
	storeURI   string           // cloud store prefix for this writer, may also include the S3 options

}

// NewTiCIIndexMeta creates a new IndexMeta.
func NewTiCIIndexMeta(
	ctx context.Context,
	tblInfo *model.TableInfo,
	fulltextIndexIDs []int64,
	schema string,
	tidbTaskID string,
	ticiMgr *ManagerCtx,
) (*IndexMeta, error) {
	storeURI, ticiJobID, err := ticiMgr.GetCloudStoragePrefix(ctx, tidbTaskID, tblInfo.ID, fulltextIndexIDs)
	if err != nil {
		return nil, err
	}
	return &IndexMeta{
		tidbTaskID: tidbTaskID,
		tblInfo:    tblInfo,
		schema:     schema,
		ticiJobID:  ticiJobID,
		storeURI:   storeURI,
	}, nil
}

// DataWriterGroup manages a group of TiCIDataWriter, each responsible for a fulltext index in a table.
type DataWriterGroup struct {
	indexMeta  *IndexMeta
	logger     *zap.Logger // logger with table/ticiJobID fields
	writable   atomic.Bool
	mgrCtx     *ManagerCtx
	etcdClient *etcd.Client
}

// NewTiCIDataWriterGroup constructs a DataWriterGroup covering all full-text
// indexes of the given table.
//
// NOTE: The 'writable' flag is a temporary workaround. It aligns with the
// current import-into implementation and how data and index engines are
// handled. The fundamental limitation is that region jobs are created and
// executed without access to engine-level context, leaving the system unaware
// of which engine is currently being written. Addressing this would require
// significant changes to the import-into interface and should be considered
// in longer-term architectural improvements.
func NewTiCIDataWriterGroup(ctx context.Context, getEtcdClient func() (*etcd.Client, error), tblInfo *model.TableInfo, schema string, tidbTaskID string) (*DataWriterGroup, error) {
	fulltextIndexes := GetFulltextIndexes(tblInfo)
	if len(fulltextIndexes) == 0 {
		return nil, nil // No full-text indexes, no writers needed
	}

	fulltextIndexIDs := make([]int64, 0, len(fulltextIndexes))
	for _, idx := range fulltextIndexes {
		fulltextIndexIDs = append(fulltextIndexIDs, idx.ID)
	}

	logger := logutil.Logger(ctx).With(zap.String("tidbTaskID", tidbTaskID), zap.Int64("tableID", tblInfo.ID))
	logger.Info("building TiCIDataWriterGroup",
		zap.String("schema", schema),
		zap.Int64s("fulltextIndexIDs", fulltextIndexIDs),
	)

	etcdClient, err := getEtcdClient()
	if err != nil {
		return nil, err
	}
	mgrCtx, err := NewManagerCtx(ctx, etcdClient.GetClient())
	if err != nil {
		return nil, err
	}

	indexMeta, err := NewTiCIIndexMeta(ctx, tblInfo, fulltextIndexIDs, schema, tidbTaskID, mgrCtx)
	if err != nil {
		return nil, err
	}

	// add the ticiJobID to the logger for better traceability
	logger = logger.With(zap.Uint64("ticiJobID", indexMeta.ticiJobID))
	g := &DataWriterGroup{
		indexMeta:  indexMeta,
		logger:     logger,
		mgrCtx:     mgrCtx,
		etcdClient: etcdClient,
	}
	g.writable.Store(true)
	return g, nil
}

func newTiCIDataWriterGroupForTest(ctx context.Context, mgrCtx *ManagerCtx, tblInfo *model.TableInfo, schema string) *DataWriterGroup {
	fulltextIndexes := GetFulltextIndexes(tblInfo)
	if len(fulltextIndexes) == 0 {
		return nil
	}

	fulltextIndexIDs := make([]int64, 0, len(fulltextIndexes))
	for _, idx := range fulltextIndexes {
		fulltextIndexIDs = append(fulltextIndexIDs, idx.ID)
	}

	logger := logutil.Logger(ctx).With(zap.Int64("tableID", tblInfo.ID))
	logger.Info("building TiCIDataWriterGroup",
		zap.String("schema", schema),
		zap.Int64s("fulltextIndexIDs", fulltextIndexIDs),
	)

	// We ignore the error in the test setup,
	indexMeta, err := NewTiCIIndexMeta(ctx, tblInfo, fulltextIndexIDs, schema, "fakeTaskID", mgrCtx)
	if err != nil {
		return nil
	}

	g := &DataWriterGroup{
		indexMeta: indexMeta,
		logger:    logger,
		mgrCtx:    mgrCtx,
	}
	g.writable.Store(true)
	return g
}

// SetTiCIDataWriterGroupWritable sets the writable state for the TiCIDataWriterGroup.
func SetTiCIDataWriterGroupWritable(
	ctx context.Context,
	g *DataWriterGroup,
	engineUUID uuid.UUID,
	engineID int32,
) {
	if g == nil {
		// Ignore the logic if we are not dealing with full-text
		// indexes within this engine.
		return
	}

	writable := engineID != IndexEngineID
	g.writable.Store(writable)

	logger := logutil.Logger(ctx)
	if logger != nil {
		logger.Info("setting TiCIDataWriterGroup writable",
			zap.Bool("writable", writable),
			zap.String("engine UUID", engineUUID.String()),
			zap.Int32("engine ID", engineID),
		)
	}
}

// CreateFileWriter create the ticiFileWriter according to the writers in the group.
// cloudStoreURI is the S3 URI, logger is taken from DataWriters.
func (g *DataWriterGroup) CreateFileWriter(ctx context.Context) (*FileWriter, error) {
	if !g.writable.Load() {
		return nil, nil
	}
	logger := logutil.Logger(ctx)
	logger.Info("initializing TICIFileWriter for all writers in the group")
	if g.indexMeta.storeURI == "" {
		return nil, errors.New("storageURI is not set, cannot initialize TICIFileWriter")
	}
	// Generate a unique filename
	filename := uuid.New().String()

	// storage.ParseBackend parse all path components as the storage path prefix
	storeBackend, err := storage.ParseBackend(g.indexMeta.storeURI, nil)
	if err != nil {
		return nil, err
	}
	store, err := storage.NewWithDefaultOpt(ctx, storeBackend)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = logutil.Logger(ctx)
	}

	writer, err := NewTICIFileWriter(ctx, store, filename, TiCIMinUploadPartSize, logger)
	if err != nil {
		return nil, err
	}
	logger.Info("TiCI FileWriter created", zap.String("filename", filename))
	return writer, nil
}

// WriteHeader writes the header to the fileWriter according to the writers in the group.
// commitTS is the commit timestamp to include in the header.
func (g *DataWriterGroup) WriteHeader(ctx context.Context, fileWriter *FileWriter, commitTS uint64) error {
	if !g.writable.Load() {
		return nil
	}
	if fileWriter == nil {
		return errors.New("TICIFileWriter is not initialized")
	}
	if g.indexMeta.tblInfo == nil {
		return errors.New("tblInfo is nil for DataWriter")
	}
	tblJSON, err := json.Marshal(g.indexMeta.tblInfo)
	if err != nil {
		return errors.Annotate(err, "marshal TableInfo (JSON)")
	}
	err = fileWriter.WriteHeader(ctx, tblJSON, commitTS)
	if err != nil {
		return errors.Annotatef(err, "write header to TICIFileWriter failed, ticiJobID=%d", g.indexMeta.ticiJobID)
	}
	return nil
}

// WritePairs writes a batch of KV Pairs to all writers in the group.
// Logs detailed errors for each writer using the logger.
func (g *DataWriterGroup) WritePairs(ctx context.Context, fileWriter *FileWriter, pairs []*sst.Pair, count int) error {
	if !g.writable.Load() {
		return nil
	}
	if fileWriter == nil {
		return errors.New("TICIFileWriter is not initialized")
	}
	for i := range count {
		if err := fileWriter.WriteRow(ctx, pairs[i].Key, pairs[i].Value); err != nil {
			return err
		}
	}
	return nil
}

// FinishPartitionUpload runs FinishPartitionUpload for all writers.
func (g *DataWriterGroup) FinishPartitionUpload(
	ctx context.Context,
	fileWriter *FileWriter,
	lowerBound, upperBound []byte,
) error {
	if !g.writable.Load() {
		return nil
	}
	if fileWriter == nil {
		return errors.New("TICIFileWriter is not initialized")
	}
	uri := fileWriter.URI()
	if uri == "" {
		g.logger.Warn("no uri set for FinishPartitionUpload")
		return nil // or return an error if uri is required
	}

	err := g.mgrCtx.FinishPartitionUpload(ctx, g.indexMeta.tidbTaskID, lowerBound, upperBound, uri)
	if err != nil {
		g.logger.Error("failed to finish partition upload",
			zap.String("uri", uri),
			zap.String("startKey", hex.EncodeToString(lowerBound)),
			zap.String("endKey", hex.EncodeToString(upperBound)),
			zap.Error(err),
		)
	} else {
		g.logger.Info("successfully finish partition upload",
			zap.String("uri", uri),
			zap.String("startKey", hex.EncodeToString(lowerBound)),
			zap.String("endKey", hex.EncodeToString(upperBound)),
		)
	}
	return err
}

// FinishIndexUpload runs FinishIndexUpload for all writers.
func (g *DataWriterGroup) FinishIndexUpload(
	ctx context.Context,
) error {
	if !g.writable.Load() {
		return nil
	}
	if err := g.mgrCtx.FinishIndexUpload(ctx, g.indexMeta.tidbTaskID); err != nil {
		g.logger.Error("failed to finish index upload", zap.Error(err))
		return err
	}
	g.logger.Info("successfully finish index upload")
	return nil
}

// CloseFileWriters closes the TICIFileWriter.
func (g *DataWriterGroup) CloseFileWriters(ctx context.Context, fileWriter *FileWriter) error {
	if !g.writable.Load() {
		return nil
	}
	if fileWriter == nil {
		return nil
	}
	if err := fileWriter.Close(ctx); err != nil {
		return errors.Annotatef(err, "close TICIFileWriter failed")
	}
	return nil
}

// Close closes the manager context.
func (g *DataWriterGroup) Close() error {
	if !g.writable.Load() {
		return nil
	}
	if g.mgrCtx != nil {
		g.mgrCtx.Close()
	}
	if g.etcdClient != nil {
		return g.etcdClient.Close()
	}
	return nil
}
