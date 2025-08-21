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
	"fmt"
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

// GetPrimaryIndex returns the primary key IndexInfo of the table, or nil if not found.
func GetPrimaryIndex(tbl *model.TableInfo) *model.IndexInfo {
	for _, idx := range tbl.Indices {
		if idx.Primary {
			return idx
		}
	}
	return nil
}

// DataWriter handles S3 path management and upload notifications via TiCI Meta Service.
type DataWriter struct {
	tidbTaskID string // TiDB task ID for this writer
	tblInfo    *model.TableInfo
	idxInfo    *model.IndexInfo
	schema     string
	ticiJobID  uint64      // TiCI job ID for this writer
	storeURI   string      // cloud store prefix for this writer, may also include the S3 options
	logger     *zap.Logger // logger with table/index fields
}

// NewTiCIDataWriter creates a new TiCIDataWriter.
// Context is only used for logging.
func NewTiCIDataWriter(
	ctx context.Context,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	schema string,
	tidbTaskID string,
	ticiMgr *ManagerCtx,
) (*DataWriter, error) {
	baseLogger := logutil.Logger(ctx)
	logger := baseLogger.With(
		zap.Int64("tableID", tblInfo.ID),
		zap.Int64("indexID", idxInfo.ID),
	)
	logger.Info("building TiCIDataWriter",
		zap.String("tableName", tblInfo.Name.O),
		zap.String("indexName", idxInfo.Name.O),
		zap.String("schema", schema),
		zap.Any("fulltextInfo", idxInfo.FullTextInfo),
	)
	storeURI, ticiJobID, err := ticiMgr.GetCloudStoragePrefix(ctx, tidbTaskID, tblInfo.ID, idxInfo.ID)
	if err != nil {
		return nil, err
	}
	return &DataWriter{
		tidbTaskID: tidbTaskID,
		tblInfo:    tblInfo,
		idxInfo:    idxInfo,
		schema:     schema,
		ticiJobID:  ticiJobID,
		storeURI:   storeURI,
		// add the ticiJobID to the logger for better traceability
		logger: logger.With(zap.Uint64("ticiJobID", ticiJobID)),
	}, nil
}

// CreateTICIFileWriter create the ticiFileWriter for this TiCIDataWriter.
// cloudStoreURI is the S3 URI, logger is optional (can be nil).
func (w *DataWriter) CreateTICIFileWriter(ctx context.Context, logger *zap.Logger) (*FileWriter, error) {
	if w.storeURI == "" {
		return nil, errors.New("storageURI is not set, cannot initialize TICIFileWriter")
	}

	// Generate a unique filename
	filename := uuid.New().String()

	// storage.ParseBackend parse all path components as the storage path prefix
	storeBackend, err := storage.ParseBackend(w.storeURI, nil)
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

	// Keep track of the S3 path actually store data
	baseURL := storage.FormatBackendURL(storeBackend) // throw away the options
	s3Path := baseURL.String() + "/" + filename
	logger.Info("TiCI cloud storage path", zap.String("s3Path", s3Path))

	// FIXME: s3Path is kind of duplicated with store + filename?
	writer, err := NewTICIFileWriter(ctx, store, filename, s3Path, TiCIMinUploadPartSize, logger)
	if err != nil {
		return nil, err
	}
	return writer, nil
}

// FinishPartitionUpload notifies TiCI Meta Service that a partition upload is finished.
// Uses the stored s3Path if not explicitly provided.
func (w *DataWriter) FinishPartitionUpload(
	ctx context.Context,
	ticiMgr *ManagerCtx,
	lowerBound, upperBound []byte,
	s3Path string,
) error {
	logger := w.logger
	if s3Path == "" {
		logger.Warn("no s3Path set for FinishPartitionUpload")
		return nil // or return an error if s3Path is required
	}
	err := ticiMgr.FinishPartitionUpload(ctx, w.tidbTaskID, lowerBound, upperBound, s3Path)
	if err != nil {
		logger.Error("failed to finish partition upload",
			zap.String("s3Path", s3Path),
			zap.String("startKey", hex.EncodeToString(lowerBound)),
			zap.String("endKey", hex.EncodeToString(upperBound)),
			zap.Error(err),
		)
	} else {
		logger.Info("successfully finish partition upload",
			zap.String("s3Path", s3Path),
			zap.String("startKey", hex.EncodeToString(lowerBound)),
			zap.String("endKey", hex.EncodeToString(upperBound)),
		)
	}
	return err
}

// FinishIndexUpload notifies TiCI Meta Service that the whole table/index upload is finished.
func (w *DataWriter) FinishIndexUpload(
	ctx context.Context,
	ticiMgr *ManagerCtx,
) error {
	logger := w.logger
	if err := ticiMgr.FinishIndexUpload(ctx, w.tidbTaskID); err != nil {
		logger.Error("failed to finish index upload", zap.Error(err))
		return err
	}
	logger.Info("successfully finish index upload")
	return nil
}

// DataWriterGroup manages a group of TiCIDataWriter, each responsible for a fulltext index in a table.
type DataWriterGroup struct {
	writers    []*DataWriter
	writable   atomic.Bool
	mgrCtx     *ManagerCtx
	etcdClient *etcd.Client
}

// WriteHeader writes the header to the fileWriter according to the writers in the group.
// commitTS is the commit timestamp to include in the header.
func (g *DataWriterGroup) WriteHeader(ctx context.Context, fileWriters []*FileWriter, commitTS uint64) error {
	if !g.writable.Load() {
		return nil
	}
	if len(fileWriters) != len(g.writers) {
		return errors.New(fmt.Sprintf("number of file writers does not match number of data writers, n_fileWriters=%d, n_writers=%d", len(fileWriters), len(g.writers)))
	}
	for idx, fileW := range fileWriters {
		if fileW == nil {
			return errors.New("TICIFileWriter is not initialized")
		}
		gW := g.writers[idx]
		if gW.tblInfo == nil {
			return errors.New("tblInfo is nil for DataWriter")
		}
		tblJSON, err := json.Marshal(gW.tblInfo)
		if err != nil {
			return errors.Annotate(err, "marshal TableInfo (JSON)")
		}
		err = fileW.WriteHeader(ctx, tblJSON, commitTS)
		if err != nil {
			return errors.Annotatef(err, "write header to TICIFileWriter failed, ticiJobID=%d", gW.ticiJobID)
		}
	}
	return nil
}

// WritePairs writes a batch of KV Pairs to all writers in the group.
// Logs detailed errors for each writer using the logger.
func (g *DataWriterGroup) WritePairs(ctx context.Context, fileWriters []*FileWriter, pairs []*sst.Pair, count int) error {
	if !g.writable.Load() {
		return nil
	}
	for _, fileW := range fileWriters {
		if fileW == nil {
			return errors.New("TICIFileWriter is not initialized")
		}
		for i := range count {
			if err := fileW.WriteRow(ctx, pairs[i].Key, pairs[i].Value); err != nil {
				return err
			}
		}
	}
	return nil
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

	logger := logutil.Logger(ctx)
	logger.Info("building TiCIDataWriterGroup",
		zap.Int64("tableID", tblInfo.ID),
		zap.String("schema", schema),
		zap.Int("fulltextIndexCount", len(fulltextIndexes)),
		zap.String("tidbTaskID", tidbTaskID),
	)

	etcdClient, err := getEtcdClient()
	if err != nil {
		return nil, err
	}
	mgrCtx, err := NewManagerCtx(ctx, etcdClient.GetClient())
	if err != nil {
		return nil, err
	}

	writers := make([]*DataWriter, 0, len(fulltextIndexes))
	for _, idx := range fulltextIndexes {
		w, err := NewTiCIDataWriter(ctx, tblInfo, idx, schema, tidbTaskID, mgrCtx)
		if err != nil {
			return nil, err
		}
		writers = append(writers, w)
	}

	g := &DataWriterGroup{
		writers:    writers,
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
	writers := make([]*DataWriter, 0, len(fulltextIndexes))

	logger := logutil.Logger(ctx)
	logger.Info("building TiCIDataWriterGroup",
		zap.Int64("tableID", tblInfo.ID),
		zap.String("schema", schema),
		zap.Int("fulltextIndexCount", len(fulltextIndexes)),
	)

	for _, idx := range fulltextIndexes {
		// We ignore the error in the test setup,
		w, err := NewTiCIDataWriter(ctx, tblInfo, idx, schema, "fakeTaskID", mgrCtx)
		if err != nil {
			// create write group fails
			return nil
		}
		writers = append(writers, w)
	}

	g := &DataWriterGroup{
		writers: writers,
		mgrCtx:  mgrCtx,
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

// CreateTICIFileWriters create the ticiFileWriter according to the writers in the group.
// cloudStoreURI is the S3 URI, logger is taken from DataWriters.
func (g *DataWriterGroup) CreateFileWriters(ctx context.Context) ([]*FileWriter, error) {
	if !g.writable.Load() {
		return nil, nil
	}
	logger := logutil.Logger(ctx)
	logger.Info("initializing TICIFileWriters for all writers in the group",
		zap.Int("numWriters", len(g.writers)))
	fileWriters := make([]*FileWriter, 0, len(g.writers))
	for _, w := range g.writers {
		fileWriter, err := w.CreateTICIFileWriter(ctx, logger)
		if err != nil {
			w.logger.Error("failed to initialize TICIFileWriter",
				zap.Error(err),
				zap.String("cloudStoreURI", w.storeURI),
				zap.Int("numWriters", len(g.writers)),
			)
			return nil, err
		}
		fileWriters = append(fileWriters, fileWriter)
	}
	return fileWriters, nil
}

// FinishPartitionUpload runs FinishPartitionUpload for all writers.
// Optionally, you can pass a slice of s3Paths to override the stored s3Path for each writer.
func (g *DataWriterGroup) FinishPartitionUpload(
	ctx context.Context,
	fileWriters []*FileWriter,
	lowerBound, upperBound []byte,
) error {
	if !g.writable.Load() {
		return nil
	}
	if len(fileWriters) != len(g.writers) {
		return errors.New(fmt.Sprintf("number of file writers does not match number of data writers, numFileWriters=%d, numWriters=%d", len(fileWriters), len(g.writers)))
	}
	for idx, w := range g.writers {
		fileW := fileWriters[idx]
		if err := w.FinishPartitionUpload(ctx, g.mgrCtx, lowerBound, upperBound, fileW.s3Path); err != nil {
			return err
		}
	}
	return nil
}

// FinishIndexUpload runs FinishIndexUpload for all writers.
func (g *DataWriterGroup) FinishIndexUpload(
	ctx context.Context,
) error {
	for _, w := range g.writers {
		if err := w.FinishIndexUpload(ctx, g.mgrCtx); err != nil {
			return err
		}
		w.logger.Info("successfully marked table upload finished for TICIDataWriter")
	}
	return nil
}

// CloseFileWriters closes the TICIFileWriter.
func (g *DataWriterGroup) CloseFileWriters(ctx context.Context, fileWriters []*FileWriter) error {
	if !g.writable.Load() {
		return nil
	}
	for _, w := range fileWriters {
		if w == nil {
			continue
		}
		if err := w.Close(ctx); err != nil {
			return errors.Annotatef(err, "close TICIFileWriter failed")
		}
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
