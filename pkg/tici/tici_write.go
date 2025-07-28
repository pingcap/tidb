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
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// TiCI host and port for Meta Service.
const (
	defaultTiCIHost = "0.0.0.0"
	defaultTiCIPort = "50061"

	// IndexEngineID is the engine ID for index engine, referring to common.IndexEngineID.
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
	tblInfo        *model.TableInfo
	idxInfo        *model.IndexInfo
	schema         string
	s3Path         string      // stores the S3 URI for this writer
	ticiFileWriter *FileWriter // handles writing to S3 file for this writer
}

// NewTiCIDataWriter creates a new TiCIDataWriter.
// Context is only used for logging.
func NewTiCIDataWriter(
	ctx context.Context,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	schema string,
) *DataWriter {
	logger := logutil.Logger(ctx)
	logger.Info("building TiCIDataWriter",
		zap.Int64("tableID", tblInfo.ID),
		zap.String("tableName", tblInfo.Name.O),
		zap.Int64("indexID", idxInfo.ID),
		zap.String("indexName", idxInfo.Name.O),
		zap.String("schema", schema),
		zap.Any("fulltextInfo", idxInfo.FullTextInfo),
	)
	return &DataWriter{
		tblInfo: tblInfo,
		idxInfo: idxInfo,
		schema:  schema,
	}
}

// InitTICIFileWriter initializes the ticiFileWriter for this TiCIDataWriter.
// cloudStoreURI is the S3 URI, logger is optional (can be nil).
func (w *DataWriter) InitTICIFileWriter(ctx context.Context, logger *zap.Logger) error {
	cloudStoreURI := w.s3Path
	if cloudStoreURI == "" {
		return errors.New("s3Path is not set, cannot initialize TICIFileWriter")
	}

	// storage.ParseBackend parse all path components as the storage path
	// prefix, but we expect our filename to be the last component
	// of the URI path, so we need to parse it as a raw URL.
	// This allows us to handle URIs like "s3://bucket/prefix/file.txt?query=param"
	// and extract the file name ("file.txt") correctly.

	// Split the URI path to isolate the actual file name.
	u, err := storage.ParseRawURL(cloudStoreURI)
	if err != nil {
		return errors.Annotate(err, "failed to parse s3Path as URI")
	}

	segments := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(segments) < 2 {
		return errors.New("s3Path must include at least a bucket name and one file name")
	}

	// Extract the filename and reconstruct the base URI path
	filename := segments[len(segments)-1]
	u.Path = "/" + strings.Join(segments[:len(segments)-1], "/") // strip the filename
	baseURI := u.String()

	storeBackend, err := storage.ParseBackend(baseURI, nil)
	if err != nil {
		return err
	}
	store, err := storage.NewWithDefaultOpt(ctx, storeBackend)
	if err != nil {
		return err
	}
	if logger == nil {
		logger = logutil.Logger(ctx)
	}

	writer, err := NewTICIFileWriter(ctx, store, filename, TiCIMinUploadPartSize, logger)
	if err != nil {
		return err
	}
	w.ticiFileWriter = writer
	return nil
}

// FetchCloudStoragePath requests the S3 path for a baseline shard upload and stores it in the struct.
func (w *DataWriter) FetchCloudStoragePath(
	ctx context.Context,
	lowerBound, upperBound []byte,
) (string, error) {
	logger := logutil.Logger(ctx)
	ticiMgr, err := NewTiCIManager(defaultTiCIHost, defaultTiCIPort)
	if err != nil {
		logger.Error("failed to create TiCI manager",
			zap.Error(err),
			zap.Int64("tableID", w.tblInfo.ID),
			zap.String("tableName", w.tblInfo.Name.O),
			zap.Int64("indexID", w.idxInfo.ID),
			zap.String("indexName", w.idxInfo.Name.O),
			zap.Binary("startKey", lowerBound),
			zap.Binary("endKey", upperBound),
		)
		return "", err
	}
	defer func() {
		closeErr := ticiMgr.Close()
		if closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	s3Path, err := ticiMgr.GetCloudStoragePath(ctx, w.tblInfo, w.idxInfo, w.schema, lowerBound, upperBound)
	if err != nil {
		logger.Error("failed to get TiCI cloud storage path",
			zap.Error(err),
			zap.Int64("tableID", w.tblInfo.ID),
			zap.String("tableName", w.tblInfo.Name.O),
			zap.Int64("indexID", w.idxInfo.ID),
			zap.String("indexName", w.idxInfo.Name.O),
			zap.Binary("startKey", lowerBound),
			zap.Binary("endKey", upperBound),
		)
		return "", err
	}
	logger.Info("got TiCI cloud storage path",
		zap.String("s3Path", s3Path),
		zap.Int64("tableID", w.tblInfo.ID),
		zap.String("tableName", w.tblInfo.Name.O),
		zap.Int64("indexID", w.idxInfo.ID),
		zap.String("indexName", w.idxInfo.Name.O),
		zap.Binary("startKey", lowerBound),
		zap.Binary("endKey", upperBound),
	)
	w.s3Path = s3Path
	return s3Path, nil
}

// MarkPartitionUploadFinished notifies TiCI Meta Service that a partition upload is finished.
// Uses the stored s3Path if not explicitly provided.
func (w *DataWriter) MarkPartitionUploadFinished(
	ctx context.Context,
	s3PathOpt ...string,
) error {
	logger := logutil.Logger(ctx)
	s3Path := w.s3Path
	if len(s3PathOpt) > 0 && s3PathOpt[0] != "" {
		s3Path = s3PathOpt[0]
	}
	if s3Path == "" {
		logger.Warn("no s3Path set for MarkPartitionUploadFinished",
			zap.Int64("tableID", w.tblInfo.ID),
			zap.String("tableName", w.tblInfo.Name.O),
			zap.Int64("indexID", w.idxInfo.ID),
			zap.String("indexName", w.idxInfo.Name.O),
		)
		return nil // or return an error if s3Path is required
	}
	ticiMgr, err := NewTiCIManager(defaultTiCIHost, defaultTiCIPort)
	if err != nil {
		logger.Error("failed to create TiCI manager",
			zap.Error(err),
			zap.Int64("tableID", w.tblInfo.ID),
			zap.String("tableName", w.tblInfo.Name.O),
			zap.Int64("indexID", w.idxInfo.ID),
			zap.String("indexName", w.idxInfo.Name.O),
		)
		return err
	}
	defer func() {
		closeErr := ticiMgr.Close()
		if closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	err = ticiMgr.MarkPartitionUploadFinished(ctx, s3Path)
	if err != nil {
		logger.Error("failed to mark partition upload finished",
			zap.String("s3Path", s3Path),
			zap.Error(err),
			zap.Int64("tableID", w.tblInfo.ID),
			zap.String("tableName", w.tblInfo.Name.O),
			zap.Int64("indexID", w.idxInfo.ID),
			zap.String("indexName", w.idxInfo.Name.O),
		)
	} else {
		logger.Info("successfully marked partition upload finished",
			zap.String("s3Path", s3Path),
			zap.Int64("tableID", w.tblInfo.ID),
			zap.String("tableName", w.tblInfo.Name.O),
			zap.Int64("indexID", w.idxInfo.ID),
			zap.String("indexName", w.idxInfo.Name.O),
		)
	}
	return err
}

// MarkTableUploadFinished notifies TiCI Meta Service that the whole table/index upload is finished.
func (w *DataWriter) MarkTableUploadFinished(
	ctx context.Context,
) error {
	logger := logutil.Logger(ctx)
	ticiMgr, err := NewTiCIManager(defaultTiCIHost, defaultTiCIPort)
	if err != nil {
		logger.Error("failed to create TiCI manager",
			zap.Error(err),
			zap.Int64("tableID", w.tblInfo.ID),
			zap.String("tableName", w.tblInfo.Name.O),
			zap.Int64("indexID", w.idxInfo.ID),
			zap.String("indexName", w.idxInfo.Name.O),
		)
		return err
	}
	defer func() {
		closeErr := ticiMgr.Close()
		if closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	err = ticiMgr.MarkTableUploadFinished(ctx, w.tblInfo.ID, w.idxInfo.ID)
	if err != nil {
		logger.Error("failed to mark table upload finished",
			zap.Int64("tableID", w.tblInfo.ID),
			zap.String("tableName", w.tblInfo.Name.O),
			zap.Int64("indexID", w.idxInfo.ID),
			zap.String("indexName", w.idxInfo.Name.O),
			zap.Error(err),
		)
	}
	return err
}

// WriteHeader writes the header to the underlying TICIFileWriter.
// commitTS is the commit timestamp to include in the header.
func (w *DataWriter) WriteHeader(ctx context.Context, commitTS uint64) error {
	if w.ticiFileWriter == nil {
		return errors.New("TICIFileWriter is not initialized")
	}

	if w.tblInfo == nil || w.idxInfo == nil {
		return errors.New("tblInfo / idxInfo is nil")
	}

	tblPB := ModelTableToTiCITableInfo(w.tblInfo, w.schema)
	idxPB := ModelIndexToTiCIIndexInfo(w.idxInfo, w.tblInfo)
	pkIdxPB := ModelPrimaryKeyToTiCIIndexInfo(w.tblInfo)

	// Use proto.Marshal to serialize TableInfo and IndexInfo.
	tblBytes, err := proto.Marshal(tblPB)
	if err != nil {
		return errors.Annotate(err, "marshal TableInfo (proto)")
	}
	idxBytes, err := proto.Marshal(idxPB)
	if err != nil {
		return errors.Annotate(err, "marshal IndexInfo (proto)")
	}

	var pkIdxBytes []byte
	// If the primary key index is nil, we can skip writing it.
	if pkIdxPB == nil {
		pkIdxBytes = nil
	} else {
		pkIdxBytes, err = proto.Marshal(pkIdxPB)
		if err != nil {
			return errors.Annotate(err, "marshal PKIndexInfo (proto)")
		}
	}

	return w.ticiFileWriter.WriteHeader(ctx, tblBytes, idxBytes, pkIdxBytes, commitTS)
}

// WritePairs writes a batch of KV Pairs to the S3 file using the underlying TICIFileWriter.
func (w *DataWriter) WritePairs(ctx context.Context, pairs []*sst.Pair, count int) error {
	for i := range count {
		if err := w.ticiFileWriter.WriteRow(ctx, pairs[i].Key, pairs[i].Value); err != nil {
			return err
		}
	}
	return nil
}

// CloseFileWriter closes the underlying TICIFileWriter if it is initialized.
// It logs before closing, and flushes the writer before return.
func (w *DataWriter) CloseFileWriter(ctx context.Context) error {
	logger := logutil.Logger(ctx)
	if w.ticiFileWriter == nil {
		return nil
	}
	logger.Info("closing TICIFileWriter",
		zap.Int64("tableID", w.tblInfo.ID),
		zap.String("tableName", w.tblInfo.Name.O),
		zap.Int64("indexID", w.idxInfo.ID),
		zap.String("indexName", w.idxInfo.Name.O),
		zap.String("s3Path", w.s3Path),
	)
	// If there is a flush method, call it here. Otherwise, just close.
	// Example: if w.ticiFileWriter.Flush != nil { w.ticiFileWriter.Flush(ctx) }
	// But TICIFileWriter only has Close, so just call Close.
	return w.ticiFileWriter.Close(ctx)
}

// DataWriterGroup manages a group of TiCIDataWriter, each responsible for a fulltext index in a table.
type DataWriterGroup struct {
	writers  []*DataWriter
	writable atomic.Bool
}

// WriteHeader writes the header to all writers in the group.
// commitTS is the commit timestamp to include in the header.
func (g *DataWriterGroup) WriteHeader(ctx context.Context, commitTS uint64) error {
	if !g.writable.Load() {
		return nil
	}
	for _, w := range g.writers {
		if err := w.WriteHeader(ctx, commitTS); err != nil {
			return err
		}
	}
	return nil
}

// WritePairs writes a batch of KV Pairs to all writers in the group.
// Logs detailed errors for each writer using the logger.
func (g *DataWriterGroup) WritePairs(ctx context.Context, pairs []*sst.Pair, count int) error {
	if !g.writable.Load() {
		return nil
	}
	logger := logutil.Logger(ctx)
	for _, w := range g.writers {
		if err := w.WritePairs(ctx, pairs, count); err != nil {
			logger.Error("failed to write pairs to TICIDataWriter",
				zap.Error(err),
				zap.Int64("tableID", w.tblInfo.ID),
				zap.String("tableName", w.tblInfo.Name.O),
				zap.Int64("indexID", w.idxInfo.ID),
				zap.String("indexName", w.idxInfo.Name.O),
			)
			return err
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
func NewTiCIDataWriterGroup(ctx context.Context, tblInfo *model.TableInfo, schema string) *DataWriterGroup {
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
		writers = append(writers, NewTiCIDataWriter(ctx, tblInfo, idx, schema))
	}

	g := &DataWriterGroup{writers: writers}
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

// InitTICIFileWriters initializes the ticiFileWriter for all writers in the group.
// cloudStoreURI is the S3 URI, logger is taken from context.
func (g *DataWriterGroup) InitTICIFileWriters(ctx context.Context) error {
	if !g.writable.Load() {
		return nil
	}
	logger := logutil.Logger(ctx)
	for _, w := range g.writers {
		err := w.InitTICIFileWriter(ctx, logger)
		if err != nil {
			logger.Error("failed to initialize TICIFileWriter",
				zap.Error(err),
				zap.Int64("tableID", w.tblInfo.ID),
				zap.String("tableName", w.tblInfo.Name.O),
				zap.Int64("indexID", w.idxInfo.ID),
				zap.String("indexName", w.idxInfo.Name.O),
				zap.String("cloudStoreURI", w.s3Path),
			)
			return err
		}
	}
	return nil
}

// FetchCloudStoragePath runs FetchCloudStoragePath for all writers.
// Sets the s3Path for each writer, returns the first error encountered.
func (g *DataWriterGroup) FetchCloudStoragePath(
	ctx context.Context,
	lowerBound, upperBound []byte,
) error {
	if !g.writable.Load() {
		return nil
	}
	for _, w := range g.writers {
		_, err := w.FetchCloudStoragePath(ctx, lowerBound, upperBound)
		if err != nil {
			return err
		}
	}
	return nil
}

// MarkPartitionUploadFinished runs MarkPartitionUploadFinished for all writers.
// Optionally, you can pass a slice of s3Paths to override the stored s3Path for each writer.
func (g *DataWriterGroup) MarkPartitionUploadFinished(
	ctx context.Context,
) error {
	if !g.writable.Load() {
		return nil
	}
	for _, w := range g.writers {
		if err := w.MarkPartitionUploadFinished(ctx); err != nil {
			return err
		}
	}
	return nil
}

// MarkTableUploadFinished runs MarkTableUploadFinished for all writers.
func (g *DataWriterGroup) MarkTableUploadFinished(
	ctx context.Context,
) error {
	logger := logutil.Logger(ctx)
	for _, w := range g.writers {
		if err := w.MarkTableUploadFinished(ctx); err != nil {
			return err
		}
		logger.Info("successfully marked table upload finished for TICIDataWriter",
			zap.Int64("tableID", w.tblInfo.ID),
			zap.String("tableName", w.tblInfo.Name.O),
			zap.Int64("indexID", w.idxInfo.ID),
			zap.String("indexName", w.idxInfo.Name.O),
		)
	}
	return nil
}

// CloseFileWriters closes the underlying TICIFileWriter for each writer in the group.
func (g *DataWriterGroup) CloseFileWriters(ctx context.Context) error {
	if !g.writable.Load() {
		return nil
	}
	logger := logutil.Logger(ctx)
	for _, w := range g.writers {
		if err := w.CloseFileWriter(ctx); err != nil {
			logger.Error("failed to close TICIFileWriter",
				zap.Error(err),
				zap.Int64("tableID", w.tblInfo.ID),
				zap.String("tableName", w.tblInfo.Name.O),
				zap.Int64("indexID", w.idxInfo.ID),
				zap.String("indexName", w.idxInfo.Name.O),
			)
			return err
		}
		logger.Info("successfully closed TICIFileWriter in group",
			zap.Int64("tableID", w.tblInfo.ID),
			zap.String("tableName", w.tblInfo.Name.O),
			zap.Int64("indexID", w.idxInfo.ID),
			zap.String("indexName", w.idxInfo.Name.O),
			zap.String("s3Path", w.s3Path),
		)
	}
	return nil
}
