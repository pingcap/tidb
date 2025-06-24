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

package local

import (
	"context"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// TiCI host and port for Meta Service.
const (
	defaultTiCIHost = "0.0.0.0"
	defaultTiCIPort = "50061"
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

// TiCIDataWriter handles S3 path management and upload notifications via TiCI Meta Service.
type TiCIDataWriter struct {
	tblInfo        *model.TableInfo
	idxInfo        *model.IndexInfo
	schema         string
	s3Path         string                   // stores the S3 URI for this writer
	ticiFileWriter *external.TICIFileWriter // handles writing to S3 file for this writer
}

// NewTiCIDataWriter creates a new TiCIDataWriter.
// Context is only used for logging.
func NewTiCIDataWriter(
	ctx context.Context,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	schema string,
) *TiCIDataWriter {
	logger := log.FromContext(ctx)
	logger.Info("building TiCIDataWriter",
		zap.Int64("tableID", tblInfo.ID),
		zap.String("tableName", tblInfo.Name.O),
		zap.Int64("indexID", idxInfo.ID),
		zap.String("indexName", idxInfo.Name.O),
		zap.String("schema", schema),
		zap.Any("fulltextInfo", idxInfo.FullTextInfo),
	)
	return &TiCIDataWriter{
		tblInfo: tblInfo,
		idxInfo: idxInfo,
		schema:  schema,
	}
}

// InitTICIFileWriter initializes the ticiFileWriter for this TiCIDataWriter.
// cloudStoreURI is the S3 URI, logger is optional (can be nil).
func (w *TiCIDataWriter) InitTICIFileWriter(ctx context.Context, logger *zap.Logger) error {
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

	writer, err := external.NewTICIFileWriter(ctx, store, filename, external.TiCIMinUploadPartSize, logger)
	if err != nil {
		return err
	}
	w.ticiFileWriter = writer
	return nil
}

// GetCloudStoragePath requests the S3 path for a baseline shard upload and stores it in the struct.
func (w *TiCIDataWriter) GetCloudStoragePath(
	ctx context.Context,
	lowerBound, upperBound []byte,
) (string, error) {
	logger := log.FromContext(ctx)
	ticiMgr, err := infosync.NewTiCIManager(defaultTiCIHost, defaultTiCIPort)
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
func (w *TiCIDataWriter) MarkPartitionUploadFinished(
	ctx context.Context,
	s3PathOpt ...string,
) error {
	logger := log.FromContext(ctx)
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
	ticiMgr, err := infosync.NewTiCIManager(defaultTiCIHost, defaultTiCIPort)
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
func (w *TiCIDataWriter) MarkTableUploadFinished(
	ctx context.Context,
) error {
	logger := log.FromContext(ctx)
	ticiMgr, err := infosync.NewTiCIManager(defaultTiCIHost, defaultTiCIPort)
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
func (w *TiCIDataWriter) WriteHeader(ctx context.Context, commitTS uint64) error {
	if w.ticiFileWriter == nil {
		return errors.New("TICIFileWriter is not initialized")
	}

	tblPB := infosync.ModelTableToTiCITableInfo(w.tblInfo, w.schema)
	idxPB := infosync.ModelIndexToTiCIIndexInfo(w.idxInfo, w.tblInfo)

	// Use proto.Marshal to serialize TableInfo and IndexInfo.
	tblBytes, err := proto.Marshal(tblPB)
	if err != nil {
		return errors.Annotate(err, "marshal TableInfo (json)")
	}
	idxBytes, err := proto.Marshal(idxPB)
	if err != nil {
		return errors.Annotate(err, "marshal IndexInfo (json)")
	}
	return w.ticiFileWriter.WriteHeader(ctx, tblBytes, idxBytes, commitTS)
}

// WritePairs writes a batch of KV Pairs to the S3 file using the underlying TICIFileWriter.
func (w *TiCIDataWriter) WritePairs(ctx context.Context, pairs []*sst.Pair, count int) error {
	for i := range count {
		if err := w.ticiFileWriter.WriteRow(ctx, pairs[i].Key, pairs[i].Value); err != nil {
			return err
		}
	}
	return nil
}

// CloseFileWriter closes the underlying TICIFileWriter if it is initialized.
// It logs before closing, and flushes the writer before return.
func (w *TiCIDataWriter) CloseFileWriter(ctx context.Context) error {
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

// TiCIDataWriterGroup manages a group of TiCIDataWriter, each responsible for a fulltext index in a table.
type TiCIDataWriterGroup struct {
	writers []*TiCIDataWriter
}

// WriteHeader writes the header to all writers in the group.
// commitTS is the commit timestamp to include in the header.
func (g *TiCIDataWriterGroup) WriteHeader(ctx context.Context, commitTS uint64) error {
	for _, w := range g.writers {
		if err := w.WriteHeader(ctx, commitTS); err != nil {
			return err
		}
	}
	return nil
}

// WritePairs writes a batch of KV Pairs to all writers in the group.
// Logs detailed errors for each writer using the logger.
func (g *TiCIDataWriterGroup) WritePairs(ctx context.Context, pairs []*sst.Pair, count int) error {
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

// NewTiCIDataWriterGroup creates a new TiCIDataWriterGroup for all fulltext indexes in the given table.
func NewTiCIDataWriterGroup(ctx context.Context, tblInfo *model.TableInfo, schema string) *TiCIDataWriterGroup {
	fulltextIndexes := GetFulltextIndexes(tblInfo)
	writers := make([]*TiCIDataWriter, 0, len(fulltextIndexes))

	logger := log.FromContext(ctx)
	logger.Info("building TiCIDataWriterGroup",
		zap.Int64("tableID", tblInfo.ID),
		zap.String("schema", schema),
		zap.Int("fulltextIndexCount", len(fulltextIndexes)),
	)

	for _, idx := range fulltextIndexes {
		writers = append(writers, NewTiCIDataWriter(ctx, tblInfo, idx, schema))
	}
	return &TiCIDataWriterGroup{writers: writers}
}

// InitTICIFileWriters initializes the ticiFileWriter for all writers in the group.
// cloudStoreURI is the S3 URI, logger is taken from context.
func (g *TiCIDataWriterGroup) InitTICIFileWriters(ctx context.Context) error {
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

// GetCloudStoragePath runs GetCloudStoragePath for all writers.
// Sets the s3Path for each writer, returns the first error encountered.
func (g *TiCIDataWriterGroup) GetCloudStoragePath(
	ctx context.Context,
	lowerBound, upperBound []byte,
) error {
	for _, w := range g.writers {
		_, err := w.GetCloudStoragePath(ctx, lowerBound, upperBound)
		if err != nil {
			return err
		}
	}
	return nil
}

// MarkPartitionUploadFinished runs MarkPartitionUploadFinished for all writers.
// Optionally, you can pass a slice of s3Paths to override the stored s3Path for each writer.
func (g *TiCIDataWriterGroup) MarkPartitionUploadFinished(
	ctx context.Context,
) error {
	for _, w := range g.writers {
		if err := w.MarkPartitionUploadFinished(ctx); err != nil {
			return err
		}
	}
	return nil
}

// MarkTableUploadFinished runs MarkTableUploadFinished for all writers.
func (g *TiCIDataWriterGroup) MarkTableUploadFinished(
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
func (g *TiCIDataWriterGroup) CloseFileWriters(ctx context.Context) error {
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
