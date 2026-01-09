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
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbconfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// GetFulltextIndexes returns all IndexInfo in the table that are fulltext indexes.
func GetFulltextIndexes(tbl *model.TableInfo) []*model.IndexInfo {
	var result []*model.IndexInfo
	for _, idx := range tbl.Indices {
		if idx.IsTiCIIndex() {
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
	mgrCtx     *ManagerCtx
	etcdClient *clientv3.Client
}

const (
	etcdDialTimeout = 5 * time.Second
)

func getEtcdClient() (cli *clientv3.Client, err error) {
	tidbCfg := tidbconfig.GetGlobalConfig()
	tls, err := util.NewTLSConfig(
		util.WithCAPath(tidbCfg.Security.ClusterSSLCA),
		util.WithCertAndKeyPath(tidbCfg.Security.ClusterSSLCert, tidbCfg.Security.ClusterSSLKey),
	)
	if err != nil {
		return nil, err
	}
	etcdEndpoints, err := util.ParseHostPortAddr(tidbCfg.Path)
	if err != nil {
		return nil, err
	}
	return clientv3.New(clientv3.Config{
		Endpoints:        etcdEndpoints,
		DialTimeout:      etcdDialTimeout,
		TLS:              tls,
		AutoSyncInterval: 30 * time.Second,
	})
}

// NewTiCIDataWriterGroup constructs a DataWriterGroup covering all full-text
// indexes of the given table.
func NewTiCIDataWriterGroup(ctx context.Context, tblInfo *model.TableInfo, schema string, tidbTaskID string, keyspaceID uint32) (*DataWriterGroup, error) {
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
	mgrCtx, err := NewManagerCtx(ctx, etcdClient)
	if err != nil {
		return nil, err
	}
	mgrCtx.SetKeyspaceID(keyspaceID)

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
	return g
}

// CreateFileWriter create the ticiFileWriter according to the writers in the group.
// cloudStoreURI is the S3 URI, logger is taken from DataWriters.
func (g *DataWriterGroup) CreateFileWriter(ctx context.Context) (*FileWriter, error) {
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
	if fileWriter == nil {
		return errors.New("TICIFileWriter is not initialized")
	}
	if g.indexMeta.tblInfo == nil {
		return errors.New("tblInfo is nil for DataWriter")
	}
	// Clone and then Marshal table info to ensure longtext/json flen is narrowed to int32
	tblJSON, err := cloneAndMarshalTableInfo(g.indexMeta.tblInfo)
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
	if fileWriter == nil {
		return errors.New("TICIFileWriter is not initialized")
	}
	uri, err := fileWriter.URI()
	if err != nil || uri == "" {
		return fmt.Errorf("uri is empty or Error occurred: %v", err)
	}

	err = g.mgrCtx.FinishPartitionUpload(ctx, g.indexMeta.tidbTaskID, lowerBound, upperBound, uri)
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
	if err := g.mgrCtx.FinishIndexUpload(ctx, g.indexMeta.tidbTaskID); err != nil {
		g.logger.Error("failed to finish index upload", zap.Error(err))
		return err
	}
	g.logger.Info("successfully finish index upload")
	return nil
}

// CloseFileWriters closes the TICIFileWriter.
func (g *DataWriterGroup) CloseFileWriters(ctx context.Context, fileWriter *FileWriter) error {
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
	if g.mgrCtx != nil {
		g.mgrCtx.Close()
	}
	if g.etcdClient != nil {
		return g.etcdClient.Close()
	}
	return nil
}
