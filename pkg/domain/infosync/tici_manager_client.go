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

package infosync

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tici"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TiCIManagerCtx manages fulltext index for TiCI.
type TiCIManagerCtx struct {
	conn              *grpc.ClientConn
	metaServiceClient tici.MetaServiceClient
}

// NewTiCIManager creates a new TiCI manager.
func NewTiCIManager(ticiHost string, ticiPort string) (*TiCIManagerCtx, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%s", ticiHost, ticiPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	metaServiceClient := tici.NewMetaServiceClient(conn)
	return &TiCIManagerCtx{
		conn:              conn,
		metaServiceClient: metaServiceClient,
	}, nil
}

// CreateFulltextIndex creates fulltext index on TiCI.
func (t *TiCIManagerCtx) CreateFulltextIndex(ctx context.Context, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, schemaName string) error {
	indexColumns := make([]*tici.ColumnInfo, 0)
	for i := range indexInfo.Columns {
		offset := indexInfo.Columns[i].Offset
		indexColumns = append(indexColumns, &tici.ColumnInfo{
			ColumnId:     tblInfo.Columns[offset].ID,
			ColumnName:   tblInfo.Columns[offset].Name.String(),
			Type:         int32(tblInfo.Columns[offset].GetType()),
			ColumnLength: int32(tblInfo.Columns[offset].FieldType.StorageLength()),
			Decimal:      int32(tblInfo.Columns[offset].GetDecimal()),
			DefaultVal:   tblInfo.Columns[offset].DefaultValueBit,
			IsPrimaryKey: mysql.HasPriKeyFlag(tblInfo.Columns[offset].GetFlag()),
			IsArray:      len(indexInfo.Columns) > 1,
		})
	}
	tableColumns := make([]*tici.ColumnInfo, 0)
	for i := range tblInfo.Columns {
		tableColumns = append(tableColumns, &tici.ColumnInfo{
			ColumnId:     tblInfo.Columns[i].ID,
			ColumnName:   tblInfo.Columns[i].Name.String(),
			Type:         int32(tblInfo.Columns[i].GetType()),
			ColumnLength: int32(tblInfo.Columns[i].FieldType.StorageLength()),
			Decimal:      int32(tblInfo.Columns[i].GetDecimal()),
			DefaultVal:   tblInfo.Columns[i].DefaultValueBit,
			IsPrimaryKey: mysql.HasPriKeyFlag(tblInfo.Columns[i].GetFlag()),
			IsArray:      len(tblInfo.Columns) > 1,
		})
	}
	req := &tici.CreateIndexRequest{
		IndexInfo: &tici.IndexInfo{
			TableId:   tblInfo.ID,
			IndexId:   indexInfo.ID,
			IndexName: indexInfo.Name.String(),
			IndexType: tici.IndexType_FULL_TEXT,
			Columns:   indexColumns,
			IsUnique:  indexInfo.Unique,
			ParserInfo: &tici.ParserInfo{
				ParserType: tici.ParserType_DEFAULT_PARSER,
			},
		},
		TableInfo: &tici.TableInfo{
			TableId:      tblInfo.ID,
			TableName:    tblInfo.Name.L,
			DatabaseName: schemaName,
			Version:      int64(tblInfo.Version),
			Columns:      tableColumns,
		},
	}
	resp, err := t.metaServiceClient.CreateIndex(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != 0 {
		logutil.BgLogger().Error("create fulltext index failed", zap.String("indexID", resp.IndexId), zap.String("errorMessage", resp.ErrorMessage))
		return nil
	}
	logutil.BgLogger().Info("create fulltext index success", zap.String("indexID", resp.IndexId))

	return nil
}

// GetCloudStoragePath requests the S3 path from TiCI Meta Service
// for a baseline shard upload.
func (t *TiCIManagerCtx) GetCloudStoragePath(
	ctx context.Context,
	tblInfo *model.TableInfo,
	indexInfo *model.IndexInfo,
	schemaName string,
	lowerBound, upperBound []byte,
) (string, error) {
	// Convert model.IndexInfo to tici.IndexInfo and extract information needed
	indexColumns := make([]*tici.ColumnInfo, 0)
	for i := range indexInfo.Columns {
		offset := indexInfo.Columns[i].Offset
		indexColumns = append(indexColumns, &tici.ColumnInfo{
			ColumnId:     tblInfo.Columns[offset].ID,
			ColumnName:   tblInfo.Columns[offset].Name.String(),
			Type:         int32(tblInfo.Columns[offset].GetType()),
			ColumnLength: int32(tblInfo.Columns[offset].FieldType.StorageLength()),
			Decimal:      int32(tblInfo.Columns[offset].GetDecimal()),
			DefaultVal:   tblInfo.Columns[offset].DefaultValueBit,
			IsPrimaryKey: mysql.HasPriKeyFlag(tblInfo.Columns[offset].GetFlag()),
			IsArray:      len(indexInfo.Columns) > 1,
		})
	}
	indexerIndexInfo := &tici.IndexInfo{
		TableId:   tblInfo.ID,
		IndexId:   indexInfo.ID,
		IndexName: indexInfo.Name.String(),
		IndexType: tici.IndexType_FULL_TEXT,
		Columns:   indexColumns,
		IsUnique:  indexInfo.Unique,
		ParserInfo: &tici.ParserInfo{
			ParserType: tici.ParserType_DEFAULT_PARSER,
		},
	}

	tableColumns := make([]*tici.ColumnInfo, 0)
	for i := range tblInfo.Columns {
		tableColumns = append(tableColumns, &tici.ColumnInfo{
			ColumnId:     tblInfo.Columns[i].ID,
			ColumnName:   tblInfo.Columns[i].Name.String(),
			Type:         int32(tblInfo.Columns[i].GetType()),
			ColumnLength: int32(tblInfo.Columns[i].FieldType.StorageLength()),
			Decimal:      int32(tblInfo.Columns[i].GetDecimal()),
			DefaultVal:   tblInfo.Columns[i].DefaultValueBit,
			IsPrimaryKey: mysql.HasPriKeyFlag(tblInfo.Columns[i].GetFlag()),
			IsArray:      len(tblInfo.Columns) > 1,
		})
	}
	indexerTableInfo := &tici.TableInfo{
		TableId:      tblInfo.ID,
		TableName:    tblInfo.Name.L,
		DatabaseName: schemaName,
		Version:      int64(tblInfo.Version),
		Columns:      tableColumns,
	}

	req := &tici.GetCloudStoragePathRequest{
		IndexInfo:  indexerIndexInfo,
		TableInfo:  indexerTableInfo,
		LowerBound: lowerBound,
		UpperBound: upperBound,
	}
	resp, err := t.metaServiceClient.GetCloudStoragePath(ctx, req)
	if err != nil {
		return "", err
	}
	if resp.Status != 0 {
		logutil.BgLogger().Error("Request TiCI cloud storage path failed",
			zap.Int64("tableID", indexerTableInfo.TableId),
			zap.Int64("indexID", indexerIndexInfo.TableId),
			zap.String("startKey", string(lowerBound)),
			zap.String("endKey", string(upperBound)),
			zap.String("errorMessage", resp.ErrorMessage))
		return "", fmt.Errorf("tici cloud storage path error: %s", resp.ErrorMessage)
	}
	logutil.BgLogger().Info("Requested TiCI cloud storage path",
		zap.Int64("tableID", indexerTableInfo.TableId),
		zap.Int64("indexID", indexerIndexInfo.TableId),
		zap.String("startKey", string(lowerBound)),
		zap.String("endKey", string(upperBound)),
		zap.String("filepath", resp.S3Path))
	return resp.S3Path, nil
}

// MarkPartitionUploadFinished notifies TiCI Meta Service that all partitions for the given table are uploaded.
func (t *TiCIManagerCtx) MarkPartitionUploadFinished(
	ctx context.Context,
	s3Path string,
) error {
	req := &tici.MarkPartitionUploadFinishedRequest{
		S3Path: s3Path,
	}
	resp, err := t.metaServiceClient.MarkPartitionUploadFinished(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != 0 {
		logutil.BgLogger().Error("MarkPartitionUploadFinished failed",
			zap.String("s3Path", s3Path),
			zap.String("errorMessage", resp.ErrorMessage))
		return fmt.Errorf("tici mark partition upload finished error: %s", resp.ErrorMessage)
	}
	logutil.BgLogger().Info("MarkPartitionUploadFinished success", zap.String("s3Path", s3Path))
	return nil
}

// MarkTableUploadFinished notifies TiCI Meta Service that the whole table/index upload is finished.
func (t *TiCIManagerCtx) MarkTableUploadFinished(
	ctx context.Context,
	tableID int64,
	indexID int64,
) error {
	req := &tici.MarkTableUploadFinishedRequest{
		TableId: tableID,
		IndexId: indexID,
	}
	resp, err := t.metaServiceClient.MarkTableUploadFinished(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != 0 {
		logutil.BgLogger().Error("MarkTableUploadFinished failed",
			zap.Int64("tableID", tableID),
			zap.Int64("indexID", indexID),
			zap.String("errorMessage", resp.ErrorMessage))
		return fmt.Errorf("tici mark table upload finished error: %s", resp.ErrorMessage)
	}
	logutil.BgLogger().Info("MarkTableUploadFinished success",
		zap.Int64("tableID", tableID),
		zap.Int64("indexID", indexID))
	return nil
}

// Close closes the underlying gRPC connection to TiCI Meta Service.
func (t *TiCIManagerCtx) Close() error {
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}

// ModelTableToTiCITableInfo converts a model.TableInfo to a tici.TableInfo.
// It extracts the necessary information from the model.TableInfo to create a tici.TableInfo
// suitable for TiCI operations.
func ModelTableToTiCITableInfo(tblInfo *model.TableInfo, schemaName string) *tici.TableInfo {
	tableColumns := make([]*tici.ColumnInfo, 0)
	for i := range tblInfo.Columns {
		tableColumns = append(tableColumns, &tici.ColumnInfo{
			ColumnId:     tblInfo.Columns[i].ID,
			ColumnName:   tblInfo.Columns[i].Name.String(),
			Type:         int32(tblInfo.Columns[i].GetType()),
			ColumnLength: int32(tblInfo.Columns[i].FieldType.StorageLength()),
			Decimal:      int32(tblInfo.Columns[i].GetDecimal()),
			DefaultVal:   tblInfo.Columns[i].DefaultValueBit,
			IsPrimaryKey: mysql.HasPriKeyFlag(tblInfo.Columns[i].GetFlag()),
			IsArray:      len(tblInfo.Columns) > 1,
		})
	}

	return &tici.TableInfo{
		TableId:      tblInfo.ID,
		TableName:    tblInfo.Name.L,
		DatabaseName: schemaName,
		Version:      int64(tblInfo.Version),
		Columns:      tableColumns,
	}
}

// ModelIndexToTiCIIndexInfo converts a model.IndexInfo to a tici.IndexInfo.
// It extracts the necessary information from the model.IndexInfo and model.TableInfo
// to create a tici.IndexInfo suitable for TiCI operations.
func ModelIndexToTiCIIndexInfo(indexInfo *model.IndexInfo, tblInfo *model.TableInfo,
) *tici.IndexInfo {
	indexColumns := make([]*tici.ColumnInfo, 0)
	for i := range indexInfo.Columns {
		offset := indexInfo.Columns[i].Offset
		indexColumns = append(indexColumns, &tici.ColumnInfo{
			ColumnId:     tblInfo.Columns[offset].ID,
			ColumnName:   tblInfo.Columns[offset].Name.String(),
			Type:         int32(tblInfo.Columns[offset].GetType()),
			ColumnLength: int32(tblInfo.Columns[offset].FieldType.StorageLength()),
			Decimal:      int32(tblInfo.Columns[offset].GetDecimal()),
			DefaultVal:   tblInfo.Columns[offset].DefaultValueBit,
			IsPrimaryKey: mysql.HasPriKeyFlag(tblInfo.Columns[offset].GetFlag()),
			IsArray:      len(indexInfo.Columns) > 1,
		})
	}

	return &tici.IndexInfo{
		TableId:   tblInfo.ID,
		IndexId:   indexInfo.ID,
		IndexName: indexInfo.Name.String(),
		IndexType: tici.IndexType_FULL_TEXT,
		Columns:   indexColumns,
		IsUnique:  indexInfo.Unique,
		ParserInfo: &tici.ParserInfo{
			ParserType: tici.ParserType_DEFAULT_PARSER,
		},
	}
}
