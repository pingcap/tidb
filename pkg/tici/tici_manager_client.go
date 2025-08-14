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
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type metaClient struct {
	conn   *grpc.ClientConn
	client MetaServiceClient
}

func newMetaClient(ctx context.Context, addr string) (*metaClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logutil.BgLogger().Error("failed to create grpc connection", zap.String("address", addr), zap.Error(err))
		return nil, err
	}
	metaServiceClient := NewMetaServiceClient(conn)
	return &metaClient{
		conn:   conn,
		client: metaServiceClient,
	}, nil
}

func (c *metaClient) Close() {
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			logutil.BgLogger().Error("failed to close grpc connection", zap.Error(err))
		}
		c.conn = nil
	}
	if c.client != nil {
		c.client = nil
	}
}

// ManagerCtx manages fulltext index for TiCI.
type ManagerCtx struct {
	mu         sync.RWMutex
	err        error
	metaClient *metaClient
	ctx        context.Context
	cancel     context.CancelFunc // cancel is used to cancel the context when the manager is closed
	wg         sync.WaitGroup     // wg is used to wait for goroutines to finish
}

// MetaServiceEelectionKey is the election path used for meta service leader election.
// The same as https://github.com/pingcap-inc/tici/blob/master/src/servicediscovery/mod.rs#L4
const MetaServiceEelectionKey = "/tici/metaservice/election"

// NewManagerCtx creates a new TiCI manager.
func NewManagerCtx(ctx context.Context, client *clientv3.Client) (*ManagerCtx, error) {
	if client == nil {
		logutil.BgLogger().Error("etcd client is nil")
		return nil, errors.New("etcd client is nil")
	}

	var metaClient *metaClient
	if addr, err := getMetaServiceLeaderAddress(ctx, client); err == nil {
		metaClient, err = newMetaClient(ctx, addr)
		if err != nil {
			return nil, err
		}
	} else {
		logutil.BgLogger().Error("failed to get meta service leader address", zap.Error(err))
		// If we cannot get the leader address, let metaClient be nil.
		// updateClient will initialize metaClient if a new meta service leader is elected.
		metaClient = nil
	}

	ctx, cancel := context.WithCancel(ctx)
	managerCtx := &ManagerCtx{
		metaClient: metaClient,
		ctx:        ctx,
		cancel:     cancel,
	}
	ch := client.Watch(managerCtx.ctx, MetaServiceEelectionKey, clientv3.WithPrefix())
	managerCtx.wg.Add(1)
	go func() {
		defer managerCtx.wg.Done()
		managerCtx.updateClient(ch)
	}()
	return managerCtx, nil
}

func getMetaServiceLeaderAddress(ctx context.Context, client *clientv3.Client) (string, error) {
	resp, err := client.Get(ctx, MetaServiceEelectionKey, clientv3.WithPrefix())
	if err != nil {
		return "", errors.Wrap(err, "failed to get meta service election key")
	}
	if len(resp.Kvs) == 0 {
		return "", errors.New("no meta service leader found")
	}
	if len(resp.Kvs) > 1 {
		return "", errors.New("multiple meta service leaders found")
	}
	kv := resp.Kvs[0]
	return string(kv.Value), nil
}

// updateClient listens for changes in the MetaServiceEelectionKey in etcd and updates the metaServiceClient accordingly.
func (t *ManagerCtx) updateClient(ch clientv3.WatchChan) {
	for resp := range ch {
		for _, event := range resp.Events {
			switch event.Type {
			case clientv3.EventTypePut:
				func() {
					t.mu.Lock()
					defer t.mu.Unlock()
					// Close previous connection if it exists
					if t.metaClient != nil {
						t.metaClient.Close()
					}
					metaClient, err := newMetaClient(t.ctx, string(event.Kv.Value))
					if err != nil {
						t.metaClient = nil
						t.err = err
					}
					t.metaClient = metaClient
					t.err = nil
				}()
				logutil.BgLogger().Info("Received Put event, update leader address", zap.String("key", string(event.Kv.Key)), zap.String("value", string(event.Kv.Value)))
			case clientv3.EventTypeDelete:
				// just ignore delete event
			}
		}
	}
}

// Close closes the grpc connection and cleans up the metaServiceClient.
func (t *ManagerCtx) Close() {
	t.cancel()
	t.wg.Wait()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.metaClient != nil {
		t.metaClient.Close()
		t.metaClient = nil
	}
	t.err = errors.New("ManagerCtx closed")
}

// checkMetaClient checks if the metaClient is nil and logs an error if it is.
// Please note that this function should be called with the mutex locked.
func (t *ManagerCtx) checkMetaClient() error {
	if t.metaClient == nil {
		var errMsg string
		if t.err != nil {
			errMsg = t.err.Error()
		}
		logutil.BgLogger().Error("meta service client is nil", zap.String("errorMessage", errMsg))
		return errors.Wrap(t.err, "meta service client is nil")
	}
	return nil
}

// CreateFulltextIndex creates fulltext index on TiCI.
func (t *ManagerCtx) CreateFulltextIndex(ctx context.Context, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, schemaName string) error {
	indexColumns := make([]*ColumnInfo, 0)
	for i := range indexInfo.Columns {
		offset := indexInfo.Columns[i].Offset
		indexColumns = append(indexColumns, &ColumnInfo{
			ColumnId:     tblInfo.Columns[offset].ID,
			ColumnName:   tblInfo.Columns[offset].Name.String(),
			Type:         int32(tblInfo.Columns[offset].GetType()),
			ColumnLength: int32(tblInfo.Columns[offset].FieldType.StorageLength()),
			Decimal:      int32(tblInfo.Columns[offset].GetDecimal()),
			Flag:         uint32(tblInfo.Columns[offset].GetFlag()),
			DefaultVal:   tblInfo.Columns[offset].DefaultValueBit,
			IsPrimaryKey: mysql.HasPriKeyFlag(tblInfo.Columns[offset].GetFlag()),
			IsArray:      len(indexInfo.Columns) > 1,
		})
	}
	tableColumns := make([]*ColumnInfo, 0)
	for i := range tblInfo.Columns {
		tableColumns = append(tableColumns, &ColumnInfo{
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
	req := &CreateIndexRequest{
		IndexInfo: &IndexInfo{
			TableId:   tblInfo.ID,
			IndexId:   indexInfo.ID,
			IndexName: indexInfo.Name.String(),
			IndexType: IndexType_FULL_TEXT,
			Columns:   indexColumns,
			IsUnique:  indexInfo.Unique,
			ParserInfo: &ParserInfo{
				ParserType: ParserType_DEFAULT_PARSER,
			},
		},
		TableInfo: &TableInfo{
			TableId:      tblInfo.ID,
			TableName:    tblInfo.Name.L,
			DatabaseName: schemaName,
			Version:      int64(tblInfo.Version),
			Columns:      tableColumns,
			IsClustered:  tblInfo.HasClusteredIndex(),
		},
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return err
	}
	resp, err := t.metaClient.client.CreateIndex(ctx, req)
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	if resp.Status != 0 {
		logutil.BgLogger().Error("create fulltext index failed", zap.String("indexID", resp.IndexId), zap.String("errorMessage", resp.ErrorMessage))
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(resp.ErrorMessage)
	}
	logutil.BgLogger().Info("create fulltext index success", zap.String("indexID", resp.IndexId))
	return nil
}

// DropFullTextIndex drop full text index on TiCI.
func (t *ManagerCtx) DropFullTextIndex(ctx context.Context, tableID, indexID int64) error {
	req := &DropIndexRequest{
		TableId: tableID,
		IndexId: indexID,
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return err
	}
	resp, err := t.metaClient.client.DropIndex(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != 0 {
		return errors.New(resp.ErrorMessage)
	}
	logutil.BgLogger().Info("drop full text index success", zap.Int64("index ID", req.TableId), zap.Int64("index ID", req.IndexId))
	return nil
}

// GetCloudStoragePath requests the S3 path from TiCI Meta Service
// for a baseline shard upload.
func (t *ManagerCtx) GetCloudStoragePath(
	ctx context.Context,
	tblInfo *model.TableInfo,
	indexInfo *model.IndexInfo,
	schemaName string,
	lowerBound, upperBound []byte,
) (string, error) {
	// Convert model.IndexInfo to IndexInfo and extract information needed
	indexColumns := make([]*ColumnInfo, 0)
	for i := range indexInfo.Columns {
		offset := indexInfo.Columns[i].Offset
		indexColumns = append(indexColumns, &ColumnInfo{
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
	indexerIndexInfo := &IndexInfo{
		TableId:   tblInfo.ID,
		IndexId:   indexInfo.ID,
		IndexName: indexInfo.Name.String(),
		IndexType: IndexType_FULL_TEXT,
		Columns:   indexColumns,
		IsUnique:  indexInfo.Unique,
		ParserInfo: &ParserInfo{
			ParserType: ParserType_DEFAULT_PARSER,
		},
	}

	tableColumns := make([]*ColumnInfo, 0)
	for i := range tblInfo.Columns {
		tableColumns = append(tableColumns, &ColumnInfo{
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
	indexerTableInfo := &TableInfo{
		TableId:      tblInfo.ID,
		TableName:    tblInfo.Name.L,
		DatabaseName: schemaName,
		Version:      int64(tblInfo.Version),
		Columns:      tableColumns,
		IsClustered:  tblInfo.HasClusteredIndex(),
	}

	req := &GetCloudStoragePathRequest{
		IndexInfo:  indexerIndexInfo,
		TableInfo:  indexerTableInfo,
		LowerBound: lowerBound,
		UpperBound: upperBound,
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return "", err
	}
	resp, err := t.metaClient.client.GetCloudStoragePath(ctx, req)
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
func (t *ManagerCtx) MarkPartitionUploadFinished(
	ctx context.Context,
	s3Path string,
) error {
	req := &MarkPartitionUploadFinishedRequest{
		S3Path: s3Path,
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return err
	}
	resp, err := t.metaClient.client.MarkPartitionUploadFinished(ctx, req)
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
func (t *ManagerCtx) MarkTableUploadFinished(
	ctx context.Context,
	tableID int64,
	indexID int64,
) error {
	req := &MarkTableUploadFinishedRequest{
		TableId: tableID,
		IndexId: indexID,
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return err
	}
	resp, err := t.metaClient.client.MarkTableUploadFinished(ctx, req)
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

// ScanRanges sends a request to the TiCI shard cache service to scan ranges for a given table and index.
func (t *ManagerCtx) ScanRanges(ctx context.Context, tableID int64, indexID int64, keyRanges []kv.KeyRange, limit int) ([]*ShardLocalCacheInfo, error) {
	ticiKeyRanges := make([]*KeyRange, 0, len(keyRanges))
	for _, r := range keyRanges {
		ticiKeyRanges = append(ticiKeyRanges, &KeyRange{
			StartKey: r.StartKey,
			EndKey:   r.EndKey,
		})
	}

	request := &GetShardLocalCacheRequest{
		TableId:   tableID,
		IndexId:   indexID,
		KeyRanges: ticiKeyRanges,
		Limit:     int32(limit),
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return nil, err
	}
	resp, err := t.metaClient.client.GetShardLocalCacheInfo(ctx, request)
	if err != nil {
		return nil, err
	}
	if resp.Status != 0 {
		return nil, fmt.Errorf("GetShardLocalCacheInfo failed: %d", resp.Status)
	}

	var s = "ShardLocalCacheInfos:["
	for _, info := range resp.ShardLocalCacheInfos {
		if info != nil {
			s += fmt.Sprintf("[ShardId: %d, StartKey: %v, EndKey: %v, Epoch: %d, LocalCacheAddrs: %v; ]",
				info.Shard.ShardId, info.Shard.StartKey, info.Shard.EndKey, info.Shard.Epoch, info.LocalCacheAddrs)
		}
	}
	s += "]"
	logutil.BgLogger().Info("GetShardLocalCacheInfo", zap.String("info", s))
	return resp.ShardLocalCacheInfos, nil
}

// ModelTableToTiCITableInfo converts a model.TableInfo to a TableInfo.
// It extracts the necessary information from the model.TableInfo to create a TableInfo
// suitable for TiCI operations.
func ModelTableToTiCITableInfo(tblInfo *model.TableInfo, schemaName string) *TableInfo {
	tableColumns := make([]*ColumnInfo, 0)
	for i := range tblInfo.Columns {
		tableColumns = append(tableColumns, &ColumnInfo{
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

	return &TableInfo{
		TableId:      tblInfo.ID,
		TableName:    tblInfo.Name.L,
		DatabaseName: schemaName,
		Version:      int64(tblInfo.Version),
		Columns:      tableColumns,
		IsClustered:  tblInfo.HasClusteredIndex(),
	}
}

// ModelIndexToTiCIIndexInfo converts a model.IndexInfo to a IndexInfo.
// It extracts the necessary information from the model.IndexInfo and model.TableInfo
// to create a IndexInfo suitable for TiCI operations.
func ModelIndexToTiCIIndexInfo(indexInfo *model.IndexInfo, tblInfo *model.TableInfo,
) *IndexInfo {
	indexColumns := make([]*ColumnInfo, 0)
	for i := range indexInfo.Columns {
		offset := indexInfo.Columns[i].Offset
		indexColumns = append(indexColumns, &ColumnInfo{
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

	return &IndexInfo{
		TableId:   tblInfo.ID,
		IndexId:   indexInfo.ID,
		IndexName: indexInfo.Name.String(),
		IndexType: IndexType_FULL_TEXT,
		Columns:   indexColumns,
		IsUnique:  indexInfo.Unique,
		ParserInfo: &ParserInfo{
			ParserType: ParserType_DEFAULT_PARSER,
		},
	}
}

// ModelPrimaryKeyToTiCIIndexInfo returns a IndexInfo describing the
// primary key index of the table. If the table has no primary key, returns nil.
func ModelPrimaryKeyToTiCIIndexInfo(
	tblInfo *model.TableInfo,
) *IndexInfo {
	var pkIndex *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Primary {
			pkIndex = idx
			break
		}
	}
	if pkIndex == nil ||
		pkIndex.Name.String() == "" ||
		len(pkIndex.Columns) == 0 {
		return nil
	}

	pkColumns := make([]*ColumnInfo, 0, len(pkIndex.Columns))
	for _, idxCol := range pkIndex.Columns {
		offset := idxCol.Offset
		if offset < 0 || offset >= len(tblInfo.Columns) {
			return nil
		}
		col := tblInfo.Columns[offset]
		pkColumns = append(pkColumns, &ColumnInfo{
			ColumnId:     col.ID,
			ColumnName:   col.Name.String(),
			Type:         int32(col.GetType()),
			ColumnLength: int32(col.FieldType.StorageLength()),
			Decimal:      int32(col.GetDecimal()),
			DefaultVal:   col.DefaultValueBit,
			IsPrimaryKey: true,
			IsArray:      false,
		})
	}

	return &IndexInfo{
		TableId:   tblInfo.ID,
		IndexId:   pkIndex.ID,
		IndexName: pkIndex.Name.String(),
		Columns:   pkColumns,
		IsUnique:  pkIndex.Unique,
	}
}

// CreateFulltextIndex create fulltext index on TiCI.
func CreateFulltextIndex(ctx context.Context, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, schemaName string) error {
	failpoint.Inject("MockCreateTiCIIndexSuccess", func(val failpoint.Value) {
		if x := val.(bool); x {
			failpoint.Return(nil)
		}
		failpoint.Return(dbterror.ErrInvalidDDLJob.FastGenByArgs("mock create TiCI index failed"))
	})
	ticiManager, err := NewManagerCtx(ctx, infosync.GetEtcdClient())
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer ticiManager.Close()
	return ticiManager.CreateFulltextIndex(ctx, tblInfo, indexInfo, schemaName)
}

// DropFullTextIndex drop fulltext index on TiCI.
func DropFullTextIndex(ctx context.Context, tableID int64, indexID int64) error {
	failpoint.Inject("MockDropTiCIIndexSuccess", func(val failpoint.Value) {
		if x := val.(bool); x {
			failpoint.Return(nil)
		}
		failpoint.Return(errors.New("mock drop TiCI index failed"))
	})
	ticiManager, err := NewManagerCtx(ctx, infosync.GetEtcdClient())
	if err != nil {
		return err
	}
	defer ticiManager.Close()
	return ticiManager.DropFullTextIndex(ctx, tableID, indexID)
}
