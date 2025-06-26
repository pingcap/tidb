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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tici"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TiCIManagerCtx manages fulltext index for TiCI.
type TiCIManagerCtx struct {
	mu                sync.RWMutex
	err               error
	conn              *grpc.ClientConn
	metaServiceClient tici.MetaServiceClient
	ctx               context.Context
}

// MetaServiceEelectionKey is the election path used for meta service leader election.
// The same as https://github.com/pingcap-inc/tici/blob/master/src/servicediscovery/mod.rs#L4
const MetaServiceEelectionKey = "/tici/metaserivce/election"

// NewTiCIManager creates a new TiCI manager.
func NewTiCIManager(ticiHost string, ticiPort string) (*TiCIManagerCtx, error) {
	ctx := context.Background()
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%s", ticiHost, ticiPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create meta service client")
	}
	metaServiceClient := tici.NewMetaServiceClient(conn)
	return &TiCIManagerCtx{
		conn:              conn,
		metaServiceClient: metaServiceClient,
		ctx:               ctx,
	}, nil
}

func createMetaServiceClient(ctx context.Context, client *clientv3.Client) (tici.MetaServiceClient, error) {
	opt := clientv3.WithPrefix()
	resp, err := client.Get(ctx, MetaServiceEelectionKey, opt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get meta service election key")
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("no meta service leader found")
	}
	if len(resp.Kvs) > 1 {
		return nil, errors.New("multiple meta service leaders found")
	}
	kv := resp.Kvs[0]
	conn, err := grpc.NewClient(string(kv.Value), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create grpc client for meta service at %s", kv.Value)
	}
	return tici.NewMetaServiceClient(conn), nil
}

// updateClient listens for changes in the MetaServiceEelectionKey in etcd and updates the metaServiceClient accordingly.
func (t *TiCIManagerCtx) updateClient(ch clientv3.WatchChan) {
	for resp := range ch {
		for _, event := range resp.Events {
			switch event.Type {
			case clientv3.EventTypePut:
				func() {
					t.mu.Lock()
					defer t.mu.Unlock()
					conn, err := grpc.NewClient(string(event.Kv.Value), grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						t.conn.Close()
						t.conn = nil
						t.metaServiceClient = nil
						t.err = err
						return
					}
					// Close previous connection if it exists
					if t.conn != nil {
						t.conn.Close()
					}
					t.conn = conn
					t.metaServiceClient = tici.NewMetaServiceClient(t.conn)
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
func (t *TiCIManagerCtx) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ctx.Done() // Ensure the context is done before closing
	if t.conn != nil {
		if err := t.conn.Close(); err != nil {
			logutil.BgLogger().Error("failed to close grpc connection", zap.Error(err))
		}
		t.conn = nil
	}
	if t.metaServiceClient != nil {
		t.metaServiceClient = nil
	}
	t.err = errors.New("TiCIManagerCtx closed")
}

// CreateFullTextIndex creates fulltext index on TiCI.
func (t *TiCIManagerCtx) CreateFullTextIndex(ctx context.Context, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, schemaName string) error {
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
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.metaServiceClient == nil {
		var errMsg string
		if t.err != nil {
			errMsg = t.err.Error()
		}
		logutil.BgLogger().Error("meta service client is nil", zap.String("errorMessage", errMsg))
		return nil
	}
	resp, err := t.metaServiceClient.CreateIndex(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != 0 {
		logutil.BgLogger().Error("create fulltext index failed", zap.Int64("indexID", req.IndexInfo.IndexId), zap.String("errorMessage", resp.ErrorMessage))
		return nil
	}
	logutil.BgLogger().Info("create fulltext index success", zap.Int64("indexID", req.IndexInfo.IndexId))

	return nil
}

// DropFullTextIndex drop full text index on TiCI.
func (t *TiCIManagerCtx) DropFullTextIndex(ctx context.Context, tableID, indexID int64) error {
	req := &tici.DropIndexRequest{
		TableId: tableID,
		IndexId: indexID,
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.metaServiceClient == nil {
		var errMsg string
		if t.err != nil {
			errMsg = t.err.Error()
		}
		logutil.BgLogger().Error("meta service client is nil", zap.String("errorMessage", errMsg))
		return nil
	}
	resp, err := t.metaServiceClient.DropIndex(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != 0 {
		logutil.BgLogger().Error("drop full text index failed", zap.Int64("table ID", req.TableId), zap.Int64("index ID", req.IndexId), zap.String("errorMessage", resp.ErrorMessage))
		return nil
	}
	logutil.BgLogger().Info("drop full text index success", zap.Int64("index ID", req.TableId), zap.Int64("index ID", req.IndexId))
	return nil
}
