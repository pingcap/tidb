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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/pd/client/constants"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	getEtcdClientFunc = getEtcdClient
	newManagerCtxFunc = NewManagerCtx
)

type addIndexProgressLogKey struct {
	keyspaceID uint32
	tableID    int64
	indexID    int64
}

var addIndexProgressLogState sync.Map // map[addIndexProgressLogKey]GetIndexProgressResponse_State

var mockTiCICreateIndexRequest atomic.Value            // stores []byte of CreateIndexRequest for tests.
var mockTiCIAddPartitionRequest atomic.Value           // stores []byte of AddPartitionRequest for tests.
var mockTiCIDropPartitionRequest atomic.Value          // stores []byte of DropPartitionRequest for tests.
var mockTiCIGetImportStoragePrefixRequest atomic.Value // stores []byte of GetImportStoragePrefixRequest for tests.
var mockTiCIFinishIndexUploadRequest atomic.Value      // stores []byte of FinishImportIndexUploadRequest for tests.
var mockTiCIPreSplitImportShardsRequest atomic.Value   // stores []byte of PreSplitImportShardsRequest for tests.

// MetaServiceStatusError is returned when MetaService replies with a non-success status.
type MetaServiceStatusError struct {
	Method string
	Status ErrorCode
}

// Error implements the error interface.
func (e *MetaServiceStatusError) Error() string {
	return fmt.Sprintf("%s failed: %d", e.Method, e.Status)
}

// GetMockTiCICreateIndexRequest returns the marshaled CreateIndexRequest bytes captured by the
// `MockCreateTiCIIndexRequest` failpoint. It returns nil if nothing was captured.
func GetMockTiCICreateIndexRequest() []byte {
	v := mockTiCICreateIndexRequest.Load()
	if v == nil {
		return nil
	}
	b, ok := v.([]byte)
	if !ok || len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}

// ResetMockTiCICreateIndexRequest clears the captured request for tests.
func ResetMockTiCICreateIndexRequest() {
	mockTiCICreateIndexRequest.Store([]byte{})
}

// GetMockTiCIAddPartitionRequest returns the marshaled AddPartitionRequest bytes captured by the
// `MockAddTiCIPartitionRequest` failpoint. It returns nil if nothing was captured.
func GetMockTiCIAddPartitionRequest() []byte {
	v := mockTiCIAddPartitionRequest.Load()
	if v == nil {
		return nil
	}
	b, ok := v.([]byte)
	if !ok || len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}

// ResetMockTiCIAddPartitionRequest clears the captured request for tests.
func ResetMockTiCIAddPartitionRequest() {
	mockTiCIAddPartitionRequest.Store([]byte{})
}

// GetMockTiCIDropPartitionRequest returns the marshaled DropPartitionRequest bytes captured by the
// `MockDropTiCIPartitionRequest` failpoint. It returns nil if nothing was captured.
func GetMockTiCIDropPartitionRequest() []byte {
	v := mockTiCIDropPartitionRequest.Load()
	if v == nil {
		return nil
	}
	b, ok := v.([]byte)
	if !ok || len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}

// ResetMockTiCIDropPartitionRequest clears the captured request for tests.
func ResetMockTiCIDropPartitionRequest() {
	mockTiCIDropPartitionRequest.Store([]byte{})
}

// GetMockTiCIGetImportStoragePrefixRequest returns the marshaled GetImportStoragePrefixRequest bytes captured by the
// `MockGetCloudStoragePrefix` failpoint. It returns nil if nothing was captured.
func GetMockTiCIGetImportStoragePrefixRequest() []byte {
	v := mockTiCIGetImportStoragePrefixRequest.Load()
	if v == nil {
		return nil
	}
	b, ok := v.([]byte)
	if !ok || len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}

// ResetMockTiCIGetImportStoragePrefixRequest clears the captured request for tests.
func ResetMockTiCIGetImportStoragePrefixRequest() {
	mockTiCIGetImportStoragePrefixRequest.Store([]byte{})
}

// GetMockTiCIFinishIndexUploadRequest returns the marshaled FinishImportIndexUploadRequest bytes captured by the
// `MockFinishIndexUpload` failpoint. It returns nil if nothing was captured.
func GetMockTiCIFinishIndexUploadRequest() []byte {
	v := mockTiCIFinishIndexUploadRequest.Load()
	if v == nil {
		return nil
	}
	b, ok := v.([]byte)
	if !ok || len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}

// ResetMockTiCIFinishIndexUploadRequest clears the captured request for tests.
func ResetMockTiCIFinishIndexUploadRequest() {
	mockTiCIFinishIndexUploadRequest.Store([]byte{})
}

// GetMockTiCIPreSplitImportShardsRequest returns the marshaled PreSplitImportShardsRequest bytes captured by the
// `MockPreSplitImportShards` failpoint. It returns nil if nothing was captured.
func GetMockTiCIPreSplitImportShardsRequest() []byte {
	v := mockTiCIPreSplitImportShardsRequest.Load()
	if v == nil {
		return nil
	}
	b, ok := v.([]byte)
	if !ok || len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}

// ResetMockTiCIPreSplitImportShardsRequest clears the captured request for tests.
func ResetMockTiCIPreSplitImportShardsRequest() {
	mockTiCIPreSplitImportShardsRequest.Store([]byte{})
}

type metaClient struct {
	conn   *grpc.ClientConn
	client MetaServiceClient
}

// keyspaceStorage is the store capability needed to derive the keyspace for a TiCI request.
type keyspaceStorage interface {
	GetCodec() tikv.Codec
}

func newMetaClient(addr string) (*metaClient, error) {
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
	keyspaceID atomic.Uint32
}

// MetaServiceElectionKey is the election path used for meta service leader election.
// The same as https://github.com/pingcap-inc/tici/blob/master/src/servicediscovery/mod.rs#L4
const MetaServiceElectionKey = "/tici/metaservice/election"

// NewManagerCtx creates a new TiCI manager.
func NewManagerCtx(ctx context.Context, client *clientv3.Client) (*ManagerCtx, error) {
	if client == nil {
		logutil.BgLogger().Error("etcd client is nil")
		return nil, errors.New("etcd client is nil")
	}

	var metaClient *metaClient
	if addr, err := getMetaServiceLeaderAddress(ctx, client); err == nil {
		metaClient, err = newMetaClient(addr)
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
	ch := client.Watch(managerCtx.ctx, MetaServiceElectionKey, clientv3.WithPrefix())
	managerCtx.wg.Add(1)
	go func() {
		defer managerCtx.wg.Done()
		managerCtx.updateClient(ch)
	}()
	return managerCtx, nil
}

func getMetaServiceLeaderAddress(ctx context.Context, client *clientv3.Client) (string, error) {
	resp, err := client.Get(ctx, MetaServiceElectionKey, clientv3.WithPrefix())
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

// updateClient listens for changes in the MetaServiceElectionKey in etcd and updates the metaServiceClient accordingly.
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
					metaClient, err := newMetaClient(string(event.Kv.Value))
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
		logutil.BgLogger().Error("meta service client is nil", zap.String("errorMessage", errMsg), zap.Stack("stack"))
		return errors.Errorf("meta service client is nil: %s", errMsg)
	}
	return nil
}

// SetKeyspaceID configures the keyspace id carried in TiCI requests.
func (t *ManagerCtx) SetKeyspaceID(keyspaceID uint32) {
	t.keyspaceID.Store(keyspaceID)
}

func (t *ManagerCtx) getKeyspaceID() uint32 {
	return t.keyspaceID.Load()
}

func buildCreateFulltextIndexRequest(tblInfo *model.TableInfo, indexID int64, schemaName string, keyspaceID uint32, parserInfo *ParserInfo) (*CreateIndexRequest, error) {
	tableInfoJSON, err := cloneAndMarshalTableInfo(tblInfo)
	if err != nil {
		return nil, err
	}
	return &CreateIndexRequest{
		DatabaseName: schemaName,
		TableInfo:    tableInfoJSON,
		IndexId:      indexID,
		KeyspaceId:   keyspaceID,
		ParserInfo:   parserInfo,
	}, nil
}

func buildAddPartitionRequest(tblInfo *model.TableInfo, indexIDs []int64, schemaName string, keyspaceID uint32, parserInfo *ParserInfo) (*AddPartitionRequest, error) {
	tableInfoJSON, err := cloneAndMarshalAddPartitionTableInfo(tblInfo)
	if err != nil {
		return nil, err
	}
	return &AddPartitionRequest{
		DatabaseName: schemaName,
		TableInfo:    tableInfoJSON,
		IndexIds:     append([]int64(nil), indexIDs...),
		KeyspaceId:   keyspaceID,
		ParserInfo:   parserInfo,
	}, nil
}

func (t *ManagerCtx) createIndex(ctx context.Context, req *CreateIndexRequest) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return err
	}
	resp, err := t.metaClient.client.CreateIndex(ctx, req)
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	if resp.Status != ErrorCode_SUCCESS {
		logutil.BgLogger().Error("create index failed", zap.String("indexID", resp.IndexId), zap.String("errorMessage", resp.ErrorMessage))
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(resp.ErrorMessage)
	}
	logutil.BgLogger().Info("create index success", zap.String("indexID", resp.IndexId))
	return nil
}

func (t *ManagerCtx) addPartition(ctx context.Context, req *AddPartitionRequest) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return err
	}
	resp, err := t.metaClient.client.AddPartition(ctx, req)
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(fmt.Sprintf("tici AddPartition rpc error: %v", err))
	}
	if resp.Status != ErrorCode_SUCCESS {
		code := resp.Status
		errMsg := fmt.Sprintf(
			"tici AddPartition failed: status=%s(%d) keyspaceID=%d indexIDs=%v error=%s",
			code.String(),
			resp.Status,
			req.KeyspaceId,
			req.IndexIds,
			resp.ErrorMessage,
		)
		logutil.BgLogger().Error("add partition failed",
			zap.Int64s("indexIDs", req.IndexIds),
			zap.Uint32("keyspaceID", req.KeyspaceId),
			zap.Int32("statusCode", int32(resp.Status)),
			zap.String("status", code.String()),
			zap.String("errorMessage", resp.ErrorMessage),
		)
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(errMsg)
	}
	logutil.BgLogger().Info("add partition success", zap.Int64s("indexIDs", resp.IndexIds))
	return nil
}

// CreateFulltextIndex creates fulltext index on TiCI.
func (t *ManagerCtx) CreateFulltextIndex(ctx context.Context, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, schemaName string, parserInfo *ParserInfo) error {
	req, err := buildCreateFulltextIndexRequest(tblInfo, indexInfo.ID, schemaName, t.getKeyspaceID(), parserInfo)
	if err != nil {
		return err
	}
	return t.createIndex(ctx, req)
}

// AddPartition notifies TiCI to add new partition(s) for the given indexes.
func (t *ManagerCtx) AddPartition(ctx context.Context, tblInfo *model.TableInfo, indexIDs []int64, schemaName string, parserInfo *ParserInfo) error {
	if len(indexIDs) == 0 {
		return nil
	}
	req, err := buildAddPartitionRequest(tblInfo, indexIDs, schemaName, t.getKeyspaceID(), parserInfo)
	if err != nil {
		return err
	}
	return t.addPartition(ctx, req)
}

// DropFullTextIndex drop fulltext index on TiCI.
func (t *ManagerCtx) DropFullTextIndex(ctx context.Context, tableID, indexID int64) error {
	req := &DropIndexRequest{
		TableId:    tableID,
		IndexId:    indexID,
		KeyspaceId: t.getKeyspaceID(),
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
	logutil.BgLogger().Info("drop full text index success", zap.Int64("tableID", req.TableId), zap.Int64("indexID", req.IndexId))
	return nil
}

// DropPartition notifies TiCI to drop a partition for the given indexes.
func (t *ManagerCtx) DropPartition(ctx context.Context, tableID int64, indexIDs []int64) error {
	if len(indexIDs) == 0 {
		return nil
	}
	req := &DropPartitionRequest{
		TableId:    tableID,
		IndexIds:   append([]int64(nil), indexIDs...),
		KeyspaceId: t.getKeyspaceID(),
	}
	return t.dropPartition(ctx, req)
}

func (t *ManagerCtx) dropPartition(ctx context.Context, req *DropPartitionRequest) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return err
	}
	resp, err := t.metaClient.client.DropPartition(ctx, req)
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(fmt.Sprintf("tici DropPartition rpc error: %v", err))
	}
	if resp.Status != ErrorCode_SUCCESS {
		code := resp.Status
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(fmt.Sprintf(
			"tici DropPartition failed: status=%s(%d) keyspaceID=%d tableID=%d indexIDs=%v error=%s",
			code.String(),
			resp.Status,
			req.KeyspaceId,
			req.TableId,
			req.IndexIds,
			resp.ErrorMessage,
		))
	}
	logutil.BgLogger().Info("drop partition success", zap.Int64("tableID", req.TableId), zap.Int64s("indexIDs", req.IndexIds))
	return nil
}

// GetCloudStoragePrefix returns a cloud storage prefix from TiCI with a unique identifier
// (naming tidbTaskID) for each standalone job. TiCI ensures different "standalone job"
// will return different cloud storage prefixes. For example,
// "s3://my-bucket/t_{table_id}/i_{index_id}/import/{import_job_id}".
func (t *ManagerCtx) GetCloudStoragePrefix(
	ctx context.Context,
	tidbTaskID string,
	tableID int64,
	indexIDs []int64,
) (string, uint64, error) {
	failpoint.Inject("MockGetCloudStoragePrefix", func(val failpoint.Value) {
		reqData, marshalErr := json.Marshal(&GetImportStoragePrefixRequest{
			TidbTaskId: tidbTaskID,
			TableId:    tableID,
			IndexIds:   append([]int64(nil), indexIDs...),
		})
		if marshalErr == nil {
			mockTiCIGetImportStoragePrefixRequest.Store(reqData)
		}
		mockStorageURI := "s3://mock-tici/t_{table_id}/i_{index_id}/import/mock_job"
		mockJobID := uint64(1)
		mockErrorMessage := ""
		switch v := val.(type) {
		case string:
			if v != "" {
				var payload struct {
					StorageURI   string `json:"storage_uri"`
					JobID        uint64 `json:"job_id"`
					ErrorMessage string `json:"error_message"`
				}
				if err := json.Unmarshal([]byte(v), &payload); err == nil {
					if payload.StorageURI != "" {
						mockStorageURI = payload.StorageURI
					}
					if payload.JobID != 0 {
						mockJobID = payload.JobID
					}
					if payload.ErrorMessage != "" {
						mockErrorMessage = payload.ErrorMessage
					}
				} else if parts := strings.SplitN(v, ",", 2); len(parts) > 0 {
					mockStorageURI = parts[0]
					if len(parts) == 2 {
						if parsedJobID, parseErr := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64); parseErr == nil {
							mockJobID = parsedJobID
						} else {
							mockErrorMessage = fmt.Sprintf("mock job ID parse error: %v", parseErr)
						}
					}
				}
			}
		case uint64:
			mockJobID = v
		case int:
			if v > 0 {
				mockJobID = uint64(v)
			}
		}
		if mockErrorMessage != "" {
			logutil.BgLogger().Warn("MockGetCloudStoragePrefix failpoint triggered with error",
				zap.String("tidbTaskID", tidbTaskID),
				zap.Int64("tableID", tableID),
				zap.Int64s("indexIDs", indexIDs),
				zap.String("errorMessage", mockErrorMessage))
			failpoint.Return("", uint64(0), fmt.Errorf("tici GetCloudStoragePrefix error: %s", mockErrorMessage))
		}
		logutil.BgLogger().Info("MockGetCloudStoragePrefix failpoint triggered",
			zap.String("tidbTaskID", tidbTaskID),
			zap.Int64("tableID", tableID),
			zap.Int64s("indexIDs", indexIDs),
			zap.String("ticiStorageURI", mockStorageURI),
			zap.Uint64("ticiJobID", mockJobID))
		failpoint.Return(mockStorageURI, mockJobID, nil)
	})
	// TODO: Can we set the start tso here?
	if len(indexIDs) == 0 {
		return "", 0, errors.New("indexIDs is invalid")
	}
	req := &GetImportStoragePrefixRequest{
		TidbTaskId: tidbTaskID,
		TableId:    tableID,
		IndexIds:   indexIDs,
		KeyspaceId: t.getKeyspaceID(),
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return "", 0, err
	}
	resp, err := t.metaClient.client.GetImportStoragePrefix(ctx, req)
	if err != nil {
		return "", 0, err
	}
	if resp.Status != ErrorCode_SUCCESS {
		logutil.BgLogger().Error("GetCloudStoragePrefix failed",
			zap.String("tidbTaskID", tidbTaskID),
			zap.Int64("tableID", tableID),
			zap.Int64s("indexIDs", indexIDs),
			zap.String("errorMessage", resp.ErrorMessage))
		return "", 0, fmt.Errorf("tici GetCloudStoragePrefix error: %s", resp.ErrorMessage)
	}
	logutil.BgLogger().Info("GetCloudStoragePrefix success",
		zap.String("tidbTaskID", tidbTaskID),
		zap.Int64("tableID", tableID),
		zap.Int64s("indexIDs", indexIDs),
		// log down the tici job ID for tracking
		zap.Uint64("ticiJobID", resp.JobId),
		zap.String("ticiStorageURI", resp.StorageUri),
	)
	return resp.StorageUri, resp.JobId, nil
}

// PreSplitImportShards asks TiCI to pre-split internal shards for an incoming TiDB global-sort ingest job.
func (t *ManagerCtx) PreSplitImportShards(ctx context.Context, req *PreSplitImportShardsRequest) error {
	if handled, err := maybeMockPreSplitImportShards(req); handled {
		return err
	}
	if req == nil {
		return errors.New("pre split import shards request is nil")
	}
	req.KeyspaceId = t.getKeyspaceID()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return err
	}
	resp, err := t.metaClient.client.PreSplitImportShards(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != ErrorCode_SUCCESS {
		logutil.BgLogger().Error("PreSplitImportShards failed",
			zap.String("tidbTaskID", req.TidbTaskId),
			zap.Int64("tableID", req.TableId),
			zap.Int64s("indexIDs", req.IndexIds),
			zap.String("errorMessage", resp.ErrorMessage))
		return fmt.Errorf("tici PreSplitImportShards error: %s", resp.ErrorMessage)
	}
	logutil.BgLogger().Info("PreSplitImportShards success",
		zap.String("tidbTaskID", req.TidbTaskId),
		zap.Int64("tableID", req.TableId),
		zap.Int64s("indexIDs", req.IndexIds),
		zap.Uint64("totalKvSize", req.TotalKvSize),
		zap.Uint64("totalKvCnt", req.TotalKvCnt))
	return nil
}

// FinishPartitionUpload notifies TiCI that one partition for the given key-range of
// data has been uploaded.
//
// There may be some retries when TiDB handling the partition data, and TiDB may upload
// multiple files for a key-range. It is acceptable to call this RPC multiple times for
// a key-range that may have uploaded data before. TiCI will use the latest upload data
// to generate the index.
// The `storage_uri` should be a file with the prefix generate by `GetImportStoragePrefix`
// using the same `tidbTaskID`. The file stored in `storage_uri` should contain the table
// info, indexes info and the key-value pairs within the specified key-range.
// The file stored in `storage_uri` will be deleted after the index is built successfully
// or the job is aborted.
func (t *ManagerCtx) FinishPartitionUpload(
	ctx context.Context,
	tidbTaskID string,
	indexID int64,
	lowerBound, upperBound []byte,
	storageURI string,
) error {
	failpoint.Inject("MockFinishPartitionUpload", func(val failpoint.Value) {
		mockSuccess := false
		if v, ok := val.(bool); ok {
			mockSuccess = v
		}
		if mockSuccess {
			logutil.BgLogger().Info("MockFinishPartitionUpload failpoint triggered",
				zap.String("tidbTaskID", tidbTaskID),
				zap.Int64("indexID", indexID),
				zap.String("storageURI", storageURI),
				zap.String("startKey", hex.EncodeToString(lowerBound)),
				zap.String("endKey", hex.EncodeToString(upperBound)))
			failpoint.Return(nil)
		}
		err := errors.New("mock FinishPartitionUpload failed")
		logutil.BgLogger().Warn("MockFinishPartitionUpload failpoint triggered with error",
			zap.String("tidbTaskID", tidbTaskID),
			zap.Int64("indexID", indexID),
			zap.String("storageURI", storageURI),
			zap.String("startKey", hex.EncodeToString(lowerBound)),
			zap.String("endKey", hex.EncodeToString(upperBound)),
			zap.Error(err))
		failpoint.Return(err)
	})
	req := &FinishImportPartitionUploadRequest{
		TidbTaskId: tidbTaskID,
		KeyRange: &KeyRange{
			StartKey: lowerBound,
			EndKey:   upperBound,
		},
		StorageUri: storageURI,
		KeyspaceId: t.getKeyspaceID(),
		IndexId:    indexID,
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return err
	}
	resp, err := t.metaClient.client.FinishImportPartitionUpload(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != ErrorCode_SUCCESS {
		logutil.BgLogger().Error("FinishPartitionUpload failed",
			zap.String("tidbTaskID", tidbTaskID),
			zap.Int64("indexID", indexID),
			zap.String("startKey", hex.EncodeToString(lowerBound)),
			zap.String("endKey", hex.EncodeToString(upperBound)),
			zap.String("errorMessage", resp.ErrorMessage))
		return fmt.Errorf("tici FinishPartitionUpload error: %s", resp.ErrorMessage)
	}
	return nil
}

// FinishIndexUpload notifies TiCI that all partitions for the given job in all TiDB instances
// have been uploaded ** successfully **.
// TiCI will mark the job as finished and make the files available to GC data after consumed
// by downstream consumers.
func (t *ManagerCtx) FinishIndexUpload(
	ctx context.Context,
	tidbTaskID string,
) error {
	if handled, err := maybeMockFinishIndexUpload(tidbTaskID); handled {
		return err
	}
	req := &FinishImportIndexUploadRequest{
		TidbTaskId: tidbTaskID,
		Status:     ErrorCode_SUCCESS,
		KeyspaceId: t.getKeyspaceID(),
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return err
	}
	resp, err := t.metaClient.client.FinishImportIndexUpload(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != ErrorCode_SUCCESS {
		logutil.BgLogger().Error("FinishIndexUpload failed",
			zap.String("tidbTaskID", tidbTaskID),
			zap.String("errorMessage", resp.ErrorMessage))
		return fmt.Errorf("tici FinishIndexUpload error: %s", resp.ErrorMessage)
	}
	return nil
}

// CheckAddIndexProgress checks whether a TiCI index build has completed.
func (t *ManagerCtx) CheckAddIndexProgress(ctx context.Context, tableID, indexID int64) (bool, error) {
	if handled, ready, err := maybeMockCheckAddIndexProgress(tableID, indexID); handled {
		return ready, err
	}
	req := &GetIndexProgressRequest{
		TableId:    tableID,
		IndexId:    indexID,
		KeyspaceId: t.getKeyspaceID(),
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return false, err
	}
	resp, err := t.metaClient.client.GetIndexProgress(ctx, req)
	if err != nil {
		return false, err
	}
	logGetIndexProgressResponse(addIndexProgressLogKey{
		keyspaceID: t.getKeyspaceID(),
		tableID:    tableID,
		indexID:    indexID,
	}, resp)
	// State is the source of truth for GetIndexProgress.
	// Some TiCI implementations may only return state and omit status/error_message.
	switch resp.State {
	case GetIndexProgressResponse_COMPLETED:
		if resp.Status != ErrorCode_SUCCESS {
			logutil.BgLogger().Warn("GetIndexProgress completed with non-success status",
				zap.Int64("tableID", tableID),
				zap.Int64("indexID", indexID),
				zap.Int32("statusCode", int32(resp.Status)),
				zap.String("status", resp.Status.String()),
				zap.String("errorMessage", resp.ErrorMessage))
		}
		return true, nil
	case GetIndexProgressResponse_PENDING, GetIndexProgressResponse_RUNNING, GetIndexProgressResponse_NOTFOUND:
		if resp.Status != ErrorCode_SUCCESS {
			logutil.BgLogger().Warn("GetIndexProgress in-progress with non-success status",
				zap.Int64("tableID", tableID),
				zap.Int64("indexID", indexID),
				zap.Int32("stateCode", int32(resp.State)),
				zap.String("state", resp.State.String()),
				zap.Int32("statusCode", int32(resp.Status)),
				zap.String("status", resp.Status.String()),
				zap.String("errorMessage", resp.ErrorMessage))
		}
		return false, nil
	case GetIndexProgressResponse_FAILED, GetIndexProgressResponse_ERROR:
		errMsg := resp.ErrorMessage
		if errMsg == "" {
			errMsg = resp.State.String()
		}
		logutil.BgLogger().Error("TiCI index build failed",
			zap.Int64("tableID", tableID),
			zap.Int64("indexID", indexID),
			zap.Int32("stateCode", int32(resp.State)),
			zap.String("state", resp.State.String()),
			zap.Int32("statusCode", int32(resp.Status)),
			zap.String("status", resp.Status.String()),
			zap.String("errorMessage", errMsg))
		return false, fmt.Errorf("tici index build %s: %s", resp.State.String(), errMsg)
	default:
		if resp.Status != ErrorCode_SUCCESS {
			errMsg := resp.ErrorMessage
			if errMsg == "" {
				errMsg = resp.Status.String()
			}
			logutil.BgLogger().Error("GetIndexProgress failed",
				zap.Int64("tableID", tableID),
				zap.Int64("indexID", indexID),
				zap.Int32("stateCode", int32(resp.State)),
				zap.String("state", resp.State.String()),
				zap.Int32("statusCode", int32(resp.Status)),
				zap.String("status", resp.Status.String()),
				zap.String("errorMessage", errMsg))
			return false, fmt.Errorf("tici GetIndexProgress error: %s", errMsg)
		}
		return false, fmt.Errorf("tici GetIndexProgress unknown state: %s", resp.State.String())
	}
}

func logGetIndexProgressResponse(key addIndexProgressLogKey, resp *GetIndexProgressResponse) {
	if resp == nil {
		return
	}
	terminal := resp.State == GetIndexProgressResponse_COMPLETED ||
		resp.State == GetIndexProgressResponse_FAILED ||
		resp.State == GetIndexProgressResponse_ERROR
	if terminal {
		addIndexProgressLogState.Delete(key)
	} else {
		prev, loaded := addIndexProgressLogState.Load(key)
		if loaded {
			if prevState, ok := prev.(GetIndexProgressResponse_State); ok && prevState == resp.State {
				return
			}
		}
		addIndexProgressLogState.Store(key, resp.State)
	}
	logutil.BgLogger().Info("GetIndexProgress response",
		zap.Int64("tableID", key.tableID),
		zap.Int64("indexID", key.indexID),
		zap.Uint32("keyspaceID", key.keyspaceID),
		zap.Int32("statusCode", int32(resp.Status)),
		zap.String("status", resp.Status.String()),
		zap.Int32("stateCode", int32(resp.State)),
		zap.String("state", resp.State.String()),
		zap.String("errorMessage", resp.ErrorMessage))
}

func maybeMockFinishIndexUpload(tidbTaskID string) (bool, error) {
	var (
		handled bool
		err     error
	)
	failpoint.Inject("MockFinishIndexUpload", func(val failpoint.Value) {
		handled = true
		reqData, marshalErr := json.Marshal(&FinishImportIndexUploadRequest{
			TidbTaskId: tidbTaskID,
			Status:     ErrorCode_SUCCESS,
		})
		if marshalErr == nil {
			mockTiCIFinishIndexUploadRequest.Store(reqData)
		}
		mockSuccess := false
		if v, ok := val.(bool); ok {
			mockSuccess = v
		}
		if mockSuccess {
			logutil.BgLogger().Info("MockFinishIndexUpload failpoint triggered",
				zap.String("tidbTaskID", tidbTaskID))
		} else {
			err = errors.New("mock FinishIndexUpload failed")
			logutil.BgLogger().Warn("MockFinishIndexUpload failpoint triggered with error",
				zap.String("tidbTaskID", tidbTaskID),
				zap.Error(err))
		}
	})
	return handled, err
}

// maybeMockPreSplitImportShards intercepts TiCI pre-split requests in tests and stores the captured payload.
func maybeMockPreSplitImportShards(req *PreSplitImportShardsRequest) (handled bool, err error) {
	failpoint.Inject("MockPreSplitImportShards", func(val failpoint.Value) {
		handled = true
		var tidbTaskID string
		var tableID int64
		var indexIDs []int64
		if req != nil {
			tidbTaskID = req.TidbTaskId
			tableID = req.TableId
			indexIDs = req.IndexIds
			data, marshalErr := json.Marshal(req)
			if marshalErr == nil {
				mockTiCIPreSplitImportShardsRequest.Store(data)
			}
		}
		switch v := val.(type) {
		case bool:
			if v {
				logutil.BgLogger().Info("MockPreSplitImportShards failpoint triggered",
					zap.String("tidbTaskID", tidbTaskID),
					zap.Int64("tableID", tableID),
					zap.Int64s("indexIDs", indexIDs))
				return
			}
			err = errors.New("mock PreSplitImportShards failed")
		case string:
			if v != "" {
				err = errors.New(v)
			}
		}
		if err != nil {
			logutil.BgLogger().Warn("MockPreSplitImportShards failpoint triggered with error",
				zap.String("tidbTaskID", tidbTaskID),
				zap.Int64("tableID", tableID),
				zap.Int64s("indexIDs", indexIDs),
				zap.Error(err))
		}
	})
	return handled, err
}

func maybeMockCheckAddIndexProgress(tableID, indexID int64) (handled bool, ready bool, err error) {
	failpoint.Inject("MockCheckAddIndexProgress", func(val failpoint.Value) {
		handled = true
		switch v := val.(type) {
		case bool:
			ready = v
		case string:
			if v != "" {
				err = errors.New(v)
			}
		}
		if err != nil {
			logutil.BgLogger().Warn("MockCheckAddIndexProgress failpoint triggered with error",
				zap.Int64("tableID", tableID),
				zap.Int64("indexID", indexID),
				zap.Error(err))
			return
		}
		logutil.BgLogger().Info("MockCheckAddIndexProgress failpoint triggered",
			zap.Int64("tableID", tableID),
			zap.Int64("indexID", indexID),
			zap.Bool("ready", ready))
	})
	return handled, ready, err
}

func closeEtcdClient(etcdClient *clientv3.Client) {
	if etcdClient == nil || etcdClient.ActiveConnection() == nil {
		return
	}
	if err := etcdClient.Close(); err != nil {
		logutil.BgLogger().Warn("close TiCI etcd client failed", zap.Error(err))
	}
}

// AbortIndexUpload notifies TiCI that the job has failed and the related TiCI job should be aborted.
// TiCI will clean up all the related data belonging to the tidbTaskID.
// It is not currently being called by TiDB, but rather reserved for future use, if any.
func (t *ManagerCtx) AbortIndexUpload(
	ctx context.Context,
	tidbTaskID string,
	errMsg string,
) error {
	req := &FinishImportIndexUploadRequest{
		TidbTaskId:   tidbTaskID,
		Status:       ErrorCode_UNKNOWN_ERROR,
		ErrorMessage: errMsg,
		KeyspaceId:   t.getKeyspaceID(),
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if err := t.checkMetaClient(); err != nil {
		return err
	}
	resp, err := t.metaClient.client.FinishImportIndexUpload(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != ErrorCode_SUCCESS {
		logutil.BgLogger().Error("AbortIndexUpload failed",
			zap.String("tidbTaskID", tidbTaskID),
			zap.String("reason", errMsg),
			zap.String("errorMessage", resp.ErrorMessage))
		return fmt.Errorf("tici AbortIndexUpload error: %s", resp.ErrorMessage)
	}
	return nil
}

func isRetryableGetShardLocalCacheStatus(status int32) bool {
	switch ErrorCode(status) {
	case ErrorCode_TRY_AGAIN, ErrorCode_WORKER_NOT_FOUND, ErrorCode_SHARD_NOT_SCHEDULED:
		return true
	default:
		return false
	}
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
		TableId:    tableID,
		IndexId:    indexID,
		KeyspaceId: t.keyspaceID.Load(),
		KeyRanges:  ticiKeyRanges,
		Limit:      int32(limit),
	}
	var (
		backoff    = 100 * time.Millisecond
		maxBackoff = time.Second

		start   = time.Now()
		maxWait = 30 * time.Second
	)
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 && remaining < maxWait {
			maxWait = remaining
		}
	}
	for attempt := 0; ; attempt++ {
		resp, err := func() (*GetShardLocalCacheResponse, error) {
			t.mu.RLock()
			defer t.mu.RUnlock()
			if err := t.checkMetaClient(); err != nil {
				return nil, err
			}
			return t.metaClient.client.GetShardLocalCacheInfo(ctx, request)
		}()
		if err != nil {
			return nil, err
		}
		if resp.Status == 0 {
			var s = "ShardLocalCacheInfos:["
			for _, info := range resp.ShardLocalCacheInfos {
				if info != nil {
					s += fmt.Sprintf("[ShardId: %d, StartKey: %v, EndKey: %v, Epoch: %d, LocalCacheAddrs: %v; ]",
						info.Shard.ShardId, hex.EncodeToString(info.Shard.StartKey), hex.EncodeToString(info.Shard.EndKey), info.Shard.Epoch, info.LocalCacheAddrs)
				}
			}
			s += "]"
			logutil.BgLogger().Info("GetShardLocalCacheInfo", zap.String("info", s))
			return resp.ShardLocalCacheInfos, nil
		}

		code := ErrorCode(resp.Status)
		if !isRetryableGetShardLocalCacheStatus(resp.Status) || time.Since(start) >= maxWait {
			return nil, fmt.Errorf(
				"GetShardLocalCacheInfo failed after %s (attempts=%d, keyspaceID=%d, tableID=%d, indexID=%d, ranges=%d, limit=%d, returnedShards=%d): %w",
				time.Since(start).Round(time.Millisecond),
				attempt+1,
				request.KeyspaceId,
				tableID,
				indexID,
				len(keyRanges),
				limit,
				len(resp.ShardLocalCacheInfos),
				&MetaServiceStatusError{
					Method: "GetShardLocalCacheInfo",
					Status: code,
				},
			)
		}
		logutil.BgLogger().Debug("GetShardLocalCacheInfo retryable error",
			zap.Int64("tableID", tableID),
			zap.Int64("indexID", indexID),
			zap.Int("attempt", attempt+1),
			zap.Duration("elapsed", time.Since(start)),
			zap.Duration("maxWait", maxWait),
			zap.String("status", code.String()),
			zap.Int32("status_code", resp.Status),
		)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}

		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
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
			Flag:         uint32(tblInfo.Columns[offset].GetFlag()),
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

// CreateFulltextIndex create fulltext index on TiCI.
func CreateFulltextIndex(ctx context.Context, store kv.Storage, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, schemaName string, parserInfo *ParserInfo) error {
	keyspaceID := uint32(0)
	if store != nil {
		keyspaceID = uint32(store.GetCodec().GetKeyspaceID())
		// Log when the KeyspaceID is the special null value, as per requested by TiCI team.
		if keyspaceID == constants.NullKeyspaceID {
			logutil.BgLogger().Debug("Setting special KeyspaceID for TiCI", zap.Uint32("KeyspaceID", keyspaceID))
		}
	}

	req, err := buildCreateFulltextIndexRequest(tblInfo, indexInfo.ID, schemaName, keyspaceID, parserInfo)
	if err != nil {
		return err
	}

	failpoint.Inject("MockCreateTiCIIndexRequest", func(val failpoint.Value) {
		v, _ := val.(int)
		switch v {
		case 1:
			data, err := req.Marshal()
			if err != nil {
				failpoint.Return(errors.Trace(err))
			}
			mockTiCICreateIndexRequest.Store(data)
			failpoint.Return(nil)
		case 2:
			data, err := req.Marshal()
			if err != nil {
				failpoint.Return(errors.Trace(err))
			}
			failpoint.Return(errors.Errorf("mock tici create index request: %s", hex.EncodeToString(data)))
		}
	})

	failpoint.Inject("MockCreateTiCIIndexSuccess", func(val failpoint.Value) {
		if x := val.(bool); x {
			logutil.BgLogger().Info("MockCreateTiCIIndexSuccess failpoint triggered", zap.Bool("success", true))
			failpoint.Return(nil)
		}
		err := dbterror.ErrInvalidDDLJob.FastGenByArgs("mock create TiCI index failed")
		logutil.BgLogger().Warn("MockCreateTiCIIndexSuccess failpoint triggered", zap.Bool("success", false), zap.Error(err))
		failpoint.Return(err)
	})
	etcdClient, err := getEtcdClientFunc()
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer closeEtcdClient(etcdClient)
	ticiManager, err := newManagerCtxFunc(ctx, etcdClient)
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer ticiManager.Close()
	if store != nil {
		ticiManager.SetKeyspaceID(keyspaceID)
	}
	return ticiManager.createIndex(ctx, req)
}

// DropFullTextIndex drop fulltext index on TiCI.
func DropFullTextIndex(ctx context.Context, store kv.Storage, tableID int64, indexID int64) error {
	failpoint.Inject("MockDropTiCIIndexSuccess", func(val failpoint.Value) {
		if x := val.(bool); x {
			logutil.BgLogger().Info("MockDropTiCIIndexSuccess failpoint triggered", zap.Bool("success", true))
			failpoint.Return(nil)
		}
		err := errors.New("mock drop TiCI index failed")
		logutil.BgLogger().Warn("MockDropTiCIIndexSuccess failpoint triggered", zap.Bool("success", false), zap.Error(err))
		failpoint.Return(err)
	})
	etcdClient, err := getEtcdClientFunc()
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer closeEtcdClient(etcdClient)
	ticiManager, err := newManagerCtxFunc(ctx, etcdClient)
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer ticiManager.Close()
	if store != nil {
		keyspaceID := uint32(store.GetCodec().GetKeyspaceID())
		// Log when the KeyspaceID is the special null value, as per requested by TiCI team.
		if keyspaceID == constants.NullKeyspaceID {
			logutil.BgLogger().Debug("Setting special KeyspaceID for TiCI", zap.Uint32("KeyspaceID", keyspaceID))
		}
		ticiManager.SetKeyspaceID(keyspaceID)
	}
	return ticiManager.DropFullTextIndex(ctx, tableID, indexID)
}

// AddPartition notifies TiCI to create new partition(s) for the given table and
// create corresponding index metadata for existing indexes.
func AddPartition(ctx context.Context, store kv.Storage, tblInfo *model.TableInfo, schemaName string, indexIDs []int64, parserInfo *ParserInfo) error {
	if len(indexIDs) == 0 {
		return nil
	}

	keyspaceID := uint32(0)
	if store != nil {
		keyspaceID = uint32(store.GetCodec().GetKeyspaceID())
		// Log when the KeyspaceID is the special null value, as per requested by TiCI team.
		if keyspaceID == constants.NullKeyspaceID {
			logutil.BgLogger().Debug("Setting special KeyspaceID for TiCI", zap.Uint32("KeyspaceID", keyspaceID))
		}
	}

	req, err := buildAddPartitionRequest(tblInfo, indexIDs, schemaName, keyspaceID, parserInfo)
	if err != nil {
		return err
	}

	failpoint.Inject("MockAddTiCIPartitionRequest", func(val failpoint.Value) {
		v, _ := val.(int)
		switch v {
		case 1:
			data, err := req.Marshal()
			if err != nil {
				failpoint.Return(errors.Trace(err))
			}
			mockTiCIAddPartitionRequest.Store(data)
			failpoint.Return(nil)
		case 2:
			data, err := req.Marshal()
			if err != nil {
				failpoint.Return(errors.Trace(err))
			}
			failpoint.Return(errors.Errorf("mock tici add partition request: %s", hex.EncodeToString(data)))
		}
	})

	failpoint.Inject("MockAddTiCIPartitionSuccess", func(val failpoint.Value) {
		if x := val.(bool); x {
			logutil.BgLogger().Info("MockAddTiCIPartitionSuccess failpoint triggered", zap.Bool("success", true))
			failpoint.Return(nil)
		}
		err := dbterror.ErrInvalidDDLJob.FastGenByArgs("mock TiCI add partition failed")
		logutil.BgLogger().Warn("MockAddTiCIPartitionSuccess failpoint triggered", zap.Bool("success", false), zap.Error(err))
		failpoint.Return(err)
	})

	etcdClient, err := getEtcdClientFunc()
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer closeEtcdClient(etcdClient)
	ticiManager, err := newManagerCtxFunc(ctx, etcdClient)
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer ticiManager.Close()
	if store != nil {
		ticiManager.SetKeyspaceID(keyspaceID)
	}
	return ticiManager.addPartition(ctx, req)
}

// DropPartition notifies TiCI to drop a physical table (partition) and remove
// corresponding index metadata for the given indexes.
func DropPartition(ctx context.Context, store kv.Storage, physicalTableID int64, indexIDs []int64) error {
	if len(indexIDs) == 0 {
		return nil
	}

	keyspaceID := uint32(0)
	if store != nil {
		keyspaceID = uint32(store.GetCodec().GetKeyspaceID())
		// Log when the KeyspaceID is the special null value, as per requested by TiCI team.
		if keyspaceID == constants.NullKeyspaceID {
			logutil.BgLogger().Debug("Setting special KeyspaceID for TiCI", zap.Uint32("KeyspaceID", keyspaceID))
		}
	}

	req := &DropPartitionRequest{
		TableId:    physicalTableID,
		IndexIds:   append([]int64(nil), indexIDs...),
		KeyspaceId: keyspaceID,
	}

	failpoint.Inject("MockDropTiCIPartitionRequest", func(val failpoint.Value) {
		v, _ := val.(int)
		switch v {
		case 1:
			data, err := req.Marshal()
			if err != nil {
				failpoint.Return(errors.Trace(err))
			}
			mockTiCIDropPartitionRequest.Store(data)
			failpoint.Return(nil)
		case 2:
			data, err := req.Marshal()
			if err != nil {
				failpoint.Return(errors.Trace(err))
			}
			failpoint.Return(errors.Errorf("mock tici drop partition request: %s", hex.EncodeToString(data)))
		}
	})

	failpoint.Inject("MockDropTiCIPartitionSuccess", func(val failpoint.Value) {
		if x := val.(bool); x {
			logutil.BgLogger().Info("MockDropTiCIPartitionSuccess failpoint triggered", zap.Bool("success", true))
			failpoint.Return(nil)
		}
		err := errors.New("mock TiCI drop partition failed")
		logutil.BgLogger().Warn("MockDropTiCIPartitionSuccess failpoint triggered", zap.Bool("success", false), zap.Error(err))
		failpoint.Return(err)
	})

	etcdClient, err := getEtcdClientFunc()
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer closeEtcdClient(etcdClient)
	ticiManager, err := newManagerCtxFunc(ctx, etcdClient)
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer ticiManager.Close()
	if store != nil {
		ticiManager.SetKeyspaceID(keyspaceID)
	}
	return ticiManager.dropPartition(ctx, req)
}

// FinishIndexUpload notifies TiCI that all partitions for the given job in all TiDB instances
// have been uploaded successfully.
func FinishIndexUpload(ctx context.Context, store kv.Storage, tidbTaskID string) error {
	if handled, err := maybeMockFinishIndexUpload(tidbTaskID); handled {
		return err
	}
	etcdClient, err := getEtcdClientFunc()
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer closeEtcdClient(etcdClient)
	ticiManager, err := newManagerCtxFunc(ctx, etcdClient)
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer ticiManager.Close()
	if store != nil {
		keyspaceID := uint32(store.GetCodec().GetKeyspaceID())
		// Log when the KeyspaceID is the special null value, as per requested by TiCI team.
		if keyspaceID == constants.NullKeyspaceID {
			logutil.BgLogger().Debug("Setting special KeyspaceID for TiCI", zap.Uint32("KeyspaceID", keyspaceID))
		}
		ticiManager.SetKeyspaceID(keyspaceID)
	}
	return ticiManager.FinishIndexUpload(ctx, tidbTaskID)
}

// PreSplitImportShards asks TiCI to pre-split shards for an incoming global-sort ingest.
func PreSplitImportShards(ctx context.Context, store keyspaceStorage, req *PreSplitImportShardsRequest) error {
	if handled, err := maybeMockPreSplitImportShards(req); handled {
		return err
	}
	etcdClient, err := getEtcdClientFunc()
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer closeEtcdClient(etcdClient)
	ticiManager, err := newManagerCtxFunc(ctx, etcdClient)
	if err != nil {
		return dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer ticiManager.Close()
	if store != nil {
		keyspaceID := uint32(store.GetCodec().GetKeyspaceID())
		if keyspaceID == constants.NullKeyspaceID {
			logutil.BgLogger().Debug("Setting special KeyspaceID for TiCI", zap.Uint32("KeyspaceID", keyspaceID))
		}
		ticiManager.SetKeyspaceID(keyspaceID)
	}
	return ticiManager.PreSplitImportShards(ctx, req)
}

// CheckAddIndexProgress checks whether a TiCI index build has completed.
func CheckAddIndexProgress(ctx context.Context, store kv.Storage, tableID, indexID int64) (bool, error) {
	if handled, ready, err := maybeMockCheckAddIndexProgress(tableID, indexID); handled {
		return ready, err
	}
	etcdClient, err := getEtcdClientFunc()
	if err != nil {
		return false, dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer closeEtcdClient(etcdClient)
	ticiManager, err := newManagerCtxFunc(ctx, etcdClient)
	if err != nil {
		return false, dbterror.ErrInvalidDDLJob.FastGenByArgs(err)
	}
	defer ticiManager.Close()
	if store != nil {
		keyspaceID := uint32(store.GetCodec().GetKeyspaceID())
		// Log when the KeyspaceID is the special null value, as per requested by TiCI team.
		if keyspaceID == constants.NullKeyspaceID {
			logutil.BgLogger().Debug("Setting special KeyspaceID for TiCI", zap.Uint32("KeyspaceID", keyspaceID))
		}
		ticiManager.SetKeyspaceID(keyspaceID)
	}
	return ticiManager.CheckAddIndexProgress(ctx, tableID, indexID)
}

// cloneAndNormalizeTableInfo deep-clones the given model.TableInfo via JSON
// marshal/unmarshal and forces FieldType.Flen for longtext and json column types
// through an int32 narrowing (int(int32(flen))). This ensures TiCI parses the
// length as int32 and avoids 4GB default-value related errors.
func cloneAndNormalizeTableInfo(tbl *model.TableInfo) (*model.TableInfo, error) {
	intest.Assert(tbl != nil, "tableInfo is nil")

	// deep clone via JSON marshal/unmarshal
	b, err := json.Marshal(tbl)
	if err != nil {
		return nil, err
	}
	var dup model.TableInfo
	if err := json.Unmarshal(b, &dup); err != nil {
		return nil, err
	}
	// normalize flen for longtext and json types by narrowing to int32 and back
	for i := range dup.Columns {
		tp := dup.Columns[i].GetType()
		if tp == mysql.TypeLongBlob || tp == mysql.TypeJSON {
			dup.Columns[i].FieldType.SetFlen(int(int32(dup.Columns[i].FieldType.GetFlen())))
		}
	}
	return &dup, nil
}

// cloneAndMarshalTableInfo clones and normalizes the given TableInfo and returns
// the marshalled JSON bytes. It returns an error if tbl is nil or if marshalling fails.
func cloneAndMarshalTableInfo(tbl *model.TableInfo) ([]byte, error) {
	if tbl == nil {
		return nil, errors.New("tableInfo is nil")
	}
	clonedTbl, err := cloneAndNormalizeTableInfo(tbl)
	if err != nil {
		return nil, err
	}
	return json.Marshal(clonedTbl)
}

// cloneAndMarshalAddPartitionTableInfo narrows the partition snapshot in AddPartition
// requests to the newly-added partitions only, so TiCI does not treat existing
// partitions as fresh additions.
func cloneAndMarshalAddPartitionTableInfo(tbl *model.TableInfo) ([]byte, error) {
	if tbl == nil {
		return nil, errors.New("tableInfo is nil")
	}
	clonedTbl, err := cloneAndNormalizeTableInfo(tbl)
	if err != nil {
		return nil, err
	}
	if clonedTbl.Partition != nil && len(clonedTbl.Partition.AddingDefinitions) > 0 {
		clonedTbl.Partition.Definitions = append([]model.PartitionDefinition(nil), clonedTbl.Partition.AddingDefinitions...)
		clonedTbl.Partition.AddingDefinitions = nil
		clonedTbl.Partition.DroppingDefinitions = nil
	}
	return json.Marshal(clonedTbl)
}
