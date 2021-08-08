// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package importer

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/tikv"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/table"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	defaultRetryBackoffTime = time.Second * 3
	writeRowsMaxRetryTimes  = 3
)

var (
	// Importer backend is compatible with TiDB [2.1.0, NextMajorVersion).
	requiredMinTiDBVersion = *semver.New("2.1.0")
	requiredMinPDVersion   = *semver.New("2.1.0")
	requiredMinTiKVVersion = *semver.New("2.1.0")
	requiredMaxTiDBVersion = version.NextMajorVersion()
	requiredMaxPDVersion   = version.NextMajorVersion()
	requiredMaxTiKVVersion = version.NextMajorVersion()
)

// importer represents a gRPC connection to tikv-importer. This type is
// goroutine safe: you can share this instance and execute any method anywhere.
type importer struct {
	conn   *grpc.ClientConn
	cli    import_kvpb.ImportKVClient
	pdAddr string
	tls    *common.TLS

	mutationPool sync.Pool
	// lock ensures ImportEngine are runs serially
	lock sync.Mutex

	tsMap sync.Map // engineUUID -> commitTS
	// For testing convenience.
	getTSFunc func(ctx context.Context) (uint64, error)
}

// NewImporter creates a new connection to tikv-importer. A single connection
// per tidb-lightning instance is enough.
func NewImporter(ctx context.Context, tls *common.TLS, importServerAddr string, pdAddr string) (backend.Backend, error) {
	conn, err := grpc.DialContext(ctx, importServerAddr, tls.ToGRPCDialOption())
	if err != nil {
		return backend.MakeBackend(nil), errors.Trace(err)
	}

	getTSFunc := func(ctx context.Context) (uint64, error) {
		pdCli, err := pd.NewClientWithContext(ctx, []string{pdAddr}, tls.ToPDSecurityOption())
		if err != nil {
			return 0, err
		}
		defer pdCli.Close()

		physical, logical, err := pdCli.GetTS(ctx)
		if err != nil {
			return 0, err
		}
		return oracle.ComposeTS(physical, logical), nil
	}

	return backend.MakeBackend(&importer{
		conn:         conn,
		cli:          import_kvpb.NewImportKVClient(conn),
		pdAddr:       pdAddr,
		tls:          tls,
		mutationPool: sync.Pool{New: func() interface{} { return &import_kvpb.Mutation{} }},
		getTSFunc:    getTSFunc,
	}), nil
}

// NewMockImporter creates an *unconnected* importer based on a custom
// ImportKVClient. This is provided for testing only. Do not use this function
// outside of tests.
func NewMockImporter(cli import_kvpb.ImportKVClient, pdAddr string) backend.Backend {
	return backend.MakeBackend(&importer{
		conn:         nil,
		cli:          cli,
		pdAddr:       pdAddr,
		mutationPool: sync.Pool{New: func() interface{} { return &import_kvpb.Mutation{} }},
		getTSFunc: func(ctx context.Context) (uint64, error) {
			return uint64(time.Now().UnixNano()), nil
		},
	})
}

// Close the importer connection.
func (importer *importer) Close() {
	if importer.conn != nil {
		if err := importer.conn.Close(); err != nil {
			log.L().Warn("close importer gRPC connection failed", zap.Error(err))
		}
	}
}

func (*importer) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

func (*importer) MaxChunkSize() int {
	// 31 MB. hardcoded by importer, so do we
	return 31 << 10
}

func (*importer) ShouldPostProcess() bool {
	return true
}

// isIgnorableOpenCloseEngineError checks if the error from
// CloseEngine can be safely ignored.
func isIgnorableOpenCloseEngineError(err error) bool {
	// We allow "FileExists" error. This happens when the engine has been
	// closed before. This error typically arise when resuming from a
	// checkpoint with a partially-imported engine.
	//
	// If the error is legit in a no-checkpoints settings, the later WriteEngine
	// API will bail us out to keep us safe.
	return err == nil || strings.Contains(err.Error(), "FileExists")
}

func (importer *importer) OpenEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	req := &import_kvpb.OpenEngineRequest{
		Uuid: engineUUID[:],
	}

	_, err := importer.cli.OpenEngine(ctx, req)
	if err != nil {
		return errors.Trace(err)
	}
	if err = importer.allocateTSIfNotExists(ctx, engineUUID); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (importer *importer) getEngineTS(engineUUID uuid.UUID) uint64 {
	if v, ok := importer.tsMap.Load(engineUUID); ok {
		return v.(uint64)
	}
	return 0
}

func (importer *importer) allocateTSIfNotExists(ctx context.Context, engineUUID uuid.UUID) error {
	if importer.getEngineTS(engineUUID) > 0 {
		return nil
	}
	ts, err := importer.getTSFunc(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	importer.tsMap.LoadOrStore(engineUUID, ts)
	return nil
}

func (importer *importer) CloseEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	req := &import_kvpb.CloseEngineRequest{
		Uuid: engineUUID[:],
	}

	_, err := importer.cli.CloseEngine(ctx, req)
	if !isIgnorableOpenCloseEngineError(err) {
		return errors.Trace(err)
	}
	return nil
}

func (importer *importer) Flush(_ context.Context, _ uuid.UUID) error {
	return nil
}

func (importer *importer) ImportEngine(ctx context.Context, engineUUID uuid.UUID) error {
	importer.lock.Lock()
	defer importer.lock.Unlock()
	req := &import_kvpb.ImportEngineRequest{
		Uuid:   engineUUID[:],
		PdAddr: importer.pdAddr,
	}

	_, err := importer.cli.ImportEngine(ctx, req)
	return errors.Trace(err)
}

func (importer *importer) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &import_kvpb.CleanupEngineRequest{
		Uuid: engineUUID[:],
	}

	_, err := importer.cli.CleanupEngine(ctx, req)
	if err == nil {
		importer.tsMap.Delete(engineUUID)
	}
	return errors.Trace(err)
}

func (importer *importer) CollectLocalDuplicateRows(ctx context.Context, tbl table.Table) error {
	panic("Unsupported Operation")
}

func (importer *importer) CollectRemoteDuplicateRows(ctx context.Context, tbl table.Table) error {
	panic("Unsupported Operation")
}

func (importer *importer) WriteRows(
	ctx context.Context,
	engineUUID uuid.UUID,
	tableName string,
	_ []string,
	rows kv.Rows,
) (finalErr error) {
	var err error
	ts := importer.getEngineTS(engineUUID)
outside:
	for _, r := range rows.SplitIntoChunks(importer.MaxChunkSize()) {
		for i := 0; i < writeRowsMaxRetryTimes; i++ {
			err = importer.WriteRowsToImporter(ctx, engineUUID, ts, r)
			switch {
			case err == nil:
				continue outside
			case common.IsRetryableError(err):
				// retry next loop
			default:
				return err
			}
		}
		return errors.Annotatef(err, "[%s] write rows reach max retry %d and still failed", tableName, writeRowsMaxRetryTimes)
	}
	return nil
}

func (importer *importer) WriteRowsToImporter(
	ctx context.Context,
	//nolint:interfacer // false positive
	engineUUID uuid.UUID,
	ts uint64,
	rows kv.Rows,
) (finalErr error) {
	kvs := kv.KvPairsFromRows(rows)
	if len(kvs) == 0 {
		return nil
	}

	wstream, err := importer.cli.WriteEngine(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	logger := log.With(zap.Stringer("engineUUID", engineUUID))

	defer func() {
		resp, closeErr := wstream.CloseAndRecv()
		if closeErr == nil && resp != nil && resp.Error != nil {
			closeErr = errors.Errorf("Engine '%s' not found", resp.Error.EngineNotFound.Uuid)
		}
		if closeErr != nil {
			if finalErr == nil {
				finalErr = errors.Trace(closeErr)
			} else {
				// just log the close error, we need to propagate the earlier error instead
				logger.Warn("close write stream failed", log.ShortError(closeErr))
			}
		}
	}()

	// Bind uuid for this write request
	req := &import_kvpb.WriteEngineRequest{
		Chunk: &import_kvpb.WriteEngineRequest_Head{
			Head: &import_kvpb.WriteHead{
				Uuid: engineUUID[:],
			},
		},
	}
	if err := wstream.Send(req); err != nil {
		return errors.Trace(err)
	}

	// Send kv paris as write request content
	mutations := make([]*import_kvpb.Mutation, len(kvs))
	for i, pair := range kvs {
		mutations[i] = importer.mutationPool.Get().(*import_kvpb.Mutation)
		mutations[i].Op = import_kvpb.Mutation_Put
		mutations[i].Key = pair.Key
		mutations[i].Value = pair.Val
	}

	req.Reset()
	req.Chunk = &import_kvpb.WriteEngineRequest_Batch{
		Batch: &import_kvpb.WriteBatch{
			CommitTs:  ts,
			Mutations: mutations,
		},
	}

	err = wstream.Send(req)
	for _, mutation := range mutations {
		importer.mutationPool.Put(mutation)
	}

	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (*importer) MakeEmptyRows() kv.Rows {
	return kv.MakeRowsFromKvPairs(nil)
}

func (*importer) NewEncoder(tbl table.Table, options *kv.SessionOptions) (kv.Encoder, error) {
	return kv.NewTableKVEncoder(tbl, options)
}

func (importer *importer) CheckRequirements(ctx context.Context, _ *backend.CheckCtx) error {
	if err := checkTiDBVersionByTLS(ctx, importer.tls, requiredMinTiDBVersion, requiredMaxTiDBVersion); err != nil {
		return err
	}
	if err := tikv.CheckPDVersion(ctx, importer.tls, importer.pdAddr, requiredMinPDVersion, requiredMaxPDVersion); err != nil {
		return err
	}
	if err := tikv.CheckTiKVVersion(ctx, importer.tls, importer.pdAddr, requiredMinTiKVVersion, requiredMaxTiKVVersion); err != nil {
		return err
	}
	return nil
}

func checkTiDBVersionByTLS(ctx context.Context, tls *common.TLS, requiredMinVersion, requiredMaxVersion semver.Version) error {
	var status struct{ Version string }
	err := tls.GetJSON(ctx, "/status", &status)
	if err != nil {
		return err
	}

	return version.CheckTiDBVersion(status.Version, requiredMinVersion, requiredMaxVersion)
}

func (importer *importer) FetchRemoteTableModels(ctx context.Context, schema string) ([]*model.TableInfo, error) {
	return tikv.FetchRemoteTableModelsFromTLS(ctx, importer.tls, schema)
}

func (importer *importer) EngineFileSizes() []backend.EngineFileSize {
	return nil
}

func (importer *importer) FlushEngine(context.Context, uuid.UUID) error {
	return nil
}

func (importer *importer) FlushAllEngines(context.Context) error {
	return nil
}

func (importer *importer) ResetEngine(context.Context, uuid.UUID) error {
	return errors.New("cannot reset an engine in importer backend")
}

func (importer *importer) LocalWriter(_ context.Context, _ *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	return &Writer{importer: importer, engineUUID: engineUUID}, nil
}

type Writer struct {
	importer   *importer
	engineUUID uuid.UUID
}

func (w *Writer) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	return nil, nil
}

func (w *Writer) AppendRows(ctx context.Context, tableName string, columnNames []string, rows kv.Rows) error {
	return w.importer.WriteRows(ctx, w.engineUUID, tableName, columnNames, rows)
}

func (w *Writer) IsSynced() bool {
	return true
}
