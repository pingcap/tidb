// Copyright 2024 PingCAP, Inc.
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

package logclient

import (
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	logsplit "github.com/pingcap/tidb/br/pkg/restore/internal/log_split"
	"github.com/pingcap/tidb/br/pkg/restore/internal/rawkv"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/br/pkg/version"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/tikv/client-go/v2/config"
	kvutil "github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/keepalive"
)

const MetaKVBatchSize = 64 * 1024 * 1024
const maxSplitKeysOnce = 10240

// rawKVBatchCount specifies the count of entries that the rawkv client puts into TiKV.
const rawKVBatchCount = 64

type LogClient struct {
	cipher        *backuppb.CipherInfo
	pdClient      pd.Client
	pdHTTPClient  pdhttp.Client
	clusterID     uint64
	dom           *domain.Domain
	tlsConf       *tls.Config
	keepaliveConf keepalive.ClientParameters

	rawKVClient *rawkv.RawKVBatchClient
	storage     storage.ExternalStorage

	se glue.Session

	// currentTS is used for rewrite meta kv when restore stream.
	// Can not use `restoreTS` directly, because schema created in `full backup` maybe is new than `restoreTS`.
	currentTS uint64

	*LogFileManager

	workerPool   *tidbutil.WorkerPool
	fileImporter *LogFileImporter

	// the query to insert rows into table `gc_delete_range`, lack of ts.
	deleteRangeQuery          []*stream.PreDelRangeQuery
	deleteRangeQueryCh        chan *stream.PreDelRangeQuery
	deleteRangeQueryWaitGroup sync.WaitGroup

	// checkpoint information for log restore
	useCheckpoint bool
}

// NewRestoreClient returns a new RestoreClient.
func NewRestoreClient(
	pdClient pd.Client,
	pdHTTPCli pdhttp.Client,
	tlsConf *tls.Config,
	keepaliveConf keepalive.ClientParameters,
) *LogClient {
	return &LogClient{
		pdClient:           pdClient,
		pdHTTPClient:       pdHTTPCli,
		tlsConf:            tlsConf,
		keepaliveConf:      keepaliveConf,
		deleteRangeQuery:   make([]*stream.PreDelRangeQuery, 0),
		deleteRangeQueryCh: make(chan *stream.PreDelRangeQuery, 10),
	}
}

// Close a client.
func (rc *LogClient) Close() {
	// close the connection, and it must be succeed when in SQL mode.
	if rc.se != nil {
		rc.se.Close()
	}

	if rc.rawKVClient != nil {
		rc.rawKVClient.Close()
	}

	if err := rc.fileImporter.Close(); err != nil {
		log.Warn("failed to close file improter")
	}

	log.Info("Restore client closed")
}

func (rc *LogClient) SetRawKVBatchClient(
	ctx context.Context,
	pdAddrs []string,
	security config.Security,
) error {
	rawkvClient, err := rawkv.NewRawkvClient(ctx, pdAddrs, security)
	if err != nil {
		return errors.Trace(err)
	}

	rc.rawKVClient = rawkv.NewRawKVBatchClient(rawkvClient, rawKVBatchCount)
	return nil
}

func (rc *LogClient) SetCrypter(crypter *backuppb.CipherInfo) {
	rc.cipher = crypter
}

func (rc *LogClient) SetConcurrency(c uint) {
	log.Info("download worker pool", zap.Uint("size", c))
	rc.workerPool = tidbutil.NewWorkerPool(c, "file")
}

func (rc *LogClient) SetStorage(ctx context.Context, backend *backuppb.StorageBackend, opts *storage.ExternalStorageOptions) error {
	var err error
	rc.storage, err = storage.New(ctx, backend, opts)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (rc *LogClient) SetCurrentTS(ts uint64) {
	rc.currentTS = ts
}

// GetClusterID gets the cluster id from down-stream cluster.
func (rc *LogClient) GetClusterID(ctx context.Context) uint64 {
	if rc.clusterID <= 0 {
		rc.clusterID = rc.pdClient.GetClusterID(ctx)
	}
	return rc.clusterID
}

func (rc *LogClient) GetDomain() *domain.Domain {
	return rc.dom
}

func (rc *LogClient) CleanUpKVFiles(
	ctx context.Context,
) error {
	// Current we only have v1 prefix.
	// In the future, we can add more operation for this interface.
	return rc.fileImporter.ClearFiles(ctx, rc.pdClient, "v1")
}

func (rc *LogClient) StartCheckpointRunnerForLogRestore(ctx context.Context, taskName string) (*checkpoint.CheckpointRunner[checkpoint.LogRestoreKeyType, checkpoint.LogRestoreValueType], error) {
	runner, err := checkpoint.StartCheckpointRunnerForLogRestore(ctx, rc.storage, rc.cipher, taskName)
	return runner, errors.Trace(err)
}

// Init create db connection and domain for storage.
func (rc *LogClient) Init(g glue.Glue, store kv.Storage) error {
	var err error
	rc.se, err = g.CreateSession(store)
	if err != nil {
		return errors.Trace(err)
	}

	// Set SQL mode to None for avoiding SQL compatibility problem
	err = rc.se.Execute(context.Background(), "set @@sql_mode=''")
	if err != nil {
		return errors.Trace(err)
	}

	rc.dom, err = g.GetDomain(store)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (rc *LogClient) InitClients(ctx context.Context, backend *backuppb.StorageBackend) {
	stores, err := conn.GetAllTiKVStoresWithRetry(ctx, rc.pdClient, util.SkipTiFlash)
	if err != nil {
		log.Fatal("failed to get stores", zap.Error(err))
	}

	metaClient := split.NewClient(rc.pdClient, rc.pdHTTPClient, rc.tlsConf, maxSplitKeysOnce, len(stores)+1)
	importCli := importclient.NewImportClient(metaClient, rc.tlsConf, rc.keepaliveConf)
	rc.fileImporter = NewLogFileImporter(metaClient, importCli, backend)
}

func (rc *LogClient) InitCheckpointMetadataForLogRestore(ctx context.Context, taskName string, gcRatio string) (string, error) {
	rc.useCheckpoint = true

	// it shows that the user has modified gc-ratio, if `gcRatio` doesn't equal to "1.1".
	// update the `gcRatio` for checkpoint metadata.
	if gcRatio == utils.DefaultGcRatioVal {
		// if the checkpoint metadata exists in the external storage, the restore is not
		// for the first time.
		exists, err := checkpoint.ExistsRestoreCheckpoint(ctx, rc.storage, taskName)
		if err != nil {
			return "", errors.Trace(err)
		}

		if exists {
			// load the checkpoint since this is not the first time to restore
			meta, err := checkpoint.LoadCheckpointMetadataForRestore(ctx, rc.storage, taskName)
			if err != nil {
				return "", errors.Trace(err)
			}

			log.Info("reuse gc ratio from checkpoint metadata", zap.String("gc-ratio", gcRatio))
			return meta.GcRatio, nil
		}
	}

	// initialize the checkpoint metadata since it is the first time to restore.
	log.Info("save gc ratio into checkpoint metadata", zap.String("gc-ratio", gcRatio))
	if err := checkpoint.SaveCheckpointMetadataForRestore(ctx, rc.storage, &checkpoint.CheckpointMetadataForRestore{
		GcRatio: gcRatio,
	}, taskName); err != nil {
		return gcRatio, errors.Trace(err)
	}

	return gcRatio, nil
}

func (rc *LogClient) InstallLogFileManager(ctx context.Context, startTS, restoreTS uint64, metadataDownloadBatchSize uint) error {
	init := LogFileManagerInit{
		StartTS:   startTS,
		RestoreTS: restoreTS,
		Storage:   rc.storage,

		MetadataDownloadBatchSize: metadataDownloadBatchSize,
	}
	var err error
	rc.LogFileManager, err = CreateLogFileManager(ctx, init)
	if err != nil {
		return err
	}
	return nil
}

type FilesInRegion struct {
	defaultSize    uint64
	defaultKVCount int64
	writeSize      uint64
	writeKVCount   int64

	defaultFiles []*LogDataFileInfo
	writeFiles   []*LogDataFileInfo
	deleteFiles  []*LogDataFileInfo
}

type FilesInTable struct {
	regionMapFiles map[int64]*FilesInRegion
}

func ApplyKVFilesWithBatchMethod(
	ctx context.Context,
	logIter LogIter,
	batchCount int,
	batchSize uint64,
	applyFunc func(files []*LogDataFileInfo, kvCount int64, size uint64),
	applyWg *sync.WaitGroup,
) error {
	var (
		tableMapFiles        = make(map[int64]*FilesInTable)
		tmpFiles             = make([]*LogDataFileInfo, 0, batchCount)
		tmpSize       uint64 = 0
		tmpKVCount    int64  = 0
	)
	for r := logIter.TryNext(ctx); !r.Finished; r = logIter.TryNext(ctx) {
		if r.Err != nil {
			return r.Err
		}

		f := r.Item
		if f.GetType() == backuppb.FileType_Put && f.GetLength() >= batchSize {
			applyFunc([]*LogDataFileInfo{f}, f.GetNumberOfEntries(), f.GetLength())
			continue
		}

		fit, exist := tableMapFiles[f.TableId]
		if !exist {
			fit = &FilesInTable{
				regionMapFiles: make(map[int64]*FilesInRegion),
			}
			tableMapFiles[f.TableId] = fit
		}
		fs, exist := fit.regionMapFiles[f.RegionId]
		if !exist {
			fs = &FilesInRegion{}
			fit.regionMapFiles[f.RegionId] = fs
		}

		if f.GetType() == backuppb.FileType_Delete {
			if fs.defaultFiles == nil {
				fs.deleteFiles = make([]*LogDataFileInfo, 0)
			}
			fs.deleteFiles = append(fs.deleteFiles, f)
		} else {
			if f.GetCf() == stream.DefaultCF {
				if fs.defaultFiles == nil {
					fs.defaultFiles = make([]*LogDataFileInfo, 0, batchCount)
				}
				fs.defaultFiles = append(fs.defaultFiles, f)
				fs.defaultSize += f.Length
				fs.defaultKVCount += f.GetNumberOfEntries()
				if len(fs.defaultFiles) >= batchCount || fs.defaultSize >= batchSize {
					applyFunc(fs.defaultFiles, fs.defaultKVCount, fs.defaultSize)
					fs.defaultFiles = nil
					fs.defaultSize = 0
					fs.defaultKVCount = 0
				}
			} else {
				if fs.writeFiles == nil {
					fs.writeFiles = make([]*LogDataFileInfo, 0, batchCount)
				}
				fs.writeFiles = append(fs.writeFiles, f)
				fs.writeSize += f.GetLength()
				fs.writeKVCount += f.GetNumberOfEntries()
				if len(fs.writeFiles) >= batchCount || fs.writeSize >= batchSize {
					applyFunc(fs.writeFiles, fs.writeKVCount, fs.writeSize)
					fs.writeFiles = nil
					fs.writeSize = 0
					fs.writeKVCount = 0
				}
			}
		}
	}

	for _, fwt := range tableMapFiles {
		for _, fs := range fwt.regionMapFiles {
			if len(fs.defaultFiles) > 0 {
				applyFunc(fs.defaultFiles, fs.defaultKVCount, fs.defaultSize)
			}
			if len(fs.writeFiles) > 0 {
				applyFunc(fs.writeFiles, fs.writeKVCount, fs.writeSize)
			}
		}
	}

	applyWg.Wait()
	for _, fwt := range tableMapFiles {
		for _, fs := range fwt.regionMapFiles {
			for _, d := range fs.deleteFiles {
				tmpFiles = append(tmpFiles, d)
				tmpSize += d.GetLength()
				tmpKVCount += d.GetNumberOfEntries()

				if len(tmpFiles) >= batchCount || tmpSize >= batchSize {
					applyFunc(tmpFiles, tmpKVCount, tmpSize)
					tmpFiles = make([]*LogDataFileInfo, 0, batchCount)
					tmpSize = 0
					tmpKVCount = 0
				}
			}
			if len(tmpFiles) > 0 {
				applyFunc(tmpFiles, tmpKVCount, tmpSize)
				tmpFiles = make([]*LogDataFileInfo, 0, batchCount)
				tmpSize = 0
				tmpKVCount = 0
			}
		}
	}

	return nil
}

func ApplyKVFilesWithSingelMethod(
	ctx context.Context,
	files LogIter,
	applyFunc func(file []*LogDataFileInfo, kvCount int64, size uint64),
	applyWg *sync.WaitGroup,
) error {
	deleteKVFiles := make([]*LogDataFileInfo, 0)

	for r := files.TryNext(ctx); !r.Finished; r = files.TryNext(ctx) {
		if r.Err != nil {
			return r.Err
		}

		f := r.Item
		if f.GetType() == backuppb.FileType_Delete {
			deleteKVFiles = append(deleteKVFiles, f)
			continue
		}
		applyFunc([]*LogDataFileInfo{f}, f.GetNumberOfEntries(), f.GetLength())
	}

	applyWg.Wait()
	log.Info("restore delete files", zap.Int("count", len(deleteKVFiles)))
	for _, file := range deleteKVFiles {
		f := file
		applyFunc([]*LogDataFileInfo{f}, f.GetNumberOfEntries(), f.GetLength())
	}

	return nil
}

func (rc *LogClient) RestoreKVFiles(
	ctx context.Context,
	rules map[int64]*restoreutils.RewriteRules,
	idrules map[int64]int64,
	logIter LogIter,
	runner *checkpoint.CheckpointRunner[checkpoint.LogRestoreKeyType, checkpoint.LogRestoreValueType],
	pitrBatchCount uint32,
	pitrBatchSize uint32,
	updateStats func(kvCount uint64, size uint64),
	onProgress func(cnt int64),
) error {
	var (
		err          error
		fileCount    = 0
		start        = time.Now()
		supportBatch = version.CheckPITRSupportBatchKVFiles()
		skipFile     = 0
	)
	defer func() {
		if err == nil {
			elapsed := time.Since(start)
			log.Info("Restore KV files", zap.Duration("take", elapsed))
			summary.CollectSuccessUnit("files", fileCount, elapsed)
		}
	}()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.RestoreKVFiles", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	var applyWg sync.WaitGroup
	eg, ectx := errgroup.WithContext(ctx)
	applyFunc := func(files []*LogDataFileInfo, kvCount int64, size uint64) {
		if len(files) == 0 {
			return
		}
		// get rewrite rule from table id.
		// because the tableID of files is the same.
		rule, ok := rules[files[0].TableId]
		if !ok {
			// TODO handle new created table
			// For this version we do not handle new created table after full backup.
			// in next version we will perform rewrite and restore meta key to restore new created tables.
			// so we can simply skip the file that doesn't have the rule here.
			onProgress(int64(len(files)))
			summary.CollectInt("FileSkip", len(files))
			log.Debug("skip file due to table id not matched", zap.Int64("table-id", files[0].TableId))
			skipFile += len(files)
		} else {
			applyWg.Add(1)
			downstreamId := idrules[files[0].TableId]
			rc.workerPool.ApplyOnErrorGroup(eg, func() (err error) {
				fileStart := time.Now()
				defer applyWg.Done()
				defer func() {
					onProgress(int64(len(files)))
					updateStats(uint64(kvCount), size)
					summary.CollectInt("File", len(files))

					if err == nil {
						filenames := make([]string, 0, len(files))
						if runner == nil {
							for _, f := range files {
								filenames = append(filenames, f.Path+", ")
							}
						} else {
							for _, f := range files {
								filenames = append(filenames, f.Path+", ")
								if e := checkpoint.AppendRangeForLogRestore(ectx, runner, f.MetaDataGroupName, downstreamId, f.OffsetInMetaGroup, f.OffsetInMergedGroup); e != nil {
									err = errors.Annotate(e, "failed to append checkpoint data")
									break
								}
							}
						}
						log.Info("import files done", zap.Int("batch-count", len(files)), zap.Uint64("batch-size", size),
							zap.Duration("take", time.Since(fileStart)), zap.Strings("files", filenames))
					}
				}()

				return rc.fileImporter.ImportKVFiles(ectx, files, rule, rc.shiftStartTS, rc.startTS, rc.restoreTS, supportBatch)
			})
		}
	}

	rc.workerPool.ApplyOnErrorGroup(eg, func() error {
		if supportBatch {
			err = ApplyKVFilesWithBatchMethod(ectx, logIter, int(pitrBatchCount), uint64(pitrBatchSize), applyFunc, &applyWg)
		} else {
			err = ApplyKVFilesWithSingelMethod(ectx, logIter, applyFunc, &applyWg)
		}
		return errors.Trace(err)
	})

	if err = eg.Wait(); err != nil {
		summary.CollectFailureUnit("file", err)
		log.Error("restore files failed", zap.Error(err))
	}

	log.Info("total skip files due to table id not matched", zap.Int("count", skipFile))
	if skipFile > 0 {
		log.Debug("table id in full backup storage", zap.Any("tables", rules))
	}

	return errors.Trace(err)
}

func (rc *LogClient) initSchemasMap(
	ctx context.Context,
	clusterID uint64,
	restoreTS uint64,
) ([]*backuppb.PitrDBMap, error) {
	filename := metautil.PitrIDMapsFilename(clusterID, restoreTS)
	exist, err := rc.storage.FileExists(ctx, filename)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to check filename:%s ", filename)
	} else if !exist {
		log.Info("pitr id maps isn't existed", zap.String("file", filename))
		return nil, nil
	}

	metaData, err := rc.storage.ReadFile(ctx, filename)
	if err != nil {
		return nil, errors.Trace(err)
	}
	backupMeta := &backuppb.BackupMeta{}
	if err = backupMeta.Unmarshal(metaData); err != nil {
		return nil, errors.Trace(err)
	}

	return backupMeta.GetDbMaps(), nil
}

func initFullBackupTables(
	ctx context.Context,
	s storage.ExternalStorage,
	tableFilter filter.Filter,
) (map[int64]*metautil.Table, error) {
	metaData, err := s.ReadFile(ctx, metautil.MetaFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	backupMeta := &backuppb.BackupMeta{}
	if err = backupMeta.Unmarshal(metaData); err != nil {
		return nil, errors.Trace(err)
	}

	// read full backup databases to get map[table]table.Info
	reader := metautil.NewMetaReader(backupMeta, s, nil)

	databases, err := metautil.LoadBackupTables(ctx, reader, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tables := make(map[int64]*metautil.Table)
	for _, db := range databases {
		dbName := db.Info.Name.O
		if name, ok := utils.GetSysDBName(db.Info.Name); utils.IsSysDB(name) && ok {
			dbName = name
		}

		if !tableFilter.MatchSchema(dbName) {
			continue
		}

		for _, table := range db.Tables {
			// check this db is empty.
			if table.Info == nil {
				tables[db.Info.ID] = table
				continue
			}
			if !tableFilter.MatchTable(dbName, table.Info.Name.O) {
				continue
			}
			tables[table.Info.ID] = table
		}
	}

	return tables, nil
}

type FullBackupStorageConfig struct {
	Backend *backuppb.StorageBackend
	Opts    *storage.ExternalStorageOptions
}

type InitSchemaConfig struct {
	// required
	IsNewTask      bool
	HasFullRestore bool
	TableFilter    filter.Filter

	// optional
	TiFlashRecorder   *tiflashrec.TiFlashRecorder
	FullBackupStorage *FullBackupStorageConfig
}

// InitSchemasReplaceForDDL gets schemas information Mapping from old schemas to new schemas.
// It is used to rewrite meta kv-event.
func (rc *LogClient) InitSchemasReplaceForDDL(
	ctx context.Context,
	cfg *InitSchemaConfig,
) (*stream.SchemasReplace, error) {
	var (
		err    error
		dbMaps []*backuppb.PitrDBMap
		// the id map doesn't need to construct only when it is not the first execution
		needConstructIdMap bool

		dbReplaces = make(map[stream.UpstreamID]*stream.DBReplace)
	)

	// not new task, load schemas map from external storage
	if !cfg.IsNewTask {
		log.Info("try to load pitr id maps")
		needConstructIdMap = false
		dbMaps, err = rc.initSchemasMap(ctx, rc.GetClusterID(ctx), rc.restoreTS)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// a new task, but without full snapshot restore, tries to load
	// schemas map whose `restore-ts`` is the task's `start-ts`.
	if len(dbMaps) <= 0 && !cfg.HasFullRestore {
		log.Info("try to load pitr id maps of the previous task", zap.Uint64("start-ts", rc.startTS))
		needConstructIdMap = true
		dbMaps, err = rc.initSchemasMap(ctx, rc.GetClusterID(ctx), rc.startTS)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if len(dbMaps) <= 0 {
		log.Info("no id maps, build the table replaces from cluster and full backup schemas")
		needConstructIdMap = true
		s, err := storage.New(ctx, cfg.FullBackupStorage.Backend, cfg.FullBackupStorage.Opts)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fullBackupTables, err := initFullBackupTables(ctx, s, cfg.TableFilter)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, t := range fullBackupTables {
			dbName, _ := utils.GetSysDBCIStrName(t.DB.Name)
			newDBInfo, exist := rc.dom.InfoSchema().SchemaByName(dbName)
			if !exist {
				log.Info("db not existed", zap.String("dbname", dbName.String()))
				continue
			}

			dbReplace, exist := dbReplaces[t.DB.ID]
			if !exist {
				dbReplace = stream.NewDBReplace(t.DB.Name.O, newDBInfo.ID)
				dbReplaces[t.DB.ID] = dbReplace
			}

			if t.Info == nil {
				// If the db is empty, skip it.
				continue
			}
			newTableInfo, err := restore.GetTableSchema(rc.GetDomain(), dbName, t.Info.Name)
			if err != nil {
				log.Info("table not existed", zap.String("tablename", dbName.String()+"."+t.Info.Name.String()))
				continue
			}

			dbReplace.TableMap[t.Info.ID] = &stream.TableReplace{
				Name:         newTableInfo.Name.O,
				TableID:      newTableInfo.ID,
				PartitionMap: restoreutils.GetPartitionIDMap(newTableInfo, t.Info),
				IndexMap:     restoreutils.GetIndexIDMap(newTableInfo, t.Info),
			}
		}
	} else {
		dbReplaces = stream.FromSchemaMaps(dbMaps)
	}

	for oldDBID, dbReplace := range dbReplaces {
		log.Info("replace info", func() []zapcore.Field {
			fields := make([]zapcore.Field, 0, (len(dbReplace.TableMap)+1)*3)
			fields = append(fields,
				zap.String("dbName", dbReplace.Name),
				zap.Int64("oldID", oldDBID),
				zap.Int64("newID", dbReplace.DbID))
			for oldTableID, tableReplace := range dbReplace.TableMap {
				fields = append(fields,
					zap.String("table", tableReplace.Name),
					zap.Int64("oldID", oldTableID),
					zap.Int64("newID", tableReplace.TableID))
			}
			return fields
		}()...)
	}

	rp := stream.NewSchemasReplace(
		dbReplaces, needConstructIdMap, cfg.TiFlashRecorder, rc.currentTS, cfg.TableFilter, rc.GenGlobalID, rc.GenGlobalIDs,
		rc.RecordDeleteRange)
	return rp, nil
}

func SortMetaKVFiles(files []*backuppb.DataFileInfo) []*backuppb.DataFileInfo {
	slices.SortFunc(files, func(i, j *backuppb.DataFileInfo) int {
		if c := cmp.Compare(i.GetMinTs(), j.GetMinTs()); c != 0 {
			return c
		}
		if c := cmp.Compare(i.GetMaxTs(), j.GetMaxTs()); c != 0 {
			return c
		}
		return cmp.Compare(i.GetResolvedTs(), j.GetResolvedTs())
	})
	return files
}

// RestoreMetaKVFiles tries to restore files about meta kv-event from stream-backup.
func (rc *LogClient) RestoreMetaKVFiles(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
	schemasReplace *stream.SchemasReplace,
	updateStats func(kvCount uint64, size uint64),
	progressInc func(),
) error {
	filesInWriteCF := make([]*backuppb.DataFileInfo, 0, len(files))
	filesInDefaultCF := make([]*backuppb.DataFileInfo, 0, len(files))

	// The k-v events in default CF should be restored firstly. The reason is that:
	// The error of transactions of meta could happen if restore write CF events successfully,
	// but failed to restore default CF events.
	for _, f := range files {
		if f.Cf == stream.WriteCF {
			filesInWriteCF = append(filesInWriteCF, f)
			continue
		}
		if f.Type == backuppb.FileType_Delete {
			// this should happen abnormally.
			// only do some preventive checks here.
			log.Warn("detected delete file of meta key, skip it", zap.Any("file", f))
			continue
		}
		if f.Cf == stream.DefaultCF {
			filesInDefaultCF = append(filesInDefaultCF, f)
		}
	}
	filesInDefaultCF = SortMetaKVFiles(filesInDefaultCF)
	filesInWriteCF = SortMetaKVFiles(filesInWriteCF)

	failpoint.Inject("failed-before-id-maps-saved", func(_ failpoint.Value) {
		failpoint.Return(errors.New("failpoint: failed before id maps saved"))
	})

	log.Info("start to restore meta files",
		zap.Int("total files", len(files)),
		zap.Int("default files", len(filesInDefaultCF)),
		zap.Int("write files", len(filesInWriteCF)))

	if schemasReplace.NeedConstructIdMap() {
		// Preconstruct the map and save it into external storage.
		if err := rc.PreConstructAndSaveIDMap(
			ctx,
			filesInWriteCF,
			filesInDefaultCF,
			schemasReplace,
		); err != nil {
			return errors.Trace(err)
		}
	}
	failpoint.Inject("failed-after-id-maps-saved", func(_ failpoint.Value) {
		failpoint.Return(errors.New("failpoint: failed after id maps saved"))
	})

	// run the rewrite and restore meta-kv into TiKV cluster.
	if err := RestoreMetaKVFilesWithBatchMethod(
		ctx,
		filesInDefaultCF,
		filesInWriteCF,
		schemasReplace,
		updateStats,
		progressInc,
		rc.RestoreBatchMetaKVFiles,
	); err != nil {
		return errors.Trace(err)
	}

	// Update global schema version and report all of TiDBs.
	if err := rc.UpdateSchemaVersion(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// PreConstructAndSaveIDMap constructs id mapping and save it.
func (rc *LogClient) PreConstructAndSaveIDMap(
	ctx context.Context,
	fsInWriteCF, fsInDefaultCF []*backuppb.DataFileInfo,
	sr *stream.SchemasReplace,
) error {
	sr.SetPreConstructMapStatus()

	if err := rc.constructIDMap(ctx, fsInWriteCF, sr); err != nil {
		return errors.Trace(err)
	}
	if err := rc.constructIDMap(ctx, fsInDefaultCF, sr); err != nil {
		return errors.Trace(err)
	}

	if err := rc.SaveIDMap(ctx, sr); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (rc *LogClient) constructIDMap(
	ctx context.Context,
	fs []*backuppb.DataFileInfo,
	sr *stream.SchemasReplace,
) error {
	for _, f := range fs {
		entries, _, err := rc.ReadAllEntries(ctx, f, math.MaxUint64)
		if err != nil {
			return errors.Trace(err)
		}

		for _, entry := range entries {
			if _, err := sr.RewriteKvEntry(&entry.E, f.GetCf()); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func RestoreMetaKVFilesWithBatchMethod(
	ctx context.Context,
	defaultFiles []*backuppb.DataFileInfo,
	writeFiles []*backuppb.DataFileInfo,
	schemasReplace *stream.SchemasReplace,
	updateStats func(kvCount uint64, size uint64),
	progressInc func(),
	restoreBatch func(
		ctx context.Context,
		files []*backuppb.DataFileInfo,
		schemasReplace *stream.SchemasReplace,
		kvEntries []*KvEntryWithTS,
		filterTS uint64,
		updateStats func(kvCount uint64, size uint64),
		progressInc func(),
		cf string,
	) ([]*KvEntryWithTS, error),
) error {
	// the average size of each KV is 2560 Bytes
	// kvEntries is kvs left by the previous batch
	const kvSize = 2560
	var (
		rangeMin uint64
		rangeMax uint64
		err      error

		batchSize  uint64 = 0
		defaultIdx int    = 0
		writeIdx   int    = 0

		defaultKvEntries = make([]*KvEntryWithTS, 0)
		writeKvEntries   = make([]*KvEntryWithTS, 0)
	)
	// Set restoreKV to SchemaReplace.
	schemasReplace.SetRestoreKVStatus()

	for i, f := range defaultFiles {
		if i == 0 {
			rangeMax = f.MaxTs
			rangeMin = f.MinTs
			batchSize = f.Length
		} else {
			if f.MinTs <= rangeMax && batchSize+f.Length <= MetaKVBatchSize {
				rangeMin = min(rangeMin, f.MinTs)
				rangeMax = max(rangeMax, f.MaxTs)
				batchSize += f.Length
			} else {
				// Either f.MinTS > rangeMax or f.MinTs is the filterTs we need.
				// So it is ok to pass f.MinTs as filterTs.
				defaultKvEntries, err = restoreBatch(ctx, defaultFiles[defaultIdx:i], schemasReplace, defaultKvEntries, f.MinTs, updateStats, progressInc, stream.DefaultCF)
				if err != nil {
					return errors.Trace(err)
				}
				defaultIdx = i
				rangeMin = f.MinTs
				rangeMax = f.MaxTs
				// the initial batch size is the size of left kvs and the current file length.
				batchSize = uint64(len(defaultKvEntries)*kvSize) + f.Length

				// restore writeCF kv to f.MinTs
				var toWriteIdx int
				for toWriteIdx = writeIdx; toWriteIdx < len(writeFiles); toWriteIdx++ {
					if writeFiles[toWriteIdx].MinTs >= f.MinTs {
						break
					}
				}
				writeKvEntries, err = restoreBatch(ctx, writeFiles[writeIdx:toWriteIdx], schemasReplace, writeKvEntries, f.MinTs, updateStats, progressInc, stream.WriteCF)
				if err != nil {
					return errors.Trace(err)
				}
				writeIdx = toWriteIdx
			}
		}
	}

	// restore the left meta kv files and entries
	// Notice: restoreBatch needs to realize the parameter `files` and `kvEntries` might be empty
	// Assert: defaultIdx <= len(defaultFiles) && writeIdx <= len(writeFiles)
	_, err = restoreBatch(ctx, defaultFiles[defaultIdx:], schemasReplace, defaultKvEntries, math.MaxUint64, updateStats, progressInc, stream.DefaultCF)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = restoreBatch(ctx, writeFiles[writeIdx:], schemasReplace, writeKvEntries, math.MaxUint64, updateStats, progressInc, stream.WriteCF)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (rc *LogClient) RestoreBatchMetaKVFiles(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
	schemasReplace *stream.SchemasReplace,
	kvEntries []*KvEntryWithTS,
	filterTS uint64,
	updateStats func(kvCount uint64, size uint64),
	progressInc func(),
	cf string,
) ([]*KvEntryWithTS, error) {
	nextKvEntries := make([]*KvEntryWithTS, 0)
	curKvEntries := make([]*KvEntryWithTS, 0)
	if len(files) == 0 && len(kvEntries) == 0 {
		return nextKvEntries, nil
	}

	// filter the kv from kvEntries again.
	for _, kv := range kvEntries {
		if kv.Ts < filterTS {
			curKvEntries = append(curKvEntries, kv)
		} else {
			nextKvEntries = append(nextKvEntries, kv)
		}
	}

	// read all of entries from files.
	for _, f := range files {
		es, nextEs, err := rc.ReadAllEntries(ctx, f, filterTS)
		if err != nil {
			return nextKvEntries, errors.Trace(err)
		}

		curKvEntries = append(curKvEntries, es...)
		nextKvEntries = append(nextKvEntries, nextEs...)
	}

	// sort these entries.
	slices.SortFunc(curKvEntries, func(i, j *KvEntryWithTS) int {
		return cmp.Compare(i.Ts, j.Ts)
	})

	// restore these entries with rawPut() method.
	kvCount, size, err := rc.restoreMetaKvEntries(ctx, schemasReplace, curKvEntries, cf)
	if err != nil {
		return nextKvEntries, errors.Trace(err)
	}

	if schemasReplace.IsRestoreKVStatus() {
		updateStats(kvCount, size)
		for i := 0; i < len(files); i++ {
			progressInc()
		}
	}
	return nextKvEntries, nil
}

func (rc *LogClient) restoreMetaKvEntries(
	ctx context.Context,
	sr *stream.SchemasReplace,
	entries []*KvEntryWithTS,
	columnFamily string,
) (uint64, uint64, error) {
	var (
		kvCount uint64
		size    uint64
	)

	rc.rawKVClient.SetColumnFamily(columnFamily)

	for _, entry := range entries {
		log.Debug("before rewrte entry", zap.Uint64("key-ts", entry.Ts), zap.Int("key-len", len(entry.E.Key)),
			zap.Int("value-len", len(entry.E.Value)), zap.ByteString("key", entry.E.Key))

		newEntry, err := sr.RewriteKvEntry(&entry.E, columnFamily)
		if err != nil {
			log.Error("rewrite txn entry failed", zap.Int("klen", len(entry.E.Key)),
				logutil.Key("txn-key", entry.E.Key))
			return 0, 0, errors.Trace(err)
		} else if newEntry == nil {
			continue
		}
		log.Debug("after rewrite entry", zap.Int("new-key-len", len(newEntry.Key)),
			zap.Int("new-value-len", len(entry.E.Value)), zap.ByteString("new-key", newEntry.Key))

		failpoint.Inject("failed-to-restore-metakv", func(_ failpoint.Value) {
			failpoint.Return(0, 0, errors.Errorf("failpoint: failed to restore metakv"))
		})
		if err := rc.rawKVClient.Put(ctx, newEntry.Key, newEntry.Value, entry.Ts); err != nil {
			return 0, 0, errors.Trace(err)
		}
		// for failpoint, we need to flush the cache in rawKVClient every time
		failpoint.Inject("do-not-put-metakv-in-batch", func(_ failpoint.Value) {
			if err := rc.rawKVClient.PutRest(ctx); err != nil {
				failpoint.Return(0, 0, errors.Trace(err))
			}
		})
		kvCount++
		size += uint64(len(newEntry.Key) + len(newEntry.Value))
	}

	return kvCount, size, rc.rawKVClient.PutRest(ctx)
}

// GenGlobalID generates a global id by transaction way.
func (rc *LogClient) GenGlobalID(ctx context.Context) (int64, error) {
	var id int64
	storage := rc.GetDomain().Store()

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	err := kv.RunInNewTxn(
		ctx,
		storage,
		true,
		func(ctx context.Context, txn kv.Transaction) error {
			var e error
			t := meta.NewMeta(txn)
			id, e = t.GenGlobalID()
			return e
		})

	return id, err
}

// GenGlobalIDs generates several global ids by transaction way.
func (rc *LogClient) GenGlobalIDs(ctx context.Context, n int) ([]int64, error) {
	ids := make([]int64, 0)
	storage := rc.GetDomain().Store()

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	err := kv.RunInNewTxn(
		ctx,
		storage,
		true,
		func(ctx context.Context, txn kv.Transaction) error {
			var e error
			t := meta.NewMeta(txn)
			ids, e = t.GenGlobalIDs(n)
			return e
		})

	return ids, err
}

// UpdateSchemaVersion updates schema version by transaction way.
func (rc *LogClient) UpdateSchemaVersion(ctx context.Context) error {
	storage := rc.GetDomain().Store()
	var schemaVersion int64

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	if err := kv.RunInNewTxn(
		ctx,
		storage,
		true,
		func(ctx context.Context, txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			var e error
			// To trigger full-reload instead of diff-reload, we need to increase the schema version
			// by at least `domain.LoadSchemaDiffVersionGapThreshold`.
			schemaVersion, e = t.GenSchemaVersions(64 + domain.LoadSchemaDiffVersionGapThreshold)
			if e != nil {
				return e
			}
			// add the diff key so that the domain won't retry to reload the schemas with `schemaVersion` frequently.
			return t.SetSchemaDiff(&model.SchemaDiff{
				Version:             schemaVersion,
				Type:                model.ActionNone,
				SchemaID:            -1,
				TableID:             -1,
				RegenerateSchemaMap: true,
			})
		},
	); err != nil {
		return errors.Trace(err)
	}

	log.Info("update global schema version", zap.Int64("global-schema-version", schemaVersion))

	ver := strconv.FormatInt(schemaVersion, 10)
	if err := ddlutil.PutKVToEtcd(
		ctx,
		rc.GetDomain().GetEtcdClient(),
		math.MaxInt,
		ddlutil.DDLGlobalSchemaVersion,
		ver,
	); err != nil {
		return errors.Annotatef(err, "failed to put global schema verson %v to etcd", ver)
	}

	return nil
}

func (rc *LogClient) WrapLogFilesIterWithSplitHelper(logIter LogIter, rules map[int64]*restoreutils.RewriteRules, g glue.Glue, store kv.Storage) (LogIter, error) {
	se, err := g.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	execCtx := se.GetSessionCtx().GetRestrictedSQLExecutor()
	splitSize, splitKeys := utils.GetRegionSplitInfo(execCtx)
	log.Info("get split threshold from tikv config", zap.Uint64("split-size", splitSize), zap.Int64("split-keys", splitKeys))
	client := split.NewClient(rc.pdClient, rc.pdHTTPClient, rc.tlsConf, maxSplitKeysOnce, 3)
	return NewLogFilesIterWithSplitHelper(logIter, rules, client, splitSize, splitKeys), nil
}

func (rc *LogClient) generateKvFilesSkipMap(ctx context.Context, downstreamIdset map[int64]struct{}, taskName string) (*LogFilesSkipMap, error) {
	skipMap := NewLogFilesSkipMap()
	t, err := checkpoint.WalkCheckpointFileForRestore(ctx, rc.storage, rc.cipher, taskName, func(groupKey checkpoint.LogRestoreKeyType, off checkpoint.LogRestoreValueMarshaled) {
		for tableID, foffs := range off.Foffs {
			// filter out the checkpoint data of dropped table
			if _, exists := downstreamIdset[tableID]; exists {
				for _, foff := range foffs {
					skipMap.Insert(groupKey, off.Goff, foff)
				}
			}
		}
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	summary.AdjustStartTimeToEarlierTime(t)
	return skipMap, nil
}

func (rc *LogClient) WrapLogFilesIterWithCheckpoint(
	ctx context.Context,
	logIter LogIter,
	downstreamIdset map[int64]struct{},
	taskName string,
	updateStats func(kvCount, size uint64),
	onProgress func(),
) (LogIter, error) {
	skipMap, err := rc.generateKvFilesSkipMap(ctx, downstreamIdset, taskName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return iter.FilterOut(logIter, func(d *LogDataFileInfo) bool {
		if skipMap.NeedSkip(d.MetaDataGroupName, d.OffsetInMetaGroup, d.OffsetInMergedGroup) {
			onProgress()
			updateStats(uint64(d.NumberOfEntries), d.Length)
			return true
		}
		return false
	}), nil
}

const (
	alterTableDropIndexSQL         = "ALTER TABLE %n.%n DROP INDEX %n"
	alterTableAddIndexFormat       = "ALTER TABLE %%n.%%n ADD INDEX %%n(%s)"
	alterTableAddUniqueIndexFormat = "ALTER TABLE %%n.%%n ADD UNIQUE KEY %%n(%s)"
	alterTableAddPrimaryFormat     = "ALTER TABLE %%n.%%n ADD PRIMARY KEY (%s) NONCLUSTERED"
)

func (rc *LogClient) generateRepairIngestIndexSQLs(
	ctx context.Context,
	ingestRecorder *ingestrec.IngestRecorder,
	allSchema []*model.DBInfo,
	taskName string,
) ([]checkpoint.CheckpointIngestIndexRepairSQL, bool, error) {
	var sqls []checkpoint.CheckpointIngestIndexRepairSQL
	if rc.useCheckpoint {
		exists, err := checkpoint.ExistsCheckpointIngestIndexRepairSQLs(ctx, rc.storage, taskName)
		if err != nil {
			return sqls, false, errors.Trace(err)
		}
		if exists {
			checkpointSQLs, err := checkpoint.LoadCheckpointIngestIndexRepairSQLs(ctx, rc.storage, taskName)
			if err != nil {
				return sqls, false, errors.Trace(err)
			}
			sqls = checkpointSQLs.SQLs
			log.Info("load ingest index repair sqls from checkpoint", zap.String("category", "ingest"), zap.Reflect("sqls", sqls))
			return sqls, true, nil
		}
	}

	ingestRecorder.UpdateIndexInfo(allSchema)
	if err := ingestRecorder.Iterate(func(_, indexID int64, info *ingestrec.IngestIndexInfo) error {
		var (
			addSQL  strings.Builder
			addArgs []any = make([]any, 0, 5+len(info.ColumnArgs))
		)
		if info.IsPrimary {
			addSQL.WriteString(fmt.Sprintf(alterTableAddPrimaryFormat, info.ColumnList))
			addArgs = append(addArgs, info.SchemaName.O, info.TableName.O)
			addArgs = append(addArgs, info.ColumnArgs...)
		} else if info.IndexInfo.Unique {
			addSQL.WriteString(fmt.Sprintf(alterTableAddUniqueIndexFormat, info.ColumnList))
			addArgs = append(addArgs, info.SchemaName.O, info.TableName.O, info.IndexInfo.Name.O)
			addArgs = append(addArgs, info.ColumnArgs...)
		} else {
			addSQL.WriteString(fmt.Sprintf(alterTableAddIndexFormat, info.ColumnList))
			addArgs = append(addArgs, info.SchemaName.O, info.TableName.O, info.IndexInfo.Name.O)
			addArgs = append(addArgs, info.ColumnArgs...)
		}
		// USING BTREE/HASH/RTREE
		indexTypeStr := info.IndexInfo.Tp.String()
		if len(indexTypeStr) > 0 {
			addSQL.WriteString(" USING ")
			addSQL.WriteString(indexTypeStr)
		}

		// COMMENT [...]
		if len(info.IndexInfo.Comment) > 0 {
			addSQL.WriteString(" COMMENT %?")
			addArgs = append(addArgs, info.IndexInfo.Comment)
		}

		if info.IndexInfo.Invisible {
			addSQL.WriteString(" INVISIBLE")
		} else {
			addSQL.WriteString(" VISIBLE")
		}

		sqls = append(sqls, checkpoint.CheckpointIngestIndexRepairSQL{
			IndexID:    indexID,
			SchemaName: info.SchemaName,
			TableName:  info.TableName,
			IndexName:  info.IndexInfo.Name.O,
			AddSQL:     addSQL.String(),
			AddArgs:    addArgs,
		})

		return nil
	}); err != nil {
		return sqls, false, errors.Trace(err)
	}

	if rc.useCheckpoint && len(sqls) > 0 {
		if err := checkpoint.SaveCheckpointIngestIndexRepairSQLs(ctx, rc.storage, &checkpoint.CheckpointIngestIndexRepairSQLs{
			SQLs: sqls,
		}, taskName); err != nil {
			return sqls, false, errors.Trace(err)
		}
	}
	return sqls, false, nil
}

// RepairIngestIndex drops the indexes from IngestRecorder and re-add them.
func (rc *LogClient) RepairIngestIndex(ctx context.Context, ingestRecorder *ingestrec.IngestRecorder, g glue.Glue, taskName string) error {
	info := rc.dom.InfoSchema()

	sqls, fromCheckpoint, err := rc.generateRepairIngestIndexSQLs(ctx, ingestRecorder, info.AllSchemas(), taskName)
	if err != nil {
		return errors.Trace(err)
	}

	console := glue.GetConsole(g)
NEXTSQL:
	for _, sql := range sqls {
		progressTitle := fmt.Sprintf("repair ingest index %s for table %s.%s", sql.IndexName, sql.SchemaName, sql.TableName)

		tableInfo, err := info.TableByName(sql.SchemaName, sql.TableName)
		if err != nil {
			return errors.Trace(err)
		}
		oldIndexIDFound := false
		if fromCheckpoint {
			for _, idx := range tableInfo.Indices() {
				indexInfo := idx.Meta()
				if indexInfo.ID == sql.IndexID {
					// the original index id is not dropped
					oldIndexIDFound = true
					break
				}
				// what if index's state is not public?
				if indexInfo.Name.O == sql.IndexName {
					// find the same name index, but not the same index id,
					// which means the repaired index id is created
					if _, err := fmt.Fprintf(console.Out(), "%s ... %s\n", progressTitle, color.HiGreenString("SKIPPED DUE TO CHECKPOINT MODE")); err != nil {
						return errors.Trace(err)
					}
					continue NEXTSQL
				}
			}
		}

		if err := func(sql checkpoint.CheckpointIngestIndexRepairSQL) error {
			w := console.StartProgressBar(progressTitle, glue.OnlyOneTask)
			defer w.Close()

			// TODO: When the TiDB supports the DROP and CREATE the same name index in one SQL,
			//   the checkpoint for ingest recorder can be removed and directly use the SQL:
			//      ALTER TABLE db.tbl DROP INDEX `i_1`, ADD IDNEX `i_1` ...
			//
			// This SQL is compatible with checkpoint: If one ingest index has been recreated by
			// the SQL, the index's id would be another one. In the next retry execution, BR can
			// not find the ingest index's dropped id so that BR regards it as a dropped index by
			// restored metakv and then skips repairing it.

			// only when first execution or old index id is not dropped
			if !fromCheckpoint || oldIndexIDFound {
				if err := rc.se.ExecuteInternal(ctx, alterTableDropIndexSQL, sql.SchemaName.O, sql.TableName.O, sql.IndexName); err != nil {
					return errors.Trace(err)
				}
			}
			failpoint.Inject("failed-before-create-ingest-index", func(v failpoint.Value) {
				if v != nil && v.(bool) {
					failpoint.Return(errors.New("failed before create ingest index"))
				}
			})
			// create the repaired index when first execution or not found it
			if err := rc.se.ExecuteInternal(ctx, sql.AddSQL, sql.AddArgs...); err != nil {
				return errors.Trace(err)
			}
			w.Inc()
			if err := w.Wait(ctx); err != nil {
				return errors.Trace(err)
			}
			return nil
		}(sql); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (rc *LogClient) RecordDeleteRange(sql *stream.PreDelRangeQuery) {
	rc.deleteRangeQueryCh <- sql
}

// use channel to save the delete-range query to make it thread-safety.
func (rc *LogClient) RunGCRowsLoader(ctx context.Context) {
	rc.deleteRangeQueryWaitGroup.Add(1)

	go func() {
		defer rc.deleteRangeQueryWaitGroup.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case query, ok := <-rc.deleteRangeQueryCh:
				if !ok {
					return
				}
				rc.deleteRangeQuery = append(rc.deleteRangeQuery, query)
			}
		}
	}()
}

// InsertGCRows insert the querys into table `gc_delete_range`
func (rc *LogClient) InsertGCRows(ctx context.Context) error {
	close(rc.deleteRangeQueryCh)
	rc.deleteRangeQueryWaitGroup.Wait()
	ts, err := restore.GetTSWithRetry(ctx, rc.pdClient)
	if err != nil {
		return errors.Trace(err)
	}
	jobIDMap := make(map[int64]int64)
	for _, query := range rc.deleteRangeQuery {
		paramsList := make([]any, 0, len(query.ParamsList)*5)
		for _, params := range query.ParamsList {
			newJobID, exists := jobIDMap[params.JobID]
			if !exists {
				newJobID, err = rc.GenGlobalID(ctx)
				if err != nil {
					return errors.Trace(err)
				}
				jobIDMap[params.JobID] = newJobID
			}
			log.Info("insert into the delete range",
				zap.Int64("jobID", newJobID),
				zap.Int64("elemID", params.ElemID),
				zap.String("startKey", params.StartKey),
				zap.String("endKey", params.EndKey),
				zap.Uint64("ts", ts))
			// (job_id, elem_id, start_key, end_key, ts)
			paramsList = append(paramsList, newJobID, params.ElemID, params.StartKey, params.EndKey, ts)
		}
		if len(paramsList) > 0 {
			// trim the ',' behind the query.Sql if exists
			// that's when the rewrite rule of the last table id is not exist
			sql := strings.TrimSuffix(query.Sql, ",")
			if err := rc.se.ExecuteInternal(ctx, sql, paramsList...); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// only for unit test
func (rc *LogClient) GetGCRows() []*stream.PreDelRangeQuery {
	close(rc.deleteRangeQueryCh)
	rc.deleteRangeQueryWaitGroup.Wait()
	return rc.deleteRangeQuery
}

// SaveIDMap saves the id mapping information.
func (rc *LogClient) SaveIDMap(
	ctx context.Context,
	sr *stream.SchemasReplace,
) error {
	idMaps := sr.TidySchemaMaps()
	clusterID := rc.GetClusterID(ctx)
	metaFileName := metautil.PitrIDMapsFilename(clusterID, rc.restoreTS)
	metaWriter := metautil.NewMetaWriter(rc.storage, metautil.MetaFileSize, false, metaFileName, nil)
	metaWriter.Update(func(m *backuppb.BackupMeta) {
		// save log startTS to backupmeta file
		m.ClusterId = clusterID
		m.DbMaps = idMaps
	})

	if err := metaWriter.FlushBackupMeta(ctx); err != nil {
		return errors.Trace(err)
	}
	if rc.useCheckpoint {
		var items map[int64]model.TiFlashReplicaInfo
		if sr.TiflashRecorder != nil {
			items = sr.TiflashRecorder.GetItems()
		}
		log.Info("save checkpoint task info with InLogRestoreAndIdMapPersist status")
		if err := checkpoint.SaveCheckpointTaskInfoForLogRestore(ctx, rc.storage, &checkpoint.CheckpointTaskInfoForLogRestore{
			Progress:     checkpoint.InLogRestoreAndIdMapPersist,
			StartTS:      rc.startTS,
			RestoreTS:    rc.restoreTS,
			RewriteTS:    rc.currentTS,
			TiFlashItems: items,
		}, rc.GetClusterID(ctx)); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// called by failpoint, only used for test
// it would print the checksum result into the log, and
// the auto-test script records them to compare another
// cluster's checksum.
func (rc *LogClient) FailpointDoChecksumForLogRestore(
	ctx context.Context,
	kvClient kv.Client,
	pdClient pd.Client,
	idrules map[int64]int64,
	rewriteRules map[int64]*restoreutils.RewriteRules,
) (finalErr error) {
	startTS, err := restore.GetTSWithRetry(ctx, rc.pdClient)
	if err != nil {
		return errors.Trace(err)
	}
	// set gc safepoint for checksum
	sp := utils.BRServiceSafePoint{
		BackupTS: startTS,
		TTL:      utils.DefaultBRGCSafePointTTL,
		ID:       utils.MakeSafePointID(),
	}
	cctx, gcSafePointKeeperCancel := context.WithCancel(ctx)
	defer func() {
		log.Info("start to remove gc-safepoint keeper")
		// close the gc safe point keeper at first
		gcSafePointKeeperCancel()
		// set the ttl to 0 to remove the gc-safe-point
		sp.TTL = 0
		if err := utils.UpdateServiceSafePoint(ctx, pdClient, sp); err != nil {
			log.Warn("failed to update service safe point, backup may fail if gc triggered",
				zap.Error(err),
			)
		}
		log.Info("finish removing gc-safepoint keeper")
	}()
	err = utils.StartServiceSafePointKeeper(cctx, pdClient, sp)
	if err != nil {
		return errors.Trace(err)
	}

	eg, ectx := errgroup.WithContext(ctx)
	pool := tidbutil.NewWorkerPool(4, "checksum for log restore")
	infoSchema := rc.GetDomain().InfoSchema()
	// downstream id -> upstream id
	reidRules := make(map[int64]int64)
	for upstreamID, downstreamID := range idrules {
		reidRules[downstreamID] = upstreamID
	}
	for upstreamID, downstreamID := range idrules {
		newTable, ok := infoSchema.TableByID(downstreamID)
		if !ok {
			// a dropped table
			continue
		}
		rewriteRule, ok := rewriteRules[upstreamID]
		if !ok {
			continue
		}
		newTableInfo := newTable.Meta()
		var definitions []model.PartitionDefinition
		if newTableInfo.Partition != nil {
			for _, def := range newTableInfo.Partition.Definitions {
				upid, ok := reidRules[def.ID]
				if !ok {
					log.Panic("no rewrite rule for parition table id", zap.Int64("id", def.ID))
				}
				definitions = append(definitions, model.PartitionDefinition{
					ID: upid,
				})
			}
		}
		oldPartition := &model.PartitionInfo{
			Definitions: definitions,
		}
		oldTable := &metautil.Table{
			Info: &model.TableInfo{
				ID:        upstreamID,
				Indices:   newTableInfo.Indices,
				Partition: oldPartition,
			},
		}
		pool.ApplyOnErrorGroup(eg, func() error {
			exe, err := checksum.NewExecutorBuilder(newTableInfo, startTS).
				SetOldTable(oldTable).
				SetConcurrency(4).
				SetOldKeyspace(rewriteRule.OldKeyspace).
				SetNewKeyspace(rewriteRule.NewKeyspace).
				SetExplicitRequestSourceType(kvutil.ExplicitTypeBR).
				Build()
			if err != nil {
				return errors.Trace(err)
			}
			checksumResp, err := exe.Execute(ectx, kvClient, func() {})
			if err != nil {
				return errors.Trace(err)
			}
			// print to log so that the test script can get final checksum
			log.Info("failpoint checksum completed",
				zap.String("table-name", newTableInfo.Name.O),
				zap.Int64("upstream-id", oldTable.Info.ID),
				zap.Uint64("checksum", checksumResp.Checksum),
				zap.Uint64("total-kvs", checksumResp.TotalKvs),
				zap.Uint64("total-bytes", checksumResp.TotalBytes),
			)
			return nil
		})
	}

	return eg.Wait()
}

type LogFilesIterWithSplitHelper struct {
	iter   LogIter
	helper *logsplit.LogSplitHelper
	buffer []*LogDataFileInfo
	next   int
}

const SplitFilesBufferSize = 4096

func NewLogFilesIterWithSplitHelper(iter LogIter, rules map[int64]*restoreutils.RewriteRules, client split.SplitClient, splitSize uint64, splitKeys int64) LogIter {
	return &LogFilesIterWithSplitHelper{
		iter:   iter,
		helper: logsplit.NewLogSplitHelper(rules, client, splitSize, splitKeys),
		buffer: nil,
		next:   0,
	}
}

func (splitIter *LogFilesIterWithSplitHelper) TryNext(ctx context.Context) iter.IterResult[*LogDataFileInfo] {
	if splitIter.next >= len(splitIter.buffer) {
		splitIter.buffer = make([]*LogDataFileInfo, 0, SplitFilesBufferSize)
		for r := splitIter.iter.TryNext(ctx); !r.Finished; r = splitIter.iter.TryNext(ctx) {
			if r.Err != nil {
				return r
			}
			f := r.Item
			splitIter.helper.Merge(f.DataFileInfo)
			splitIter.buffer = append(splitIter.buffer, f)
			if len(splitIter.buffer) >= SplitFilesBufferSize {
				break
			}
		}
		splitIter.next = 0
		if len(splitIter.buffer) == 0 {
			return iter.Done[*LogDataFileInfo]()
		}
		log.Info("start to split the regions")
		startTime := time.Now()
		if err := splitIter.helper.Split(ctx); err != nil {
			return iter.Throw[*LogDataFileInfo](errors.Trace(err))
		}
		log.Info("end to split the regions", zap.Duration("takes", time.Since(startTime)))
	}

	res := iter.Emit(splitIter.buffer[splitIter.next])
	splitIter.next += 1
	return res
}
