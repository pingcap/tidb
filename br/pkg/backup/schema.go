// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"encoding/json"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultSchemaConcurrency is the default number of the concurrent
	// backup schema tasks.
	DefaultSchemaConcurrency = 64
)

type schemaInfo struct {
	tableInfo  *model.TableInfo
	dbInfo     *model.DBInfo
	crc64xor   uint64
	totalKvs   uint64
	totalBytes uint64
	stats      *util.JSONTable
	statsIndex []*backuppb.StatsFileIndex
}

type iterFuncTp func(kv.Storage, func(*model.DBInfo, *model.TableInfo)) error

// Schemas is task for backuping schemas.
type Schemas struct {
	iterFunc iterFuncTp

	size int

	// checkpoint: table id -> checksum
	checkpointChecksum map[int64]*checkpoint.ChecksumItem
}

func NewBackupSchemas(iterFunc iterFuncTp, size int) *Schemas {
	return &Schemas{
		iterFunc:           iterFunc,
		size:               size,
		checkpointChecksum: nil,
	}
}

func (ss *Schemas) SetCheckpointChecksum(checkpointChecksum map[int64]*checkpoint.ChecksumItem) {
	ss.checkpointChecksum = checkpointChecksum
}

// BackupSchemas backups table info, including checksum and stats.
func (ss *Schemas) BackupSchemas(
	ctx context.Context,
	metaWriter *metautil.MetaWriter,
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.BackupKeyType, checkpoint.BackupValueType],
	store kv.Storage,
	statsHandle *handle.Handle,
	backupTS uint64,
	concurrency uint,
	copConcurrency uint,
	skipChecksum bool,
	updateCh glue.Progress,
) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Schemas.BackupSchemas", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	workerPool := tidbutil.NewWorkerPool(concurrency, "Schemas")
	errg, ectx := errgroup.WithContext(ctx)
	startAll := time.Now()
	op := metautil.AppendSchema
	metaWriter.StartWriteMetasAsync(ctx, op)
	err := ss.iterFunc(store, func(dbInfo *model.DBInfo, tableInfo *model.TableInfo) {
		// because the field of `dbInfo` would be modified, which affects the later iteration.
		// so copy the `dbInfo` for each to `newDBInfo`
		newDBInfo := *dbInfo
		schema := &schemaInfo{
			tableInfo: tableInfo,
			dbInfo:    &newDBInfo,
		}

		if utils.IsSysDB(schema.dbInfo.Name.L) {
			schema.dbInfo.Name = utils.TemporaryDBName(schema.dbInfo.Name.O)
		}

		var checksum *checkpoint.ChecksumItem
		var exists bool = false
		if ss.checkpointChecksum != nil && schema.tableInfo != nil {
			checksum, exists = ss.checkpointChecksum[schema.tableInfo.ID]
		}
		workerPool.ApplyOnErrorGroup(errg, func() error {
			if schema.tableInfo != nil {
				logger := log.L().With(
					zap.String("db", schema.dbInfo.Name.O),
					zap.String("table", schema.tableInfo.Name.O),
				)

				if !skipChecksum {
					logger.Info("Calculate table checksum start")
					if exists && checksum != nil {
						schema.crc64xor = checksum.Crc64xor
						schema.totalKvs = checksum.TotalKvs
						schema.totalBytes = checksum.TotalBytes
						logger.Info("Calculate table checksum completed (from checkpoint)",
							zap.Uint64("Crc64Xor", schema.crc64xor),
							zap.Uint64("TotalKvs", schema.totalKvs),
							zap.Uint64("TotalBytes", schema.totalBytes))
					} else {
						start := time.Now()
						err := schema.calculateChecksum(ectx, store.GetClient(), backupTS, copConcurrency)
						if err != nil {
							return errors.Trace(err)
						}
						calculateCost := time.Since(start)
						if checkpointRunner != nil {
							// if checkpoint runner is running and the checksum is not from checkpoint
							// then flush the checksum by the checkpoint runner
							if err = checkpointRunner.FlushChecksum(ctx, schema.tableInfo.ID, schema.crc64xor, schema.totalKvs, schema.totalBytes); err != nil {
								return errors.Trace(err)
							}
						}
						logger.Info("Calculate table checksum completed",
							zap.Uint64("Crc64Xor", schema.crc64xor),
							zap.Uint64("TotalKvs", schema.totalKvs),
							zap.Uint64("TotalBytes", schema.totalBytes),
							zap.Duration("calculate-take", calculateCost))
					}
				}
				if statsHandle != nil {
					statsWriter := metaWriter.NewStatsWriter()
					if err := schema.dumpStatsToJSON(ctx, statsWriter, statsHandle, backupTS); err != nil {
						logger.Error("dump table stats failed", logutil.ShortError(err))
						return errors.Trace(err)
					}
				}
			}
			// Send schema to metawriter
			s, err := schema.encodeToSchema()
			if err != nil {
				return errors.Trace(err)
			}
			if err := metaWriter.Send(s, op); err != nil {
				return errors.Trace(err)
			}
			if updateCh != nil {
				updateCh.Inc()
			}
			return nil
		})
	})
	if err != nil {
		return errors.Trace(err)
	}
	if err := errg.Wait(); err != nil {
		return errors.Trace(err)
	}
	log.Info("Backup calculated table checksum into metas", zap.Duration("take", time.Since(startAll)))
	summary.CollectDuration("backup checksum", time.Since(startAll))
	return metaWriter.FinishWriteMetas(ctx, op)
}

// Len returns the number of schemas.
func (ss *Schemas) Len() int {
	return ss.size
}

func (s *schemaInfo) calculateChecksum(
	ctx context.Context,
	client kv.Client,
	backupTS uint64,
	concurrency uint,
) error {
	exe, err := checksum.NewExecutorBuilder(s.tableInfo, backupTS).
		SetExplicitRequestSourceType(kvutil.ExplicitTypeBR).
		SetConcurrency(concurrency).
		Build()
	if err != nil {
		return errors.Trace(err)
	}

	checksumResp, err := exe.Execute(ctx, client, func() {
		// TODO: update progress here.
	})
	if err != nil {
		return errors.Trace(err)
	}

	s.crc64xor = checksumResp.Checksum
	s.totalKvs = checksumResp.TotalKvs
	s.totalBytes = checksumResp.TotalBytes
	return nil
}

func (s *schemaInfo) dumpStatsToJSON(ctx context.Context, statsWriter *metautil.StatsWriter, statsHandle *handle.Handle, backupTS uint64) error {
	log.Info("dump stats to json", zap.Stringer("db", s.dbInfo.Name), zap.Stringer("table", s.tableInfo.Name))
	if err := statsHandle.PersistStatsBySnapshot(
		ctx, s.dbInfo.Name.String(), s.tableInfo, backupTS, statsWriter.BackupStats,
	); err != nil {
		return errors.Trace(err)
	}

	statsFileIndexes, err := statsWriter.BackupStatsDone(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	s.statsIndex = statsFileIndexes
	return nil
}

func (s *schemaInfo) encodeToSchema() (*backuppb.Schema, error) {
	dbBytes, err := json.Marshal(s.dbInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var tableBytes []byte
	if s.tableInfo != nil {
		tableBytes, err = json.Marshal(s.tableInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	var statsBytes []byte
	if s.stats != nil {
		statsBytes, err = json.Marshal(s.stats)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return &backuppb.Schema{
		Db:         dbBytes,
		Table:      tableBytes,
		Crc64Xor:   s.crc64xor,
		TotalKvs:   s.totalKvs,
		TotalBytes: s.totalBytes,
		Stats:      statsBytes,
		StatsIndex: s.statsIndex,
	}, nil
}
