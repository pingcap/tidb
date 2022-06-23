// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/statistics/handle"
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
	stats      *handle.JSONTable
}

// Schemas is task for backuping schemas.
type Schemas struct {
	// name -> schema
	schemas map[string]*schemaInfo
}

func NewBackupSchemas() *Schemas {
	return &Schemas{
		schemas: make(map[string]*schemaInfo),
	}
}

func (ss *Schemas) AddSchema(
	dbInfo *model.DBInfo, tableInfo *model.TableInfo,
) {
	if tableInfo == nil {
		ss.schemas[utils.EncloseName(dbInfo.Name.L)] = &schemaInfo{
			dbInfo: dbInfo,
		}
		return
	}
	name := fmt.Sprintf("%s.%s",
		utils.EncloseName(dbInfo.Name.L), utils.EncloseName(tableInfo.Name.L))
	ss.schemas[name] = &schemaInfo{
		tableInfo: tableInfo,
		dbInfo:    dbInfo,
	}
}

// BackupSchemas backups table info, including checksum and stats.
func (ss *Schemas) BackupSchemas(
	ctx context.Context,
	metaWriter *metautil.MetaWriter,
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

	workerPool := utils.NewWorkerPool(concurrency, "Schemas")
	errg, ectx := errgroup.WithContext(ctx)
	startAll := time.Now()
	op := metautil.AppendSchema
	metaWriter.StartWriteMetasAsync(ctx, op)
	for _, s := range ss.schemas {
		schema := s
		// Because schema.dbInfo is a pointer that many tables point to.
		// Remove "add Temporary-prefix into dbName" from closure to prevent concurrent operations.
		if utils.IsSysDB(schema.dbInfo.Name.L) {
			schema.dbInfo.Name = utils.TemporaryDBName(schema.dbInfo.Name.O)
		}

		workerPool.ApplyOnErrorGroup(errg, func() error {
			if schema.tableInfo != nil {
				logger := log.With(
					zap.String("db", schema.dbInfo.Name.O),
					zap.String("table", schema.tableInfo.Name.O),
				)

				if !skipChecksum {
					logger.Info("table checksum start")
					start := time.Now()
					err := schema.calculateChecksum(ectx, store.GetClient(), backupTS, copConcurrency)
					if err != nil {
						return errors.Trace(err)
					}
					logger.Info("table checksum finished",
						zap.Uint64("Crc64Xor", schema.crc64xor),
						zap.Uint64("TotalKvs", schema.totalKvs),
						zap.Uint64("TotalBytes", schema.totalBytes),
						zap.Duration("take", time.Since(start)))
				}
				if statsHandle != nil {
					if err := schema.dumpStatsToJSON(statsHandle); err != nil {
						logger.Error("dump table stats failed", logutil.ShortError(err))
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
	}
	if err := errg.Wait(); err != nil {
		return errors.Trace(err)
	}
	log.Info("backup checksum", zap.Duration("take", time.Since(startAll)))
	summary.CollectDuration("backup checksum", time.Since(startAll))
	return metaWriter.FinishWriteMetas(ctx, op)
}

// Len returns the number of schemas.
func (ss *Schemas) Len() int {
	return len(ss.schemas)
}

func (s *schemaInfo) calculateChecksum(
	ctx context.Context,
	client kv.Client,
	backupTS uint64,
	concurrency uint,
) error {
	exe, err := checksum.NewExecutorBuilder(s.tableInfo, backupTS).
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

func (s *schemaInfo) dumpStatsToJSON(statsHandle *handle.Handle) error {
	jsonTable, err := statsHandle.DumpStatsToJSON(
		s.dbInfo.Name.String(), s.tableInfo, nil)
	if err != nil {
		return errors.Trace(err)
	}

	s.stats = jsonTable
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
	}, nil
}
