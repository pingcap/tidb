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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultSchemaConcurrency is the default number of the concurrent
	// backup schema tasks.
	DefaultSchemaConcurrency = 64
)

type scheamInfo struct {
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
	schemas map[string]*scheamInfo
}

func newBackupSchemas() *Schemas {
	return &Schemas{
		schemas: make(map[string]*scheamInfo),
	}
}

func (ss *Schemas) addSchema(
	dbInfo *model.DBInfo, tableInfo *model.TableInfo,
) {
	name := fmt.Sprintf("%s.%s",
		utils.EncloseName(dbInfo.Name.L), utils.EncloseName(tableInfo.Name.L))
	ss.schemas[name] = &scheamInfo{
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
		workerPool.ApplyOnErrorGroup(errg, func() error {
			if utils.IsSysDB(schema.dbInfo.Name.L) {
				schema.dbInfo.Name = utils.TemporaryDBName(schema.dbInfo.Name.O)
			}
			logger := log.With(
				zap.String("db", schema.dbInfo.Name.O),
				zap.String("table", schema.tableInfo.Name.O),
			)

			if !skipChecksum {
				logger.Info("table checksum start")
				start := time.Now()
				checksumResp, err := calculateChecksum(
					ectx, schema.tableInfo, store.GetClient(), backupTS, copConcurrency)
				if err != nil {
					return errors.Trace(err)
				}
				schema.crc64xor = checksumResp.Checksum
				schema.totalKvs = checksumResp.TotalKvs
				schema.totalBytes = checksumResp.TotalBytes
				logger.Info("table checksum finished",
					zap.Uint64("Crc64Xor", checksumResp.Checksum),
					zap.Uint64("TotalKvs", checksumResp.TotalKvs),
					zap.Uint64("TotalBytes", checksumResp.TotalBytes),
					zap.Duration("take", time.Since(start)))
			}
			if statsHandle != nil {
				jsonTable, err := statsHandle.DumpStatsToJSON(
					schema.dbInfo.Name.String(), schema.tableInfo, nil)
				if err != nil {
					logger.Error("dump table stats failed", logutil.ShortError(err))
				}
				schema.stats = jsonTable
			}
			// Send schema to metawriter
			dbBytes, err := json.Marshal(schema.dbInfo)
			if err != nil {
				return errors.Trace(err)
			}
			tableBytes, err := json.Marshal(schema.tableInfo)
			if err != nil {
				return errors.Trace(err)
			}
			var statsBytes []byte
			if schema.stats != nil {
				statsBytes, err = json.Marshal(schema.stats)
				if err != nil {
					return errors.Trace(err)
				}
			}
			s := &backuppb.Schema{
				Db:         dbBytes,
				Table:      tableBytes,
				Crc64Xor:   schema.crc64xor,
				TotalKvs:   schema.totalKvs,
				TotalBytes: schema.totalBytes,
				Stats:      statsBytes,
			}

			if err := metaWriter.Send(s, op); err != nil {
				return errors.Trace(err)
			}
			updateCh.Inc()
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

func calculateChecksum(
	ctx context.Context,
	table *model.TableInfo,
	client kv.Client,
	backupTS uint64,
	concurrency uint,
) (*tipb.ChecksumResponse, error) {
	exe, err := checksum.NewExecutorBuilder(table, backupTS).
		SetConcurrency(concurrency).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	checksumResp, err := exe.Execute(ctx, client, func() {
		// TODO: update progress here.
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return checksumResp, nil
}
