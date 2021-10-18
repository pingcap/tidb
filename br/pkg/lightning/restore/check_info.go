// Copyright 2020 PingCAP, Inc.
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

package restore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/table/tables"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/api"
	pdconfig "github.com/tikv/pd/server/config"
	"go.uber.org/zap"
)

const (
	pdWriteFlow = "/pd/api/v1/regions/writeflow"
	pdReadFlow  = "/pd/api/v1/regions/readflow"

	// OnlineBytesLimitation/OnlineKeysLimitation is the statistics of
	// Bytes/Keys used per region from pdWriteFlow/pdReadFlow
	// this determines whether the cluster has some region that have other loads
	// and might influence the import task in the future.
	OnlineBytesLimitation = 10 * units.MiB
	OnlineKeysLimitation  = 5000

	pdStores    = "/pd/api/v1/stores"
	pdReplicate = "/pd/api/v1/config/replicate"

	defaultCSVSize    = 10 * units.GiB
	maxSampleDataSize = 10 * 1024 * 1024
	maxSampleRowCount = 10 * 1024
)

func (rc *Controller) isSourceInLocal() bool {
	return strings.HasPrefix(rc.store.URI(), storage.LocalURIPrefix)
}

func (rc *Controller) getReplicaCount(ctx context.Context) (uint64, error) {
	result := &pdconfig.ReplicationConfig{}
	err := rc.tls.WithHost(rc.cfg.TiDB.PdAddr).GetJSON(ctx, pdReplicate, &result)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return result.MaxReplicas, nil
}

// ClusterResource check cluster has enough resource to import data. this test can by skipped.
func (rc *Controller) ClusterResource(ctx context.Context, localSource int64) error {
	passed := true
	message := "Cluster resources are rich for this import task"
	defer func() {
		rc.checkTemplate.Collect(Critical, passed, message)
	}()

	result := &api.StoresInfo{}
	err := rc.tls.WithHost(rc.cfg.TiDB.PdAddr).GetJSON(ctx, pdStores, result)
	if err != nil {
		return errors.Trace(err)
	}
	totalCapacity := typeutil.ByteSize(0)
	for _, store := range result.Stores {
		totalCapacity += store.Status.Capacity
	}
	clusterSource := localSource
	if rc.taskMgr != nil {
		clusterSource, err = rc.taskMgr.CheckClusterSource(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}

	replicaCount, err := rc.getReplicaCount(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	estimateSize := uint64(clusterSource) * replicaCount
	if typeutil.ByteSize(estimateSize) > totalCapacity {
		passed = false
		message = fmt.Sprintf("Cluster doesn't have enough space, capacity is %s, but we need %s",
			units.BytesSize(float64(totalCapacity)), units.BytesSize(float64(estimateSize)))
	} else {
		message = fmt.Sprintf("Cluster capacity is rich, capacity is %s, we need %s",
			units.BytesSize(float64(totalCapacity)), units.BytesSize(float64(estimateSize)))
	}
	return nil
}

// ClusterIsAvailable check cluster is available to import data. this test can be skipped.
func (rc *Controller) ClusterIsAvailable(ctx context.Context) error {
	passed := true
	message := "Cluster is available"
	defer func() {
		rc.checkTemplate.Collect(Critical, passed, message)
	}()
	// skip requirement check if explicitly turned off
	if !rc.cfg.App.CheckRequirements {
		message = "Cluster's available check is skipped by user requirement"
		return nil
	}
	checkCtx := &backend.CheckCtx{
		DBMetas: rc.dbMetas,
	}
	if err := rc.backend.CheckRequirements(ctx, checkCtx); err != nil {
		passed = false
		message = fmt.Sprintf("cluster available check failed: %s", err.Error())
	}
	return nil
}

// StoragePermission checks whether Lightning has enough permission to storage.
// this test cannot be skipped.
func (rc *Controller) StoragePermission(ctx context.Context) error {
	passed := true
	message := "Lightning has the correct storage permission"
	defer func() {
		rc.checkTemplate.Collect(Critical, passed, message)
	}()

	u, err := storage.ParseBackend(rc.cfg.Mydumper.SourceDir, nil)
	if err != nil {
		return errors.Annotate(err, "parse backend failed")
	}
	_, err = storage.New(ctx, u, &storage.ExternalStorageOptions{
		CheckPermissions: []storage.Permission{
			storage.ListObjects,
			storage.GetObject,
		},
	})
	if err != nil {
		passed = false
		message = err.Error()
	}
	return nil
}

// HasLargeCSV checks whether input csvs is fit for Lightning import.
// If strictFormat is false, and csv file is large. Lightning will have performance issue.
// this test cannot be skipped.
func (rc *Controller) HasLargeCSV(dbMetas []*mydump.MDDatabaseMeta) error {
	passed := true
	message := "Source csv files size is proper"
	defer func() {
		rc.checkTemplate.Collect(Warn, passed, message)
	}()
	if !rc.cfg.Mydumper.StrictFormat {
		for _, db := range dbMetas {
			for _, t := range db.Tables {
				for _, f := range t.DataFiles {
					if f.FileMeta.FileSize > defaultCSVSize {
						message = fmt.Sprintf("large csv: %s file exists and it will slow down import performance", f.FileMeta.Path)
						passed = false
					}
				}
			}
		}
	} else {
		message = "Skip the csv size check, because config.StrictFormat is true"
	}
	return nil
}

func (rc *Controller) EstimateSourceData(ctx context.Context) (int64, error) {
	sourceSize := int64(0)
	originSource := int64(0)
	bigTableCount := 0
	tableCount := 0
	unSortedTableCount := 0
	for _, db := range rc.dbMetas {
		info, ok := rc.dbInfos[db.Name]
		if !ok {
			continue
		}
		for _, tbl := range db.Tables {
			tableInfo, ok := info.Tables[tbl.Name]
			if ok {
				if err := rc.SampleDataFromTable(ctx, db.Name, tbl, tableInfo.Core); err != nil {
					return sourceSize, errors.Trace(err)
				}
				sourceSize += int64(float64(tbl.TotalSize) * tbl.IndexRatio)
				originSource += tbl.TotalSize
				if tbl.TotalSize > int64(config.DefaultBatchSize)*2 {
					bigTableCount += 1
					if !tbl.IsRowOrdered {
						unSortedTableCount += 1
					}
				}
				tableCount += 1
			}
		}
	}

	// Do not import with too large concurrency because these data may be all unsorted.
	if bigTableCount > 0 && unSortedTableCount > 0 {
		if rc.cfg.App.TableConcurrency > rc.cfg.App.IndexConcurrency {
			rc.cfg.App.TableConcurrency = rc.cfg.App.IndexConcurrency
		}
	}
	return sourceSize, nil
}

// LocalResource checks the local node has enough resources for this import when local backend enabled;
func (rc *Controller) LocalResource(ctx context.Context, sourceSize int64) error {
	if rc.isSourceInLocal() {
		sourceDir := strings.TrimPrefix(rc.cfg.Mydumper.SourceDir, storage.LocalURIPrefix)
		same, err := common.SameDisk(sourceDir, rc.cfg.TikvImporter.SortedKVDir)
		if err != nil {
			return errors.Trace(err)
		}
		if same {
			rc.checkTemplate.Collect(Warn, false,
				fmt.Sprintf("sorted-kv-dir:%s and data-source-dir:%s are in the same disk, may slow down performance",
					rc.cfg.TikvImporter.SortedKVDir, sourceDir))
		}
	}

	storageSize, err := common.GetStorageSize(rc.cfg.TikvImporter.SortedKVDir)
	if err != nil {
		return errors.Trace(err)
	}
	localAvailable := storageSize.Available
	if err = rc.taskMgr.InitTask(ctx, sourceSize); err != nil {
		return errors.Trace(err)
	}

	var message string
	var passed bool
	switch {
	case localAvailable > uint64(sourceSize):
		message = fmt.Sprintf("local disk resources are rich, estimate sorted data size %s, local available is %s",
			units.BytesSize(float64(sourceSize)), units.BytesSize(float64(localAvailable)))
		passed = true
	default:
		if int64(rc.cfg.TikvImporter.DiskQuota) > int64(localAvailable) {
			message = fmt.Sprintf("local disk space may not enough to finish import"+
				"estimate sorted data size is %s, but local available is %s,"+
				"you need a smaller number for tikv-importer.disk-quota (%s) to finish imports",
				units.BytesSize(float64(sourceSize)),
				units.BytesSize(float64(localAvailable)), units.BytesSize(float64(rc.cfg.TikvImporter.DiskQuota)))
			passed = false
			log.L().Error(message)
		} else {
			message = fmt.Sprintf("local disk space may not enough to finish import, "+
				"estimate sorted data size is %s, but local available is %s,"+
				"we will use disk-quota (size: %s) to finish imports, which may slow down import",
				units.BytesSize(float64(sourceSize)),
				units.BytesSize(float64(localAvailable)), units.BytesSize(float64(rc.cfg.TikvImporter.DiskQuota)))
			passed = true
			log.L().Warn(message)
		}
	}
	rc.checkTemplate.Collect(Critical, passed, message)
	return nil
}

// CheckpointIsValid checks whether we can start this import with this checkpoint.
func (rc *Controller) CheckpointIsValid(ctx context.Context, tableInfo *mydump.MDTableMeta) ([]string, bool, error) {
	msgs := make([]string, 0)
	uniqueName := common.UniqueTable(tableInfo.DB, tableInfo.Name)
	tableCheckPoint, err := rc.checkpointsDB.Get(ctx, uniqueName)
	if err != nil {
		// there is no checkpoint
		log.L().Debug("no checkpoint detected", zap.String("table", uniqueName))
		return nil, true, nil
	}
	// if checkpoint enable and not missing, we skip the check table empty progress.
	if tableCheckPoint.Status <= checkpoints.CheckpointStatusMissing {
		return nil, false, nil
	}

	if tableCheckPoint.Status <= checkpoints.CheckpointStatusMaxInvalid {
		failedStep := tableCheckPoint.Status * 10
		var action strings.Builder
		action.WriteString("./tidb-lightning-ctl --checkpoint-error-")
		switch failedStep {
		case checkpoints.CheckpointStatusAlteredAutoInc, checkpoints.CheckpointStatusAnalyzed:
			action.WriteString("ignore")
		default:
			action.WriteString("destroy")
		}
		action.WriteString("='")
		action.WriteString(uniqueName)
		action.WriteString("' --config=...")

		msgs = append(msgs, fmt.Sprintf("TiDB Lightning has failed last time. To prevent data loss, this run will stop now, "+
			"%s failed in step(%s), please run command %s,"+
			"You may also run `./tidb-lightning-ctl --checkpoint-error-destroy=all --config=...` to start from scratch,"+
			"For details of this failure, read the log file from the PREVIOUS run",
			uniqueName, failedStep.MetricName(), action.String()))
		return msgs, false, nil
	}

	dbInfo, ok := rc.dbInfos[tableInfo.DB]
	if ok {
		t, ok := dbInfo.Tables[tableInfo.Name]
		if ok {
			if tableCheckPoint.TableID > 0 && tableCheckPoint.TableID != t.ID {
				msgs = append(msgs, fmt.Sprintf("TiDB Lightning has detected tables with illegal checkpoints. To prevent data loss, this run will stop now,"+
					"please run command \"./tidb-lightning-ctl --checkpoint-remove='%s' --config=...\""+
					"You may also run `./tidb-lightning-ctl --checkpoint-error-destroy=all --config=...` to start from scratch,"+
					"For details of this failure, read the log file from the PREVIOUS run",
					uniqueName))
				return msgs, false, nil
			}
		}
	}

	var permFromCheckpoint []int
	var columns []string
	for _, eng := range tableCheckPoint.Engines {
		if len(eng.Chunks) > 0 {
			chunk := eng.Chunks[0]
			permFromCheckpoint = chunk.ColumnPermutation
			columns = chunk.Chunk.Columns
			if filepath.Dir(chunk.FileMeta.Path) != rc.cfg.Mydumper.SourceDir {
				message := fmt.Sprintf("chunk checkpoints path is not equal to config"+
					"checkpoint is %s, config source dir is %s", chunk.FileMeta.Path, rc.cfg.Mydumper.SourceDir)
				msgs = append(msgs, message)
			}
		}
	}
	if len(columns) == 0 {
		log.L().Debug("no valid checkpoint detected", zap.String("table", uniqueName))
		return nil, false, nil
	}
	info := rc.dbInfos[tableInfo.DB].Tables[tableInfo.Name]
	if info != nil {
		permFromTiDB, err := parseColumnPermutations(info.Core, columns, nil)
		if err != nil {
			msgs = append(msgs, fmt.Sprintf("failed to calculate columns %s, table %s's info has changed,"+
				"consider remove this checkpoint, and start import again.", err.Error(), uniqueName))
		}
		if !reflect.DeepEqual(permFromCheckpoint, permFromTiDB) {
			msgs = append(msgs, fmt.Sprintf("compare columns perm failed. table %s's info has changed,"+
				"consider remove this checkpoint, and start import again.", uniqueName))
		}
	}
	return msgs, false, nil
}

// hasDefault represents col has default value.
func hasDefault(col *model.ColumnInfo) bool {
	return col.DefaultIsExpr || col.DefaultValue != nil || !mysql.HasNotNullFlag(col.Flag) ||
		col.IsGenerated() || mysql.HasAutoIncrementFlag(col.Flag)
}

func (rc *Controller) readColumnsAndCount(ctx context.Context, dataFileMeta mydump.SourceFileMeta) (cols []string, colCnt int, err error) {
	var reader storage.ReadSeekCloser
	if dataFileMeta.Type == mydump.SourceTypeParquet {
		reader, err = mydump.OpenParquetReader(ctx, rc.store, dataFileMeta.Path, dataFileMeta.FileSize)
	} else {
		reader, err = rc.store.Open(ctx, dataFileMeta.Path)
	}
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	var parser mydump.Parser
	blockBufSize := int64(rc.cfg.Mydumper.ReadBlockSize)
	switch dataFileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := rc.cfg.Mydumper.CSV.Header
		parser = mydump.NewCSVParser(&rc.cfg.Mydumper.CSV, reader, blockBufSize, rc.ioWorkers, hasHeader)
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(rc.cfg.TiDB.SQLMode, reader, blockBufSize, rc.ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, rc.store, reader, dataFileMeta.Path)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
	default:
		panic(fmt.Sprintf("unknown file type '%s'", dataFileMeta.Type))
	}
	defer parser.Close()

	err = parser.ReadRow()
	if err != nil && errors.Cause(err) != io.EOF {
		return nil, 0, errors.Trace(err)
	}
	return parser.Columns(), len(parser.LastRow().Row), nil
}

// SchemaIsValid checks the import file and cluster schema is match.
func (rc *Controller) SchemaIsValid(ctx context.Context, tableInfo *mydump.MDTableMeta) ([]string, error) {
	msgs := make([]string, 0)
	info, ok := rc.dbInfos[tableInfo.DB].Tables[tableInfo.Name]
	if !ok {
		msgs = append(msgs, fmt.Sprintf("TiDB schema `%s`.`%s` doesn't exists,"+
			"please give a schema file in source dir or create table manually", tableInfo.DB, tableInfo.Name))
		return msgs, nil
	}

	igCols := make(map[string]struct{})
	igCol, err := rc.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(tableInfo.DB, tableInfo.Name, rc.cfg.Mydumper.CaseSensitive)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, col := range igCol.Columns {
		igCols[col] = struct{}{}
	}

	if len(tableInfo.DataFiles) == 0 {
		log.L().Info("no data files detected", zap.String("db", tableInfo.DB), zap.String("table", tableInfo.Name))
		return nil, nil
	}

	colCountFromTiDB := len(info.Core.Columns)
	core := info.Core
	defaultCols := make(map[string]struct{})
	for _, col := range core.Columns {
		if hasDefault(col) || (info.Core.ContainsAutoRandomBits() && mysql.HasPriKeyFlag(col.Flag)) {
			// this column has default value or it's auto random id, so we can ignore it
			defaultCols[col.Name.L] = struct{}{}
		}
	}
	// tidb_rowid have a default value.
	defaultCols[model.ExtraHandleName.String()] = struct{}{}

	// only check the first file of this table.
	if len(tableInfo.DataFiles) > 0 {
		dataFile := tableInfo.DataFiles[0]
		log.L().Info("datafile to check", zap.String("db", tableInfo.DB),
			zap.String("table", tableInfo.Name), zap.String("path", dataFile.FileMeta.Path))
		// get columns name from data file.
		dataFileMeta := dataFile.FileMeta

		if tp := dataFileMeta.Type; tp != mydump.SourceTypeCSV && tp != mydump.SourceTypeSQL && tp != mydump.SourceTypeParquet {
			msgs = append(msgs, fmt.Sprintf("file '%s' with unknown source type '%s'", dataFileMeta.Path, dataFileMeta.Type.String()))
			return msgs, nil
		}
		colsFromDataFile, colCountFromDataFile, err := rc.readColumnsAndCount(ctx, dataFileMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if colsFromDataFile == nil && colCountFromDataFile == 0 {
			log.L().Info("file contains no data, skip checking against schema validity", zap.String("path", dataFileMeta.Path))
			return msgs, nil
		}

		if colsFromDataFile == nil {
			// when there is no columns name in data file. we must insert data in order.
			// so the last several columns either can be ignored or has a default value.
			for i := colCountFromDataFile; i < colCountFromTiDB; i++ {
				if _, ok := defaultCols[core.Columns[i].Name.L]; !ok {
					msgs = append(msgs, fmt.Sprintf("TiDB schema `%s`.`%s` has %d columns,"+
						"and data file has %d columns, but column %s are missing the default value,"+
						"please give column a default value to skip this check",
						tableInfo.DB, tableInfo.Name, colCountFromTiDB, colCountFromDataFile, core.Columns[i].Name.L))
				}
			}
		} else {
			// compare column names and make sure
			// 1. TiDB table info has data file's all columns(besides ignore columns)
			// 2. Those columns not introduced in data file always have a default value.
			colMap := make(map[string]struct{})
			for col := range igCols {
				colMap[col] = struct{}{}
			}
			for _, col := range core.Columns {
				if _, ok := colMap[col.Name.L]; ok {
					// tidb's column is ignored
					// we need ensure this column has the default value.
					if _, hasDefault := defaultCols[col.Name.L]; !hasDefault {
						msgs = append(msgs, fmt.Sprintf("TiDB schema `%s`.`%s`'s column %s cannot be ignored,"+
							"because it doesn't hava a default value, please set tables.ignoreColumns properly",
							tableInfo.DB, tableInfo.Name, col.Name.L))
					}
				} else {
					colMap[col.Name.L] = struct{}{}
				}
			}
			// tidb_rowid can be ignored in check
			colMap[model.ExtraHandleName.String()] = struct{}{}
			for _, col := range colsFromDataFile {
				if _, ok := colMap[col]; !ok {
					checkMsg := "please check table schema"
					if dataFileMeta.Type == mydump.SourceTypeCSV && rc.cfg.Mydumper.CSV.Header {
						checkMsg += " and csv file header"
					}
					msgs = append(msgs, fmt.Sprintf("TiDB schema `%s`.`%s` doesn't have column %s, "+
						"%s or use tables.ignoreColumns to ignore %s",
						tableInfo.DB, tableInfo.Name, col, checkMsg, col))
				} else {
					// remove column for next iteration
					delete(colMap, col)
				}
			}
			// if theses rest columns don't have a default value.
			for col := range colMap {
				if _, ok := defaultCols[col]; ok {
					continue
				}
				msgs = append(msgs, fmt.Sprintf("TiDB schema `%s`.`%s` doesn't have the default value for %s"+
					"please give a default value for %s or choose another column to ignore or add this column in data file",
					tableInfo.DB, tableInfo.Name, col, col))
			}
		}
	}
	return msgs, nil
}

func (rc *Controller) SampleDataFromTable(ctx context.Context, dbName string, tableMeta *mydump.MDTableMeta, tableInfo *model.TableInfo) error {
	if len(tableMeta.DataFiles) == 0 {
		return nil
	}
	sampleFile := tableMeta.DataFiles[0].FileMeta
	var reader storage.ReadSeekCloser
	var err error
	if sampleFile.Type == mydump.SourceTypeParquet {
		reader, err = mydump.OpenParquetReader(ctx, rc.store, sampleFile.Path, sampleFile.FileSize)
	} else {
		reader, err = rc.store.Open(ctx, sampleFile.Path)
	}
	if err != nil {
		return errors.Trace(err)
	}
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, tableInfo)

	kvEncoder, err := rc.backend.NewEncoder(tbl, &kv.SessionOptions{
		SQLMode:        rc.cfg.TiDB.SQLMode,
		Timestamp:      0,
		SysVars:        rc.sysVars,
		AutoRandomSeed: 0,
	})
	blockBufSize := int64(rc.cfg.Mydumper.ReadBlockSize)

	var parser mydump.Parser
	switch tableMeta.DataFiles[0].FileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := rc.cfg.Mydumper.CSV.Header
		parser = mydump.NewCSVParser(&rc.cfg.Mydumper.CSV, reader, blockBufSize, rc.ioWorkers, hasHeader)
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(rc.cfg.TiDB.SQLMode, reader, blockBufSize, rc.ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, rc.store, reader, sampleFile.Path)
		if err != nil {
			return errors.Trace(err)
		}
	default:
		panic(fmt.Sprintf("file '%s' with unknown source type '%s'", sampleFile.Path, sampleFile.Type.String()))
	}
	defer parser.Close()
	logTask := log.With(zap.String("table", tableMeta.Name)).Begin(zap.InfoLevel, "sample file")
	igCols, err := rc.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(dbName, tableMeta.Name, rc.cfg.Mydumper.CaseSensitive)
	if err != nil {
		return errors.Trace(err)
	}

	initializedColumns, reachEOF := false, false
	var columnPermutation []int
	var kvSize uint64 = 0
	var rowSize uint64 = 0
	rowCount := 0
	dataKVs := rc.backend.MakeEmptyRows()
	indexKVs := rc.backend.MakeEmptyRows()
	lastKey := make([]byte, 0)
	tableMeta.IsRowOrdered = true
	tableMeta.IndexRatio = 1.0
outloop:
	for !reachEOF {
		offset, _ := parser.Pos()
		err = parser.ReadRow()
		columnNames := parser.Columns()

		switch errors.Cause(err) {
		case nil:
			if !initializedColumns {
				if len(columnPermutation) == 0 {
					columnPermutation, err = createColumnPermutation(columnNames, igCols.Columns, tableInfo)
					if err != nil {
						return errors.Trace(err)
					}
				}
				initializedColumns = true
			}
		case io.EOF:
			reachEOF = true
			break outloop
		default:
			err = errors.Annotatef(err, "in file offset %d", offset)
			return errors.Trace(err)
		}
		lastRow := parser.LastRow()
		rowSize += uint64(lastRow.Length)
		rowCount += 1

		var dataChecksum, indexChecksum verification.KVChecksum
		kvs, encodeErr := kvEncoder.Encode(logTask.Logger, lastRow.Row, lastRow.RowID, columnPermutation, offset)
		parser.RecycleRow(lastRow)
		if encodeErr != nil {
			err = errors.Annotatef(encodeErr, "in file at offset %d", offset)
			return errors.Trace(err)
		}
		if tableMeta.IsRowOrdered {
			kvs.ClassifyAndAppend(&dataKVs, &dataChecksum, &indexKVs, &indexChecksum)
			for _, kv := range kv.KvPairsFromRows(dataKVs) {
				if len(lastKey) == 0 {
					lastKey = kv.Key
				} else if bytes.Compare(lastKey, kv.Key) > 0 {
					tableMeta.IsRowOrdered = false
					break
				}
			}
			dataKVs = dataKVs.Clear()
			indexKVs = indexKVs.Clear()
		}
		kvSize += kvs.Size()

		failpoint.Inject("mock-kv-size", func(val failpoint.Value) {
			kvSize += uint64(val.(int))
		})
		if rowSize > maxSampleDataSize && rowCount > maxSampleRowCount {
			break
		}
	}

	if rowSize > 0 && kvSize > rowSize {
		tableMeta.IndexRatio = float64(kvSize) / float64(rowSize)
	}
	log.L().Info("Sample source data", zap.String("table", tableMeta.Name), zap.Float64("IndexRatio", tableMeta.IndexRatio), zap.Bool("IsSourceOrder", tableMeta.IsRowOrdered))
	return nil
}
