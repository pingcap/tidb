// Copyright 2018 PingCAP, Inc.
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

package diff

import (
	"container/heap"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

var (
	// cancel the context for `Equal`, only used in test
	cancelEqualFunc context.CancelFunc
)

// TableInstance record a table instance
type TableInstance struct {
	Conn       *sql.DB `json:"-"`
	Schema     string  `json:"schema"`
	Table      string  `json:"table"`
	InstanceID string  `json:"instance-id"`
	info       *model.TableInfo
}

// TableDiff saves config for diff table
type TableDiff struct {
	// source tables
	SourceTables []*TableInstance `json:"source-tables"`
	// target table
	TargetTable *TableInstance `json:"target-table"`

	// columns be ignored
	IgnoreColumns []string `json:"-"`

	// field should be the primary key, unique key or field with index
	Fields string `json:"fields"`

	// select range, for example: "age > 10 AND age < 20"
	Range string `json:"range"`

	// for example, the whole data is [1...100]
	// we can split these data to [1...10], [11...20], ..., [91...100]
	// the [1...10] is a chunk, and it's chunk size is 10
	// size of the split chunk
	ChunkSize int `json:"chunk-size"`

	// sampling check percent, for example 10 means only check 10% data
	Sample int `json:"sample"`

	// how many goroutines are created to check data
	CheckThreadCount int `json:"-"`

	// set false if want to comapre the data directly
	UseChecksum bool `json:"-"`

	// set true if just want compare data by checksum, will skip select data when checksum is not equal
	OnlyUseChecksum bool `json:"-"`

	// collation config in mysql/tidb, should corresponding to charset.
	Collation string `json:"collation"`

	// ignore check table's struct
	IgnoreStructCheck bool `json:"-"`

	// ignore check table's data
	IgnoreDataCheck bool `json:"-"`

	// set true will continue check from the latest checkpoint
	UseCheckpoint bool `json:"-"`

	// get tidb statistics information from which table instance. if is nil, will split chunk by random.
	TiDBStatsSource *TableInstance `json:"tidb-stats-source"`

	sqlCh chan string

	wg sync.WaitGroup

	configHash string

	CpDB *sql.DB `json:"-"`

	// 1 means true, 0 means false
	checkpointLoaded int32

	// create after all chunks is splited, or load from checkpoint
	summaryInfo *tableSummaryInfo
}

func (t *TableDiff) setConfigHash() error {
	jsonBytes, err := json.Marshal(t)
	if err != nil {
		return errors.Trace(err)
	}

	t.configHash = fmt.Sprintf("%x", md5.Sum(jsonBytes))
	log.Debug("sync-diff-inspector config", zap.ByteString("config", jsonBytes), zap.String("hash", t.configHash))

	return nil
}

// Equal tests whether two database have same data and schema.
func (t *TableDiff) Equal(ctx context.Context, writeFixSQL func(string) error) (bool, bool, error) {
	t.adjustConfig()
	t.sqlCh = make(chan string)

	err := t.getTableInfo(ctx)
	if err != nil {
		return false, false, errors.Trace(err)
	}

	structEqual := true
	dataEqual := true

	if !t.IgnoreStructCheck {
		structEqual, err = t.CheckTableStruct(ctx)
		if err != nil {
			return false, false, errors.Trace(err)
		}

		if !structEqual {
			return false, false, nil
		}
	}

	if !t.IgnoreDataCheck {
		stopWriteSqlsCh := t.WriteSqls(ctx, writeFixSQL)
		stopUpdateSummaryCh := t.UpdateSummaryInfo(ctx)

		dataEqual, err = t.CheckTableData(ctx)
		if err != nil {
			return structEqual, false, errors.Trace(err)
		}

		select {
		case <-ctx.Done():
		case stopWriteSqlsCh <- true:
		}

		select {
		case <-ctx.Done():
		case stopUpdateSummaryCh <- true:
		}
	}

	t.wg.Wait()
	return structEqual, dataEqual, nil
}

// CheckTableStruct checks table's struct
func (t *TableDiff) CheckTableStruct(ctx context.Context) (bool, error) {
	for _, sourceTable := range t.SourceTables {
		eq, msg := dbutil.EqualTableInfo(sourceTable.info, t.TargetTable.info)
		if !eq {
			log.Warn("table struct is not equal", zap.String("reason", msg))
			return false, nil
		}
		log.Info("table struct is equal", zap.Reflect("source", sourceTable.info), zap.Reflect("target", t.TargetTable.info))
	}

	return true, nil
}

func (t *TableDiff) adjustConfig() {
	if t.ChunkSize <= 0 {
		log.Warn("chunk size is less than 0, will use default value 1000", zap.Int("chunk size", t.ChunkSize))
		t.ChunkSize = 1000
	}

	if t.ChunkSize < 1000 || t.ChunkSize > 10000 {
		log.Warn("chunk size is recommend in range [1000, 10000]", zap.Int("chunk size", t.ChunkSize))
	}

	if len(t.Range) == 0 {
		t.Range = "TRUE"
	}
	if t.Sample <= 0 {
		t.Sample = 100
	}

	if t.CheckThreadCount <= 0 {
		t.CheckThreadCount = 4
	}
}

func (t *TableDiff) getTableInfo(ctx context.Context) error {
	tableInfo, err := dbutil.GetTableInfo(ctx, t.TargetTable.Conn, t.TargetTable.Schema, t.TargetTable.Table)
	if err != nil {
		return errors.Trace(err)
	}
	t.TargetTable.info = ignoreColumns(tableInfo, t.IgnoreColumns)

	for _, sourceTable := range t.SourceTables {
		tableInfo, err := dbutil.GetTableInfo(ctx, sourceTable.Conn, sourceTable.Schema, sourceTable.Table)
		if err != nil {
			return errors.Trace(err)
		}
		sourceTable.info = ignoreColumns(tableInfo, t.IgnoreColumns)
	}

	return nil
}

// CheckTableData checks table's data
func (t *TableDiff) CheckTableData(ctx context.Context) (equal bool, err error) {
	table := t.TargetTable

	useTiDB := false
	if t.TiDBStatsSource != nil {
		table = t.TiDBStatsSource
		useTiDB = true
	}

	fromCheckpoint := true
	chunks, err := t.LoadCheckpoint(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}

	if len(chunks) == 0 {
		log.Info("don't have checkpoint info, or the last check success, or config changed, will split chunks")

		fromCheckpoint = false
		chunks, err = SplitChunks(ctx, table, t.Fields, t.Range, t.ChunkSize, t.Collation, useTiDB, t.CpDB)
		if err != nil {
			return false, errors.Trace(err)
		}
	}

	if len(chunks) == 0 {
		log.Warn("get 0 chunks, table is not checked", zap.String("table", dbutil.TableName(t.TargetTable.Schema, t.TargetTable.Table)))
		return true, nil
	}

	t.summaryInfo = newTableSummaryInfo(int64(len(chunks)))

	checkResultCh := make(chan bool, t.CheckThreadCount)
	defer close(checkResultCh)

	var checkWg sync.WaitGroup
	checkWorkerCh := make([]chan *ChunkRange, 0, t.CheckThreadCount)
	for i := 0; i < t.CheckThreadCount; i++ {
		checkWorkerCh = append(checkWorkerCh, make(chan *ChunkRange, 10))
		checkWg.Add(1)
		go func(j int) {
			defer checkWg.Done()
			t.checkChunksDataEqual(ctx, t.Sample < 100 && !fromCheckpoint, checkWorkerCh[j], checkResultCh)
		}(i)
	}

	go func() {
		defer func() {
			for _, ch := range checkWorkerCh {
				close(ch)
			}
		}()

		for _, chunk := range chunks {
			select {
			case checkWorkerCh[chunk.ID%t.CheckThreadCount] <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()

	checkedNum := 0
	equal = true

CheckResult:
	for {
		select {
		case eq := <-checkResultCh:
			checkedNum++
			if !eq {
				equal = false
			}
			if len(chunks) == checkedNum {
				break CheckResult
			}
		case <-ctx.Done():
			equal = false
			break CheckResult
		}
	}
	checkWg.Wait()

	return equal, nil
}

// LoadCheckpoint do some prepare work before check data, like adjust config and create checkpoint table
func (t *TableDiff) LoadCheckpoint(ctx context.Context) ([]*ChunkRange, error) {
	ctx1, cancel1 := context.WithTimeout(ctx, 5*dbutil.DefaultTimeout)
	defer cancel1()

	err := t.setConfigHash()
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = createCheckpointTable(ctx1, t.CpDB)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if t.UseCheckpoint {
		useCheckpoint, err := loadFromCheckPoint(ctx1, t.CpDB, t.TargetTable.Schema, t.TargetTable.Table, t.configHash)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if useCheckpoint {
			log.Info("use checkpoint to load chunks")
			chunks, err := loadChunks(ctx1, t.CpDB, t.TargetTable.InstanceID, t.TargetTable.Schema, t.TargetTable.Table)
			if err != nil {
				log.Error("load chunks info", zap.Error(err))
				return nil, errors.Trace(err)
			}

			atomic.StoreInt32(&t.checkpointLoaded, 1)
			return chunks, nil
		}
	}

	// clean old checkpoint infomation, and initial table summary
	err = cleanCheckpoint(ctx1, t.CpDB, t.TargetTable.Schema, t.TargetTable.Table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = initTableSummary(ctx1, t.CpDB, t.TargetTable.Schema, t.TargetTable.Table, t.configHash)
	if err != nil {
		return nil, errors.Trace(err)
	}

	atomic.StoreInt32(&t.checkpointLoaded, 1)
	return nil, nil
}

func (t *TableDiff) checkChunksDataEqual(ctx context.Context, filterByRand bool, chunks chan *ChunkRange, resultCh chan bool) {
	var err error
	for {
		select {
		case chunk, ok := <-chunks:
			if !ok {
				return
			}
			eq := false
			if chunk.State == successState || chunk.State == ignoreState {
				eq = true
			} else {
				eq, err = t.checkChunkDataEqual(ctx, filterByRand, chunk)
				if err != nil {
					log.Error("check chunk data equal failed", zap.String("chunk", chunk.String()), zap.Error(err))
					eq = false
				} else if !eq {
					log.Warn("check chunk data not equal", zap.String("chunk", chunk.String()))
				}
			}

			select {
			case resultCh <- eq:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (t *TableDiff) checkChunkDataEqual(ctx context.Context, filterByRand bool, chunk *ChunkRange) (equal bool, err error) {
	failpoint.Inject("CancelCheckChunkDataEqual", func(val failpoint.Value) {
		chunkID := val.(int)
		if chunkID != chunk.ID {
			failpoint.Return(false, nil)
		}

		log.Info("check chunk data equal failed", zap.String("failpoint", "CancelCheckChunkDataEqual"))
		cancelEqualFunc()
	})

	update := func() {
		ctx1, cancel1 := context.WithTimeout(ctx, dbutil.DefaultTimeout)
		defer cancel1()

		err1 := saveChunk(ctx1, t.CpDB, chunk.ID, t.TargetTable.InstanceID, t.TargetTable.Schema, t.TargetTable.Table, "", chunk)
		if err1 != nil {
			log.Warn("update chunk info", zap.Error(err1))
		}
	}

	defer func() {
		if chunk.State == ignoreState {
			t.summaryInfo.addIgnoreNum()
		} else {
			if err != nil {
				chunk.State = errorState
				t.summaryInfo.addFailedNum()
			} else {
				if equal {
					chunk.State = successState
					t.summaryInfo.addSuccessNum()
				} else {
					chunk.State = failedState
					t.summaryInfo.addFailedNum()
				}
			}
		}
		update()
	}()

	if filterByRand {
		rand.Seed(time.Now().UnixNano())
		r := rand.Intn(100)
		if r > t.Sample {
			chunk.State = ignoreState
			return true, nil
		}
	}

	chunk.State = checkingState
	update()

	if t.UseChecksum {
		// first check the checksum is equal or not
		equal, err = t.compareChecksum(ctx, chunk)
		if err != nil {
			return false, errors.Trace(err)
		}
		if equal {
			return true, nil
		}
	}

	if t.UseChecksum && t.OnlyUseChecksum {
		return false, nil
	}

	// if checksum is not equal or don't need compare checksum, compare the data
	log.Info("select data and then check data", zap.String("table", dbutil.TableName(t.TargetTable.Schema, t.TargetTable.Table)), zap.String("where", dbutil.ReplacePlaceholder(chunk.Where, chunk.Args)))

	equal, err = t.compareRows(ctx, chunk)
	if err != nil {
		return false, errors.Trace(err)
	}

	return equal, nil
}

// checksumInfo save some information about checksum
type checksumInfo struct {
	checksum int64
	err      error
	cost     time.Duration
	tp       string
}

// check the checksum is equal or not
func (t *TableDiff) compareChecksum(ctx context.Context, chunk *ChunkRange) (bool, error) {
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()

	var (
		getSourceChecksumDuration, getTargetChecksumDuration time.Duration
		sourceChecksum, targetChecksum                       int64
		checksumInfoCh                                       = make(chan checksumInfo)
		firstErr                                             error
	)
	defer close(checksumInfoCh)

	getChecksum := func(db *sql.DB, schema, table, limitRange string, tbInfo *model.TableInfo, args []interface{}, tp string) {
		beginTime := time.Now()
		checksum, err := dbutil.GetCRC32Checksum(ctx1, db, schema, table, tbInfo, limitRange, args)
		cost := time.Since(beginTime)

		checksumInfoCh <- checksumInfo{
			checksum: checksum,
			err:      err,
			cost:     cost,
			tp:       tp,
		}
	}

	args := utils.StringsToInterfaces(chunk.Args)
	for _, sourceTable := range t.SourceTables {
		go getChecksum(sourceTable.Conn, sourceTable.Schema, sourceTable.Table, chunk.Where, t.TargetTable.info, args, "source")
	}

	go getChecksum(t.TargetTable.Conn, t.TargetTable.Schema, t.TargetTable.Table, chunk.Where, t.TargetTable.info, args, "target")

	for i := 0; i < len(t.SourceTables)+1; i++ {
		checksumInfo := <-checksumInfoCh
		if checksumInfo.err != nil {
			// only need to return the first error, others are context cancel error
			if firstErr == nil {
				firstErr = checksumInfo.err
				cancel1()
			}

			continue
		}

		if checksumInfo.tp == "source" {
			sourceChecksum ^= checksumInfo.checksum
			if checksumInfo.cost > getSourceChecksumDuration {
				getSourceChecksumDuration = checksumInfo.cost
			}
		} else {
			targetChecksum = checksumInfo.checksum
			getTargetChecksumDuration = checksumInfo.cost
		}
	}

	if firstErr != nil {
		return false, errors.Trace(firstErr)
	}

	if sourceChecksum == targetChecksum {
		log.Info("checksum is equal", zap.String("table", dbutil.TableName(t.TargetTable.Schema, t.TargetTable.Table)), zap.String("where", dbutil.ReplacePlaceholder(chunk.Where, chunk.Args)), zap.Int64("checksum", sourceChecksum), zap.Duration("get source checksum cost", getSourceChecksumDuration), zap.Duration("get target checksum cost", getTargetChecksumDuration))
		return true, nil
	}

	log.Warn("checksum is not equal", zap.String("table", dbutil.TableName(t.TargetTable.Schema, t.TargetTable.Table)), zap.String("where", dbutil.ReplacePlaceholder(chunk.Where, chunk.Args)), zap.Int64("source checksum", sourceChecksum), zap.Int64("target checksum", targetChecksum), zap.Duration("get source checksum cost", getSourceChecksumDuration), zap.Duration("get target checksum cost", getTargetChecksumDuration))

	return false, nil
}

func (t *TableDiff) compareRows(ctx context.Context, chunk *ChunkRange) (bool, error) {
	beginTime := time.Now()

	sourceRows := make(map[int]*sql.Rows)
	sourceHaveData := make(map[int]bool)
	args := utils.StringsToInterfaces(chunk.Args)

	targetRows, orderKeyCols, err := getChunkRows(ctx, t.TargetTable.Conn, t.TargetTable.Schema, t.TargetTable.Table, t.TargetTable.info, chunk.Where, args, t.Collation)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer targetRows.Close()

	for i, sourceTable := range t.SourceTables {
		rows, _, err := getChunkRows(ctx, sourceTable.Conn, sourceTable.Schema, sourceTable.Table, sourceTable.info, chunk.Where, args, t.Collation)
		if err != nil {
			return false, errors.Trace(err)
		}
		defer rows.Close()

		sourceRows[i] = rows
		sourceHaveData[i] = false
	}

	sourceRowDatas := &RowDatas{
		Rows:         make([]RowData, 0, len(sourceRows)),
		OrderKeyCols: orderKeyCols,
	}
	heap.Init(sourceRowDatas)

	getRowData := func(rows *sql.Rows) (rowData map[string]*dbutil.ColumnData, err error) {
		for rows.Next() {
			rowData, err = dbutil.ScanRow(rows)
			return
		}
		return
	}

	// getSourceRow gets one row from all the sources, it should be the smallest.
	// first get rows from every source, and then push them to the heap, and then pop to get the smallest one
	getSourceRow := func() (map[string]*dbutil.ColumnData, error) {
		if len(sourceHaveData) == 0 {
			return nil, nil
		}

		needDeleteSource := make([]int, 0, 1)
		for i, haveData := range sourceHaveData {
			if !haveData {
				rowData, err := getRowData(sourceRows[i])
				if err != nil {
					return nil, err
				}

				if rowData != nil {
					sourceHaveData[i] = true
					heap.Push(sourceRowDatas, RowData{
						Data:   rowData,
						Source: i,
					})
				}
			}

			if !sourceHaveData[i] {
				if sourceRows[i].Err() != nil {
					return nil, sourceRows[i].Err()

				}
				// still don't have data, means the rows is read to the end, so delete the source
				needDeleteSource = append(needDeleteSource, i)
			}
		}

		for _, i := range needDeleteSource {
			delete(sourceHaveData, i)
		}

		// all the sources had read to the end, no data to return
		if len(sourceRowDatas.Rows) == 0 {
			return nil, nil
		}

		rowData := heap.Pop(sourceRowDatas).(RowData)
		sourceHaveData[rowData.Source] = false

		return rowData.Data, nil
	}

	var lastSourceData, lastTargetData map[string]*dbutil.ColumnData
	equal := true

	for {
		if lastSourceData == nil {
			lastSourceData, err = getSourceRow()
			if err != nil {
				return false, err
			}
		}

		if lastTargetData == nil {
			lastTargetData, err = getRowData(targetRows)
			if err != nil {
				return false, err
			}
		}

		if lastSourceData == nil {
			// don't have source data, so all the targetRows's data is redundant, should be deleted
			for lastTargetData != nil {
				sql := generateDML("delete", lastTargetData, t.TargetTable.info, t.TargetTable.Schema)
				log.Info("[delete]", zap.String("sql", sql))

				select {
				case t.sqlCh <- sql:
				case <-ctx.Done():
					return false, nil
				}
				equal = false

				lastTargetData, err = getRowData(targetRows)
				if err != nil {
					return false, err
				}
			}
			break
		}

		if lastTargetData == nil {
			// target lack some data, should insert the last source datas
			for lastSourceData != nil {
				sql := generateDML("replace", lastSourceData, t.TargetTable.info, t.TargetTable.Schema)
				log.Info("[insert]", zap.String("sql", sql))

				select {
				case t.sqlCh <- sql:
				case <-ctx.Done():
					return false, nil
				}
				equal = false

				lastSourceData, err = getSourceRow()
				if err != nil {
					return false, err
				}
			}
			break
		}

		eq, cmp, err := compareData(lastSourceData, lastTargetData, orderKeyCols)
		if err != nil {
			return false, errors.Trace(err)
		}
		if eq {
			lastSourceData = nil
			lastTargetData = nil
			continue
		}

		equal = false
		sql := ""

		switch cmp {
		case 1:
			// delete
			sql = generateDML("delete", lastTargetData, t.TargetTable.info, t.TargetTable.Schema)
			log.Info("[delete]", zap.String("sql", sql))
			lastTargetData = nil
		case -1:
			// insert
			sql = generateDML("replace", lastSourceData, t.TargetTable.info, t.TargetTable.Schema)
			log.Info("[insert]", zap.String("sql", sql))
			lastSourceData = nil
		case 0:
			// update
			sql = generateDML("replace", lastSourceData, t.TargetTable.info, t.TargetTable.Schema)
			log.Info("[update]", zap.String("sql", sql))
			lastSourceData = nil
			lastTargetData = nil
		}

		select {
		case t.sqlCh <- sql:
		case <-ctx.Done():
			return false, nil
		}
	}

	if equal {
		log.Info("rows is equal", zap.String("table", dbutil.TableName(t.TargetTable.Schema, t.TargetTable.Table)), zap.String("where", dbutil.ReplacePlaceholder(chunk.Where, chunk.Args)), zap.Duration("cost", time.Since(beginTime)))
	} else {
		log.Warn("rows is not equal", zap.String("table", dbutil.TableName(t.TargetTable.Schema, t.TargetTable.Table)), zap.String("where", dbutil.ReplacePlaceholder(chunk.Where, chunk.Args)), zap.Duration("cost", time.Since(beginTime)))
	}

	return equal, nil
}

// WriteSqls write sqls to file
func (t *TableDiff) WriteSqls(ctx context.Context, writeFixSQL func(string) error) chan bool {
	t.wg.Add(1)
	stopWriteCh := make(chan bool)

	go func() {
		defer t.wg.Done()

		stop := false
		for {
			select {
			case dml, ok := <-t.sqlCh:
				if !ok {
					return
				}

				err := writeFixSQL(fmt.Sprintf("%s\n", dml))
				if err != nil {
					log.Error("write sql failed", zap.String("sql", dml), zap.Error(err))
				}

			case <-stopWriteCh:
				stop = true
			case <-ctx.Done():
				return
			default:
				if stop {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	return stopWriteCh
}

// UpdateSummaryInfo updaets summary infomation
func (t *TableDiff) UpdateSummaryInfo(ctx context.Context) chan bool {
	t.wg.Add(1)
	stopUpdateCh := make(chan bool)

	go func() {
		update := func() {
			ctx1, cancel1 := context.WithTimeout(ctx, dbutil.DefaultTimeout)
			defer cancel1()

			err := updateTableSummary(ctx1, t.CpDB, t.TargetTable.InstanceID, t.TargetTable.Schema, t.TargetTable.Table, t.summaryInfo)
			if err != nil {
				log.Warn("save table summary info failed", zap.String("schema", t.TargetTable.Schema), zap.String("table", t.TargetTable.Table), zap.Error(err))
			}
		}
		defer func() {
			update()
			t.wg.Done()
		}()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-stopUpdateCh:
				return
			case <-ticker.C:
				if atomic.LoadInt32(&t.checkpointLoaded) == 1 {
					update()
				}
			}
		}
	}()

	return stopUpdateCh
}

func generateDML(tp string, data map[string]*dbutil.ColumnData, table *model.TableInfo, schema string) (sql string) {
	switch tp {
	case "replace":
		colNames := make([]string, 0, len(table.Columns))
		values := make([]string, 0, len(table.Columns))
		for _, col := range table.Columns {
			if col.IsGenerated() {
				continue
			}

			colNames = append(colNames, dbutil.ColumnName(col.Name.O))
			if data[col.Name.O].IsNull {
				values = append(values, "NULL")
				continue
			}

			if needQuotes(col.FieldType) {
				values = append(values, fmt.Sprintf("'%s'", strings.Replace(string(data[col.Name.O].Data), "'", "\\'", -1)))
			} else {
				values = append(values, string(data[col.Name.O].Data))
			}
		}

		sql = fmt.Sprintf("REPLACE INTO %s(%s) VALUES (%s);", dbutil.TableName(schema, table.Name.O), strings.Join(colNames, ","), strings.Join(values, ","))
	case "delete":
		kvs := make([]string, 0, len(table.Columns))
		for _, col := range table.Columns {
			if col.IsGenerated() {
				continue
			}

			if data[col.Name.O].IsNull {
				kvs = append(kvs, fmt.Sprintf("%s is NULL", dbutil.ColumnName(col.Name.O)))
				continue
			}

			if needQuotes(col.FieldType) {
				kvs = append(kvs, fmt.Sprintf("%s = '%s'", dbutil.ColumnName(col.Name.O), strings.Replace(string(data[col.Name.O].Data), "'", "\\'", -1)))
			} else {
				kvs = append(kvs, fmt.Sprintf("%s = %s", dbutil.ColumnName(col.Name.O), string(data[col.Name.O].Data)))
			}
		}
		sql = fmt.Sprintf("DELETE FROM %s WHERE %s;", dbutil.TableName(schema, table.Name.O), strings.Join(kvs, " AND "))
	default:
		log.Error("unknown sql type", zap.String("type", tp))
	}

	return
}

func compareData(map1, map2 map[string]*dbutil.ColumnData, orderKeyCols []*model.ColumnInfo) (equal bool, cmp int32, err error) {
	var (
		data1, data2 *dbutil.ColumnData
		key          string
		ok           bool
	)

	equal = true

	defer func() {
		if equal || err != nil {
			return
		}

		if cmp == 0 {
			log.Warn("find different row", zap.String("column", key), zap.String("row1", rowToString(map1)), zap.String("row2", rowToString(map2)))
		} else if cmp > 0 {
			log.Warn("target had superfluous data", zap.String("row", rowToString(map2)))
		} else {
			log.Warn("target lack data", zap.String("row", rowToString(map1)))
		}
	}()

	for key, data1 = range map1 {
		if data2, ok = map2[key]; !ok {
			return false, 0, errors.Errorf("don't have key %s", key)
		}
		if (string(data1.Data) == string(data2.Data)) && (data1.IsNull == data2.IsNull) {
			continue
		}
		equal = false

		break
	}
	if equal {
		return
	}

	for _, col := range orderKeyCols {
		if data1, ok = map1[col.Name.O]; !ok {
			err = errors.Errorf("don't have key %s", col.Name.O)
			return
		}
		if data2, ok = map2[col.Name.O]; !ok {
			err = errors.Errorf("don't have key %s", col.Name.O)
			return
		}
		if needQuotes(col.FieldType) {
			strData1 := string(data1.Data)
			strData2 := string(data2.Data)

			if len(strData1) == len(strData2) && strData1 == strData2 {
				continue
			}

			if strData1 < strData2 {
				cmp = -1
			} else if strData1 > strData2 {
				cmp = 1
			}
			break

		} else if data1.IsNull || data2.IsNull {
			if data1.IsNull && data2.IsNull {
				continue
			}
			if data1.IsNull {
				cmp = -1
			} else if data2.IsNull {
				cmp = 1
			}
			break
		} else {
			num1, err1 := strconv.ParseFloat(string(data1.Data), 64)
			num2, err2 := strconv.ParseFloat(string(data2.Data), 64)
			if err1 != nil || err2 != nil {
				err = errors.Errorf("convert %s, %s to float failed, err1: %v, err2: %v", string(data1.Data), string(data2.Data), err1, err2)
				return
			}

			if num1 == num2 {
				continue
			}

			if num1 < num2 {
				cmp = -1
			} else if num1 > num2 {
				cmp = 1
			}
			break
		}
	}

	return
}

func getChunkRows(ctx context.Context, db *sql.DB, schema, table string, tableInfo *model.TableInfo, where string,
	args []interface{}, collation string) (*sql.Rows, []*model.ColumnInfo, error) {
	orderKeys, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)

	columnNames := make([]string, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columnNames = append(columnNames, dbutil.ColumnName(col.Name.O))
	}
	columns := strings.Join(columnNames, ", ")

	if collation != "" {
		collation = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	for i, key := range orderKeys {
		orderKeys[i] = dbutil.ColumnName(key)
	}

	query := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM %s WHERE %s ORDER BY %s%s",
		columns, dbutil.TableName(schema, table), where, strings.Join(orderKeys, ","), collation)

	log.Debug("select data", zap.String("sql", query), zap.Reflect("args", args))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return rows, orderKeyCols, nil
}
