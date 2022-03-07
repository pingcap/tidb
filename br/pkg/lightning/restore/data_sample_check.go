// Copyright 2022 PingCAP, Inc.
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
	"context"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/types"
)

type dataSampleCheck struct {
	controller *Controller

	checkCfg *config.CheckOnly
	wg       sync.WaitGroup

	totalRows                    atomic.Int64
	totalColumnCountMismatchRows atomic.Int64
	totalInvalidCharRows         atomic.Int64
}

func newDataSampleCheck(controller *Controller) *dataSampleCheck {
	return &dataSampleCheck{
		controller: controller,
		checkCfg:   controller.cfg.CheckOnly,
	}
}

func (d *dataSampleCheck) logCheckFailedRow(row []types.Datum, fileMeta mydump.SourceFileMeta, offset int64, errMsg string) {
	var sb strings.Builder
	for i, d := range row {
		if i > 0 {
			sb.WriteString(", ")
		}
		// see Datum.String()
		v := d.GetValue()
		if b, ok := v.([]byte); ok && d.Kind() == types.KindBytes {
			v = string(b)
		}
		sb.WriteString(fmt.Sprintf("%v", v))
	}
	log.L().Error("data check failed", zap.String("raw-data", sb.String()),
		zap.String("file", fileMeta.Path),
		zap.Int64("offset", offset),
		zap.String("error", errMsg))
}

func (d *dataSampleCheck) checkRoutine(ctx context.Context, fileChan chan *sampledDataFileInfo, errChan chan error) {
	defer d.wg.Done()

	var resultErr error
	defer func() {
		errChan <- errors.Trace(resultErr)
	}()

	rc := d.controller
	for fileInfo := range fileChan {
		fileMeta := fileInfo.DataFile.FileMeta
		fileParser, err := rc.createDataFileParser(ctx, fileMeta, config.DefaultCSVDataInvalidCharReplace)
		if err != nil {
			if errors.Cause(err) != context.Canceled {
				resultErr = err
			}
			return
		}

		ddlColumnMap := make(map[string]*model.ColumnInfo, len(fileInfo.TableInfo.Columns))
		for _, col := range fileInfo.TableInfo.Columns {
			ddlColumnMap[col.Name.L] = col
		}
		var columnNames []string
		var rowsChecked, columnCountMismatchRows, invalidCharRows, lastOffset int64
		columnCount := -1
		var count int64
		for {
			count++
			if d.checkCfg.Rows != config.CheckAllRows && count > d.checkCfg.Rows {
				break
			}
			err = fileParser.ReadRow()
			if err != nil {
				if errors.Cause(err) == io.EOF {
					break
				}

				if errors.Cause(err) != context.Canceled {
					resultErr = err
				}
				fileParser.Close()
				return
			}

			row := fileParser.LastRow()
			if columnCount == -1 {
				columnNames = fileParser.Columns()
				if len(columnNames) == 0 {
					columnCount = 0
					columnNames = make([]string, 0, columnCount)
					for _, col := range fileInfo.TableInfo.Columns {
						if !col.Hidden {
							columnNames = append(columnNames, col.Name.L)
							columnCount++
						}
					}
				} else {
					columnCount = len(columnNames)
				}
			}
			rowsChecked++
			if len(row.Row) != columnCount {
				columnCountMismatchRows++
				d.logCheckFailedRow(row.Row, fileMeta, lastOffset,
					fmt.Sprintf("column count mismatch, expect %d, got %d", columnCount, len(row.Row)))
			}

			for colIdx, col := range row.Row {
				if colIdx >= columnCount {
					break
				}

				columnInfo := ddlColumnMap[columnNames[colIdx]]
				if columnInfo == nil {
					// we don't check this kind of error here
					continue
				}
				colType := columnInfo.FieldType
				// we only check non-binary string types here
				if !types.IsNonBinaryStr(&colType) {
					continue
				}
				colStr := col.GetString()
				if !utf8.ValidString(colStr) || strings.Contains(colStr, config.DefaultCSVDataInvalidCharReplace) {
					invalidCharRows++
					d.logCheckFailedRow(row.Row, fileMeta, lastOffset, "contains invalid char")
				}
			}
			fileParser.RecycleRow(row)
			// lastOffset is the start offset of current row
			lastOffset, _ = fileParser.Pos()
		}

		fileParser.Close()

		d.totalRows.Add(rowsChecked)
		d.totalColumnCountMismatchRows.Add(columnCountMismatchRows)
		d.totalInvalidCharRows.Add(invalidCharRows)
	}
}

func (d *dataSampleCheck) doCheck(ctx context.Context) error {
	rc := d.controller

	targetFiles, err := d.getSampledDataFiles()
	if err != nil {
		return errors.Trace(err)
	}

	concurrency := utils.MinInt(runtime.NumCPU(), 8)
	fileChan := make(chan *sampledDataFileInfo, concurrency)
	errChan := make(chan error, concurrency)
	newCtx, cancelFunc := context.WithCancel(ctx)

	for i := 0; i < concurrency; i++ {
		d.wg.Add(1)
		go d.checkRoutine(newCtx, fileChan, errChan)
	}

	for _, fileInfo := range targetFiles {
		fileChan <- fileInfo
	}
	close(fileChan)

	// we need to exit if any error occurs in check routine
	// each routine should return a result in the error channel, return nil if there's no error.
	for i := 0; i < concurrency; i++ {
		if err = <-errChan; err != nil {
			cancelFunc()
			d.wg.Wait()

			log.L().Error("meet error when checking", zap.Error(err))
			fmt.Printf("failed to do data sample check: %s\n", err.Error())
			return errors.Trace(err)
		}
	}

	d.wg.Wait()
	cancelFunc()

	passed := d.totalColumnCountMismatchRows.Load() == 0 && d.totalInvalidCharRows.Load() == 0
	msg := fmt.Sprintf("Total sample of %d rows of data checked, %d errors found.",
		d.totalRows.Load(), d.totalColumnCountMismatchRows.Load()+d.totalInvalidCharRows.Load())
	rc.checkTemplate.Collect(Critical, passed, msg)

	if rc.tidbGlue.OwnsSQLExecutor() {
		fmt.Println(rc.checkTemplate.Output())

		if rc.checkTemplate.Success() {
			fmt.Println("All checks have been passed, but there may still be other types of errors that can only be found during the actual insertion of data.")
		} else {
			fmt.Println("Some checks failed, please check the log for more information.")
			fmt.Printf("Log file location: %s\n", rc.cfg.LogCfg.File)
		}
	}

	return nil
}

type sampledDataFileInfo struct {
	DataFile  *mydump.FileInfo
	Table     *mydump.MDTableMeta
	TableInfo *model.TableInfo
}

func (d *dataSampleCheck) getSampledDataFiles() ([]*sampledDataFileInfo, error) {
	rc := d.controller

	dataFiles := d.getRandomDataFiles()
	for _, fileInfo := range dataFiles {
		tableMeta := fileInfo.Table

		tableInfo := rc.dbInfos[tableMeta.DB].Tables[tableMeta.Name]
		fileInfo.TableInfo = tableInfo.Core
	}
	return dataFiles, nil
}

func (d *dataSampleCheck) getRandomDataFiles() []*sampledDataFileInfo {
	allFiles := make([]*sampledDataFileInfo, 0)

	rc := d.controller
	for _, dbInfo := range rc.dbMetas {
		for _, tableInfo := range dbInfo.Tables {
			for i := range tableInfo.DataFiles {
				allFiles = append(allFiles, &sampledDataFileInfo{
					DataFile: &tableInfo.DataFiles[i],
					Table:    tableInfo,
				})
			}
		}
	}

	count := int(float64(len(allFiles)) * d.checkCfg.Rate)
	if count <= 0 {
		count = 1
	}
	if count > len(allFiles) {
		count = len(allFiles)
	}

	if count == len(allFiles) {
		return allFiles
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec G404
	rnd.Shuffle(len(allFiles), func(i, j int) {
		allFiles[i], allFiles[j] = allFiles[j], allFiles[i]
	})
	return allFiles[:count]
}
