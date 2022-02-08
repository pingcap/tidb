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

	"github.com/pingcap/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
)

type dataSampleCheck struct {
	controller *Controller

	checkCfg      *config.CheckOnlyConfig
	checkTemplate Template
	wg            sync.WaitGroup

	totalRows                    atomic.Int64
	totalColumnCountMismatchRows atomic.Int64
	totalInvalidCharRows         atomic.Int64
}

func newDataSampleCheck(controller *Controller) *dataSampleCheck {
	return &dataSampleCheck{
		controller:    controller,
		checkTemplate: NewSimpleTemplate(),
		checkCfg:      controller.cfg.CheckOnlyCfg,
	}
}

func (d *dataSampleCheck) checkRoutine(ctx context.Context, fileChan chan *sampledDataFileInfo, errChan chan error) {
	defer d.wg.Done()

	var resultErr error
	defer func() {
		errChan <- errors.Trace(resultErr)
	}()

	rc := d.controller
	mydumperCfg := &rc.cfg.Mydumper
	for fileInfo := range fileChan {
		fileParser, err := rc.createDataFileParser(ctx, fileInfo.DataFile.FileMeta, config.DefaultCSVDataInvalidCharReplace)
		if err != nil {
			if errors.Cause(err) != context.Canceled {
				resultErr = err
			}
			return
		}

		var columnCountMismatchRows, invalidCharRows int64
		columnCount := len(fileInfo.TableInfo.Columns)
		for i := 0; i < d.checkCfg.Rows; i++ {
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
			if len(row.Row) != columnCount {
				columnCountMismatchRows++
			}

			if fileInfo.DataFile.FileMeta.Type != mydump.SourceTypeCSV ||
				mydumperCfg.DataCharacterSet != config.DefaultCSVDataCharacterSet {
				for _, col := range row.Row {
					colStr := col.GetString()
					if strings.Contains(colStr, config.DefaultCSVDataInvalidCharReplace) {
						invalidCharRows++
					}
				}
			}
			fileParser.RecycleRow(row)
		}

		fileParser.Close()

		if columnCountMismatchRows > 0 || invalidCharRows > 0 {
			log.L().Error("data error found in data file",
				zap.Reflect("file", fileInfo.DataFile.FileMeta),
				zap.Int64("column_count_mismatch_rows", columnCountMismatchRows),
				zap.Int64("invalid_char_rows", invalidCharRows))
		}

		d.totalRows.Add(int64(d.checkCfg.Rows))
		d.totalColumnCountMismatchRows.Add(columnCountMismatchRows)
		d.totalInvalidCharRows.Add(invalidCharRows)
	}
}

func (d *dataSampleCheck) doCheck(ctx context.Context) error {
	rc := d.controller
	mydumperCfg := &rc.cfg.Mydumper
	if mydumperCfg.DataCharacterSet == config.DefaultCSVDataCharacterSet {
		log.L().Warn("sample data check of csv files will be skipped since charset is binary")
	}

	targetFiles, err := d.getSampledDataFiles(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	concurrency := runtime.NumCPU()
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
			fmt.Printf("failed to do data sample check: %s", err.Error())
			return errors.Trace(err)
		}
	}

	d.wg.Wait()
	cancelFunc()

	passed := d.totalColumnCountMismatchRows.Load() == 0 && d.totalInvalidCharRows.Load() == 0
	msg := fmt.Sprintf("Total sample of %d rows of data checked, %d errors found.",
		d.totalRows, d.totalColumnCountMismatchRows.Load()+d.totalInvalidCharRows.Load())
	d.checkTemplate.Collect(Critical, passed, msg)

	fmt.Println(d.checkTemplate.Output())

	if d.checkTemplate.Success() {
		fmt.Println("All checks have been passed, but there may still be other types of errors that can only be found during the actual insertion of data.")
	} else {
		fmt.Println("Some checks failed, please check the log for more information.")
		fmt.Printf("Log file location: %s", rc.cfg.LogCfg.File)
		return errors.Errorf("tidb-lightning data file sample check failed: %s", d.checkTemplate.FailedMsg())
	}

	return nil
}

type sampledDataFileInfo struct {
	DataFile  *mydump.FileInfo
	Table     *mydump.MDTableMeta
	TableInfo *model.TableInfo
}

func (d *dataSampleCheck) getSampledDataFiles(ctx context.Context) ([]*sampledDataFileInfo, error) {
	rc := d.controller
	sqlParser := rc.tidbGlue.GetParser()

	dataFiles := d.getRandomDataFiles()
	stmtMap := make(map[string]*model.TableInfo)
	for _, fileInfo := range dataFiles {
		tableMeta := fileInfo.Table
		fullName := utils.EncloseDBAndTable(tableMeta.DB, tableMeta.Name)
		if t, ok := stmtMap[fullName]; ok {
			fileInfo.TableInfo = t
			continue
		}
		schema, err := tableMeta.GetSchema(ctx, rc.store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		stmtNodes, _, err := sqlParser.ParseSQL(schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(stmtNodes) != 1 {
			return nil, errors.Errorf("unexpected create table statement: %s", schema)
		}
		createTableStmt, ok := stmtNodes[0].(*ast.CreateTableStmt)
		if !ok {
			return nil, errors.Errorf("invalid create table statement")
		}

		fileInfo.TableInfo = createTableStmt.Table.TableInfo
		stmtMap[fullName] = fileInfo.TableInfo
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

	currSlice := allFiles
	for i := 0; i < count; i++ {
		j := rand.Int() % len(currSlice)
		currSlice[0], currSlice[j] = currSlice[j], currSlice[0]
		currSlice = currSlice[1:]
	}
	return allFiles[:count]
}
