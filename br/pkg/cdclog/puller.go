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

package cdclog

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// EventPuller pulls next event in ts order.
type EventPuller struct {
	ddlDecoder            *JSONEventBatchMixedDecoder
	rowChangedDecoder     *JSONEventBatchMixedDecoder
	currentDDLItem        *SortItem
	currentRowChangedItem *SortItem

	schema string
	table  string

	storage         storage.ExternalStorage
	ddlFiles        []string
	rowChangedFiles []string

	ddlFileIndex        int
	rowChangedFileIndex int
}

// NewEventPuller create eventPuller by given log files, we assume files come in ts order.
func NewEventPuller(
	ctx context.Context,
	schema string,
	table string,
	ddlFiles []string,
	rowChangedFiles []string,
	storage storage.ExternalStorage) (*EventPuller, error) {
	var (
		ddlDecoder        *JSONEventBatchMixedDecoder
		ddlFileIndex      int
		rowChangedDecoder *JSONEventBatchMixedDecoder
		rowFileIndex      int
	)
	if len(ddlFiles) == 0 {
		log.Info("There is no ddl file to restore")
	} else {
		data, err := storage.ReadFile(ctx, ddlFiles[0])
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(data) != 0 {
			ddlFileIndex++
			ddlDecoder, err = NewJSONEventBatchDecoder(data)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	if len(rowChangedFiles) == 0 {
		log.Info("There is no row changed file to restore")
	} else {
		data, err := storage.ReadFile(ctx, rowChangedFiles[0])
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(data) != 0 {
			rowFileIndex++
			rowChangedDecoder, err = NewJSONEventBatchDecoder(data)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	return &EventPuller{
		schema: schema,
		table:  table,

		ddlDecoder:        ddlDecoder,
		rowChangedDecoder: rowChangedDecoder,

		ddlFiles:            ddlFiles,
		rowChangedFiles:     rowChangedFiles,
		ddlFileIndex:        ddlFileIndex,
		rowChangedFileIndex: rowFileIndex,

		storage: storage,
	}, nil
}

// PullOneEvent pulls one event in ts order.
// The Next event which can be DDL item or Row changed Item depends on next commit ts.
func (e *EventPuller) PullOneEvent(ctx context.Context) (*SortItem, error) {
	var (
		err  error
		data []byte
	)
	// ddl exists
	if e.ddlDecoder != nil {
		// current file end, read next file if next file exists
		if !e.ddlDecoder.HasNext() && e.ddlFileIndex < len(e.ddlFiles) {
			path := e.ddlFiles[e.ddlFileIndex]
			data, err = e.storage.ReadFile(ctx, path)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(data) > 0 {
				e.ddlFileIndex++
				e.ddlDecoder, err = NewJSONEventBatchDecoder(data)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
		// set current DDL item first
		if e.currentDDLItem == nil {
			e.currentDDLItem, err = e.ddlDecoder.NextEvent(DDL)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	// dml exists
	if e.rowChangedDecoder != nil {
		// current file end, read next file if next file exists
		if !e.rowChangedDecoder.HasNext() && e.rowChangedFileIndex < len(e.rowChangedFiles) {
			path := e.rowChangedFiles[e.rowChangedFileIndex]
			data, err = e.storage.ReadFile(ctx, path)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(data) != 0 {
				e.rowChangedFileIndex++
				e.rowChangedDecoder, err = NewJSONEventBatchDecoder(data)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
		if e.currentRowChangedItem == nil {
			e.currentRowChangedItem, err = e.rowChangedDecoder.NextEvent(RowChanged)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	var returnItem *SortItem
	switch {
	case e.currentDDLItem != nil:
		if e.currentDDLItem.LessThan(e.currentRowChangedItem) {
			returnItem = e.currentDDLItem
			e.currentDDLItem, err = e.ddlDecoder.NextEvent(DDL)
			if err != nil {
				return nil, errors.Trace(err)
			}
			break
		}
		fallthrough
	case e.currentRowChangedItem != nil:
		returnItem = e.currentRowChangedItem
		e.currentRowChangedItem, err = e.rowChangedDecoder.NextEvent(RowChanged)
		if err != nil {
			return nil, errors.Trace(err)
		}
	default:
		log.Info("puller finished")
	}
	return returnItem, nil
}
