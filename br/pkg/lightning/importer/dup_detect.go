// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"context"
	"fmt"
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/duplicate"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/keyspace"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/extsort"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type dupDetector struct {
	tr     *TableImporter
	rc     *Controller
	cp     *checkpoints.TableCheckpoint
	logger log.Logger
}

func (d *dupDetector) run(
	ctx context.Context,
	workingDir string,
	ignoreRows extsort.ExternalSorter,
) (retErr error) {
	var numDups int64
	task := d.logger.Begin(zap.InfoLevel, "duplicate detection")
	defer func() {
		task.End(zap.ErrorLevel, retErr, zap.Int64("numDups", numDups))
	}()

	sorter, err := extsort.OpenDiskSorter(workingDir, &extsort.DiskSorterOptions{
		Concurrency: d.rc.cfg.App.RegionConcurrency,
	})
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		_ = sorter.CloseAndCleanup()
	}()

	detector := duplicate.NewDetector(sorter, d.logger)
	if err := d.addKeys(ctx, detector); err != nil {
		return errors.Trace(err)
	}

	handlerConstructor := makeDupHandlerConstructor(ignoreRows, d.rc.cfg.TikvImporter.OnDuplicate)
	numDups, err = detector.Detect(ctx, &duplicate.DetectOptions{
		Concurrency:        d.rc.cfg.App.RegionConcurrency,
		HandlerConstructor: handlerConstructor,
	})
	return errors.Trace(err)
}

func makeDupHandlerConstructor(
	sorter extsort.ExternalSorter, onDup string,
) duplicate.HandlerConstructor {
	switch onDup {
	case config.ErrorOnDup:
		return func(ctx context.Context) (duplicate.Handler, error) {
			return &errorOnDup{}, nil
		}
	case config.ReplaceOnDup:
		return func(ctx context.Context) (duplicate.Handler, error) {
			w, err := sorter.NewWriter(ctx)
			if err != nil {
				return nil, err
			}
			return &replaceOnDup{w: w}, nil
		}
	case config.IgnoreOnDup:
		return func(ctx context.Context) (duplicate.Handler, error) {
			w, err := sorter.NewWriter(ctx)
			if err != nil {
				return nil, err
			}
			return &ignoreOnDup{w: w}, nil
		}
	default:
		panic(fmt.Sprintf("unexpected on-duplicate strategy: %s", onDup))
	}
}

var (
	_ duplicate.Handler = &errorOnDup{}
	_ duplicate.Handler = &replaceOnDup{}
	_ duplicate.Handler = &ignoreOnDup{}
)

type errorOnDup struct{}

func (errorOnDup) Begin(key []byte) error {
	// TODO: add more useful information to the error message.
	return errors.Errorf("duplicate key detected: %X", key)
}

func (errorOnDup) Append(_ []byte) error { return nil }
func (errorOnDup) End() error            { return nil }
func (errorOnDup) Close() error          { return nil }

type replaceOnDup struct {
	// All keyIDs except the last one will be written to w.
	// keyID written to w will be ignored during importing.
	w     extsort.Writer
	keyID []byte
}

func (h *replaceOnDup) Begin(_ []byte) error {
	h.keyID = h.keyID[:0]
	return nil
}

func (h *replaceOnDup) Append(keyID []byte) error {
	if len(h.keyID) > 0 {
		if err := h.w.Put(h.keyID, nil); err != nil {
			return err
		}
	}
	h.keyID = append(h.keyID[:0], keyID...)
	return nil
}

func (*replaceOnDup) End() error {
	return nil
}

func (h *replaceOnDup) Close() error {
	return h.w.Close()
}

type ignoreOnDup struct {
	// All keyIDs except the first one will be written to w.
	// keyID written to w will be ignored during importing.
	w     extsort.Writer
	first bool
}

func (h *ignoreOnDup) Begin(_ []byte) error {
	h.first = true
	return nil
}

func (h *ignoreOnDup) Append(keyID []byte) error {
	if h.first {
		h.first = false
		return nil
	}
	return h.w.Put(keyID, nil)
}

func (*ignoreOnDup) End() error {
	return nil
}

func (h *ignoreOnDup) Close() error {
	return h.w.Close()
}

func (d *dupDetector) addKeys(ctx context.Context, detector *duplicate.Detector) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(d.rc.cfg.App.RegionConcurrency)

	for engineID, ecp := range d.cp.Engines {
		if engineID < 0 {
			// Ignore index engine.
			continue
		}
		for _, chunk := range ecp.Chunks {
			chunk := chunk
			g.Go(func() error {
				adder, err := detector.KeyAdder(ctx)
				if err != nil {
					return errors.Trace(err)
				}

				if err := d.addKeysByChunk(ctx, adder, chunk); err != nil {
					_ = adder.Close()
					return errors.Trace(err)
				}
				return adder.Flush()
			})
		}
	}
	return g.Wait()
}

func (d *dupDetector) addKeysByChunk(
	ctx context.Context,
	adder *duplicate.KeyAdder,
	chunk *checkpoints.ChunkCheckpoint,
) error {
	parser, err := openParser(ctx, d.rc.cfg, chunk, d.rc.ioWorkers, d.rc.store, d.tr.tableInfo.Core)
	if err != nil {
		return err
	}
	defer func() {
		_ = parser.Close()
	}()

	offset, _ := parser.Pos()
	if err := parser.ReadRow(); err != nil {
		if errors.Cause(err) == io.EOF {
			return nil
		}
		return err
	}
	columnNames := parser.Columns()

	cfg := d.rc.cfg
	tblInfo := d.tr.tableInfo.Core

	// 1. Initialize the column permutation.
	igCols, err := d.rc.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(d.tr.dbInfo.Name, tblInfo.Name.O, cfg.Mydumper.CaseSensitive)
	if err != nil {
		return errors.Trace(err)
	}
	ignoreColsMap := igCols.ColumnsMap()
	colPerm, err := createColumnPermutation(columnNames, ignoreColsMap, tblInfo, d.logger)
	if err != nil {
		return errors.Trace(err)
	}

	// 2. Initialize the extend column values.
	var extendVals []types.Datum
	if len(chunk.FileMeta.ExtendData.Columns) > 0 {
		_, extendVals = filterColumns(columnNames, chunk.FileMeta.ExtendData, ignoreColsMap, tblInfo)
	}
	lastRow := parser.LastRow()
	lastRowLen := len(lastRow.Row)
	extendColsMap := make(map[string]int)
	for i, c := range chunk.FileMeta.ExtendData.Columns {
		extendColsMap[c] = lastRowLen + i
	}
	for i, col := range tblInfo.Columns {
		if p, ok := extendColsMap[col.Name.O]; ok {
			colPerm[i] = p
		}
	}

	// 3. Simplify table structure and create kv encoder.
	encTable, colPerm, err := d.simplifyTable(colPerm)
	if err != nil {
		return errors.Trace(err)
	}
	kvEncoder, err := d.rc.encBuilder.NewEncoder(ctx, &encode.EncodingConfig{
		SessionOptions: encode.SessionOptions{
			SQLMode:   d.rc.cfg.TiDB.SQLMode,
			Timestamp: chunk.Timestamp,
			SysVars:   d.rc.sysVars,
			// use chunk.PrevRowIDMax as the auto random seed, so it can stay the same value after recover from checkpoint.
			AutoRandomSeed: chunk.Chunk.PrevRowIDMax,
		},
		Path:   chunk.Key.Path,
		Table:  encTable,
		Logger: d.logger,
	})
	if err != nil {
		return errors.Trace(err)
	}

	dataKVs := d.rc.encBuilder.MakeEmptyRows()
	indexKVs := d.rc.encBuilder.MakeEmptyRows()
	c := keyspace.CodecV1
	if d.tr.kvStore != nil {
		c = d.tr.kvStore.GetCodec()
	}
	dataChecksum := verify.NewKVChecksumWithKeyspace(c)
	indexChecksum := verify.NewKVChecksumWithKeyspace(c)

	for {
		lastRow := parser.LastRow()
		lastRow.Row = append(lastRow.Row, extendVals...)

		kvs, err := kvEncoder.Encode(lastRow.Row, lastRow.RowID, colPerm, offset)
		if err != nil {
			return errors.Trace(err)
		}
		kvs.ClassifyAndAppend(&dataKVs, dataChecksum, &indexKVs, indexChecksum)

		for _, kvPair := range kv.Rows2KvPairs(dataKVs) {
			if err := adder.Add(kvPair.Key, kvPair.RowID); err != nil {
				return err
			}
		}
		for _, kvPair := range kv.Rows2KvPairs(indexKVs) {
			if err := adder.Add(kvPair.Key, kvPair.RowID); err != nil {
				return err
			}
		}
		dataKVs.Clear()
		indexKVs.Clear()

		offset, _ = parser.Pos()
		if err := parser.ReadRow(); err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (d *dupDetector) simplifyTable(colPerm []int) (_ table.Table, newColPerm []int, _ error) {
	tblInfo := d.tr.tableInfo.Core.Clone()

	var usedIndices []*model.IndexInfo
	usedCols := make(map[int]struct{})
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.Primary || idxInfo.Unique {
			usedIndices = append(usedIndices, idxInfo)
			for _, col := range idxInfo.Columns {
				usedCols[col.Offset] = struct{}{}
			}
		}
	}
	tblInfo.Indices = usedIndices
	if tblInfo.PKIsHandle {
		usedCols[tblInfo.GetPkColInfo().Offset] = struct{}{}
	}

	hasGenCols := false
	for _, col := range tblInfo.Columns {
		if col.IsGenerated() {
			hasGenCols = true
			break
		}
	}

	encTable := d.tr.encTable
	newColPerm = colPerm
	// If there is no generated column, we can remove all unused columns.
	// TODO: We can also remove columns that are not referenced by generated columns.
	if !hasGenCols {
		cols := make([]*model.ColumnInfo, 0, len(tblInfo.Columns))
		colOffsets := make(map[string]int)
		newColPerm = make([]int, 0, len(tblInfo.Columns))
		for i, col := range tblInfo.Columns {
			if _, ok := usedCols[i]; ok {
				col.Offset = len(cols)
				cols = append(cols, col)
				colOffsets[col.Name.L] = col.Offset
				newColPerm = append(newColPerm, colPerm[i])
			}
		}
		// Fix up the index columns offset.
		for _, idxInfo := range tblInfo.Indices {
			for _, col := range idxInfo.Columns {
				col.Offset = colOffsets[col.Name.L]
			}
		}
		tblInfo.Columns = cols

		// Rebuild the table meta.
		tbl, err := tables.TableFromMeta(d.tr.alloc, tblInfo)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		encTable = tbl
	}
	return encTable, newColPerm, nil
}
