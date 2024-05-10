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
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/duplicate"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/extsort"
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

	handlerConstructor := makeDupHandlerConstructor(ignoreRows, d.rc.cfg.Conflict.Strategy)
	numDups, err = detector.Detect(ctx, &duplicate.DetectOptions{
		Concurrency:        d.rc.cfg.App.RegionConcurrency,
		HandlerConstructor: handlerConstructor,
	})
	return errors.Trace(err)
}

func makeDupHandlerConstructor(
	sorter extsort.ExternalSorter, onDup config.DuplicateResolutionAlgorithm,
) duplicate.HandlerConstructor {
	switch onDup {
	case config.ErrorOnDup:
		return func(context.Context) (duplicate.Handler, error) {
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
	default:
		panic(fmt.Sprintf("unexpected conflict.strategy: %s", onDup))
	}
}

// ErrDuplicateKey is an error class for duplicate key error.
var ErrDuplicateKey = errors.Normalize("duplicate key detected on indexID %d of KeyID: %v", errors.RFCCodeText("Lightning:PreDedup:ErrDuplicateKey"))

var (
	_ duplicate.Handler = &errorOnDup{}
	_ duplicate.Handler = &replaceOnDup{}
)

type errorOnDup struct {
	idxID  int64
	keyIDs [][]byte
}

func (h *errorOnDup) Begin(key []byte) error {
	idxID, err := decodeIndexID(key)
	if err != nil {
		return err
	}
	h.idxID = idxID
	return nil
}

func (h *errorOnDup) Append(keyID []byte) error {
	if len(h.keyIDs) >= 2 {
		// we only need 2 keyIDs to report the error.
		return nil
	}
	h.keyIDs = append(h.keyIDs, slices.Clone(keyID))
	return nil
}
func (h *errorOnDup) End() error {
	return ErrDuplicateKey.GenWithStackByArgs(h.idxID, h.keyIDs)
}
func (*errorOnDup) Close() error { return nil }

type replaceOnDup struct {
	// All keyIDs except the last one will be written to w.
	// keyID written to w will be ignored during importing.
	w     extsort.Writer
	keyID []byte
	idxID []byte // Varint encoded indexID
}

func (h *replaceOnDup) Begin(key []byte) error {
	h.keyID = h.keyID[:0]
	idxID, err := decodeIndexID(key)
	if err != nil {
		return err
	}
	h.idxID = codec.EncodeVarint(nil, idxID)
	return nil
}

func (h *replaceOnDup) Append(keyID []byte) error {
	if len(h.keyID) > 0 {
		if err := h.w.Put(h.keyID, h.idxID); err != nil {
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
	tblInfo, colPerm = simplifyTable(tblInfo, colPerm)
	encTable, err := tables.TableFromMeta(d.tr.alloc, tblInfo)
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

	for {
		lastRow := parser.LastRow()
		lastRow.Row = append(lastRow.Row, extendVals...)

		row, err := kvEncoder.Encode(lastRow.Row, lastRow.RowID, colPerm, offset)
		if err != nil {
			return errors.Trace(err)
		}
		for _, kvPair := range kv.Row2KvPairs(row) {
			if err := adder.Add(kvPair.Key, kvPair.RowID); err != nil {
				kv.ClearRow(row)
				return err
			}
		}
		kv.ClearRow(row)

		offset, _ = parser.Pos()
		if err := parser.ReadRow(); err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}
			return err
		}
	}
}

// simplifyTable simplifies the table structure for duplicate detection.
// It tries to remove all unused indices and columns and make the table
// structure as simple as possible.
func simplifyTable(
	tblInfo *model.TableInfo, colPerm []int,
) (_ *model.TableInfo, newColPerm []int) {
	newTblInfo := tblInfo.Clone()

	var usedIndices []*model.IndexInfo
	usedColOffsets := make(map[int]struct{})
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.Primary || idxInfo.Unique {
			usedIndices = append(usedIndices, idxInfo.Clone())
			for _, col := range idxInfo.Columns {
				usedColOffsets[col.Offset] = struct{}{}
			}
		}
	}
	if tblInfo.PKIsHandle {
		usedColOffsets[tblInfo.GetPkColInfo().Offset] = struct{}{}
	}
	newTblInfo.Indices = usedIndices

	hasGenCols := false
	for _, col := range tblInfo.Columns {
		if col.IsGenerated() {
			hasGenCols = true
			break
		}
	}

	newColPerm = colPerm
	// If there is no generated column, we can remove all unused columns.
	// TODO: We can also remove columns that are not referenced by generated columns.
	if !hasGenCols {
		newCols := make([]*model.ColumnInfo, 0, len(usedColOffsets))
		newColPerm = make([]int, 0, len(usedColOffsets)+1)
		colNameOffsets := make(map[string]int)
		for i, col := range tblInfo.Columns {
			if _, ok := usedColOffsets[i]; ok {
				newCol := col.Clone()
				newCol.Offset = len(newCols)
				newCols = append(newCols, newCol)
				colNameOffsets[col.Name.L] = newCol.Offset
				newColPerm = append(newColPerm, colPerm[i])
			}
		}
		if common.TableHasAutoRowID(tblInfo) {
			newColPerm = append(newColPerm, colPerm[len(tblInfo.Columns)])
		}
		newTblInfo.Columns = newCols

		// Fix up the index columns offset.
		for _, idxInfo := range newTblInfo.Indices {
			for _, col := range idxInfo.Columns {
				col.Offset = colNameOffsets[col.Name.L]
			}
		}
	}
	return newTblInfo, newColPerm
}

const conflictOnHandle = int64(-1)

func decodeIndexID(key []byte) (int64, error) {
	switch {
	case tablecodec.IsRecordKey(key):
		return conflictOnHandle, nil
	case tablecodec.IsIndexKey(key):
		_, idxID, _, err := tablecodec.DecodeIndexKey(key)
		return idxID, errors.Trace(err)

	default:
		return 0, errors.Errorf("unexpected key: %X, expected a record key or index key", key)
	}
}
