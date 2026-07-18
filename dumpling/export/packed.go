// Copyright 2026 PingCAP, Inc.
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

package export

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/kv"
	tidbmeta "github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/structure"
	tidbtable "github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	packedSlowScanThreshold    = 10 * time.Second
	packedSlowTableThreshold   = 30 * time.Second
	packedNoProgressThreshold  = 5 * time.Minute
	packedScanProgressRows     = 1024
	packedScanProgressBytes    = 1 << 20
	packedMaxSlowOperationLogs = 3
)

type packedTableMeta struct {
	database string
	table    string
	columns  []*model.ColumnInfo
	types    []string
	names    []string
	selected string
	create   string
}

func newPackedTableMeta(database string, table *model.TableInfo, createSQL string) *packedTableMeta {
	columns := packedVisibleColumns(table)
	types := make([]string, 0, len(columns))
	names := make([]string, 0, len(columns))
	selected := make([]string, 0, len(columns))
	for _, column := range columns {
		types = append(types, packedColumnType(column))
		names = append(names, column.Name.O)
		selected = append(selected, wrapBackTicks(column.Name.O))
	}
	return &packedTableMeta{
		database: database,
		table:    table.Name.O,
		columns:  columns,
		types:    types,
		names:    names,
		selected: strings.Join(selected, ","),
		create:   createSQL,
	}
}

func (*packedTableMeta) SpecialComments() StringIter { return newStringIter() }
func (m *packedTableMeta) DatabaseName() string      { return m.database }
func (m *packedTableMeta) TableName() string         { return m.table }
func (m *packedTableMeta) ColumnCount() uint         { return uint(len(m.columns)) }
func (m *packedTableMeta) ColumnTypes() []string     { return m.types }
func (m *packedTableMeta) ColumnNames() []string     { return m.names }
func (m *packedTableMeta) SelectedField() string     { return m.selected }
func (m *packedTableMeta) SelectedLen() int          { return len(m.columns) }
func (m *packedTableMeta) ShowCreateTable() string   { return m.create }
func (*packedTableMeta) ShowCreateView() string      { return "" }
func (*packedTableMeta) AvgRowLength() uint64        { return 0 }
func (*packedTableMeta) HasImplicitRowID() bool      { return false }

func packedColumnType(column *model.ColumnInfo) string {
	if column.GetCharset() == charset.CharsetBin {
		switch column.GetType() {
		case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			return "BLOB"
		}
	}
	switch column.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return "BIGINT"
	case mysql.TypeFloat:
		return "FLOAT"
	case mysql.TypeDouble:
		return "DOUBLE"
	case mysql.TypeNewDecimal:
		return "DECIMAL"
	case mysql.TypeBit:
		return "BIT"
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeGeometry:
		return "BLOB"
	case mysql.TypeDate, mysql.TypeNewDate:
		return "DATE"
	case mysql.TypeDatetime:
		return "DATETIME"
	case mysql.TypeTimestamp:
		return "TIMESTAMP"
	case mysql.TypeDuration:
		return "TIME"
	case mysql.TypeYear:
		return "YEAR"
	case mysql.TypeEnum:
		return "ENUM"
	case mysql.TypeSet:
		return "SET"
	case mysql.TypeJSON:
		return "JSON"
	default:
		return "VARCHAR"
	}
}

type packedTableData struct {
	executable       string
	metadataURL      string
	legacyEncryption bool
	database         string
	table            *model.TableInfo
	ranges           []packedRange
	scanTotals       *packedScanTotals
	iter             *packedRowIter
}

type packedRange struct {
	physicalTableID int64
	start           []byte
	end             []byte
}

type packedScanTotals struct {
	planned              atomic.Int64
	active               atomic.Int64
	started              atomic.Uint64
	completed            atomic.Uint64
	failed               atomic.Uint64
	canceled             atomic.Uint64
	slow                 atomic.Uint64
	rows                 atomic.Uint64
	rawBytes             atomic.Uint64
	durationNano         atomic.Int64
	maxDurationNano      atomic.Int64
	maxProcessSpawnNano  atomic.Int64
	maxFirstRowNano      atomic.Int64
	protocolReadNano     atomic.Int64
	maxProtocolReadNano  atomic.Int64
	processWaitNano      atomic.Int64
	maxProcessWaitNano   atomic.Int64
	childUserCPUNano     atomic.Int64
	childSystemCPUNano   atomic.Int64
	missingCSEStats      atomic.Uint64
	invalidCSEStats      atomic.Uint64
	slowScanLogSlots     atomic.Uint64
	slowTableLogSlots    atomic.Uint64
	tables               atomic.Uint64
	failedTables         atomic.Uint64
	tableDurationNano    atomic.Int64
	maxTableDurationNano atomic.Int64
	rowDecodeNano        atomic.Int64
	maxRowDecodeNano     atomic.Int64
	cseIdentityOnce      sync.Once
	cse                  packedCSETotals
}

type packedCSETotals struct {
	reportedScans             atomic.Uint64
	manifestBytesRead         atomic.Uint64
	manifestShards            atomic.Uint64
	contentFiles              atomic.Uint64
	plaintextShards           atomic.Uint64
	legacyShards              atomic.Uint64
	cmekShards                atomic.Uint64
	mixedEncryptionShards     atomic.Uint64
	shardsScanned             atomic.Uint64
	slowShards                atomic.Uint64
	scannedPlaintextShards    atomic.Uint64
	scannedLegacyShards       atomic.Uint64
	scannedCMEKShards         atomic.Uint64
	scannedMixedShards        atomic.Uint64
	sstCandidates             atomic.Uint64
	sstRetained               atomic.Uint64
	missingSSTBounds          atomic.Uint64
	sstCandidateBytesHint     atomic.Uint64
	sstRetainedBytesHint      atomic.Uint64
	contentReadAttempts       atomic.Uint64
	contentReads              atomic.Uint64
	contentReadFailures       atomic.Uint64
	slowContentReads          atomic.Uint64
	contentBytes              atomic.Uint64
	rows                      atomic.Uint64
	keyBytes                  atomic.Uint64
	valueBytes                atomic.Uint64
	stdoutBytes               atomic.Uint64
	stdoutWriteCalls          atomic.Uint64
	stdoutWriteFailures       atomic.Uint64
	maxFirstRowProducedNano   atomic.Int64
	maxFirstStdoutWriteNano   atomic.Int64
	maxScanFirstRowNano       atomic.Int64
	manifestLoadNano          atomic.Int64
	legacyKeyLoadNano         atomic.Int64
	readerInitNano            atomic.Int64
	rangeFilterNano           atomic.Int64
	snapshotLoadNano          atomic.Int64
	iteratorInitNano          atomic.Int64
	plaintextSnapshotLoadNano atomic.Int64
	legacySnapshotLoadNano    atomic.Int64
	cmekSnapshotLoadNano      atomic.Int64
	mixedSnapshotLoadNano     atomic.Int64
	contentReadNano           atomic.Int64
	maxContentReadNano        atomic.Int64
	iterateEmitNano           atomic.Int64
	maxIterateEmitNano        atomic.Int64
	maxRangeFilterNano        atomic.Int64
	maxSnapshotLoadNano       atomic.Int64
	maxIteratorInitNano       atomic.Int64
	stdoutWriteNano           atomic.Int64
	maxStdoutWriteNano        atomic.Int64
	scanNano                  atomic.Int64
	flushNano                 atomic.Int64
	totalNano                 atomic.Int64
	maxPeakRSSBytes           atomic.Uint64
}

type packedCSETotalsSnapshot struct {
	reportedScans          uint64
	manifestBytesRead      uint64
	manifestShards         uint64
	contentFiles           uint64
	plaintextShards        uint64
	legacyShards           uint64
	cmekShards             uint64
	mixedEncryptionShards  uint64
	shardsScanned          uint64
	slowShards             uint64
	scannedPlaintextShards uint64
	scannedLegacyShards    uint64
	scannedCMEKShards      uint64
	scannedMixedShards     uint64
	sstCandidates          uint64
	sstRetained            uint64
	missingSSTBounds       uint64
	sstCandidateBytesHint  uint64
	sstRetainedBytesHint   uint64
	contentReadAttempts    uint64
	contentReads           uint64
	contentReadFailures    uint64
	slowContentReads       uint64
	contentBytes           uint64
	rows                   uint64
	keyBytes               uint64
	valueBytes             uint64
	stdoutBytes            uint64
	stdoutWriteCalls       uint64
	stdoutWriteFailures    uint64
	maxFirstRowProduced    time.Duration
	maxFirstStdoutWrite    time.Duration
	maxScanFirstRow        time.Duration
	manifestLoad           time.Duration
	legacyKeyLoad          time.Duration
	readerInit             time.Duration
	rangeFilter            time.Duration
	snapshotLoad           time.Duration
	iteratorInit           time.Duration
	plaintextSnapshotLoad  time.Duration
	legacySnapshotLoad     time.Duration
	cmekSnapshotLoad       time.Duration
	mixedSnapshotLoad      time.Duration
	contentRead            time.Duration
	maxContentRead         time.Duration
	iterateEmit            time.Duration
	maxIterateEmit         time.Duration
	maxRangeFilter         time.Duration
	maxSnapshotLoad        time.Duration
	maxIteratorInit        time.Duration
	stdoutWrite            time.Duration
	maxStdoutWrite         time.Duration
	scan                   time.Duration
	flush                  time.Duration
	total                  time.Duration
	maxPeakRSSBytes        uint64
}

type packedScanTotalsSnapshot struct {
	planned          int64
	active           int64
	started          uint64
	completed        uint64
	failed           uint64
	canceled         uint64
	slow             uint64
	rows             uint64
	rawBytes         uint64
	duration         time.Duration
	maxDuration      time.Duration
	maxProcessSpawn  time.Duration
	maxFirstRow      time.Duration
	protocolRead     time.Duration
	maxProtocolRead  time.Duration
	processWait      time.Duration
	maxProcessWait   time.Duration
	childUserCPU     time.Duration
	childSystemCPU   time.Duration
	missingCSEStats  uint64
	invalidCSEStats  uint64
	tables           uint64
	failedTables     uint64
	tableDuration    time.Duration
	maxTableDuration time.Duration
	rowDecode        time.Duration
	maxRowDecode     time.Duration
	cse              packedCSETotalsSnapshot
}

type packedScanProgress struct {
	totals       *packedScanTotals
	pendingRows  uint64
	pendingBytes uint64
}

type packedOutputTotals struct {
	createCalls    atomic.Uint64
	createFailures atomic.Uint64
	writeCalls     atomic.Uint64
	writeFailures  atomic.Uint64
	closeCalls     atomic.Uint64
	closeFailures  atomic.Uint64
	bytes          atomic.Uint64
	createNano     atomic.Int64
	writeNano      atomic.Int64
	closeNano      atomic.Int64
	maxCreateNano  atomic.Int64
	maxWriteNano   atomic.Int64
	maxCloseNano   atomic.Int64
	activeCreates  atomic.Int64
	activeWrites   atomic.Int64
	activeCloses   atomic.Int64
	slowLogSlots   atomic.Uint64
}

type packedOutputTotalsSnapshot struct {
	createCalls    uint64
	createFailures uint64
	writeCalls     uint64
	writeFailures  uint64
	closeCalls     uint64
	closeFailures  uint64
	bytes          uint64
	create         time.Duration
	write          time.Duration
	close          time.Duration
	maxCreate      time.Duration
	maxWrite       time.Duration
	maxClose       time.Duration
	activeCreates  int64
	activeWrites   int64
	activeCloses   int64
	slowOperations uint64
}

type packedObservedStorage struct {
	storeapi.Storage
	tctx   *tcontext.Context
	totals *packedOutputTotals
}

type packedObservedOutputWriter struct {
	objectio.Writer
	tctx   *tcontext.Context
	totals *packedOutputTotals
}

func updatePackedMaxDuration(target *atomic.Int64, duration time.Duration) {
	nanos := duration.Nanoseconds()
	for current := target.Load(); nanos > current; current = target.Load() {
		if target.CompareAndSwap(current, nanos) {
			return
		}
	}
}

func updatePackedMaxUint64(target *atomic.Uint64, value uint64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

func (t *packedOutputTotals) snapshot() packedOutputTotalsSnapshot {
	return packedOutputTotalsSnapshot{
		createCalls:    t.createCalls.Load(),
		createFailures: t.createFailures.Load(),
		writeCalls:     t.writeCalls.Load(),
		writeFailures:  t.writeFailures.Load(),
		closeCalls:     t.closeCalls.Load(),
		closeFailures:  t.closeFailures.Load(),
		bytes:          t.bytes.Load(),
		create:         time.Duration(t.createNano.Load()),
		write:          time.Duration(t.writeNano.Load()),
		close:          time.Duration(t.closeNano.Load()),
		maxCreate:      time.Duration(t.maxCreateNano.Load()),
		maxWrite:       time.Duration(t.maxWriteNano.Load()),
		maxClose:       time.Duration(t.maxCloseNano.Load()),
		activeCreates:  t.activeCreates.Load(),
		activeWrites:   t.activeWrites.Load(),
		activeCloses:   t.activeCloses.Load(),
		slowOperations: t.slowLogSlots.Load(),
	}
}

func logSlowPackedOutputOperation(
	tctx *tcontext.Context,
	totals *packedOutputTotals,
	operation string,
	duration time.Duration,
	bytes uint64,
	err error,
) {
	if duration < packedSlowScanThreshold {
		return
	}
	index := totals.slowLogSlots.Add(1)
	if index > packedMaxSlowOperationLogs {
		return
	}
	result := "success"
	if err != nil {
		result = "failed"
	}
	fields := []zap.Field{
		zap.String("operation", operation),
		zap.Duration("duration", duration),
		zap.Uint64("bytes", bytes),
		zap.String("result", result),
		zap.Uint64("slow_output_index", index),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	tctx.L().Info("slow packed backup output operation finished", fields...)
}

func (s *packedObservedStorage) Create(
	ctx context.Context,
	name string,
	option *storeapi.WriterOption,
) (objectio.Writer, error) {
	s.totals.activeCreates.Add(1)
	startedAt := time.Now()
	writer, err := s.Storage.Create(ctx, name, option)
	duration := time.Since(startedAt)
	s.totals.activeCreates.Add(-1)
	s.totals.createCalls.Add(1)
	s.totals.createNano.Add(duration.Nanoseconds())
	updatePackedMaxDuration(&s.totals.maxCreateNano, duration)
	if err != nil {
		s.totals.createFailures.Add(1)
	}
	logSlowPackedOutputOperation(s.tctx, s.totals, "create", duration, 0, err)
	if err != nil {
		return nil, err
	}
	return &packedObservedOutputWriter{Writer: writer, tctx: s.tctx, totals: s.totals}, nil
}

func (w *packedObservedOutputWriter) Write(ctx context.Context, data []byte) (int, error) {
	w.totals.activeWrites.Add(1)
	startedAt := time.Now()
	written, err := w.Writer.Write(ctx, data)
	duration := time.Since(startedAt)
	w.totals.activeWrites.Add(-1)
	w.totals.writeCalls.Add(1)
	w.totals.writeNano.Add(duration.Nanoseconds())
	w.totals.bytes.Add(uint64(written))
	updatePackedMaxDuration(&w.totals.maxWriteNano, duration)
	if err != nil {
		w.totals.writeFailures.Add(1)
	}
	logSlowPackedOutputOperation(w.tctx, w.totals, "write", duration, uint64(written), err)
	return written, err
}

func (w *packedObservedOutputWriter) Close(ctx context.Context) error {
	w.totals.activeCloses.Add(1)
	startedAt := time.Now()
	err := w.Writer.Close(ctx)
	duration := time.Since(startedAt)
	w.totals.activeCloses.Add(-1)
	w.totals.closeCalls.Add(1)
	w.totals.closeNano.Add(duration.Nanoseconds())
	updatePackedMaxDuration(&w.totals.maxCloseNano, duration)
	if err != nil {
		w.totals.closeFailures.Add(1)
	}
	logSlowPackedOutputOperation(w.tctx, w.totals, "close", duration, 0, err)
	return err
}

func newPackedScanProgress(totals *packedScanTotals) *packedScanProgress {
	return &packedScanProgress{totals: totals}
}

func (p *packedScanProgress) record(key, value []byte) {
	p.pendingRows++
	p.pendingBytes += uint64(len(key) + len(value))
	if p.pendingRows >= packedScanProgressRows || p.pendingBytes >= packedScanProgressBytes {
		p.flush()
	}
}

func (p *packedScanProgress) flush() {
	if p.pendingRows == 0 {
		return
	}
	p.totals.rows.Add(p.pendingRows)
	p.totals.rawBytes.Add(p.pendingBytes)
	p.pendingRows = 0
	p.pendingBytes = 0
}

func durationFromCSEStats(nanos uint64) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	if nanos > uint64(maxDuration) {
		return maxDuration
	}
	return time.Duration(nanos)
}

func durationFromOptionalCSEStats(nanos *uint64) time.Duration {
	if nanos == nil {
		return 0
	}
	return durationFromCSEStats(*nanos)
}

func (t *packedCSETotals) record(stats cseDumperScanStats) {
	if !stats.hasCSEStats {
		return
	}
	cse := stats.cse
	t.reportedScans.Add(1)
	t.manifestBytesRead.Add(cse.ManifestBytes)
	updatePackedMaxUint64(&t.manifestShards, cse.ManifestShards)
	updatePackedMaxUint64(&t.contentFiles, cse.ContentFiles)
	updatePackedMaxUint64(&t.plaintextShards, cse.PlaintextShards)
	updatePackedMaxUint64(&t.legacyShards, cse.LegacyShards)
	updatePackedMaxUint64(&t.cmekShards, cse.CMEKShards)
	updatePackedMaxUint64(&t.mixedEncryptionShards, cse.MixedEncryptionShards)
	t.shardsScanned.Add(cse.ShardsScanned)
	t.slowShards.Add(cse.SlowShards)
	t.scannedPlaintextShards.Add(cse.ScannedPlaintextShards)
	t.scannedLegacyShards.Add(cse.ScannedLegacyShards)
	t.scannedCMEKShards.Add(cse.ScannedCMEKShards)
	t.scannedMixedShards.Add(cse.ScannedMixedShards)
	t.sstCandidates.Add(cse.L0Candidates + cse.LNCandidates)
	t.sstRetained.Add(cse.L0Retained + cse.LNRetained)
	t.missingSSTBounds.Add(cse.MissingSSTBounds)
	t.sstCandidateBytesHint.Add(cse.SSTCandidateBytesHint)
	t.sstRetainedBytesHint.Add(cse.SSTRetainedBytesHint)
	t.contentReadAttempts.Add(cse.ContentReadAttempts)
	t.contentReads.Add(cse.ContentReads)
	t.contentReadFailures.Add(cse.ContentReadFailures)
	t.slowContentReads.Add(cse.SlowContentReads)
	t.contentBytes.Add(cse.ContentBytes)
	t.rows.Add(cse.Rows)
	t.keyBytes.Add(cse.KeyBytes)
	t.valueBytes.Add(cse.ValueBytes)
	t.stdoutBytes.Add(cse.StdoutBytes)
	t.stdoutWriteCalls.Add(cse.StdoutWriteCalls)
	t.stdoutWriteFailures.Add(cse.StdoutWriteFailures)
	updatePackedMaxDuration(&t.maxFirstRowProducedNano, durationFromOptionalCSEStats(cse.FirstRowProducedNanos))
	updatePackedMaxDuration(&t.maxFirstStdoutWriteNano, durationFromOptionalCSEStats(cse.FirstStdoutWriteNanos))
	updatePackedMaxDuration(&t.maxScanFirstRowNano, durationFromOptionalCSEStats(cse.ScanFirstRowNanos))
	t.manifestLoadNano.Add(durationFromCSEStats(cse.ManifestLoadNanos).Nanoseconds())
	t.legacyKeyLoadNano.Add(durationFromCSEStats(cse.LegacyKeyLoadNanos).Nanoseconds())
	t.readerInitNano.Add(durationFromCSEStats(cse.ReaderInitNanos).Nanoseconds())
	t.rangeFilterNano.Add(durationFromCSEStats(cse.RangeFilterNanos).Nanoseconds())
	t.snapshotLoadNano.Add(durationFromCSEStats(cse.SnapshotLoadNanos).Nanoseconds())
	t.iteratorInitNano.Add(durationFromCSEStats(cse.IteratorInitNanos).Nanoseconds())
	t.plaintextSnapshotLoadNano.Add(durationFromCSEStats(cse.PlaintextSnapshotLoadNanos).Nanoseconds())
	t.legacySnapshotLoadNano.Add(durationFromCSEStats(cse.LegacySnapshotLoadNanos).Nanoseconds())
	t.cmekSnapshotLoadNano.Add(durationFromCSEStats(cse.CMEKSnapshotLoadNanos).Nanoseconds())
	t.mixedSnapshotLoadNano.Add(durationFromCSEStats(cse.MixedSnapshotLoadNanos).Nanoseconds())
	t.contentReadNano.Add(durationFromCSEStats(cse.ContentReadNanos).Nanoseconds())
	updatePackedMaxDuration(&t.maxContentReadNano, durationFromCSEStats(cse.MaxContentReadNanos))
	t.iterateEmitNano.Add(durationFromCSEStats(cse.IterateEmitNanos).Nanoseconds())
	updatePackedMaxDuration(&t.maxIterateEmitNano, durationFromCSEStats(cse.MaxIterateEmitNanos))
	updatePackedMaxDuration(&t.maxRangeFilterNano, durationFromCSEStats(cse.MaxRangeFilterNanos))
	updatePackedMaxDuration(&t.maxSnapshotLoadNano, durationFromCSEStats(cse.MaxSnapshotLoadNanos))
	updatePackedMaxDuration(&t.maxIteratorInitNano, durationFromCSEStats(cse.MaxIteratorInitNanos))
	t.stdoutWriteNano.Add(durationFromCSEStats(cse.StdoutWriteNanos).Nanoseconds())
	updatePackedMaxDuration(&t.maxStdoutWriteNano, durationFromCSEStats(cse.MaxStdoutWriteNanos))
	t.scanNano.Add(durationFromCSEStats(cse.ScanNanos).Nanoseconds())
	t.flushNano.Add(durationFromCSEStats(cse.FlushNanos).Nanoseconds())
	t.totalNano.Add(durationFromCSEStats(cse.TotalNanos).Nanoseconds())
	if cse.PeakRSSBytes != nil {
		updatePackedMaxUint64(&t.maxPeakRSSBytes, *cse.PeakRSSBytes)
	}
}

func (t *packedCSETotals) snapshot() packedCSETotalsSnapshot {
	return packedCSETotalsSnapshot{
		reportedScans:          t.reportedScans.Load(),
		manifestBytesRead:      t.manifestBytesRead.Load(),
		manifestShards:         t.manifestShards.Load(),
		contentFiles:           t.contentFiles.Load(),
		plaintextShards:        t.plaintextShards.Load(),
		legacyShards:           t.legacyShards.Load(),
		cmekShards:             t.cmekShards.Load(),
		mixedEncryptionShards:  t.mixedEncryptionShards.Load(),
		shardsScanned:          t.shardsScanned.Load(),
		slowShards:             t.slowShards.Load(),
		scannedPlaintextShards: t.scannedPlaintextShards.Load(),
		scannedLegacyShards:    t.scannedLegacyShards.Load(),
		scannedCMEKShards:      t.scannedCMEKShards.Load(),
		scannedMixedShards:     t.scannedMixedShards.Load(),
		sstCandidates:          t.sstCandidates.Load(),
		sstRetained:            t.sstRetained.Load(),
		missingSSTBounds:       t.missingSSTBounds.Load(),
		sstCandidateBytesHint:  t.sstCandidateBytesHint.Load(),
		sstRetainedBytesHint:   t.sstRetainedBytesHint.Load(),
		contentReadAttempts:    t.contentReadAttempts.Load(),
		contentReads:           t.contentReads.Load(),
		contentReadFailures:    t.contentReadFailures.Load(),
		slowContentReads:       t.slowContentReads.Load(),
		contentBytes:           t.contentBytes.Load(),
		rows:                   t.rows.Load(),
		keyBytes:               t.keyBytes.Load(),
		valueBytes:             t.valueBytes.Load(),
		stdoutBytes:            t.stdoutBytes.Load(),
		stdoutWriteCalls:       t.stdoutWriteCalls.Load(),
		stdoutWriteFailures:    t.stdoutWriteFailures.Load(),
		maxFirstRowProduced:    time.Duration(t.maxFirstRowProducedNano.Load()),
		maxFirstStdoutWrite:    time.Duration(t.maxFirstStdoutWriteNano.Load()),
		maxScanFirstRow:        time.Duration(t.maxScanFirstRowNano.Load()),
		manifestLoad:           time.Duration(t.manifestLoadNano.Load()),
		legacyKeyLoad:          time.Duration(t.legacyKeyLoadNano.Load()),
		readerInit:             time.Duration(t.readerInitNano.Load()),
		rangeFilter:            time.Duration(t.rangeFilterNano.Load()),
		snapshotLoad:           time.Duration(t.snapshotLoadNano.Load()),
		iteratorInit:           time.Duration(t.iteratorInitNano.Load()),
		plaintextSnapshotLoad:  time.Duration(t.plaintextSnapshotLoadNano.Load()),
		legacySnapshotLoad:     time.Duration(t.legacySnapshotLoadNano.Load()),
		cmekSnapshotLoad:       time.Duration(t.cmekSnapshotLoadNano.Load()),
		mixedSnapshotLoad:      time.Duration(t.mixedSnapshotLoadNano.Load()),
		contentRead:            time.Duration(t.contentReadNano.Load()),
		maxContentRead:         time.Duration(t.maxContentReadNano.Load()),
		iterateEmit:            time.Duration(t.iterateEmitNano.Load()),
		maxIterateEmit:         time.Duration(t.maxIterateEmitNano.Load()),
		maxRangeFilter:         time.Duration(t.maxRangeFilterNano.Load()),
		maxSnapshotLoad:        time.Duration(t.maxSnapshotLoadNano.Load()),
		maxIteratorInit:        time.Duration(t.maxIteratorInitNano.Load()),
		stdoutWrite:            time.Duration(t.stdoutWriteNano.Load()),
		maxStdoutWrite:         time.Duration(t.maxStdoutWriteNano.Load()),
		scan:                   time.Duration(t.scanNano.Load()),
		flush:                  time.Duration(t.flushNano.Load()),
		total:                  time.Duration(t.totalNano.Load()),
		maxPeakRSSBytes:        t.maxPeakRSSBytes.Load(),
	}
}

func (s *packedScanTotals) recordStarted() {
	s.started.Add(1)
	s.active.Add(1)
}

func (s *packedScanTotals) recordStartFailure() {
	s.started.Add(1)
	s.failed.Add(1)
}

type packedCSETelemetryIssue struct {
	reason string
	index  uint64
}

func cseProtocolTotalsMatch(stats cseDumperScanStats) bool {
	return stats.hasCSEStats &&
		stats.rows == stats.cse.Rows &&
		stats.keyBytes == stats.cse.KeyBytes &&
		stats.valueBytes == stats.cse.ValueBytes
}

func (s *packedScanTotals) recordFinished(stats cseDumperScanStats, scanErr error) packedCSETelemetryIssue {
	s.active.Add(-1)
	if scanErr == nil {
		s.completed.Add(1)
	} else {
		s.failed.Add(1)
	}
	if stats.duration >= packedSlowScanThreshold {
		s.slow.Add(1)
	}
	s.durationNano.Add(stats.duration.Nanoseconds())
	updatePackedMaxDuration(&s.maxDurationNano, stats.duration)
	updatePackedMaxDuration(&s.maxProcessSpawnNano, stats.processSpawn)
	updatePackedMaxDuration(&s.maxFirstRowNano, stats.firstRowLatency)
	s.protocolReadNano.Add(stats.protocolRead.Nanoseconds())
	updatePackedMaxDuration(&s.maxProtocolReadNano, stats.maxProtocolRead)
	s.processWaitNano.Add(stats.processWait.Nanoseconds())
	updatePackedMaxDuration(&s.maxProcessWaitNano, stats.processWait)
	s.childUserCPUNano.Add(stats.childUserCPU.Nanoseconds())
	s.childSystemCPUNano.Add(stats.childSystemCPU.Nanoseconds())
	var issue packedCSETelemetryIssue
	if scanErr == nil {
		switch {
		case stats.cseStatsError != "":
			issue = packedCSETelemetryIssue{reason: "invalid", index: s.invalidCSEStats.Add(1)}
		case !stats.hasCSEStats:
			issue = packedCSETelemetryIssue{reason: "missing", index: s.missingCSEStats.Add(1)}
		case !stats.cse.Success || !stats.cse.ScanCompleted:
			issue = packedCSETelemetryIssue{reason: "inconsistent", index: s.invalidCSEStats.Add(1)}
		case !cseProtocolTotalsMatch(stats):
			issue = packedCSETelemetryIssue{reason: "protocol_totals_mismatch", index: s.invalidCSEStats.Add(1)}
		}
	}
	s.cse.record(stats)
	return issue
}

func (s *packedScanTotals) recordCanceled(stats cseDumperScanStats) {
	s.active.Add(-1)
	s.canceled.Add(1)
	s.durationNano.Add(stats.duration.Nanoseconds())
	updatePackedMaxDuration(&s.maxDurationNano, stats.duration)
	updatePackedMaxDuration(&s.maxProcessSpawnNano, stats.processSpawn)
	updatePackedMaxDuration(&s.maxFirstRowNano, stats.firstRowLatency)
	s.protocolReadNano.Add(stats.protocolRead.Nanoseconds())
	updatePackedMaxDuration(&s.maxProtocolReadNano, stats.maxProtocolRead)
	s.processWaitNano.Add(stats.processWait.Nanoseconds())
	updatePackedMaxDuration(&s.maxProcessWaitNano, stats.processWait)
	s.childUserCPUNano.Add(stats.childUserCPU.Nanoseconds())
	s.childSystemCPUNano.Add(stats.childSystemCPU.Nanoseconds())
	s.cse.record(stats)
}

func (s *packedScanTotals) logCSEIdentityAndManifest(
	tctx *tcontext.Context,
	stats cseDumperScanStats,
	extra ...zap.Field,
) {
	if !stats.hasCSEStats {
		return
	}
	s.cseIdentityOnce.Do(func() {
		cse := stats.cse
		fields := []zap.Field{
			zap.Int("telemetry_version", cse.Version),
			zap.String("cse_build_version", cse.BuildVersion),
			zap.String("cse_build_git_hash", cse.BuildGitHash),
			zap.String("cse_build_profile", cse.BuildProfile),
			zap.Uint64("manifest_shards", cse.ManifestShards),
			zap.Uint64("plaintext_shards", cse.PlaintextShards),
			zap.Uint64("legacy_shards", cse.LegacyShards),
			zap.Uint64("cmek_shards", cse.CMEKShards),
			zap.Uint64("mixed_encryption_shards", cse.MixedEncryptionShards),
			zap.Bool("legacy_encryption", cse.LegacyEncryption),
		}
		fields = append(fields, extra...)
		tctx.L().Info("identified packed backup CSE helper", fields...)
		if cse.MixedEncryptionShards > 0 {
			tctx.L().Warn("packed backup manifest contains mixed-encryption shards", fields...)
		}
		if cse.LegacyShards > 0 && !cse.LegacyEncryption {
			tctx.L().Warn("packed backup manifest contains legacy-encrypted shards while legacy decryption is disabled", fields...)
		}
	})
}

func (s *packedScanTotals) slowScanLogIndex(scan *cseDumperScan) uint64 {
	if index := scan.slowLogIndex.Load(); index != 0 {
		return index
	}
	index := s.slowScanLogSlots.Add(1)
	if scan.slowLogIndex.CompareAndSwap(0, index) {
		return index
	}
	return scan.slowLogIndex.Load()
}

func (s *packedScanTotals) recordTable(
	duration time.Duration,
	rowDecodeDuration time.Duration,
	maxRowDecodeDuration time.Duration,
	success bool,
) {
	s.tables.Add(1)
	if !success {
		s.failedTables.Add(1)
	}
	s.tableDurationNano.Add(duration.Nanoseconds())
	s.rowDecodeNano.Add(rowDecodeDuration.Nanoseconds())
	updatePackedMaxDuration(&s.maxTableDurationNano, duration)
	updatePackedMaxDuration(&s.maxRowDecodeNano, maxRowDecodeDuration)
}

func (s *packedScanTotals) snapshot() packedScanTotalsSnapshot {
	return packedScanTotalsSnapshot{
		planned:          s.planned.Load(),
		active:           s.active.Load(),
		started:          s.started.Load(),
		completed:        s.completed.Load(),
		failed:           s.failed.Load(),
		canceled:         s.canceled.Load(),
		slow:             s.slow.Load(),
		rows:             s.rows.Load(),
		rawBytes:         s.rawBytes.Load(),
		duration:         time.Duration(s.durationNano.Load()),
		maxDuration:      time.Duration(s.maxDurationNano.Load()),
		maxProcessSpawn:  time.Duration(s.maxProcessSpawnNano.Load()),
		maxFirstRow:      time.Duration(s.maxFirstRowNano.Load()),
		protocolRead:     time.Duration(s.protocolReadNano.Load()),
		maxProtocolRead:  time.Duration(s.maxProtocolReadNano.Load()),
		processWait:      time.Duration(s.processWaitNano.Load()),
		maxProcessWait:   time.Duration(s.maxProcessWaitNano.Load()),
		childUserCPU:     time.Duration(s.childUserCPUNano.Load()),
		childSystemCPU:   time.Duration(s.childSystemCPUNano.Load()),
		missingCSEStats:  s.missingCSEStats.Load(),
		invalidCSEStats:  s.invalidCSEStats.Load(),
		tables:           s.tables.Load(),
		failedTables:     s.failedTables.Load(),
		tableDuration:    time.Duration(s.tableDurationNano.Load()),
		maxTableDuration: time.Duration(s.maxTableDurationNano.Load()),
		rowDecode:        time.Duration(s.rowDecodeNano.Load()),
		maxRowDecode:     time.Duration(s.maxRowDecodeNano.Load()),
		cse:              s.cse.snapshot(),
	}
}

func packedScanLogFields(stats cseDumperScanStats, diagnostics string, extra ...zap.Field) []zap.Field {
	fields := []zap.Field{
		zap.Int("pid", stats.pid),
		zap.Uint64("rows", stats.rows),
		zap.Uint64("key_bytes", stats.keyBytes),
		zap.Uint64("value_bytes", stats.valueBytes),
		zap.Duration("process_spawn_duration", stats.processSpawn),
		zap.Duration("first_row_latency", stats.firstRowLatency),
		zap.Duration("protocol_read_duration", stats.protocolRead),
		zap.Duration("max_protocol_read_duration", stats.maxProtocolRead),
		zap.Duration("process_wait_duration", stats.processWait),
		zap.Duration("cse_child_user_cpu", stats.childUserCPU),
		zap.Duration("cse_child_system_cpu", stats.childSystemCPU),
		zap.Duration("duration", stats.duration),
		zap.Bool("stderr_truncated", stats.stderrTruncated),
	}
	if stats.exitCode != nil {
		fields = append(fields, zap.Int("cse_child_exit_code", *stats.exitCode))
	}
	if stats.cseStage != "" {
		fields = append(fields, zap.String("cse_stage", stats.cseStage))
	}
	if stats.hasCSEStats {
		cse := stats.cse
		fields = append(fields,
			zap.Bool("cse_success", cse.Success),
			zap.Bool("cse_scan_completed", cse.ScanCompleted),
			zap.Uint32("cse_keyspace_id", cse.KeyspaceID),
			zap.Uint64("cse_backup_ts", cse.BackupTS),
			zap.Uint64("cse_manifest_bytes", cse.ManifestBytes),
			zap.Uint64("cse_manifest_shards", cse.ManifestShards),
			zap.Uint64("cse_content_files", cse.ContentFiles),
			zap.Uint64("cse_shards_scanned", cse.ShardsScanned),
			zap.Uint64("cse_slow_shards", cse.SlowShards),
			zap.Uint64("cse_scanned_plaintext_shards", cse.ScannedPlaintextShards),
			zap.Uint64("cse_scanned_legacy_shards", cse.ScannedLegacyShards),
			zap.Uint64("cse_scanned_cmek_shards", cse.ScannedCMEKShards),
			zap.Uint64("cse_scanned_mixed_encryption_shards", cse.ScannedMixedShards),
			zap.Uint64("cse_sst_candidates", cse.L0Candidates+cse.LNCandidates),
			zap.Uint64("cse_sst_retained", cse.L0Retained+cse.LNRetained),
			zap.Uint64("cse_missing_sst_bounds", cse.MissingSSTBounds),
			zap.Uint64("cse_sst_candidate_bytes_hint", cse.SSTCandidateBytesHint),
			zap.Uint64("cse_sst_retained_bytes_hint", cse.SSTRetainedBytesHint),
			zap.Uint64("cse_content_read_attempts", cse.ContentReadAttempts),
			zap.Uint64("cse_content_reads", cse.ContentReads),
			zap.Uint64("cse_content_read_failures", cse.ContentReadFailures),
			zap.Uint64("cse_slow_content_reads", cse.SlowContentReads),
			zap.Uint64("cse_content_bytes", cse.ContentBytes),
			zap.Uint64("cse_rows", cse.Rows),
			zap.Uint64("cse_raw_kv_bytes", cse.KeyBytes+cse.ValueBytes),
			zap.Uint64("cse_stdout_bytes", cse.StdoutBytes),
			zap.Uint64("cse_stdout_write_calls", cse.StdoutWriteCalls),
			zap.Uint64("cse_stdout_write_failures", cse.StdoutWriteFailures),
			zap.Duration("cse_first_row_produced_latency", durationFromOptionalCSEStats(cse.FirstRowProducedNanos)),
			zap.Duration("cse_first_stdout_write_latency", durationFromOptionalCSEStats(cse.FirstStdoutWriteNanos)),
			zap.Duration("cse_scan_first_row_latency", durationFromOptionalCSEStats(cse.ScanFirstRowNanos)),
			zap.Duration("cse_manifest_load_duration", durationFromCSEStats(cse.ManifestLoadNanos)),
			zap.Duration("cse_legacy_key_load_duration", durationFromCSEStats(cse.LegacyKeyLoadNanos)),
			zap.Duration("cse_reader_init_duration", durationFromCSEStats(cse.ReaderInitNanos)),
			zap.Duration("cse_range_filter_duration", durationFromCSEStats(cse.RangeFilterNanos)),
			zap.Duration("cse_snapshot_load_duration", durationFromCSEStats(cse.SnapshotLoadNanos)),
			zap.Duration("cse_iterator_init_duration", durationFromCSEStats(cse.IteratorInitNanos)),
			zap.Duration("cse_plaintext_snapshot_load_duration", durationFromCSEStats(cse.PlaintextSnapshotLoadNanos)),
			zap.Duration("cse_legacy_snapshot_load_duration", durationFromCSEStats(cse.LegacySnapshotLoadNanos)),
			zap.Duration("cse_cmek_snapshot_load_duration", durationFromCSEStats(cse.CMEKSnapshotLoadNanos)),
			zap.Duration("cse_mixed_encryption_snapshot_load_duration", durationFromCSEStats(cse.MixedSnapshotLoadNanos)),
			zap.Duration("cse_content_read_duration", durationFromCSEStats(cse.ContentReadNanos)),
			zap.Duration("cse_max_content_read_duration", durationFromCSEStats(cse.MaxContentReadNanos)),
			zap.Duration("cse_iterate_emit_duration", durationFromCSEStats(cse.IterateEmitNanos)),
			zap.Duration("cse_max_iterate_emit_duration", durationFromCSEStats(cse.MaxIterateEmitNanos)),
			zap.Duration("cse_max_range_filter_duration", durationFromCSEStats(cse.MaxRangeFilterNanos)),
			zap.Duration("cse_max_snapshot_load_duration", durationFromCSEStats(cse.MaxSnapshotLoadNanos)),
			zap.Duration("cse_max_iterator_init_duration", durationFromCSEStats(cse.MaxIteratorInitNanos)),
			zap.Duration("cse_stdout_write_duration", durationFromCSEStats(cse.StdoutWriteNanos)),
			zap.Duration("cse_max_stdout_write_duration", durationFromCSEStats(cse.MaxStdoutWriteNanos)),
			zap.Duration("cse_scan_duration", durationFromCSEStats(cse.ScanNanos)),
			zap.Duration("cse_flush_duration", durationFromCSEStats(cse.FlushNanos)),
			zap.Duration("cse_total_duration", durationFromCSEStats(cse.TotalNanos)))
		if cse.PeakRSSBytes != nil {
			fields = append(fields, zap.Uint64("cse_peak_rss_bytes", *cse.PeakRSSBytes))
		}
		if cse.ScanStage != "" {
			fields = append(fields,
				zap.String("cse_scan_stage", cse.ScanStage),
				zap.Uint64("cse_scan_shard_id", cse.ScanShardID),
				zap.Uint64("cse_scan_shard_ver", cse.ScanShardVersion))
		}
	}
	if stats.cseStatsError != "" {
		fields = append(fields, zap.String("cse_stats_error", stats.cseStatsError))
	}
	fields = append(fields, extra...)
	if diagnostics != "" {
		fields = append(fields, zap.String("cse_diagnostics", diagnostics))
	}
	return fields
}

func logPackedScanStarted(
	tctx *tcontext.Context,
	totals *packedScanTotals,
	scan *cseDumperScan,
	extra ...zap.Field,
) {
	fields := []zap.Field{zap.Int("pid", scan.pid())}
	fields = append(fields, extra...)
	tctx.L().Debug("packed backup range scan started", fields...)
	go func() {
		timer := time.NewTimer(packedSlowScanThreshold)
		defer timer.Stop()
		select {
		case <-scan.done:
			return
		case <-timer.C:
		}
		select {
		case <-scan.done:
			return
		default:
		}
		stats := scan.liveStats()
		index := totals.slowScanLogIndex(scan)
		if index > packedMaxSlowOperationLogs {
			return
		}
		fields := packedScanLogFields(stats, scan.diagnostics(), extra...)
		fields = append(fields, zap.Uint64("slow_scan_index", index))
		tctx.L().Info("slow packed backup range scan still running", fields...)
	}()
}

func logPackedScanFinished(
	tctx *tcontext.Context,
	totals *packedScanTotals,
	scan *cseDumperScan,
	scanErr error,
	canceled bool,
	extra ...zap.Field,
) cseDumperScanStats {
	stats := scan.stats()
	fields := packedScanLogFields(stats, scan.diagnostics(), extra...)
	if canceled {
		fields = append(fields, zap.Error(scanErr))
		tctx.L().Info("packed backup range scan canceled", fields...)
	} else if scanErr != nil {
		fields = append(fields, zap.Error(scanErr))
		tctx.L().Warn("packed backup range scan failed", fields...)
	} else if stats.duration >= packedSlowScanThreshold {
		index := totals.slowScanLogIndex(scan)
		fields = append(fields, zap.Uint64("slow_scan_index", index))
		if index <= packedMaxSlowOperationLogs {
			tctx.L().Info("slow packed backup range scan finished", fields...)
		} else {
			tctx.L().Debug("packed backup range scan finished", fields...)
		}
	} else {
		tctx.L().Debug("packed backup range scan finished", fields...)
	}
	return stats
}

func logPackedCSETelemetryIssue(
	tctx *tcontext.Context,
	stats cseDumperScanStats,
	issue packedCSETelemetryIssue,
	extra ...zap.Field,
) {
	if issue.index == 0 || issue.index > packedMaxSlowOperationLogs {
		return
	}
	fields := []zap.Field{
		zap.String("reason", issue.reason),
		zap.Uint64("telemetry_issue_index", issue.index),
		zap.Int("pid", stats.pid),
	}
	if stats.exitCode != nil {
		fields = append(fields, zap.Int("cse_child_exit_code", *stats.exitCode))
	}
	if stats.cseStatsError != "" {
		fields = append(fields, zap.String("cse_stats_error", stats.cseStatsError))
	}
	if stats.hasCSEStats {
		fields = append(fields,
			zap.Uint64("protocol_rows", stats.rows),
			zap.Uint64("protocol_key_bytes", stats.keyBytes),
			zap.Uint64("protocol_value_bytes", stats.valueBytes),
			zap.Uint64("cse_rows", stats.cse.Rows),
			zap.Uint64("cse_key_bytes", stats.cse.KeyBytes),
			zap.Uint64("cse_value_bytes", stats.cse.ValueBytes))
	}
	fields = append(fields, extra...)
	message := "packed backup range scan returned unusable CSE telemetry"
	if issue.reason == "protocol_totals_mismatch" {
		message = "packed backup CSE protocol totals mismatch"
	}
	tctx.L().Warn(message, fields...)
}

func newPackedTableData(
	executable, metadataURL string,
	legacyEncryption bool,
	database string,
	table *model.TableInfo,
	scanTotals *packedScanTotals,
) *packedTableData {
	return &packedTableData{
		executable:       executable,
		metadataURL:      metadataURL,
		legacyEncryption: legacyEncryption,
		database:         database,
		table:            table,
		ranges:           packedPhysicalTableRanges(table),
		scanTotals:       scanTotals,
	}
}

func (d *packedTableData) Start(tctx *tcontext.Context, _ *sql.Conn) error {
	decoder, err := newPackedRowDecoder(d.table)
	if err != nil {
		return err
	}
	iter := &packedRowIter{
		ctx:              tctx,
		executable:       d.executable,
		metadataURL:      d.metadataURL,
		legacyEncryption: d.legacyEncryption,
		database:         d.database,
		ranges:           d.ranges,
		scanTotals:       d.scanTotals,
		table:            d.table,
		decoder:          decoder,
		args:             make([]any, len(decoder.columns)),
		startedAt:        time.Now(),
	}
	iter.readNext()
	d.iter = iter
	return iter.err
}

func (d *packedTableData) Rows() SQLRowIter { return d.iter }

func (d *packedTableData) Close() error {
	if d.iter != nil {
		err := d.iter.Close()
		d.iter.logTableFinished(err)
		return err
	}
	return nil
}

func (*packedTableData) RawRows() *sql.Rows { return nil }

type packedRowIter struct {
	ctx                  *tcontext.Context
	executable           string
	metadataURL          string
	legacyEncryption     bool
	database             string
	ranges               []packedRange
	nextRange            int
	activeRange          int
	scan                 *cseDumperScan
	scanProgress         *packedScanProgress
	scanTotals           *packedScanTotals
	table                *model.TableInfo
	decoder              *packedRowDecoder
	key                  []byte
	value                []byte
	args                 []any
	defaults             expression.BuildContext
	err                  error
	hasRow               bool
	startedAt            time.Time
	completedRanges      uint64
	rows                 uint64
	keyBytes             uint64
	valueBytes           uint64
	scanDuration         time.Duration
	rowDecodeDuration    time.Duration
	maxRowDecodeDuration time.Duration
	tableLogged          bool
}

func (i *packedRowIter) HasNext() bool { return i.err == nil && i.hasRow }

func (i *packedRowIter) Decode(receiver RowReceiver) error {
	if !i.HasNext() {
		return errors.New("packed backup row iterator has no current row")
	}
	startedAt := time.Now()
	defer func() {
		duration := time.Since(startedAt)
		i.rowDecodeDuration += duration
		i.maxRowDecodeDuration = max(i.maxRowDecodeDuration, duration)
	}()
	if i.decoder == nil {
		decoder, err := newPackedRowDecoder(i.table)
		if err != nil {
			return err
		}
		i.decoder = decoder
	}
	if i.defaults == nil {
		i.defaults = exprstatic.NewExprContext()
	}
	values, err := i.decoder.decode(i.table, i.key, i.value, i.defaults)
	if err != nil {
		return err
	}
	if len(values) != len(i.args) {
		return errors.Errorf("packed backup row has %d values for %d columns", len(values), len(i.args))
	}
	receiver.BindAddress(i.args)
	for index := range values {
		destination, ok := i.args[index].(*sql.RawBytes)
		if !ok {
			return errors.Errorf("unsupported Dumpling packed row receiver %T", i.args[index])
		}
		if values[index].IsNull() {
			*destination = nil
			continue
		}
		data, err := values[index].ToBytes()
		if err != nil {
			return errors.Annotatef(err, "format packed backup column %d", index)
		}
		*destination = append((*destination)[:0], data...)
		if len(data) == 0 {
			*destination = sql.RawBytes{}
		}
	}
	return nil
}

func (i *packedRowIter) Next() {
	if i.HasNext() {
		i.readNext()
	}
}

func (i *packedRowIter) Error() error { return i.err }

func (i *packedRowIter) Close() error {
	if i.scan == nil {
		return nil
	}
	scan := i.scan
	i.scan = nil
	err := scan.close()
	i.scanProgress.flush()
	i.scanProgress = nil
	stats := scan.stats()
	i.scanTotals.logCSEIdentityAndManifest(i.ctx, stats, i.rangeLogFields()...)
	i.scanTotals.recordCanceled(stats)
	i.addScanStats(stats, false)
	fields := packedScanLogFields(stats, scan.diagnostics(), i.rangeLogFields()...)
	if err != nil {
		fields = append(fields, zap.Error(err))
		i.ctx.L().Warn("failed to stop packed backup range scan", fields...)
	} else {
		i.ctx.L().Debug("packed backup range scan stopped before EOF", fields...)
	}
	return err
}

func (i *packedRowIter) readNext() {
	for {
		if i.scan == nil {
			if i.nextRange == len(i.ranges) {
				i.hasRow = false
				return
			}
			i.activeRange = i.nextRange
			rangeToScan := i.ranges[i.activeRange]
			i.nextRange++
			scan, err := startCSEDumperScan(
				i.ctx,
				i.executable,
				i.metadataURL,
				i.legacyEncryption,
				rangeToScan.start,
				rangeToScan.end,
			)
			if err != nil {
				i.scanTotals.recordStartFailure()
				fields := i.rangeLogFields()
				fields = append(fields, zap.Error(err))
				i.ctx.L().Warn("failed to start packed backup range scan", fields...)
				i.err = err
				i.hasRow = false
				return
			}
			i.scan = scan
			i.scanProgress = newPackedScanProgress(i.scanTotals)
			i.scanTotals.recordStarted()
			logPackedScanStarted(i.ctx, i.scanTotals, scan, i.rangeLogFields()...)
		}

		key, value, end, err := i.scan.readRow(i.key, i.value)
		if err != nil {
			scan := i.scan
			i.scanProgress.flush()
			i.scanProgress = nil
			canceled := err != nil && i.ctx.Err() != nil
			stats := logPackedScanFinished(i.ctx, i.scanTotals, scan, err, canceled, i.rangeLogFields()...)
			i.scanTotals.logCSEIdentityAndManifest(i.ctx, stats, i.rangeLogFields()...)
			if canceled {
				i.scanTotals.recordCanceled(stats)
			} else {
				issue := i.scanTotals.recordFinished(stats, err)
				logPackedCSETelemetryIssue(i.ctx, stats, issue, i.rangeLogFields()...)
			}
			i.addScanStats(stats, false)
			i.err = err
			i.hasRow = false
			i.scan = nil
			return
		}
		if end {
			scan := i.scan
			i.scanProgress.flush()
			i.scanProgress = nil
			stats := logPackedScanFinished(i.ctx, i.scanTotals, scan, nil, false, i.rangeLogFields()...)
			i.scanTotals.logCSEIdentityAndManifest(i.ctx, stats, i.rangeLogFields()...)
			issue := i.scanTotals.recordFinished(stats, nil)
			logPackedCSETelemetryIssue(i.ctx, stats, issue, i.rangeLogFields()...)
			i.addScanStats(stats, true)
			i.scan = nil
			continue
		}
		i.scanProgress.record(key, value)
		i.key = key
		i.value = value
		i.hasRow = true
		return
	}
}

func (i *packedRowIter) rangeLogFields() []zap.Field {
	rangeToScan := i.ranges[i.activeRange]
	return []zap.Field{
		zap.String("scan_kind", "table"),
		zap.String("database", i.database),
		zap.String("table", i.table.Name.O),
		zap.Int64("table_id", i.table.ID),
		zap.Int64("physical_table_id", rangeToScan.physicalTableID),
		zap.Int("range_index", i.activeRange),
		zap.Int("range_count", len(i.ranges)),
	}
}

func (i *packedRowIter) addScanStats(stats cseDumperScanStats, completed bool) {
	if completed {
		i.completedRanges++
	}
	i.rows += stats.rows
	i.keyBytes += stats.keyBytes
	i.valueBytes += stats.valueBytes
	i.scanDuration += stats.duration
}

func (i *packedRowIter) logTableFinished(closeErr error) {
	if i.tableLogged {
		return
	}
	i.tableLogged = true
	duration := time.Since(i.startedAt)
	success := i.err == nil && closeErr == nil
	i.scanTotals.recordTable(duration, i.rowDecodeDuration, i.maxRowDecodeDuration, success)
	fields := []zap.Field{
		zap.String("database", i.database),
		zap.String("table", i.table.Name.O),
		zap.Int64("table_id", i.table.ID),
		zap.Int("physical_ranges", len(i.ranges)),
		zap.Uint64("completed_ranges", i.completedRanges),
		zap.Uint64("rows", i.rows),
		zap.Uint64("raw_kv_bytes", i.keyBytes+i.valueBytes),
		zap.Duration("cumulative_scan_duration", i.scanDuration),
		zap.Duration("row_decode_duration", i.rowDecodeDuration),
		zap.Duration("max_row_decode_duration", i.maxRowDecodeDuration),
		zap.Duration("duration", duration),
	}
	if i.err != nil {
		fields = append(fields, zap.Error(i.err))
	}
	if closeErr != nil {
		fields = append(fields, zap.NamedError("close_error", closeErr))
	}
	if success && duration >= packedSlowTableThreshold {
		index := i.scanTotals.slowTableLogSlots.Add(1)
		fields = append(fields, zap.Uint64("slow_table_index", index))
		if index <= packedMaxSlowOperationLogs {
			i.ctx.L().Info("slow packed backup table export finished", fields...)
		} else {
			i.ctx.L().Debug("packed backup table export finished", fields...)
		}
	} else {
		i.ctx.L().Debug("packed backup table export finished", fields...)
	}
}

type packedRowDecoder struct {
	columns             []*model.ColumnInfo
	columnTypes         map[int64]*types.FieldType
	commonHandleOffsets map[int64]int
}

func newPackedRowDecoder(table *model.TableInfo) (*packedRowDecoder, error) {
	columns := packedVisibleColumns(table)
	columnTypes := make(map[int64]*types.FieldType, len(columns))
	for _, column := range columns {
		if !table.PKIsHandle || !mysql.HasPriKeyFlag(column.GetFlag()) {
			columnTypes[column.ID] = &column.FieldType
		}
	}
	commonHandleOffsets, err := packedCommonHandleColumnOffsets(table)
	if err != nil {
		return nil, err
	}
	return &packedRowDecoder{
		columns:             columns,
		columnTypes:         columnTypes,
		commonHandleOffsets: commonHandleOffsets,
	}, nil
}

func (d *packedRowDecoder) decode(
	table *model.TableInfo,
	key, value []byte,
	defaults expression.BuildContext,
) ([]types.Datum, error) {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return nil, errors.Annotatef(err, "decode packed backup row key %x", key)
	}
	rowMap, err := tablecodec.DecodeRowToDatumMap(value, d.columnTypes, time.UTC)
	if err != nil {
		return nil, errors.Annotatef(err, "decode packed backup row value at key %x", key)
	}
	values := make([]types.Datum, 0, len(d.columns))
	for _, column := range d.columns {
		decoded, err := decodePackedColumn(table, column, handle, rowMap, d.commonHandleOffsets, defaults)
		if err != nil {
			return nil, errors.Annotatef(err, "decode column %q at packed backup key %x", column.Name.O, key)
		}
		values = append(values, decoded)
	}
	return values, nil
}

func decodePackedColumn(
	table *model.TableInfo,
	column *model.ColumnInfo,
	handle kv.Handle,
	rowMap map[int64]types.Datum,
	commonHandleOffsets map[int64]int,
	defaults expression.BuildContext,
) (types.Datum, error) {
	if table.PKIsHandle && mysql.HasPriKeyFlag(column.GetFlag()) {
		var value types.Datum
		if mysql.HasUnsignedFlag(column.GetFlag()) {
			value.SetUint64(uint64(handle.IntValue()))
		} else {
			value.SetInt64(handle.IntValue())
		}
		return value, nil
	}
	if handleOffset, ok := commonHandleOffsets[column.ID]; ok {
		if handleOffset >= handle.NumCols() {
			return types.Datum{}, errors.Errorf("common handle has %d columns, need offset %d", handle.NumCols(), handleOffset)
		}
		_, value, err := codec.DecodeOne(handle.EncodedCol(handleOffset))
		if err != nil {
			return types.Datum{}, err
		}
		return tablecodec.Unflatten(value, &column.FieldType, time.UTC)
	}
	if value, ok := rowMap[column.ID]; ok {
		return value, nil
	}
	return tidbtable.GetColOriginDefaultValue(defaults, column)
}

func packedVisibleColumns(table *model.TableInfo) []*model.ColumnInfo {
	columns := make([]*model.ColumnInfo, 0, len(table.Columns))
	for _, column := range table.Cols() {
		if column != nil && !column.Hidden && !column.IsGenerated() {
			columns = append(columns, column)
		}
	}
	return columns
}

func packedPhysicalTableIDs(table *model.TableInfo) []int64 {
	partition := table.GetPartitionInfo()
	if partition == nil || len(partition.Definitions) == 0 {
		return []int64{table.ID}
	}
	tableIDs := make([]int64, 0, len(partition.Definitions))
	for _, definition := range partition.Definitions {
		tableIDs = append(tableIDs, definition.ID)
	}
	return tableIDs
}

func packedPhysicalTableRanges(table *model.TableInfo) []packedRange {
	tableIDs := packedPhysicalTableIDs(table)
	ranges := make([]packedRange, 0, len(tableIDs))
	for _, tableID := range tableIDs {
		start := tablecodec.GenTableRecordPrefix(tableID)
		ranges = append(ranges, packedRange{
			physicalTableID: tableID,
			start:           start,
			end:             start.PrefixNext(),
		})
	}
	return ranges
}

func packedCommonHandleColumnOffsets(table *model.TableInfo) (map[int64]int, error) {
	offsets := make(map[int64]int)
	if !table.IsCommonHandle {
		return offsets, nil
	}
	primary := table.GetPrimaryKey()
	if primary == nil || !primary.Primary {
		return nil, errors.Errorf("packed backup table %q has a common handle without a primary index", table.Name.O)
	}
	for handleOffset, indexColumn := range primary.Columns {
		if indexColumn.Offset < 0 || indexColumn.Offset >= len(table.Columns) {
			return nil, errors.Errorf("packed backup table %q has invalid primary column offset %d", table.Name.O, indexColumn.Offset)
		}
		offsets[table.Columns[indexColumn.Offset].ID] = handleOffset
	}
	return offsets, nil
}

type packedRangeScanner func(
	ctx context.Context,
	target packedScanTarget,
	startKey, endKey []byte,
	emit func(key, value []byte) error,
) error

type packedScanTarget struct {
	metadataKind string
	database     string
	databaseID   int64
}

func (t packedScanTarget) logFields() []zap.Field {
	fields := []zap.Field{
		zap.String("scan_kind", "metadata"),
		zap.String("metadata_kind", t.metadataKind),
	}
	if t.database != "" {
		fields = append(fields,
			zap.String("database", t.database),
			zap.Int64("database_id", t.databaseID))
	}
	return fields
}

func newCSEDumperRangeScanner(
	tctx *tcontext.Context,
	executable, metadataURL string,
	legacyEncryption bool,
	scanTotals *packedScanTotals,
) packedRangeScanner {
	return func(
		ctx context.Context,
		target packedScanTarget,
		startKey, endKey []byte,
		emit func(key, value []byte) error,
	) error {
		logFields := target.logFields()
		progress := newPackedScanProgress(scanTotals)
		defer progress.flush()
		started := false
		observer := &cseDumperScanObserver{
			started: func(scan *cseDumperScan) {
				started = true
				scanTotals.recordStarted()
				logPackedScanStarted(tctx, scanTotals, scan, logFields...)
			},
			finished: func(scan *cseDumperScan, scanErr error) {
				canceled := scanErr != nil && ctx.Err() != nil
				stats := logPackedScanFinished(tctx, scanTotals, scan, scanErr, canceled, logFields...)
				scanTotals.logCSEIdentityAndManifest(tctx, stats, logFields...)
				if canceled {
					scanTotals.recordCanceled(stats)
					return
				}
				issue := scanTotals.recordFinished(stats, scanErr)
				logPackedCSETelemetryIssue(tctx, stats, issue, logFields...)
			},
		}
		observedEmit := func(key, value []byte) error {
			progress.record(key, value)
			return emit(key, value)
		}
		err := scanCSEDumperRange(
			ctx,
			executable,
			metadataURL,
			legacyEncryption,
			startKey,
			endKey,
			observedEmit,
			observer,
		)
		if err != nil && !started {
			scanTotals.recordStartFailure()
			fields := append(logFields, zap.Error(err))
			tctx.L().Warn("failed to start packed backup range scan", fields...)
		}
		return err
	}
}

func packedHashDataPrefix(hashKey []byte) kv.Key {
	prefix := []byte{'m'}
	prefix = codec.EncodeBytes(prefix, hashKey)
	return codec.EncodeUint(prefix, uint64(structure.HashData))
}

func scanPackedHash(
	ctx context.Context,
	scan packedRangeScanner,
	target packedScanTarget,
	hashKey []byte,
	emit func(field, value []byte) error,
) error {
	prefix := packedHashDataPrefix(hashKey)
	return scan(ctx, target, prefix, prefix.PrefixNext(), func(key, value []byte) error {
		if !bytes.HasPrefix(key, prefix) {
			return errors.Errorf("packed metadata key %x is outside hash prefix %x", key, prefix)
		}
		remaining, field, err := codec.DecodeBytes(key[len(prefix):], nil)
		if err != nil {
			return errors.Annotatef(err, "decode packed metadata hash key %x", key)
		}
		if len(remaining) != 0 {
			return errors.Errorf("packed metadata hash key %x has %d trailing bytes", key, len(remaining))
		}
		return emit(field, value)
	})
}

type packedMetadataLoadStats struct {
	databaseRows       uint64
	tableRows          uint64
	jsonDecodeDuration time.Duration
	sortDuration       time.Duration
}

func loadPackedDatabases(
	ctx context.Context,
	scan packedRangeScanner,
) ([]*model.DBInfo, packedMetadataLoadStats, error) {
	var databases []*model.DBInfo
	var stats packedMetadataLoadStats
	if err := scanPackedHash(ctx, scan, packedScanTarget{metadataKind: "databases"}, []byte("DBs"), func(field, value []byte) error {
		if !tidbmeta.IsDBkey(field) {
			return nil
		}
		stats.databaseRows++
		database := &model.DBInfo{}
		decodeStartedAt := time.Now()
		if err := json.Unmarshal(value, database); err != nil {
			stats.jsonDecodeDuration += time.Since(decodeStartedAt)
			return errors.Annotatef(err, "decode packed database schema at field %q", field)
		}
		stats.jsonDecodeDuration += time.Since(decodeStartedAt)
		if database.State == model.StatePublic {
			databases = append(databases, database)
		}
		return nil
	}); err != nil {
		return nil, stats, err
	}

	for _, database := range databases {
		target := packedScanTarget{
			metadataKind: "tables",
			database:     database.Name.O,
			databaseID:   database.ID,
		}
		if err := scanPackedHash(ctx, scan, target, tidbmeta.DBkey(database.ID), func(field, value []byte) error {
			if !tidbmeta.IsTableKey(field) {
				return nil
			}
			stats.tableRows++
			table := &model.TableInfo{}
			decodeStartedAt := time.Now()
			if err := json.Unmarshal(value, table); err != nil {
				stats.jsonDecodeDuration += time.Since(decodeStartedAt)
				return errors.Annotatef(err, "decode packed table schema in database %q", database.Name.O)
			}
			stats.jsonDecodeDuration += time.Since(decodeStartedAt)
			if table.State == model.StatePublic {
				database.Deprecated.Tables = append(database.Deprecated.Tables, table)
			}
			return nil
		}); err != nil {
			return nil, stats, err
		}
		sortStartedAt := time.Now()
		slices.SortFunc(database.Deprecated.Tables, func(left, right *model.TableInfo) int {
			return strings.Compare(left.Name.L, right.Name.L)
		})
		stats.sortDuration += time.Since(sortStartedAt)
	}
	sortStartedAt := time.Now()
	slices.SortFunc(databases, model.LessDBInfo)
	stats.sortDuration += time.Since(sortStartedAt)
	return databases, stats, nil
}

func packedCreateDatabaseSQL(database *model.DBInfo) (string, error) {
	var output bytes.Buffer
	if err := executor.ConstructResultOfShowCreateDatabase(mock.NewContextDeprecated(), database, true, &output); err != nil {
		return "", errors.Annotatef(err, "build schema for packed database %q", database.Name.O)
	}
	return output.String(), nil
}

func packedCreateTableSQL(table *model.TableInfo) (string, error) {
	var output bytes.Buffer
	if err := executor.ConstructResultOfShowCreateTable(mock.NewContextDeprecated(), table, autoid.Allocators{}, &output); err != nil {
		return "", errors.Annotatef(err, "build schema for packed table %q", table.Name.O)
	}
	return output.String(), nil
}

type packedExportSummary struct {
	stage                    atomic.Value
	selectedTables           int
	selectedRanges           int
	scheduledTables          int
	scheduledTasks           int
	metadataDuration         time.Duration
	metadataDecodeDuration   time.Duration
	metadataSortDuration     time.Duration
	metadataPlanningDuration time.Duration
	taskSchedulingDuration   time.Duration
	schemaBuildDuration      time.Duration
	taskEnqueueWaitDuration  time.Duration
	writersStartedAt         time.Time
	outputTotals             *packedOutputTotals
}

func newPackedExportSummary(outputTotals *packedOutputTotals) *packedExportSummary {
	summary := &packedExportSummary{outputTotals: outputTotals}
	summary.setStage("load_metadata")
	return summary
}

func (s *packedExportSummary) setStage(stage string) {
	s.stage.Store(stage)
}

func (s *packedExportSummary) currentStage() string {
	return s.stage.Load().(string)
}

func (d *Dumper) logPackedExportFinished(
	startedAt time.Time,
	scanTotals *packedScanTotals,
	summary *packedExportSummary,
	resultErr error,
) {
	status := d.GetStatus()
	scans := scanTotals.snapshot()
	output := summary.outputTotals.snapshot()
	fields := []zap.Field{
		zap.String("stage", summary.currentStage()),
		zap.Int("selected_tables", summary.selectedTables),
		zap.Int("selected_ranges", summary.selectedRanges),
		zap.Int("scheduled_tables", summary.scheduledTables),
		zap.Int("scheduled_tasks", summary.scheduledTasks),
		zap.Int64("planned_scans", scans.planned),
		zap.Uint64("started_scans", scans.started),
		zap.Uint64("completed_scans", scans.completed),
		zap.Int64("active_scans", scans.active),
		zap.Uint64("failed_scans", scans.failed),
		zap.Uint64("canceled_scans", scans.canceled),
		zap.Uint64("slow_scans", scans.slow),
		zap.Uint64("scan_rows", scans.rows),
		zap.Uint64("scan_bytes", scans.rawBytes),
		zap.Duration("cumulative_scan_duration", scans.duration),
		zap.Duration("max_scan_duration", scans.maxDuration),
		zap.Duration("max_process_spawn_duration", scans.maxProcessSpawn),
		zap.Duration("max_first_row_latency", scans.maxFirstRow),
		zap.Duration("cumulative_protocol_read_duration", scans.protocolRead),
		zap.Duration("max_protocol_read_duration", scans.maxProtocolRead),
		zap.Duration("cumulative_process_wait_duration", scans.processWait),
		zap.Duration("max_process_wait_duration", scans.maxProcessWait),
		zap.Duration("cumulative_cse_child_user_cpu", scans.childUserCPU),
		zap.Duration("cumulative_cse_child_system_cpu", scans.childSystemCPU),
		zap.Uint64("cse_stats_missing_scans", scans.missingCSEStats),
		zap.Uint64("cse_stats_invalid_scans", scans.invalidCSEStats),
		zap.Uint64("cse_reported_scans", scans.cse.reportedScans),
		zap.Uint64("cse_manifest_bytes_read", scans.cse.manifestBytesRead),
		zap.Uint64("cse_shards_scanned", scans.cse.shardsScanned),
		zap.Uint64("cse_slow_shards", scans.cse.slowShards),
		zap.Uint64("cse_scanned_plaintext_shards", scans.cse.scannedPlaintextShards),
		zap.Uint64("cse_scanned_legacy_shards", scans.cse.scannedLegacyShards),
		zap.Uint64("cse_scanned_cmek_shards", scans.cse.scannedCMEKShards),
		zap.Uint64("cse_scanned_mixed_encryption_shards", scans.cse.scannedMixedShards),
		zap.Uint64("cse_sst_candidates", scans.cse.sstCandidates),
		zap.Uint64("cse_sst_retained", scans.cse.sstRetained),
		zap.Uint64("cse_missing_sst_bounds", scans.cse.missingSSTBounds),
		zap.Uint64("cse_sst_candidate_bytes_hint", scans.cse.sstCandidateBytesHint),
		zap.Uint64("cse_sst_retained_bytes_hint", scans.cse.sstRetainedBytesHint),
		zap.Uint64("cse_content_read_attempts", scans.cse.contentReadAttempts),
		zap.Uint64("cse_content_reads", scans.cse.contentReads),
		zap.Uint64("cse_content_read_failures", scans.cse.contentReadFailures),
		zap.Uint64("cse_slow_content_reads", scans.cse.slowContentReads),
		zap.Uint64("cse_content_bytes", scans.cse.contentBytes),
		zap.Uint64("cse_stdout_bytes", scans.cse.stdoutBytes),
		zap.Uint64("cse_stdout_write_calls", scans.cse.stdoutWriteCalls),
		zap.Uint64("cse_stdout_write_failures", scans.cse.stdoutWriteFailures),
		zap.Bool("cse_protocol_totals_match",
			scans.cse.reportedScans == scans.completed &&
				scans.missingCSEStats == 0 && scans.invalidCSEStats == 0 &&
				scans.rows == scans.cse.rows && scans.rawBytes == scans.cse.keyBytes+scans.cse.valueBytes),
		zap.Duration("max_cse_scan_first_row_latency", scans.cse.maxScanFirstRow),
		zap.Duration("max_cse_first_stdout_write_latency", scans.cse.maxFirstStdoutWrite),
		zap.Duration("cumulative_cse_manifest_load_duration", scans.cse.manifestLoad),
		zap.Duration("cumulative_cse_legacy_key_load_duration", scans.cse.legacyKeyLoad),
		zap.Duration("cumulative_cse_reader_init_duration", scans.cse.readerInit),
		zap.Duration("cumulative_cse_range_filter_duration", scans.cse.rangeFilter),
		zap.Duration("cumulative_cse_snapshot_load_duration", scans.cse.snapshotLoad),
		zap.Duration("cumulative_cse_iterator_init_duration", scans.cse.iteratorInit),
		zap.Duration("cumulative_cse_plaintext_snapshot_load_duration", scans.cse.plaintextSnapshotLoad),
		zap.Duration("cumulative_cse_legacy_snapshot_load_duration", scans.cse.legacySnapshotLoad),
		zap.Duration("cumulative_cse_cmek_snapshot_load_duration", scans.cse.cmekSnapshotLoad),
		zap.Duration("cumulative_cse_mixed_encryption_snapshot_load_duration", scans.cse.mixedSnapshotLoad),
		zap.Duration("cumulative_cse_content_read_duration", scans.cse.contentRead),
		zap.Duration("max_cse_content_read_duration", scans.cse.maxContentRead),
		zap.Duration("cumulative_cse_iterate_emit_duration", scans.cse.iterateEmit),
		zap.Duration("max_cse_iterate_emit_duration", scans.cse.maxIterateEmit),
		zap.Duration("max_cse_range_filter_duration", scans.cse.maxRangeFilter),
		zap.Duration("max_cse_snapshot_load_duration", scans.cse.maxSnapshotLoad),
		zap.Duration("max_cse_iterator_init_duration", scans.cse.maxIteratorInit),
		zap.Duration("cumulative_cse_stdout_write_duration", scans.cse.stdoutWrite),
		zap.Duration("max_cse_stdout_write_duration", scans.cse.maxStdoutWrite),
		zap.Duration("cumulative_cse_scan_duration", scans.cse.scan),
		zap.Duration("cumulative_cse_flush_duration", scans.cse.flush),
		zap.Duration("cumulative_cse_total_duration", scans.cse.total),
		zap.Uint64("max_cse_child_peak_rss_bytes", scans.cse.maxPeakRSSBytes),
		zap.Uint64("finished_rows", uint64(status.FinishedRows)),
		zap.Uint64("finished_uncompressed_bytes", uint64(status.FinishedBytes)),
		zap.Uint64("packed_tables", scans.tables),
		zap.Uint64("failed_packed_tables", scans.failedTables),
		zap.Duration("cumulative_packed_table_pipeline_duration", scans.tableDuration),
		zap.Duration("max_packed_table_pipeline_duration", scans.maxTableDuration),
		zap.Duration("cumulative_packed_row_decode_duration", scans.rowDecode),
		zap.Duration("max_packed_row_decode_duration", scans.maxRowDecode),
		zap.Uint64("output_storage_bytes", output.bytes),
		zap.Uint64("output_storage_create_calls", output.createCalls),
		zap.Uint64("output_storage_create_failures", output.createFailures),
		zap.Uint64("output_storage_write_calls", output.writeCalls),
		zap.Uint64("output_storage_write_failures", output.writeFailures),
		zap.Uint64("output_storage_close_calls", output.closeCalls),
		zap.Uint64("output_storage_close_failures", output.closeFailures),
		zap.Duration("cumulative_output_storage_create_duration", output.create),
		zap.Duration("cumulative_output_storage_write_duration", output.write),
		zap.Duration("cumulative_output_storage_close_duration", output.close),
		zap.Duration("max_output_storage_create_duration", output.maxCreate),
		zap.Duration("max_output_storage_write_duration", output.maxWrite),
		zap.Duration("max_output_storage_close_duration", output.maxClose),
		zap.Int64("active_output_storage_creates", output.activeCreates),
		zap.Int64("active_output_storage_writes", output.activeWrites),
		zap.Int64("active_output_storage_closes", output.activeCloses),
		zap.Uint64("slow_output_storage_operations", output.slowOperations),
		zap.Duration("metadata_duration", summary.metadataDuration),
		zap.Duration("metadata_json_decode_duration", summary.metadataDecodeDuration),
		zap.Duration("metadata_sort_duration", summary.metadataSortDuration),
		zap.Duration("metadata_planning_duration", summary.metadataPlanningDuration),
		zap.Duration("task_scheduling_duration", summary.taskSchedulingDuration),
		zap.Duration("schema_build_duration", summary.schemaBuildDuration),
		zap.Duration("task_enqueue_wait_duration", summary.taskEnqueueWaitDuration),
		zap.Duration("duration", time.Since(startedAt)),
	}
	if !summary.writersStartedAt.IsZero() {
		fields = append(fields, zap.Duration("writer_wall_duration", time.Since(summary.writersStartedAt)))
	}
	if resultErr == nil {
		d.tctx.L().Info("finished dumping packed backup", fields...)
		return
	}
	fields = append(fields, zap.Error(resultErr))
	if errors.Cause(resultErr) == context.Canceled {
		d.tctx.L().Info("packed backup export canceled", fields...)
		return
	}
	d.tctx.L().Warn("packed backup export failed", fields...)
}

func (d *Dumper) runPackedLogProgress(
	tctx *tcontext.Context,
	startedAt time.Time,
	scanTotals *packedScanTotals,
	summary *packedExportSummary,
) {
	ticker := time.NewTicker(logProgressTick)
	defer ticker.Stop()
	lastCheckpoint := time.Now()
	lastProgressAt := lastCheckpoint
	var lastScanBytes uint64
	var lastScanRows uint64
	var lastCompletedScans uint64
	var lastOutputBytes float64
	var lastStorageBytes uint64
	var lastFinishedRows float64
	var noProgressWarned bool
	for {
		select {
		case <-tctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			seconds := now.Sub(lastCheckpoint).Seconds()
			scans := scanTotals.snapshot()
			output := summary.outputTotals.snapshot()
			status := d.GetStatus()
			scanBytes := scans.rawBytes
			progressMade := scanBytes != lastScanBytes || scans.rows != lastScanRows ||
				scans.completed != lastCompletedScans || status.FinishedBytes != lastOutputBytes ||
				status.FinishedRows != lastFinishedRows || output.bytes != lastStorageBytes
			if progressMade {
				lastProgressAt = now
				noProgressWarned = false
			}
			fields := []zap.Field{
				zap.String("stage", summary.currentStage()),
				zap.Duration("elapsed", now.Sub(startedAt)),
				zap.Duration("no_progress_duration", now.Sub(lastProgressAt)),
				zap.Int64("planned_scans", scans.planned),
				zap.Uint64("started_scans", scans.started),
				zap.Uint64("completed_scans", scans.completed),
				zap.Int64("active_scans", scans.active),
				zap.Uint64("failed_scans", scans.failed),
				zap.Uint64("canceled_scans", scans.canceled),
				zap.Uint64("slow_scans", scans.slow),
				zap.Uint64("scan_rows", scans.rows),
				zap.Uint64("scan_bytes", scanBytes),
				zap.Float64("recent_scan_mib_per_sec", float64(scanBytes-lastScanBytes)/(1024*1024*seconds)),
				zap.Float64("finished_rows", status.FinishedRows),
				zap.Uint64("finished_uncompressed_bytes", uint64(status.FinishedBytes)),
				zap.Float64("recent_uncompressed_output_mib_per_sec", (status.FinishedBytes-lastOutputBytes)/(1024*1024*seconds)),
				zap.Uint64("output_storage_bytes", output.bytes),
				zap.Float64("recent_output_storage_mib_per_sec", float64(output.bytes-lastStorageBytes)/(1024*1024*seconds)),
				zap.Int64("active_output_storage_creates", output.activeCreates),
				zap.Int64("active_output_storage_writes", output.activeWrites),
				zap.Int64("active_output_storage_closes", output.activeCloses),
			}
			if status.TotalTables > 0 {
				fields = append(fields,
					zap.Int64("total_tables", status.TotalTables),
					zap.Float64("completed_tables", status.CompletedTables))
			}
			if !noProgressWarned && now.Sub(lastProgressAt) >= packedNoProgressThreshold {
				tctx.L().Warn("packed backup export has made no progress", fields...)
				noProgressWarned = true
			} else {
				tctx.L().Info("packed backup progress", fields...)
			}
			lastCheckpoint = now
			lastScanBytes = scanBytes
			lastScanRows = scans.rows
			lastCompletedScans = scans.completed
			lastOutputBytes = status.FinishedBytes
			lastStorageBytes = output.bytes
			lastFinishedRows = status.FinishedRows
		}
	}
}

func packedBackupLogID(rawURL string) string {
	value := rawURL
	if parsed, err := url.Parse(rawURL); err == nil {
		parsed.User = nil
		parsed.RawQuery = ""
		parsed.Fragment = ""
		value = parsed.String()
	}
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:8])
}

func packedStorageScheme(uri string) string {
	parsed, err := url.Parse(uri)
	if err != nil || parsed.Scheme == "" {
		return "unknown"
	}
	return strings.ToLower(parsed.Scheme)
}

func packedCompressionName(suffix string) string {
	if suffix == "" {
		return "none"
	}
	return strings.TrimPrefix(suffix, ".")
}

func (d *Dumper) dumpPacked() (resultErr error) {
	start := time.Now()
	scanTotals := &packedScanTotals{}
	outputTotals := &packedOutputTotals{}
	summary := newPackedExportSummary(outputTotals)
	outputStorage := &packedObservedStorage{
		Storage: d.extStore,
		tctx:    d.tctx,
		totals:  outputTotals,
	}
	defer func() {
		d.logPackedExportFinished(start, scanTotals, summary, resultErr)
	}()
	d.tctx.L().Info("starting packed backup export",
		zap.String("backup_id", packedBackupLogID(d.conf.PackedBackup)),
		zap.String("input_storage", packedStorageScheme(d.conf.PackedBackup)),
		zap.Int("threads", d.conf.Threads),
		zap.String("cse_ctl_path", d.conf.CSEExecutable),
		zap.String("output_storage", packedStorageScheme(d.extStore.URI())),
		zap.String("compression", packedCompressionName(d.conf.CompressType.FileSuffix())),
		zap.Uint64("file_size", d.conf.FileSize),
		zap.Bool("legacy_encryption", d.conf.CSELegacyEncryption),
		zap.Bool("no_header", d.conf.NoHeader),
		zap.Bool("no_schemas", d.conf.NoSchemas),
		zap.Bool("no_data", d.conf.NoData))
	progressCtx, cancelProgress := d.tctx.WithCancel()
	go d.runPackedLogProgress(progressCtx, start, scanTotals, summary)
	defer cancelProgress()

	metadataStarted := time.Now()
	databases, metadataLoadStats, err := loadPackedDatabases(
		d.tctx,
		newCSEDumperRangeScanner(
			d.tctx,
			d.conf.CSEExecutable,
			d.conf.PackedBackup,
			d.conf.CSELegacyEncryption,
			scanTotals,
		),
	)
	summary.metadataDuration = time.Since(metadataStarted)
	summary.metadataDecodeDuration = metadataLoadStats.jsonDecodeDuration
	summary.metadataSortDuration = metadataLoadStats.sortDuration
	if err != nil {
		return err
	}
	metadataPlanningStartedAt := time.Now()
	publicTableCount := 0
	selectedDatabaseCount := 0
	selectedTableCount := 0
	selectedRangeCount := 0
	for _, database := range databases {
		for _, table := range database.Deprecated.Tables {
			if !table.IsView() && !table.IsSequence() {
				publicTableCount++
			}
		}
		if !d.conf.TableFilter.MatchSchema(database.Name.O) {
			continue
		}
		selectedDatabaseCount++
		for _, table := range database.Deprecated.Tables {
			if table.IsView() || table.IsSequence() || !d.conf.TableFilter.MatchTable(database.Name.O, table.Name.O) {
				continue
			}
			selectedTableCount++
			if !d.conf.NoData {
				selectedRangeCount += len(packedPhysicalTableIDs(table))
			}
		}
	}
	summary.metadataPlanningDuration = time.Since(metadataPlanningStartedAt)
	metadataScans := scanTotals.snapshot()
	metadataNonScanDuration := summary.metadataDuration - metadataScans.duration
	if metadataNonScanDuration < 0 {
		metadataNonScanDuration = 0
	}
	scanTotals.planned.Store(int64(metadataScans.started) + int64(selectedRangeCount))
	summary.selectedTables = selectedTableCount
	summary.selectedRanges = selectedRangeCount
	atomic.StoreInt64(&d.totalTables, int64(selectedTableCount))
	if selectedTableCount == 0 {
		d.tctx.L().Warn("packed backup export selected no tables",
			zap.Int("databases", len(databases)),
			zap.Int("public_tables", publicTableCount),
			zap.Int("selected_databases", selectedDatabaseCount),
			zap.Bool("no_schemas", d.conf.NoSchemas),
			zap.Bool("no_data", d.conf.NoData))
	}
	if d.conf.NoData {
		d.metrics.totalChunks.Store(0)
	} else {
		d.metrics.totalChunks.Store(int64(selectedTableCount))
	}
	d.tctx.L().Info("loaded packed backup metadata",
		zap.Int("databases", len(databases)),
		zap.Int("public_tables", publicTableCount),
		zap.Int("selected_databases", selectedDatabaseCount),
		zap.Int("selected_tables", selectedTableCount),
		zap.Int("selected_ranges", selectedRangeCount),
		zap.Uint64("metadata_scans", metadataScans.completed),
		zap.Uint64("metadata_rows", metadataScans.rows),
		zap.Uint64("metadata_database_rows", metadataLoadStats.databaseRows),
		zap.Uint64("metadata_table_rows", metadataLoadStats.tableRows),
		zap.Uint64("metadata_raw_bytes", metadataScans.rawBytes),
		zap.Duration("cumulative_metadata_scan_duration", metadataScans.duration),
		zap.Uint64("metadata_cse_manifest_bytes_read", metadataScans.cse.manifestBytesRead),
		zap.Uint64("cse_manifest_shards", metadataScans.cse.manifestShards),
		zap.Uint64("cse_content_files", metadataScans.cse.contentFiles),
		zap.Uint64("cse_plaintext_shards", metadataScans.cse.plaintextShards),
		zap.Uint64("cse_legacy_shards", metadataScans.cse.legacyShards),
		zap.Uint64("cse_cmek_shards", metadataScans.cse.cmekShards),
		zap.Uint64("cse_mixed_encryption_shards", metadataScans.cse.mixedEncryptionShards),
		zap.Uint64("metadata_cse_sst_candidates", metadataScans.cse.sstCandidates),
		zap.Uint64("metadata_cse_sst_retained", metadataScans.cse.sstRetained),
		zap.Uint64("metadata_cse_content_bytes", metadataScans.cse.contentBytes),
		zap.Duration("cumulative_metadata_cse_manifest_load_duration", metadataScans.cse.manifestLoad),
		zap.Duration("cumulative_metadata_cse_snapshot_load_duration", metadataScans.cse.snapshotLoad),
		zap.Duration("cumulative_metadata_cse_iterator_init_duration", metadataScans.cse.iteratorInit),
		zap.Duration("cumulative_metadata_cse_content_read_duration", metadataScans.cse.contentRead),
		zap.Duration("cumulative_metadata_cse_stdout_write_duration", metadataScans.cse.stdoutWrite),
		zap.Duration("metadata_non_scan_duration", metadataNonScanDuration),
		zap.Duration("metadata_json_decode_duration", summary.metadataDecodeDuration),
		zap.Duration("metadata_sort_duration", summary.metadataSortDuration),
		zap.Duration("metadata_planning_duration", summary.metadataPlanningDuration),
		zap.Duration("duration", summary.metadataDuration))

	summary.setStage("prepare_writers")
	summary.writersStartedAt = time.Now()
	taskIn, taskOut := infiniteChan[Task]()
	wg, writingCtx := errgroup.WithContext(d.tctx)
	writerCtx := d.tctx.WithContext(writingCtx)
	writers := make([]*Writer, d.conf.Threads)
	for index := range d.conf.Threads {
		writer := NewWriter(writerCtx, int64(index), d.conf, nil, outputStorage, d.metrics)
		writer.rebuildConnFn = func(conn *sql.Conn, _ bool) (*sql.Conn, error) { return conn, nil }
		writer.setFinishTableCallBack(func(task Task) {
			if _, ok := task.(*TaskTableData); ok {
				IncCounter(d.metrics.finishedTablesCounter)
			}
		})
		writer.setFinishTaskCallBack(func(task Task) {
			if _, ok := task.(*TaskTableData); ok {
				d.metrics.completedChunks.Add(1)
			}
		})
		wg.Go(func() error { return writer.run(taskOut) })
		writers[index] = writer
	}

	summary.setStage("schedule_tasks")
	schedulingStartedAt := time.Now()
	schedulingFinished := false
	finishScheduling := func() {
		if schedulingFinished {
			return
		}
		schedulingFinished = true
		summary.taskSchedulingDuration = time.Since(schedulingStartedAt)
	}
	defer finishScheduling()
	send := func(task Task) error {
		enqueueStartedAt := time.Now()
		if d.sendTaskToChan(writerCtx, task, taskIn) {
			summary.taskEnqueueWaitDuration += time.Since(enqueueStartedAt)
			return writerCtx.Err()
		}
		summary.taskEnqueueWaitDuration += time.Since(enqueueStartedAt)
		summary.scheduledTasks++
		return nil
	}
	for _, database := range databases {
		if !d.conf.TableFilter.MatchSchema(database.Name.O) {
			continue
		}
		if !d.conf.NoSchemas {
			schemaBuildStartedAt := time.Now()
			createSQL, err := packedCreateDatabaseSQL(database)
			summary.schemaBuildDuration += time.Since(schemaBuildStartedAt)
			if err != nil {
				close(taskIn)
				_ = wg.Wait()
				return err
			}
			if err := send(NewTaskDatabaseMeta(database.Name.O, createSQL)); err != nil {
				close(taskIn)
				_ = wg.Wait()
				return err
			}
		}
		for _, table := range database.Deprecated.Tables {
			if table.IsView() || table.IsSequence() || !d.conf.TableFilter.MatchTable(database.Name.O, table.Name.O) {
				continue
			}
			summary.scheduledTables++
			var createSQL string
			if !d.conf.NoSchemas {
				schemaBuildStartedAt := time.Now()
				createSQL, err = packedCreateTableSQL(table)
				summary.schemaBuildDuration += time.Since(schemaBuildStartedAt)
				if err != nil {
					close(taskIn)
					_ = wg.Wait()
					return err
				}
				if err := send(NewTaskTableMeta(database.Name.O, table.Name.O, createSQL)); err != nil {
					close(taskIn)
					_ = wg.Wait()
					return err
				}
			}
			meta := newPackedTableMeta(database.Name.O, table, createSQL)
			if !d.conf.NoData {
				data := newPackedTableData(
					d.conf.CSEExecutable,
					d.conf.PackedBackup,
					d.conf.CSELegacyEncryption,
					database.Name.O,
					table,
					scanTotals,
				)
				if err := send(NewTaskTableData(meta, data, 0, 1)); err != nil {
					close(taskIn)
					_ = wg.Wait()
					return err
				}
			}
		}
	}
	close(taskIn)
	finishScheduling()
	d.tctx.L().Info("scheduled packed backup tasks",
		zap.Int("selected_tables", summary.selectedTables),
		zap.Int("selected_ranges", summary.selectedRanges),
		zap.Int("scheduled_tables", summary.scheduledTables),
		zap.Int("tasks", summary.scheduledTasks),
		zap.Duration("schema_build_duration", summary.schemaBuildDuration),
		zap.Duration("task_enqueue_wait_duration", summary.taskEnqueueWaitDuration),
		zap.Duration("duration", summary.taskSchedulingDuration))
	d.metrics.progressReady.Store(true)
	summary.setStage("write_tasks")
	if err := wg.Wait(); err != nil {
		return errors.Trace(err)
	}
	summary.setStage("complete")
	return nil
}

func (m *packedTableMeta) String() string {
	return fmt.Sprintf("%s.%s", m.database, m.table)
}
