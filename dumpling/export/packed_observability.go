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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

const (
	packedSlowScanThreshold    = 10 * time.Second
	packedSlowTableThreshold   = 30 * time.Second
	packedNoProgressThreshold  = 5 * time.Minute
	packedScanProgressRows     = 1024
	packedScanProgressBytes    = 1 << 20
	packedMaxSlowOperationLogs = 3
	packedMaxLogTextBytes      = 2 << 10
)

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
		fields = append(fields, packedErrorLogField(err))
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

func newCSEDumperRangeScanner(
	tctx *tcontext.Context,
	executable, metadataURL string,
	legacyEncryption bool,
	scanTotals *packedScanTotals,
) packedRangeScanner {
	return func(
		ctx context.Context,
		startKey, endKey []byte,
		emit func(key, value []byte) error,
	) error {
		logFields := []zap.Field{zap.String("scan_kind", "metadata")}
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
			fields := append(logFields, packedErrorLogField(err))
			tctx.L().Warn("failed to start packed backup range scan", fields...)
		}
		return err
	}
}

func packedLogText(text string) string {
	if len(text) <= packedMaxLogTextBytes {
		return text
	}
	return text[:packedMaxLogTextBytes] + "...(truncated)"
}

func packedErrorLogField(err error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	return zap.String("error", packedLogText(err.Error()))
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
	slot := &scan.observation.slowLogIndex
	if index := slot.Load(); index != 0 {
		return index
	}
	index := s.slowScanLogSlots.Add(1)
	if slot.CompareAndSwap(0, index) {
		return index
	}
	return slot.Load()
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

type packedLogLevel uint8

const (
	packedDebugLog packedLogLevel = iota
	packedInfoLog
	packedWarnLog
)

func (level packedLogLevel) log(tctx *tcontext.Context, message string, fields ...zap.Field) {
	switch level {
	case packedDebugLog:
		tctx.L().Debug(message, fields...)
	case packedInfoLog:
		tctx.L().Info(message, fields...)
	case packedWarnLog:
		tctx.L().Warn(message, fields...)
	}
}

func packedTopicFields(extra []zap.Field, fields ...zap.Field) []zap.Field {
	result := make([]zap.Field, 0, len(extra)+len(fields))
	result = append(result, extra...)
	return append(result, fields...)
}

func packedScanProcessFields(stats cseDumperScanStats, extra ...zap.Field) []zap.Field {
	fields := packedTopicFields(extra,
		zap.Int("pid", stats.pid),
		zap.Uint64("rows", stats.rows),
		zap.Uint64("raw_kv_bytes", stats.keyBytes+stats.valueBytes),
		zap.Duration("process_spawn_duration", stats.processSpawn),
		zap.Duration("first_row_latency", stats.firstRowLatency),
		zap.Duration("protocol_read_duration", stats.protocolRead),
		zap.Duration("max_protocol_read_duration", stats.maxProtocolRead),
		zap.Duration("process_wait_duration", stats.processWait),
		zap.Duration("cse_child_user_cpu", stats.childUserCPU),
		zap.Duration("cse_child_system_cpu", stats.childSystemCPU),
		zap.Duration("duration", stats.duration),
		zap.Bool("stderr_truncated", stats.stderrTruncated))
	if stats.exitCode != nil {
		fields = append(fields, zap.Int("cse_child_exit_code", *stats.exitCode))
	}
	if stats.cseStage != "" {
		fields = append(fields, zap.String("cse_stage", packedLogText(stats.cseStage)))
	}
	if stats.cseStatsError != "" {
		fields = append(fields, zap.String("cse_stats_error", packedLogText(stats.cseStatsError)))
	}
	return fields
}

func logPackedScanCSEDetails(
	tctx *tcontext.Context,
	level packedLogLevel,
	stats cseDumperScanStats,
	extra ...zap.Field,
) {
	if !stats.hasCSEStats {
		return
	}
	cse := stats.cse
	selectionFields := packedTopicFields(extra,
		zap.Bool("success", cse.Success),
		zap.Bool("scan_completed", cse.ScanCompleted),
		zap.Uint32("keyspace_id", cse.KeyspaceID),
		zap.Uint64("backup_ts", cse.BackupTS),
		zap.Uint64("manifest_bytes", cse.ManifestBytes),
		zap.Uint64("manifest_shards", cse.ManifestShards),
		zap.Uint64("content_files", cse.ContentFiles),
		zap.Uint64("shards_scanned", cse.ShardsScanned),
		zap.Uint64("slow_shards", cse.SlowShards),
		zap.Uint64("scanned_plaintext_shards", cse.ScannedPlaintextShards),
		zap.Uint64("scanned_legacy_shards", cse.ScannedLegacyShards),
		zap.Uint64("scanned_cmek_shards", cse.ScannedCMEKShards),
		zap.Uint64("scanned_mixed_encryption_shards", cse.ScannedMixedShards),
		zap.Uint64("sst_candidates", cse.L0Candidates+cse.LNCandidates),
		zap.Uint64("sst_retained", cse.L0Retained+cse.LNRetained),
		zap.Uint64("missing_sst_bounds", cse.MissingSSTBounds),
		zap.Uint64("sst_candidate_bytes_hint", cse.SSTCandidateBytesHint),
		zap.Uint64("sst_retained_bytes_hint", cse.SSTRetainedBytesHint))
	if cse.ScanStage != "" {
		selectionFields = append(selectionFields,
			zap.String("scan_stage", packedLogText(cse.ScanStage)),
			zap.Uint64("scan_shard_id", cse.ScanShardID),
			zap.Uint64("scan_shard_ver", cse.ScanShardVersion))
	}
	level.log(tctx, "packed backup range scan CSE selection summary", selectionFields...)

	ioFields := packedTopicFields(extra,
		zap.Uint64("content_read_attempts", cse.ContentReadAttempts),
		zap.Uint64("content_reads", cse.ContentReads),
		zap.Uint64("content_read_failures", cse.ContentReadFailures),
		zap.Uint64("slow_content_reads", cse.SlowContentReads),
		zap.Uint64("content_bytes", cse.ContentBytes),
		zap.Uint64("rows", cse.Rows),
		zap.Uint64("raw_kv_bytes", cse.KeyBytes+cse.ValueBytes),
		zap.Uint64("stdout_bytes", cse.StdoutBytes),
		zap.Uint64("stdout_write_calls", cse.StdoutWriteCalls),
		zap.Uint64("stdout_write_failures", cse.StdoutWriteFailures))
	if cse.PeakRSSBytes != nil {
		ioFields = append(ioFields, zap.Uint64("peak_rss_bytes", *cse.PeakRSSBytes))
	}
	level.log(tctx, "packed backup range scan CSE I/O summary", ioFields...)

	level.log(tctx, "packed backup range scan CSE setup timing",
		packedTopicFields(extra,
			zap.Duration("first_row_produced_latency", durationFromOptionalCSEStats(cse.FirstRowProducedNanos)),
			zap.Duration("first_stdout_write_latency", durationFromOptionalCSEStats(cse.FirstStdoutWriteNanos)),
			zap.Duration("scan_first_row_latency", durationFromOptionalCSEStats(cse.ScanFirstRowNanos)),
			zap.Duration("manifest_load_duration", durationFromCSEStats(cse.ManifestLoadNanos)),
			zap.Duration("legacy_key_load_duration", durationFromCSEStats(cse.LegacyKeyLoadNanos)),
			zap.Duration("reader_init_duration", durationFromCSEStats(cse.ReaderInitNanos)),
			zap.Duration("range_filter_duration", durationFromCSEStats(cse.RangeFilterNanos)),
			zap.Duration("snapshot_load_duration", durationFromCSEStats(cse.SnapshotLoadNanos)),
			zap.Duration("iterator_init_duration", durationFromCSEStats(cse.IteratorInitNanos)),
			zap.Duration("max_range_filter_duration", durationFromCSEStats(cse.MaxRangeFilterNanos)),
			zap.Duration("max_snapshot_load_duration", durationFromCSEStats(cse.MaxSnapshotLoadNanos)),
			zap.Duration("max_iterator_init_duration", durationFromCSEStats(cse.MaxIteratorInitNanos)))...)

	level.log(tctx, "packed backup range scan CSE data timing",
		packedTopicFields(extra,
			zap.Duration("plaintext_snapshot_load_duration", durationFromCSEStats(cse.PlaintextSnapshotLoadNanos)),
			zap.Duration("legacy_snapshot_load_duration", durationFromCSEStats(cse.LegacySnapshotLoadNanos)),
			zap.Duration("cmek_snapshot_load_duration", durationFromCSEStats(cse.CMEKSnapshotLoadNanos)),
			zap.Duration("mixed_encryption_snapshot_load_duration", durationFromCSEStats(cse.MixedSnapshotLoadNanos)),
			zap.Duration("content_read_duration", durationFromCSEStats(cse.ContentReadNanos)),
			zap.Duration("max_content_read_duration", durationFromCSEStats(cse.MaxContentReadNanos)),
			zap.Duration("iterate_emit_duration", durationFromCSEStats(cse.IterateEmitNanos)),
			zap.Duration("max_iterate_emit_duration", durationFromCSEStats(cse.MaxIterateEmitNanos)),
			zap.Duration("stdout_write_duration", durationFromCSEStats(cse.StdoutWriteNanos)),
			zap.Duration("max_stdout_write_duration", durationFromCSEStats(cse.MaxStdoutWriteNanos)),
			zap.Duration("scan_duration", durationFromCSEStats(cse.ScanNanos)),
			zap.Duration("flush_duration", durationFromCSEStats(cse.FlushNanos)),
			zap.Duration("total_duration", durationFromCSEStats(cse.TotalNanos)))...)
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
		fields := packedScanProcessFields(stats, extra...)
		fields = append(fields, zap.Uint64("slow_scan_index", index))
		tctx.L().Info("slow packed backup range scan still running", fields...)
		if diagnostics := scan.diagnostics(); diagnostics != "" {
			tctx.L().Info("slow packed backup range scan diagnostics",
				packedTopicFields(extra,
					zap.Uint64("slow_scan_index", index),
					zap.String("diagnostics", packedLogText(diagnostics)))...)
		}
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
	fields := packedScanProcessFields(stats, extra...)
	level := packedDebugLog
	message := "packed backup range scan finished"
	if canceled {
		level = packedInfoLog
		message = "packed backup range scan canceled"
		fields = append(fields, packedErrorLogField(scanErr))
	} else if scanErr != nil {
		level = packedWarnLog
		message = "packed backup range scan failed"
		fields = append(fields, packedErrorLogField(scanErr))
	} else if stats.duration >= packedSlowScanThreshold {
		index := totals.slowScanLogIndex(scan)
		fields = append(fields, zap.Uint64("slow_scan_index", index))
		if index <= packedMaxSlowOperationLogs {
			level = packedInfoLog
			message = "slow packed backup range scan finished"
		}
	}
	level.log(tctx, message, fields...)
	logPackedScanCSEDetails(tctx, level, stats, extra...)
	if diagnostics := scan.diagnostics(); diagnostics != "" {
		level.log(tctx, "packed backup range scan diagnostics",
			packedTopicFields(extra, zap.String("diagnostics", packedLogText(diagnostics)))...)
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
		fields = append(fields, zap.String("cse_stats_error", packedLogText(stats.cseStatsError)))
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

type packedTableObservation struct {
	tctx                 *tcontext.Context
	totals               *packedScanTotals
	database             string
	table                *model.TableInfo
	physicalTableIDs     []int64
	progress             *packedScanProgress
	startedAt            time.Time
	completedRanges      uint64
	rows                 uint64
	keyBytes             uint64
	valueBytes           uint64
	scanDuration         time.Duration
	rowDecodeDuration    time.Duration
	maxRowDecodeDuration time.Duration
	finished             bool
}

func newPackedTableObservation(
	tctx *tcontext.Context,
	totals *packedScanTotals,
	database string,
	table *model.TableInfo,
) *packedTableObservation {
	return &packedTableObservation{
		tctx:             tctx,
		totals:           totals,
		database:         database,
		table:            table,
		physicalTableIDs: packedPhysicalTableIDs(table),
	}
}

func (o *packedTableObservation) start() {
	o.startedAt = time.Now()
}

func (o *packedTableObservation) decode(decode func() error) error {
	startedAt := time.Now()
	err := decode()
	duration := time.Since(startedAt)
	o.rowDecodeDuration += duration
	o.maxRowDecodeDuration = max(o.maxRowDecodeDuration, duration)
	return err
}

func (o *packedTableObservation) rangeLogFields(index int) []zap.Field {
	return []zap.Field{
		zap.String("scan_kind", "table"),
		zap.String("database", o.database),
		zap.String("table", o.table.Name.O),
		zap.Int64("table_id", o.table.ID),
		zap.Int64("physical_table_id", o.physicalTableIDs[index]),
		zap.Int("range_index", index),
		zap.Int("range_count", len(o.physicalTableIDs)),
	}
}

func (o *packedTableObservation) rangeStartFailed(index int, err error) {
	o.totals.recordStartFailure()
	fields := append(o.rangeLogFields(index), packedErrorLogField(err))
	o.tctx.L().Warn("failed to start packed backup range scan", fields...)
}

func (o *packedTableObservation) rangeStarted(index int, scan *cseDumperScan) {
	o.progress = newPackedScanProgress(o.totals)
	o.totals.recordStarted()
	logPackedScanStarted(o.tctx, o.totals, scan, o.rangeLogFields(index)...)
}

func (o *packedTableObservation) row(key, value []byte) {
	o.progress.record(key, value)
}

func (o *packedTableObservation) rangeFinished(
	index int,
	scan *cseDumperScan,
	scanErr error,
	canceled bool,
	completed bool,
) {
	o.progress.flush()
	o.progress = nil
	fields := o.rangeLogFields(index)
	stats := logPackedScanFinished(o.tctx, o.totals, scan, scanErr, canceled, fields...)
	o.totals.logCSEIdentityAndManifest(o.tctx, stats, fields...)
	if canceled {
		o.totals.recordCanceled(stats)
	} else {
		issue := o.totals.recordFinished(stats, scanErr)
		logPackedCSETelemetryIssue(o.tctx, stats, issue, fields...)
	}
	o.addRangeStats(stats, completed)
}

func (o *packedTableObservation) cancelRange(index int, scan *cseDumperScan, err error) {
	if o.progress != nil {
		o.progress.flush()
		o.progress = nil
	}
	stats := scan.stats()
	fields := o.rangeLogFields(index)
	o.totals.logCSEIdentityAndManifest(o.tctx, stats, fields...)
	o.totals.recordCanceled(stats)
	o.addRangeStats(stats, false)
	processFields := packedScanProcessFields(stats, fields...)
	level := packedDebugLog
	message := "packed backup range scan stopped before EOF"
	if err != nil {
		level = packedWarnLog
		message = "failed to stop packed backup range scan"
		processFields = append(processFields, packedErrorLogField(err))
	}
	level.log(o.tctx, message, processFields...)
	logPackedScanCSEDetails(o.tctx, level, stats, fields...)
	if diagnostics := scan.diagnostics(); diagnostics != "" {
		level.log(o.tctx, "packed backup range scan diagnostics",
			packedTopicFields(fields, zap.String("diagnostics", packedLogText(diagnostics)))...)
	}
}

func (o *packedTableObservation) addRangeStats(stats cseDumperScanStats, completed bool) {
	if completed {
		o.completedRanges++
	}
	o.rows += stats.rows
	o.keyBytes += stats.keyBytes
	o.valueBytes += stats.valueBytes
	o.scanDuration += stats.duration
}

func (o *packedTableObservation) finish(iterErr, closeErr error) {
	if o.finished {
		return
	}
	o.finished = true
	duration := time.Since(o.startedAt)
	success := iterErr == nil && closeErr == nil
	o.totals.recordTable(duration, o.rowDecodeDuration, o.maxRowDecodeDuration, success)
	fields := []zap.Field{
		zap.String("database", o.database),
		zap.String("table", o.table.Name.O),
		zap.Int64("table_id", o.table.ID),
		zap.Int("physical_ranges", len(o.physicalTableIDs)),
		zap.Uint64("completed_ranges", o.completedRanges),
		zap.Uint64("rows", o.rows),
		zap.Uint64("raw_kv_bytes", o.keyBytes+o.valueBytes),
		zap.Duration("cumulative_scan_duration", o.scanDuration),
		zap.Duration("row_decode_duration", o.rowDecodeDuration),
		zap.Duration("max_row_decode_duration", o.maxRowDecodeDuration),
		zap.Duration("duration", duration),
	}
	if iterErr != nil {
		fields = append(fields, zap.String("iterator_error", packedLogText(iterErr.Error())))
	}
	if closeErr != nil {
		fields = append(fields, zap.String("close_error", packedLogText(closeErr.Error())))
	}
	if success && duration >= packedSlowTableThreshold {
		index := o.totals.slowTableLogSlots.Add(1)
		fields = append(fields, zap.Uint64("slow_table_index", index))
		if index <= packedMaxSlowOperationLogs {
			o.tctx.L().Info("slow packed backup table export finished", fields...)
			return
		}
	}
	o.tctx.L().Debug("packed backup table export finished", fields...)
}

type packedExportSummary struct {
	stage                    atomic.Value
	selectedTables           int
	selectedRanges           int
	scheduledTables          int
	scheduledTasks           int
	metadataDuration         time.Duration
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

func logPackedMetadataLoaded(
	tctx *tcontext.Context,
	databases int,
	publicTables int,
	selectedDatabases int,
	selectedTables int,
	selectedRanges int,
	scans packedScanTotalsSnapshot,
	nonScanDuration time.Duration,
	summary *packedExportSummary,
) {
	tctx.L().Info("loaded packed backup metadata",
		zap.Int("databases", databases),
		zap.Int("public_tables", publicTables),
		zap.Int("selected_databases", selectedDatabases),
		zap.Int("selected_tables", selectedTables),
		zap.Int("selected_ranges", selectedRanges),
		zap.Uint64("metadata_scans", scans.completed),
		zap.Uint64("metadata_rows", scans.rows),
		zap.Uint64("metadata_raw_bytes", scans.rawBytes),
		zap.Duration("cumulative_scan_duration", scans.duration),
		zap.Duration("non_scan_duration", nonScanDuration),
		zap.Duration("planning_duration", summary.metadataPlanningDuration),
		zap.Duration("duration", summary.metadataDuration))

	tctx.L().Debug("packed backup metadata CSE selection summary",
		zap.Uint64("manifest_bytes_read", scans.cse.manifestBytesRead),
		zap.Uint64("manifest_shards", scans.cse.manifestShards),
		zap.Uint64("content_files", scans.cse.contentFiles),
		zap.Uint64("plaintext_shards", scans.cse.plaintextShards),
		zap.Uint64("legacy_shards", scans.cse.legacyShards),
		zap.Uint64("cmek_shards", scans.cse.cmekShards),
		zap.Uint64("mixed_encryption_shards", scans.cse.mixedEncryptionShards),
		zap.Uint64("sst_candidates", scans.cse.sstCandidates),
		zap.Uint64("sst_retained", scans.cse.sstRetained),
		zap.Uint64("content_bytes", scans.cse.contentBytes))

	tctx.L().Debug("packed backup metadata CSE timing summary",
		zap.Duration("manifest_load_duration", scans.cse.manifestLoad),
		zap.Duration("snapshot_load_duration", scans.cse.snapshotLoad),
		zap.Duration("iterator_init_duration", scans.cse.iteratorInit),
		zap.Duration("content_read_duration", scans.cse.contentRead),
		zap.Duration("stdout_write_duration", scans.cse.stdoutWrite))
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
	level := packedInfoLog
	message := "finished dumping packed backup"
	fields := []zap.Field{
		zap.String("stage", summary.currentStage()),
		zap.Int("selected_tables", summary.selectedTables),
		zap.Int("selected_ranges", summary.selectedRanges),
		zap.Int("scheduled_tables", summary.scheduledTables),
		zap.Int("scheduled_tasks", summary.scheduledTasks),
		zap.Uint64("finished_rows", uint64(status.FinishedRows)),
		zap.Uint64("finished_uncompressed_bytes", uint64(status.FinishedBytes)),
		zap.Duration("metadata_duration", summary.metadataDuration),
		zap.Duration("metadata_planning_duration", summary.metadataPlanningDuration),
		zap.Duration("task_scheduling_duration", summary.taskSchedulingDuration),
		zap.Duration("schema_build_duration", summary.schemaBuildDuration),
		zap.Duration("task_enqueue_wait_duration", summary.taskEnqueueWaitDuration),
		zap.Duration("duration", time.Since(startedAt)),
	}
	if !summary.writersStartedAt.IsZero() {
		fields = append(fields, zap.Duration("writer_wall_duration", time.Since(summary.writersStartedAt)))
	}
	if resultErr != nil {
		fields = append(fields, packedErrorLogField(resultErr))
		if errors.Cause(resultErr) == context.Canceled {
			message = "packed backup export canceled"
		} else {
			level = packedWarnLog
			message = "packed backup export failed"
		}
	}
	level.log(d.tctx, message, fields...)

	packedInfoLog.log(d.tctx, "packed backup scan process summary",
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
		zap.Uint64("cse_stats_invalid_scans", scans.invalidCSEStats))

	packedInfoLog.log(d.tctx, "packed backup CSE selection summary",
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
		zap.Uint64("cse_content_bytes", scans.cse.contentBytes))

	packedInfoLog.log(d.tctx, "packed backup CSE protocol summary",
		zap.Uint64("cse_stdout_bytes", scans.cse.stdoutBytes),
		zap.Uint64("cse_stdout_write_calls", scans.cse.stdoutWriteCalls),
		zap.Uint64("cse_stdout_write_failures", scans.cse.stdoutWriteFailures),
		zap.Bool("cse_protocol_totals_match",
			scans.cse.reportedScans == scans.completed &&
				scans.missingCSEStats == 0 && scans.invalidCSEStats == 0 &&
				scans.rows == scans.cse.rows && scans.rawBytes == scans.cse.keyBytes+scans.cse.valueBytes),
		zap.Duration("max_cse_scan_first_row_latency", scans.cse.maxScanFirstRow),
		zap.Duration("max_cse_first_stdout_write_latency", scans.cse.maxFirstStdoutWrite),
		zap.Uint64("max_cse_child_peak_rss_bytes", scans.cse.maxPeakRSSBytes))

	packedInfoLog.log(d.tctx, "packed backup CSE setup timing summary",
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
		zap.Duration("max_cse_range_filter_duration", scans.cse.maxRangeFilter),
		zap.Duration("max_cse_snapshot_load_duration", scans.cse.maxSnapshotLoad),
		zap.Duration("max_cse_iterator_init_duration", scans.cse.maxIteratorInit))

	packedInfoLog.log(d.tctx, "packed backup CSE data timing summary",
		zap.Duration("cumulative_cse_content_read_duration", scans.cse.contentRead),
		zap.Duration("max_cse_content_read_duration", scans.cse.maxContentRead),
		zap.Duration("cumulative_cse_iterate_emit_duration", scans.cse.iterateEmit),
		zap.Duration("max_cse_iterate_emit_duration", scans.cse.maxIterateEmit),
		zap.Duration("cumulative_cse_stdout_write_duration", scans.cse.stdoutWrite),
		zap.Duration("max_cse_stdout_write_duration", scans.cse.maxStdoutWrite),
		zap.Duration("cumulative_cse_scan_duration", scans.cse.scan),
		zap.Duration("cumulative_cse_flush_duration", scans.cse.flush),
		zap.Duration("cumulative_cse_total_duration", scans.cse.total))

	packedInfoLog.log(d.tctx, "packed backup table pipeline summary",
		zap.Uint64("packed_tables", scans.tables),
		zap.Uint64("failed_packed_tables", scans.failedTables),
		zap.Duration("cumulative_packed_table_pipeline_duration", scans.tableDuration),
		zap.Duration("max_packed_table_pipeline_duration", scans.maxTableDuration),
		zap.Duration("cumulative_packed_row_decode_duration", scans.rowDecode),
		zap.Duration("max_packed_row_decode_duration", scans.maxRowDecode))

	packedInfoLog.log(d.tctx, "packed backup output storage summary",
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
		zap.Uint64("slow_output_storage_operations", output.slowOperations))
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
