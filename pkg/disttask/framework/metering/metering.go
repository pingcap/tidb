// Copyright 2025 PingCAP, Inc.
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

package metering

import (
	"context"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/metering_sdk/common"
	mconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriterapi "github.com/pingcap/metering_sdk/writer"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/dxfmetric"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// The timeout can not be too long because the pod grace termination period is fixed.
	writeTimeout = 10 * time.Second
	category     = "dxf"
	// maxRetryCount defines the maximum retry count for writing metering data.
	// if the write still fails after maxRetryCount, those data will be dropped.
	maxRetryCount = 10
	retryInterval = 5 * time.Second
)

var (
	// FlushInterval is the interval to flush metering data.
	// exported for testing.
	FlushInterval = time.Minute

	meteringInstance atomic.Pointer[Meter]
)

// RegisterRecorder returns the Recorder for the given task.
func RegisterRecorder(task *proto.TaskBase) *Recorder {
	meter := meteringInstance.Load()
	if kerneltype.IsClassic() || meter == nil {
		return &Recorder{}
	}
	return meter.getOrRegisterRecorder(&Recorder{
		taskID:   task.ID,
		taskType: task.Type.String(),
		keyspace: task.Keyspace,
	})
}

// UnregisterRecorder unregisters the Recorder for the given task.
// metering should make sure to flush the un-flushed data after unregistering.
func UnregisterRecorder(taskID int64) {
	meter := meteringInstance.Load()
	if kerneltype.IsClassic() || meter == nil {
		return
	}
	meter.unregisterRecorder(taskID)
}

// WriteMeterData writes the metering data.
// ts+category+uuid uniquely identifies a metering data file, the SDK also use the
// shared-pool-id in the file name, but for each meter writer, it's the same.
func WriteMeterData(ctx context.Context, ts int64, uuid string, items []map[string]any) error {
	meter := meteringInstance.Load()
	if kerneltype.IsClassic() || meter == nil {
		return nil
	}
	return meter.WriteMeterData(ctx, ts, uuid, items)
}

// SetMetering sets the metering instance for dxf.
func SetMetering(m *Meter) {
	meteringInstance.Store(m)
}

type wrappedRecorder struct {
	*Recorder
	unregistered bool
}

type writeFailData struct {
	ts       int64
	retryCnt int
	items    []map[string]any
}

type retryData struct {
	mu sync.Mutex
	// TS -> write failed data
	data map[int64]*writeFailData
}

func (d *retryData) addFailedData(ts int64, items []map[string]any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.data == nil {
		d.data = make(map[int64]*writeFailData)
	}
	d.data[ts] = &writeFailData{ts: ts, items: items}
}

func (d *retryData) getDataClone() map[int64]*writeFailData {
	d.mu.Lock()
	defer d.mu.Unlock()
	return maps.Clone(d.data)
}

func (d *retryData) remove(needRemove []*writeFailData) {
	if len(needRemove) == 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, wd := range needRemove {
		delete(d.data, wd.ts)
	}
}

// Meter is responsible for recording and reporting metering data.
type Meter struct {
	mu        sync.Mutex
	recorders map[int64]*wrappedRecorder
	// taskID -> last flushed data
	// when flushing, we scrape the latest data from recorders and calculate the
	// delta and write to the metering storage.
	// we will store the latest data here regardless of whether the flush is
	// successful or not,
	lastFlushedData map[int64]*Data
	// pendingRetryData is the data that failed to write and need to retry.
	pendingRetryData retryData
	uuid             string
	writer           meteringwriterapi.MeteringWriter
	logger           *zap.Logger
	wg               util.WaitGroupWrapper
}

// NewMeter creates a new Meter instance.
func NewMeter(cfg *mconfig.MeteringConfig) (*Meter, error) {
	logger := logutil.BgLogger().With(zap.String("component", "meter"))
	if len(cfg.Type) == 0 || len(cfg.Bucket) == 0 {
		return nil, nil
	}
	s3Config := cfg.ToProviderConfig()
	provider, err := storage.NewObjectStorageProvider(s3Config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create storage provider")
	}
	// if there are network issues, we might successfully write the metering data,
	// but the writer still returns error, we will retry write with the same TS
	// in this case, which means the metering data file will have the same name,
	// we set WithOverwriteExisting to true to avoid the retry write report error
	// in this case.
	// IgnoreExisting wound be more appropriate, but the SDK doesn't provide it.
	meteringConfig := mconfig.DefaultConfig().WithLogger(logger).WithOverwriteExisting(true)
	writer := meteringwriter.NewMeteringWriterFromConfig(provider, meteringConfig, cfg)
	return newMeterWithWriter(logger, writer), nil
}

func newMeterWithWriter(logger *zap.Logger, writer meteringwriterapi.MeteringWriter) *Meter {
	return &Meter{
		logger:          logger,
		recorders:       make(map[int64]*wrappedRecorder),
		lastFlushedData: make(map[int64]*Data),
		writer:          writer,
		uuid:            strings.ReplaceAll(uuid.New().String(), "-", "_"), // no dash in the metering sdk
	}
}

func (m *Meter) getOrRegisterRecorder(r *Recorder) *Recorder {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.recorders[r.taskID]; ok {
		// each task might have different steps, it's possible for below sequence
		//  - step 1 get recorder
		//  - step 1 executor exist, and unregisters recorder, but not flushed yet
		//  - step 2 get recorder again, we should reset the unregistered flag
		if old.unregistered {
			old.unregistered = false
		}
		return old.Recorder
	}
	m.recorders[r.taskID] = &wrappedRecorder{Recorder: r}
	return r
}

// UnregisterRecorder unregisters a recorder.
func (m *Meter) unregisterRecorder(taskID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// we still need to flush for the unregistered recorder once more, so we only
	// mark it here, and delete when it's flushed.
	if r, ok := m.recorders[taskID]; ok {
		r.unregistered = true
	}
}

func (m *Meter) cleanupUnregisteredRecorders() []*Recorder {
	removed := make([]*Recorder, 0, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	for taskID, r := range m.recorders {
		if !r.unregistered {
			continue
		}
		// since register and flush run in async, it's possible that:
		//  - flush start, and scrape current data(without recorder R)
		//  - register recorder R, and unregister fast
		//  - flush finish, so here lastFlushedData doesn't contain R, we should
		//    keep the recorder and do a final flush.
		if fd, ok := m.lastFlushedData[taskID]; ok {
			// unregister and scrape is run in async, it's possible there are still
			// some non-flushed data even the recorder is unregistered, so we check
			// current data too.
			if fd.equals(r.currData()) {
				delete(m.recorders, taskID)
				delete(m.lastFlushedData, taskID)
				removed = append(removed, r.Recorder)
			}
		}
	}

	return removed
}

func (m *Meter) afterFlush(flushedData map[int64]*Data) {
	m.lastFlushedData = flushedData
	removedRecorders := m.cleanupUnregisteredRecorders()
	for _, r := range removedRecorders {
		data := r.currData()
		failpoint.Call(_curpkg_("meteringFinalFlush"), data)
		m.logger.Info("recorder unregistered and finished final flush",
			zap.Stringer("accumulatedData", data))
	}
}

func (m *Meter) scrapeCurrData() map[int64]*Data {
	m.mu.Lock()
	defer m.mu.Unlock()
	data := make(map[int64]*Data, len(m.recorders))
	for taskID, r := range m.recorders {
		data[taskID] = r.currData()
	}
	return data
}

func (m *Meter) calculateDataItems(currData map[int64]*Data) []map[string]any {
	items := make([]map[string]any, 0, len(currData))
	for taskID, curr := range currData {
		theLast := &Data{}
		if last, ok := m.lastFlushedData[taskID]; ok {
			theLast = last
		}
		if item := curr.calMeterDataItem(theLast); item != nil {
			items = append(items, item)
		}
	}
	return items
}

// StartFlushLoop creates a flush loop.
func (m *Meter) StartFlushLoop(ctx context.Context) {
	m.wg.RunWithLog(func() {
		m.flushLoop(ctx)
	})
	m.wg.RunWithLog(func() {
		m.retryLoop(ctx)
	})
	m.wg.Wait()
}

func (m *Meter) retryLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryInterval):
		}

		m.retryWrite(ctx)
	}
}

func (m *Meter) retryWrite(ctx context.Context) {
	data := m.pendingRetryData.getDataClone()
	if len(data) == 0 {
		return
	}

	var (
		firstErr   error
		needRemove = make([]*writeFailData, 0, len(data))
	)
	for ts, wd := range data {
		err := m.WriteMeterData(ctx, ts, m.uuid, wd.items)
		if err == nil {
			m.logger.Info("succeed to write metering data after retry",
				zap.Int64("timestamp", ts), zap.Int("retry-count", wd.retryCnt),
				zap.Any("data", wd.items))
			needRemove = append(needRemove, wd)
			continue
		}

		if ctx.Err() != nil {
			break
		}
		if firstErr == nil {
			firstErr = err
		}

		wd.retryCnt++
		if wd.retryCnt >= maxRetryCount {
			m.logger.Warn("dropping metering data after max retry count reached",
				zap.Int64("timestamp", ts), zap.Int("retry-count", wd.retryCnt),
				zap.Any("data", wd.items), zap.Error(err))
			needRemove = append(needRemove, wd)
		}
	}

	if firstErr != nil {
		m.logger.Warn("failed to retry writing some metering data", zap.Error(firstErr))
	}

	m.pendingRetryData.remove(needRemove)
}

func (m *Meter) flushLoop(ctx context.Context) {
	// Control the writing timestamp accurately enough so that the previous round won't be overwritten by the next round.
	curTime := time.Now()
	nextTime := curTime.Truncate(FlushInterval).Add(FlushInterval)
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-time.After(nextTime.Sub(curTime)):
			m.flush(ctx, nextTime.Unix())
			nextTime = nextTime.Add(FlushInterval)
			curTime = time.Now()
		}
	}
	// Try our best to flush the final data even after closing.
	m.flush(ctx, nextTime.Unix())
	err := m.writer.Close()
	if err != nil {
		m.logger.Warn("metering writer closed", zap.Error(err))
	}
}

func (m *Meter) flush(ctx context.Context, ts int64) {
	startTime := time.Now()
	currData := m.scrapeCurrData()
	items := m.calculateDataItems(currData)
	logger := m.logger.With(zap.Int64("timestamp", ts))
	if len(items) == 0 {
		logger.Info("no metering data to flush", zap.Int("recorder-count", len(currData)),
			zap.Duration("duration", time.Since(startTime)))
		m.afterFlush(currData)
		return
	}

	// each metering background loop sends data with the same uuid.
	if err := m.WriteMeterData(ctx, ts, m.uuid, items); err != nil {
		logger.Warn("failed to write metering data", zap.Error(err),
			zap.Duration("duration", time.Since(startTime)),
			zap.Any("data", items))
		// metering expect incremental data. due to the case described in NewMeter,
		// we can only retry the data with given TS, and cannot accumulate with
		// new data and send with new TS as this will cause data duplication.
		m.pendingRetryData.addFailedData(ts, items)
	} else {
		logger.Info("succeed to write metering data",
			zap.Duration("duration", time.Since(startTime)),
			zap.Any("data", items))
	}
	m.afterFlush(currData)
}

// WriteMeterData writes the metering data.
func (m *Meter) WriteMeterData(ctx context.Context, ts int64, uuid string, items []map[string]any) (err error) {
	failpoint.Call(_curpkg_("forceTSAtMinuteBoundary"), &ts)
	meteringData := &common.MeteringData{
		SelfID:    uuid,
		Timestamp: ts,
		Category:  category,
		Data:      items,
	}
	flushCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer func() {
		cancel()
		if err != nil {
			// task executor will delete counters relates to a task after the task
			// is finished, so we use "-" as the task id label here to avoid this,
			// as we also write meter data during cleanup.
			dxfmetric.ExecuteEventCounter.WithLabelValues("-", dxfmetric.EventMeterWriteFailed).Add(1)
		}
	}()

	return m.writer.Write(flushCtx, meteringData)
}

// Close closes the metering writer.
func (m *Meter) Close() error {
	if m.writer != nil {
		err := m.writer.Close()
		if err != nil {
			m.logger.Warn("failed to close metering writer", zap.Error(err))
		}
		return err
	}
	return nil
}
