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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/metering_sdk/common"
	mconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// The timeout can not be too long because the pod grace termination period is fixed.
	writeTimeout = 10 * time.Second
	category     = "dxf"
)

// task type values for billing service, those names are better for display than
// DXF task type.
const (
	taskTypeUnknown    = "unknown"
	taskTypeAddIndex   = "add-index"
	taskTypeImportInto = "import-into"
)

var meteringInstance atomic.Pointer[Meter]

// Recorder is used to record metering data.
type Recorder struct {
	taskID         int64
	keyspace       string
	taskType       string
	getRequests    atomic.Uint64
	putRequests    atomic.Uint64
	readDataBytes  atomic.Uint64
	writeDataBytes atomic.Uint64
}

// RegisterRecorder returns the Recorder for the given task.
func RegisterRecorder(task *proto.TaskBase) *Recorder {
	meter := meteringInstance.Load()
	if kerneltype.IsClassic() || meter == nil {
		return &Recorder{}
	}
	tp := taskTypeUnknown
	switch task.Type {
	case proto.ImportInto:
		tp = taskTypeImportInto
	case proto.Backfill:
		tp = taskTypeAddIndex
	}
	return meter.getOrRegisterRecorder(&Recorder{
		taskID:   task.ID,
		taskType: tp,
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

// RecordGetRequestCount records the get request count.
func (r *Recorder) RecordGetRequestCount(v uint64) {
	r.getRequests.Add(v)
}

// RecordPutRequestCount records the put request count.
func (r *Recorder) RecordPutRequestCount(v uint64) {
	r.putRequests.Add(v)
}

// RecordReadDataBytes records the read data bytes.
func (r *Recorder) RecordReadDataBytes(v uint64) {
	r.readDataBytes.Add(v)
}

// RecordWriteDataBytes records the write data bytes.
func (r *Recorder) RecordWriteDataBytes(v uint64) {
	r.writeDataBytes.Add(v)
}

func (r *Recorder) currData() *Data {
	return &Data{
		taskID:      r.taskID,
		keyspace:    r.keyspace,
		taskType:    r.taskType,
		getRequests: r.getRequests.Load(),
		putRequests: r.putRequests.Load(),
		readBytes:   r.readDataBytes.Load(),
		writeBytes:  r.writeDataBytes.Load(),
	}
}

// SetMetering sets the metering instance for dxf.
func SetMetering(m *Meter) {
	meteringInstance.Store(m)
}

// Data represents the metering data.
// we might use this struct to store accumulated or diff data.
type Data struct {
	taskID   int64
	keyspace string
	taskType string

	getRequests uint64
	putRequests uint64
	readBytes   uint64
	writeBytes  uint64
}

func (m *Data) merge(other *Data) {
	m.getRequests += other.getRequests
	m.putRequests += other.putRequests
	m.readBytes += other.readBytes
	m.writeBytes += other.writeBytes
}

func (m *Data) isNotEmpty() bool {
	return m.getRequests > 0 || m.putRequests > 0 || m.readBytes > 0 || m.writeBytes > 0
}

func (m *Data) isSame(other *Data) bool {
	return m.getRequests == other.getRequests &&
		m.putRequests == other.putRequests &&
		m.readBytes == other.readBytes &&
		m.writeBytes == other.writeBytes
}

// String implements fmt.Stringer interface.
func (m *Data) String() string {
	return fmt.Sprintf("{taskID: %d, keyspace: %s, taskType: %s, getReqs: %d, putReqs: %d, readBytes: %d, writeBytes: %d}",
		m.taskID,
		m.keyspace,
		m.taskType,
		m.getRequests,
		m.putRequests,
		m.readBytes,
		m.writeBytes)
}

type wrappedRecorder struct {
	*Recorder
	unregistered bool
}

// Meter is responsible for recording and reporting metering data.
type Meter struct {
	sync.Mutex
	recorders map[int64]*wrappedRecorder
	// taskID -> last flushed data
	// when flushing, we scrape the latest data from recorders and calculate the
	// diff and write to the metering storage.
	lastFlushedData map[int64]*Data
	uuid            string
	writer          *meteringwriter.MeteringWriter
	logger          *zap.Logger
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
	meteringConfig := mconfig.DefaultConfig().WithLogger(logger)
	writer := meteringwriter.NewMeteringWriterFromConfig(provider, meteringConfig, cfg)
	return &Meter{
		logger:          logger,
		recorders:       make(map[int64]*wrappedRecorder),
		lastFlushedData: make(map[int64]*Data),
		writer:          writer,
		uuid:            strings.ReplaceAll(uuid.New().String(), "-", "_"), // no dash in the metering sdk
	}, nil
}

func (m *Meter) getOrRegisterRecorder(r *Recorder) *Recorder {
	m.Lock()
	defer m.Unlock()
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
	m.Lock()
	defer m.Unlock()
	// we still need to flush for the unregistered recorder once more, so we only
	// mark it here, and delete when it's flushed.
	if r, ok := m.recorders[taskID]; ok {
		r.unregistered = true
	}
}

func (m *Meter) cleanupUnregisteredRecorders() []*Recorder {
	removed := make([]*Recorder, 0, 1)
	m.Lock()
	defer m.Unlock()
	for taskID, r := range m.recorders {
		if !r.unregistered {
			continue
		}
		// since register and flush run in async, it's possible that:
		//  - flush start, and scrape current data(doesn't contain R)
		//  - register recorder R, and unregister fast
		//  - flush finish, so here lastFlushedData doesn't contain R
		if fd, ok := m.lastFlushedData[taskID]; ok {
			// unregister and scrape is run in async, it's possible there are still
			// some non-flushed data even the recorder is unregistered, so we check
			// current data too.
			if fd.isSame(r.currData()) {
				delete(m.recorders, r.taskID)
				removed = append(removed, r.Recorder)
			}
		}
	}

	return removed
}

func (m *Meter) onSuccessFlush(flushedData map[int64]*Data) {
	m.lastFlushedData = flushedData
	removedRecorders := m.cleanupUnregisteredRecorders()
	for _, r := range removedRecorders {
		m.logger.Info("recorder unregistered and finished final flush",
			zap.Stringer("accumulatedData", r.currData()))
	}
}

func (m *Meter) scrapeCurrData() map[int64]*Data {
	m.Lock()
	defer m.Unlock()
	data := make(map[int64]*Data, len(m.recorders))
	for taskID, r := range m.recorders {
		data[taskID] = r.currData()
	}
	return data
}

func (m *Meter) calculateDataDiffs(currData map[int64]*Data) map[int64]*Data {
	diffs := make(map[int64]*Data, len(currData))
	for taskID, curr := range currData {
		diff := curr
		if last, ok := m.lastFlushedData[taskID]; ok {
			diff = &Data{
				taskID:      taskID,
				keyspace:    curr.keyspace,
				taskType:    curr.taskType,
				getRequests: curr.getRequests - last.getRequests,
				putRequests: curr.putRequests - last.putRequests,
				readBytes:   curr.readBytes - last.readBytes,
				writeBytes:  curr.writeBytes - last.writeBytes,
			}
		}
		if diff.isNotEmpty() {
			diffs[taskID] = diff
		}
	}
	return diffs
}

// StartFlushLoop creates a flush loop.
func (m *Meter) StartFlushLoop(ctx context.Context) {
	// Control the writing timestamp accurately enough so that the previous round won't be overwritten by the next round.
	curTime := time.Now()
	nextTime := curTime.Truncate(time.Minute).Add(time.Minute)
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-time.After(nextTime.Sub(curTime)):
			m.flush(ctx, nextTime.Unix())
			nextTime = nextTime.Add(time.Minute)
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
	diffs := m.calculateDataDiffs(currData)
	if len(diffs) == 0 {
		m.logger.Info("no metering data to flush", zap.Duration("duration", time.Since(startTime)))
		m.onSuccessFlush(currData)
		return
	}
	array := make([]map[string]any, 0, len(diffs))
	for taskID, d := range diffs {
		array = append(array, map[string]any{
			"version":               "1",
			"cluster_id":            d.keyspace,
			"source_name":           category,
			"task_type":             d.taskType,
			"task_id":               taskID,
			"get_external_requests": d.getRequests,
			"put_external_requests": d.putRequests,
			"read_data_bytes":       &common.MeteringValue{Value: d.readBytes, Unit: "bytes"},
			"write_data_bytes":      &common.MeteringValue{Value: d.writeBytes, Unit: "bytes"},
		})
	}

	meteringData := &common.MeteringData{
		SelfID:    m.uuid,
		Timestamp: ts,
		Category:  category,
		Data:      array,
	}
	flushCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	if err := m.writer.Write(flushCtx, meteringData); err != nil {
		m.logger.Warn("failed to write metering data",
			zap.Error(err),
			zap.Duration("duration", time.Since(startTime)),
			zap.Any("data", meteringData))
	} else {
		m.logger.Info("succeed to write metering data",
			zap.Duration("duration", time.Since(startTime)),
			zap.Any("data", meteringData))
		m.onSuccessFlush(currData)
	}
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
