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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	writeInterval = 60
	// The timeout can not be too long because the pod grace termination period is fixed.
	writeTimeout = 10 * time.Second
	category     = "dxf"
)

// TaskType is the type of metering during DXF execution.
type TaskType string

const (
	// TaskTypeUnknown is the type of metering when task type is unknown.
	TaskTypeUnknown TaskType = ""
	// TaskTypeAddIndex is the type of metering during add index.
	TaskTypeAddIndex TaskType = "add-index"
	// TaskTypeImportInto is the type of metering during import into.
	TaskTypeImportInto TaskType = "import-into"
)

var meteringInstance atomic.Pointer[Meter]

// Recorder is used to record metering data.
type Recorder struct {
	m        *Meter
	taskID   int64
	taskType TaskType
	keyspace string
}

// NewRecorder creates a new recorder.
func NewRecorder(store kv.Storage, taskType TaskType, taskID int64) *Recorder {
	if kerneltype.IsClassic() || meteringInstance.Load() == nil {
		return &Recorder{}
	}
	return &Recorder{
		m:        meteringInstance.Load(),
		taskID:   taskID,
		taskType: taskType,
		keyspace: store.GetKeyspace(),
	}
}

// RecordGetRequestCount records the get request count.
func (r *Recorder) RecordGetRequestCount(v uint64) {
	r.record(&Data{getRequests: v})
}

// RecordPutRequestCount records the put request count.
func (r *Recorder) RecordPutRequestCount(v uint64) {
	r.record(&Data{putRequests: v})
}

// RecordReadDataBytes records the read data bytes.
func (r *Recorder) RecordReadDataBytes(v uint64) {
	r.record(&Data{readDataBytes: v})
}

// RecordWriteDataBytes records the write data bytes.
func (r *Recorder) RecordWriteDataBytes(v uint64) {
	r.record(&Data{writeDataBytes: v})
}

func (r *Recorder) record(d *Data) {
	if r.m == nil {
		return
	}
	r.m.Record(r.keyspace, r.taskType, r.taskID, d)
}

// GetMetering gets the metering instance.
func GetMetering() (*Meter, error) {
	v := meteringInstance.Load()
	if v == nil {
		return nil, errors.New("metering instance is not initialized")
	}
	return v, nil
}

// SetMetering sets the metering instance for dxf.
func SetMetering(m *Meter) {
	meteringInstance.Store(m)
}

// Data represents the metering data.
type Data struct {
	keyspace string
	taskType TaskType

	putRequests    uint64
	getRequests    uint64
	readDataBytes  uint64
	writeDataBytes uint64
}

func (m *Data) merge(other *Data) {
	if m.keyspace == "" || m.taskType == TaskTypeUnknown {
		m.keyspace = other.keyspace
		m.taskType = other.taskType
	}
	m.putRequests += other.putRequests
	m.getRequests += other.getRequests
	m.readDataBytes += other.readDataBytes
	m.writeDataBytes += other.writeDataBytes
}

type meteringEntry struct {
	keyspace string
	taskID   int64
	taskType TaskType
}

// Meter is responsible for recording and reporting metering data.
type Meter struct {
	sync.Mutex
	ctx    context.Context
	data   map[int64]Data // taskID -> meter data
	uuid   string
	writer *meteringwriter.MeteringWriter
	logger *zap.Logger
}

// NewMeter creates a new Meter instance.
func NewMeter(cfg *mconfig.MeteringConfig) (*Meter, error) {
	logger := logutil.BgLogger().With(zap.String("component", "meter"))
	if len(cfg.Bucket) == 0 {
		return nil, nil
	}
	if cfg.AWS == nil {
		return nil, nil
	}
	providerType := storage.ProviderTypeS3
	if len(cfg.Type) > 0 {
		providerType = cfg.Type
	}

	s3Config := &storage.ProviderConfig{
		Type:   providerType,
		Bucket: cfg.Bucket,
		Region: cfg.Region,
		Prefix: cfg.Prefix,
		AWS: &storage.AWSConfig{
			AssumeRoleARN: cfg.AWS.AssumeRoleARN,
		},
	}
	provider, err := storage.NewObjectStorageProvider(s3Config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create storage provider")
	}
	meteringConfig := mconfig.DefaultConfig().WithLogger(logger)
	writer := meteringwriter.NewMeteringWriter(provider, meteringConfig)
	return &Meter{
		logger: logger,
		data:   make(map[int64]Data),
		writer: writer,
		uuid:   strings.ReplaceAll(uuid.New().String(), "-", "_"), // no dash in the metering sdk
	}, nil
}

// Record records a meter data.
func (m *Meter) Record(keyspace string, taskType TaskType, taskID int64, other *Data) {
	m.Lock()
	defer m.Unlock()
	other.keyspace = keyspace
	other.taskType = taskType
	orig := m.data[taskID]
	orig.merge(other)
	m.data[taskID] = orig
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
			m.flush(nextTime.Unix())
			nextTime = nextTime.Add(time.Minute)
			curTime = time.Now()
		}
	}
	// Try our best to flush the final data even after closing.
	m.flush(nextTime.Unix())
	err := m.writer.Close()
	m.logger.Warn("metering writer closed", zap.Error(err))
}

func (m *Meter) flush(ts int64) {
	startTime := time.Now()
	var data map[int64]Data
	m.Lock()
	data = m.data
	m.data = make(map[int64]Data, len(data))
	m.Unlock()

	if len(data) == 0 {
		return
	}
	array := make([]map[string]any, 0, len(data))
	for taskID, d := range data {
		array = append(array, map[string]any{
			"version":               "1",
			"cluster_id":            d.keyspace,
			"source_name":           category,
			"task_type":             d.taskType,
			"task_id":               &common.MeteringValue{Value: uint64(taskID)},
			"put_external_requests": &common.MeteringValue{Value: d.putRequests},
			"get_external_requests": &common.MeteringValue{Value: d.getRequests},
			"read_data_bytes":       &common.MeteringValue{Value: d.readDataBytes, Unit: "bytes"},
			"write_data_bytes":      &common.MeteringValue{Value: d.writeDataBytes, Unit: "bytes"},
		})
	}

	meteringData := &common.MeteringData{
		SelfID:    m.uuid,
		Timestamp: ts,
		Category:  category,
		Data:      array,
	}
	flushCtx, cancel := context.WithTimeout(m.ctx, writeTimeout)
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
