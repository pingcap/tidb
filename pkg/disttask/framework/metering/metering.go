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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	writeInterval = 60
	// The timeout can not be too long because the pod grace termination period is fixed.
	writeTimeout = 10 * time.Second
	category     = "disttask"
)

// TaskType is the type of metering during DXF execution.
type TaskType string

const (
	// TaskTypeAddIndex is the type of metering during add index.
	TaskTypeAddIndex = "add-index"
	// TaskTypeImportInto is the type of metering during import into.
	TaskTypeImportInto = "import-into"
)

var meteringInstance atomic.Pointer[Meter]

// RecordDXFS3GetRequests records the S3 GET requests for DXF.
func RecordDXFS3GetRequests(store kv.Storage, taskType TaskType, taskID int64, getReqCnt uint64) {
	if !kv.IsUserKS(store) || meteringInstance.Load() == nil {
		return
	}
	userKS := store.GetKeyspace()
	meteringInstance.Load().Record(userKS, &Data{
		taskType:    string(taskType),
		taskID:      taskID,
		getRequests: getReqCnt,
	})
}

// RecordDXFS3PutRequests records the S3 PUT requests for DXF.
func RecordDXFS3PutRequests(store kv.Storage, taskType TaskType, taskID int64, putReqCnt uint64) {
	if !kv.IsUserKS(store) || meteringInstance.Load() == nil {
		return
	}
	userKS := store.GetKeyspace()
	meteringInstance.Load().Record(userKS, &Data{
		taskType:    string(taskType),
		taskID:      taskID,
		putRequests: putReqCnt,
	})
}

// RecordDXFReadDataBytes records the scan data bytes from user store for DXF.
func RecordDXFReadDataBytes(store kv.Storage, taskType TaskType, taskID int64, size uint64) {
	if !kv.IsUserKS(store) || meteringInstance.Load() == nil {
		return
	}
	userKS := store.GetKeyspace()
	meteringInstance.Load().Record(userKS, &Data{
		taskType:      string(taskType),
		taskID:        taskID,
		readDataBytes: size,
	})
}

// RecordDXFWriteDataBytes records the write data size for DXF.
func RecordDXFWriteDataBytes(store kv.Storage, taskType TaskType, taskID int64, size uint64) {
	if !kv.IsUserKS(store) || meteringInstance.Load() == nil {
		return
	}
	userKS := store.GetKeyspace()
	meteringInstance.Load().Record(userKS, &Data{
		taskType:       string(taskType),
		taskID:         taskID,
		writeDataBytes: size,
	})
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
	putRequests    uint64
	getRequests    uint64
	readDataBytes  uint64
	writeDataBytes uint64

	taskType string
	taskID   int64
}

func (m *Data) merge(other *Data) {
	m.putRequests += other.putRequests
	m.getRequests += other.getRequests
	m.readDataBytes += other.readDataBytes
	m.writeDataBytes += other.writeDataBytes
}

// Meter is responsible for recording and reporting metering data.
type Meter struct {
	sync.Mutex
	ctx    context.Context
	data   map[string]Data // keyspace -> meter data
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
		data:   make(map[string]Data),
		writer: writer,
		uuid:   strings.ReplaceAll(uuid.New().String(), "-", "_"), // no dash in the S3 path
	}, nil
}

// Record records a meter data.
func (m *Meter) Record(userKeyspace string, other *Data) {
	m.Lock()
	defer m.Unlock()
	orig := m.data[userKeyspace]
	orig.merge(other)
	m.data[userKeyspace] = orig
}

// StartFlushLoop creates a flush loop.
func (m *Meter) StartFlushLoop(ctx context.Context) {
	// Control the writing timestamp accurately enough so that the previous round won't be overwritten by the next round.
	curTime := time.Now().Unix()
	nextTime := curTime/writeInterval*writeInterval + writeInterval
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(nextTime-curTime) * time.Second):
			m.flush(nextTime, writeTimeout)
			nextTime += writeInterval
			curTime = time.Now().Unix()
		}
	}
	// Try our best to flush the final data even after closing.
	m.flush(nextTime, writeTimeout)
	err := m.writer.Close()
	m.logger.Warn("metering writer closed", zap.Error(err))
}

func (m *Meter) flush(ts int64, timeout time.Duration) {
	var data map[string]Data
	m.Lock()
	data = m.data
	m.data = make(map[string]Data, len(data))
	m.Unlock()

	if len(data) == 0 {
		return
	}
	array := make([]map[string]any, 0, len(data))
	for keyspace, d := range data {
		array = append(array, map[string]any{
			"version":          "1",
			"cluster_id":       keyspace,
			"source_name":      category,
			"task_type":        d.taskType,
			"task_id":          &common.MeteringValue{Value: uint64(d.taskID)},
			"s3_put_requests":  &common.MeteringValue{Value: d.putRequests},
			"s3_get_requests":  &common.MeteringValue{Value: d.getRequests},
			"read_data_bytes":  &common.MeteringValue{Value: d.readDataBytes, Unit: "bytes"},
			"write_data_bytes": &common.MeteringValue{Value: d.writeDataBytes, Unit: "bytes"},
		})
	}

	meteringData := &common.MeteringData{
		SelfID:    m.uuid,
		Timestamp: ts,
		Category:  category,
		Data:      array,
	}
	flushCtx, cancel := context.WithTimeout(m.ctx, timeout)
	defer cancel()
	if err := m.writer.Write(flushCtx, meteringData); err != nil {
		m.logger.Warn("failed to write metering data", zap.Error(err))
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
