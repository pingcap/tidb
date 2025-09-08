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

package meter

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/metering_sdk/common"
	mconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	writeInterval = 60
	// The timeout can not be too long because the pod grace termination period is fixed.
	writeTimeout = 10 * time.Second
	category     = "dxf"
)

var meteringInstance atomic.Pointer[Meter]

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

// Config represents the configuration for metering.
type Config struct {
	Type    storage.ProviderType `toml:"type" json:"type"`
	Bucket  string               `toml:"bucket" json:"bucket"`
	Prefix  string               `toml:"prefix" json:"prefix"`
	Region  string               `toml:"region" json:"region"`
	RoleARN string               `toml:"role-arn" json:"role-arn"`
}

// MeterData
type MeterData struct {
	putRequests uint64
	getRequests uint64
}

func (m *MeterData) merge(other *MeterData) {
	m.putRequests += other.putRequests
	m.getRequests += other.getRequests
}

// Meter is responsible for recording and reporting metering data.
type Meter struct {
	sync.Mutex
	ctx    context.Context
	data   map[string]MeterData // keyspace -> meter data
	uuid   string
	writer *meteringwriter.MeteringWriter
	logger *zap.Logger
}

// NewMeter creates a new Meter instance.
func NewMeter(cfg *Config) (*Meter, error) {
	logger := logutil.BgLogger().With(zap.String("component", "meter"))
	if len(cfg.Bucket) == 0 {
		return nil, nil
	}
	providerType := storage.ProviderTypeS3
	if len(cfg.Type) > 0 {
		providerType = storage.ProviderType(cfg.Type)
	}

	s3Config := &storage.ProviderConfig{
		Type:   providerType,
		Bucket: cfg.Bucket,
		Region: cfg.Region,
		Prefix: cfg.Prefix,
		AWS: &storage.AWSConfig{
			AssumeRoleARN: cfg.RoleARN,
		},
	}
	provider, err := storage.NewObjectStorageProvider(s3Config)
	if err != nil {
		logger.Error("failed to create storage provider", zap.Error(err))
		return nil, err
	}
	meteringConfig := mconfig.DefaultConfig().WithLogger(logger)
	writer := meteringwriter.NewMeteringWriter(provider, meteringConfig)
	return &Meter{
		logger: logger,
		data:   make(map[string]MeterData),
		writer: writer,
		uuid:   strings.ReplaceAll(uuid.New().String(), "-", "_"), // no dash in the S3 path
	}, nil
}

// Record records a meter data.
func (m *Meter) Record(userKeyspace string, other *MeterData) {
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
}

func (m *Meter) flush(ts int64, timeout time.Duration) {
	var data map[string]MeterData
	m.Lock()
	data = m.data
	m.data = make(map[string]MeterData, len(data))
	m.Unlock()

	if len(data) == 0 {
		return
	}
	array := make([]map[string]any, len(data))
	for keyspace, d := range data {
		array = append(array, map[string]any{
			"version":         "1",
			"cluster_id":      keyspace,
			"source_name":     category,
			"s3_put_requests": &common.MeteringValue{Value: uint64(d.putRequests), Unit: "count"},
			"s3_get_requests": &common.MeteringValue{Value: uint64(d.getRequests), Unit: "count"},
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
		m.logger.Error("failed to write metering data", zap.Error(err))
	}
}

// Close closes the metering writer.
func (m *Meter) Close() error {
	if m.writer != nil {
		err := m.writer.Close()
		if err != nil {
			m.logger.Error("failed to close metering writer", zap.Error(err))
		}
		return err
	}
	return nil
}
