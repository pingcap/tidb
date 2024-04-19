// Copyright 2022 PingCAP, Inc.
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

package server

import (
	"database/sql"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type options struct {
	dumpFileStorage   storage.ExternalStorage
	checkpointStorage storage.ExternalStorage
	checkpointName    string
	promFactory       promutil.Factory
	promRegistry      promutil.Registry
	logger            log.Logger
	dupIndicator      *atomic.Bool
	// only used in tests
	db *sql.DB
}

// Option is a function that configures a lightning task.
type Option func(*options)

// WithDumpFileStorage sets the external storage to a lightning task.
// Typically, the external storage is set when lightning is integrated with dataflow engine by DM.
func WithDumpFileStorage(s storage.ExternalStorage) Option {
	return func(o *options) {
		o.dumpFileStorage = s
	}
}

// WithCheckpointStorage sets the checkpoint name in external storage to a lightning task.
// Typically, the checkpoint name is set when lightning is integrated with dataflow engine by DM.
func WithCheckpointStorage(s storage.ExternalStorage, cpName string) Option {
	return func(o *options) {
		o.checkpointStorage = s
		o.checkpointName = cpName
	}
}

// WithPromFactory sets the prometheus factory to a lightning task.
func WithPromFactory(f promutil.Factory) Option {
	return func(o *options) {
		o.promFactory = f
	}
}

// WithPromRegistry sets the prometheus registry to a lightning task.
// The task metrics will be registered to the registry before the task starts
// and unregistered after the task ends.
func WithPromRegistry(r promutil.Registry) Option {
	return func(o *options) {
		o.promRegistry = r
	}
}

// WithLogger sets the logger to a lightning task.
func WithLogger(logger *zap.Logger) Option {
	return func(o *options) {
		o.logger = log.Logger{Logger: logger}
	}
}

// WithDupIndicator sets a *bool to indicate duplicate detection has found duplicate data.
func WithDupIndicator(b *atomic.Bool) Option {
	return func(o *options) {
		o.dupIndicator = b
	}
}
