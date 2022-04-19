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

package lightning

import (
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type options struct {
	glue              glue.Glue
	dumpFileStorage   storage.ExternalStorage
	checkpointStorage storage.ExternalStorage
	checkpointName    string
}

type Option func(*options)

// WithGlue sets the glue to a lightning task.
// Typically, the glue is set when lightning is integrated with a TiDB.
func WithGlue(g glue.Glue) Option {
	return func(o *options) {
		o.glue = g
	}
}

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
