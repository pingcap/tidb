// Copyright 2024 PingCAP, Inc.
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

package repository

import (
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
)

const (
	ownerKey  = "/tidb/repository/owner"
	promptKey = "repository"
	snapIDKey = "/tidb/repository/snap_id"

	etcdOpTimeout = 5 * time.Second

	// WorkloadSchema is the name of database for repository worker.
	WorkloadSchema     = "WORKLOAD_SCHEMA"
	histSnapshotsTable = "HIST_SNAPSHOTS"
)

var (
	workloadSchemaCIStr = model.NewCIStr(WorkloadSchema)
	zeroTime            = time.Time{}
)
