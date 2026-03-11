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

package taskkey

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/keyspace"
)

// ForJob returns the task key for an import-into job.
func ForJob(jobID int64) string {
	if kerneltype.IsNextGen() {
		return forJobInKeyspace(keyspace.GetKeyspaceNameBySettings(), jobID)
	}
	return fmt.Sprintf("%s/%d", proto.ImportInto, jobID)
}

// ForJobInKeyspace returns the task key for an import-into job in a keyspace.
// In classic mode, keyspace is ignored because task keys are not keyspace-scoped.
func ForJobInKeyspace(keyspaceName string, jobID int64) string {
	if kerneltype.IsNextGen() {
		return forJobInKeyspace(keyspaceName, jobID)
	}
	return ForJob(jobID)
}

func forJobInKeyspace(keyspaceName string, jobID int64) string {
	return fmt.Sprintf("%s/%s/%d", keyspaceName, proto.ImportInto, jobID)
}
