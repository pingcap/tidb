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

package gcworker

import (
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/tablecodec"
)

func spfreshBootstrapCleanupPlacementIDs(store kv.Storage, liveTableID int64, task util.DelRangeTask) ([]int64, error) {
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return nil, err
	}
	record, err := meta.ReadSPFreshBootstrapJob(store.GetSnapshot(version), task.JobID)
	if err != nil {
		return nil, err
	}
	if record == nil || !record.Finished || record.Succeeded {
		return nil, errors.New("truncate GC requires a failed SPFresh bootstrap intent")
	}
	start, end := task.Range()
	for _, target := range record.Targets {
		prefix := tablecodec.GenTablePrefix(target.TableID)
		if target.TableID != liveTableID && bytes.Equal(start, prefix) && bytes.Equal(end, prefix.PrefixNext()) {
			return []int64{target.TableID}, nil
		}
	}
	return nil, errors.New("truncate GC range does not match an abandoned replacement table")
}
