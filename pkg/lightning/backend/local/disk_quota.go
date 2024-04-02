// Copyright 2023 PingCAP, Inc.
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

package local

import (
	"cmp"
	"slices"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/lightning/backend"
)

// DiskUsage is an interface to obtain the size occupied locally of all engines
type DiskUsage interface {
	// EngineFileSizes obtains the size occupied locally of all engines managed
	// by this backend. This method is used to compute disk quota.
	// It can return nil if the content are all stored remotely.
	EngineFileSizes() (res []backend.EngineFileSize)
}

// CheckDiskQuota verifies if the total engine file size is below the given
// quota. If the quota is exceeded, this method returns an array of engines,
// which after importing can decrease the total size below quota.
func CheckDiskQuota(mgr DiskUsage, quota int64) (
	largeEngines []uuid.UUID,
	inProgressLargeEngines int,
	totalDiskSize int64,
	totalMemSize int64,
) {
	sizes := mgr.EngineFileSizes()
	slices.SortFunc(sizes, func(i, j backend.EngineFileSize) int {
		if i.IsImporting != j.IsImporting {
			if i.IsImporting {
				return -1
			}
			return 1
		}
		return cmp.Compare(i.DiskSize+i.MemSize, j.DiskSize+j.MemSize)
	})
	for _, size := range sizes {
		totalDiskSize += size.DiskSize
		totalMemSize += size.MemSize
		if totalDiskSize+totalMemSize > quota {
			if size.IsImporting {
				inProgressLargeEngines++
			} else {
				largeEngines = append(largeEngines, size.UUID)
			}
		}
	}
	return
}
