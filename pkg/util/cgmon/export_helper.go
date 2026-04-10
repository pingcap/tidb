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

package cgmon

// ExportedGetCgroupCPUPeriodAndQuota is an exported wrapper for getCgroupCPUPeriodAndQuota to be used in tests.
func ExportedGetCgroupCPUPeriodAndQuota() (period int64, quota int64, err error) {
	return getCgroupCPUPeriodAndQuota()
}

// ExportedSetCgroupCPUPeriodAndQuota sets the getCgroupCPUPeriodAndQuota function for testing.
func ExportedSetCgroupCPUPeriodAndQuota(f func() (int64, int64, error)) {
	getCgroupCPUPeriodAndQuota = f
}

// ExportedGetCgroupMemoryLimit is an exported wrapper for getCgroupMemoryLimit to be used in tests.
func ExportedGetCgroupMemoryLimit() (uint64, error) {
	return getCgroupMemoryLimit()
}

// ExportedSetCgroupMemoryLimit sets the getCgroupMemoryLimit function for testing.
func ExportedSetCgroupMemoryLimit(f func() (uint64, error)) {
	getCgroupMemoryLimit = f
}

// ExportedRefreshCgroupCPU is an exported wrapper for refreshCgroupCPU to be used in tests.
func ExportedRefreshCgroupCPU() error {
	return refreshCgroupCPU()
}

// ExportedRefreshCgroupMemory is an exported wrapper for refreshCgroupMemory to be used in tests.
func ExportedRefreshCgroupMemory() error {
	return refreshCgroupMemory()
}

// ExportedLastCPU is an exported pointer to lastCPU variable to be used in tests.
var ExportedLastCPU = &lastCPU

// ExportedLastMemoryLimit is an exported pointer to lastMemoryLimit variable to be used in tests.
var ExportedLastMemoryLimit = &lastMemoryLimit
