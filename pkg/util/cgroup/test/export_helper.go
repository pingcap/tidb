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

package cgroup_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/cgroup"
)

// RunCgroupsGetMemoryUsage is a test wrapper for cgroup.GetMemoryUsage
func RunCgroupsGetMemoryUsage(t *testing.T) {
	_, err := cgroup.GetMemoryUsage()
	if err != nil {
		t.Logf("GetMemoryUsage failed: %v", err)
	}
}

// RunCgroupsGetMemoryInactiveFileUsage is a test wrapper for cgroup.GetMemoryInactiveFileUsage
func RunCgroupsGetMemoryInactiveFileUsage(t *testing.T) {
	_, err := cgroup.GetMemoryInactiveFileUsage()
	if err != nil {
		t.Logf("GetMemoryInactiveFileUsage failed: %v", err)
	}
}

// RunCgroupsGetMemoryLimit is a test wrapper for cgroup.GetMemoryLimit
func RunCgroupsGetMemoryLimit(t *testing.T) {
	_, err := cgroup.GetMemoryLimit()
	if err != nil {
		t.Logf("GetMemoryLimit failed: %v", err)
	}
}

// RunCgroupsGetCPU is a test wrapper for cgroup.GetCPUPeriodAndQuota
func RunCgroupsGetCPU(t *testing.T) {
	_, _, err := cgroup.GetCPUPeriodAndQuota()
	if err != nil {
		t.Logf("GetCPUPeriodAndQuota failed: %v", err)
	}
}
