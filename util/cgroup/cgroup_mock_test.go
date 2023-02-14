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

package cgroup

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func isError(err error, re string) bool {
	if err == nil && re == "" {
		return true
	}
	if err == nil || re == "" {
		return false
	}
	matched, merr := regexp.MatchString(re, err.Error())
	if merr != nil {
		return false
	}
	return matched
}

func TestCgroupsGetMemoryUsage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		paths  map[string]string
		errMsg string
		value  uint64
		warn   string
	}{
		{
			errMsg: "failed to read memory cgroup from cgroups file:",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithoutMemoryController,
				"/proc/self/mountinfo": v1MountsWithoutMemController,
			},
			warn:  "no cgroup memory controller detected",
			value: 0,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup": v1CgroupWithMemoryController,
			},
			errMsg: "failed to read mounts info from file:",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithMemoryController,
				"/proc/self/mountinfo": v1MountsWithoutMemController,
			},
			errMsg: "failed to detect cgroup root mount and version",
			value:  0,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":                           v1CgroupWithMemoryController,
				"/proc/self/mountinfo":                        v1MountsWithMemController,
				"/sys/fs/cgroup/memory/memory.usage_in_bytes": v1MemoryUsageInBytes,
			},
			value: 276328448,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithMemoryControllerNS,
				"/proc/self/mountinfo": v1MountsWithMemControllerNS,
				"/sys/fs/cgroup/memory/cgroup_test/memory.usage_in_bytes": v1MemoryUsageInBytes,
			},
			value: 276328448,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
			},
			errMsg: "can't read memory.current from cgroup v2",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/memory.current": "unparsable\n",
			},
			errMsg: "failed to parse value in memory.current from cgroup v2",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/memory.current": "276328448",
			},
			value: 276328448,
		},
	} {
		dir := createFiles(t, tc.paths)

		limit, err := getCgroupMemUsage(dir)
		require.True(t, isError(err, tc.errMsg),
			"%v %v", err, tc.errMsg)
		require.Equal(t, tc.value, limit)
	}
}

func TestCgroupsGetMemoryInactiveFileUsage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		paths  map[string]string
		errMsg string
		value  uint64
		warn   string
	}{
		{
			errMsg: "failed to read memory cgroup from cgroups file:",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithoutMemoryController,
				"/proc/self/mountinfo": v1MountsWithoutMemController,
			},
			warn:  "no cgroup memory controller detected",
			value: 0,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup": v1CgroupWithMemoryController,
			},
			errMsg: "failed to read mounts info from file:",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithMemoryController,
				"/proc/self/mountinfo": v1MountsWithoutMemController,
			},
			errMsg: "failed to detect cgroup root mount and version",
			value:  0,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":                 v1CgroupWithMemoryController,
				"/proc/self/mountinfo":              v1MountsWithMemController,
				"/sys/fs/cgroup/memory/memory.stat": v1MemoryStat,
			},
			value: 1363746816,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":                             v1CgroupWithMemoryControllerNS,
				"/proc/self/mountinfo":                          v1MountsWithMemControllerNS,
				"/sys/fs/cgroup/memory/cgroup_test/memory.stat": v1MemoryStat,
			},
			value: 1363746816,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
			},
			errMsg: "can't read file memory.stat from cgroup v2",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/memory.stat": "inactive_file unparsable\n",
			},
			errMsg: "can't read \"inactive_file\" memory stat from cgroup v2 in memory.stat",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/memory.stat": v2MemoryStat,
			},
			value: 1363746816,
		},
	} {
		dir := createFiles(t, tc.paths)
		limit, err := getCgroupMemInactiveFileUsage(dir)
		require.True(t, isError(err, tc.errMsg),
			"%v %v", err, tc.errMsg)
		require.Equal(t, tc.value, limit)
	}
}

func TestCgroupsGetMemoryLimit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		paths  map[string]string
		errMsg string
		limit  uint64
		warn   string
	}{
		{

			errMsg: "failed to read memory cgroup from cgroups file:",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithoutMemoryController,
				"/proc/self/mountinfo": v1MountsWithoutMemController,
			},
			limit: 0,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup": v1CgroupWithMemoryController,
			},
			errMsg: "failed to read mounts info from file:",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithMemoryController,
				"/proc/self/mountinfo": v1MountsWithoutMemController,
			},
			errMsg: "failed to detect cgroup root mount and version",
			limit:  0,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":                 v1CgroupWithMemoryController,
				"/proc/self/mountinfo":              v1MountsWithMemController,
				"/sys/fs/cgroup/memory/memory.stat": v1MemoryStat,
			},
			limit: 2936016896,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":                             v1CgroupWithMemoryControllerNS,
				"/proc/self/mountinfo":                          v1MountsWithMemControllerNS,
				"/sys/fs/cgroup/memory/cgroup_test/memory.stat": v1MemoryStat,
			},
			limit: 2936016896,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
			},
			errMsg: "can't read memory.max from cgroup v2",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/memory.max": "unparsable\n",
			},
			errMsg: "failed to parse value in memory.max from cgroup v2",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/memory.max": "1073741824\n",
			},
			limit: 1073741824,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/memory.max": "max\n",
			},
			limit: 9223372036854775807,
		},
	} {
		dir := createFiles(t, tc.paths)
		limit, err := getCgroupMemLimit(dir)
		require.True(t, isError(err, tc.errMsg),
			"%v %v", err, tc.errMsg)
		require.Equal(t, tc.limit, limit)
	}
}

func TestCgroupsGetCPU(t *testing.T) {
	for _, tc := range []struct {
		name   string
		paths  map[string]string
		errMsg string
		period int64
		quota  int64
		user   uint64
		system uint64
	}{
		{
			errMsg: "failed to read cpu,cpuacct cgroup from cgroups file:",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithoutCPUController,
				"/proc/self/mountinfo": v1MountsWithoutCPUController,
			},
			errMsg: "no cpu controller detected",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup": v1CgroupWithCPUController,
			},
			errMsg: "failed to read mounts info from file:",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithCPUController,
				"/proc/self/mountinfo": v1MountsWithoutCPUController,
			},
			errMsg: "failed to detect cgroup root mount and version",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":                             v1CgroupWithCPUController,
				"/proc/self/mountinfo":                          v1MountsWithCPUController,
				"/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us":   "12345",
				"/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us":  "67890",
				"/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage_sys":  "123",
				"/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage_user": "456",
			},
			quota:  int64(12345),
			period: int64(67890),
			system: uint64(123),
			user:   uint64(456),
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithCPUControllerNS,
				"/proc/self/mountinfo": v1MountsWithCPUControllerNS,
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_quota_us":   "12345",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_period_us":  "67890",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpuacct.usage_sys":  "123",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpuacct.usage_user": "456",
			},
			quota:  int64(12345),
			period: int64(67890),
			system: uint64(123),
			user:   uint64(456),
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithCPUControllerNSMountRel,
				"/proc/self/mountinfo": v1MountsWithCPUControllerNSMountRel,
			},
			errMsg: "failed to detect cgroup root mount and version",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithCPUControllerNSMountRelRemount,
				"/proc/self/mountinfo": v1MountsWithCPUControllerNSMountRelRemount,
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_quota_us":   "12345",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_period_us":  "67890",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpuacct.usage_sys":  "123",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpuacct.usage_user": "456",
			},
			quota:  int64(12345),
			period: int64(67890),
			system: uint64(123),
			user:   uint64(456),
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithCPUControllerNS2,
				"/proc/self/mountinfo": v1MountsWithCPUControllerNS2,
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_quota_us":   "12345",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_period_us":  "67890",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpuacct.usage_sys":  "123",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpuacct.usage_user": "456",
			},
			quota:  int64(12345),
			period: int64(67890),
			system: uint64(123),
			user:   uint64(456),
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":                            v1CgroupWithCPUController,
				"/proc/self/mountinfo":                         v1MountsWithCPUController,
				"/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us":  "-1",
				"/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us": "67890",
			},
			quota:  int64(-1),
			period: int64(67890),
			errMsg: "error when reading cpu system time from cgroup v1",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
			},
			errMsg: "error when read cpu quota from cgroup v2",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/cpu.max": "foo bar\n",
			},
			errMsg: "error when reading cpu quota from cgroup v2 at",
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/cpu.max":  "100 1000\n",
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/cpu.stat": "user_usec 100\nsystem_usec 200",
			},
			quota:  int64(100),
			period: int64(1000),
			user:   uint64(100),
			system: uint64(200),
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/cpu.max":  "max 1000\n",
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/cpu.stat": "user_usec 100\nsystem_usec 200",
			},
			quota:  int64(-1),
			period: int64(1000),
			user:   uint64(100),
			system: uint64(200),
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/cpu.max": "100 1000\n",
			},
			quota:  int64(100),
			period: int64(1000),
			errMsg: "can't read cpu usage from cgroup v2",
		},
	} {
		dir := createFiles(t, tc.paths)

		cpuusage, err := getCgroupCPU(dir)
		require.True(t, isError(err, tc.errMsg),
			"%v %v", err, tc.errMsg)
		require.Equal(t, tc.quota, cpuusage.Quota)
		require.Equal(t, tc.period, cpuusage.Period)
		require.Equal(t, tc.system, cpuusage.Stime)
		require.Equal(t, tc.user, cpuusage.Utime)
	}
}

func createFiles(t *testing.T, paths map[string]string) (dir string) {
	dir = t.TempDir()

	for path, data := range paths {
		path = filepath.Join(dir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(data), 0755))
	}
	return dir
}

const (
	v1CgroupWithMemoryController = `11:blkio:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
10:devices:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
9:perf_event:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
8:cpu,cpuacct:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
7:pids:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
6:cpuset:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
5:memory:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
4:net_cls,net_prio:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
3:hugetlb:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
2:freezer:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
1:name=systemd:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
`
	v1CgroupWithoutMemoryController = `10:blkio:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
9:devices:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
8:perf_event:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
7:cpu,cpuacct:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
6:pids:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
5:cpuset:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
4:net_cls,net_prio:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
3:hugetlb:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
2:freezer:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
1:name=systemd:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
`
	v1CgroupWithCPUController = `11:blkio:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
10:devices:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
9:perf_event:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
8:cpu,cpuacct:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
7:pids:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
6:cpuset:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
5:memory:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
4:net_cls,net_prio:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
3:hugetlb:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
2:freezer:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
1:name=systemd:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
`
	v1CgroupWithoutCPUController = `10:blkio:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
9:devices:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
8:perf_event:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
7:pids:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
6:cpuset:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
5:memory:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
4:net_cls,net_prio:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
3:hugetlb:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
2:freezer:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
1:name=systemd:/kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
`
	v2CgroupWithMemoryController = `0::/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope
`

	v1MountsWithMemController = `625 367 0:71 / / rw,relatime master:85 - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/DOLSFLPSKANL4GJ7XKF3OG6PKN:/var/lib/docker/overlay2/l/P7UJPLDFEUSRQ7CZILB7L4T5OP:/var/lib/docker/overlay2/l/FSKO5FFFNQ6XOSVF7T6R2DWZVZ:/var/lib/docker/overlay2/l/YNE4EZZE2GW2DIXRBUP47LB3GU:/var/lib/docker/overlay2/l/F2JNS7YWT5CU7FUXHNV5JUJWQY,upperdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/diff,workdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/work
626 625 0:79 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
687 625 0:75 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
691 687 0:82 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666
702 625 0:159 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro
703 702 0:99 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,mode=755
705 703 0:23 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/systemd ro,nosuid,nodev,noexec,relatime master:9 - cgroup cgroup rw,xattr,release_agent=/usr/lib/systemd/systemd-cgroups-agent,name=systemd
711 703 0:25 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/freezer ro,nosuid,nodev,noexec,relatime master:10 - cgroup cgroup rw,freezer
726 703 0:26 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/hugetlb ro,nosuid,nodev,noexec,relatime master:11 - cgroup cgroup rw,hugetlb
727 703 0:27 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/net_cls,net_prio ro,nosuid,nodev,noexec,relatime master:12 - cgroup cgroup rw,net_cls,net_prio
733 703 0:28 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime master:13 - cgroup cgroup rw,memory
734 703 0:29 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/cpuset ro,nosuid,nodev,noexec,relatime master:14 - cgroup cgroup rw,cpuset
735 703 0:30 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/pids ro,nosuid,nodev,noexec,relatime master:15 - cgroup cgroup rw,pids
736 703 0:31 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/cpu,cpuacct ro,nosuid,nodev,noexec,relatime master:16 - cgroup cgroup rw,cpu,cpuacct
737 703 0:32 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/perf_event ro,nosuid,nodev,noexec,relatime master:17 - cgroup cgroup rw,perf_event
740 703 0:33 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/devices ro,nosuid,nodev,noexec,relatime master:18 - cgroup cgroup rw,devices
742 703 0:34 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/blkio ro,nosuid,nodev,noexec,relatime master:19 - cgroup cgroup rw,blkio
744 687 0:78 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw
746 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/volumes/kubernetes.io~empty-dir/cockroach-env /etc/cockroach-env ro,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
760 687 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/containers/cockroachdb/3e868c1f /dev/termination-log rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
776 625 259:3 / /cockroach/cockroach-data rw,relatime - ext4 /dev/nvme2n1 rw,data=ordered
814 625 0:68 / /cockroach/cockroach-certs ro,relatime - tmpfs tmpfs rw
815 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/resolv.conf /etc/resolv.conf rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
816 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/hostname /etc/hostname rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
817 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/etc-hosts /etc/hosts rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
818 687 0:77 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=65536k
819 625 0:69 / /run/secrets/kubernetes.io/serviceaccount ro,relatime - tmpfs tmpfs rw
368 626 0:79 /bus /proc/bus ro,relatime - proc proc rw
375 626 0:79 /fs /proc/fs ro,relatime - proc proc rw
376 626 0:79 /irq /proc/irq ro,relatime - proc proc rw
381 626 0:79 /sys /proc/sys ro,relatime - proc proc rw
397 626 0:79 /sysrq-trigger /proc/sysrq-trigger ro,relatime - proc proc rw
213 626 0:70 / /proc/acpi ro,relatime - tmpfs tmpfs ro
216 626 0:75 /null /proc/kcore rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
217 626 0:75 /null /proc/keys rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
218 626 0:75 /null /proc/latency_stats rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
222 626 0:75 /null /proc/timer_list rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
223 626 0:75 /null /proc/sched_debug rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
224 702 0:101 / /sys/firmware ro,relatime - tmpfs tmpfs ro
`
	v1MountsWithoutMemController = `625 367 0:71 / / rw,relatime master:85 - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/DOLSFLPSKANL4GJ7XKF3OG6PKN:/var/lib/docker/overlay2/l/P7UJPLDFEUSRQ7CZILB7L4T5OP:/var/lib/docker/overlay2/l/FSKO5FFFNQ6XOSVF7T6R2DWZVZ:/var/lib/docker/overlay2/l/YNE4EZZE2GW2DIXRBUP47LB3GU:/var/lib/docker/overlay2/l/F2JNS7YWT5CU7FUXHNV5JUJWQY,upperdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/diff,workdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/work
626 625 0:79 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
687 625 0:75 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
691 687 0:82 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666
702 625 0:159 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro
703 702 0:99 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,mode=755
705 703 0:23 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/systemd ro,nosuid,nodev,noexec,relatime master:9 - cgroup cgroup rw,xattr,release_agent=/usr/lib/systemd/systemd-cgroups-agent,name=systemd
711 703 0:25 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/freezer ro,nosuid,nodev,noexec,relatime master:10 - cgroup cgroup rw,freezer
726 703 0:26 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/hugetlb ro,nosuid,nodev,noexec,relatime master:11 - cgroup cgroup rw,hugetlb
727 703 0:27 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/net_cls,net_prio ro,nosuid,nodev,noexec,relatime master:12 - cgroup cgroup rw,net_cls,net_prio
734 703 0:29 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/cpuset ro,nosuid,nodev,noexec,relatime master:14 - cgroup cgroup rw,cpuset
735 703 0:30 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/pids ro,nosuid,nodev,noexec,relatime master:15 - cgroup cgroup rw,pids
736 703 0:31 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/cpu,cpuacct ro,nosuid,nodev,noexec,relatime master:16 - cgroup cgroup rw,cpu,cpuacct
737 703 0:32 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/perf_event ro,nosuid,nodev,noexec,relatime master:17 - cgroup cgroup rw,perf_event
740 703 0:33 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/devices ro,nosuid,nodev,noexec,relatime master:18 - cgroup cgroup rw,devices
742 703 0:34 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/blkio ro,nosuid,nodev,noexec,relatime master:19 - cgroup cgroup rw,blkio
744 687 0:78 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw
746 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/volumes/kubernetes.io~empty-dir/cockroach-env /etc/cockroach-env ro,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
760 687 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/containers/cockroachdb/3e868c1f /dev/termination-log rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
776 625 259:3 / /cockroach/cockroach-data rw,relatime - ext4 /dev/nvme2n1 rw,data=ordered
814 625 0:68 / /cockroach/cockroach-certs ro,relatime - tmpfs tmpfs rw
815 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/resolv.conf /etc/resolv.conf rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
816 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/hostname /etc/hostname rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
817 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/etc-hosts /etc/hosts rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
818 687 0:77 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=65536k
819 625 0:69 / /run/secrets/kubernetes.io/serviceaccount ro,relatime - tmpfs tmpfs rw
368 626 0:79 /bus /proc/bus ro,relatime - proc proc rw
375 626 0:79 /fs /proc/fs ro,relatime - proc proc rw
376 626 0:79 /irq /proc/irq ro,relatime - proc proc rw
381 626 0:79 /sys /proc/sys ro,relatime - proc proc rw
397 626 0:79 /sysrq-trigger /proc/sysrq-trigger ro,relatime - proc proc rw
213 626 0:70 / /proc/acpi ro,relatime - tmpfs tmpfs ro
216 626 0:75 /null /proc/kcore rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
217 626 0:75 /null /proc/keys rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
218 626 0:75 /null /proc/latency_stats rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
222 626 0:75 /null /proc/timer_list rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
223 626 0:75 /null /proc/sched_debug rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
224 702 0:101 / /sys/firmware ro,relatime - tmpfs tmpfs ro
`
	v1MountsWithCPUController = `625 367 0:71 / / rw,relatime master:85 - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/DOLSFLPSKANL4GJ7XKF3OG6PKN:/var/lib/docker/overlay2/l/P7UJPLDFEUSRQ7CZILB7L4T5OP:/var/lib/docker/overlay2/l/FSKO5FFFNQ6XOSVF7T6R2DWZVZ:/var/lib/docker/overlay2/l/YNE4EZZE2GW2DIXRBUP47LB3GU:/var/lib/docker/overlay2/l/F2JNS7YWT5CU7FUXHNV5JUJWQY,upperdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/diff,workdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/work
626 625 0:79 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
687 625 0:75 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
691 687 0:82 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666
702 625 0:159 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro
703 702 0:99 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,mode=755
705 703 0:23 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/systemd ro,nosuid,nodev,noexec,relatime master:9 - cgroup cgroup rw,xattr,release_agent=/usr/lib/systemd/systemd-cgroups-agent,name=systemd
711 703 0:25 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/freezer ro,nosuid,nodev,noexec,relatime master:10 - cgroup cgroup rw,freezer
726 703 0:26 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/hugetlb ro,nosuid,nodev,noexec,relatime master:11 - cgroup cgroup rw,hugetlb
727 703 0:27 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/net_cls,net_prio ro,nosuid,nodev,noexec,relatime master:12 - cgroup cgroup rw,net_cls,net_prio
733 703 0:28 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime master:13 - cgroup cgroup rw,memory
734 703 0:29 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/cpuset ro,nosuid,nodev,noexec,relatime master:14 - cgroup cgroup rw,cpuset
735 703 0:30 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/pids ro,nosuid,nodev,noexec,relatime master:15 - cgroup cgroup rw,pids
736 703 0:31 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/cpu,cpuacct ro,nosuid,nodev,noexec,relatime master:16 - cgroup cgroup rw,cpu,cpuacct
737 703 0:32 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/perf_event ro,nosuid,nodev,noexec,relatime master:17 - cgroup cgroup rw,perf_event
740 703 0:33 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/devices ro,nosuid,nodev,noexec,relatime master:18 - cgroup cgroup rw,devices
742 703 0:34 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/blkio ro,nosuid,nodev,noexec,relatime master:19 - cgroup cgroup rw,blkio
744 687 0:78 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw
746 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/volumes/kubernetes.io~empty-dir/cockroach-env /etc/cockroach-env ro,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
760 687 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/containers/cockroachdb/3e868c1f /dev/termination-log rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
776 625 259:3 / /cockroach/cockroach-data rw,relatime - ext4 /dev/nvme2n1 rw,data=ordered
814 625 0:68 / /cockroach/cockroach-certs ro,relatime - tmpfs tmpfs rw
815 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/resolv.conf /etc/resolv.conf rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
816 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/hostname /etc/hostname rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
817 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/etc-hosts /etc/hosts rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
818 687 0:77 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=65536k
819 625 0:69 / /run/secrets/kubernetes.io/serviceaccount ro,relatime - tmpfs tmpfs rw
368 626 0:79 /bus /proc/bus ro,relatime - proc proc rw
375 626 0:79 /fs /proc/fs ro,relatime - proc proc rw
376 626 0:79 /irq /proc/irq ro,relatime - proc proc rw
381 626 0:79 /sys /proc/sys ro,relatime - proc proc rw
397 626 0:79 /sysrq-trigger /proc/sysrq-trigger ro,relatime - proc proc rw
213 626 0:70 / /proc/acpi ro,relatime - tmpfs tmpfs ro
216 626 0:75 /null /proc/kcore rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
217 626 0:75 /null /proc/keys rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
218 626 0:75 /null /proc/latency_stats rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
222 626 0:75 /null /proc/timer_list rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
223 626 0:75 /null /proc/sched_debug rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
224 702 0:101 / /sys/firmware ro,relatime - tmpfs tmpfs ro
`
	v1MountsWithoutCPUController = `625 367 0:71 / / rw,relatime master:85 - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/DOLSFLPSKANL4GJ7XKF3OG6PKN:/var/lib/docker/overlay2/l/P7UJPLDFEUSRQ7CZILB7L4T5OP:/var/lib/docker/overlay2/l/FSKO5FFFNQ6XOSVF7T6R2DWZVZ:/var/lib/docker/overlay2/l/YNE4EZZE2GW2DIXRBUP47LB3GU:/var/lib/docker/overlay2/l/F2JNS7YWT5CU7FUXHNV5JUJWQY,upperdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/diff,workdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/work
626 625 0:79 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
687 625 0:75 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
691 687 0:82 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666
702 625 0:159 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro
703 702 0:99 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,mode=755
705 703 0:23 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40scha6577eedze81g7227xa518dbffd51fdf7f624e3 /sys/fs/cgroup/systemd ro,nosuid,nodev,noexec,relatime master:9 - cgroup cgroup rw,xattr,release_agent=/usr/lib/systemd/systemd-cgroups-agent,name=systemd
711 703 0:25 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40scha6577eedze81g7227xa518dbffd51fdf7f624e3 /sys/fs/cgroup/freezer ro,nosuid,nodev,noexec,relatime master:10 - cgroup cgroup rw,freezer
726 703 0:26 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40scha6577eedze81g7227xa518dbffd51fdf7f624e3 /sys/fs/cgroup/hugetlb ro,nosuid,nodev,noexec,relatime master:11 - cgroup cgroup rw,hugetlb
727 703 0:27 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40scha6577eedze81g7227xa518dbffd51fdf7f624e3 /sys/fs/cgroup/net_cls,net_prio ro,nosuid,nodev,noexec,relatime master:12 - cgroup cgroup rw,net_cls,net_prio
734 703 0:29 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40scha6577eedze81g7227xa518dbffd51fdf7f624e3 /sys/fs/cgroup/cpuset ro,nosuid,nodev,noexec,relatime master:14 - cgroup cgroup rw,cpuset
735 703 0:30 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40scha6577eedze81g7227xa518dbffd51fdf7f624e3 /sys/fs/cgroup/pids ro,nosuid,nodev,noexec,relatime master:15 - cgroup cgroup rw,pids
737 703 0:32 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40scha6577eedze81g7227xa518dbffd51fdf7f624e3 /sys/fs/cgroup/perf_event ro,nosuid,nodev,noexec,relatime master:17 - cgroup cgroup rw,perf_event
740 703 0:33 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40scha6577eedze81g7227xa518dbffd51fdf7f624e3 /sys/fs/cgroup/devices ro,nosuid,nodev,noexec,relatime master:18 - cgroup cgroup rw,devices
742 703 0:34 /kubepods/besteffort/podcbfx2j5d-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40scha6577eedze81g7227xa518dbffd51fdf7f624e3 /sys/fs/cgroup/blkio ro,nosuid,nodev,noexec,relatime master:19 - cgroup cgroup rw,blkio
744 687 0:78 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw
746 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/volumes/kubernetes.io~empty-dir/cockroach-env /etc/cockroach-env ro,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
760 687 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/containers/cockroachdb/3e868c1f /dev/termination-log rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
776 625 259:3 / /cockroach/cockroach-data rw,relatime - ext4 /dev/nvme2n1 rw,data=ordered
814 625 0:68 / /cockroach/cockroach-certs ro,relatime - tmpfs tmpfs rw
815 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/resolv.conf /etc/resolv.conf rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
816 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/hostname /etc/hostname rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
817 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/etc-hosts /etc/hosts rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
818 687 0:77 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=65536k
819 625 0:69 / /run/secrets/kubernetes.io/serviceaccount ro,relatime - tmpfs tmpfs rw
368 626 0:79 /bus /proc/bus ro,relatime - proc proc rw
375 626 0:79 /fs /proc/fs ro,relatime - proc proc rw
376 626 0:79 /irq /proc/irq ro,relatime - proc proc rw
381 626 0:79 /sys /proc/sys ro,relatime - proc proc rw
397 626 0:79 /sysrq-trigger /proc/sysrq-trigger ro,relatime - proc proc rw
213 626 0:70 / /proc/acpi ro,relatime - tmpfs tmpfs ro
216 626 0:75 /null /proc/kcore rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
217 626 0:75 /null /proc/keys rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
218 626 0:75 /null /proc/latency_stats rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
222 626 0:75 /null /proc/timer_list rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
223 626 0:75 /null /proc/sched_debug rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
224 702 0:101 / /sys/firmware ro,relatime - tmpfs tmpfs ro
`

	v2Mounts = `371 344 0:35 / / rw,relatime - overlay overlay rw,context="system_u:object_r:container_file_t:s0:c200,c321",lowerdir=/var/lib/containers/storage/overlay/l/SPNDOAU3AZNJMNKU3F5THCA36R,upperdir=/var/lib/containers/storage/overlay/7dcd88f815bded7b833fb5dc0f25de897250bcfa828624c0d78393689d0bc312/diff,workdir=/var/lib/containers/storage/overlay/7dcd88f815bded7b833fb5dc0f25de897250bcfa828624c0d78393689d0bc312/work
372 371 0:37 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
373 371 0:38 / /dev rw,nosuid - tmpfs tmpfs rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=65536k,mode=755
374 371 0:39 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs rw,seclabel
375 373 0:40 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,context="system_u:object_r:container_file_t:s0:c200,c321",gid=5,mode=620,ptmxmode=666
376 373 0:36 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw,seclabel
377 371 0:24 /containers/storage/overlay-containers/f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810/userdata/hostname /etc/hostname rw,nosuid,nodev - tmpfs tmpfs rw,seclabel,mode=755
378 371 0:24 /containers/storage/overlay-containers/f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810/userdata/.containerenv /run/.containerenv rw,nosuid,nodev - tmpfs tmpfs rw,seclabel,mode=755
379 371 0:24 /containers/storage/overlay-containers/f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810/userdata/run/secrets /run/secrets rw,nosuid,nodev - tmpfs tmpfs rw,seclabel,mode=755
380 371 0:24 /containers/storage/overlay-containers/f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810/userdata/resolv.conf /etc/resolv.conf rw,nosuid,nodev - tmpfs tmpfs rw,seclabel,mode=755
381 371 0:24 /containers/storage/overlay-containers/f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810/userdata/hosts /etc/hosts rw,nosuid,nodev - tmpfs tmpfs rw,seclabel,mode=755
382 373 0:33 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=64000k
383 374 0:25 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw,seclabel
384 372 0:41 / /proc/acpi ro,relatime - tmpfs tmpfs rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=0k
385 372 0:6 /null /proc/kcore rw,nosuid - devtmpfs devtmpfs rw,seclabel,size=1869464k,nr_inodes=467366,mode=755
386 372 0:6 /null /proc/keys rw,nosuid - devtmpfs devtmpfs rw,seclabel,size=1869464k,nr_inodes=467366,mode=755
387 372 0:6 /null /proc/timer_list rw,nosuid - devtmpfs devtmpfs rw,seclabel,size=1869464k,nr_inodes=467366,mode=755
388 372 0:6 /null /proc/sched_debug rw,nosuid - devtmpfs devtmpfs rw,seclabel,size=1869464k,nr_inodes=467366,mode=755
389 372 0:42 / /proc/scsi ro,relatime - tmpfs tmpfs rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=0k
390 374 0:43 / /sys/firmware ro,relatime - tmpfs tmpfs rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=0k
391 374 0:44 / /sys/fs/selinux ro,relatime - tmpfs tmpfs rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=0k
392 372 0:37 /bus /proc/bus ro,relatime - proc proc rw
393 372 0:37 /fs /proc/fs ro,relatime - proc proc rw
394 372 0:37 /irq /proc/irq ro,relatime - proc proc rw
395 372 0:37 /sys /proc/sys ro,relatime - proc proc rw
396 372 0:37 /sysrq-trigger /proc/sysrq-trigger ro,relatime - proc proc rw
345 373 0:40 /0 /dev/console rw,nosuid,noexec,relatime - devpts devpts rw,context="system_u:object_r:container_file_t:s0:c200,c321",gid=5,mode=620,ptmxmode=666
`
	v1MemoryStat = `cache 784113664
rss 1703952384
rss_huge 27262976
shmem 0
mapped_file 14520320
dirty 4096
writeback 0
swap 0
pgpgin 35979039
pgpgout 35447229
pgfault 24002539
pgmajfault 3871
inactive_anon 0
active_anon 815435776
inactive_file 1363746816
active_file 308867072
unevictable 0
hierarchical_memory_limit 2936016896
hierarchical_memsw_limit 9223372036854771712
total_cache 784113664
total_rss 1703952384
total_rss_huge 27262976
total_shmem 0
total_mapped_file 14520320
total_dirty 4096
total_writeback 0
total_swap 0
total_pgpgin 35979039
total_pgpgout 35447229
total_pgfault 24002539
total_pgmajfault 3871
total_inactive_anon 0
total_active_anon 815435776
total_inactive_file 1363746816
total_active_file 308867072
total_unevictable 0
`

	v2MemoryStat = `anon 784113664
file 1703952384
kernel_stack 27262976
pagetables 0
percpu 14520320
sock 4096
shmem 0
file_mapped 0
file_dirty 35979039
file_writeback 35447229
swapcached 24002539
anon_thp 3871
file_thp 0
shmem_thp 815435776
inactive_anon 1363746816
active_anon 308867072
inactive_file 1363746816
active_file 2936016896
unevictable 9223372036854771712
slab_reclaimable 784113664
slab_unreclaimable 1703952384
slab 27262976
workingset_refault_anon 0
workingset_refault_file 14520320
workingset_activate_anon 4096
workingset_activate_file 0
workingset_restore_anon 0
workingset_restore_file 35979039
workingset_nodereclaim 35447229
pgfault 24002539
pgmajfault 3871
pgrefill 0
pgscan 815435776
pgsteal 1363746816
pgactivate 308867072
pgdeactivate 0
pglazyfree 0
pglazyfreed 0
thp_fault_alloc 0
thp_collapse_alloc 0
`
	v1MemoryUsageInBytes = "276328448"

	// Both /proc/<pid>/mountinfo and /proc/<pid>/cgroup will show the mount and the cgroup relative to the cgroup NS root
	// This tests the case where the memory controller mount and the cgroup are not exactly the same (as is with k8s pods).
	v1CgroupWithMemoryControllerNS = "12:memory:/cgroup_test"
	v1MountsWithMemControllerNS    = "50 35 0:44 / /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime shared:25 - cgroup cgroup rw,memory"

	// Example where the paths in /proc/self/mountinfo and /proc/self/cgroup are not the same for the cpu controller
	//
	// sudo cgcreate -t $USER:$USER -a $USER:$USER -g cpu:crdb_test
	// echo 100000 > /sys/fs/cgroup/cpu/crdb_test/cpu.cfs_period_us
	// echo 33300 > /sys/fs/cgroup/cpu/crdb_test/cpu.cfs_quota_us
	// cgexec -g cpu:crdb_test ./cockroach ...
	v1CgroupWithCPUControllerNS = "5:cpu,cpuacct:/crdb_test"
	v1MountsWithCPUControllerNS = "43 35 0:37 / /sys/fs/cgroup/cpu,cpuacct rw,nosuid,nodev,noexec,relatime shared:18 - cgroup cgroup rw,cpu,cpuacct"

	// Same as above but with unshare -C
	// Can't determine the location of the mount
	v1CgroupWithCPUControllerNSMountRel = "5:cpu,cpuacct:/"
	v1MountsWithCPUControllerNSMountRel = "43 35 0:37 /.. /sys/fs/cgroup/cpu,cpuacct rw,nosuid,nodev,noexec,relatime shared:18 - cgroup cgroup rw,cpu,cpuacct"

	// Same as above but with mounting the cgroup fs one more time in the NS
	// sudo mount -t cgroup -o cpu,cpuacct none /sys/fs/cgroup/cpu,cpuacct/crdb_test
	v1CgroupWithCPUControllerNSMountRelRemount = "5:cpu,cpuacct:/"
	v1MountsWithCPUControllerNSMountRelRemount = `
43 35 0:37 /.. /sys/fs/cgroup/cpu,cpuacct rw,nosuid,nodev,noexec,relatime shared:18 - cgroup cgroup rw,cpu,cpuacct
161 43 0:37 / /sys/fs/cgroup/cpu,cpuacct/crdb_test rw,relatime shared:95 - cgroup none rw,cpu,cpuacct
`
	// Same as above but exiting the NS w/o unmounting
	v1CgroupWithCPUControllerNS2 = "5:cpu,cpuacct:/crdb_test"
	v1MountsWithCPUControllerNS2 = "161 43 0:37 /crdb_test /sys/fs/cgroup/cpu,cpuacct/crdb_test rw,relatime shared:95 - cgroup none rw,cpu,cpuacct"
)
