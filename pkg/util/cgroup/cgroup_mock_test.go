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
	"strings"
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
				"/proc/self/cgroup":                 v1CgroupWithEccentricMemoryController,
				"/proc/self/mountinfo":              v1MountsWithEccentricMemController,
				"/sys/fs/cgroup/memory/memory.stat": v1MemoryStat,
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
		name    string
		paths   map[string]string
		errMsg  string
		limit   uint64
		warn    string
		version Version
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
			limit:   2936016896,
			version: V1,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":                             v1CgroupWithMemoryControllerNS,
				"/proc/self/mountinfo":                          v1MountsWithMemControllerNS,
				"/sys/fs/cgroup/memory/cgroup_test/memory.stat": v1MemoryStat,
			},
			limit:   2936016896,
			version: V1,
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
			limit:   1073741824,
			version: V2,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/memory.max": "max\n",
			},
			limit:   9223372036854775807,
			version: V2,
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":                 v1CgroupWithEccentricMemoryController,
				"/proc/self/mountinfo":              v1MountsWithEccentricMemController,
				"/sys/fs/cgroup/memory/memory.stat": v1MemoryStat,
			},
			limit:   2936016896,
			version: V1,
		},
	} {
		dir := createFiles(t, tc.paths)
		limit, version, err := getCgroupMemLimit(dir)
		require.True(t, isError(err, tc.errMsg),
			"%v %v", err, tc.errMsg)
		require.Equal(t, tc.limit, limit)
		if err == nil {
			require.Equal(t, tc.version, version)
		}
	}
}

const (
	v1CgroupWithEccentricMemoryController = `
13:devices:/system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2
12:freezer:/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2
11:cpu,cpuacct:/system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2
10:perf_event:/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2
9:rdma:/
8:pids:/system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2
7:blkio:/system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2
6:hugetlb:/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2
5:memory:/system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2
4:cpuset:/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2
3:files:/
2:net_cls,net_prio:/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2
1:name=systemd:/system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2
0::/
`
	v1MountsWithEccentricMemController = `
1421 1021 0:133 / / rw,relatime master:412 - overlay overlay rw,lowerdir=/apps/data/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1285288/fs:/apps/data/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1285287/fs:/apps/data/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1285286/fs:/apps/data/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1285285/fs:/apps/data/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1283928/fs,upperdir=/apps/data/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1287880/fs,workdir=/apps/data/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1287880/work
1442 1421 0:136 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
1443 1421 0:137 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
1444 1443 0:138 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666
2303 1443 0:119 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw
2304 1421 0:129 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro
2305 2304 0:139 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,mode=755
2306 2305 0:25 /system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2 /sys/fs/cgroup/systemd ro,nosuid,nodev,noexec,relatime master:5 - cgroup cgroup rw,xattr,release_agent=/usr/lib/systemd/systemd-cgroups-agent,name=systemd
2307 2305 0:28 /kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2 /sys/fs/cgroup/net_cls,net_prio ro,nosuid,nodev,noexec,relatime master:6 - cgroup cgroup rw,net_cls,net_prio
2308 2305 0:29 / /sys/fs/cgroup/files ro,nosuid,nodev,noexec,relatime master:7 - cgroup cgroup rw,files
2309 2305 0:30 /kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2 /sys/fs/cgroup/cpuset ro,nosuid,nodev,noexec,relatime master:8 - cgroup cgroup rw,cpuset
2310 2305 0:31 /system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2 /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime master:9 - cgroup cgroup rw,memory
2311 2305 0:32 /kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2 /sys/fs/cgroup/hugetlb ro,nosuid,nodev,noexec,relatime master:10 - cgroup cgroup rw,hugetlb
2312 2305 0:33 /system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2 /sys/fs/cgroup/blkio ro,nosuid,nodev,noexec,relatime master:11 - cgroup cgroup rw,blkio
2313 2305 0:34 /system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2 /sys/fs/cgroup/pids ro,nosuid,nodev,noexec,relatime master:12 - cgroup cgroup rw,pids
2314 2305 0:35 / /sys/fs/cgroup/rdma ro,nosuid,nodev,noexec,relatime master:13 - cgroup cgroup rw,rdma
2315 2305 0:36 /kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2 /sys/fs/cgroup/perf_event ro,nosuid,nodev,noexec,relatime master:14 - cgroup cgroup rw,perf_event
2316 2305 0:37 /system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2 /sys/fs/cgroup/cpu,cpuacct ro,nosuid,nodev,noexec,relatime master:15 - cgroup cgroup rw,cpu,cpuacct
2317 2305 0:38 /kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2 /sys/fs/cgroup/freezer ro,nosuid,nodev,noexec,relatime master:16 - cgroup cgroup rw,freezer
2318 2305 0:39 /system.slice/containerd.service/kubepods-burstable-pod94598a35_ad1e_4a00_91b1_1db37e8f52f6.slice:cri-containerd:0ac322a00cf64a4d58144a1974b993d91537f3ceec12928b10d881af6be8bbb2 /sys/fs/cgroup/devices ro,nosuid,nodev,noexec,relatime master:17 - cgroup cgroup rw,devices
2319 1421 0:101 / /etc/podinfo ro,relatime - tmpfs tmpfs rw
2320 1421 253:3 /data/containerd/io.containerd.grpc.v1.cri/sandboxes/22c18c845c47667097eb8973fd0ec05256be685cd1b1a8b0fe7c748a04401cdb/hostname /etc/hostname rw,relatime - xfs /dev/mapper/vg1-lvm1k8s rw,attr2,inode64,sunit=512,swidth=512,noquota
2321 1421 253:3 /data/kubelet/pods/94598a35-ad1e-4a00-91b1-1db37e8f52f6/volumes/kubernetes.io~configmap/config /etc/tikv ro,relatime - xfs /dev/mapper/vg1-lvm1k8s rw,attr2,inode64,sunit=512,swidth=512,noquota
2322 1443 0:104 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=65536k
2323 1421 253:3 /data/kubelet/pods/94598a35-ad1e-4a00-91b1-1db37e8f52f6/etc-hosts /etc/hosts rw,relatime - xfs /dev/mapper/vg1-lvm1k8s rw,attr2,inode64,sunit=512,swidth=512,noquota
2324 1443 253:3 /data/kubelet/pods/94598a35-ad1e-4a00-91b1-1db37e8f52f6/containers/tikv/0981845c /dev/termination-log rw,relatime - xfs /dev/mapper/vg1-lvm1k8s rw,attr2,inode64,sunit=512,swidth=512,noquota
2325 1421 253:3 /data/containerd/io.containerd.grpc.v1.cri/sandboxes/22c18c845c47667097eb8973fd0ec05256be685cd1b1a8b0fe7c748a04401cdb/resolv.conf /etc/resolv.conf rw,relatime - xfs /dev/mapper/vg1-lvm1k8s rw,attr2,inode64,sunit=512,swidth=512,noquota
2326 1421 253:2 /pv03 /var/lib/tikv rw,relatime - xfs /dev/mapper/vg2-lvm2k8s rw,attr2,inode64,sunit=512,swidth=512,noquota
2327 1421 253:3 /data/kubelet/pods/94598a35-ad1e-4a00-91b1-1db37e8f52f6/volumes/kubernetes.io~configmap/startup-script /usr/local/bin ro,relatime - xfs /dev/mapper/vg1-lvm1k8s rw,attr2,inode64,sunit=512,swidth=512,noquota
2328 1421 0:102 / /run/secrets/kubernetes.io/serviceaccount ro,relatime - tmpfs tmpfs rw
1022 1442 0:136 /bus /proc/bus ro,nosuid,nodev,noexec,relatime - proc proc rw
1034 1442 0:136 /fs /proc/fs ro,nosuid,nodev,noexec,relatime - proc proc rw
1035 1442 0:136 /irq /proc/irq ro,nosuid,nodev,noexec,relatime - proc proc rw
1036 1442 0:136 /sys /proc/sys ro,nosuid,nodev,noexec,relatime - proc proc rw
1037 1442 0:136 /sysrq-trigger /proc/sysrq-trigger ro,nosuid,nodev,noexec,relatime - proc proc rw
1038 1442 0:161 / /proc/acpi ro,relatime - tmpfs tmpfs ro
1039 1442 0:137 /null /proc/kcore rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
1040 1442 0:137 /null /proc/keys rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
1041 1442 0:137 /null /proc/timer_list rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
1042 1442 0:137 /null /proc/sched_debug rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
1043 1442 0:162 / /proc/scsi ro,relatime - tmpfs tmpfs ro
1044 2304 0:163 / /sys/firmware ro,relatime - tmpfs tmpfs ro
`
)

func TestCgroupsGetCPU(t *testing.T) {
	for i := 0; i < 2; i++ {
		if i == 1 {
			// The field in /proc/self/cgroup and /proc/self/mountinfo may appear as "cpuacct,cpu" or "rw,cpuacct,cpu"
			// while the input controller is "cpu,cpuacct"
			v1CgroupWithCPUController = strings.ReplaceAll(v1CgroupWithCPUController, "cpu,cpuacct", "cpuacct,cpu")
			v1CgroupWithCPUControllerNS = strings.ReplaceAll(v1CgroupWithCPUControllerNS, "cpu,cpuacct", "cpuacct,cpu")
			v1CgroupWithCPUControllerNSMountRel = strings.ReplaceAll(v1CgroupWithCPUControllerNSMountRel, "cpu,cpuacct", "cpuacct,cpu")
			v1CgroupWithCPUControllerNSMountRelRemount = strings.ReplaceAll(v1CgroupWithCPUControllerNSMountRelRemount, "cpu,cpuacct", "cpuacct,cpu")
			v1CgroupWithCPUControllerNS2 = strings.ReplaceAll(v1CgroupWithCPUControllerNS2, "cpu,cpuacct", "cpuacct,cpu")

			v1MountsWithCPUController = strings.ReplaceAll(v1MountsWithCPUController, "rw,cpu,cpuacct", "rw,cpuacct,cpu")
			v1MountsWithCPUControllerNS = strings.ReplaceAll(v1MountsWithCPUControllerNS, "rw,cpu,cpuacct", "rw,cpuacct,cpu")
			v1MountsWithCPUControllerNSMountRel = strings.ReplaceAll(v1MountsWithCPUControllerNSMountRel, "rw,cpu,cpuacct", "rw,cpuacct,cpu")
			v1MountsWithCPUControllerNSMountRelRemount = strings.ReplaceAll(v1MountsWithCPUControllerNSMountRelRemount, "rw,cpu,cpuacct", "rw,cpuacct,cpu")
			v1MountsWithCPUControllerNS2 = strings.ReplaceAll(v1MountsWithCPUControllerNS2, "rw,cpu,cpuacct", "rw,cpuacct,cpu")
		}
		testCgroupsGetCPU(t)
		testCgroupsGetCPUPeriodAndQuota(t)
	}
}

func testCgroupsGetCPU(t *testing.T) {
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
		{
			paths: map[string]string{
				"/proc/self/cgroup":    MixCgroup,
				"/proc/self/mountinfo": MixMounts,
				"/sys/fs/cgroup/cpu,cpuacct/user.slice/cpu.cfs_quota_us":   "12345",
				"/sys/fs/cgroup/cpu,cpuacct/user.slice/cpu.cfs_period_us":  "67890",
				"/sys/fs/cgroup/cpu,cpuacct/user.slice/cpuacct.usage_sys":  "123",
				"/sys/fs/cgroup/cpu,cpuacct/user.slice/cpuacct.usage_user": "456",
			},
			quota:  int64(12345),
			period: int64(67890),
			system: uint64(123),
			user:   uint64(456),
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

func testCgroupsGetCPUPeriodAndQuota(t *testing.T) {
	for _, tc := range []struct {
		name   string
		paths  map[string]string
		errMsg string
		period int64
		quota  int64
	}{
		{
			errMsg: "failed to read cpu cgroup from cgroups file:",
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
				"/proc/self/cgroup":                            v1CgroupWithCPUController,
				"/proc/self/mountinfo":                         v1MountsWithCPUController,
				"/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us":  "12345",
				"/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us": "67890",
			},
			quota:  int64(12345),
			period: int64(67890),
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithCPUControllerNS,
				"/proc/self/mountinfo": v1MountsWithCPUControllerNS,
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_quota_us":  "12345",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_period_us": "67890",
			},
			quota:  int64(12345),
			period: int64(67890),
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
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_quota_us":  "12345",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_period_us": "67890",
			},
			quota:  int64(12345),
			period: int64(67890),
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithCPUControllerNS2,
				"/proc/self/mountinfo": v1MountsWithCPUControllerNS2,
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_quota_us":  "12345",
				"/sys/fs/cgroup/cpu,cpuacct/crdb_test/cpu.cfs_period_us": "67890",
			},
			quota:  int64(12345),
			period: int64(67890),
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
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/cpu.max": "100 1000\n",
			},
			quota:  int64(100),
			period: int64(1000),
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/cpu.max": "max 1000\n",
			},
			quota:  int64(-1),
			period: int64(1000),
		},
		{
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/cpu.max": "100 1000\n",
			},
			quota:  int64(100),
			period: int64(1000),
		},
	} {
		dir := createFiles(t, tc.paths)

		period, quota, err := getCgroupCPUPeriodAndQuota(dir)
		require.True(t, isError(err, tc.errMsg),
			"%v %v", err, tc.errMsg)
		require.Equal(t, tc.quota, quota)
		require.Equal(t, tc.period, period)
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

var (
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

	MixCgroup = `12:hugetlb:/
11:memory:/user.slice/user-1006.slice/session-17838.scope
10:pids:/user.slice/user-1006.slice/session-17838.scope
9:devices:/user.slice
8:perf_event:/
7:cpu,cpuacct:/user.slice
6:blkio:/user.slice
5:cpuset:/
4:net_cls,net_prio:/
3:freezer:/
2:rdma:/
1:name=systemd:/user.slice/user-1006.slice/session-17838.scope
0::/user.slice/user-1006.slice/session-17838.scope
`

	MixMounts = `
25 30 0:23 / /sys rw,relatime shared:7 - sysfs sysfs rw
26 30 0:5 / /proc rw,relatime shared:14 - proc proc rw
27 30 0:6 / /dev rw,nosuid,noexec,relatime shared:2 - devtmpfs udev rw,size=197385544k,nr_inodes=49346386,mode=755
28 27 0:24 / /dev/pts rw,relatime shared:3 - devpts devpts rw,gid=5,mode=620,ptmxmode=000
29 30 0:25 / /run rw,nosuid,nodev,noexec,relatime shared:5 - tmpfs tmpfs rw,size=39486148k,mode=755
30 1 8:3 / / rw,relatime shared:1 - ext4 /dev/sda3 rw,stripe=16
31 25 0:7 / /sys/kernel/security rw,nosuid,nodev,noexec,relatime shared:8 - securityfs securityfs rw
32 27 0:26 / /dev/shm rw shared:4 - tmpfs tmpfs rw
33 29 0:27 / /run/lock rw,nosuid,nodev,noexec,relatime shared:6 - tmpfs tmpfs rw,size=5120k
34 25 0:28 / /sys/fs/cgroup ro,nosuid,nodev,noexec shared:9 - tmpfs tmpfs ro,mode=755
35 34 0:29 / /sys/fs/cgroup/unified rw,nosuid,nodev,noexec,relatime shared:10 - cgroup2 cgroup2 rw
36 34 0:30 / /sys/fs/cgroup/systemd rw,nosuid,nodev,noexec,relatime shared:11 - cgroup cgroup rw,xattr,name=systemd
37 25 0:31 / /sys/fs/pstore rw,nosuid,nodev,noexec,relatime shared:12 - pstore pstore rw
39 34 0:33 / /sys/fs/cgroup/rdma rw,nosuid,nodev,noexec,relatime shared:15 - cgroup cgroup rw,rdma
40 34 0:34 / /sys/fs/cgroup/freezer rw,nosuid,nodev,noexec,relatime shared:16 - cgroup cgroup rw,freezer
41 34 0:35 / /sys/fs/cgroup/net_cls,net_prio rw,nosuid,nodev,noexec,relatime shared:17 - cgroup cgroup rw,net_cls,net_prio
42 34 0:36 / /sys/fs/cgroup/cpuset rw,nosuid,nodev,noexec,relatime shared:18 - cgroup cgroup rw,cpuset
43 34 0:37 / /sys/fs/cgroup/blkio rw,nosuid,nodev,noexec,relatime shared:19 - cgroup cgroup rw,blkio
44 34 0:38 / /sys/fs/cgroup/cpu,cpuacct rw,nosuid,nodev,noexec,relatime shared:20 - cgroup cgroup rw,cpu,cpuacct
45 34 0:39 / /sys/fs/cgroup/perf_event rw,nosuid,nodev,noexec,relatime shared:21 - cgroup cgroup rw,perf_event
46 34 0:40 / /sys/fs/cgroup/devices rw,nosuid,nodev,noexec,relatime shared:22 - cgroup cgroup rw,devices
47 34 0:41 / /sys/fs/cgroup/pids rw,nosuid,nodev,noexec,relatime shared:23 - cgroup cgroup rw,pids
48 34 0:42 / /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime shared:24 - cgroup cgroup rw,memory
49 34 0:43 / /sys/fs/cgroup/hugetlb rw,nosuid,nodev,noexec,relatime shared:25 - cgroup cgroup rw,hugetlb
50 26 0:44 / /proc/sys/fs/binfmt_misc rw,relatime shared:26 - autofs systemd-1 rw,fd=28,pgrp=1,timeout=0,minproto=5,maxproto=5,direct,pipe_ino=91621
51 27 0:45 / /dev/hugepages rw,relatime shared:27 - hugetlbfs hugetlbfs rw,pagesize=2M
52 27 0:21 / /dev/mqueue rw,nosuid,nodev,noexec,relatime shared:28 - mqueue mqueue rw
53 25 0:8 / /sys/kernel/debug rw,nosuid,nodev,noexec,relatime shared:29 - debugfs debugfs rw
54 25 0:12 / /sys/kernel/tracing rw,nosuid,nodev,noexec,relatime shared:30 - tracefs tracefs rw
55 25 0:46 / /sys/fs/fuse/connections rw,nosuid,nodev,noexec,relatime shared:31 - fusectl fusectl rw
56 25 0:22 / /sys/kernel/config rw,nosuid,nodev,noexec,relatime shared:32 - configfs configfs rw
142 30 8:2 / /boot rw,relatime shared:79 - ext4 /dev/sda2 rw,stripe=16,data=ordered
145 30 259:1 / /data/nvme0n1 rw,relatime shared:81 - ext4 /dev/nvme0n1 rw
605 29 0:25 /snapd/ns /run/snapd/ns rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,size=39486148k,mode=755
624 29 0:49 / /run/user/0 rw,nosuid,nodev,relatime shared:341 - tmpfs tmpfs rw,size=39486144k,mode=700
642 30 259:3 / /mnt/c42ca499-9a7c-4d19-ae60-e8a46a6956ba rw,relatime shared:348 - ext4 /dev/nvme2n1 rw
798 30 259:2 / /mnt/a688878a-492b-4536-a03c-f50ce8a1f014 rw,relatime shared:386 - ext4 /dev/nvme3n1 rw
887 30 259:0 / /mnt/f97f162d-be90-4bfa-bae5-2698f5ce634d rw,relatime shared:424 - ext4 /dev/nvme1n1 rw
976 29 0:53 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/2191a8cf52bd8313c6abef93260d6964f6b7240117f7cc0723c9647caa78bb45/shm rw,nosuid,nodev,noexec,relatime shared:462 - tmpfs shm rw,size=65536k
993 29 0:54 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/2191a8cf52bd8313c6abef93260d6964f6b7240117f7cc0723c9647caa78bb45/rootfs rw,relatime shared:469 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/8260/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/8260/work,xino=off
1113 145 0:80 / /data/nvme0n1/kubelet/pods/428eee2f-da5c-44de-aae1-951b3746e3d8/volumes/kubernetes.io~secret/clustermesh-secrets rw,relatime shared:497 - tmpfs tmpfs rw
1185 145 0:89 / /data/nvme0n1/kubelet/pods/07d67bd3-f23b-4d2c-84ae-03a2df2f42a6/volumes/kubernetes.io~projected/kube-api-access-6c2d6 rw,relatime shared:518 - tmpfs tmpfs rw
1223 29 0:90 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/8231ae5d138d3e9da5a996bbc833175580fe7c201dfe66398df2cc0285e1bdfe/shm rw,nosuid,nodev,noexec,relatime shared:525 - tmpfs shm rw,size=65536k
1257 29 0:92 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/8231ae5d138d3e9da5a996bbc833175580fe7c201dfe66398df2cc0285e1bdfe/rootfs rw,relatime shared:539 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/8264/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/8264/work,xino=off
1521 145 0:112 / /data/nvme0n1/kubelet/pods/428eee2f-da5c-44de-aae1-951b3746e3d8/volumes/kubernetes.io~projected/kube-api-access-6kcqx rw,relatime shared:567 - tmpfs tmpfs rw
1519 29 0:121 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/11cbc306aeead0190cb68c6329bafaf979353cd193e8ecb47e61b693cfd2b0f5/shm rw,nosuid,nodev,noexec,relatime shared:574 - tmpfs shm rw,size=65536k
1556 29 0:122 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/11cbc306aeead0190cb68c6329bafaf979353cd193e8ecb47e61b693cfd2b0f5/rootfs rw,relatime shared:581 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/8268/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/8268/work,xino=off
5312 50 0:803 / /proc/sys/fs/binfmt_misc rw,nosuid,nodev,noexec,relatime shared:1817 - binfmt_misc binfmt_misc rw
2312 30 7:2 / /snap/lxd/23991 ro,nodev,relatime shared:742 - squashfs /dev/loop2 ro
458 605 0:4 mnt:[4026537320] /run/snapd/ns/lxd.mnt rw - nsfs nsfs rw
755 30 7:4 / /snap/lxd/24061 ro,nodev,relatime shared:511 - squashfs /dev/loop4 ro
1027 29 0:63 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/a802a3a6b73af45d20aa3d7a07b6c280b618b91320b4378812a2552e413935c2/rootfs rw,relatime shared:476 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/13/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/12/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/11/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/10/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/6/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/4/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/26137/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/26137/work,xino=off
4698 29 0:110 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/8b6bac7cc0fd04bc2d1baf50f8f2fee0f81f36a13bf06f4b4ff195586a87588f/rootfs rw,relatime shared:2321 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/15/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/9/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/26141/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/26141/work,xino=off
10124 29 0:176 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/ed885e6b247e68336830e4120db20d7f43c7a996a82fa86a196e48743a52eff4/rootfs rw,relatime shared:2417 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/28/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/27/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/26/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/25/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/24/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/23/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/22/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/26143/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/26143/work,xino=off
3432 30 7:3 / /snap/snapd/18357 ro,nodev,relatime shared:1001 - squashfs /dev/loop3 ro
2768 30 7:11 / /snap/core/14784 ro,nodev,relatime shared:938 - squashfs /dev/loop11 ro
2265 30 7:12 / /snap/k9s/151 ro,nodev,relatime shared:725 - squashfs /dev/loop12 ro
2297 30 7:13 / /snap/core20/1828 ro,nodev,relatime shared:765 - squashfs /dev/loop13 ro
1411 30 7:14 / /snap/go/10073 ro,nodev,relatime shared:550 - squashfs /dev/loop14 ro
115 145 0:103 / /data/nvme0n1/kubelet/pods/bb3ece51-01a1-4d9d-a48a-43093c72a3a2/volumes/kubernetes.io~projected/kube-api-access-5nvm9 rw,relatime shared:442 - tmpfs tmpfs rw
1454 29 0:108 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/282a3e51b05ed2b168285995fce71e9d882db9d4cb33e54a367791fe92fd8cd2/shm rw,nosuid,nodev,noexec,relatime shared:740 - tmpfs shm rw,size=65536k
1516 29 0:109 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/282a3e51b05ed2b168285995fce71e9d882db9d4cb33e54a367791fe92fd8cd2/rootfs rw,relatime shared:788 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/26610/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/26610/work,xino=off
2409 29 0:209 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/29d6387bdeed4df9882df5f73645c071317999c6f913a739d42390507485e8c5/rootfs rw,relatime shared:869 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/9/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/26611/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/26611/work,xino=off
2270 30 7:15 / /snap/core18/2714 ro,nodev,relatime shared:1068 - squashfs /dev/loop15 ro
137 30 0:71 / /var/lib/docker/overlay2/41ea64be1d943b66e7cce1d07ca48f1c6359dd7e983ffc4100b122289d4fc457/merged rw,relatime shared:49 - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/MZDFUOLZV7OH5FLPS5ZWNO6JHC:/var/lib/docker/overlay2/l/SFPFSYXYX4E3ST5E7Y5BTG3RAG:/var/lib/docker/overlay2/l/NVHRNLY3A7KDIXEPAEZZUAKKAF:/var/lib/docker/overlay2/l/3NWZQJULTIAEMU33EBOV3DO4KK:/var/lib/docker/overlay2/l/24BFJIPXS2PJ4XI7A4OB4FCK3N,upperdir=/var/lib/docker/overlay2/41ea64be1d943b66e7cce1d07ca48f1c6359dd7e983ffc4100b122289d4fc457/diff,workdir=/var/lib/docker/overlay2/41ea64be1d943b66e7cce1d07ca48f1c6359dd7e983ffc4100b122289d4fc457/work,xino=off
138 30 0:72 / /var/lib/docker/overlay2/6ba83a6794d649bd38fb9698e067ce8fb22e6a976af0cd9d003b86847b54dde7/merged rw,relatime shared:73 - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/DAWHHXAM2AYTI37UJOH6W453EZ:/var/lib/docker/overlay2/l/SFPFSYXYX4E3ST5E7Y5BTG3RAG:/var/lib/docker/overlay2/l/NVHRNLY3A7KDIXEPAEZZUAKKAF:/var/lib/docker/overlay2/l/3NWZQJULTIAEMU33EBOV3DO4KK:/var/lib/docker/overlay2/l/24BFJIPXS2PJ4XI7A4OB4FCK3N,upperdir=/var/lib/docker/overlay2/6ba83a6794d649bd38fb9698e067ce8fb22e6a976af0cd9d003b86847b54dde7/diff,workdir=/var/lib/docker/overlay2/6ba83a6794d649bd38fb9698e067ce8fb22e6a976af0cd9d003b86847b54dde7/work,xino=off
4039 29 0:4 net:[4026537384] /run/docker/netns/c8477f57c25f rw shared:123 - nsfs nsfs rw
4059 29 0:4 net:[4026537817] /run/docker/netns/64d7952bb68f rw shared:833 - nsfs nsfs rw
665 30 259:3 /vol1 /mnt/disks/c42ca499-9a7c-4d19-ae60-e8a46a6956ba_vol1 rw,relatime shared:348 - ext4 /dev/nvme2n1 rw
750 30 259:2 /vol1 /mnt/disks/a688878a-492b-4536-a03c-f50ce8a1f014_vol1 rw,relatime shared:386 - ext4 /dev/nvme3n1 rw
779 30 259:0 /vol1 /mnt/disks/f97f162d-be90-4bfa-bae5-2698f5ce634d_vol1 rw,relatime shared:424 - ext4 /dev/nvme1n1 rw
38 25 0:256 / /sys/fs/bpf rw,relatime shared:13 - bpf none rw
3174 30 7:16 / /snap/core/14946 ro,nodev,relatime shared:846 - squashfs /dev/loop16 ro
965 30 7:17 / /snap/core20/1852 ro,nodev,relatime shared:436 - squashfs /dev/loop17 ro
3663 30 7:5 / /snap/snapd/18596 ro,nodev,relatime shared:1170 - squashfs /dev/loop5 ro
2275 30 7:8 / /snap/core22/583 ro,nodev,relatime shared:449 - squashfs /dev/loop8 ro
4856 30 7:9 / /snap/core18/2721 ro,nodev,relatime shared:1229 - squashfs /dev/loop9 ro
1225 29 0:487 / /run/user/1003 rw,nosuid,nodev,relatime shared:987 - tmpfs tmpfs rw,size=39486144k,mode=700,uid=1003,gid=1003
311 605 0:4 mnt:[4026537731] /run/snapd/ns/k9s.mnt rw - nsfs nsfs rw
80 29 0:32 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/c6189676909828b8d0cbce711e115fa3037e30122a4e359bfca0fc4f628d91da/shm rw,nosuid,nodev,noexec,relatime shared:55 - tmpfs shm rw,size=65536k
100 29 0:50 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/c6189676909828b8d0cbce711e115fa3037e30122a4e359bfca0fc4f628d91da/rootfs rw,relatime shared:62 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30973/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30973/work,xino=off
497 29 0:91 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/19a62c19cdd472887f8b1510143d8a79151f70f24bef25a649b8c063a7fe0dff/rootfs rw,relatime shared:75 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30976/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30975/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30974/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30977/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30977/work,xino=off
643 145 0:113 / /data/nvme0n1/kubelet/pods/8c096a91-03cb-4948-b0b1-10bf212928e8/volumes/kubernetes.io~secret/kubevirt-virt-handler-certs rw,relatime shared:171 - tmpfs tmpfs rw
696 145 0:115 / /data/nvme0n1/kubelet/pods/8c096a91-03cb-4948-b0b1-10bf212928e8/volumes/kubernetes.io~secret/kubevirt-virt-handler-server-certs rw,relatime shared:223 - tmpfs tmpfs rw
699 145 0:116 / /data/nvme0n1/kubelet/pods/8c096a91-03cb-4948-b0b1-10bf212928e8/volumes/kubernetes.io~downward-api/podinfo rw,relatime shared:272 - tmpfs tmpfs rw
810 145 0:119 / /data/nvme0n1/kubelet/pods/8c096a91-03cb-4948-b0b1-10bf212928e8/volumes/kubernetes.io~projected/kube-api-access-b4ctw rw,relatime shared:323 - tmpfs tmpfs rw
848 29 0:4 net:[4026537303] /run/netns/cni-86b41b5e-f846-34a4-3d24-4bc6f7a2b70d rw shared:360 - nsfs nsfs rw
1191 29 0:133 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/e5326f2fb12efbadde2acc5095415191214862b730fb4abbe4acc4f0a982dfc0/shm rw,nosuid,nodev,noexec,relatime shared:382 - tmpfs shm rw,size=65536k
1226 29 0:134 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/e5326f2fb12efbadde2acc5095415191214862b730fb4abbe4acc4f0a982dfc0/rootfs rw,relatime shared:396 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30978/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30978/work,xino=off
3252 29 0:350 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/0525c449b405367e89061da50eb3cd480692aa0c3f47e98944fd61a6d9d686e1/rootfs rw,relatime shared:1057 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30998/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30989/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30999/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30999/work,xino=off
4090 145 0:422 / /data/nvme0n1/kubelet/pods/c7676e13-5326-4a20-9823-1709d2ec124f/volumes/kubernetes.io~secret/chaos-daemon-cert rw,relatime shared:1120 - tmpfs tmpfs rw
4149 145 0:423 / /data/nvme0n1/kubelet/pods/c7676e13-5326-4a20-9823-1709d2ec124f/volumes/kubernetes.io~projected/kube-api-access-jvl6g rw,relatime shared:1140 - tmpfs tmpfs rw
4166 29 0:4 net:[4026538463] /run/netns/cni-f4356533-4c6e-f374-942a-f8fb8aaef128 rw shared:1147 - nsfs nsfs rw
4218 29 0:424 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/38bf769b3260aac70efc295dbeeba2d9ae1e0de3df2347f1926e2344336ed61a/shm rw,nosuid,nodev,noexec,relatime shared:1154 - tmpfs shm rw,size=65536k
4238 29 0:425 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/38bf769b3260aac70efc295dbeeba2d9ae1e0de3df2347f1926e2344336ed61a/rootfs rw,relatime shared:1161 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31135/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31135/work,xino=off
3764 29 0:414 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/debe913e95afafbe487c2ff40032ef88fedab297dc35f01873a17eb4d3911137/rootfs rw,relatime shared:1099 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31149/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31148/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31147/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31146/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31145/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31144/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31143/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31142/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31141/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31140/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31139/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31138/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31137/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31136/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31150/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31150/work,xino=off
4325 145 0:447 / /data/nvme0n1/kubelet/pods/eaa8a805-56af-4054-a3d1-87d89b4427f7/volumes/kubernetes.io~projected/kube-api-access-kdkpp rw,relatime shared:1189 - tmpfs tmpfs rw
4354 29 0:4 net:[4026538607] /run/netns/cni-3767b6e7-dbc4-0ccd-d26c-b7a20f4db13a rw shared:1196 - nsfs nsfs rw
4374 29 0:448 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/ea5708a1a0574c874fa64e782929b1ce52a68561d6d4c57200dcaeae42a412a0/shm rw,nosuid,nodev,noexec,relatime shared:1203 - tmpfs shm rw,size=65536k
4402 29 0:449 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/ea5708a1a0574c874fa64e782929b1ce52a68561d6d4c57200dcaeae42a412a0/rootfs rw,relatime shared:1210 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31154/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31154/work,xino=off
4472 29 0:460 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/3f73b6ed13e1e285c13f20c568e353945cc9705dd7b027985c281a0f03c514b4/rootfs rw,relatime shared:1217 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31158/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31157/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31156/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31155/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31159/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31159/work,xino=off
4655 29 0:465 / /run/user/1006 rw,nosuid,nodev,relatime shared:1117 - tmpfs tmpfs rw,size=39486144k,mode=700,uid=1006,gid=1006
5412 30 7:6 / /snap/core22/607 ro,nodev,relatime shared:1364 - squashfs /dev/loop6 ro
4865 30 7:1 / /snap/hello-world/29 ro,nodev,relatime shared:1292 - squashfs /dev/loop1 ro
4982 30 7:18 / /snap/go/10135 ro,nodev,relatime shared:1299 - squashfs /dev/loop18 ro
1136 145 0:149 / /data/nvme0n1/kubelet/pods/61b41e1e-6caf-4f4c-aa97-b8ba1b38168e/volumes/kubernetes.io~projected/kube-api-access-r9chm rw,relatime shared:404 - tmpfs tmpfs rw
1357 29 0:4 net:[4026537584] /run/netns/cni-b6788c05-b9f7-d2a8-1cca-729821632d90 rw shared:451 - nsfs nsfs rw
1497 29 0:167 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/af946ebe99e17756f7b3d7a97fcb290dcae7cc6b9f816f2d6942f15afba7c28b/shm rw,nosuid,nodev,noexec,relatime shared:477 - tmpfs shm rw,size=65536k
1657 29 0:168 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/af946ebe99e17756f7b3d7a97fcb290dcae7cc6b9f816f2d6942f15afba7c28b/rootfs rw,relatime shared:510 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33975/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33975/work,xino=off
1679 145 0:180 / /data/nvme0n1/kubelet/pods/7830474c-474d-4ea7-b568-bc70f0c804b2/volumes/kubernetes.io~projected/kube-api-access-lsctl rw,relatime shared:547 - tmpfs tmpfs rw
1740 29 0:4 net:[4026537658] /run/netns/cni-85144543-ac5f-d5cb-e113-613e2360d33e rw shared:559 - nsfs nsfs rw
1765 29 0:181 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/ecd66ebdf9e85e447076ab5f010a9a721102b34878b61a090537ffe4735d5cc8/shm rw,nosuid,nodev,noexec,relatime shared:596 - tmpfs shm rw,size=65536k
1785 29 0:182 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/ecd66ebdf9e85e447076ab5f010a9a721102b34878b61a090537ffe4735d5cc8/rootfs rw,relatime shared:604 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33979/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33979/work,xino=off
1827 29 0:192 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/c78d2dc899501734043224ccae3b1844ca17bde418fa1a2e6b9230b3b8bb100c/rootfs rw,relatime shared:613 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33982/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33981/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33980/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33978/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33977/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33976/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33983/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33983/work,xino=off
1860 145 259:2 /vol1 /data/nvme0n1/kubelet/pods/a954a19c-b1eb-49f4-903a-edc75070cb01/volumes/kubernetes.io~local-volume/local-pv-2ed50413 rw,relatime shared:386 - ext4 /dev/nvme3n1 rw
2017 145 0:194 / /data/nvme0n1/kubelet/pods/a954a19c-b1eb-49f4-903a-edc75070cb01/volumes/kubernetes.io~projected/kube-api-access-pfpxr rw,relatime shared:628 - tmpfs tmpfs rw
1897 29 0:201 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/2d3a2171ef379ec2abb999beaf83e6a864352c1bfd12a3b7e02839842df996ee/rootfs rw,relatime shared:636 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33296/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33295/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33294/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31119/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33984/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33984/work,xino=off
1916 29 0:4 net:[4026537735] /run/netns/cni-10de9502-de86-307b-30b6-4038e734172f rw shared:644 - nsfs nsfs rw
1963 29 0:212 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/8883ff252cae98cf882bd79b528a009c36f3618a6e1e2f0de5e8df85c90dca7f/shm rw,nosuid,nodev,noexec,relatime shared:652 - tmpfs shm rw,size=65536k
1983 29 0:213 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/8883ff252cae98cf882bd79b528a009c36f3618a6e1e2f0de5e8df85c90dca7f/rootfs rw,relatime shared:660 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33985/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33985/work,xino=off
2001 145 0:226 / /data/nvme0n1/kubelet/pods/8faa8aef-6440-4932-8281-5350b429bbf5/volumes/kubernetes.io~downward-api/annotations rw,relatime shared:668 - tmpfs tmpfs rw
2087 145 0:227 / /data/nvme0n1/kubelet/pods/8faa8aef-6440-4932-8281-5350b429bbf5/volumes/kubernetes.io~projected/kube-api-access-6c682 rw,relatime shared:677 - tmpfs tmpfs rw
2116 29 0:4 net:[4026537808] /run/netns/cni-6c186bcd-915d-ae9f-6240-f005d7558f3c rw shared:685 - nsfs nsfs rw
2164 29 0:228 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/98d1dc3da1f767caa508aeb7a13ba91eb29f93e6afcc6d9374c477956a587c9c/shm rw,nosuid,nodev,noexec,relatime shared:694 - tmpfs shm rw,size=65536k
2184 29 0:229 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/98d1dc3da1f767caa508aeb7a13ba91eb29f93e6afcc6d9374c477956a587c9c/rootfs rw,relatime shared:703 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33991/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33991/work,xino=off
2205 145 0:240 / /data/nvme0n1/kubelet/pods/661d95b2-0ff0-4ae4-b154-2a8cbc436538/volumes/kubernetes.io~secret/tls-assets rw,relatime shared:710 - tmpfs tmpfs rw
2259 145 0:241 / /data/nvme0n1/kubelet/pods/661d95b2-0ff0-4ae4-b154-2a8cbc436538/volumes/kubernetes.io~projected/kube-api-access-c494f rw,relatime shared:717 - tmpfs tmpfs rw
2280 29 0:4 net:[4026537950] /run/netns/cni-0a5d3c52-1cc9-1d7b-2189-30d737f65e6c rw shared:724 - nsfs nsfs rw
2358 29 0:242 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/d39f5fd65ac2df941fba6dd4e7cffb25a612111082711e6afb44aa16171c86be/shm rw,nosuid,nodev,noexec,relatime shared:755 - tmpfs shm rw,size=65536k
2395 29 0:243 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/d39f5fd65ac2df941fba6dd4e7cffb25a612111082711e6afb44aa16171c86be/rootfs rw,relatime shared:766 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33992/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33992/work,xino=off
2442 29 0:265 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/9a265bdcf7c04c2d56cd82b5126f17a18dd34d557ffea27ba74f1006dcffd510/rootfs rw,relatime shared:775 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33997/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33996/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33995/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33994/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33993/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33990/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33998/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33998/work,xino=off
2476 29 0:266 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/f5bb1b65d7857de4c14b191dec87bb583ea42d0f7336ed9c622989446d52a7e3/rootfs rw,relatime shared:789 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/32889/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/32888/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31105/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33999/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33999/work,xino=off
2538 29 0:283 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/9491e655ded1c73a4d985437535e88469725b90ad8e26d64cd854eead7ff5aa3/rootfs rw,relatime shared:796 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31241/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31240/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31239/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31238/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31237/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31236/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31235/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31234/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31233/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31232/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30975/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/30974/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34006/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34006/work,xino=off
2576 29 0:291 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/cf0caadef274fa509bb964c214abc9329509c2372f39107d334a36312fd0a7e9/rootfs rw,relatime shared:804 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31245/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31244/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34007/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34007/work,xino=off
2634 29 0:299 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/e54b94b04ed316c75dea0c75e980aee377dc9d44f025017021c14dd147346c80/rootfs rw,relatime shared:811 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31258/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31257/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31256/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31255/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31254/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31253/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31252/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31251/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34008/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34008/work,xino=off
2655 145 0:307 / /data/nvme0n1/kubelet/pods/54dd0c3f-051c-48f7-840c-1842661c69a3/volumes/kubernetes.io~downward-api/annotations rw,relatime shared:820 - tmpfs tmpfs rw
2725 145 259:3 /vol1 /data/nvme0n1/kubelet/pods/54dd0c3f-051c-48f7-840c-1842661c69a3/volumes/kubernetes.io~local-volume/local-pv-2928e757 rw,relatime shared:348 - ext4 /dev/nvme2n1 rw
2747 145 0:308 / /data/nvme0n1/kubelet/pods/54dd0c3f-051c-48f7-840c-1842661c69a3/volumes/kubernetes.io~projected/kube-api-access-clj59 rw,relatime shared:841 - tmpfs tmpfs rw
2764 29 0:4 net:[4026538031] /run/netns/cni-c14eeef7-4134-bb74-f1cc-007ecd2a97b5 rw shared:856 - nsfs nsfs rw
2876 29 0:309 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/4f0074f05034c726a79d9c779d6f1d0d1f209a6ece89524d25b2a0b416d87ee9/shm rw,nosuid,nodev,noexec,relatime shared:871 - tmpfs shm rw,size=65536k
2996 29 0:310 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/4f0074f05034c726a79d9c779d6f1d0d1f209a6ece89524d25b2a0b416d87ee9/rootfs rw,relatime shared:878 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34011/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34011/work,xino=off
3036 29 0:320 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/d9bb40166d27212a2449a8cf85810a108c11aff3f9bde7d51b939ecd9acb4cf3/rootfs rw,relatime shared:885 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33296/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33295/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33294/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31119/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34012/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34012/work,xino=off
3088 145 0:329 / /data/nvme0n1/kubelet/pods/e2df7121-3621-4707-b7e9-6b99c3ce58b8/volumes/kubernetes.io~downward-api/annotations rw,relatime shared:893 - tmpfs tmpfs rw
3120 145 259:0 /vol1 /data/nvme0n1/kubelet/pods/e2df7121-3621-4707-b7e9-6b99c3ce58b8/volumes/kubernetes.io~local-volume/local-pv-b1fc6e56 rw,relatime shared:424 - ext4 /dev/nvme1n1 rw
3141 145 0:330 / /data/nvme0n1/kubelet/pods/e2df7121-3621-4707-b7e9-6b99c3ce58b8/volumes/kubernetes.io~projected/kube-api-access-jpzvq rw,relatime shared:906 - tmpfs tmpfs rw
3164 29 0:4 net:[4026538106] /run/netns/cni-bebf9810-8b52-9b5d-42cf-8760067f6b0b rw shared:915 - nsfs nsfs rw
3196 29 0:331 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/d585588652781fc3d764c036575ca7680416cdd1c7725ec9819897ab9066ede9/shm rw,nosuid,nodev,noexec,relatime shared:922 - tmpfs shm rw,size=65536k
3231 29 0:332 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/d585588652781fc3d764c036575ca7680416cdd1c7725ec9819897ab9066ede9/rootfs rw,relatime shared:930 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34015/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34015/work,xino=off
3303 29 0:342 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/a503134d5c6b832363be3926d09d6f641dfae06dd2108af08d80449368c4b04e/rootfs rw,relatime shared:937 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33296/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33295/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/33294/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31119/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34016/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34016/work,xino=off
1154 145 0:146 / /data/nvme0n1/kubelet/pods/4d283eb8-d2d7-490c-bb71-2be40dab9450/volumes/kubernetes.io~projected/kube-api-access-t8zfb rw,relatime shared:403 - tmpfs tmpfs rw
1299 29 0:4 net:[4026537510] /run/netns/cni-2f2e413a-7585-1a30-7cb1-ac92569fed15 rw shared:412 - nsfs nsfs rw
1329 29 0:147 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/3e4cd92bbd312e86fb59ebb9ce183220e0917434d86afb245c1715ffc5460bf8/shm rw,nosuid,nodev,noexec,relatime shared:421 - tmpfs shm rw,size=65536k
1355 29 0:148 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/3e4cd92bbd312e86fb59ebb9ce183220e0917434d86afb245c1715ffc5460bf8/rootfs rw,relatime shared:437 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34017/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34017/work,xino=off
1457 145 259:1 /kubelet/pods/4d283eb8-d2d7-490c-bb71-2be40dab9450/volumes/kubernetes.io~configmap/config/..2023_04_11_00_48_09.037538042/fluent-bit.conf /data/nvme0n1/kubelet/pods/4d283eb8-d2d7-490c-bb71-2be40dab9450/volume-subpaths/config/fluent-bit/0 rw,relatime shared:81 - ext4 /dev/nvme0n1 rw
1573 145 259:1 /kubelet/pods/4d283eb8-d2d7-490c-bb71-2be40dab9450/volumes/kubernetes.io~configmap/config/..2023_04_11_00_48_09.037538042/custom_parsers.conf /data/nvme0n1/kubelet/pods/4d283eb8-d2d7-490c-bb71-2be40dab9450/volume-subpaths/config/fluent-bit/1 rw,relatime shared:81 - ext4 /dev/nvme0n1 rw
1628 29 0:159 / /run/containerd/io.containerd.runtime.v2.task/k8s.io/5eec42b4a282163996409e4c7dbad906cb9649ef63f0e3cf16613c41e8c81909/rootfs rw,relatime shared:565 - overlay overlay rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31166/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31165/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31164/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31163/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31162/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/31161/fs,upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34018/fs,workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/34018/work,xino=off
`
)
