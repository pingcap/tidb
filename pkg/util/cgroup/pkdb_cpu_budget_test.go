// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package cgroup

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEffectiveCPUQuotaAncestors(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, tc := range []struct {
			name         string
			parent, leaf string
			cap, want    int
		}{
			{"parent", "600000", "-1", 24, 6},
			{"child", "600000", "300000", 24, 3},
			{"runtime", "600000", "-1", 2, 2},
			{"fractional", "150000", "-1", 24, 1},
			{"subCPU", "50000", "-1", 24, 1},
			{"unlimited", "-1", "-1", 12, 12},
		} {
			t.Run(fmt.Sprintf("v%d/%s", version, tc.name), func(t *testing.T) {
				root := t.TempDir()
				write := func(path, value string) {
					path = filepath.Join(root, path)
					require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
					require.NoError(t, os.WriteFile(path, []byte(value), 0600))
				}
				controller, fs := "cpu,cpuacct", "cgroup"
				membership := "2:cpu,cpuacct:/tenant/parent/leaf\n"
				if version == 2 {
					controller, fs = "rw", "cgroup2"
					membership = "0::/tenant/parent/leaf\n"
				}
				write(procPathCGroup, membership)
				// A namespaced mount with an escaped space exercises both path
				// mapping and the visible-root boundary. Do not read outside it.
				write(procPathMountInfo, fmt.Sprintf("1 0 0:1 /tenant /sys/cpu\\040mount rw - %s cgroup %s\n", fs, controller))
				for directory, quota := range map[string]string{"": "-1", "/parent": tc.parent, "/parent/leaf": tc.leaf} {
					base := "/sys/cpu mount" + directory
					if version == 1 {
						write(base+"/cpu.cfs_period_us", "100000")
						write(base+"/cpu.cfs_quota_us", quota)
					} else {
						if quota == "-1" {
							quota = "max"
						}
						write(base+"/cpu.max", quota+" 100000")
					}
				}
				count, err := boundCPUQuota(root, tc.cap)
				require.NoError(t, err)
				require.Equal(t, tc.want, count)
				if version == 2 {
					// CPU controller disabled in the leaf still inherits parent.
					require.NoError(t, os.Remove(filepath.Join(root, "sys/cpu mount/parent/leaf/cpu.max")))
					require.NoError(t, os.Remove(filepath.Join(root, "sys/cpu mount/cpu.max")))
					count, err = boundCPUQuota(root, tc.cap)
					require.NoError(t, err)
					parentQuota, parseErr := strconv.Atoi(tc.parent)
					require.NoError(t, parseErr)
					wantParent := tc.cap
					if parentQuota > 0 {
						wantParent = min(tc.cap, max(1, parentQuota/100000))
					}
					require.Equal(t, wantParent, count)
					write("/sys/cpu mount/parent/cpu.max", "600000 0")
				} else {
					write("/sys/cpu mount/parent/cpu.cfs_period_us", "0")
				}
				_, err = boundCPUQuota(root, tc.cap)
				require.Error(t, err)
			})
		}
	}
}

func TestEffectiveCPUQuotaUnknownMount(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "proc/self"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, procPathCGroup), []byte("0::/outside\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, procPathMountInfo), []byte("1 0 0:1 /tenant /sys/cgroup rw - cgroup2 cgroup rw\n"), 0600))
	_, err := boundCPUQuota(root, 24)
	require.ErrorContains(t, err, "no visible matching mount")
	require.NoError(t, os.WriteFile(filepath.Join(root, procPathCGroup), nil, 0600))
	count, err := boundCPUQuota(root, 24)
	require.NoError(t, err)
	require.Equal(t, 24, count)
}

func TestEffectiveCPUQuotaColonPaths(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, tc := range []struct {
			name, group, truncated, leafQuota string
			want                              int
		}{
			{"parent", "/tenant:worker/leaf", "/tenant", "-1", 2},
			{"leaf", "/tenant/leaf:worker", "/tenant/leaf", "100000", 1},
			{"both", "/tenant:worker/leaf:batch", "/tenant", "-1", 2},
		} {
			for _, sibling := range []bool{false, true} {
				t.Run(fmt.Sprintf("v%d/%s/sibling=%t", version, tc.name, sibling), func(t *testing.T) {
					controller, fs := "cpu,cpuacct", "cgroup"
					membership := "2:cpu,cpuacct:" + tc.group + "\n"
					if version == 2 {
						controller, fs = "rw", "cgroup2"
						membership = "0::" + tc.group + "\n"
					}
					files := map[string]string{
						procPathCGroup:    membership,
						procPathMountInfo: fmt.Sprintf("1 0 0:1 / /sys/cgroup rw - %s cgroup %s\n", fs, controller),
					}
					quotas := map[string]string{
						"/":                    "-1",
						filepath.Dir(tc.group): "200000",
						tc.group:               tc.leafQuota,
					}
					if sibling {
						// A truncated path must not select an unrelated cgroup,
						// even when that directory exists and has no CPU limit.
						quotas[tc.truncated] = "-1"
					}
					for directory, quota := range quotas {
						base := filepath.Join("/sys/cgroup", directory)
						if version == 1 {
							files[base+"/cpu.cfs_period_us"] = "100000"
							files[base+"/cpu.cfs_quota_us"] = quota
						} else {
							if quota == "-1" {
								quota = "max"
							}
							files[base+"/cpu.max"] = quota + " 100000"
						}
					}
					count, err := boundCPUQuota(createFiles(t, files), 24)
					require.NoError(t, err)
					require.Equal(t, tc.want, count)
				})
			}
		}
	}
}

func TestEffectiveCPUCountRealQuota(t *testing.T) {
	want := os.Getenv("SPFRESH_CPU_PROBE_EXPECTED")
	if want == "" {
		t.Skip("explicit affinity and cgroup probe required")
	}
	expected, err := strconv.Atoi(want)
	require.NoError(t, err)
	actual, err := EffectiveCPUCount()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	fmt.Printf("SPFRESH_CPU_VALIDATED effective=%d gomaxprocs=%d available=%d\n", actual, runtime.GOMAXPROCS(0), runtime.NumCPU())
}

func TestEffectiveCPUQuotaV2RejectsMalformedLimits(t *testing.T) {
	root := t.TempDir()
	for _, input := range []string{"max", "max 0", "-1 100000", "0 100000", "600000 100000 extra", "bad 100000"} {
		require.NoError(t, os.WriteFile(filepath.Join(root, "cpu.max"), []byte(input), 0600))
		_, _, err := visibleCPUQuotaV2(root)
		require.Error(t, err, input)
	}
}

func TestEffectiveCPUQuotaUsesBroadestVisibleMount(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"proc/self/cgroup":              "0::/tenant/leaf\n",
		"proc/self/mountinfo":           "1 0 0:1 / /sys/broad rw - cgroup2 cgroup rw\n2 0 0:1 /tenant/leaf /sys/bind rw - cgroup2 cgroup rw\n",
		"sys/broad/tenant/cpu.max":      "600000 100000",
		"sys/broad/tenant/leaf/cpu.max": "max 100000",
		"sys/bind/cpu.max":              "max 100000",
	}
	for path, value := range files {
		path = filepath.Join(root, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
		require.NoError(t, os.WriteFile(path, []byte(value), 0600))
	}
	count, err := boundCPUQuota(root, 24)
	require.NoError(t, err)
	require.Equal(t, 6, count)
}
