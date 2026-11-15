// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package cgroup

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// EffectiveCPUCount bounds resource-aware work by GOMAXPROCS, available CPUs,
// and, on Linux, affinity and every visible ancestor cgroup CPU quota. Fractional
// quotas round down, with one execution slot for quotas below one CPU. It does
// not alter the runtime or existing DDL policy. Call once when sizing an attempt.
func EffectiveCPUCount() (int, error) {
	budget, err := DiscoverCPUBudget()
	return budget.Effective, err
}

// CPUBudget records the independently discovered limits used to size work.
// Cgroup is zero when no visible ancestor imposes a finite quota.
type CPUBudget struct {
	GOMAXPROCS int
	Available  int
	Affinity   int
	Cgroup     int
	Effective  int
}

// DiscoverCPUBudget samples the same limits as EffectiveCPUCount and exposes
// their values for diagnostics. A failed discovery must not be used for sizing.
func DiscoverCPUBudget() (CPUBudget, error) {
	budget := CPUBudget{GOMAXPROCS: runtime.GOMAXPROCS(0), Available: runtime.NumCPU()}
	budget.Effective = min(budget.GOMAXPROCS, budget.Available)
	return discoverPlatformCPUBudget(budget)
}

// Keep discovery separate from the existing CPU usage/automaxprocs APIs: their
// callers retain their current behavior. Both cgroup versions are current OS
// interfaces, not SPFresh development-format compatibility paths.
func boundCPUQuota(root string, limit int) (int, error) {
	group, err := detectControlPath(filepath.Join(root, procPathCGroup), "cpu")
	if err != nil {
		return 0, err
	}
	if group == "" {
		return limit, nil // No CPU controller: affinity/runtime are the limits.
	}
	raw, err := os.ReadFile(filepath.Join(root, procPathMountInfo))
	if err != nil {
		return 0, err
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	var leaf, mount string
	version, specificity := 0, -1
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		fields := bytes.Fields(line)
		ver, ok := detectCgroupVersion(fields, "cpu")
		if !ok {
			continue
		}
		nsRoot := unescape.Replace(string(fields[3]))
		point := unescape.Replace(string(fields[4]))
		if !filepath.IsAbs(nsRoot) || !filepath.IsAbs(point) || !filepath.IsAbs(group) {
			return 0, fmt.Errorf("CPU cgroup paths must be absolute")
		}
		relative, err := filepath.Rel(nsRoot, group)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			continue // This mount does not expose this process's cgroup.
		}
		if version == 1 && ver == 2 || version == ver && len(nsRoot) >= specificity {
			continue
		}
		// Prefer the broadest visible mount so bind mounts cannot hide an
		// ancestor limit that is readable through another mount.
		version, specificity = ver, len(nsRoot)
		mount = filepath.Join(root, point)
		leaf = filepath.Join(mount, relative)
	}
	if version == 0 {
		return 0, fmt.Errorf("CPU cgroup has no visible matching mount")
	}
	for current := leaf; ; current = filepath.Dir(current) {
		var period, quota int64
		if version == 1 {
			period, quota, err = detectCPUQuotaInV1(current)
		} else {
			period, quota, err = visibleCPUQuotaV2(current)
			// A v2 root has no cpu.max; descendants may lack it when CPU is
			// not enabled in subtree_control. Their ancestors still constrain
			// execution. A disappearing directory or other I/O error is unknown.
			if os.IsNotExist(err) {
				if info, statErr := os.Stat(current); statErr == nil && info.IsDir() {
					period, quota, err = 1, -1, nil
				}
			}
		}
		if err != nil {
			return 0, err
		}
		if period <= 0 || quota == 0 || quota < -1 {
			return 0, fmt.Errorf("invalid CPU quota at %s: %d/%d", current, quota, period)
		}
		if quota > 0 {
			limit = int(min(int64(limit), max(int64(1), quota/period)))
		}
		if current == mount {
			return limit, nil
		}
	}
}

// Read cpu.max without hiding filesystem errors; missing files can mean that
// the CPU controller is disabled at this level, while ancestors still apply.
func visibleCPUQuotaV2(directory string) (period, quota int64, err error) {
	raw, err := os.ReadFile(filepath.Join(directory, cgroupV2CPUMax))
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(raw))
	if len(fields) != 2 {
		return 0, 0, fmt.Errorf("invalid cpu.max at %s", directory)
	}
	period, err = strconv.ParseInt(fields[1], 10, 64)
	if err != nil || period <= 0 {
		return 0, 0, fmt.Errorf("invalid CPU period at %s", directory)
	}
	if fields[0] == "max" {
		return period, -1, nil
	}
	quota, err = strconv.ParseInt(fields[0], 10, 64)
	if err != nil || quota <= 0 {
		return 0, 0, fmt.Errorf("invalid CPU quota at %s", directory)
	}
	return period, quota, nil
}
