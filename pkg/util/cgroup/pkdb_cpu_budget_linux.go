// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//go:build linux

package cgroup

import (
	"math"

	"golang.org/x/sys/unix"
)

func discoverPlatformCPUBudget(budget CPUBudget) (CPUBudget, error) {
	var affinity unix.CPUSet
	if err := unix.SchedGetaffinity(0, &affinity); err != nil {
		return CPUBudget{}, err
	}
	budget.Affinity = max(1, affinity.Count())
	quota, err := boundCPUQuota("/", math.MaxInt)
	if err != nil {
		return CPUBudget{}, err
	}
	budget.Effective = min(budget.Effective, budget.Affinity, quota)
	if quota != math.MaxInt {
		budget.Cgroup = quota
	}
	return budget, nil
}
