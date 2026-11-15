// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//go:build !linux

package cgroup

func discoverPlatformCPUBudget(budget CPUBudget) (CPUBudget, error) {
	return budget, nil // These platforms have no Linux cgroup CPU controller.
}
