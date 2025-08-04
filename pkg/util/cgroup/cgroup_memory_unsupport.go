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

//go:build !linux

package cgroup

// GetMemoryLimit returns the memory limit for the current process.
// This is Linux-specific and not supported on the current OS.
func GetMemoryLimit() (limit uint64, err error) {
	return 0, nil
}

// GetCgroupMemLimit returns the cgroup memory limit for the current process.
// This is Linux-specific and not supported on the current OS.
func GetCgroupMemLimit() (uint64, Version, error) {
	return 0, Unknown, nil
}

// GetMemoryUsage returns the memory usage for the current process.
// This is Linux-specific and not supported on the current OS.
func GetMemoryUsage() (usage uint64, err error) {
	return 0, nil
}

// GetMemoryInactiveFileUsage returns the inactive file memory usage for the current process.
// This is Linux-specific and not supported on the current OS.
func GetMemoryInactiveFileUsage() (usage uint64, err error) {
	return 0, nil
}
