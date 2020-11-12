// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// +build windows

package storage

import (
	"golang.org/x/sys/windows"
)

// GetTargetDirectoryCapacity get the capacity (bytes) of directory
func GetTargetDirectoryCapacity(path string) (uint64, error) {
	var freeBytes uint64
	err := windows.GetDiskFreeSpaceEx(windows.StringToUTF16Ptr(path), &freeBytes, nil, nil)
	if err != nil {
		return 0, err
	}
	return freeBytes, nil
}
