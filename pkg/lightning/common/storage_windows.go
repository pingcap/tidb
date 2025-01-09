// Copyright 2019 PingCAP, Inc.
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

//go:build windows

// TODO: Deduplicate this implementation with DM!

package common

import (
	"syscall"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
)

var (
	kernel32            = syscall.MustLoadDLL("kernel32.dll")
	getDiskFreeSpaceExW = kernel32.MustFindProc("GetDiskFreeSpaceExW")
)

// GetStorageSize gets storage's capacity and available size
func GetStorageSize(dir string) (size StorageSize, err error) {
	failpoint.Inject("GetStorageSize", func(val failpoint.Value) {
		injectedSize := val.(int)
		failpoint.Return(StorageSize{Capacity: uint64(injectedSize), Available: uint64(injectedSize)}, nil)
	})
	r, _, e := getDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(dir))),
		uintptr(unsafe.Pointer(&size.Available)),
		uintptr(unsafe.Pointer(&size.Capacity)),
		0,
	)
	if r == 0 {
		err = errors.Annotatef(e, "cannot get disk capacity at %s", dir)
	}
	return
}

// SameDisk is used to check dir1 and dir2 in the same disk.
func SameDisk(dir1 string, dir2 string) (bool, error) {
	// FIXME
	return false, nil
}
