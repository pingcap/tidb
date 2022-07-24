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

//go:build !windows

// TODO: Deduplicate this implementation with DM!

package common

import (
	"reflect"
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"golang.org/x/sys/unix"
)

// GetStorageSize gets storage's capacity and available size
func GetStorageSize(dir string) (size StorageSize, err error) {
	failpoint.Inject("GetStorageSize", func(val failpoint.Value) {
		injectedSize := val.(int)
		failpoint.Return(StorageSize{Capacity: uint64(injectedSize), Available: uint64(injectedSize)}, nil)
	})

	var stat unix.Statfs_t
	err = unix.Statfs(dir, &stat)
	if err != nil {
		return size, errors.Annotatef(err, "cannot get disk capacity at %s", dir)
	}

	// When container is run in MacOS, `bsize` obtained by `statfs` syscall is not the fundamental block size,
	// but the `iosize` (optimal transfer block size) instead, it's usually 1024 times larger than the `bsize`.
	// for example `4096 * 1024`. To get the correct block size, we should use `frsize`. But `frsize` isn't
	// guaranteed to be supported everywhere, so we need to check whether it's supported before use it.
	// For more details, please refer to: https://github.com/docker/for-mac/issues/2136
	bSize := uint64(stat.Bsize)
	field := reflect.ValueOf(&stat).Elem().FieldByName("Frsize")
	if field.IsValid() {
		if field.Kind() == reflect.Uint64 {
			bSize = field.Uint()
		} else {
			bSize = uint64(field.Int())
		}
	}

	// Available blocks * size per block = available space in bytes
	size.Available = stat.Bavail * bSize
	size.Capacity = stat.Blocks * bSize

	return
}

// SameDisk is used to check dir1 and dir2 in the same disk.
func SameDisk(dir1 string, dir2 string) (bool, error) {
	st1 := syscall.Stat_t{}
	st2 := syscall.Stat_t{}

	if err := syscall.Stat(dir1, &st1); err != nil {
		return false, err
	}

	if err := syscall.Stat(dir2, &st2); err != nil {
		return false, err
	}

	return st1.Dev == st2.Dev, nil
}
