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
// See the License for the specific language governing permissions and
// limitations under the License.
// +build linux

package linux

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// OSVersion returns version info of operation system.
// e.g. Linux 4.15.0-45-generic.x86_64
func OSVersion() (osVersion string, err error) {
	var un syscall.Utsname
	err = syscall.Uname(&un)
	if err != nil {
		return
	}

	osVersion = "Linux " + *(*string)(unsafe.Pointer(&un.Release[:])) + "." + *(*string)(unsafe.Pointer(&un.Machine[:]))
	return
}

// SetAffinity sets cpu affinity.
func SetAffinity(cpus []int) error {
	var cpuSet unix.CPUSet
	cpuSet.Zero()
	for _, c := range cpus {
		cpuSet.Set(c)
	}
	return unix.SchedSetaffinity(unix.Getpid(), &cpuSet)
}
