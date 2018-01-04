// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build darwin dragonfly freebsd linux,!386 netbsd openbsd

package ipv4

import (
	"syscall"
	"unsafe"
)

func getsockopt(s uintptr, level, name int, v unsafe.Pointer, l *uint32) error {
	if _, _, errno := syscall.Syscall6(syscall.SYS_GETSOCKOPT, s, uintptr(level), uintptr(name), uintptr(v), uintptr(unsafe.Pointer(l)), 0); errno != 0 {
		return error(errno)
	}
	return nil
}

func setsockopt(s uintptr, level, name int, v unsafe.Pointer, l uint32) error {
	if _, _, errno := syscall.Syscall6(syscall.SYS_SETSOCKOPT, s, uintptr(level), uintptr(name), uintptr(v), uintptr(l), 0); errno != 0 {
		return error(errno)
	}
	return nil
}
