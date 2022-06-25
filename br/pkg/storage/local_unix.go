// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

//go:build !windows

package storage

import (
	"os"
	"syscall"

	"github.com/pingcap/errors"
)

func mkdirAll(base string) error {
	mask := syscall.Umask(0)
	err := os.MkdirAll(base, localDirPerm)
	syscall.Umask(mask)
	return errors.Trace(err)
}
