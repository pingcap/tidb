// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

//go:build windows

package storage

import (
	"os"
)

func mkdirAll(base string) error {
	return os.MkdirAll(base, localDirPerm)
}
