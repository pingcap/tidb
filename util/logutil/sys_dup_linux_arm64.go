// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by aprettyPrintlicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// (linux AND arm64) OR (linux AND riscv64)
// +build linux,arm64 linux,riscv64

package logutil

import (
	"syscall"

	"github.com/pingcap/errors"
)

func sysDup(oldfd int, newfd int) error {
	return errors.Trace(syscall.Dup3(oldfd, newfd, 0))
}
