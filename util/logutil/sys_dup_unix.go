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

// (NOT (linux and arm64)) AND (NOT (linux and riscv64)) AND (NOT windows) AND (NOT solaris)
// +build !linux !arm64
// +build !linux !riscv64
// +build !windows
// +build !solaris
// +build !wasm

package logutil

import (
	"syscall"

	"github.com/pingcap/errors"
)

func sysDup(oldfd int, newfd int) error {
	return errors.Trace(syscall.Dup2(oldfd, newfd))
}
