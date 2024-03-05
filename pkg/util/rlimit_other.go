// Copyright 2022 PingCAP, Inc.
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

package util

import (
	"fmt"
	"syscall"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// GenRLimit get RLIMIT_NOFILE limit
func GenRLimit(source string) uint64 {
	rLimit := uint64(1024)
	var rl syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl)
	if err != nil {
		logutil.BgLogger().Warn(fmt.Sprintf("[%s] get system open file limit error", source), zap.Error(err), zap.String("default", "1024"))
	} else {
		//nolint: unconvert
		rLimit = uint64(rl.Cur)
	}
	return rLimit
}
