// Copyright 2025 PingCAP, Inc.
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

package signal

import (
	"syscall"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// TiDBExit sends a SIGTERM signal to the current process
func TiDBExit(sig syscall.Signal) {
	err := syscall.Kill(syscall.Getpid(), sig)
	if err != nil {
		log.Error("failed to send signal", zap.Error(err), zap.String("signal", sig.String()))
	}
}
