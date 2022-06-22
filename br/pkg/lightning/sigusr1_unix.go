// Copyright 2020 PingCAP, Inc.
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

//go:build linux || darwin || freebsd || unix

package lightning

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"go.uber.org/zap"
)

// handleSigUsr1 listens for the SIGUSR1 signal and executes `handler()` every time it is received.
func handleSigUsr1(handler func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		for sig := range ch {
			log.L().Debug("received signal", zap.Stringer("signal", sig))
			handler()
		}
	}()
}
