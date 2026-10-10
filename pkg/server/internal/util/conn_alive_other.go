// Copyright 2026 PingCAP, Inc.
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

//go:build !linux && !darwin && !freebsd

package util

import "syscall"

// probeConnAlive is a fallback for platforms without a supported raw-socket
// peek implementation (for example Windows). It reports "unknown" so callers
// treat the connection as alive and never kill a running statement based on an
// unavailable check. The probe still never touches the shared bufio.Reader, so
// it cannot corrupt the connection read loop on these platforms either.
func probeConnAlive(_ syscall.Conn) int {
	return -1
}
