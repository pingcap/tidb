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
//go:build linux || darwin || freebsd || unix

package util

import (
	"syscall"
	"time"
)

var (
	lastInspectUnixNano int64
	lastCPUUsageTime    int64
)

// GetCPUPercentage calculates CPU usage and returns percentage in float64(e.g. 2.5 means 2.5%).
// http://man7.org/linux/man-pages/man2/getrusage.2.html
func GetCPUPercentage() float64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	usageTime := ru.Utime.Nano() + ru.Stime.Nano()
	nowTime := time.Now().UnixNano()
	perc := float64(usageTime-lastCPUUsageTime) / float64(nowTime-lastInspectUnixNano) * 100.0
	lastInspectUnixNano = nowTime
	lastCPUUsageTime = usageTime
	return perc
}
