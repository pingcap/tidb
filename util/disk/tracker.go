// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package disk

import (
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

var (
	// GlobalDiskUsageTracker is the ancestor of all the Executors' disk tracker
	GlobalDiskUsageTracker *Tracker
)

const (
	// globalStorageLabel represents the label of the GlobalDiskUsageTracker
	globalStorageLabel string = "GlobalStorageLabel"
)

func init() {
	GlobalDiskUsageTracker = NewGlobalTrcaker(stringutil.StringerStr(globalStorageLabel), -1)
	action := &GlobalPanicOnExceed{}
	GlobalDiskUsageTracker.SetActionOnExceed(action)
}

// Tracker is used to track the disk usage during query execution.
type Tracker = memory.Tracker

// NewTracker creates a disk tracker.
//	1. "label" is the label used in the usage string.
//	2. "bytesLimit <= 0" means no limit.
var NewTracker = memory.NewTracker

// NewGlobalTrcaker creates a global disk tracker.
var NewGlobalTrcaker = memory.NewGlobalTracker

type GlobalPanicOnExceed = memory.GlobalPanicOnExceed
