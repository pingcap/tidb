// Copyright 2021 PingCAP, Inc.
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

package telemetry

import (
	"sync"
	"time"

	"go.uber.org/atomic"
)

var (
	// CurrentExecuteCount is CurrentExecuteCount
	CurrentExecuteCount atomic.Uint64
	// CurrentTiFlashPushDownCount is CurrentTiFlashPushDownCount
	CurrentTiFlashPushDownCount atomic.Uint64
	// CurrentTiFlashExchangePushDownCount is CurrentTiFlashExchangePushDownCount
	CurrentTiFlashExchangePushDownCount atomic.Uint64
	// CurrentCoprCacheHitRatioGTE0Count is CurrentCoprCacheHitRatioGTE0Count
	CurrentCoprCacheHitRatioGTE0Count atomic.Uint64
	// CurrentCoprCacheHitRatioGTE1Count is CurrentCoprCacheHitRatioGTE1Count
	CurrentCoprCacheHitRatioGTE1Count atomic.Uint64
	// CurrentCoprCacheHitRatioGTE10Count is CurrentCoprCacheHitRatioGTE10Count
	CurrentCoprCacheHitRatioGTE10Count atomic.Uint64
	// CurrentCoprCacheHitRatioGTE20Count is CurrentCoprCacheHitRatioGTE20Count
	CurrentCoprCacheHitRatioGTE20Count atomic.Uint64
	// CurrentCoprCacheHitRatioGTE40Count is CurrentCoprCacheHitRatioGTE40Count
	CurrentCoprCacheHitRatioGTE40Count atomic.Uint64
	// CurrentCoprCacheHitRatioGTE80Count is CurrentCoprCacheHitRatioGTE80Count
	CurrentCoprCacheHitRatioGTE80Count atomic.Uint64
	// CurrentCoprCacheHitRatioGTE100Count is CurrentCoprCacheHitRatioGTE100Count
	CurrentCoprCacheHitRatioGTE100Count atomic.Uint64
	// CurrentTiflashTableScanCount count the number of tiflash table scan and tiflash partition table scan
	CurrentTiflashTableScanCount atomic.Uint64
	// CurrentTiflashTableScanWithFastScanCount count the number of tiflash table scan and tiflash partition table scan which use fastscan
	CurrentTiflashTableScanWithFastScanCount atomic.Uint64
)

const (
	// WindowSize determines how long some data is aggregated by.
	WindowSize = 1 * time.Hour
	// SubWindowSize determines how often data is rotated.
	SubWindowSize = 1 * time.Minute

	maxSubWindowLength         = int(ReportInterval / SubWindowSize) // TODO: Ceiling?
	maxSubWindowLengthInWindow = int(WindowSize / SubWindowSize)     // TODO: Ceiling?
)

type windowData struct {
	BeginAt               time.Time          `json:"beginAt"`
	ExecuteCount          uint64             `json:"executeCount"`
	TiFlashUsage          tiFlashUsageData   `json:"tiFlashUsage"`
	CoprCacheUsage        coprCacheUsageData `json:"coprCacheUsage"`
	BuiltinFunctionsUsage map[string]uint32  `json:"builtinFunctionsUsage"`
}

type coprCacheUsageData struct {
	GTE0   uint64 `json:"gte0"`
	GTE1   uint64 `json:"gte1"`
	GTE10  uint64 `json:"gte10"`
	GTE20  uint64 `json:"gte20"`
	GTE40  uint64 `json:"gte40"`
	GTE80  uint64 `json:"gte80"`
	GTE100 uint64 `json:"gte100"`
}

type tiFlashUsageData struct {
	PushDown              uint64 `json:"pushDown"`
	ExchangePushDown      uint64 `json:"exchangePushDown"`
	TableScan             uint64 `json:"tableScan"`
	TableScanWithFastScan uint64 `json:"tableScanWithFastScan"`
}

// builtinFunctionsUsageCollector collects builtin functions usage information and dump it into windowData.
type builtinFunctionsUsageCollector struct {
	sync.Mutex

	// Should acquire lock to access this
	usageData BuiltinFunctionsUsage
}

// Merge BuiltinFunctionsUsage data
func (b *builtinFunctionsUsageCollector) Collect(usageData BuiltinFunctionsUsage) {
	// TODO(leiysky): use multi-worker to collect the usage information so we can make this asynchronous
	b.Lock()
	defer b.Unlock()
	b.usageData.Merge(usageData)
}

// Dump BuiltinFunctionsUsage data
func (b *builtinFunctionsUsageCollector) Dump() map[string]uint32 {
	b.Lock()
	ret := b.usageData
	b.usageData = make(map[string]uint32)
	b.Unlock()

	return ret
}

// BuiltinFunctionsUsage is a map from ScalarFuncSig_name(string) to usage count(uint32)
type BuiltinFunctionsUsage map[string]uint32

// Inc will increase the usage count of scalar function by 1
func (b BuiltinFunctionsUsage) Inc(scalarFuncSigName string) {
	v, ok := b[scalarFuncSigName]
	if !ok {
		b[scalarFuncSigName] = 1
	} else {
		b[scalarFuncSigName] = v + 1
	}
}

// Merge BuiltinFunctionsUsage data
func (b BuiltinFunctionsUsage) Merge(usageData BuiltinFunctionsUsage) {
	for k, v := range usageData {
		prev, ok := b[k]
		if !ok {
			b[k] = v
		} else {
			b[k] = prev + v
		}
	}
}

// GlobalBuiltinFunctionsUsage is used to collect builtin functions usage information
var GlobalBuiltinFunctionsUsage = &builtinFunctionsUsageCollector{usageData: make(BuiltinFunctionsUsage)}

var (
	rotatedSubWindows []*windowData
	subWindowsLock    = sync.RWMutex{}
)

// RotateSubWindow rotates the telemetry sub window.
func RotateSubWindow() {
	thisSubWindow := windowData{
		BeginAt:      time.Now(),
		ExecuteCount: CurrentExecuteCount.Swap(0),
		TiFlashUsage: tiFlashUsageData{
			PushDown:              CurrentTiFlashPushDownCount.Swap(0),
			ExchangePushDown:      CurrentTiFlashExchangePushDownCount.Swap(0),
			TableScan:             CurrentTiflashTableScanCount.Swap(0),
			TableScanWithFastScan: CurrentTiflashTableScanWithFastScanCount.Swap(0),
		},
		CoprCacheUsage: coprCacheUsageData{
			GTE0:   CurrentCoprCacheHitRatioGTE0Count.Swap(0),
			GTE1:   CurrentCoprCacheHitRatioGTE1Count.Swap(0),
			GTE10:  CurrentCoprCacheHitRatioGTE10Count.Swap(0),
			GTE20:  CurrentCoprCacheHitRatioGTE20Count.Swap(0),
			GTE40:  CurrentCoprCacheHitRatioGTE40Count.Swap(0),
			GTE80:  CurrentCoprCacheHitRatioGTE80Count.Swap(0),
			GTE100: CurrentCoprCacheHitRatioGTE100Count.Swap(0),
		},
		BuiltinFunctionsUsage: GlobalBuiltinFunctionsUsage.Dump(),
	}

	subWindowsLock.Lock()
	rotatedSubWindows = append(rotatedSubWindows, &thisSubWindow)
	if len(rotatedSubWindows) > maxSubWindowLength {
		// Only retain last N sub windows, according to the report interval.
		rotatedSubWindows = rotatedSubWindows[len(rotatedSubWindows)-maxSubWindowLength:]
	}
	subWindowsLock.Unlock()
}

// getWindowData returns data aggregated by window size.
func getWindowData() []*windowData {
	results := make([]*windowData, 0)

	subWindowsLock.RLock()

	i := 0
	for i < len(rotatedSubWindows) {
		thisWindow := *rotatedSubWindows[i]
		aggregatedSubWindows := 1
		// Aggregate later sub windows
		i++
		for i < len(rotatedSubWindows) && aggregatedSubWindows < maxSubWindowLengthInWindow {
			thisWindow.ExecuteCount += rotatedSubWindows[i].ExecuteCount
			thisWindow.TiFlashUsage.PushDown += rotatedSubWindows[i].TiFlashUsage.PushDown
			thisWindow.TiFlashUsage.ExchangePushDown += rotatedSubWindows[i].TiFlashUsage.ExchangePushDown
			thisWindow.TiFlashUsage.TableScan += rotatedSubWindows[i].TiFlashUsage.TableScan
			thisWindow.TiFlashUsage.TableScanWithFastScan += rotatedSubWindows[i].TiFlashUsage.TableScanWithFastScan
			thisWindow.CoprCacheUsage.GTE0 += rotatedSubWindows[i].CoprCacheUsage.GTE0
			thisWindow.CoprCacheUsage.GTE1 += rotatedSubWindows[i].CoprCacheUsage.GTE1
			thisWindow.CoprCacheUsage.GTE10 += rotatedSubWindows[i].CoprCacheUsage.GTE10
			thisWindow.CoprCacheUsage.GTE20 += rotatedSubWindows[i].CoprCacheUsage.GTE20
			thisWindow.CoprCacheUsage.GTE40 += rotatedSubWindows[i].CoprCacheUsage.GTE40
			thisWindow.CoprCacheUsage.GTE80 += rotatedSubWindows[i].CoprCacheUsage.GTE80
			thisWindow.CoprCacheUsage.GTE100 += rotatedSubWindows[i].CoprCacheUsage.GTE100

			mergedBuiltinFunctionsUsage := BuiltinFunctionsUsage(thisWindow.BuiltinFunctionsUsage)
			mergedBuiltinFunctionsUsage.Merge(BuiltinFunctionsUsage(rotatedSubWindows[i].BuiltinFunctionsUsage))
			thisWindow.BuiltinFunctionsUsage = mergedBuiltinFunctionsUsage
			aggregatedSubWindows++
			i++
		}
		results = append(results, &thisWindow)
	}

	subWindowsLock.RUnlock()

	return results
}
