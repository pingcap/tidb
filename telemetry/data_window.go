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
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	pmodel "github.com/prometheus/common/model"
	"go.uber.org/atomic"
)

var (
	// CurrentExecuteCount is CurrentExecuteCount
	CurrentExecuteCount atomic.Uint64
	// CurrentTiFlashPushDownCount is CurrentTiFlashPushDownCount
	CurrentTiFlashPushDownCount atomic.Uint64
	// CurrentTiFlashExchangePushDownCount is CurrentTiFlashExchangePushDownCount
	CurrentTiFlashExchangePushDownCount atomic.Uint64
	// CurrentCoprCacheHitRatioGTE0Count is CurrentCoprCacheHitRatioGTE1Count
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
)

const (
	// WindowSize determines how long some data is aggregated by.
	WindowSize = 1 * time.Hour
	// SubWindowSize determines how often data is rotated.
	SubWindowSize = 1 * time.Minute

	maxSubWindowLength         = int(ReportInterval / SubWindowSize) // TODO: Ceiling?
	maxSubWindowLengthInWindow = int(WindowSize / SubWindowSize)     // TODO: Ceiling?
	promReadTimeout            = time.Second * 30
)

type windowData struct {
	BeginAt        time.Time          `json:"beginAt"`
	ExecuteCount   uint64             `json:"executeCount"`
	TiFlashUsage   tiFlashUsageData   `json:"tiFlashUsage"`
	CoprCacheUsage coprCacheUsageData `json:"coprCacheUsage"`
	SQLUsage       sqlUsageData       `json:"SQLUsage"`
}

type sqlType map[string]uint64

type sqlUsageData struct {
	SQLTotal uint64  `json:"total"`
	SQLType  sqlType `json:"type"`
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
	PushDown         uint64 `json:"pushDown"`
	ExchangePushDown uint64 `json:"exchangePushDown"`
}

var (
	rotatedSubWindows []*windowData
	subWindowsLock    = sync.RWMutex{}
)

func getSQLSum(sqlTypeData *sqlType) uint64 {
	result := uint64(0)
	for _, v := range *sqlTypeData {
		result += v
	}
	return result
}

func readSQLMetric(timepoint time.Time, SQLResult *sqlUsageData) error {
	ctx := context.TODO()
	promQL := "avg(tidb_executor_statement_total{}) by (type)"
	result, err := querySQLMetric(ctx, timepoint, promQL)
	if err != nil {
		analysisSQLUsage(result, SQLResult)
	} else {
		analysisSQLUsage(result, SQLResult)
	}
	return nil
}

func querySQLMetric(ctx context.Context, queryTime time.Time, promQL string) (result pmodel.Value, err error) {
	// Add retry to avoid network error.
	var prometheusAddr string
	for i := 0; i < 5; i++ {
		//TODO: the prometheus will be Integrated into the PD, then we need to query the prometheus in PD directly, which need change the quire API
		prometheusAddr, err = infosync.GetPrometheusAddr()
		if err == nil || err == infosync.ErrPrometheusAddrIsNotSet {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return nil, err
	}
	promClient, err := api.NewClient(api.Config{
		Address: prometheusAddr,
	})
	if err != nil {
		return nil, err
	}
	promQLAPI := promv1.NewAPI(promClient)
	ctx, cancel := context.WithTimeout(ctx, promReadTimeout)
	defer cancel()
	// Add retry to avoid network error.
	for i := 0; i < 5; i++ {
		result, _, err = promQLAPI.Query(ctx, promQL, queryTime)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return result, err
}

func analysisSQLUsage(promResult pmodel.Value, SQLResult *sqlUsageData) {
	if promResult == nil {
		return
	}
	switch promResult.Type() {
	case pmodel.ValVector:
		matrix := promResult.(pmodel.Vector)
		for _, m := range matrix {
			v := m.Value
			promLable := string(m.Metric[pmodel.LabelName("type")])
			SQLResult.SQLType[promLable] = uint64(v)
		}
	}
}

// RotateSubWindow rotates the telemetry sub window.
func RotateSubWindow() {
	thisSubWindow := windowData{
		BeginAt:      time.Now(),
		ExecuteCount: CurrentExecuteCount.Swap(0),
		TiFlashUsage: tiFlashUsageData{
			PushDown:         CurrentTiFlashPushDownCount.Swap(0),
			ExchangePushDown: CurrentTiFlashExchangePushDownCount.Swap(0),
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
		SQLUsage: sqlUsageData{
			SQLTotal: 0,
			SQLType:  make(sqlType),
		},
	}

	err := readSQLMetric(time.Now(), &thisSubWindow.SQLUsage)
	if err != nil {
		logutil.BgLogger().Info("Error exists when getting the SQL Metric.")
	}

	thisSubWindow.SQLUsage.SQLTotal = getSQLSum(&thisSubWindow.SQLUsage.SQLType)

	subWindowsLock.Lock()
	rotatedSubWindows = append(rotatedSubWindows, &thisSubWindow)
	if len(rotatedSubWindows) > maxSubWindowLength {
		// Only retain last N sub windows, according to the report interval.
		rotatedSubWindows = rotatedSubWindows[len(rotatedSubWindows)-maxSubWindowLength:]
	}
	subWindowsLock.Unlock()
}

func calDeltaSQLTypeMap(cur sqlType, last sqlType) sqlType {
	deltaMap := make(sqlType)
	for key, value := range cur {
		deltaMap[key] = value - (last)[key]
	}
	return deltaMap
}

// getWindowData returns data aggregated by window size.
func getWindowData() []*windowData {
	results := make([]*windowData, 0)

	subWindowsLock.RLock()

	i := 0
	for i < len(rotatedSubWindows) {
		thisWindow := *rotatedSubWindows[i]
		var startWindow windowData
		if i == 0 {
			startWindow = thisWindow
		} else {
			startWindow = *rotatedSubWindows[i-1]
		}
		aggregatedSubWindows := 1
		// Aggregate later sub windows
		i++
		for i < len(rotatedSubWindows) && aggregatedSubWindows < maxSubWindowLengthInWindow {
			thisWindow.ExecuteCount += rotatedSubWindows[i].ExecuteCount
			thisWindow.TiFlashUsage.PushDown += rotatedSubWindows[i].TiFlashUsage.PushDown
			thisWindow.TiFlashUsage.ExchangePushDown += rotatedSubWindows[i].TiFlashUsage.ExchangePushDown
			thisWindow.CoprCacheUsage.GTE0 += rotatedSubWindows[i].CoprCacheUsage.GTE0
			thisWindow.CoprCacheUsage.GTE1 += rotatedSubWindows[i].CoprCacheUsage.GTE1
			thisWindow.CoprCacheUsage.GTE10 += rotatedSubWindows[i].CoprCacheUsage.GTE10
			thisWindow.CoprCacheUsage.GTE20 += rotatedSubWindows[i].CoprCacheUsage.GTE20
			thisWindow.CoprCacheUsage.GTE40 += rotatedSubWindows[i].CoprCacheUsage.GTE40
			thisWindow.CoprCacheUsage.GTE80 += rotatedSubWindows[i].CoprCacheUsage.GTE80
			thisWindow.CoprCacheUsage.GTE100 += rotatedSubWindows[i].CoprCacheUsage.GTE100
			thisWindow.SQLUsage.SQLTotal = rotatedSubWindows[i].SQLUsage.SQLTotal - startWindow.SQLUsage.SQLTotal
			thisWindow.SQLUsage.SQLType = calDeltaSQLTypeMap(rotatedSubWindows[i].SQLUsage.SQLType, startWindow.SQLUsage.SQLType)
			aggregatedSubWindows++
			i++
		}
		results = append(results, &thisWindow)
	}

	subWindowsLock.RUnlock()

	return results
}
