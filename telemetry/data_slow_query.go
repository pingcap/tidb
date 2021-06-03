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
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	pingcapErrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	pmodel "github.com/prometheus/common/model"
	"go.uber.org/zap"
)

type slowQueryStats struct {
	// Slow Query statistic buckets
	SQBInfo *SlowQueryBucket `json:"slowQueryBucket"`
}

// SlowQueryBucket records the statistic information of slow query buckets
// Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days  // defined in metrics/server.go
type SlowQueryBucket map[string]int

func (bucketMap SlowQueryBucket) String() string {
	if bucketMap == nil {
		return "nil"
	}
	var retStr string = "{"
	for k, v := range bucketMap {
		retStr += k + ":" + strconv.Itoa(v) + ","
	}
	retStr = retStr[:len(retStr)-1]
	return retStr
}

const slowQueryBucketNum = 29 //prometheus.ExponentialBuckets(0.001, 2, 28), and 1 more +Inf

var (
	// LastSQBInfo records last statistic information of slow query buckets
	LastSQBInfo SlowQueryBucket
	// CurrentSQBInfo records current statitic information of slow query buckets
	CurrentSQBInfo SlowQueryBucket
	slowQueryLock  = sync.RWMutex{}
)

func getSlowQueryStats(ctx sessionctx.Context) (*slowQueryStats, error) {
	slowQueryBucket, err := getSlowQueryBucket(ctx)
	if err != nil {
		logutil.BgLogger().Info(err.Error())
		return nil, err
	}

	return &slowQueryStats{slowQueryBucket}, nil
}

// getSlowQueryBucket genenrates the delta SlowQueryBucket to report
func getSlowQueryBucket(ctx sessionctx.Context) (*SlowQueryBucket, error) {
	// update CurrentSQBInfo first, then gen delta
	if err := updateCurrentSQB(ctx); err != nil {
		return nil, err
	}
	delta := calculateDeltaSQB()
	return delta, nil
}

// updateCurrentSQB records current slow query buckets
func updateCurrentSQB(ctx sessionctx.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = pingcapErrors.Errorf(fmt.Sprintln(r))
		}
	}()

	value, err := querySlowQueryMetric(ctx) //TODO: judge error here
	if err != nil && err != infosync.ErrPrometheusAddrIsNotSet {
		logutil.BgLogger().Info("querySlowQueryMetric got error")
		return err
	}

	if value.Type() != pmodel.ValVector {
		return errors.New("Prom vector expected, got " + value.Type().String())
	}
	promVec := value.(pmodel.Vector)
	slowQueryLock.Lock()
	for _, sample := range promVec {
		metric := sample.Metric
		bucketName := metric["le"] //hardcode bucket upper bound
		CurrentSQBInfo[string(bucketName)] = int(sample.Value)
	}
	slowQueryLock.Unlock()
	return nil
}

func querySlowQueryMetric(sctx sessionctx.Context) (result pmodel.Value, err error) {
	// Add retry to avoid network error.
	var prometheusAddr string
	for i := 0; i < 5; i++ {
		//TODO: the prometheus will be Integrated into the PD, then we need to query the prometheus in PD directly, which need change the query API
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
	promQL := "tidb_server_slow_query_process_duration_seconds_bucket{sql_type=\"general\"}"

	ts := time.Now().Add(-time.Minute)
	// Add retry to avoid network error.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	for i := 0; i < 5; i++ {
		result, _, err = promQLAPI.Query(ctx, promQL, ts)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return result, err
}

// calculateDeltaSQB calculate the delta between current slow query bucket and last slow query bucket
func calculateDeltaSQB() *SlowQueryBucket {
	deltaMap := make(SlowQueryBucket)
	slowQueryLock.Lock()
	for key, value := range CurrentSQBInfo {
		deltaMap[key] = value - (LastSQBInfo)[key]
	}
	slowQueryLock.Unlock()
	return &deltaMap
}

// initSlowQueryStats Init LastSQBInfo, follow the definition of metrics/server.go
// Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
func initSlowQueryStats() {
	LastSQBInfo := make(SlowQueryBucket)
	CurrentSQBInfo := make(SlowQueryBucket)

	bucketBase := 0.001 // From 0.001 to 134217.728, total 28 float number; the 29th is +Inf
	for i := 0; i < slowQueryBucketNum-1; i++ {
		LastSQBInfo[strconv.FormatFloat(bucketBase, 'f', 3, 32)] = 0
		CurrentSQBInfo[strconv.FormatFloat(bucketBase, 'f', 3, 32)] = 0
		bucketBase += bucketBase
	}
	LastSQBInfo["+Inf"] = 0
	CurrentSQBInfo["+Inf"] = 0

	logutil.BgLogger().Info("Telemetry slow query stats initialized", zap.String("CurrentSQBInfo", CurrentSQBInfo.String()), zap.String("LastSQBInfo", LastSQBInfo.String()))
}

// postReportSlowQueryStats copy CurrentSQBInfo to LastSQBInfo to be ready for next report
// this function is designed for being compatible with preview telemetry
func postReportSlowQueryStats() {
	slowQueryLock.Lock()
	LastSQBInfo = CurrentSQBInfo
	CurrentSQBInfo = make(SlowQueryBucket)
	slowQueryLock.Unlock()
	logutil.BgLogger().Info("Telemetry slow query stats, postReportSlowQueryStats finished")
}
