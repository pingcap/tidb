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
	"encoding/json"
	"errors"
	"strconv"
	"time"

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

// SlowQueryStat records the statistic information of slow query buckets
// Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days  // defined in metrics/server.go
type SlowQueryBucket map[string]int

const SLOW_QUERY_BUCKET_NUM = 29 //prometheus.ExponentialBuckets(0.001, 2, 28), and 1 more +Inf

var (
	// LastSQBInfo records last statistic information of slow query buckets
	LastSQBInfo SlowQueryBucket
	// CurrentSQBInfo records current statitic information of slow query buckets
	CurrentSQBInfo SlowQueryBucket
)

func getSlowQueryStats(ctx sessionctx.Context) (*slowQueryStats, error) {
	slowQueryBucket, err := GetSlowQueryBucket(ctx)
	if err != nil {
		logutil.BgLogger().Info(err.Error())
		return nil, err
	}

	return &slowQueryStats{slowQueryBucket}, nil
}

// GetSlowQueryBucket genenrates the delta SlowQueryBucket to report
func GetSlowQueryBucket(ctx sessionctx.Context) (*SlowQueryBucket, error) {
	// update CurrentSQBInfo first, then gen delta
	if err := UpdateCurrentSQB(ctx); err != nil {
		return nil, err
	}
	delta := CalculateDeltaSQB()
	return delta, nil
}

// UpdateCurrentSQB records current slow query buckets
func UpdateCurrentSQB(ctx sessionctx.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown failure")
			}
		}
	}()

	value, err := querySlowQueryMetric(ctx) //TODO: judge error here
	if err != nil {
		logutil.BgLogger().Info("querySlowQueryMetric got error")
		return err
	}

	if value.Type() != pmodel.ValVector {
		return errors.New("Prom vector expected, got " + value.Type().String())
	}
	promVec := value.(pmodel.Vector)
	for _, sample := range promVec {
		metric := sample.Metric
		bucketName := metric["le"] //hardcode bucket upper bound
		CurrentSQBInfo[string(bucketName)] = int(sample.Value)
	}
	return nil
}

func querySlowQueryMetric(sctx sessionctx.Context) (result pmodel.Value, err error) {
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
	promQL := "tidb_server_slow_query_process_duration_seconds_bucket{sql_type=\"general\"}"

	ts := time.Now()
	// Add retry to avoid network error.
	ctx := context.TODO() // just use default context
	for i := 0; i < 5; i++ {
		result, _, err = promQLAPI.Query(ctx, promQL, ts)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return result, err
}

// CalculateDeltaSQB calculate the delta between current slow query bucket and last slow query bucket
func CalculateDeltaSQB() *SlowQueryBucket {
	deltaMap := make(SlowQueryBucket)
	for key, value := range CurrentSQBInfo {
		deltaMap[key] = value - (LastSQBInfo)[key]
	}
	return &deltaMap
}

// InitSlowQueryStats Init LastSQBInfo, follow the definition of metrics/server.go
// Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
func InitSlowQueryStats() {
	LastSQBInfo := make(SlowQueryBucket)
	CurrentSQBInfo := make(SlowQueryBucket)

	bucketBase := 0.001 // From 0.001 to 134217.728, total 28 float number; the 29th is +Inf
	for i := 0; i < SLOW_QUERY_BUCKET_NUM-1; i++ {
		LastSQBInfo[strconv.FormatFloat(bucketBase, 'f', 3, 32)] = 0
		CurrentSQBInfo[strconv.FormatFloat(bucketBase, 'f', 3, 32)] = 0
		bucketBase += bucketBase
	}
	LastSQBInfo["+Inf"] = 0
	CurrentSQBInfo["+Inf"] = 0

	logutil.BgLogger().Info("Telemetry slow query stats initialized", zap.String("CurrentSQBInfo", bucketMap2Json(CurrentSQBInfo)))
	logutil.BgLogger().Info("Telemetry slow query stats initialized", zap.String("LastSQBInfo", bucketMap2Json(LastSQBInfo)))
}

// postReportSlowQueryStats copy CurrentSQBInfo to LastSQBInfo to be ready for next report
// this function is designed for being compatible with preview telemetry
func postReportSlowQueryStats() {
	LastSQBInfo = CurrentSQBInfo
	CurrentSQBInfo = make(SlowQueryBucket)
	logutil.BgLogger().Info("Telemetry slow query stats, postReportSlowQueryStats finished")
}

func bucketMap2Json(paramMap SlowQueryBucket) string {
	dataType, _ := json.Marshal(paramMap)
	return string(dataType)
}
