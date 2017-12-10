// Copyright 2016 PingCAP, Inc.
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

package metricutil

import (
	"time"
	"unicode"

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/pd/pkg/typeutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	cmdLabels = make(map[string]string)
)

const zeroDuration = time.Duration(0)

// MetricConfig is the metric configuration.
type MetricConfig struct {
	PushJob      string            `toml:"job" json:"job"`
	PushAddress  string            `toml:"address" json:"address"`
	PushInterval typeutil.Duration `toml:"interval" json:"interval"`
}

func runesHasLowerNeighborAt(runes []rune, idx int) bool {
	if idx > 0 && unicode.IsLower(runes[idx-1]) {
		return true
	}
	if idx+1 < len(runes) && unicode.IsLower(runes[idx+1]) {
		return true
	}
	return false
}

func camelCaseToSnakeCase(str string) string {
	runes := []rune(str)
	length := len(runes)

	var ret []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) && runesHasLowerNeighborAt(runes, i) {
			ret = append(ret, '_')
		}
		ret = append(ret, unicode.ToLower(runes[i]))
	}

	return string(ret)
}

// prometheusPushClient pushs metrics to Prometheus Pushgateway.
func prometheusPushClient(job, addr string, interval time.Duration) {
	for {
		err := push.FromGatherer(
			job, push.HostnameGroupingKey(),
			addr,
			prometheus.DefaultGatherer,
		)
		if err != nil {
			log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
		}

		time.Sleep(interval)
	}
}

// Push metircs in background.
func Push(cfg *MetricConfig) {
	if cfg.PushInterval.Duration == zeroDuration || len(cfg.PushAddress) == 0 {
		log.Info("disable Prometheus push client")
		return
	}

	log.Info("start Prometheus push client")

	interval := cfg.PushInterval.Duration
	go prometheusPushClient(cfg.PushJob, cfg.PushAddress, interval)
}
