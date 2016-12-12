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

	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/timeutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	cmdLabels = convertCmdLabels()
)

const zeroDuration = time.Duration(0)

// MetricConfig is the metric configuration.
type MetricConfig struct {
	PushJob      string            `toml:"job" json:"job"`
	PushAddress  string            `toml:"address" json:"address"`
	PushInterval timeutil.Duration `toml:"interval" json:"interval"`
}

// GetCmdLabel gets the request command label name for metrics.
func GetCmdLabel(request *pdpb.Request) string {
	name := request.GetCmdType().String()
	label, ok := cmdLabels[name]
	if !ok {
		label = convertName(name)
	}
	return label
}

func convertCmdLabels() map[string]string {
	labels := make(map[string]string)
	for name := range pdpb.CommandType_value {
		labels[name] = convertName(name)
	}
	return labels
}

// convertName converts variable name to a linux type name.
// Like `AbcDef -> abc_def`.
func convertName(str string) string {
	name := make([]byte, 0, 64)
	for i := 0; i < len(str); i++ {
		if str[i] >= 'A' && str[i] <= 'Z' {
			if i > 0 {
				name = append(name, '_')
			}
			name = append(name, str[i]+'a'-'A')
		} else {
			name = append(name, str[i])
		}
	}
	return string(name)
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
