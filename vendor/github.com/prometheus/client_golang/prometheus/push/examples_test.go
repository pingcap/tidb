// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package push_test

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

func ExampleCollectors() {
	completionTime := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "db_backup_last_completion_timestamp_seconds",
		Help: "The timestamp of the last succesful completion of a DB backup.",
	})
	completionTime.Set(float64(time.Now().Unix()))
	if err := push.Collectors(
		"db_backup", push.HostnameGroupingKey(),
		"http://pushgateway:9091",
		completionTime,
	); err != nil {
		fmt.Println("Could not push completion time to Pushgateway:", err)
	}
}

func ExampleRegistry() {
	registry := prometheus.NewRegistry()

	completionTime := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "db_backup_last_completion_timestamp_seconds",
		Help: "The timestamp of the last succesful completion of a DB backup.",
	})
	registry.MustRegister(completionTime)

	completionTime.Set(float64(time.Now().Unix()))
	if err := push.FromGatherer(
		"db_backup", push.HostnameGroupingKey(),
		"http://pushgateway:9091",
		registry,
	); err != nil {
		fmt.Println("Could not push completion time to Pushgateway:", err)
	}
}
