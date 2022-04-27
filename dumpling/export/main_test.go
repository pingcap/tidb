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

package export

import (
	"fmt"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/tidb/dumpling/log"
)

var appLogger log.Logger

func TestMain(m *testing.M) {
	initColTypeRowReceiverMap()

	logger, _, err := log.InitAppLogger(&log.Config{
		Level:  "debug",
		File:   "",
		Format: "text",
	})

	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "fail to init logger: %v\n", err)
		os.Exit(1)
	}

	appLogger = logger
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())
	RegisterMetrics(registry)

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}

	goleak.VerifyTestMain(m, opts...)
}

func defaultConfigForTest(t *testing.T) *Config {
	config := DefaultConfig()
	require.NoError(t, adjustFileFormat(config))
	return config
}
