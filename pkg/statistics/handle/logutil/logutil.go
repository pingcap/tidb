// Copyright 2023 PingCAP, Inc.
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

package logutil

import (
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// StatsLogger with category "stats" is used to log statistic related messages.
// Do not use it to log the message that is not related to statistics.
func StatsLogger() *zap.Logger {
	return logutil.BgLogger().With(zap.String("category", "stats"))
}

var (
	sampleLoggerFactory = logutil.SampleLoggerFactory(5*time.Minute, 1, zap.String(logutil.LogFieldCategory, "stats"))
)

// SingletonStatsSamplerLogger with category "stats" is used to log statistic related messages.
// It is used to sample the log to avoid too many logs.
// Do not use it to log the message that is not related to statistics.
func SingletonStatsSamplerLogger() *zap.Logger {
	return sampleLoggerFactory()
}
