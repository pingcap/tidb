// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3store

import (
	"fmt"

	"github.com/aws/smithy-go/logging"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type pingcapLogger struct {
	logger *zap.Logger
}

func newLogger(extraFields ...zap.Field) pingcapLogger {
	return pingcapLogger{
		logger: log.L().WithOptions(zap.AddCallerSkip(1)).With(extraFields...),
	}
}

func (p pingcapLogger) Logf(classification logging.Classification, format string, v ...any) {
	var loggerF func(string, ...zap.Field)
	switch classification {
	case logging.Warn:
		loggerF = p.logger.Warn
	case logging.Debug:
		loggerF = p.logger.Debug
	default:
		loggerF = p.logger.Info
	}

	msg := fmt.Sprintf(format, v...)
	loggerF(msg)
}
