// Copyright 2022 PingCAP, Inc.
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

package connection

import (
	"fmt"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func (c *Connection) logSQL(sql string, duration time.Duration, err error, args ...interface{}) {
	line := fmt.Sprintf("Success: %t, Duration: %s", err == nil, duration)
	for index, arg := range args {
		if index == 0 {
			if affectedRows, ok := arg.(int64); ok {
				line = fmt.Sprintf("%s, Affected Rows: %d", line, affectedRows)
			}
		}
	}
	if err := c.logger.Infof("%s, SQL: %s", line, sql); err != nil {
		log.Fatal("fail to log to file %v", zap.Error(err))
	}
}
