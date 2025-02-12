// Copyright 2024 PingCAP, Inc.
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

package stream

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func LogDBReplaceMap(title string, dbReplaces map[UpstreamID]*DBReplace) {
	for upstreamDbId, dbReplace := range dbReplaces {
		log.Info(title, func() []zapcore.Field {
			fields := make([]zapcore.Field, 0, (len(dbReplace.TableMap)+1)*3)
			fields = append(fields,
				zap.String("dbName", dbReplace.Name),
				zap.Int64("upstreamId", upstreamDbId),
				zap.Int64("downstreamId", dbReplace.DbID))
			for upstreamTableID, tableReplace := range dbReplace.TableMap {
				fields = append(fields,
					zap.String("table", tableReplace.Name),
					zap.Int64("upstreamId", upstreamTableID),
					zap.Int64("downstreamId", tableReplace.TableID))
				for upPartId, downPartId := range tableReplace.PartitionMap {
					fields = append(fields,
						zap.Int64("up partition", upPartId),
						zap.Int64("down partition", downPartId))
				}
			}
			return fields
		}()...)
	}
}
