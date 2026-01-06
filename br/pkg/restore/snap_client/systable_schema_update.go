// Copyright 2026 PingCAP, Inc.
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

package snapclient

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
)

type SchemaVersionType int

const (
	InvalidVersion SchemaVersionType = iota
	Version1
	Version2
)

var (
	upgradeSQLs = map[string]map[string][]string{
		"mysql": {
			"stats_meta": {
				// invalid version -> version 1
				"",
				// version 1 -> version 2
				"ALTER TABLE __TiDB_BR_Temporary_mysql.stats_meta ADD COLUMN IF NOT EXISTS last_stats_histograms_version bigint unsigned DEFAULT NULL",
			},
		},
	}

	downgradeSQLs = map[string]map[string][]string{
		"mysql": {
			"stats_meta": {
				// invalid version <- version 1
				"",
				// version 1 <- version 2
				"ALTER TABLE __TiDB_BR_Temporary_mysql.stats_meta DROP COLUMN IF EXISTS last_stats_histograms_version",
			},
		},
	}
)

var (
	updateStatsMetaSchemaFunctionMap = map[string]map[string]func(context.Context, *model.TableInfo, *model.TableInfo, func(context.Context, string) error) error{
		"mysql": {
			"stats_meta": updateStatsMetaSchema,
		},
	}
)

func getSchemaVersionFromStatsMeta(tableInfo *model.TableInfo) SchemaVersionType {
	version := Version1
	// version 1 -> version 2: add last_stats_histograms_version column
	for _, columnInfo := range tableInfo.Columns {
		if columnInfo.Name.L == "last_stats_histograms_version" {
			version = Version2
			break
		}
	}
	return version
}

func updateStatsMetaSchema(
	ctx context.Context,
	downstreamTableInfo, upstreamTableInfo *model.TableInfo,
	execution func(context.Context, string) error,
) error {
	downstreamVersion := getSchemaVersionFromStatsMeta(downstreamTableInfo)
	upstreamVersion := getSchemaVersionFromStatsMeta(upstreamTableInfo)
	if downstreamVersion == InvalidVersion || upstreamVersion == InvalidVersion {
		return errors.Errorf("invalid stats meta schema")
	}
	if downstreamVersion == upstreamVersion {
		return nil
	}
	if downstreamVersion < upstreamVersion {
		for ver := upstreamVersion - 1; ver >= downstreamVersion; ver-- {
			if err := execution(ctx, downgradeSQLs["mysql"][downstreamTableInfo.Name.L][ver]); err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		for ver := upstreamVersion; ver < downstreamVersion; ver++ {
			if err := execution(ctx, upgradeSQLs["mysql"][downstreamTableInfo.Name.L][ver]); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}
