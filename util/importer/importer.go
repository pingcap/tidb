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

package importer

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// DoProcess generates data.
func DoProcess(cfg *Config) {
	table := newTable()
	err := parseTableSQL(table, cfg.TableSQL)
	if err != nil {
		log.Fatal("parseTableSQL", zap.String("sql", cfg.TableSQL), zap.Error(err))
	}

	err = parseIndexSQL(table, cfg.IndexSQL)
	if err != nil {
		log.Fatal("parseIndexSQL", zap.Error(err))
	}

	dbs, err := createDBs(cfg.DBCfg, cfg.WorkerCount)
	if err != nil {
		log.Fatal("createDBs", zap.Error(err))
	}
	defer closeDBs(dbs)

	err = execSQL(dbs[0], cfg.TableSQL)
	if err != nil {
		log.Fatal("execSQL", zap.Error(err))
	}

	err = execSQL(dbs[0], cfg.IndexSQL)
	if err != nil {
		log.Fatal("execSQL", zap.Error(err))
	}

	doProcess(table, dbs, cfg.JobCount, cfg.WorkerCount, cfg.Batch)
}
