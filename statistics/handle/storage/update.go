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

package storage

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	statsutil "github.com/pingcap/tidb/statistics/handle/util"
)

// UpdateStatsVersion will set statistics version to the newest TS,
// then tidb-server will reload automatic.
func UpdateStatsVersion(sctx sessionctx.Context) error {
	startTS, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = statsutil.Exec(sctx, "update mysql.stats_meta set version = %?", startTS); err != nil {
		return err
	}
	if _, err = statsutil.Exec(sctx, "update mysql.stats_extended set version = %?", startTS); err != nil {
		return err
	}
	if _, err = statsutil.Exec(sctx, "update mysql.stats_histograms set version = %?", startTS); err != nil {
		return err
	}
	return nil
}
