// Copyright 2018 PingCAP, Inc.
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

package main

import (
	"encoding/json"
	"io/ioutil"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
	stats "github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/mock"
)

func loadStats(tblInfo *model.TableInfo, path string) (*stats.Table, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jsTable := &stats.JSONTable{}
	err = json.Unmarshal(data, jsTable)
	if err != nil {
		return nil, errors.Trace(err)
	}
	handle := stats.NewHandle(mock.NewContext(), 0)
	return handle.LoadStatsFromJSON(tblInfo, jsTable)
}
