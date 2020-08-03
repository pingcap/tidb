// Copyright 2020 PingCAP, Inc.
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
	"log"
	"os"

	_ "github.com/pingcap/tidb/ddl"
	_ "github.com/pingcap/tidb/domain"
	terror "github.com/pingcap/tidb/errno"
	_ "github.com/pingcap/tidb/executor"
	_ "github.com/pingcap/tidb/expression"
	_ "github.com/pingcap/tidb/infoschema"
	_ "github.com/pingcap/tidb/kv"
	_ "github.com/pingcap/tidb/meta"
	_ "github.com/pingcap/tidb/meta/autoid"
	_ "github.com/pingcap/tidb/planner/core"
	_ "github.com/pingcap/tidb/plugin"
	_ "github.com/pingcap/tidb/privilege/privileges"
	_ "github.com/pingcap/tidb/server"
	_ "github.com/pingcap/tidb/session"
	_ "github.com/pingcap/tidb/sessionctx/variable"
	_ "github.com/pingcap/tidb/store/tikv"
	_ "github.com/pingcap/tidb/structure"
	_ "github.com/pingcap/tidb/table"
	_ "github.com/pingcap/tidb/tablecodec"
	_ "github.com/pingcap/tidb/types"
	_ "github.com/pingcap/tidb/types/json"
	_ "github.com/pingcap/tidb/util/admin"
	_ "github.com/pingcap/tidb/util/memory"
)

func main() {
	f, err := os.Create("error.toml")
	if err != nil {
		log.Fatal(err)
	}
	err = terror.RegDB.ExportTo(f)
	if err != nil {
		log.Fatal(err)
	}
}
