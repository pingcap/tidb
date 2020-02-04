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

package sysschema

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/util"
)

func init() {
	dbID := autoid.SysSchemaDBID
	sysTables := make([]*model.TableInfo, 0, 0)
	dbInfo := &model.DBInfo{
		ID:      dbID,
		Name:    model.NewCIStr(util.SysSchemaName.O),
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Tables:  sysTables,
	}
	infoschema.RegisterVirtualTable(dbInfo, tableFromMeta)
}
