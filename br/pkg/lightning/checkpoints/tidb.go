// Copyright 2019 PingCAP, Inc.
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

package checkpoints

import (
	"github.com/pingcap/tidb/pkg/parser/model"
)

// TidbDBInfo is the database info in TiDB.
type TidbDBInfo struct {
	ID     int64
	Name   string
	Tables map[string]*TidbTableInfo
}

// TidbTableInfo is the table info in TiDB.
type TidbTableInfo struct {
	ID   int64
	DB   string
	Name string
	// Core is the current table info in TiDB.
	Core *model.TableInfo
	// Desired is the table info that we want to migrate to. In most cases, it is the same as Core. But
	// when we want to import index and data separately, we should first drop indices and then import data,
	// so the Core will be the table info without indices, but the Desired will be the table info with indices.
	Desired *model.TableInfo
}
