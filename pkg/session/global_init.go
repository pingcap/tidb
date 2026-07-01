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

package session

import (
	"context"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/timeutil"
)

type systemDBFilter struct{}

func (systemDBFilter) SkipLoadDiff(*model.SchemaDiff, infoschema.InfoSchema) bool {
	// when initialize global var, we don't start the domain, so this method
	// will NOT be called, ok to return false here
	return false
}

func (systemDBFilter) SkipLoadSchema(dbInfo *model.DBInfo) bool {
	return !metadef.IsSystemDB(dbInfo.Name.L)
}

// we have to use a separate Domain to init the global variables, and then
// close it. as we are using a captured new_collate setting inside TableCommon
// now, otherwise, the captured new_collate setting in TableCommon is still
// cached in the Domain schema cache and is invalid.
func initGlobalVarFromSystemDB(ctx context.Context, store kv.Storage) error {
	// the session used to load those global vars depends on the global vars
	// themselves, which is a cyclic dependency. but luckily, mysql.tidb is
	// created with DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin, it's safe to
	// decode below rows without the correct TidbNewCollationEnabled initialized.
	//
	// Note: collate.newCollationEnabled is set to 1 in init(), so if the
	// new_collate=false during first bootstrap, they mismatch.
	dom, err := domap.GetOrCreateWithFilter(store, systemDBFilter{})
	if err != nil {
		return err
	}
	sess, err := createSessionWithOpt(store, dom, dom.GetSchemaValidator(), dom.InfoCache(), nil)
	if err != nil {
		return err
	}

	// get system tz from mysql.tidb
	tz, err := sess.getTableValue(ctx, mysql.TiDBTable, tidbSystemTZ)
	if err != nil {
		return err
	}
	timeutil.SetSystemTZ(tz)

	// get the flag from `mysql`.`tidb` which indicating if new collations are enabled.
	newCollationEnabled, err := loadCollationParameter(ctx, sess)
	if err != nil {
		return err
	}
	collate.SetNewCollationEnabledForTest(newCollationEnabled)

	dom.Close()
	return nil
}
