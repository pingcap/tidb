// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// temporaryDBNamePrefix is the prefix name of system db, e.g. mysql system db will be rename to __TiDB_BR_Temporary_mysql
const temporaryDBNamePrefix = "__TiDB_BR_Temporary_"

// NeedAutoID checks whether the table needs backing up with an autoid.
func NeedAutoID(tblInfo *model.TableInfo) bool {
	hasRowID := !tblInfo.PKIsHandle && !tblInfo.IsCommonHandle
	hasAutoIncID := tblInfo.GetAutoIncrementColInfo() != nil
	return hasRowID || hasAutoIncID
}

// EncloseName formats name in sql.
func EncloseName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// EncloseDBAndTable formats the database and table name in sql.
func EncloseDBAndTable(database, table string) string {
	return fmt.Sprintf("%s.%s", EncloseName(database), EncloseName(table))
}

// IsTemplateSysDB checks wheterh the dbname is temporary system database(__TiDB_BR_Temporary_mysql or __TiDB_BR_Temporary_sys).
func IsTemplateSysDB(dbname model.CIStr) bool {
	return dbname.O == temporaryDBNamePrefix+mysql.SystemDB || dbname.O == temporaryDBNamePrefix+mysql.SysDB
}

// IsSysDB tests whether the database is system DB.
// Currently, both `mysql` and `sys` are system DB.
func IsSysDB(dbLowerName string) bool {
	return dbLowerName == mysql.SystemDB || dbLowerName == mysql.SysDB
}

// TemporaryDBName makes a 'private' database name.
func TemporaryDBName(db string) model.CIStr {
	return model.NewCIStr(temporaryDBNamePrefix + db)
}

// GetSysDBName get the original name of system DB
func GetSysDBName(tempDB model.CIStr) (string, bool) {
	if ok := strings.HasPrefix(tempDB.O, temporaryDBNamePrefix); !ok {
		return tempDB.O, false
	}
	return tempDB.O[len(temporaryDBNamePrefix):], true
}

// GetSysDBCIStrName get the CIStr name of system DB
func GetSysDBCIStrName(tempDB model.CIStr) (model.CIStr, bool) {
	if ok := strings.HasPrefix(tempDB.O, temporaryDBNamePrefix); !ok {
		return tempDB, false
	}
	tempDB.O = tempDB.O[len(temporaryDBNamePrefix):]
	tempDB.L = tempDB.L[len(temporaryDBNamePrefix):]
	return tempDB, true
}
