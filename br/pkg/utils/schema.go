// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
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

// UnquoteName removes the backticks from a name.
func UnquoteName(name string) string {
	if len(name) >= 2 && name[0] == '`' && name[len(name)-1] == '`' {
		name = name[1 : len(name)-1]
	}
	return strings.ReplaceAll(name, "``", "`")
}

// EncloseDBAndTable formats the database and table name in sql.
func EncloseDBAndTable(database, table string) string {
	return fmt.Sprintf("%s.%s", EncloseName(database), EncloseName(table))
}

// IsTemplateSysDB checks wheterh the dbname is temporary system database(__TiDB_BR_Temporary_mysql or __TiDB_BR_Temporary_sys).
func IsTemplateSysDB(dbname pmodel.CIStr) bool {
	return dbname.O == temporaryDBNamePrefix+mysql.SystemDB || dbname.O == temporaryDBNamePrefix+mysql.SysDB
}

// IsSysDB tests whether the database is system DB.
// Currently, both `mysql` and `sys` are system DB.
func IsSysDB(dbName string) bool {
	// just in case
	dbLowerName := strings.ToLower(dbName)
	return dbLowerName == mysql.SystemDB || dbLowerName == mysql.SysDB
}

// TemporaryDBName makes a 'private' database name.
func TemporaryDBName(db string) pmodel.CIStr {
	return pmodel.NewCIStr(temporaryDBNamePrefix + db)
}

// StripTempDBPrefixIfNeeded get the original name of system DB
func StripTempDBPrefixIfNeeded(tempDB string) string {
	if ok := strings.HasPrefix(tempDB, temporaryDBNamePrefix); !ok {
		return tempDB
	}
	return tempDB[len(temporaryDBNamePrefix):]
}

// StripTempDBPrefix get the original name of temporary system DB
func StripTempDBPrefix(tempDB string) (string, bool) {
	if ok := strings.HasPrefix(tempDB, temporaryDBNamePrefix); !ok {
		return tempDB, false
	}
	return tempDB[len(temporaryDBNamePrefix):], true
}

// IsSysOrTempSysDB tests whether the database is system DB or prefixed with temp.
func IsSysOrTempSysDB(db string) bool {
	db = StripTempDBPrefixIfNeeded(db)
	return IsSysDB(db)
}

// GetSysDBCIStrName get the CIStr name of system DB
func GetSysDBCIStrName(tempDB pmodel.CIStr) (pmodel.CIStr, bool) {
	if ok := strings.HasPrefix(tempDB.O, temporaryDBNamePrefix); !ok {
		return tempDB, false
	}
	tempDB.O = tempDB.O[len(temporaryDBNamePrefix):]
	tempDB.L = tempDB.L[len(temporaryDBNamePrefix):]
	return tempDB, true
}
