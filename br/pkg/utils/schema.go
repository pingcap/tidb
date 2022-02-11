// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
)

// temporaryDBNamePrefix is the prefix name of system db, e.g. mysql system db will be rename to __TiDB_BR_Temporary_mysql
const temporaryDBNamePrefix = "__TiDB_BR_Temporary_"

// NeedAutoID checks whether the table needs backing up with an autoid.
func NeedAutoID(tblInfo *model.TableInfo) bool {
	hasRowID := !tblInfo.PKIsHandle && !tblInfo.IsCommonHandle
	hasAutoIncID := tblInfo.GetAutoIncrementColInfo() != nil
	return hasRowID || hasAutoIncID
}

// Database wraps the schema and tables of a database.
type Database struct {
	Info   *model.DBInfo
	Tables []*metautil.Table
}

// GetTable returns a table of the database by name.
func (db *Database) GetTable(name string) *metautil.Table {
	for _, table := range db.Tables {
		if table.Info.Name.String() == name {
			return table
		}
	}
	return nil
}

// LoadBackupTables loads schemas from BackupMeta.
func LoadBackupTables(ctx context.Context, reader *metautil.MetaReader) (map[string]*Database, error) {
	ch := make(chan *metautil.Table)
	errCh := make(chan error)
	go func() {
		if err := reader.ReadSchemasFiles(ctx, ch); err != nil {
			errCh <- errors.Trace(err)
		}
		close(ch)
	}()

	databases := make(map[string]*Database)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-errCh:
			return nil, errors.Trace(err)
		case table, ok := <-ch:
			if !ok {
				close(errCh)
				return databases, nil
			}
			dbName := table.DB.Name.String()
			db, ok := databases[dbName]
			if !ok {
				db = &Database{
					Info:   table.DB,
					Tables: make([]*metautil.Table, 0),
				}
				databases[dbName] = db
			}
			db.Tables = append(db.Tables, table)
		}
	}
}

// ArchiveSize returns the total size of the backup archive.
func ArchiveSize(meta *backuppb.BackupMeta) uint64 {
	total := uint64(meta.Size())
	for _, file := range meta.Files {
		total += file.Size_
	}
	return total
}

// EncloseName formats name in sql.
func EncloseName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// EncloseDBAndTable formats the database and table name in sql.
func EncloseDBAndTable(database, table string) string {
	return fmt.Sprintf("%s.%s", EncloseName(database), EncloseName(table))
}

// IsSysDB tests whether the database is system DB.
// Currently, the only system DB is mysql.
func IsSysDB(dbLowerName string) bool {
	return dbLowerName == mysql.SystemDB
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

func UniqueID(schema string, table string) string {
	// QuoteSchema quotes a full table name
	return fmt.Sprintf("`%s`.`%s`", EscapeName(schema), EscapeName(table))
}

// EscapeName replaces all "`" in name with "``"
func EscapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}
