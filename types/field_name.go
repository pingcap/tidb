package types

import (
	"strings"

	"github.com/pingcap/parser/model"
)

// FieldName records the names used for mysql protocol.
type FieldName struct {
	OrigTblName model.CIStr
	OrigColName model.CIStr
	DBName      model.CIStr
	TblName     model.CIStr
	ColName     model.CIStr
}

// String implements Stringer interface.
func (name *FieldName) String() string {
	builder := strings.Builder{}
	if name.TblName.L != "" {
		builder.WriteString(name.TblName.L + ".")
	}
	if name.DBName.L != "" {
		builder.WriteString(name.DBName.L + ".")
	}
	builder.WriteString(name.ColName.L)
	return builder.String()
}
