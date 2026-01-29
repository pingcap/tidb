package materializedview

import (
	mysql "github.com/pingcap/tidb/pkg/errno"
	parser_mysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

var (
	ErrMVQueryMustIncludeCountStar = dbterror.ClassDDL.NewStdErr(mysql.ErrNotSupportedYet, parser_mysql.Message("materialized view query must explicitly include count(*)/count(1)", nil))
	ErrMVQueryOnlySupportsCountStar = dbterror.ClassDDL.NewStdErr(mysql.ErrNotSupportedYet, parser_mysql.Message("materialized view query only supports count(*)/count(1)", nil))
	ErrMVQueryColumnNotInLog       = dbterror.ClassDDL.NewStdErr(mysql.ErrNotSupportedYet, parser_mysql.Message("materialized view query uses column %-.64s which is not included in materialized view log", nil))
	ErrMVBaseTableHasNoLog         = dbterror.ClassDDL.NewStdErr(mysql.ErrNotSupportedYet, parser_mysql.Message("materialized view base table does not have a materialized view log", nil))
)

