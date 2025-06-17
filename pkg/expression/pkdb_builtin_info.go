package expression

import "github.com/pingcap/tidb/pkg/util/printer"

// may be defined by tidbx,default not in tidbx mode
var (
	VersionPrinter = printer.GetTiDBInfo()
)
