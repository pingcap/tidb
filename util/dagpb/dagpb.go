package dagpb

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tipb/go-tipb"
)

// SetPBColumnsDefaultValue sets the default values of tipb.ColumnInfos.
func SetPBColumnsDefaultValue(ctx sessionctx.Context, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	for i, c := range columns {
		if c.OriginDefaultValue == nil {
			continue
		}

		sessVars := ctx.GetSessionVars()
		originStrict := sessVars.StrictSQLMode
		sessVars.StrictSQLMode = false
		d, err := table.GetColOriginDefaultValue(ctx, c)
		sessVars.StrictSQLMode = originStrict
		if err != nil {
			return errors.Trace(err)
		}
		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(ctx.GetSessionVars().StmtCtx, d)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// StatementContextToFlags converts StatementContext to tipb.SelectRequest.Flags.
func StatementContextToFlags(sc *stmtctx.StatementContext) uint64 {
	var flags uint64
	if sc.InInsertStmt {
		flags |= model.FlagInInsertStmt
	} else if sc.InUpdateOrDeleteStmt {
		flags |= model.FlagInUpdateOrDeleteStmt
	} else if sc.InSelectStmt {
		flags |= model.FlagInSelectStmt
	}
	if sc.IgnoreTruncate {
		flags |= model.FlagIgnoreTruncate
	} else if sc.TruncateAsWarning {
		flags |= model.FlagTruncateAsWarning
	}
	if sc.OverflowAsWarning {
		flags |= model.FlagOverflowAsWarning
	}
	if sc.IgnoreZeroInDate {
		flags |= model.FlagIgnoreZeroInDate
	}
	if sc.DividedByZeroAsWarning {
		flags |= model.FlagDividedByZeroAsWarning
	}
	if sc.PadCharToFullLength {
		flags |= model.FlagPadCharToFullLength
	}
	return flags
}
