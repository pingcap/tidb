package expression

import (
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mock"
)

var distFuncs = map[tipb.ExprType]builtinFunc {
	// compare op
	tipb.ExprType_LT: &builtinCompareSig{op: opcode.LT},
	tipb.ExprType_LE: &builtinCompareSig{op: opcode.LE},
	tipb.ExprType_GT: &builtinCompareSig{op: opcode.GT},
	tipb.ExprType_GE: &builtinCompareSig{op: opcode.GE},
	tipb.ExprType_EQ: &builtinCompareSig{op: opcode.EQ},
	tipb.ExprType_NE: &builtinCompareSig{op: opcode.NE},
	tipb.ExprType_NullEQ: &builtinCompareSig{op: opcode.NullEQ},

	// bit op
	tipb.ExprType_BitAnd: &builtinBitOpSig{op: opcode.And},
	tipb.ExprType_BitOr: &builtinBitOpSig{op: opcode.Or},
	tipb.ExprType_BitXor: &builtinBitOpSig{op: opcode.Xor},
	tipb.ExprType_RighShift: &builtinBitOpSig{op: opcode.RightShift},
	tipb.ExprType_LeftShift: &builtinBitOpSig{op: opcode.LeftShift},
	tipb.ExprType_BitNeg: &builtinUnaryOpSig{op: opcode.BitNeg}, // TODO: uniform it!

	// logical op
	tipb.ExprType_And: &builtinAndAndSig{},
	tipb.ExprType_Or: &builtinOrOrSig{},
	tipb.ExprType_Xor: &builtinLogicXorSig{},
	tipb.ExprType_Not: &builtinUnaryOpSig{op: opcode.Not},

	// arithmetic operator
	tipb.ExprType_Plus: &builtinArithmeticSig{op: opcode.Plus},
	tipb.ExprType_Minus: &builtinArithmeticSig{op: opcode.Minus},
	tipb.ExprType_Mul: &builtinArithmeticSig{op: opcode.Mul},
	tipb.ExprType_Div: &builtinArithmeticSig{op: opcode.Div},
	tipb.ExprType_IntDiv: &builtinArithmeticSig{op: opcode.IntDiv},
	tipb.ExprType_Mod: &builtinArithmeticSig{op: opcode.Mod},

	// control operator
	tipb.ExprType_Case: &builtinCaseWhenSig{},
	tipb.ExprType_If: &builtinIfSig{},
	tipb.ExprType_IfNull: &builtinIfNullSig{},
	tipb.ExprType_NullIf: &builtinNullIfSig{},

	// other operator
	tipb.ExprType_Like: &builtinLikeSig{},
	tipb.ExprType_In: &builtinInSig{},
	tipb.ExprType_IsNull: &builtinIsNullSig{},
	tipb.ExprType_Coalesce: &builtinCoalesceSig{},
}

// NewDistSQLFunction only creates function for mock-tikv.
func NewDistSQLFunction(sc *variable.StatementContext, exprType tipb.ExprType, args []Expression) (*ScalarFunction, error) {
	f, ok := distFuncs[exprType]
	if !ok {
		return nil, errFunctionNotExists.GenByArgs(exprType)
	}
	// TODO: Too ugly...
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = sc
	f.init(args, ctx)
	return &ScalarFunction{Function: f}, nil
}
