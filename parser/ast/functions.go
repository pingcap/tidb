// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/types"
)

var (
	_ FuncNode = &AggregateFuncExpr{}
	_ FuncNode = &FuncCallExpr{}
	_ FuncNode = &FuncCastExpr{}
	_ FuncNode = &WindowFuncExpr{}
)

// List scalar function names.
const (
	LogicAnd           = "and"
	Cast               = "cast"
	LeftShift          = "leftshift"
	RightShift         = "rightshift"
	LogicOr            = "or"
	GE                 = "ge"
	LE                 = "le"
	EQ                 = "eq"
	NE                 = "ne"
	LT                 = "lt"
	GT                 = "gt"
	Plus               = "plus"
	Minus              = "minus"
	And                = "bitand"
	Or                 = "bitor"
	Mod                = "mod"
	Xor                = "bitxor"
	Div                = "div"
	Mul                = "mul"
	UnaryNot           = "not" // Avoid name conflict with Not in github/pingcap/check.
	BitNeg             = "bitneg"
	IntDiv             = "intdiv"
	LogicXor           = "xor"
	NullEQ             = "nulleq"
	UnaryPlus          = "unaryplus"
	UnaryMinus         = "unaryminus"
	In                 = "in"
	Like               = "like"
	Case               = "case"
	Regexp             = "regexp"
	IsNull             = "isnull"
	IsTruthWithoutNull = "istrue" // Avoid name conflict with IsTrue in github/pingcap/check.
	IsTruthWithNull    = "istrue_with_null"
	IsFalsity          = "isfalse" // Avoid name conflict with IsFalse in github/pingcap/check.
	RowFunc            = "row"
	SetVar             = "setvar"
	GetVar             = "getvar"
	Values             = "values"
	BitCount           = "bit_count"
	GetParam           = "getparam"

	// common functions
	Coalesce = "coalesce"
	Greatest = "greatest"
	Least    = "least"
	Interval = "interval"

	// math functions
	Abs      = "abs"
	Acos     = "acos"
	Asin     = "asin"
	Atan     = "atan"
	Atan2    = "atan2"
	Ceil     = "ceil"
	Ceiling  = "ceiling"
	Conv     = "conv"
	Cos      = "cos"
	Cot      = "cot"
	CRC32    = "crc32"
	Degrees  = "degrees"
	Exp      = "exp"
	Floor    = "floor"
	Ln       = "ln"
	Log      = "log"
	Log2     = "log2"
	Log10    = "log10"
	PI       = "pi"
	Pow      = "pow"
	Power    = "power"
	Radians  = "radians"
	Rand     = "rand"
	Round    = "round"
	Sign     = "sign"
	Sin      = "sin"
	Sqrt     = "sqrt"
	Tan      = "tan"
	Truncate = "truncate"

	// time functions
	AddDate          = "adddate"
	AddTime          = "addtime"
	ConvertTz        = "convert_tz"
	Curdate          = "curdate"
	CurrentDate      = "current_date"
	CurrentTime      = "current_time"
	CurrentTimestamp = "current_timestamp"
	Curtime          = "curtime"
	Date             = "date"
	DateLiteral      = "'tidb`.(dateliteral"
	DateAdd          = "date_add"
	DateFormat       = "date_format"
	DateSub          = "date_sub"
	DateDiff         = "datediff"
	Day              = "day"
	DayName          = "dayname"
	DayOfMonth       = "dayofmonth"
	DayOfWeek        = "dayofweek"
	DayOfYear        = "dayofyear"
	Extract          = "extract"
	FromDays         = "from_days"
	FromUnixTime     = "from_unixtime"
	GetFormat        = "get_format"
	Hour             = "hour"
	LocalTime        = "localtime"
	LocalTimestamp   = "localtimestamp"
	MakeDate         = "makedate"
	MakeTime         = "maketime"
	MicroSecond      = "microsecond"
	Minute           = "minute"
	Month            = "month"
	MonthName        = "monthname"
	Now              = "now"
	PeriodAdd        = "period_add"
	PeriodDiff       = "period_diff"
	Quarter          = "quarter"
	SecToTime        = "sec_to_time"
	Second           = "second"
	StrToDate        = "str_to_date"
	SubDate          = "subdate"
	SubTime          = "subtime"
	Sysdate          = "sysdate"
	Time             = "time"
	TimeLiteral      = "'tidb`.(timeliteral"
	TimeFormat       = "time_format"
	TimeToSec        = "time_to_sec"
	TimeDiff         = "timediff"
	Timestamp        = "timestamp"
	TimestampLiteral = "'tidb`.(timestampliteral"
	TimestampAdd     = "timestampadd"
	TimestampDiff    = "timestampdiff"
	ToDays           = "to_days"
	ToSeconds        = "to_seconds"
	UnixTimestamp    = "unix_timestamp"
	UTCDate          = "utc_date"
	UTCTime          = "utc_time"
	UTCTimestamp     = "utc_timestamp"
	Week             = "week"
	Weekday          = "weekday"
	WeekOfYear       = "weekofyear"
	Year             = "year"
	YearWeek         = "yearweek"
	LastDay          = "last_day"
	TiDBParseTso     = "tidb_parse_tso"

	// string functions
	ASCII           = "ascii"
	Bin             = "bin"
	Concat          = "concat"
	ConcatWS        = "concat_ws"
	Convert         = "convert"
	Elt             = "elt"
	ExportSet       = "export_set"
	Field           = "field"
	Format          = "format"
	FromBase64      = "from_base64"
	InsertFunc      = "insert_func"
	Instr           = "instr"
	Lcase           = "lcase"
	Left            = "left"
	Length          = "length"
	LoadFile        = "load_file"
	Locate          = "locate"
	Lower           = "lower"
	Lpad            = "lpad"
	LTrim           = "ltrim"
	MakeSet         = "make_set"
	Mid             = "mid"
	Oct             = "oct"
	OctetLength     = "octet_length"
	Ord             = "ord"
	Position        = "position"
	Quote           = "quote"
	Repeat          = "repeat"
	Replace         = "replace"
	Reverse         = "reverse"
	Right           = "right"
	RTrim           = "rtrim"
	Space           = "space"
	Strcmp          = "strcmp"
	Substring       = "substring"
	Substr          = "substr"
	SubstringIndex  = "substring_index"
	ToBase64        = "to_base64"
	Trim            = "trim"
	Upper           = "upper"
	Ucase           = "ucase"
	Hex             = "hex"
	Unhex           = "unhex"
	Rpad            = "rpad"
	BitLength       = "bit_length"
	CharFunc        = "char_func"
	CharLength      = "char_length"
	CharacterLength = "character_length"
	FindInSet       = "find_in_set"
	WeightString    = "weight_string"

	// information functions
	Benchmark      = "benchmark"
	Charset        = "charset"
	Coercibility   = "coercibility"
	Collation      = "collation"
	ConnectionID   = "connection_id"
	CurrentUser    = "current_user"
	CurrentRole    = "current_role"
	Database       = "database"
	FoundRows      = "found_rows"
	LastInsertId   = "last_insert_id"
	RowCount       = "row_count"
	Schema         = "schema"
	SessionUser    = "session_user"
	SystemUser     = "system_user"
	User           = "user"
	Version        = "version"
	TiDBVersion    = "tidb_version"
	TiDBIsDDLOwner = "tidb_is_ddl_owner"
	TiDBDecodePlan = "tidb_decode_plan"
	FormatBytes    = "format_bytes"
	FormatNanoTime = "format_nano_time"

	// control functions
	If     = "if"
	Ifnull = "ifnull"
	Nullif = "nullif"

	// miscellaneous functions
	AnyValue        = "any_value"
	DefaultFunc     = "default_func"
	InetAton        = "inet_aton"
	InetNtoa        = "inet_ntoa"
	Inet6Aton       = "inet6_aton"
	Inet6Ntoa       = "inet6_ntoa"
	IsFreeLock      = "is_free_lock"
	IsIPv4          = "is_ipv4"
	IsIPv4Compat    = "is_ipv4_compat"
	IsIPv4Mapped    = "is_ipv4_mapped"
	IsIPv6          = "is_ipv6"
	IsUsedLock      = "is_used_lock"
	MasterPosWait   = "master_pos_wait"
	NameConst       = "name_const"
	ReleaseAllLocks = "release_all_locks"
	Sleep           = "sleep"
	UUID            = "uuid"
	UUIDShort       = "uuid_short"
	// get_lock() and release_lock() is parsed but do nothing.
	// It is used for preventing error in Ruby's activerecord migrations.
	GetLock     = "get_lock"
	ReleaseLock = "release_lock"

	// encryption and compression functions
	AesDecrypt               = "aes_decrypt"
	AesEncrypt               = "aes_encrypt"
	Compress                 = "compress"
	Decode                   = "decode"
	DesDecrypt               = "des_decrypt"
	DesEncrypt               = "des_encrypt"
	Encode                   = "encode"
	Encrypt                  = "encrypt"
	MD5                      = "md5"
	OldPassword              = "old_password"
	PasswordFunc             = "password_func"
	RandomBytes              = "random_bytes"
	SHA1                     = "sha1"
	SHA                      = "sha"
	SHA2                     = "sha2"
	Uncompress               = "uncompress"
	UncompressedLength       = "uncompressed_length"
	ValidatePasswordStrength = "validate_password_strength"

	// json functions
	JSONType          = "json_type"
	JSONExtract       = "json_extract"
	JSONUnquote       = "json_unquote"
	JSONArray         = "json_array"
	JSONObject        = "json_object"
	JSONMerge         = "json_merge"
	JSONSet           = "json_set"
	JSONInsert        = "json_insert"
	JSONReplace       = "json_replace"
	JSONRemove        = "json_remove"
	JSONContains      = "json_contains"
	JSONContainsPath  = "json_contains_path"
	JSONValid         = "json_valid"
	JSONArrayAppend   = "json_array_append"
	JSONArrayInsert   = "json_array_insert"
	JSONMergePatch    = "json_merge_patch"
	JSONMergePreserve = "json_merge_preserve"
	JSONPretty        = "json_pretty"
	JSONQuote         = "json_quote"
	JSONSearch        = "json_search"
	JSONStorageSize   = "json_storage_size"
	JSONDepth         = "json_depth"
	JSONKeys          = "json_keys"
	JSONLength        = "json_length"

	// TiDB internal function.
	TiDBDecodeKey       = "tidb_decode_key"
	TiDBDecodeBase64Key = "tidb_decode_base64_key"

	// MVCC information fetching function.
	GetMvccInfo = "get_mvcc_info"

	// Sequence function.
	NextVal = "nextval"
	LastVal = "lastval"
	SetVal  = "setval"
)

type FuncCallExprType int8

const (
	FuncCallExprTypeKeyword FuncCallExprType = iota
	FuncCallExprTypeGeneric
)

// FuncCallExpr is for function expression.
type FuncCallExpr struct {
	funcNode
	Tp     FuncCallExprType
	Schema model.CIStr
	// FnName is the function name.
	FnName model.CIStr
	// Args is the function args.
	Args []ExprNode
}

// Restore implements Node interface.
func (n *FuncCallExpr) Restore(ctx *format.RestoreCtx) error {
	var specialLiteral string
	switch n.FnName.L {
	case DateLiteral:
		specialLiteral = "DATE "
	case TimeLiteral:
		specialLiteral = "TIME "
	case TimestampLiteral:
		specialLiteral = "TIMESTAMP "
	}
	if specialLiteral != "" {
		ctx.WritePlain(specialLiteral)
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
		return nil
	}

	if len(n.Schema.String()) != 0 {
		ctx.WriteName(n.Schema.O)
		ctx.WritePlain(".")
	}
	if n.Tp == FuncCallExprTypeGeneric {
		ctx.WriteName(n.FnName.O)
	} else {
		ctx.WriteKeyWord(n.FnName.O)
	}

	ctx.WritePlain("(")
	switch n.FnName.L {
	case "convert":
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
		ctx.WriteKeyWord(" USING ")
		if err := n.Args[1].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
	case "adddate", "subdate", "date_add", "date_sub":
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[0]")
		}
		ctx.WritePlain(", ")
		ctx.WriteKeyWord("INTERVAL ")
		if err := n.Args[1].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[1]")
		}
		ctx.WritePlain(" ")
		if err := n.Args[2].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[2]")
		}
	case "extract":
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[0]")
		}
		ctx.WriteKeyWord(" FROM ")
		if err := n.Args[1].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[1]")
		}
	case "position":
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr")
		}
		ctx.WriteKeyWord(" IN ")
		if err := n.Args[1].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr")
		}
	case "trim":
		switch len(n.Args) {
		case 3:
			if err := n.Args[2].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[2]")
			}
			ctx.WritePlain(" ")
			fallthrough
		case 2:
			if n.Args[1].(ValueExpr).GetValue() != nil {
				if err := n.Args[1].Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[1]")
				}
				ctx.WritePlain(" ")
			}
			ctx.WriteKeyWord("FROM ")
			fallthrough
		case 1:
			if err := n.Args[0].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args[0]")
			}
		}
	case WeightString:
		if err := n.Args[0].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.(WEIGHT_STRING).Args[0]")
		}
		if len(n.Args) == 3 {
			ctx.WriteKeyWord(" AS ")
			ctx.WriteKeyWord(n.Args[1].(ValueExpr).GetValue().(string))
			ctx.WritePlain("(")
			if err := n.Args[2].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.(WEIGHT_STRING).Args[2]")
			}
			ctx.WritePlain(")")
		}
	default:
		for i, argv := range n.Args {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := argv.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args %d", i)
			}
		}
	}
	ctx.WritePlain(")")
	return nil
}

// Format the ExprNode into a Writer.
func (n *FuncCallExpr) Format(w io.Writer) {
	fmt.Fprintf(w, "%s(", n.FnName.L)
	if !n.specialFormatArgs(w) {
		for i, arg := range n.Args {
			arg.Format(w)
			if i != len(n.Args)-1 {
				fmt.Fprint(w, ", ")
			}
		}
	}
	fmt.Fprint(w, ")")
}

// specialFormatArgs formats argument list for some special functions.
func (n *FuncCallExpr) specialFormatArgs(w io.Writer) bool {
	switch n.FnName.L {
	case DateAdd, DateSub, AddDate, SubDate:
		n.Args[0].Format(w)
		fmt.Fprint(w, ", INTERVAL ")
		n.Args[1].Format(w)
		fmt.Fprint(w, " ")
		n.Args[2].Format(w)
		return true
	}
	return false
}

// Accept implements Node interface.
func (n *FuncCallExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncCallExpr)
	for i, val := range n.Args {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Args[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// CastFunctionType is the type for cast function.
type CastFunctionType int

// CastFunction types
const (
	CastFunction CastFunctionType = iota + 1
	CastConvertFunction
	CastBinaryOperator
)

// FuncCastExpr is the cast function converting value to another type, e.g, cast(expr AS signed).
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html
type FuncCastExpr struct {
	funcNode
	// Expr is the expression to be converted.
	Expr ExprNode
	// Tp is the conversion type.
	Tp *types.FieldType
	// FunctionType is either Cast, Convert or Binary.
	FunctionType CastFunctionType
	// ExplicitCharSet is true when charset is explicit indicated.
	ExplicitCharSet bool
}

// Restore implements Node interface.
func (n *FuncCastExpr) Restore(ctx *format.RestoreCtx) error {
	switch n.FunctionType {
	case CastFunction:
		ctx.WriteKeyWord("CAST")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
		ctx.WriteKeyWord(" AS ")
		n.Tp.RestoreAsCastType(ctx, n.ExplicitCharSet)
		ctx.WritePlain(")")
	case CastConvertFunction:
		ctx.WriteKeyWord("CONVERT")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
		ctx.WritePlain(", ")
		n.Tp.RestoreAsCastType(ctx, n.ExplicitCharSet)
		ctx.WritePlain(")")
	case CastBinaryOperator:
		ctx.WriteKeyWord("BINARY ")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
	}
	return nil
}

// Format the ExprNode into a Writer.
func (n *FuncCastExpr) Format(w io.Writer) {
	switch n.FunctionType {
	case CastFunction:
		fmt.Fprint(w, "CAST(")
		n.Expr.Format(w)
		fmt.Fprint(w, " AS ")
		n.Tp.FormatAsCastType(w, n.ExplicitCharSet)
		fmt.Fprint(w, ")")
	case CastConvertFunction:
		fmt.Fprint(w, "CONVERT(")
		n.Expr.Format(w)
		fmt.Fprint(w, ", ")
		n.Tp.FormatAsCastType(w, n.ExplicitCharSet)
		fmt.Fprint(w, ")")
	case CastBinaryOperator:
		fmt.Fprint(w, "BINARY ")
		n.Expr.Format(w)
	}
}

// Accept implements Node Accept interface.
func (n *FuncCastExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncCastExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// TrimDirectionType is the type for trim direction.
type TrimDirectionType int

const (
	// TrimBothDefault trims from both direction by default.
	TrimBothDefault TrimDirectionType = iota
	// TrimBoth trims from both direction with explicit notation.
	TrimBoth
	// TrimLeading trims from left.
	TrimLeading
	// TrimTrailing trims from right.
	TrimTrailing
)

// String implements fmt.Stringer interface.
func (direction TrimDirectionType) String() string {
	switch direction {
	case TrimBoth, TrimBothDefault:
		return "BOTH"
	case TrimLeading:
		return "LEADING"
	case TrimTrailing:
		return "TRAILING"
	default:
		return ""
	}
}

// TrimDirectionExpr is an expression representing the trim direction used in the TRIM() function.
type TrimDirectionExpr struct {
	exprNode
	// Direction is the trim direction
	Direction TrimDirectionType
}

// Restore implements Node interface.
func (n *TrimDirectionExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(n.Direction.String())
	return nil
}

// Format the ExprNode into a Writer.
func (n *TrimDirectionExpr) Format(w io.Writer) {
	fmt.Fprint(w, n.Direction.String())
}

// Accept implements Node Accept interface.
func (n *TrimDirectionExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(n)
}

// DateArithType is type for DateArith type.
type DateArithType byte

const (
	// DateArithAdd is to run adddate or date_add function option.
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-add
	DateArithAdd DateArithType = iota + 1
	// DateArithSub is to run subdate or date_sub function option.
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-sub
	DateArithSub
)

const (
	// AggFuncCount is the name of Count function.
	AggFuncCount = "count"
	// AggFuncSum is the name of Sum function.
	AggFuncSum = "sum"
	// AggFuncAvg is the name of Avg function.
	AggFuncAvg = "avg"
	// AggFuncFirstRow is the name of FirstRowColumn function.
	AggFuncFirstRow = "firstrow"
	// AggFuncMax is the name of max function.
	AggFuncMax = "max"
	// AggFuncMin is the name of min function.
	AggFuncMin = "min"
	// AggFuncGroupConcat is the name of group_concat function.
	AggFuncGroupConcat = "group_concat"
	// AggFuncBitOr is the name of bit_or function.
	AggFuncBitOr = "bit_or"
	// AggFuncBitXor is the name of bit_xor function.
	AggFuncBitXor = "bit_xor"
	// AggFuncBitAnd is the name of bit_and function.
	AggFuncBitAnd = "bit_and"
	// AggFuncVarPop is the name of var_pop function
	AggFuncVarPop = "var_pop"
	// AggFuncVarSamp is the name of var_samp function
	AggFuncVarSamp = "var_samp"
	// AggFuncStddevPop is the name of stddev_pop/std/stddev function
	AggFuncStddevPop = "stddev_pop"
	// AggFuncStddevSamp is the name of stddev_samp function
	AggFuncStddevSamp = "stddev_samp"
	// AggFuncJsonObjectAgg is the name of json_objectagg function
	AggFuncJsonObjectAgg = "json_objectagg"
	// AggFuncApproxCountDistinct is the name of approx_count_distinct function.
	AggFuncApproxCountDistinct = "approx_count_distinct"
	// AggFuncApproxPercentile is the name of approx_percentile function.
	AggFuncApproxPercentile = "approx_percentile"
)

// AggregateFuncExpr represents aggregate function expression.
type AggregateFuncExpr struct {
	funcNode
	// F is the function name.
	F string
	// Args is the function args.
	Args []ExprNode
	// Distinct is true, function hence only aggregate distinct values.
	// For example, column c1 values are "1", "2", "2",  "sum(c1)" is "5",
	// but "sum(distinct c1)" is "3".
	Distinct bool
	// Order is only used in GROUP_CONCAT
	Order *OrderByClause
}

// Restore implements Node interface.
func (n *AggregateFuncExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(n.F)
	ctx.WritePlain("(")
	if n.Distinct {
		ctx.WriteKeyWord("DISTINCT ")
	}
	switch strings.ToLower(n.F) {
	case "group_concat":
		for i := 0; i < len(n.Args)-1; i++ {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := n.Args[i].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AggregateFuncExpr.Args[%d]", i)
			}
		}
		if n.Order != nil {
			ctx.WritePlain(" ")
			if err := n.Order.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occur while restore AggregateFuncExpr.Args Order")
			}
		}
		ctx.WriteKeyWord(" SEPARATOR ")
		if err := n.Args[len(n.Args)-1].Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AggregateFuncExpr.Args SEPARATOR")
		}
	default:
		for i, argv := range n.Args {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := argv.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AggregateFuncExpr.Args[%d]", i)
			}
		}
	}
	ctx.WritePlain(")")
	return nil
}

// Format the ExprNode into a Writer.
func (n *AggregateFuncExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *AggregateFuncExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AggregateFuncExpr)
	for i, val := range n.Args {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Args[i] = node.(ExprNode)
	}
	if n.Order != nil {
		node, ok := n.Order.Accept(v)
		if !ok {
			return n, false
		}
		n.Order = node.(*OrderByClause)
	}
	return v.Leave(n)
}

const (
	// WindowFuncRowNumber is the name of row_number function.
	WindowFuncRowNumber = "row_number"
	// WindowFuncRank is the name of rank function.
	WindowFuncRank = "rank"
	// WindowFuncDenseRank is the name of dense_rank function.
	WindowFuncDenseRank = "dense_rank"
	// WindowFuncCumeDist is the name of cume_dist function.
	WindowFuncCumeDist = "cume_dist"
	// WindowFuncPercentRank is the name of percent_rank function.
	WindowFuncPercentRank = "percent_rank"
	// WindowFuncNtile is the name of ntile function.
	WindowFuncNtile = "ntile"
	// WindowFuncLead is the name of lead function.
	WindowFuncLead = "lead"
	// WindowFuncLag is the name of lag function.
	WindowFuncLag = "lag"
	// WindowFuncFirstValue is the name of first_value function.
	WindowFuncFirstValue = "first_value"
	// WindowFuncLastValue is the name of last_value function.
	WindowFuncLastValue = "last_value"
	// WindowFuncNthValue is the name of nth_value function.
	WindowFuncNthValue = "nth_value"
)

// WindowFuncExpr represents window function expression.
type WindowFuncExpr struct {
	funcNode

	// F is the function name.
	F string
	// Args is the function args.
	Args []ExprNode
	// Distinct cannot be true for most window functions, except `max` and `min`.
	// We need to raise error if it is not allowed to be true.
	Distinct bool
	// IgnoreNull indicates how to handle null value.
	// MySQL only supports `RESPECT NULLS`, so we need to raise error if it is true.
	IgnoreNull bool
	// FromLast indicates the calculation direction of this window function.
	// MySQL only supports calculation from first, so we need to raise error if it is true.
	FromLast bool
	// Spec is the specification of this window.
	Spec WindowSpec
}

// Restore implements Node interface.
func (n *WindowFuncExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(n.F)
	ctx.WritePlain("(")
	for i, v := range n.Args {
		if i != 0 {
			ctx.WritePlain(", ")
		} else if n.Distinct {
			ctx.WriteKeyWord("DISTINCT ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore WindowFuncExpr.Args[%d]", i)
		}
	}
	ctx.WritePlain(")")
	if n.FromLast {
		ctx.WriteKeyWord(" FROM LAST")
	}
	if n.IgnoreNull {
		ctx.WriteKeyWord(" IGNORE NULLS")
	}
	ctx.WriteKeyWord(" OVER ")
	if err := n.Spec.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore WindowFuncExpr.Spec")
	}

	return nil
}

// Format formats the window function expression into a Writer.
func (n *WindowFuncExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *WindowFuncExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*WindowFuncExpr)
	for i, val := range n.Args {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Args[i] = node.(ExprNode)
	}
	node, ok := n.Spec.Accept(v)
	if !ok {
		return n, false
	}
	n.Spec = *node.(*WindowSpec)
	return v.Leave(n)
}

// TimeUnitType is the type for time and timestamp units.
type TimeUnitType int

const (
	// TimeUnitInvalid is a placeholder for an invalid time or timestamp unit
	TimeUnitInvalid TimeUnitType = iota
	// TimeUnitMicrosecond is the time or timestamp unit MICROSECOND.
	TimeUnitMicrosecond
	// TimeUnitSecond is the time or timestamp unit SECOND.
	TimeUnitSecond
	// TimeUnitMinute is the time or timestamp unit MINUTE.
	TimeUnitMinute
	// TimeUnitHour is the time or timestamp unit HOUR.
	TimeUnitHour
	// TimeUnitDay is the time or timestamp unit DAY.
	TimeUnitDay
	// TimeUnitWeek is the time or timestamp unit WEEK.
	TimeUnitWeek
	// TimeUnitMonth is the time or timestamp unit MONTH.
	TimeUnitMonth
	// TimeUnitQuarter is the time or timestamp unit QUARTER.
	TimeUnitQuarter
	// TimeUnitYear is the time or timestamp unit YEAR.
	TimeUnitYear
	// TimeUnitSecondMicrosecond is the time unit SECOND_MICROSECOND.
	TimeUnitSecondMicrosecond
	// TimeUnitMinuteMicrosecond is the time unit MINUTE_MICROSECOND.
	TimeUnitMinuteMicrosecond
	// TimeUnitMinuteSecond is the time unit MINUTE_SECOND.
	TimeUnitMinuteSecond
	// TimeUnitHourMicrosecond is the time unit HOUR_MICROSECOND.
	TimeUnitHourMicrosecond
	// TimeUnitHourSecond is the time unit HOUR_SECOND.
	TimeUnitHourSecond
	// TimeUnitHourMinute is the time unit HOUR_MINUTE.
	TimeUnitHourMinute
	// TimeUnitDayMicrosecond is the time unit DAY_MICROSECOND.
	TimeUnitDayMicrosecond
	// TimeUnitDaySecond is the time unit DAY_SECOND.
	TimeUnitDaySecond
	// TimeUnitDayMinute is the time unit DAY_MINUTE.
	TimeUnitDayMinute
	// TimeUnitDayHour is the time unit DAY_HOUR.
	TimeUnitDayHour
	// TimeUnitYearMonth is the time unit YEAR_MONTH.
	TimeUnitYearMonth
)

// String implements fmt.Stringer interface.
func (unit TimeUnitType) String() string {
	switch unit {
	case TimeUnitMicrosecond:
		return "MICROSECOND"
	case TimeUnitSecond:
		return "SECOND"
	case TimeUnitMinute:
		return "MINUTE"
	case TimeUnitHour:
		return "HOUR"
	case TimeUnitDay:
		return "DAY"
	case TimeUnitWeek:
		return "WEEK"
	case TimeUnitMonth:
		return "MONTH"
	case TimeUnitQuarter:
		return "QUARTER"
	case TimeUnitYear:
		return "YEAR"
	case TimeUnitSecondMicrosecond:
		return "SECOND_MICROSECOND"
	case TimeUnitMinuteMicrosecond:
		return "MINUTE_MICROSECOND"
	case TimeUnitMinuteSecond:
		return "MINUTE_SECOND"
	case TimeUnitHourMicrosecond:
		return "HOUR_MICROSECOND"
	case TimeUnitHourSecond:
		return "HOUR_SECOND"
	case TimeUnitHourMinute:
		return "HOUR_MINUTE"
	case TimeUnitDayMicrosecond:
		return "DAY_MICROSECOND"
	case TimeUnitDaySecond:
		return "DAY_SECOND"
	case TimeUnitDayMinute:
		return "DAY_MINUTE"
	case TimeUnitDayHour:
		return "DAY_HOUR"
	case TimeUnitYearMonth:
		return "YEAR_MONTH"
	default:
		return ""
	}
}

// Duration represented by this unit.
// Returns error if the time unit is not a fixed time interval (such as MONTH)
// or a composite unit (such as MINUTE_SECOND).
func (unit TimeUnitType) Duration() (time.Duration, error) {
	switch unit {
	case TimeUnitMicrosecond:
		return time.Microsecond, nil
	case TimeUnitSecond:
		return time.Second, nil
	case TimeUnitMinute:
		return time.Minute, nil
	case TimeUnitHour:
		return time.Hour, nil
	case TimeUnitDay:
		return time.Hour * 24, nil
	case TimeUnitWeek:
		return time.Hour * 24 * 7, nil
	case TimeUnitMonth, TimeUnitQuarter, TimeUnitYear:
		return 0, errors.Errorf("%s is not a constant time interval and cannot be used here", unit)
	default:
		return 0, errors.Errorf("%s is a composite time unit and is not supported yet", unit)
	}
}

// TimeUnitExpr is an expression representing a time or timestamp unit.
type TimeUnitExpr struct {
	exprNode
	// Unit is the time or timestamp unit.
	Unit TimeUnitType
}

// Restore implements Node interface.
func (n *TimeUnitExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(n.Unit.String())
	return nil
}

// Format the ExprNode into a Writer.
func (n *TimeUnitExpr) Format(w io.Writer) {
	fmt.Fprint(w, n.Unit.String())
}

// Accept implements Node Accept interface.
func (n *TimeUnitExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(n)
}

// GetFormatSelectorType is the type for the first argument of GET_FORMAT() function.
type GetFormatSelectorType int

const (
	// GetFormatSelectorDate is the GET_FORMAT selector DATE.
	GetFormatSelectorDate GetFormatSelectorType = iota + 1
	// GetFormatSelectorTime is the GET_FORMAT selector TIME.
	GetFormatSelectorTime
	// GetFormatSelectorDatetime is the GET_FORMAT selector DATETIME and TIMESTAMP.
	GetFormatSelectorDatetime
)

// GetFormatSelectorExpr is an expression used as the first argument of GET_FORMAT() function.
type GetFormatSelectorExpr struct {
	exprNode
	// Selector is the GET_FORMAT() selector.
	Selector GetFormatSelectorType
}

// String implements fmt.Stringer interface.
func (selector GetFormatSelectorType) String() string {
	switch selector {
	case GetFormatSelectorDate:
		return "DATE"
	case GetFormatSelectorTime:
		return "TIME"
	case GetFormatSelectorDatetime:
		return "DATETIME"
	default:
		return ""
	}
}

// Restore implements Node interface.
func (n *GetFormatSelectorExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(n.Selector.String())
	return nil
}

// Format the ExprNode into a Writer.
func (n *GetFormatSelectorExpr) Format(w io.Writer) {
	fmt.Fprint(w, n.Selector.String())
}

// Accept implements Node Accept interface.
func (n *GetFormatSelectorExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(n)
}
