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
// See the License for the specific language governing permissions and
// limitations under the License.

package hparser

// IsReserved returns true if the token type corresponds to a reserved keyword.
// This list MUST be kept in sync with pkg/parser/keywords.go.
func IsReserved(tp int) bool {
	switch tp {
	case tokAdd, tokAll, tokAlter, tokAnalyze, tokAnd, tokArray, tokAs, tokAsc,
		tokBetween, tokBigInt, tokBinary, tokBlob, tokBoth, tokBy,
		tokCall, tokCascade, tokCase, tokChange, tokChar, tokCharacter, tokCheck, tokCollate, tokColumn, tokConstraint, tokContinue, tokConvert, tokCreate, tokCross, tokCumeDist, tokCurrentDate, tokCurrentRole, tokCurrentTime, tokCurrentTs, tokCurrentUser, tokCursor,
		tokDatabase, tokDatabases, tokDayHour, tokDayMicrosecond, tokDayMinute, tokDaySecond, tokDecimal, tokDefault, tokDelayed, tokDelete, tokDenseRank, tokDesc, tokDescribe, tokDistinct, tokDistinctRow, tokDiv, tokDouble, tokDrop, tokDual,
		tokElse, tokElseIf, tokEnclosed, tokEscaped, tokExcept, tokExists, tokExit, tokExplain,
		tokFalse, tokFetch, tokFirstValue, tokFloat, tokFloat4, tokFloat8, tokFor, tokForce, tokForeign, tokFrom, tokFull, tokFulltext,
		tokGenerated, tokGrant, tokGroup, tokGroups,
		tokHaving, tokHighPriority, tokHourMicrosecond, tokHourMinute, tokHourSecond,
		tokIf, tokIgnore, tokILike, tokIn, tokIndex, tokInfile, tokInner, tokInOut, tokInsert, tokInt, tokInt1, tokInt2, tokInt3, tokInt4, tokInt8, tokInteger, tokIntersect, tokInterval, tokInto, tokIs, tokIterate,
		tokJoin,
		tokKey, tokKeys, tokKill,
		tokLag, tokLastValue, tokLead, tokLeading, tokLeave, tokLeft, tokLike, tokLimit, tokLinear, tokLines, tokLoad, tokLocalTime, tokLocalTs, tokLock, tokLong, tokLongBlob, tokLongText, tokLowPriority,
		tokMatch, tokMaxValue, tokMediumBlob, tokMediumInt, tokMediumText, tokMiddleInt, tokMinuteMicrosecond, tokMinuteSecond, tokMod,
		tokNatural, tokNot, tokNoWriteToBinLog, tokNthValue, tokNtile, tokNull, tokNumeric,
		tokOf, tokOn, tokOptimize, tokOption, tokOptionally, tokOr, tokOrder, tokOut, tokOuter, tokOutfile, tokOver,
		tokPartition, tokPercentRank, tokPrecision, tokPrimary, tokProcedure,
		tokRange, tokRank, tokRead, tokReal, tokRecursive, tokReferences, tokRegexp, tokRelease, tokRename, tokRepeat, tokReplace, tokRequire, tokRestrict, tokRevoke, tokRight, tokRLike, tokRow, tokRows, tokRowNumber,
		tokSecondMicrosecond, tokSelect, tokSet, tokShow, tokSmallInt, tokSpatial, tokSQL, tokSQLException, tokSQLState, tokSQLWarning, tokSQLBigResult, tokSQLCalcFoundRows, tokSQLSmallResult, tokSSL, tokStarting, tokStored, tokStraightJoin,
		tokTable, tokTableSample, tokTerminated, tokThen, tokTiDBCurrentTSO, tokTinyBlob, tokTinyInt, tokTinyText, tokTo, tokTrailing, tokTrigger, tokTrue,
		tokUnion, tokUnique, tokUnlock, tokUnsigned, tokUntil, tokUpdate, tokUsage, tokUse, tokUsing, tokUTCDate, tokUTCTime, tokUTCTimestamp,
		tokValues, tokVarBinary, tokVarChar, tokVarCharacter, tokVarying, tokVirtual,
		tokWhen, tokWhere, tokWhile, tokWindow, tokWith, tokWrite,
		tokXor,
		tokYearMonth,
		tokZerofill:
		return true
	}
	return false
}
