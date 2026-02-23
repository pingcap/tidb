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

package parser

// IsReserved returns true if the token type corresponds to a reserved keyword.
// This list MUST be kept in sync with pkg/parser/keywords.go.
func IsReserved(tp int) bool {
	switch tp {
	case add, all, alter, analyze, and, array, as, asc,
		between, bigIntType, binaryType, blobType, both, by,
		call, cascade, caseKwd, change, charType, character, check, collate,
		column, constraint, continueKwd, convert, create, cross, cumeDist,
		currentDate, currentRole, currentTime, currentTs, currentUser, cursor,
		database, databases, dayHour, dayMicrosecond, dayMinute, daySecond,
		decimalType, defaultKwd, delayed, deleteKwd, denseRank, desc, describe,
		distinct, distinctRow, div, doubleType, drop, dual,
		elseKwd, elseIfKwd, enclosed, escaped, except, exists, exit, explain,
		falseKwd, fetch, firstValue, floatType, float4Type, float8Type, forKwd,
		force, foreign, from, full, fulltext,
		generated, grant, group, groups,
		having, highPriority, hourMicrosecond, hourMinute, hourSecond,
		ifKwd, ignore, ilike, in, index, infile, inner, inout, insert, intType,
		int1Type, int2Type, int3Type, int4Type, int8Type, integerType, intersect,
		interval, into, is, iterate,
		join,
		key, keys, kill,
		lag, lastValue, lead, leading, leave, left, like, limit, linear, lines,
		load, localTime, localTs, lock, long, longblobType, longtextType, lowPriority,
		match, maxValue, mediumblobType, mediumIntType, mediumtextType,
		middleIntType, minuteMicrosecond, minuteSecond, mod,
		natural, not, noWriteToBinLog, nthValue, ntile, null, numericType,
		of, on, optimize, option, optionally, or, order, out, outer, outfile, over,
		partition, percentRank, precisionType, primary, procedure,
		rangeKwd, rank, read, realType, recursive, references, regexpKwd, release,
		rename, repeat, replace, require, restrict, revoke, right, rlike, row, rows, rowNumber,
		secondMicrosecond, selectKwd, set, show, smallIntType, spatial, sql,
		sqlexception, sqlstate, sqlwarning, sqlBigResult, sqlCalcFoundRows,
		sqlSmallResult, ssl, starting, stored, straightJoin,
		tableKwd, tableSample, terminated, then, tidbCurrentTSO, tinyblobType,
		tinyIntType, tinytextType, to, trailing, trigger, trueKwd,
		union, unique, unlock, unsigned, until, update, usage, use, using, utcDate, utcTime, utcTimestamp,
		values, varbinaryType, varcharType, varcharacter, varying, virtual,
		when, where, while, window, with, write,
		xor,
		yearMonth,
		zerofill:
		return true
	}
	return false
}
