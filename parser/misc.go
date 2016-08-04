// Copyright 2016 PingCAP, Inc.
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

import (
	"bytes"

	"github.com/pingcap/tidb/util/hack"
)

func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n'
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch byte) bool {
	return (ch >= '0' && ch <= '9')
}

func isIdentChar(ch byte) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$'
}

func isIdentFirstChar(ch byte) bool {
	return isLetter(ch) || ch == '_'
}

func isASCII(ch byte) bool {
	return ch >= 0 && ch <= 0177
}

type trieNode struct {
	childs [256]*trieNode
	token  int
	fn     func(s *Scanner) (int, Pos, string)
}

var ruleTable trieNode

func initTokenByte(c byte, tok int) {
	if ruleTable.childs[c] == nil {
		ruleTable.childs[c] = &trieNode{}
	}
	ruleTable.childs[c].token = tok
}

func initTokenString(str string, tok int) {
	node := &ruleTable
	for _, c := range str {
		if node.childs[c] == nil {
			node.childs[c] = &trieNode{}
		}
		node = node.childs[c]
	}
	node.token = tok
}

func initTokenFunc(str string, fn func(s *Scanner) (int, Pos, string)) {
	for i := 0; i < len(str); i++ {
		c := str[i]
		if ruleTable.childs[c] == nil {
			ruleTable.childs[c] = &trieNode{}
		}
		ruleTable.childs[c].fn = fn
	}
	return
}

func init() {
	initTokenByte('*', int('*'))
	initTokenByte('/', int('/'))
	initTokenByte('+', int('+'))
	initTokenByte('>', int('>'))
	initTokenByte('<', int('<'))
	initTokenByte('(', int('('))
	initTokenByte(')', int(')'))
	initTokenByte(';', int(';'))
	initTokenByte(',', int(','))
	initTokenByte('&', int('&'))
	initTokenByte('%', int('%'))
	initTokenByte(':', int(':'))
	initTokenByte('|', int('|'))
	initTokenByte('!', int('!'))
	initTokenByte('^', int('^'))
	initTokenByte('~', int('~'))
	initTokenByte('\\', int('\\'))
	initTokenByte('?', placeholder)
	initTokenByte('=', eq)

	initTokenString("||", oror)
	initTokenString("&&", andand)
	initTokenString("&^", andnot)
	initTokenString(":=", assignmentEq)
	initTokenString("<=>", nulleq)
	initTokenString(">=", ge)
	initTokenString("<=", le)
	initTokenString("!=", neq)
	initTokenString("<>", neqSynonym)
	initTokenString("<<", lsh)
	initTokenString(">>", rsh)

	initTokenFunc("@", startWithAt)
	initTokenFunc("/", startWithSlash)
	initTokenFunc("-", startWithDash)
	initTokenFunc("#", startWithSharp)
	initTokenFunc("Xx", startWithXx)
	initTokenFunc("x", startWithXx)
	initTokenFunc("b", startWithb)
	initTokenFunc("_$ABCDEFGHIJKLMNOPQRSTUVWYZacdefghijklmnopqrstuvwyz", scanIdentifier)
	initTokenFunc("`", startWithBackQuote)
	initTokenFunc("0123456789.", startWithNumber)
	initTokenFunc("'\"", startString)
}

var tokenMap = map[string]int{
	"ABS":                 abs,
	"ADD":                 add,
	"ADDDATE":             addDate,
	"ADMIN":               admin,
	"AFTER":               after,
	"ALL":                 all,
	"ALTER":               alter,
	"ANALYZE":             analyze,
	"AND":                 and,
	"ANY":                 any,
	"AS":                  as,
	"ASC":                 asc,
	"ASCII":               ascii,
	"AUTO_INCREMENT":      autoIncrement,
	"AVG":                 avg,
	"AVG_ROW_LENGTH":      avgRowLength,
	"BEGIN":               begin,
	"BETWEEN":             between,
	"BINLOG":              binlog,
	"BOTH":                both,
	"BTREE":               btree,
	"BY":                  by,
	"BYTE":                byteType,
	"CASE":                caseKwd,
	"CAST":                cast,
	"CHARACTER":           character,
	"CHARSET":             charsetKwd,
	"CHECK":               check,
	"CHECKSUM":            checksum,
	"COALESCE":            coalesce,
	"COLLATE":             collate,
	"COLLATION":           collation,
	"COLUMN":              column,
	"COLUMNS":             columns,
	"COMMENT":             comment,
	"COMMIT":              commit,
	"COMMITTED":           committed,
	"COMPACT":             compact,
	"COMPRESSED":          compressed,
	"COMPRESSION":         compression,
	"CONCAT":              concat,
	"CONCAT_WS":           concatWs,
	"CONNECTION":          connection,
	"CONNECTION_ID":       connectionID,
	"CONSTRAINT":          constraint,
	"CONVERT":             convert,
	"COUNT":               count,
	"CREATE":              create,
	"CROSS":               cross,
	"CURDATE":             curDate,
	"UTC_DATE":            utcDate,
	"CURRENT_DATE":        currentDate,
	"CURTIME":             curTime,
	"CURRENT_TIME":        currentTime,
	"CURRENT_USER":        currentUser,
	"DATABASE":            database,
	"DATABASES":           databases,
	"DATE_ADD":            dateAdd,
	"DATE_FORMAT":         dateFormat,
	"DATE_SUB":            dateSub,
	"DAY":                 day,
	"DAYNAME":             dayname,
	"DAYOFMONTH":          dayofmonth,
	"DAYOFWEEK":           dayofweek,
	"DAYOFYEAR":           dayofyear,
	"DDL":                 ddl,
	"DEALLOCATE":          deallocate,
	"DEFAULT":             defaultKwd,
	"DELAYED":             delayed,
	"DELAY_KEY_WRITE":     delayKeyWrite,
	"DELETE":              deleteKwd,
	"DESC":                desc,
	"DESCRIBE":            describe,
	"DISABLE":             disable,
	"DISTINCT":            distinct,
	"DIV":                 div,
	"DO":                  do,
	"DROP":                drop,
	"DUAL":                dual,
	"DUPLICATE":           duplicate,
	"DYNAMIC":             dynamic,
	"ELSE":                elseKwd,
	"ENABLE":              enable,
	"END":                 end,
	"ENGINE":              engine,
	"ENGINES":             engines,
	"ENUM":                enum,
	"ESCAPE":              escape,
	"EXECUTE":             execute,
	"EXISTS":              exists,
	"EXPLAIN":             explain,
	"EXTRACT":             extract,
	"FALSE":               falseKwd,
	"FIELDS":              fields,
	"FIRST":               first,
	"FIXED":               fixed,
	"FOREIGN":             foreign,
	"FOR":                 forKwd,
	"FORCE":               force,
	"FOUND_ROWS":          foundRows,
	"FROM":                from,
	"FULL":                full,
	"FULLTEXT":            fulltext,
	"GET_LOCK":            getLock,
	"GLOBAL":              global,
	"GRANT":               grant,
	"GRANTS":              grants,
	"GREATEST":            greatest,
	"GROUP":               group,
	"GROUP_CONCAT":        groupConcat,
	"HASH":                hash,
	"HAVING":              having,
	"HIGH_PRIORITY":       highPriority,
	"HOUR":                hour,
	"HEX":                 hex,
	"IDENTIFIED":          identified,
	"IGNORE":              ignore,
	"IF":                  ifKwd,
	"IFNULL":              ifNull,
	"IN":                  in,
	"INDEX":               index,
	"INNER":               inner,
	"INSERT":              insert,
	"INTERVAL":            interval,
	"INTO":                into,
	"IS":                  is,
	"ISNULL":              isNull,
	"ISOLATION":           isolation,
	"JOIN":                join,
	"KEY":                 key,
	"KEY_BLOCK_SIZE":      keyBlockSize,
	"KEYS":                keys,
	"LAST_INSERT_ID":      lastInsertID,
	"LEADING":             leading,
	"LEFT":                left,
	"LENGTH":              length,
	"LEVEL":               level,
	"LIKE":                like,
	"LIMIT":               limit,
	"LOCAL":               local,
	"LOCATE":              locate,
	"LOCK":                lock,
	"LOWER":               lower,
	"LCASE":               lcase,
	"LOW_PRIORITY":        lowPriority,
	"LTRIM":               ltrim,
	"MAX":                 max,
	"MAX_ROWS":            maxRows,
	"MICROSECOND":         microsecond,
	"MIN":                 min,
	"MINUTE":              minute,
	"MIN_ROWS":            minRows,
	"MOD":                 mod,
	"MODE":                mode,
	"MONTH":               month,
	"MONTHNAME":           monthname,
	"NAMES":               names,
	"NATIONAL":            national,
	"NOT":                 not,
	"NULL":                null,
	"NULLIF":              nullIf,
	"OFFSET":              offset,
	"ON":                  on,
	"ONLY":                only,
	"OPTION":              option,
	"OR":                  or,
	"ORDER":               order,
	"OUTER":               outer,
	"PASSWORD":            password,
	"POW":                 pow,
	"POWER":               power,
	"PREPARE":             prepare,
	"PRIMARY":             primary,
	"PRIVILEGES":          privileges,
	"PROCEDURE":           procedure,
	"QUARTER":             quarter,
	"QUICK":               quick,
	"RAND":                rand,
	"READ":                read,
	"REDUNDANT":           redundant,
	"REFERENCES":          references,
	"REGEXP":              regexpKwd,
	"RELEASE_LOCK":        releaseLock,
	"REPEAT":              repeat,
	"REPEATABLE":          repeatable,
	"REPLACE":             replace,
	"RIGHT":               right,
	"RLIKE":               rlike,
	"ROLLBACK":            rollback,
	"ROUND":               round,
	"ROW":                 row,
	"ROW_FORMAT":          rowFormat,
	"RTRIM":               rtrim,
	"REVERSE":             reverse,
	"SCHEMA":              schema,
	"SCHEMAS":             schemas,
	"SECOND":              second,
	"SELECT":              selectKwd,
	"SERIALIZABLE":        serializable,
	"SESSION":             session,
	"SET":                 set,
	"SHARE":               share,
	"SHOW":                show,
	"SLEEP":               sleep,
	"SIGNED":              signed,
	"SOME":                some,
	"SPACE":               space,
	"START":               start,
	"STATS_PERSISTENT":    statsPersistent,
	"STATUS":              status,
	"SUBDATE":             subDate,
	"STRCMP":              strcmp,
	"SUBSTR":              substring,
	"SUBSTRING":           substring,
	"SUBSTRING_INDEX":     substringIndex,
	"SUM":                 sum,
	"SYSDATE":             sysDate,
	"TABLE":               tableKwd,
	"TABLES":              tables,
	"THEN":                then,
	"TO":                  to,
	"TRAILING":            trailing,
	"TRANSACTION":         transaction,
	"TRIGGERS":            triggers,
	"TRIM":                trim,
	"TRUE":                trueKwd,
	"TRUNCATE":            truncate,
	"UNCOMMITTED":         uncommitted,
	"UNKNOWN":             unknown,
	"UNION":               union,
	"UNIQUE":              unique,
	"UNLOCK":              unlock,
	"UNSIGNED":            unsigned,
	"UPDATE":              update,
	"UPPER":               upper,
	"UCASE":               ucase,
	"USE":                 use,
	"USER":                user,
	"USING":               using,
	"VALUE":               value,
	"VALUES":              values,
	"VARIABLES":           variables,
	"VERSION":             version,
	"WARNINGS":            warnings,
	"WEEK":                week,
	"WEEKDAY":             weekday,
	"WEEKOFYEAR":          weekofyear,
	"WHEN":                when,
	"WHERE":               where,
	"WRITE":               write,
	"XOR":                 xor,
	"YEARWEEK":            yearweek,
	"ZEROFILL":            zerofill,
	"SQL_CALC_FOUND_ROWS": calcFoundRows,
	"SQL_CACHE":           sqlCache,
	"SQL_NO_CACHE":        sqlNoCache,
	"CURRENT_TIMESTAMP":   currentTs,
	"LOCALTIME":           localTime,
	"LOCALTIMESTAMP":      localTs,
	"NOW":                 now,
	"TINY":                tinyIntType,
	"TINYINT":             tinyIntType,
	"SMALLINT":            smallIntType,
	"MEDIUMINT":           mediumIntType,
	"INT":                 intType,
	"INTEGER":             integerType,
	"BIGINT":              bigIntType,
	"BIT":                 bitType,
	"DECIMAL":             decimalType,
	"NUMERIC":             numericType,
	"FLOAT":               floatType,
	"DOUBLE":              doubleType,
	"PRECISION":           precisionType,
	"REAL":                realType,
	"DATE":                dateType,
	"TIME":                timeType,
	"DATETIME":            datetimeType,
	"TIMESTAMP":           timestampType,
	"YEAR":                yearType,
	"CHAR":                charType,
	"VARCHAR":             varcharType,
	"BINARY":              binaryType,
	"VARBINARY":           varbinaryType,
	"TINYBLOB":            tinyblobType,
	"BLOB":                blobType,
	"MEDIUMBLOB":          mediumblobType,
	"LONGBLOB":            longblobType,
	"TINYTEXT":            tinytextType,
	"TEXT":                textType,
	"MEDIUMTEXT":          mediumtextType,
	"LONGTEXT":            longtextType,
	"BOOL":                boolType,
	"BOOLEAN":             booleanType,
	"SECOND_MICROSECOND":  secondMicrosecond,
	"MINUTE_MICROSECOND":  minuteMicrosecond,
	"MINUTE_SECOND":       minuteSecond,
	"HOUR_MICROSECOND":    hourMicrosecond,
	"HOUR_SECOND":         hourSecond,
	"HOUR_MINUTE":         hourMinute,
	"DAY_MICROSECOND":     dayMicrosecond,
	"DAY_SECOND":          daySecond,
	"DAY_MINUTE":          dayMinute,
	"DAY_HOUR":            dayHour,
	"YEAR_MONTH":          yearMonth,
	"RESTRICT":            restrict,
	"CASCADE":             cascade,
	"NO":                  no,
	"ACTION":              action,
}

func isTokenIdentifier(s string, buf *bytes.Buffer) int {
	buf.Reset()
	buf.Grow(len(s))
	data := buf.Bytes()[:len(s)]
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			data[i] = s[i] + 'A' - 'a'
		} else {
			data[i] = s[i]
		}
	}
	tok := tokenMap[hack.String(data)]
	return tok
}
