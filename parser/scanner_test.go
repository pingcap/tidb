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
package parser

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testLexerSuite{})

type testLexerSuite struct {
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
	"&&":                  andand,
	"&^":                  andnot,
	"ANY":                 any,
	"AS":                  as,
	"ASC":                 asc,
	"ASCII":               ascii,
	":=":                  assignmentEq,
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
	"false":               falseKwd,
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
	">=":                  ge,
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
	"<=":                  le,
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
	"<<":                  lsh,
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
	"!=":                  neq,
	"<>":                  neqSynonym,
	"NOT":                 not,
	"NULL":                null,
	"<=>":                 nulleq,
	"NULLIF":              nullIf,
	"OFFSET":              offset,
	"ON":                  on,
	"ONLY":                only,
	"OPTION":              option,
	"OR":                  or,
	"ORDER":               order,
	"||":                  oror,
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
	">>":                  rsh,
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
	"true":                trueKwd,
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
	"TINYINT":             tinyIntType,
	"SMALLINT":            smallIntType,
	"MEDIUMINT":           mediumIntType,
	"INT":                 intType,
	"INTEGER":             integerType,
	"BIGINT":              bigIntType,
	"BIT":                 bitType,
	"DECIMAL":             decimalType,
	"NUMERIC":             numericType,
	"float":               floatType,
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

func (s *testLexerSuite) TestTokenID(c *C) {
	defer testleak.AfterTest(c)()
	for str, tok := range tokenMap {
		l := NewLexer(str)
		var v yySymType
		tok1 := l.Lex(&v)
		c.Check(tok, Equals, tok1)
	}
}

func (s *testLexerSuite) TestSingleChar(c *C) {
	defer testleak.AfterTest(c)()
	table := []byte{'|', '&', '-', '+', '*', '/', '%', '^', '~', '(', ',', ')'}
	for _, tok := range table {
		l := NewLexer(string(tok))
		var v yySymType
		tok1 := l.Lex(&v)
		c.Check(int(tok), Equals, tok1)
	}
}

type testCaseItem struct {
	str string
	tok int
}

func (s *testLexerSuite) TestSingleCharOther(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCaseItem{
		{"@", at},
		{"AT", identifier},
		{"?", placeholder},
		{"PLACEHOLDER", identifier},
		{"=", eq},
	}
	runTest(c, table)
}

func (s *testLexerSuite) TestSysOrUserVar(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCaseItem{
		{"@a_3cbbc", userVar},
		{"@-3cbbc", at},
		{"@@global.test", sysVar},
		{"@@session.test", sysVar},
		{"@@local.test", sysVar},
		{"@@test", sysVar},
	}
	runTest(c, table)
}

func (s *testLexerSuite) TestUnderscoreCS(c *C) {
	defer testleak.AfterTest(c)()
	var v yySymType
	tok := NewLexer(`_utf8"string"`).Lex(&v)
	c.Check(tok, Equals, underscoreCS)
}

func (s *testLexerSuite) TestLiteral(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCaseItem{
		{`'''a'''`, stringLit},
		{`''a''`, stringLit},
		{`""a""`, stringLit},
		{`\'a\'`, int('\\')},
		{`\"a\"`, int('\\')},
		{"0.2314", floatLit},
		{"132.3e231", floatLit},
		{"23416", intLit},
		{"0", intLit},
		{"0x3c26", hexLit},
		{"0b01", bitLit},
	}
	runTest(c, table)
}

func runTest(c *C, table []testCaseItem) {
	var val yySymType
	for _, v := range table {
		l := NewLexer(v.str)
		tok := l.Lex(&val)
		c.Check(tok, Equals, v.tok)
	}
}

func (s *testLexerSuite) TestComment(c *C) {
	table := []testCaseItem{
		{"-- select --\n1", intLit},
		{"/*!40101 SET character_set_client = utf8 */;", int(';')},
		{"/* some comments */ SELECT ", selectKwd},
		{`/*
`, 65533},
		{`-- comment continues to the end of line
SELECT`, selectKwd},
		{`# comment continues to the end of line
SELECT`, selectKwd},
		{"#comment\n123", intLit},
		{"--5", int('-')},
	}
	runTest(c, table)
}
