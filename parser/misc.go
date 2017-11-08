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
	"strings"

	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/hack"
)

func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch rune) bool {
	return (ch >= '0' && ch <= '9')
}

func isIdentChar(ch rune) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || isIdentExtend(ch)
}

func isIdentExtend(ch rune) bool {
	return ch >= 0x80 && ch <= '\uffff'
}

func isIdentFirstChar(ch rune) bool {
	return isLetter(ch) || ch == '_'
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
	// invalid is a special token defined in parser.y, when parser meet
	// this token, it will throw an error.
	// set root trie node's token to invalid, so when input match nothing
	// in the trie, invalid will be the default return token.
	ruleTable.token = invalid
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
	initTokenByte('?', paramMarker)
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
	initTokenString("\\N", null)

	initTokenFunc("@", startWithAt)
	initTokenFunc("/", startWithSlash)
	initTokenFunc("-", startWithDash)
	initTokenFunc("#", startWithSharp)
	initTokenFunc("Xx", startWithXx)
	initTokenFunc("Nn", startWithNn)
	initTokenFunc("Bb", startWithBb)
	initTokenFunc(".", startWithDot)
	initTokenFunc("_$ACDEFGHIJKLMOPQRSTUVWYZacdefghijklmopqrstuvwyz", scanIdentifier)
	initTokenFunc("`", scanQuotedIdent)
	initTokenFunc("0123456789", startWithNumber)
	initTokenFunc("'\"", startString)
}

var tokenMap = map[string]int{
	"ACTION":                   action,
	"ADD":                      add,
	"ADDDATE":                  addDate,
	"ADMIN":                    admin,
	"AFTER":                    after,
	"ALL":                      all,
	"ALTER":                    alter,
	"ALWAYS":                   always,
	"ANALYZE":                  analyze,
	"AND":                      and,
	"ANY":                      any,
	"AS":                       as,
	"ASC":                      asc,
	"ASCII":                    ascii,
	"AUTO_INCREMENT":           autoIncrement,
	"AVG":                      avg,
	"AVG_ROW_LENGTH":           avgRowLength,
	"BEGIN":                    begin,
	"BETWEEN":                  between,
	"BIGINT":                   bigIntType,
	"BINARY":                   binaryType,
	"BINLOG":                   binlog,
	"BIT":                      bitType,
	"BIT_XOR":                  bitXor,
	"BLOB":                     blobType,
	"BOOL":                     boolType,
	"BOOLEAN":                  booleanType,
	"BOTH":                     both,
	"BTREE":                    btree,
	"BY":                       by,
	"BYTE":                     byteType,
	"CANCEL":                   cancel,
	"CASCADE":                  cascade,
	"CASE":                     caseKwd,
	"CAST":                     cast,
	"CHANGE":                   change,
	"CHAR":                     charType,
	"CHARACTER":                character,
	"CHARSET":                  charsetKwd,
	"CHECK":                    check,
	"CHECKSUM":                 checksum,
	"CLIENT":                   client,
	"COALESCE":                 coalesce,
	"COLLATE":                  collate,
	"COLLATION":                collation,
	"COLUMN":                   column,
	"COLUMNS":                  columns,
	"COMMENT":                  comment,
	"COMMIT":                   commit,
	"COMMITTED":                committed,
	"COMPACT":                  compact,
	"COMPRESSED":               compressed,
	"COMPRESSION":              compression,
	"CONNECTION":               connection,
	"CONSISTENT":               consistent,
	"CONSTRAINT":               constraint,
	"CONVERT":                  convert,
	"COUNT":                    count,
	"CREATE":                   create,
	"CROSS":                    cross,
	"CUME_DIST":                cumeDist,
	"CURRENT":                  current,
	"CURRENT_DATE":             currentDate,
	"CURRENT_TIME":             currentTime,
	"CURRENT_TIMESTAMP":        currentTs,
	"CURRENT_USER":             currentUser,
	"CURTIME":                  curTime,
	"DATA":                     data,
	"DATABASE":                 database,
	"DATABASES":                databases,
	"DATE":                     dateType,
	"DATETIME":                 datetimeType,
	"DATE_ADD":                 dateAdd,
	"DATE_SUB":                 dateSub,
	"DAY":                      day,
	"DAY_HOUR":                 dayHour,
	"DAY_MICROSECOND":          dayMicrosecond,
	"DAY_MINUTE":               dayMinute,
	"DAY_SECOND":               daySecond,
	"DDL":                      ddl,
	"DEALLOCATE":               deallocate,
	"DEC":                      decimalType,
	"DECIMAL":                  decimalType,
	"DEFAULT":                  defaultKwd,
	"DELAYED":                  delayed,
	"DELAY_KEY_WRITE":          delayKeyWrite,
	"DELETE":                   deleteKwd,
	"DENSE_RANK":               denseRank,
	"DESC":                     desc,
	"DESCRIBE":                 describe,
	"DISABLE":                  disable,
	"DISTINCT":                 distinct,
	"DISTINCTROW":              distinct,
	"DIV":                      div,
	"DO":                       do,
	"DOUBLE":                   doubleType,
	"DROP":                     drop,
	"DUAL":                     dual,
	"DUPLICATE":                duplicate,
	"DYNAMIC":                  dynamic,
	"ELSE":                     elseKwd,
	"ENABLE":                   enable,
	"ENCLOSED":                 enclosed,
	"END":                      end,
	"ENGINE":                   engine,
	"ENGINES":                  engines,
	"ENUM":                     enum,
	"ESCAPE":                   escape,
	"ESCAPED":                  escaped,
	"EVENT":                    event,
	"EVENTS":                   events,
	"EXCLUSIVE":                exclusive,
	"EXECUTE":                  execute,
	"EXISTS":                   exists,
	"EXPLAIN":                  explain,
	"EXTRACT":                  extract,
	"FALSE":                    falseKwd,
	"FIELDS":                   fields,
	"FIRST":                    first,
	"FIRST_VALUE":              firstValue,
	"FIXED":                    fixed,
	"FLOAT":                    floatType,
	"FLUSH":                    flush,
	"FOLLOWING":                following,
	"FOR":                      forKwd,
	"FORCE":                    force,
	"FOREIGN":                  foreign,
	"FORMAT":                   format,
	"FROM":                     from,
	"FULL":                     full,
	"FULLTEXT":                 fulltext,
	"FUNCTION":                 function,
	"GENERATED":                generated,
	"GET_FORMAT":               getFormat,
	"GLOBAL":                   global,
	"GRANT":                    grant,
	"GRANTS":                   grants,
	"GROUP":                    group,
	"GROUPS":                   groups,
	"GROUP_CONCAT":             groupConcat,
	"HASH":                     hash,
	"HAVING":                   having,
	"HIGH_PRIORITY":            highPriority,
	"HOUR":                     hour,
	"HOUR_MICROSECOND":         hourMicrosecond,
	"HOUR_MINUTE":              hourMinute,
	"HOUR_SECOND":              hourSecond,
	"IDENTIFIED":               identified,
	"IF":                       ifKwd,
	"IGNORE":                   ignore,
	"IN":                       in,
	"INDEX":                    index,
	"INDEXES":                  indexes,
	"INFILE":                   infile,
	"INNER":                    inner,
	"INSERT":                   insert,
	"INT":                      intType,
	"INT1":                     int1Type,
	"INT2":                     int2Type,
	"INT3":                     int3Type,
	"INT4":                     int4Type,
	"INT8":                     int8Type,
	"INTEGER":                  integerType,
	"INTERVAL":                 interval,
	"INTO":                     into,
	"IS":                       is,
	"ISOLATION":                isolation,
	"JOBS":                     jobs,
	"JOIN":                     join,
	"JSON":                     jsonType,
	"KEY":                      key,
	"KEYS":                     keys,
	"KEY_BLOCK_SIZE":           keyBlockSize,
	"KILL":                     kill,
	"LAG":                      lag,
	"LAST":                     last,
	"LAST_VALUE":               lastValue,
	"LEAD":                     lead,
	"LEADING":                  leading,
	"LEFT":                     left,
	"LESS":                     less,
	"LEVEL":                    level,
	"LIKE":                     like,
	"LIMIT":                    limit,
	"LINES":                    lines,
	"LOAD":                     load,
	"LOCAL":                    local,
	"LOCALTIME":                localTime,
	"LOCALTIMESTAMP":           localTs,
	"LOCK":                     lock,
	"LONGBLOB":                 longblobType,
	"LONGTEXT":                 longtextType,
	"LOW_PRIORITY":             lowPriority,
	"MAX":                      max,
	"MAXVALUE":                 maxValue,
	"MAX_CONNECTIONS_PER_HOUR": maxConnectionsPerHour,
	"MAX_QUERIES_PER_HOUR":     maxQueriesPerHour,
	"MAX_ROWS":                 maxRows,
	"MAX_UPDATES_PER_HOUR":     maxUpdatesPerHour,
	"MAX_USER_CONNECTIONS":     maxUserConnections,
	"MEDIUMBLOB":               mediumblobType,
	"MEDIUMINT":                mediumIntType,
	"MEDIUMTEXT":               mediumtextType,
	"MICROSECOND":              microsecond,
	"MIN":                      min,
	"MINUTE":                   minute,
	"MINUTE_MICROSECOND":       minuteMicrosecond,
	"MINUTE_SECOND":            minuteSecond,
	"MIN_ROWS":                 minRows,
	"MOD":                      mod,
	"MODE":                     mode,
	"MODIFY":                   modify,
	"MONTH":                    month,
	"NAMES":                    names,
	"NATIONAL":                 national,
	"NATURAL":                  natural,
	"NO":                       no,
	"NONE":                     none,
	"NOT":                      not,
	"NOW":                      now,
	"NO_WRITE_TO_BINLOG":       noWriteToBinLog,
	"NTH_VALUE":                nthValue,
	"NTILE":                    ntile,
	"NULL":                     null,
	"NULLS":                    nulls,
	"NUMERIC":                  numericType,
	"NVARCHAR":                 nvarcharType,
	"OFFSET":                   offset,
	"ON":                       on,
	"ONLY":                     only,
	"OPTION":                   option,
	"OR":                       or,
	"ORDER":                    order,
	"OUTER":                    outer,
	"OVER":                     over,
	"PARTITION":                partition,
	"PARTITIONS":               partitions,
	"PASSWORD":                 password,
	"PERCENT_RANK":             percentRank,
	"PLUGINS":                  plugins,
	"POSITION":                 position,
	"PRECEDING":                preceding,
	"PRECISION":                precisionType,
	"PREPARE":                  prepare,
	"PRIMARY":                  primary,
	"PRIVILEGES":               privileges,
	"PROCEDURE":                procedure,
	"PROCESS":                  process,
	"PROCESSLIST":              processlist,
	"PROFILES":                 profiles,
	"QUARTER":                  quarter,
	"QUERY":                    query,
	"QUICK":                    quick,
	"RANGE":                    rangeKwd,
	"RANK":                     rank,
	"READ":                     read,
	"REAL":                     realType,
	"REDUNDANT":                redundant,
	"REFERENCES":               references,
	"REGEXP":                   regexpKwd,
	"RELOAD":                   reload,
	"RENAME":                   rename,
	"REPEAT":                   repeat,
	"REPEATABLE":               repeatable,
	"REPLACE":                  replace,
	"REPLICATION":              replication,
	"RESPECT":                  respect,
	"RESTRICT":                 restrict,
	"REVERSE":                  reverse,
	"REVOKE":                   revoke,
	"RIGHT":                    right,
	"RLIKE":                    rlike,
	"ROLLBACK":                 rollback,
	"ROUTINE":                  routine,
	"ROW":                      row,
	"ROWS":                     rows,
	"ROW_COUNT":                rowCount,
	"ROW_FORMAT":               rowFormat,
	"ROW_NUMBER":               rowNumber,
	"SCHEMA":                   database,
	"SCHEMAS":                  databases,
	"SECOND":                   second,
	"SECOND_MICROSECOND":       secondMicrosecond,
	"SELECT":                   selectKwd,
	"SERIALIZABLE":             serializable,
	"SESSION":                  session,
	"SET":                      set,
	"SHARE":                    share,
	"SHARED":                   shared,
	"SHOW":                     show,
	"SIGNED":                   signed,
	"SLAVE":                    slave,
	"SMALLINT":                 smallIntType,
	"SNAPSHOT":                 snapshot,
	"SOME":                     some,
	"SQL_CACHE":                sqlCache,
	"SQL_CALC_FOUND_ROWS":      sqlCalcFoundRows,
	"SQL_NO_CACHE":             sqlNoCache,
	"START":                    start,
	"STARTING":                 starting,
	"STATS":                    stats,
	"STATS_BUCKETS":            statsBuckets,
	"STATS_HISTOGRAMS":         statsHistograms,
	"STATS_META":               statsMeta,
	"STATS_PERSISTENT":         statsPersistent,
	"STATUS":                   status,
	"STORED":                   stored,
	"STRAIGHT_JOIN":            straightJoin,
	"SUBDATE":                  subDate,
	"SUBSTR":                   substring,
	"SUBSTRING":                substring,
	"SUM":                      sum,
	"SUPER":                    super,
	"TABLE":                    tableKwd,
	"TABLES":                   tables,
	"TEMPORARY":                temporary,
	"TERMINATED":               terminated,
	"TEXT":                     textType,
	"THAN":                     than,
	"THEN":                     then,
	"TIDB":                     tidb,
	"TIDB_INLJ":                tidbINLJ,
	"TIDB_SMJ":                 tidbSMJ,
	"TIME":                     timeType,
	"TIMESTAMP":                timestampType,
	"TIMESTAMPADD":             timestampAdd,
	"TIMESTAMPDIFF":            timestampDiff,
	"TINYBLOB":                 tinyblobType,
	"TINYINT":                  tinyIntType,
	"TINYTEXT":                 tinytextType,
	"TO":                       to,
	"TRAILING":                 trailing,
	"TRANSACTION":              transaction,
	"TRIGGER":                  trigger,
	"TRIGGERS":                 triggers,
	"TRIM":                     trim,
	"TRUE":                     trueKwd,
	"TRUNCATE":                 truncate,
	"UNBOUNDED":                unbounded,
	"UNCOMMITTED":              uncommitted,
	"UNION":                    union,
	"UNIQUE":                   unique,
	"UNKNOWN":                  unknown,
	"UNLOCK":                   unlock,
	"UNSIGNED":                 unsigned,
	"UPDATE":                   update,
	"USAGE":                    usage,
	"USE":                      use,
	"USER":                     user,
	"USING":                    using,
	"UTC_DATE":                 utcDate,
	"UTC_TIME":                 utcTime,
	"UTC_TIMESTAMP":            utcTimestamp,
	"VALUE":                    value,
	"VALUES":                   values,
	"VARBINARY":                varbinaryType,
	"VARCHAR":                  varcharType,
	"VARIABLES":                variables,
	"VIEW":                     view,
	"VIRTUAL":                  virtual,
	"WARNINGS":                 warnings,
	"WEEK":                     week,
	"WHEN":                     when,
	"WHERE":                    where,
	"WINDOW":                   window,
	"WITH":                     with,
	"WRITE":                    write,
	"XOR":                      xor,
	"YEAR":                     yearType,
	"YEAR_MONTH":               yearMonth,
	"ZEROFILL":                 zerofill,
}

// aliases are strings directly map to another string and use the same token.
var aliases = map[string]string{
	"SCHEMA":  "DATABASE",
	"SCHEMAS": "DATABASES",
	"DEC":     "DECIMAL",
	"SUBSTR":  "SUBSTRING",
}

func (s *Scanner) isTokenIdentifier(lit string, offset int) int {
	// An identifier before or after '.' means it is part of a qualified identifier.
	// We do not parse it as keyword.
	if s.r.peek() == '.' {
		return 0
	}
	if offset > 0 && s.r.s[offset-1] == '.' {
		return 0
	}
	buf := &s.buf
	buf.Reset()
	buf.Grow(len(lit))
	data := buf.Bytes()[:len(lit)]
	for i := 0; i < len(lit); i++ {
		if lit[i] >= 'a' && lit[i] <= 'z' {
			data[i] = lit[i] + 'A' - 'a'
		} else {
			data[i] = lit[i]
		}
	}
	tok := tokenMap[hack.String(data)]
	return tok
}

func handleIdent(lval *yySymType) int {
	s := lval.ident
	// A character string literal may have an optional character set introducer and COLLATE clause:
	// [_charset_name]'string' [COLLATE collation_name]
	// See https://dev.mysql.com/doc/refman/5.7/en/charset-literal.html
	if !strings.HasPrefix(s, "_") {
		return identifier
	}
	cs, _, err := charset.GetCharsetInfo(s[1:])
	if err != nil {
		return identifier
	}
	lval.ident = cs
	return underscoreCS
}
