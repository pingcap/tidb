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

func isASCII(ch rune) bool {
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
	initTokenFunc("Nn", startWithNn)
	initTokenFunc("Bb", startWithBb)
	initTokenFunc(".", startWithDot)
	initTokenFunc("_$ACDEFGHIJKLMOPQRSTUVWYZacdefghijklmopqrstuvwyz", scanIdentifier)
	initTokenFunc("`", scanQuotedIdent)
	initTokenFunc("0123456789", startWithNumber)
	initTokenFunc("'\"", startString)
}

var tokenMap = map[string]int{
	"ABS":                        abs,
	"ACOS":                       acos,
	"ADD":                        add,
	"ADDDATE":                    addDate,
	"ADDTIME":                    addTime,
	"ADMIN":                      admin,
	"AES_DECRYPT":                aesDecrypt,
	"AES_ENCRYPT":                aesEncrypt,
	"AFTER":                      after,
	"ALL":                        all,
	"ALTER":                      alter,
	"ANALYZE":                    analyze,
	"AND":                        and,
	"ANY":                        any,
	"AS":                         as,
	"ASC":                        asc,
	"ASIN":                       asin,
	"ASCII":                      ascii,
	"ATAN":                       atan,
	"ATAN2":                      atan2,
	"AUTO_INCREMENT":             autoIncrement,
	"AVG":                        avg,
	"AVG_ROW_LENGTH":             avgRowLength,
	"BEGIN":                      begin,
	"BETWEEN":                    between,
	"BIN":                        bin,
	"BINLOG":                     binlog,
	"BOTH":                       both,
	"BTREE":                      btree,
	"BY":                         by,
	"BYTE":                       byteType,
	"CASE":                       caseKwd,
	"CAST":                       cast,
	"CEIL":                       ceil,
	"CEILING":                    ceiling,
	"CHANGE":                     change,
	"CHARACTER":                  character,
	"CHARSET":                    charsetKwd,
	"CHECK":                      check,
	"CHECKSUM":                   checksum,
	"COALESCE":                   coalesce,
	"COLLATE":                    collate,
	"COLLATION":                  collation,
	"COLUMN":                     column,
	"COLUMNS":                    columns,
	"COMMENT":                    comment,
	"COMMIT":                     commit,
	"COMMITTED":                  committed,
	"COMPACT":                    compact,
	"COMPRESSED":                 compressed,
	"COMPRESSION":                compression,
	"CONCAT":                     concat,
	"CONCAT_WS":                  concatWs,
	"CONVERT_TZ":                 convertTz,
	"CONNECTION":                 connection,
	"CONNECTION_ID":              connectionID,
	"CONSTRAINT":                 constraint,
	"CONSISTENT":                 consistent,
	"CONVERT":                    convert,
	"COS":                        cos,
	"COT":                        cot,
	"COUNT":                      count,
	"CREATE":                     create,
	"CROSS":                      cross,
	"CURDATE":                    curDate,
	"UTC_DATE":                   utcDate,
	"UTC_TIMESTAMP":              utcTimestamp,
	"CURRENT_DATE":               currentDate,
	"CURTIME":                    curTime,
	"CURRENT_TIME":               currentTime,
	"CURRENT_USER":               currentUser,
	"DATA":                       data,
	"DATABASE":                   database,
	"DATABASES":                  databases,
	"DATEDIFF":                   datediff,
	"DATE_ADD":                   dateAdd,
	"DATE_FORMAT":                dateFormat,
	"DATE_SUB":                   dateSub,
	"DAY":                        day,
	"DAYNAME":                    dayname,
	"DAYOFMONTH":                 dayofmonth,
	"DAYOFWEEK":                  dayofweek,
	"DAYOFYEAR":                  dayofyear,
	"DDL":                        ddl,
	"DEALLOCATE":                 deallocate,
	"DEGREES":                    degrees,
	"DEFAULT":                    defaultKwd,
	"DELAYED":                    delayed,
	"DELAY_KEY_WRITE":            delayKeyWrite,
	"DELETE":                     deleteKwd,
	"DESC":                       desc,
	"DESCRIBE":                   describe,
	"DISABLE":                    disable,
	"DISTINCT":                   distinct,
	"DIV":                        div,
	"DO":                         do,
	"DROP":                       drop,
	"DUAL":                       dual,
	"DUPLICATE":                  duplicate,
	"DYNAMIC":                    dynamic,
	"FROM_DAYS":                  fromDays,
	"ELSE":                       elseKwd,
	"ELT":                        elt,
	"ENABLE":                     enable,
	"ENCLOSED":                   enclosed,
	"END":                        end,
	"ENGINE":                     engine,
	"ENGINES":                    engines,
	"ENUM":                       enum,
	"ESCAPE":                     escape,
	"ESCAPED":                    escaped,
	"EVENTS":                     events,
	"EXECUTE":                    execute,
	"EXISTS":                     exists,
	"EXP":                        exp,
	"EXPLAIN":                    explain,
	"EXPORT_SET":                 exportSet,
	"EXTRACT":                    extract,
	"FALSE":                      falseKwd,
	"FIELD":                      fieldKwd,
	"FIELDS":                     fields,
	"FIND_IN_SET":                findInSet,
	"FIRST":                      first,
	"FIXED":                      fixed,
	"FOREIGN":                    foreign,
	"FOR":                        forKwd,
	"FORCE":                      force,
	"FORMAT":                     format,
	"FOUND_ROWS":                 foundRows,
	"FROM":                       from,
	"FROM_BASE64":                fromBase64,
	"FROM_UNIXTIME":              fromUnixTime,
	"FULL":                       full,
	"FULLTEXT":                   fulltext,
	"FUNCTION":                   function,
	"FLOOR":                      floor,
	"FLUSH":                      flush,
	"GET_LOCK":                   getLock,
	"GLOBAL":                     global,
	"GRANT":                      grant,
	"GRANTS":                     grants,
	"GREATEST":                   greatest,
	"GROUP":                      group,
	"GROUP_CONCAT":               groupConcat,
	"HASH":                       hash,
	"HAVING":                     having,
	"HIGH_PRIORITY":              highPriority,
	"HOUR":                       hour,
	"HEX":                        hex,
	"UNHEX":                      unhex,
	"IDENTIFIED":                 identified,
	"IGNORE":                     ignore,
	"IF":                         ifKwd,
	"IFNULL":                     ifNull,
	"IN":                         in,
	"INDEX":                      index,
	"INDEXES":                    indexes,
	"INFILE":                     infile,
	"INNER":                      inner,
	"INSERT":                     insert,
	"INSERT_FUNC":                insertFunc,
	"INSTR":                      instr,
	"INTERVAL":                   interval,
	"INTO":                       into,
	"IS":                         is,
	"ISNULL":                     isNull,
	"ISOLATION":                  isolation,
	"JOIN":                       join,
	"KEY":                        key,
	"KEY_BLOCK_SIZE":             keyBlockSize,
	"KEYS":                       keys,
	"LAST_INSERT_ID":             lastInsertID,
	"LEADING":                    leading,
	"LEAST":                      least,
	"LEFT":                       left,
	"LENGTH":                     length,
	"LESS":                       less,
	"LEVEL":                      level,
	"LIKE":                       like,
	"LIMIT":                      limit,
	"LINES":                      lines,
	"LN":                         ln,
	"LOAD":                       load,
	"LOAD_FILE":                  loadFile,
	"LOCAL":                      local,
	"LOCATE":                     locate,
	"LOCK":                       lock,
	"LOG":                        log,
	"LOG2":                       log2,
	"LOG10":                      log10,
	"LOWER":                      lower,
	"LCASE":                      lcase,
	"LOW_PRIORITY":               lowPriority,
	"LPAD":                       lpad,
	"LTRIM":                      ltrim,
	"MAKEDATE":                   makeDate,
	"MAKETIME":                   makeTime,
	"MAKE_SET":                   makeSet,
	"MAX":                        max,
	"MAXVALUE":                   maxValue,
	"MAX_ROWS":                   maxRows,
	"MICROSECOND":                microsecond,
	"MID":                        mid,
	"MIN":                        min,
	"MINUTE":                     minute,
	"MIN_ROWS":                   minRows,
	"MOD":                        mod,
	"MODE":                       mode,
	"MODIFY":                     modify,
	"MONTH":                      month,
	"MONTHNAME":                  monthname,
	"NAMES":                      names,
	"NATIONAL":                   national,
	"NONE":                       none,
	"NOT":                        not,
	"NO_WRITE_TO_BINLOG":         noWriteToBinLog,
	"NULL":                       null,
	"NULLIF":                     nullIf,
	"OCT":                        oct,
	"OCTET_LENGTH":               octetLength,
	"OFFSET":                     offset,
	"ON":                         on,
	"ONLY":                       only,
	"OPTION":                     option,
	"OR":                         or,
	"ORD":                        ord,
	"ORDER":                      order,
	"OUTER":                      outer,
	"PASSWORD":                   password,
	"PERIOD_ADD":                 periodAdd,
	"PERIOD_DIFF":                periodDiff,
	"PI":                         pi,
	"POSITION":                   position,
	"POW":                        pow,
	"POWER":                      power,
	"PREPARE":                    prepare,
	"PRIMARY":                    primary,
	"PRIVILEGES":                 privileges,
	"PROCEDURE":                  procedure,
	"PROCESSLIST":                processlist,
	"QUARTER":                    quarter,
	"QUICK":                      quick,
	"RADIANS":                    radians,
	"QUERY":                      query,
	"QUOTE":                      quote,
	"RANGE":                      rangeKwd,
	"RAND":                       rand,
	"READ":                       read,
	"REDUNDANT":                  redundant,
	"REFERENCES":                 references,
	"REGEXP":                     regexpKwd,
	"RELEASE_LOCK":               releaseLock,
	"RENAME":                     rename,
	"REPEAT":                     repeat,
	"REPEATABLE":                 repeatable,
	"REPLACE":                    replace,
	"REVOKE":                     revoke,
	"RIGHT":                      right,
	"RLIKE":                      rlike,
	"ROLLBACK":                   rollback,
	"ROUND":                      round,
	"ROW":                        row,
	"ROW_FORMAT":                 rowFormat,
	"RTRIM":                      rtrim,
	"REVERSE":                    reverse,
	"SCHEMA":                     schema,
	"SCHEMAS":                    schemas,
	"SEC_TO_TIME":                secToTime,
	"SECOND":                     second,
	"SELECT":                     selectKwd,
	"SERIALIZABLE":               serializable,
	"SESSION":                    session,
	"SET":                        set,
	"SHARE":                      share,
	"SHOW":                       show,
	"SLEEP":                      sleep,
	"SIGN":                       sign,
	"SIGNED":                     signed,
	"SIN":                        sin,
	"SNAPSHOT":                   snapshot,
	"SOME":                       some,
	"SPACE":                      space,
	"SQRT":                       sqrt,
	"START":                      start,
	"STARTING":                   starting,
	"STATS_PERSISTENT":           statsPersistent,
	"STATUS":                     status,
	"SUBDATE":                    subDate,
	"SUBTIME":                    subTime,
	"STRCMP":                     strcmp,
	"STR_TO_DATE":                strToDate,
	"SUBSTR":                     substring,
	"SUBSTRING":                  substring,
	"SUBSTRING_INDEX":            substringIndex,
	"SUM":                        sum,
	"SYSDATE":                    sysDate,
	"TIDB":                       tidb,
	"TABLE":                      tableKwd,
	"TABLES":                     tables,
	"TAN":                        tan,
	"TERMINATED":                 terminated,
	"TIMEDIFF":                   timediff,
	"TIME_FORMAT":                timeFormat,
	"TIME_TO_SEC":                timeToSec,
	"TIMESTAMPADD":               timestampAdd,
	"TIMESTAMPDIFF":              timestampDiff,
	"THAN":                       than,
	"THEN":                       then,
	"TO":                         to,
	"TO_DAYS":                    toDays,
	"TO_SECONDS":                 toSeconds,
	"TRAILING":                   trailing,
	"TRANSACTION":                transaction,
	"TRIGGERS":                   triggers,
	"TRIM":                       trim,
	"TRUE":                       trueKwd,
	"TRUNCATE":                   truncate,
	"UNCOMMITTED":                uncommitted,
	"UNKNOWN":                    unknown,
	"UNION":                      union,
	"UNIQUE":                     unique,
	"UNLOCK":                     unlock,
	"UNSIGNED":                   unsigned,
	"UNIX_TIMESTAMP":             unixTimestamp,
	"UPDATE":                     update,
	"UPPER":                      upper,
	"UCASE":                      ucase,
	"UTC_TIME":                   utcTime,
	"USE":                        use,
	"USER":                       user,
	"USING":                      using,
	"VALUE":                      value,
	"VALUES":                     values,
	"VARIABLES":                  variables,
	"VERSION":                    version,
	"VIEW":                       view,
	"WARNINGS":                   warnings,
	"WEEK":                       week,
	"WEEKDAY":                    weekday,
	"WEEKOFYEAR":                 weekofyear,
	"WHEN":                       when,
	"WHERE":                      where,
	"WITH":                       with,
	"WRITE":                      write,
	"XOR":                        xor,
	"YEARWEEK":                   yearweek,
	"ZEROFILL":                   zerofill,
	"SQL_CALC_FOUND_ROWS":        calcFoundRows,
	"SQL_CACHE":                  sqlCache,
	"SQL_NO_CACHE":               sqlNoCache,
	"CURRENT_TIMESTAMP":          currentTs,
	"LOCALTIME":                  localTime,
	"LOCALTIMESTAMP":             localTs,
	"NOW":                        now,
	"TINY":                       tinyIntType,
	"TINYINT":                    tinyIntType,
	"SMALLINT":                   smallIntType,
	"MEDIUMINT":                  mediumIntType,
	"INT":                        intType,
	"INTEGER":                    integerType,
	"BIGINT":                     bigIntType,
	"BIT":                        bitType,
	"DECIMAL":                    decimalType,
	"NUMERIC":                    numericType,
	"FLOAT":                      floatType,
	"DOUBLE":                     doubleType,
	"PRECISION":                  precisionType,
	"REAL":                       realType,
	"DATE":                       dateType,
	"TIME":                       timeType,
	"DATETIME":                   datetimeType,
	"TIMESTAMP":                  timestampType,
	"YEAR":                       yearType,
	"CHAR":                       charType,
	"VARCHAR":                    varcharType,
	"BINARY":                     binaryType,
	"VARBINARY":                  varbinaryType,
	"TINYBLOB":                   tinyblobType,
	"BLOB":                       blobType,
	"MEDIUMBLOB":                 mediumblobType,
	"LONGBLOB":                   longblobType,
	"TINYTEXT":                   tinytextType,
	"TEXT":                       textType,
	"MEDIUMTEXT":                 mediumtextType,
	"LONGTEXT":                   longtextType,
	"BOOL":                       boolType,
	"BOOLEAN":                    booleanType,
	"SECOND_MICROSECOND":         secondMicrosecond,
	"MINUTE_MICROSECOND":         minuteMicrosecond,
	"MINUTE_SECOND":              minuteSecond,
	"HOUR_MICROSECOND":           hourMicrosecond,
	"HOUR_SECOND":                hourSecond,
	"HOUR_MINUTE":                hourMinute,
	"DAY_MICROSECOND":            dayMicrosecond,
	"DAY_SECOND":                 daySecond,
	"DAY_MINUTE":                 dayMinute,
	"DAY_HOUR":                   dayHour,
	"YEAR_MONTH":                 yearMonth,
	"RESTRICT":                   restrict,
	"CASCADE":                    cascade,
	"NO":                         no,
	"ACTION":                     action,
	"PARTITION":                  partition,
	"PARTITIONS":                 partitions,
	"RPAD":                       rpad,
	"BIT_LENGTH":                 bitLength,
	"CHAR_FUNC":                  charFunc,
	"CHAR_LENGTH":                charLength,
	"CHARACTER_LENGTH":           charLength,
	"CONV":                       conv,
	"BIT_XOR":                    bitXor,
	"BENCHMARK":                  benchmark,
	"COERCIBILITY":               coercibility,
	"ROW_COUNT":                  rowCount,
	"SESSION_USER":               sessionUser,
	"SYSTEM_USER":                systemUser,
	"CRC32":                      crc32,
	"ASYMMETRIC_DECRYPT":         asymmetricDecrypt,
	"ASYMMETRIC_DERIVE":          asymmetricDerive,
	"ASYMMETRIC_ENCRYPT":         asymmetricEncrypt,
	"ASYMMETRIC_SIGN":            asymmetricSign,
	"ASYMMETRIC_VERIFY":          asymmetricVerify,
	"COMPRESS":                   compress,
	"CREATE_ASYMMETRIC_PRIV_KEY": createAsymmetricPrivKey,
	"CREATE_ASYMMETRIC_PUB_KEY":  createAsymmetricPubKey,
	"CREATE_DH_PARAMETERS":       createDHParameters,
	"CREATE_DIGEST":              createDigest,
	"DECODE":                     decode,
	"DES_DECRYPT":                desDecrypt,
	"DES_ENCRYPT":                desEncrypt,
	"ENCODE":                     encode,
	"ENCRYPT":                    encrypt,
	"MD5":                        md5,
	"OLD_PASSWORD":               oldPassword,
	"RANDOM_BYTES":               randomBytes,
	"SHA1":                       sha1,
	"SHA":                        sha,
	"SHA2":                       sha2,
	"UNCOMPRESS":                 uncompress,
	"UNCOMPRESSED_LENGTH":        uncompressedLength,
	"VALIDATE_PASSWORD_STRENGTH": validatePasswordStrength,
	"ANY_VALUE":                  anyValue,
	"INET_ATON":                  inetAton,
	"INET_NTOA":                  inetNtoa,
	"INET6_ATON":                 inet6Aton,
	"INET6_NTOA":                 inet6Ntoa,
	"IS_FREE_LOCK":               isFreeLock,
	"IS_IPV4":                    isIPv4,
	"IS_IPV4_COMPAT":             isIPv4Compat,
	"IS_IPV4_MAPPED":             isIPv4Mapped,
	"IS_IPV6":                    isIPv6,
	"IS_USED_LOCK":               isUsedLock,
	"MASTER_POS_WAIT":            masterPosWait,
	"NAME_CONST":                 nameConst,
	"RELEASE_ALL_LOCKS":          releaseAllLocks,
	"UUID":                       uuid,
	"UUID_SHORT":                 uuidShort,
	"KILL":                       kill,
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
