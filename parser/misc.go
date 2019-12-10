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
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/parser/charset"
)

// CommentCodeVersion is used to track the highest version can be parsed in the comment with pattern /*T!00001 xxx */
type CommentCodeVersion int

const (
	CommentCodeNoVersion  CommentCodeVersion = iota
	CommentCodeAutoRandom CommentCodeVersion = 40000

	CommentCodeCurrentVersion
)

func (ccv CommentCodeVersion) String() string {
	return fmt.Sprintf("%05d", ccv)
}

func extractVersionCodeInComment(comment string) CommentCodeVersion {
	code, err := strconv.Atoi(specVersionCodeValue.FindString(comment))
	if err != nil {
		return CommentCodeNoVersion
	}
	return CommentCodeVersion(code)
}

// WrapStringWithCodeVersion convert a string `str` to `/*T!xxxxx str */`, where `xxxxx` is determined by CommentCodeVersion.
func WrapStringWithCodeVersion(str string, ccv CommentCodeVersion) string {
	return fmt.Sprintf("/*T!%05d %s */", ccv, str)
}

func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch rune) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentChar(ch rune) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || isIdentExtend(ch)
}

func isIdentExtend(ch rune) bool {
	return ch >= 0x80 && ch <= '\uffff'
}

func isUserVarChar(ch rune) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || ch == '.' || isIdentExtend(ch)
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
	initTokenByte('[', int('['))
	initTokenByte(']', int(']'))
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
	initTokenByte('{', int('{'))
	initTokenByte('}', int('}'))

	initTokenString("||", pipes)
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
	"ACCOUNT":                  account,
	"ACTION":                   action,
	"ADD":                      add,
	"ADDDATE":                  addDate,
	"ADVISE":                   advise,
	"ADMIN":                    admin,
	"AFTER":                    after,
	"AGAINST":                  against,
	"AGG_TO_COP":               hintAggToCop,
	"ALL":                      all,
	"ALGORITHM":                algorithm,
	"ALTER":                    alter,
	"ALWAYS":                   always,
	"ANALYZE":                  analyze,
	"AND":                      and,
	"ANY":                      any,
	"AS":                       as,
	"ASC":                      asc,
	"ASCII":                    ascii,
	"AUTO_INCREMENT":           autoIncrement,
	"AUTO_RANDOM":              autoRandom,
	"AVG":                      avg,
	"AVG_ROW_LENGTH":           avgRowLength,
	"BEGIN":                    begin,
	"BETWEEN":                  between,
	"BIGINT":                   bigIntType,
	"BINARY":                   binaryType,
	"BINLOG":                   binlog,
	"BIT":                      bitType,
	"BIT_AND":                  bitAnd,
	"BIT_OR":                   bitOr,
	"BIT_XOR":                  bitXor,
	"BLOB":                     blobType,
	"BLOCK":                    block,
	"BOOL":                     boolType,
	"BOOLEAN":                  booleanType,
	"BOTH":                     both,
	"BOUND":                    bound,
	"BTREE":                    btree,
	"BUCKETS":                  buckets,
	"BUILTINS":                 builtins,
	"BY":                       by,
	"BYTE":                     byteType,
	"CACHE":                    cache,
	"CANCEL":                   cancel,
	"CASCADE":                  cascade,
	"CASCADED":                 cascaded,
	"CASE":                     caseKwd,
	"CAST":                     cast,
	"CAPTURE":                  capture,
	"CHANGE":                   change,
	"CHAR":                     charType,
	"CHARACTER":                character,
	"CHARSET":                  charsetKwd,
	"CHECK":                    check,
	"CHECKSUM":                 checksum,
	"CIPHER":                   cipher,
	"CLEANUP":                  cleanup,
	"CLIENT":                   client,
	"CMSKETCH":                 cmSketch,
	"COALESCE":                 coalesce,
	"COLLATE":                  collate,
	"COLLATION":                collation,
	"COLUMN":                   column,
	"COLUMN_FORMAT":            columnFormat,
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
	"CONTEXT":                  context,
	"CONVERT":                  convert,
	"COPY":                     copyKwd,
	"COUNT":                    count,
	"CPU":                      cpu,
	"CREATE":                   create,
	"CROSS":                    cross,
	"CURRENT":                  current,
	"CURRENT_DATE":             currentDate,
	"CURRENT_TIME":             currentTime,
	"CURRENT_TIMESTAMP":        currentTs,
	"CURRENT_USER":             currentUser,
	"CURRENT_ROLE":             currentRole,
	"CURTIME":                  curTime,
	"CYCLE":                    cycle,
	"DATA":                     data,
	"DATABASE":                 database,
	"DATABASES":                databases,
	"DATE":                     dateType,
	"DATE_ADD":                 dateAdd,
	"DATE_SUB":                 dateSub,
	"DATETIME":                 datetimeType,
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
	"DEFINER":                  definer,
	"DELAY_KEY_WRITE":          delayKeyWrite,
	"DELAYED":                  delayed,
	"DELETE":                   deleteKwd,
	"DEPTH":                    depth,
	"DESC":                     desc,
	"DESCRIBE":                 describe,
	"DIRECTORY":                directory,
	"DISABLE":                  disable,
	"DISCARD":                  discard,
	"DISK":                     disk,
	"DISTINCT":                 distinct,
	"DISTINCTROW":              distinct,
	"DIV":                      div,
	"DO":                       do,
	"DOUBLE":                   doubleType,
	"DRAINER":                  drainer,
	"DROP":                     drop,
	"DUAL":                     dual,
	"DUPLICATE":                duplicate,
	"DYNAMIC":                  dynamic,
	"ELSE":                     elseKwd,
	"ENABLE":                   enable,
	"ENABLE_PLAN_CACHE":        hintEnablePlanCache,
	"ENCLOSED":                 enclosed,
	"ENCRYPTION":               encryption,
	"END":                      end,
	"ENFORCED":                 enforced,
	"ENGINE":                   engine,
	"ENGINES":                  engines,
	"ENUM":                     enum,
	"ESCAPE":                   escape,
	"ESCAPED":                  escaped,
	"EVENT":                    event,
	"EVENTS":                   events,
	"EVOLVE":                   evolve,
	"EXACT":                    exact,
	"EXCLUSIVE":                exclusive,
	"EXCEPT":                   except,
	"EXCHANGE":                 exchange,
	"EXECUTE":                  execute,
	"EXISTS":                   exists,
	"EXPANSION":                expansion,
	"EXPIRE":                   expire,
	"EXPLAIN":                  explain,
	"EXTENDED":                 extended,
	"EXTRACT":                  extract,
	"FALSE":                    falseKwd,
	"FAULTS":                   faultsSym,
	"FIELDS":                   fields,
	"FIRST":                    first,
	"FIXED":                    fixed,
	"FLOAT":                    floatType,
	"FLUSH":                    flush,
	"FLASHBACK":                flashback,
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
	"GROUP_CONCAT":             groupConcat,
	"HASH":                     hash,
	"HASH_AGG":                 hintHASHAGG,
	"HASH_JOIN":                hintHJ,
	"HAVING":                   having,
	"HIGH_PRIORITY":            highPriority,
	"HISTORY":                  history,
	"HOSTS":                    hosts,
	"HOUR":                     hour,
	"HOUR_MICROSECOND":         hourMicrosecond,
	"HOUR_MINUTE":              hourMinute,
	"HOUR_SECOND":              hourSecond,
	"IDENTIFIED":               identified,
	"IF":                       ifKwd,
	"IGNORE":                   ignore,
	"IGNORE_INDEX":             hintIgnoreIndex,
	"IMPORT":                   importKwd,
	"IN":                       in,
	"INCREMENT":                increment,
	"INCREMENTAL":              incremental,
	"INDEX":                    index,
	"INDEXES":                  indexes,
	"INFILE":                   infile,
	"INL_JOIN":                 hintINLJ,
	"INL_HASH_JOIN":            hintINLHJ,
	"INL_MERGE_JOIN":           hintINLMJ,
	"INNER":                    inner,
	"INPLACE":                  inplace,
	"INSTANT":                  instant,
	"INSERT":                   insert,
	"INSERT_METHOD":            insertMethod,
	"INT":                      intType,
	"INT1":                     int1Type,
	"INT2":                     int2Type,
	"INT3":                     int3Type,
	"INT4":                     int4Type,
	"INT8":                     int8Type,
	"IO":                       io,
	"IPC":                      ipc,
	"INTEGER":                  integerType,
	"INTERVAL":                 interval,
	"INTERNAL":                 internal,
	"INTO":                     into,
	"INVISIBLE":                invisible,
	"INVOKER":                  invoker,
	"IS":                       is,
	"ISSUER":                   issuer,
	"ISOLATION":                isolation,
	"JOBS":                     jobs,
	"JOB":                      job,
	"JOIN":                     join,
	"JSON":                     jsonType,
	"KEY":                      key,
	"KEY_BLOCK_SIZE":           keyBlockSize,
	"KEYS":                     keys,
	"KILL":                     kill,
	"LABELS":                   labels,
	"LANGUAGE":                 language,
	"LAST":                     last,
	"LEADING":                  leading,
	"LEFT":                     left,
	"LESS":                     less,
	"LEVEL":                    level,
	"LIKE":                     like,
	"LIMIT":                    limit,
	"LINES":                    lines,
	"LINEAR":                   linear,
	"LIST":                     list,
	"LOAD":                     load,
	"LOCAL":                    local,
	"LOCALTIME":                localTime,
	"LOCALTIMESTAMP":           localTs,
	"LOCATION":                 location,
	"LOCK":                     lock,
	"LOGS":                     logs,
	"LONG":                     long,
	"LONGBLOB":                 longblobType,
	"LONGTEXT":                 longtextType,
	"LOW_PRIORITY":             lowPriority,
	"MASTER":                   master,
	"MATCH":                    match,
	"MAX":                      max,
	"MAX_CONNECTIONS_PER_HOUR": maxConnectionsPerHour,
	"MAX_EXECUTION_TIME":       maxExecutionTime,
	"MAX_IDXNUM":               max_idxnum,
	"MAX_MINUTES":              max_minutes,
	"MAX_QUERIES_PER_HOUR":     maxQueriesPerHour,
	"MAX_ROWS":                 maxRows,
	"MAX_UPDATES_PER_HOUR":     maxUpdatesPerHour,
	"MAX_USER_CONNECTIONS":     maxUserConnections,
	"MAXVALUE":                 maxValue,
	"MEDIUMBLOB":               mediumblobType,
	"MEDIUMINT":                mediumIntType,
	"MEDIUMTEXT":               mediumtextType,
	"MEMORY":                   memory,
	"MEMORY_QUOTA":             hintMemoryQuota,
	"MERGE":                    merge,
	"MICROSECOND":              microsecond,
	"MIN":                      min,
	"MIN_ROWS":                 minRows,
	"MINUTE":                   minute,
	"MINUTE_MICROSECOND":       minuteMicrosecond,
	"MINUTE_SECOND":            minuteSecond,
	"MINVALUE":                 minValue,
	"MOD":                      mod,
	"MODE":                     mode,
	"MODIFY":                   modify,
	"MONTH":                    month,
	"NAMES":                    names,
	"NATIONAL":                 national,
	"NATURAL":                  natural,
	"NEVER":                    never,
	"NEXT_ROW_ID":              next_row_id,
	"NO":                       no,
	"NO_INDEX_MERGE":           hintNoIndexMerge,
	"NO_SWAP_JOIN_INPUTS":      hintNSJI,
	"NO_WRITE_TO_BINLOG":       noWriteToBinLog,
	"NOCACHE":                  nocache,
	"NOCYCLE":                  nocycle,
	"NODE_ID":                  nodeID,
	"NODE_STATE":               nodeState,
	"NODEGROUP":                nodegroup,
	"NOMAXVALUE":               nomaxvalue,
	"NOMINVALUE":               nominvalue,
	"NONE":                     none,
	"NOORDER":                  noorder,
	"NOT":                      not,
	"NOW":                      now,
	"NULL":                     null,
	"NULLS":                    nulls,
	"NUMERIC":                  numericType,
	"NCHAR":                    ncharType,
	"NVARCHAR":                 nvarcharType,
	"OFFSET":                   offset,
	"OLAP":                     hintOLAP,
	"OLTP":                     hintOLTP,
	"ON":                       on,
	"ONLY":                     only,
	"OPTIMISTIC":               optimistic,
	"OPTIMIZE":                 optimize,
	"OPTION":                   option,
	"OPTIONALLY":               optionally,
	"OR":                       or,
	"ORDER":                    order,
	"OUTER":                    outer,
	"PACK_KEYS":                packKeys,
	"PAGE":                     pageSym,
	"PARSER":                   parser,
	"PARTIAL":                  partial,
	"PARTITION":                partition,
	"PARTITIONING":             partitioning,
	"PARTITIONS":               partitions,
	"PASSWORD":                 password,
	"PESSIMISTIC":              pessimistic,
	"PER_TABLE":                per_table,
	"PER_DB":                   per_db,
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
	"PROFILE":                  profile,
	"PROFILES":                 profiles,
	"PUMP":                     pump,
	"QB_NAME":                  hintQBName,
	"QUARTER":                  quarter,
	"QUERY":                    query,
	"QUERY_TYPE":               hintQueryType,
	"QUERIES":                  queries,
	"QUICK":                    quick,
	"SHARD_ROW_ID_BITS":        shardRowIDBits,
	"PRE_SPLIT_REGIONS":        preSplitRegions,
	"RANGE":                    rangeKwd,
	"RECOVER":                  recover,
	"REBUILD":                  rebuild,
	"READ":                     read,
	"READ_CONSISTENT_REPLICA":  hintReadConsistentReplica,
	"READ_FROM_STORAGE":        hintReadFromStorage,
	"REAL":                     realType,
	"RECENT":                   recent,
	"REDUNDANT":                redundant,
	"REFERENCES":               references,
	"REGEXP":                   regexpKwd,
	"REGIONS":                  regions,
	"REGION":                   region,
	"RELOAD":                   reload,
	"REMOVE":                   remove,
	"RENAME":                   rename,
	"REORGANIZE":               reorganize,
	"REPAIR":                   repair,
	"REPEAT":                   repeat,
	"REPEATABLE":               repeatable,
	"REPLACE":                  replace,
	"RESPECT":                  respect,
	"REPLICA":                  replica,
	"REPLICATION":              replication,
	"REQUIRE":                  require,
	"RESTRICT":                 restrict,
	"REVERSE":                  reverse,
	"REVOKE":                   revoke,
	"RIGHT":                    right,
	"RLIKE":                    rlike,
	"ROLE":                     role,
	"ROLLBACK":                 rollback,
	"ROUTINE":                  routine,
	"ROW":                      row,
	"ROW_COUNT":                rowCount,
	"ROW_FORMAT":               rowFormat,
	"RTREE":                    rtree,
	"SAMPLES":                  samples,
	"SWAP_JOIN_INPUTS":         hintSJI,
	"SCHEMA":                   database,
	"SCHEMAS":                  databases,
	"SECOND":                   second,
	"SECONDARY_ENGINE":         secondaryEngine,
	"SECONDARY_LOAD":           secondaryLoad,
	"SECONDARY_UNLOAD":         secondaryUnload,
	"SECOND_MICROSECOND":       secondMicrosecond,
	"SECURITY":                 security,
	"SELECT":                   selectKwd,
	"SEQUENCE":                 sequence,
	"SERIAL":                   serial,
	"SERIALIZABLE":             serializable,
	"SESSION":                  session,
	"SET":                      set,
	"SEPARATOR":                separator,
	"SHARE":                    share,
	"SHARED":                   shared,
	"SHOW":                     show,
	"SHUTDOWN":                 shutdown,
	"SIGNED":                   signed,
	"SIMPLE":                   simple,
	"SLAVE":                    slave,
	"SLOW":                     slow,
	"SM_JOIN":                  hintSMJ,
	"SMALLINT":                 smallIntType,
	"SNAPSHOT":                 snapshot,
	"SOME":                     some,
	"SPATIAL":                  spatial,
	"SPLIT":                    split,
	"SQL":                      sql,
	"SQL_BIG_RESULT":           sqlBigResult,
	"SQL_BUFFER_RESULT":        sqlBufferResult,
	"SQL_CACHE":                sqlCache,
	"SQL_CALC_FOUND_ROWS":      sqlCalcFoundRows,
	"SQL_NO_CACHE":             sqlNoCache,
	"SQL_SMALL_RESULT":         sqlSmallResult,
	"SQL_TSI_DAY":              sqlTsiDay,
	"SQL_TSI_HOUR":             sqlTsiHour,
	"SQL_TSI_MINUTE":           sqlTsiMinute,
	"SQL_TSI_MONTH":            sqlTsiMonth,
	"SQL_TSI_QUARTER":          sqlTsiQuarter,
	"SQL_TSI_SECOND":           sqlTsiSecond,
	"SQL_TSI_WEEK":             sqlTsiWeek,
	"SQL_TSI_YEAR":             sqlTsiYear,
	"SOURCE":                   source,
	"SSL":                      ssl,
	"STALENESS":                staleness,
	"START":                    start,
	"STARTING":                 starting,
	"STATS":                    stats,
	"STATS_BUCKETS":            statsBuckets,
	"STATS_HISTOGRAMS":         statsHistograms,
	"STATS_HEALTHY":            statsHealthy,
	"STATS_META":               statsMeta,
	"STATS_AUTO_RECALC":        statsAutoRecalc,
	"STATS_PERSISTENT":         statsPersistent,
	"STATS_SAMPLE_PAGES":       statsSamplePages,
	"STATUS":                   status,
	"STORAGE":                  storage,
	"SWAPS":                    swaps,
	"SWITCHES":                 switchesSym,
	"SYSTEM_TIME":              systemTime,
	"OPEN":                     open,
	"STD":                      stddevPop,
	"STDDEV":                   stddevPop,
	"STDDEV_POP":               stddevPop,
	"STDDEV_SAMP":              stddevSamp,
	"STORED":                   stored,
	"STRAIGHT_JOIN":            straightJoin,
	"STREAM_AGG":               hintSTREAMAGG,
	"STRONG":                   strong,
	"SUBDATE":                  subDate,
	"SUBJECT":                  subject,
	"SUBPARTITION":             subpartition,
	"SUBPARTITIONS":            subpartitions,
	"SUBSTR":                   substring,
	"SUBSTRING":                substring,
	"SUM":                      sum,
	"SUPER":                    super,
	"TABLE":                    tableKwd,
	"TABLE_CHECKSUM":           tableChecksum,
	"TABLES":                   tables,
	"TABLESPACE":               tablespace,
	"TEMPORARY":                temporary,
	"TEMPTABLE":                temptable,
	"TERMINATED":               terminated,
	"TEXT":                     textType,
	"THAN":                     than,
	"THEN":                     then,
	"TIDB":                     tidb,
	"TIDB_HJ":                  hintHJ,
	"TIDB_INLJ":                hintINLJ,
	"TIDB_SMJ":                 hintSMJ,
	"TIKV":                     hintTiKV,
	"TIFLASH":                  hintTiFlash,
	"TIME":                     timeType,
	"TIMESTAMP":                timestampType,
	"TIMESTAMPADD":             timestampAdd,
	"TIMESTAMPDIFF":            timestampDiff,
	"TINYBLOB":                 tinyblobType,
	"TINYINT":                  tinyIntType,
	"TINYTEXT":                 tinytextType,
	"TO":                       to,
	"TOKUDB_DEFAULT":           tokudbDefault,
	"TOKUDB_FAST":              tokudbFast,
	"TOKUDB_LZMA":              tokudbLzma,
	"TOKUDB_QUICKLZ":           tokudbQuickLZ,
	"TOKUDB_SNAPPY":            tokudbSnappy,
	"TOKUDB_SMALL":             tokudbSmall,
	"TOKUDB_UNCOMPRESSED":      tokudbUncompressed,
	"TOKUDB_ZLIB":              tokudbZlib,
	"TOP":                      top,
	"TOPN":                     topn,
	"TRACE":                    trace,
	"TRADITIONAL":              traditional,
	"TRAILING":                 trailing,
	"TRANSACTION":              transaction,
	"TRIGGER":                  trigger,
	"TRIGGERS":                 triggers,
	"TRIM":                     trim,
	"TRUE":                     trueKwd,
	"TRUNCATE":                 truncate,
	"TYPE":                     tp,
	"UNBOUNDED":                unbounded,
	"UNCOMMITTED":              uncommitted,
	"UNICODE":                  unicodeSym,
	"UNDEFINED":                undefined,
	"UNION":                    union,
	"UNIQUE":                   unique,
	"UNKNOWN":                  unknown,
	"UNLOCK":                   unlock,
	"UNSIGNED":                 unsigned,
	"UNTIL":                    until,
	"UPDATE":                   update,
	"USAGE":                    usage,
	"USE":                      use,
	"USE_INDEX":                hintUseIndex,
	"USE_INDEX_MERGE":          hintUseIndexMerge,
	"USE_PLAN_CACHE":           hintUsePlanCache,
	"USE_TOJA":                 hintUseToja,
	"USER":                     user,
	"USING":                    using,
	"UTC_DATE":                 utcDate,
	"UTC_TIME":                 utcTime,
	"UTC_TIMESTAMP":            utcTimestamp,
	"VALIDATION":               validation,
	"VALUE":                    value,
	"VALUES":                   values,
	"VARBINARY":                varbinaryType,
	"VARCHAR":                  varcharType,
	"VARCHARACTER":             varcharacter,
	"VARIABLES":                variables,
	"VARIANCE":                 varPop,
	"VARYING":                  varying,
	"VAR_POP":                  varPop,
	"VAR_SAMP":                 varSamp,
	"VIEW":                     view,
	"VIRTUAL":                  virtual,
	"VISIBLE":                  visible,
	"WARNINGS":                 warnings,
	"ERRORS":                   identSQLErrors,
	"WEEK":                     week,
	"WHEN":                     when,
	"WHERE":                    where,
	"WIDTH":                    width,
	"WITH":                     with,
	"WITHOUT":                  without,
	"WRITE":                    write,
	"XOR":                      xor,
	"X509":                     x509,
	"YEAR":                     yearType,
	"YEAR_MONTH":               yearMonth,
	"ZEROFILL":                 zerofill,
	"BINDING":                  binding,
	"BINDINGS":                 bindings,
	"EXPR_PUSHDOWN_BLACKLIST":  exprPushdownBlacklist,
	"OPT_RULE_BLACKLIST":       optRuleBlacklist,
	"NOWAIT":                   nowait,
}

// See https://dev.mysql.com/doc/refman/5.7/en/function-resolution.html for details
var btFuncTokenMap = map[string]int{
	"ADDDATE":      builtinAddDate,
	"BIT_AND":      builtinBitAnd,
	"BIT_OR":       builtinBitOr,
	"BIT_XOR":      builtinBitXor,
	"CAST":         builtinCast,
	"COUNT":        builtinCount,
	"CURDATE":      builtinCurDate,
	"CURTIME":      builtinCurTime,
	"DATE_ADD":     builtinDateAdd,
	"DATE_SUB":     builtinDateSub,
	"EXTRACT":      builtinExtract,
	"GROUP_CONCAT": builtinGroupConcat,
	"MAX":          builtinMax,
	"MID":          builtinSubstring,
	"MIN":          builtinMin,
	"NOW":          builtinNow,
	"POSITION":     builtinPosition,
	"SESSION_USER": builtinUser,
	"STD":          builtinStddevPop,
	"STDDEV":       builtinStddevPop,
	"STDDEV_POP":   builtinStddevPop,
	"STDDEV_SAMP":  builtinStddevSamp,
	"SUBDATE":      builtinSubDate,
	"SUBSTR":       builtinSubstring,
	"SUBSTRING":    builtinSubstring,
	"SUM":          builtinSum,
	"SYSDATE":      builtinSysDate,
	"SYSTEM_USER":  builtinUser,
	"TRIM":         builtinTrim,
	"VARIANCE":     builtinVarPop,
	"VAR_POP":      builtinVarPop,
	"VAR_SAMP":     builtinVarSamp,
}

var windowFuncTokenMap = map[string]int{
	"CUME_DIST":    cumeDist,
	"DENSE_RANK":   denseRank,
	"FIRST_VALUE":  firstValue,
	"GROUPS":       groups,
	"LAG":          lag,
	"LAST_VALUE":   lastValue,
	"LEAD":         lead,
	"NTH_VALUE":    nthValue,
	"NTILE":        ntile,
	"OVER":         over,
	"PERCENT_RANK": percentRank,
	"RANK":         rank,
	"ROWS":         rows,
	"ROW_NUMBER":   rowNumber,
	"WINDOW":       window,
}

// aliases are strings directly map to another string and use the same token.
var aliases = map[string]string{
	"SCHEMA":    "DATABASE",
	"SCHEMAS":   "DATABASES",
	"DEC":       "DECIMAL",
	"SUBSTR":    "SUBSTRING",
	"TIDB_HJ":   "HASH_JOIN",
	"TIDB_INLJ": "INL_JOIN",
	"TIDB_SMJ":  "SM_JOIN",
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

	checkBtFuncToken := false
	if s.r.peek() == '(' {
		checkBtFuncToken = true
	} else if s.sqlMode.HasIgnoreSpaceMode() {
		s.skipWhitespace()
		if s.r.peek() == '(' {
			checkBtFuncToken = true
		}
	}
	if checkBtFuncToken {
		if tok := btFuncTokenMap[string(data)]; tok != 0 {
			return tok
		}
	}
	tok, ok := tokenMap[string(data)]
	if !ok && s.supportWindowFunc {
		tok = windowFuncTokenMap[string(data)]
	}
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
