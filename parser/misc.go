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

	"github.com/pingcap/parser/charset"
)

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
	initTokenFunc("*", startWithStar)
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

// tokenMap is a map of known identifiers to the parser token ID.
// Please try to keep the map in alphabetical order.
var tokenMap = map[string]int{
	"ACCOUNT":                    account,
	"ACTION":                     action,
	"ADD":                        add,
	"ADDDATE":                    addDate,
	"ADMIN":                      admin,
	"ADVISE":                     advise,
	"AFTER":                      after,
	"AGAINST":                    against,
	"AGO":                        ago,
	"ALGORITHM":                  algorithm,
	"ALL":                        all,
	"ALTER":                      alter,
	"ALWAYS":                     always,
	"ANALYZE":                    analyze,
	"AND":                        and,
	"ANY":                        any,
	"AS":                         as,
	"ASC":                        asc,
	"ASCII":                      ascii,
	"AUTO_INCREMENT":             autoIncrement,
	"AUTO_RANDOM":                autoRandom,
	"AVG_ROW_LENGTH":             avgRowLength,
	"AVG":                        avg,
	"BACKUP":                     backup,
	"BEGIN":                      begin,
	"BETWEEN":                    between,
	"BIGINT":                     bigIntType,
	"BINARY":                     binaryType,
	"BINDING":                    binding,
	"BINDINGS":                   bindings,
	"BINLOG":                     binlog,
	"BIT_AND":                    bitAnd,
	"BIT_OR":                     bitOr,
	"BIT_XOR":                    bitXor,
	"BIT":                        bitType,
	"BLOB":                       blobType,
	"BLOCK":                      block,
	"BOOL":                       boolType,
	"BOOLEAN":                    booleanType,
	"BOTH":                       both,
	"BOUND":                      bound,
	"BTREE":                      btree,
	"BUCKETS":                    buckets,
	"BUILTINS":                   builtins,
	"BY":                         by,
	"BYTE":                       byteType,
	"CACHE":                      cache,
	"CANCEL":                     cancel,
	"CAPTURE":                    capture,
	"CASCADE":                    cascade,
	"CASCADED":                   cascaded,
	"CASE":                       caseKwd,
	"CAST":                       cast,
	"CHAIN":                      chain,
	"CHANGE":                     change,
	"CHAR":                       charType,
	"CHARACTER":                  character,
	"CHARSET":                    charsetKwd,
	"CHECK":                      check,
	"CHECKSUM":                   checksum,
	"CIPHER":                     cipher,
	"CLEANUP":                    cleanup,
	"CLIENT":                     client,
	"CMSKETCH":                   cmSketch,
	"COALESCE":                   coalesce,
	"COLLATE":                    collate,
	"COLLATION":                  collation,
	"COLUMN_FORMAT":              columnFormat,
	"COLUMN":                     column,
	"COLUMNS":                    columns,
	"COMMENT":                    comment,
	"COMMIT":                     commit,
	"COMMITTED":                  committed,
	"COMPACT":                    compact,
	"COMPRESSED":                 compressed,
	"COMPRESSION":                compression,
	"CONCURRENCY":                concurrency,
	"CONFIG":                     config,
	"CONNECTION":                 connection,
	"CONSISTENT":                 consistent,
	"CONSTRAINT":                 constraint,
	"CONTEXT":                    context,
	"CONVERT":                    convert,
	"COPY":                       copyKwd,
	"COUNT":                      count,
	"CPU":                        cpu,
	"CREATE":                     create,
	"CROSS":                      cross,
	"CURRENT_DATE":               currentDate,
	"CURRENT_ROLE":               currentRole,
	"CURRENT_TIME":               currentTime,
	"CURRENT_TIMESTAMP":          currentTs,
	"CURRENT_USER":               currentUser,
	"CURRENT":                    current,
	"CURTIME":                    curTime,
	"CYCLE":                      cycle,
	"DATA":                       data,
	"DATABASE":                   database,
	"DATABASES":                  databases,
	"DATE_ADD":                   dateAdd,
	"DATE_SUB":                   dateSub,
	"DATE":                       dateType,
	"DATETIME":                   datetimeType,
	"DAY_HOUR":                   dayHour,
	"DAY_MICROSECOND":            dayMicrosecond,
	"DAY_MINUTE":                 dayMinute,
	"DAY_SECOND":                 daySecond,
	"DAY":                        day,
	"DDL":                        ddl,
	"DEALLOCATE":                 deallocate,
	"DEC":                        decimalType,
	"DECIMAL":                    decimalType,
	"DEFAULT":                    defaultKwd,
	"DEFINER":                    definer,
	"DELAY_KEY_WRITE":            delayKeyWrite,
	"DELAYED":                    delayed,
	"DELETE":                     deleteKwd,
	"DEPTH":                      depth,
	"DESC":                       desc,
	"DESCRIBE":                   describe,
	"DIRECTORY":                  directory,
	"DISABLE":                    disable,
	"DISCARD":                    discard,
	"DISK":                       disk,
	"DISTINCT":                   distinct,
	"DISTINCTROW":                distinct,
	"DIV":                        div,
	"DO":                         do,
	"DOUBLE":                     doubleType,
	"DRAINER":                    drainer,
	"DROP":                       drop,
	"DUAL":                       dual,
	"DUPLICATE":                  duplicate,
	"DYNAMIC":                    dynamic,
	"ELSE":                       elseKwd,
	"ENABLE":                     enable,
	"ENCLOSED":                   enclosed,
	"ENCRYPTION":                 encryption,
	"END":                        end,
	"ENFORCED":                   enforced,
	"ENGINE":                     engine,
	"ENGINES":                    engines,
	"ENUM":                       enum,
	"ERROR":                      errorKwd,
	"ERRORS":                     identSQLErrors,
	"ESCAPE":                     escape,
	"ESCAPED":                    escaped,
	"EVENT":                      event,
	"EVENTS":                     events,
	"EVOLVE":                     evolve,
	"EXACT":                      exact,
	"EXCEPT":                     except,
	"EXCHANGE":                   exchange,
	"EXCLUSIVE":                  exclusive,
	"EXECUTE":                    execute,
	"EXISTS":                     exists,
	"EXPANSION":                  expansion,
	"EXPIRE":                     expire,
	"EXPLAIN":                    explain,
	"EXPR_PUSHDOWN_BLACKLIST":    exprPushdownBlacklist,
	"EXTENDED":                   extended,
	"EXTRACT":                    extract,
	"FALSE":                      falseKwd,
	"FAULTS":                     faultsSym,
	"FIELDS":                     fields,
	"FILE":                       file,
	"FIRST":                      first,
	"FIXED":                      fixed,
	"FLASHBACK":                  flashback,
	"FLOAT":                      floatType,
	"FLUSH":                      flush,
	"FOLLOWING":                  following,
	"FOR":                        forKwd,
	"FORCE":                      force,
	"FOREIGN":                    foreign,
	"FORMAT":                     format,
	"FROM":                       from,
	"FULL":                       full,
	"FULLTEXT":                   fulltext,
	"FUNCTION":                   function,
	"GCS_CREDENTIALS_FILE":       gcsCredentialsFile,
	"GCS_ENDPOINT":               gcsEndpoint,
	"GCS_PREDEFINED_ACL":         gcsPredefinedACL,
	"GCS_STORAGE_CLASS":          gcsStorageClass,
	"GENERAL":                    general,
	"GENERATED":                  generated,
	"GET_FORMAT":                 getFormat,
	"GLOBAL":                     global,
	"GRANT":                      grant,
	"GRANTS":                     grants,
	"GROUP_CONCAT":               groupConcat,
	"GROUP":                      group,
	"HASH":                       hash,
	"HAVING":                     having,
	"HIGH_PRIORITY":              highPriority,
	"HISTORY":                    history,
	"HOSTS":                      hosts,
	"HOUR_MICROSECOND":           hourMicrosecond,
	"HOUR_MINUTE":                hourMinute,
	"HOUR_SECOND":                hourSecond,
	"HOUR":                       hour,
	"IDENTIFIED":                 identified,
	"IF":                         ifKwd,
	"IGNORE":                     ignore,
	"IMPORT":                     importKwd,
	"IN":                         in,
	"INCREMENT":                  increment,
	"INCREMENTAL":                incremental,
	"INDEX":                      index,
	"INDEXES":                    indexes,
	"INFILE":                     infile,
	"INNER":                      inner,
	"INPLACE":                    inplace,
	"INSERT_METHOD":              insertMethod,
	"INSERT":                     insert,
	"INSTANCE":                   instance,
	"INSTANT":                    instant,
	"INT":                        intType,
	"INT1":                       int1Type,
	"INT2":                       int2Type,
	"INT3":                       int3Type,
	"INT4":                       int4Type,
	"INT8":                       int8Type,
	"INTEGER":                    integerType,
	"INTERNAL":                   internal,
	"INTERVAL":                   interval,
	"INTO":                       into,
	"INVISIBLE":                  invisible,
	"INVOKER":                    invoker,
	"IO":                         io,
	"IPC":                        ipc,
	"IS":                         is,
	"ISOLATION":                  isolation,
	"ISSUER":                     issuer,
	"JOB":                        job,
	"JOBS":                       jobs,
	"JOIN":                       join,
	"JSON_OBJECTAGG":             jsonObjectAgg,
	"JSON":                       jsonType,
	"KEY_BLOCK_SIZE":             keyBlockSize,
	"KEY":                        key,
	"KEYS":                       keys,
	"KILL":                       kill,
	"LABELS":                     labels,
	"LANGUAGE":                   language,
	"LAST":                       last,
	"LASTVAL":                    lastval,
	"LEADING":                    leading,
	"LEFT":                       left,
	"LESS":                       less,
	"LEVEL":                      level,
	"LIKE":                       like,
	"LIMIT":                      limit,
	"LINEAR":                     linear,
	"LINES":                      lines,
	"LIST":                       list,
	"LOAD":                       load,
	"LOCAL":                      local,
	"LOCALTIME":                  localTime,
	"LOCALTIMESTAMP":             localTs,
	"LOCATION":                   location,
	"LOCK":                       lock,
	"LOGS":                       logs,
	"LONG":                       long,
	"LONGBLOB":                   longblobType,
	"LONGTEXT":                   longtextType,
	"LOW_PRIORITY":               lowPriority,
	"MASTER":                     master,
	"MATCH":                      match,
	"MAX_CONNECTIONS_PER_HOUR":   maxConnectionsPerHour,
	"MAX_IDXNUM":                 max_idxnum,
	"MAX_MINUTES":                max_minutes,
	"MAX_QUERIES_PER_HOUR":       maxQueriesPerHour,
	"MAX_ROWS":                   maxRows,
	"MAX_UPDATES_PER_HOUR":       maxUpdatesPerHour,
	"MAX_USER_CONNECTIONS":       maxUserConnections,
	"MAX":                        max,
	"MAXVALUE":                   maxValue,
	"MB":                         mb,
	"MEDIUMBLOB":                 mediumblobType,
	"MEDIUMINT":                  mediumIntType,
	"MEDIUMTEXT":                 mediumtextType,
	"MEMORY":                     memory,
	"MERGE":                      merge,
	"MICROSECOND":                microsecond,
	"MIN_ROWS":                   minRows,
	"MIN":                        min,
	"MINUTE_MICROSECOND":         minuteMicrosecond,
	"MINUTE_SECOND":              minuteSecond,
	"MINUTE":                     minute,
	"MINVALUE":                   minValue,
	"MOD":                        mod,
	"MODE":                       mode,
	"MODIFY":                     modify,
	"MONTH":                      month,
	"NAMES":                      names,
	"NATIONAL":                   national,
	"NATURAL":                    natural,
	"NCHAR":                      ncharType,
	"NEVER":                      never,
	"NEXT_ROW_ID":                next_row_id,
	"NEXT":                       next,
	"NEXTVAL":                    nextval,
	"NO_WRITE_TO_BINLOG":         noWriteToBinLog,
	"NO":                         no,
	"NOCACHE":                    nocache,
	"NOCYCLE":                    nocycle,
	"NODE_ID":                    nodeID,
	"NODE_STATE":                 nodeState,
	"NODEGROUP":                  nodegroup,
	"NOMAXVALUE":                 nomaxvalue,
	"NOMINVALUE":                 nominvalue,
	"NONE":                       none,
	"NOORDER":                    noorder,
	"NOT":                        not,
	"NOW":                        now,
	"NOWAIT":                     nowait,
	"NULL":                       null,
	"NULLS":                      nulls,
	"NUMERIC":                    numericType,
	"NVARCHAR":                   nvarcharType,
	"OFFSET":                     offset,
	"ON":                         on,
	"ONLINE":                     online,
	"ONLY":                       only,
	"OPEN":                       open,
	"OPT_RULE_BLACKLIST":         optRuleBlacklist,
	"OPTIMISTIC":                 optimistic,
	"OPTIMIZE":                   optimize,
	"OPTION":                     option,
	"OPTIONALLY":                 optionally,
	"OR":                         or,
	"ORDER":                      order,
	"OUTER":                      outer,
	"OUTFILE":                    outfile,
	"PACK_KEYS":                  packKeys,
	"PAGE":                       pageSym,
	"PARSER":                     parser,
	"PARTIAL":                    partial,
	"PARTITION":                  partition,
	"PARTITIONING":               partitioning,
	"PARTITIONS":                 partitions,
	"PASSWORD":                   password,
	"PER_DB":                     per_db,
	"PER_TABLE":                  per_table,
	"PESSIMISTIC":                pessimistic,
	"PLUGINS":                    plugins,
	"POSITION":                   position,
	"PRE_SPLIT_REGIONS":          preSplitRegions,
	"PRECEDING":                  preceding,
	"PRECISION":                  precisionType,
	"PREPARE":                    prepare,
	"PRIMARY":                    primary,
	"PRIVILEGES":                 privileges,
	"PROCEDURE":                  procedure,
	"PROCESS":                    process,
	"PROCESSLIST":                processlist,
	"PROFILE":                    profile,
	"PROFILES":                   profiles,
	"PUMP":                       pump,
	"QUARTER":                    quarter,
	"QUERIES":                    queries,
	"QUERY":                      query,
	"QUICK":                      quick,
	"RANGE":                      rangeKwd,
	"RATE_LIMIT":                 rateLimit,
	"READ":                       read,
	"REAL":                       realType,
	"REBUILD":                    rebuild,
	"RECENT":                     recent,
	"RECOVER":                    recover,
	"REDUNDANT":                  redundant,
	"REFERENCES":                 references,
	"REGEXP":                     regexpKwd,
	"REGION":                     region,
	"REGIONS":                    regions,
	"RELEASE":                    release,
	"RELOAD":                     reload,
	"REMOVE":                     remove,
	"RENAME":                     rename,
	"REORGANIZE":                 reorganize,
	"REPAIR":                     repair,
	"REPEAT":                     repeat,
	"REPEATABLE":                 repeatable,
	"REPLACE":                    replace,
	"REPLICA":                    replica,
	"REPLICATION":                replication,
	"REQUIRE":                    require,
	"RESPECT":                    respect,
	"RESTORE":                    restore,
	"RESTRICT":                   restrict,
	"REVERSE":                    reverse,
	"REVOKE":                     revoke,
	"RIGHT":                      right,
	"RLIKE":                      rlike,
	"ROLE":                       role,
	"ROLLBACK":                   rollback,
	"ROUTINE":                    routine,
	"ROW_COUNT":                  rowCount,
	"ROW_FORMAT":                 rowFormat,
	"ROW":                        row,
	"RTREE":                      rtree,
	"S3_ACCESS_KEY":              s3AccessKey,
	"S3_ACL":                     s3ACL,
	"S3_ENDPOINT":                s3Endpoint,
	"S3_FORCE_PATH_STYLE":        s3ForcePathStyle,
	"S3_PROVIDER":                s3Provider,
	"S3_REGION":                  s3Region,
	"S3_SECRET_ACCESS_KEY":       s3SecretAccessKey,
	"S3_SSE":                     s3SSE,
	"S3_STORAGE_CLASS":           s3StorageClass,
	"S3_USE_ACCELERATE_ENDPOINT": s3UseAccelerateEndpoint,
	"SAMPLES":                    samples,
	"SCHEMA":                     database,
	"SCHEMAS":                    databases,
	"SECOND_MICROSECOND":         secondMicrosecond,
	"SECOND":                     second,
	"SECONDARY_ENGINE":           secondaryEngine,
	"SECONDARY_LOAD":             secondaryLoad,
	"SECONDARY_UNLOAD":           secondaryUnload,
	"SECURITY":                   security,
	"SELECT":                     selectKwd,
	"SEND_CREDENTIALS_TO_TIKV":   sendCredentialsToTiKV,
	"SEPARATOR":                  separator,
	"SEQUENCE":                   sequence,
	"SERIAL":                     serial,
	"SERIALIZABLE":               serializable,
	"SESSION":                    session,
	"SET":                        set,
	"SETVAL":                     setval,
	"SHARD_ROW_ID_BITS":          shardRowIDBits,
	"SHARE":                      share,
	"SHARED":                     shared,
	"SHOW":                       show,
	"SHUTDOWN":                   shutdown,
	"SIGNED":                     signed,
	"SIMPLE":                     simple,
	"SLAVE":                      slave,
	"SLOW":                       slow,
	"SMALLINT":                   smallIntType,
	"SNAPSHOT":                   snapshot,
	"SOME":                       some,
	"SOURCE":                     source,
	"SPATIAL":                    spatial,
	"SPLIT":                      split,
	"SQL_BIG_RESULT":             sqlBigResult,
	"SQL_BUFFER_RESULT":          sqlBufferResult,
	"SQL_CACHE":                  sqlCache,
	"SQL_CALC_FOUND_ROWS":        sqlCalcFoundRows,
	"SQL_NO_CACHE":               sqlNoCache,
	"SQL_SMALL_RESULT":           sqlSmallResult,
	"SQL_TSI_DAY":                sqlTsiDay,
	"SQL_TSI_HOUR":               sqlTsiHour,
	"SQL_TSI_MINUTE":             sqlTsiMinute,
	"SQL_TSI_MONTH":              sqlTsiMonth,
	"SQL_TSI_QUARTER":            sqlTsiQuarter,
	"SQL_TSI_SECOND":             sqlTsiSecond,
	"SQL_TSI_WEEK":               sqlTsiWeek,
	"SQL_TSI_YEAR":               sqlTsiYear,
	"SQL":                        sql,
	"SSL":                        ssl,
	"STALENESS":                  staleness,
	"START":                      start,
	"STARTING":                   starting,
	"STATS_AUTO_RECALC":          statsAutoRecalc,
	"STATS_BUCKETS":              statsBuckets,
	"STATS_HEALTHY":              statsHealthy,
	"STATS_HISTOGRAMS":           statsHistograms,
	"STATS_META":                 statsMeta,
	"STATS_PERSISTENT":           statsPersistent,
	"STATS_SAMPLE_PAGES":         statsSamplePages,
	"STATS":                      stats,
	"STATUS":                     status,
	"STD":                        stddevPop,
	"STDDEV_POP":                 stddevPop,
	"STDDEV_SAMP":                stddevSamp,
	"STDDEV":                     stddevPop,
	"STORAGE":                    storage,
	"STORED":                     stored,
	"STRAIGHT_JOIN":              straightJoin,
	"STRONG":                     strong,
	"SUBDATE":                    subDate,
	"SUBJECT":                    subject,
	"SUBPARTITION":               subpartition,
	"SUBPARTITIONS":              subpartitions,
	"SUBSTR":                     substring,
	"SUBSTRING":                  substring,
	"SUM":                        sum,
	"SUPER":                      super,
	"SWAPS":                      swaps,
	"SWITCHES":                   switchesSym,
	"SYSTEM_TIME":                systemTime,
	"TABLE_CHECKSUM":             tableChecksum,
	"TABLE":                      tableKwd,
	"TABLES":                     tables,
	"TABLESPACE":                 tablespace,
	"TEMPORARY":                  temporary,
	"TEMPTABLE":                  temptable,
	"TERMINATED":                 terminated,
	"TEXT":                       textType,
	"THAN":                       than,
	"THEN":                       then,
	"TIDB":                       tidb,
	"TIFLASH":                    tiFlash,
	"TIME":                       timeType,
	"TIMESTAMP_ORACLE":           timestampOracle,
	"TIMESTAMP":                  timestampType,
	"TIMESTAMPADD":               timestampAdd,
	"TIMESTAMPDIFF":              timestampDiff,
	"TINYBLOB":                   tinyblobType,
	"TINYINT":                    tinyIntType,
	"TINYTEXT":                   tinytextType,
	"TLS":                        tls,
	"TO":                         to,
	"TOKUDB_DEFAULT":             tokudbDefault,
	"TOKUDB_FAST":                tokudbFast,
	"TOKUDB_LZMA":                tokudbLzma,
	"TOKUDB_QUICKLZ":             tokudbQuickLZ,
	"TOKUDB_SMALL":               tokudbSmall,
	"TOKUDB_SNAPPY":              tokudbSnappy,
	"TOKUDB_UNCOMPRESSED":        tokudbUncompressed,
	"TOKUDB_ZLIB":                tokudbZlib,
	"TOP":                        top,
	"TOPN":                       topn,
	"TRACE":                      trace,
	"TRADITIONAL":                traditional,
	"TRAILING":                   trailing,
	"TRANSACTION":                transaction,
	"TRIGGER":                    trigger,
	"TRIGGERS":                   triggers,
	"TRIM":                       trim,
	"TRUE":                       trueKwd,
	"TRUNCATE":                   truncate,
	"TYPE":                       tp,
	"UNBOUNDED":                  unbounded,
	"UNCOMMITTED":                uncommitted,
	"UNDEFINED":                  undefined,
	"UNICODE":                    unicodeSym,
	"UNION":                      union,
	"UNIQUE":                     unique,
	"UNKNOWN":                    unknown,
	"UNLOCK":                     unlock,
	"UNSIGNED":                   unsigned,
	"UNTIL":                      until,
	"UPDATE":                     update,
	"USAGE":                      usage,
	"USE":                        use,
	"USER":                       user,
	"USING":                      using,
	"UTC_DATE":                   utcDate,
	"UTC_TIME":                   utcTime,
	"UTC_TIMESTAMP":              utcTimestamp,
	"VALIDATION":                 validation,
	"VALUE":                      value,
	"VALUES":                     values,
	"VAR_POP":                    varPop,
	"VAR_SAMP":                   varSamp,
	"VARBINARY":                  varbinaryType,
	"VARCHAR":                    varcharType,
	"VARCHARACTER":               varcharacter,
	"VARIABLES":                  variables,
	"VARIANCE":                   varPop,
	"VARYING":                    varying,
	"VIEW":                       view,
	"VIRTUAL":                    virtual,
	"VISIBLE":                    visible,
	"WARNINGS":                   warnings,
	"WEEK":                       week,
	"WEIGHT_STRING":              weightString,
	"WHEN":                       when,
	"WHERE":                      where,
	"WIDTH":                      width,
	"WITH":                       with,
	"WITHOUT":                    without,
	"WRITE":                      write,
	"X509":                       x509,
	"XOR":                        xor,
	"YEAR_MONTH":                 yearMonth,
	"YEAR":                       yearType,
	"ZEROFILL":                   zerofill,
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
	"SCHEMA":  "DATABASE",
	"SCHEMAS": "DATABASES",
	"DEC":     "DECIMAL",
	"SUBSTR":  "SUBSTRING",
}

// hintedTokens is a set of tokens which recognizes a hint.
// According to https://dev.mysql.com/doc/refman/8.0/en/optimizer-hints.html,
// only SELECT, INSERT, REPLACE, UPDATE and DELETE accept optimizer hints.
// additionally we support CREATE and PARTITION for hints at table creation.
var hintedTokens = map[int]struct{}{
	selectKwd: {},
	insert:    {},
	replace:   {},
	update:    {},
	deleteKwd: {},
	create:    {},
	partition: {},
}

var hintTokenMap = map[string]int{
	// MySQL 8.0 hint names
	"JOIN_FIXED_ORDER":      hintJoinFixedOrder,
	"JOIN_ORDER":            hintJoinOrder,
	"JOIN_PREFIX":           hintJoinPrefix,
	"JOIN_SUFFIX":           hintJoinSuffix,
	"BKA":                   hintBKA,
	"NO_BKA":                hintNoBKA,
	"BNL":                   hintBNL,
	"NO_BNL":                hintNoBNL,
	"HASH_JOIN":             hintHashJoin,
	"NO_HASH_JOIN":          hintNoHashJoin,
	"MERGE":                 hintMerge,
	"NO_MERGE":              hintNoMerge,
	"INDEX_MERGE":           hintIndexMerge,
	"NO_INDEX_MERGE":        hintNoIndexMerge,
	"MRR":                   hintMRR,
	"NO_MRR":                hintNoMRR,
	"NO_ICP":                hintNoICP,
	"NO_RANGE_OPTIMIZATION": hintNoRangeOptimization,
	"SKIP_SCAN":             hintSkipScan,
	"NO_SKIP_SCAN":          hintNoSkipScan,
	"SEMIJOIN":              hintSemijoin,
	"NO_SEMIJOIN":           hintNoSemijoin,
	"MAX_EXECUTION_TIME":    hintMaxExecutionTime,
	"SET_VAR":               hintSetVar,
	"RESOURCE_GROUP":        hintResourceGroup,
	"QB_NAME":               hintQBName,

	// TiDB hint names
	"AGG_TO_COP":              hintAggToCop,
	"IGNORE_PLAN_CACHE":       hintIgnorePlanCache,
	"HASH_AGG":                hintHashAgg,
	"IGNORE_INDEX":            hintIgnoreIndex,
	"INL_HASH_JOIN":           hintInlHashJoin,
	"INL_JOIN":                hintInlJoin,
	"INL_MERGE_JOIN":          hintInlMergeJoin,
	"MEMORY_QUOTA":            hintMemoryQuota,
	"NO_SWAP_JOIN_INPUTS":     hintNoSwapJoinInputs,
	"QUERY_TYPE":              hintQueryType,
	"READ_CONSISTENT_REPLICA": hintReadConsistentReplica,
	"READ_FROM_STORAGE":       hintReadFromStorage,
	"MERGE_JOIN":              hintSMJoin,
	"STREAM_AGG":              hintStreamAgg,
	"SWAP_JOIN_INPUTS":        hintSwapJoinInputs,
	"USE_INDEX_MERGE":         hintUseIndexMerge,
	"USE_INDEX":               hintUseIndex,
	"USE_PLAN_CACHE":          hintUsePlanCache,
	"USE_TOJA":                hintUseToja,
	"TIME_RANGE":              hintTimeRange,
	"USE_CASCADES":            hintUseCascades,

	// TiDB hint aliases
	"TIDB_HJ":   hintHashJoin,
	"TIDB_INLJ": hintInlJoin,
	"TIDB_SMJ":  hintSMJoin,

	// Other keywords
	"OLAP":            hintOLAP,
	"OLTP":            hintOLTP,
	"TIKV":            hintTiKV,
	"TIFLASH":         hintTiFlash,
	"FALSE":           hintFalse,
	"TRUE":            hintTrue,
	"MB":              hintMB,
	"GB":              hintGB,
	"DUPSWEEDOUT":     hintDupsWeedOut,
	"FIRSTMATCH":      hintFirstMatch,
	"LOOSESCAN":       hintLooseScan,
	"MATERIALIZATION": hintMaterialization,
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

// SpecialCommentsController controls whether special comments like `/*T![xxx] yyy */`
// can be parsed as `yyy`. To add such rules, please use SpecialCommentsController.Register().
// For example:
//     SpecialCommentsController.Register("30100");
// Now the parser will treat
//   select a, /*T![30100] mysterious_keyword */ from t;
// and
//   select a, mysterious_keyword from t;
// equally.
// Similar special comments without registration are ignored by parser.
var SpecialCommentsController = specialCommentsCtrl{
	supportedFeatures: map[string]struct{}{},
}

type specialCommentsCtrl struct {
	supportedFeatures map[string]struct{}
}

func (s *specialCommentsCtrl) Register(featureID string) {
	s.supportedFeatures[featureID] = struct{}{}
}

func (s *specialCommentsCtrl) Unregister(featureID string) {
	delete(s.supportedFeatures, featureID)
}

func (s *specialCommentsCtrl) ContainsAll(featureIDs []string) bool {
	for _, f := range featureIDs {
		if _, found := s.supportedFeatures[f]; !found {
			return false
		}
	}
	return true
}
