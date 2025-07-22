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

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentChar(ch byte) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || isIdentExtend(ch)
}

func isIdentExtend(ch byte) bool {
	return ch >= 0x80
}

// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
func isInCorrectIdentifierName(name string) bool {
	if len(name) == 0 {
		return true
	}
	if name[len(name)-1] == ' ' {
		return true
	}
	return false
}

// Initialize a lookup table for isUserVarChar
var isUserVarCharTable [256]bool

func init() {
	for i := 0; i < 256; i++ {
		ch := byte(i)
		isUserVarCharTable[i] = isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || ch == '.' || isIdentExtend(ch)
	}
}

func isUserVarChar(ch byte) bool {
	return isUserVarCharTable[ch]
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

// isInTokenMap indicates whether the target string is contained in tokenMap.
func isInTokenMap(target string) bool {
	_, ok := tokenMap[target]
	return ok
}

// tokenMap is a map of known identifiers to the parser token ID.
// Please try to keep the map in alphabetical order.
var tokenMap = map[string]int{
	"ACCOUNT":                  account,
	"ACTION":                   action,
	"ADD":                      add,
	"ADDDATE":                  addDate,
	"ADMIN":                    admin,
	"ADVISE":                   advise,
	"AFTER":                    after,
	"AGAINST":                  against,
	"AGO":                      ago,
	"ALGORITHM":                algorithm,
	"ALL":                      all,
	"ALTER":                    alter,
	"ALWAYS":                   always,
	"ANALYZE":                  analyze,
	"AND":                      and,
	"ANY":                      any,
	"APPROX_COUNT_DISTINCT":    approxCountDistinct,
	"APPROX_PERCENTILE":        approxPercentile,
	"ARRAY":                    array,
	"AS":                       as,
	"ASC":                      asc,
	"ASCII":                    ascii,
	"APPLY":                    apply,
	"ATTRIBUTE":                attribute,
	"ATTRIBUTES":               attributes,
	"BATCH":                    batch,
	"BACKGROUND":               background,
	"STATS_OPTIONS":            statsOptions,
	"STATS_SAMPLE_RATE":        statsSampleRate,
	"STATS_COL_CHOICE":         statsColChoice,
	"STATS_COL_LIST":           statsColList,
	"AUTO_ID_CACHE":            autoIdCache,
	"AUTO_INCREMENT":           autoIncrement,
	"AUTO_RANDOM":              autoRandom,
	"AUTO_RANDOM_BASE":         autoRandomBase,
	"AVG_ROW_LENGTH":           avgRowLength,
	"AVG":                      avg,
	"BACKEND":                  backend,
	"BACKUP":                   backup,
	"BACKUPS":                  backups,
	"BDR":                      bdr,
	"BEGIN":                    begin,
	"BETWEEN":                  between,
	"BERNOULLI":                bernoulli,
	"BIGINT":                   bigIntType,
	"BINARY":                   binaryType,
	"BINDING":                  binding,
	"BINDING_CACHE":            bindingCache,
	"BINDINGS":                 bindings,
	"BINLOG":                   binlog,
	"BIT_AND":                  bitAnd,
	"BIT_OR":                   bitOr,
	"BIT_XOR":                  bitXor,
	"BIT":                      bitType,
	"BLOB":                     blobType,
	"BLOCK":                    block,
	"BOOL":                     boolType,
	"BOOLEAN":                  booleanType,
	"BOTH":                     both,
	"BOUND":                    bound,
	"BR":                       br,
	"BRIEF":                    briefType,
	"BTREE":                    btree,
	"BUCKETS":                  buckets,
	"BUILTINS":                 builtins,
	"BURSTABLE":                burstable,
	"BY":                       by,
	"BYTE":                     byteType,
	"CACHE":                    cache,
	"CALIBRATE":                calibrate,
	"CALL":                     call,
	"CANCEL":                   cancel,
	"CAPTURE":                  capture,
	"CARDINALITY":              cardinality,
	"CASCADE":                  cascade,
	"CASCADED":                 cascaded,
	"CASE":                     caseKwd,
	"CAST":                     cast,
	"CAUSAL":                   causal,
	"CHAIN":                    chain,
	"CHANGE":                   change,
	"CHAR":                     charType,
	"CHARACTER":                character,
	"CHARSET":                  charsetKwd,
	"CHECK":                    check,
	"CHECKPOINT":               checkpoint,
	"CHECKSUM":                 checksum,
	"CIPHER":                   cipher,
	"CLEANUP":                  cleanup,
	"CLIENT":                   client,
	"CLIENT_ERRORS_SUMMARY":    clientErrorsSummary,
	"CLOSE":                    close,
	"CLUSTER":                  cluster,
	"CLUSTERED":                clustered,
	"CMSKETCH":                 cmSketch,
	"COALESCE":                 coalesce,
	"COLLATE":                  collate,
	"COLLATION":                collation,
	"COLUMN_FORMAT":            columnFormat,
	"COLUMN_STATS_USAGE":       columnStatsUsage,
	"COLUMN":                   column,
	"COLUMNAR":                 columnar,
	"COLUMNS":                  columns,
	"COMMENT":                  comment,
	"COMMIT":                   commit,
	"COMMITTED":                committed,
	"COMPACT":                  compact,
	"COMPRESS":                 compress,
	"COMPRESSED":               compressed,
	"COMPRESSION":              compression,
	"CONCURRENCY":              concurrency,
	"CONFIG":                   config,
	"CONNECTION":               connection,
	"CONSISTENCY":              consistency,
	"CONSISTENT":               consistent,
	"CONSTRAINT":               constraint,
	"CONSTRAINTS":              constraints,
	"CONTEXT":                  context,
	"CONTINUE":                 continueKwd,
	"CONVERT":                  convert,
	"COOLDOWN":                 cooldown,
	"COPY":                     copyKwd,
	"CORRELATION":              correlation,
	"CPU":                      cpu,
	"CREATE":                   create,
	"CROSS":                    cross,
	"CSV_BACKSLASH_ESCAPE":     csvBackslashEscape,
	"CSV_DELIMITER":            csvDelimiter,
	"CSV_HEADER":               csvHeader,
	"CSV_NOT_NULL":             csvNotNull,
	"CSV_NULL":                 csvNull,
	"CSV_SEPARATOR":            csvSeparator,
	"CSV_TRIM_LAST_SEPARATORS": csvTrimLastSeparators,
	"WAIT_TIFLASH_READY":       waitTiflashReady,
	"WITH_SYS_TABLE":           withSysTable,
	"IGNORE_STATS":             ignoreStats,
	"LOAD_STATS":               loadStats,
	"CHECKSUM_CONCURRENCY":     checksumConcurrency,
	"COMPRESSION_LEVEL":        compressionLevel,
	"COMPRESSION_TYPE":         compressionType,
	"ENCRYPTION_METHOD":        encryptionMethod,
	"ENCRYPTION_KEYFILE":       encryptionKeyFile,
	"CURDATE":                  curDate,
	"CURRENT_DATE":             currentDate,
	"CURRENT_ROLE":             currentRole,
	"CURRENT_TIME":             currentTime,
	"CURRENT_TIMESTAMP":        currentTs,
	"CURRENT_USER":             currentUser,
	"CURRENT":                  current,
	"CURSOR":                   cursor,
	"CURTIME":                  curTime,
	"CYCLE":                    cycle,
	"DATA":                     data,
	"DATABASE":                 database,
	"DATABASES":                databases,
	"DATE_ADD":                 dateAdd,
	"DATE_SUB":                 dateSub,
	"DATE":                     dateType,
	"DATETIME":                 datetimeType,
	"DAY_HOUR":                 dayHour,
	"DAY_MICROSECOND":          dayMicrosecond,
	"DAY_MINUTE":               dayMinute,
	"DAY_SECOND":               daySecond,
	"DAY":                      day,
	"DDL":                      ddl,
	"DEALLOCATE":               deallocate,
	"DEC":                      decimalType,
	"DECIMAL":                  decimalType,
	"DECLARE":                  declare,
	"DEFAULT":                  defaultKwd,
	"DEFINED":                  defined,
	"DEFINER":                  definer,
	"DELAY_KEY_WRITE":          delayKeyWrite,
	"DELAYED":                  delayed,
	"DELETE":                   deleteKwd,
	"DEPENDENCY":               dependency,
	"DEPTH":                    depth,
	"DESC":                     desc,
	"DESCRIBE":                 describe,
	"DIGEST":                   digest,
	"DIRECTORY":                directory,
	"DISABLE":                  disable,
	"DISABLED":                 disabled,
	"DISCARD":                  discard,
	"DISK":                     disk,
	"DISTINCT":                 distinct,
	"DISTINCTROW":              distinct,
	"DISTRIBUTE":               distribute,
	"DISTRIBUTION":             distribution,
	"DISTRIBUTIONS":            distributions,
	"DIV":                      div,
	"DO":                       do,
	"DOT":                      dotType,
	"DOUBLE":                   doubleType,
	"DROP":                     drop,
	"DRY":                      dry,
	"DRYRUN":                   dryRun,
	"DUAL":                     dual,
	"DUMP":                     dump,
	"DUPLICATE":                duplicate,
	"DURATION":                 timeDuration,
	"DYNAMIC":                  dynamic,
	"ELSE":                     elseKwd,
	"ELSEIF":                   elseIfKwd,
	"ENABLE":                   enable,
	"ENABLED":                  enabled,
	"ENCLOSED":                 enclosed,
	"ENCRYPTION":               encryption,
	"END":                      end,
	"END_TIME":                 endTime,
	"ENFORCED":                 enforced,
	"ENGINE":                   engine,
	"ENGINES":                  engines,
	"ENGINE_ATTRIBUTE":         engine_attribute,
	"ENUM":                     enum,
	"ERROR":                    errorKwd,
	"ERRORS":                   identSQLErrors,
	"ESCAPE":                   escape,
	"ESCAPED":                  escaped,
	"EVENT":                    event,
	"EVENTS":                   events,
	"EVOLVE":                   evolve,
	"EXACT":                    exact,
	"EXEC_ELAPSED":             execElapsed,
	"EXCEPT":                   except,
	"EXCHANGE":                 exchange,
	"EXCLUSIVE":                exclusive,
	"EXECUTE":                  execute,
	"EXISTS":                   exists,
	"EXIT":                     exit,
	"EXPANSION":                expansion,
	"EXPIRE":                   expire,
	"EXPLAIN":                  explain,
	"EXPR_PUSHDOWN_BLACKLIST":  exprPushdownBlacklist,
	"EXTENDED":                 extended,
	"EXTRACT":                  extract,
	"FALSE":                    falseKwd,
	"FAULTS":                   faultsSym,
	"FETCH":                    fetch,
	"FIELDS":                   fields,
	"FILE":                     file,
	"FIRST":                    first,
	"FIXED":                    fixed,
	"FLASHBACK":                flashback,
	"FLOAT":                    floatType,
	"FLOAT4":                   float4Type,
	"FLOAT8":                   float8Type,
	"FLUSH":                    flush,
	"FOLLOWER":                 follower,
	"FOLLOWERS":                followers,
	"FOLLOWER_CONSTRAINTS":     followerConstraints,
	"FOLLOWING":                following,
	"FOR":                      forKwd,
	"FORCE":                    force,
	"FOREIGN":                  foreign,
	"FORMAT":                   format,
	"FOUND":                    found,
	"FROM":                     from,
	"FULL":                     full,
	"FULL_BACKUP_STORAGE":      fullBackupStorage,
	"FULLTEXT":                 fulltext,
	"FUNCTION":                 function,
	"GC_TTL":                   gcTTL,
	"GENERAL":                  general,
	"GENERATED":                generated,
	"GET_FORMAT":               getFormat,
	"GLOBAL":                   global,
	"GRANT":                    grant,
	"GRANTS":                   grants,
	"GROUP_CONCAT":             groupConcat,
	"GROUP":                    group,
	"HASH":                     hash,
	"HANDLER":                  handler,
	"HAVING":                   having,
	"HELP":                     help,
	"HIGH_PRIORITY":            highPriority,
	"HISTORY":                  history,
	"HISTOGRAM":                histogram,
	"HNSW":                     hnsw,
	"HOSTS":                    hosts,
	"HOUR_MICROSECOND":         hourMicrosecond,
	"HOUR_MINUTE":              hourMinute,
	"HOUR_SECOND":              hourSecond,
	"HOUR":                     hour,
	"IDENTIFIED":               identified,
	"IF":                       ifKwd,
	"IGNORE":                   ignore,
	"ILIKE":                    ilike,
	"IMPORT":                   importKwd,
	"IMPORTS":                  imports,
	"IN":                       in,
	"INCREMENT":                increment,
	"INCREMENTAL":              incremental,
	"INDEX":                    index,
	"INDEXES":                  indexes,
	"INFILE":                   infile,
	"INNER":                    inner,
	"INOUT":                    inout,
	"INPLACE":                  inplace,
	"INSERT_METHOD":            insertMethod,
	"INSERT":                   insert,
	"INSTANCE":                 instance,
	"INSTANT":                  instant,
	"INT":                      intType,
	"INT1":                     int1Type,
	"INT2":                     int2Type,
	"INT3":                     int3Type,
	"INT4":                     int4Type,
	"INT8":                     int8Type,
	"INTEGER":                  integerType,
	"INTERNAL":                 internal,
	"INTERSECT":                intersect,
	"INTERVAL":                 interval,
	"INTO":                     into,
	"INVERTED":                 inverted,
	"INVISIBLE":                invisible,
	"INVOKER":                  invoker,
	"ITERATE":                  iterate,
	"IO":                       io,
	"RU_PER_SEC":               ruRate,
	"PRIORITY":                 priority,
	"HIGH":                     high,
	"MEDIUM":                   medium,
	"LOW":                      low,
	"IO_READ_BANDWIDTH":        ioReadBandwidth,
	"IO_WRITE_BANDWIDTH":       ioWriteBandwidth,
	"IPC":                      ipc,
	"IS":                       is,
	"ISOLATION":                isolation,
	"ISSUER":                   issuer,
	"JOB":                      job,
	"JOBS":                     jobs,
	"JOIN":                     join,
	"JSON_ARRAYAGG":            jsonArrayagg,
	"JSON_OBJECTAGG":           jsonObjectAgg,
	"JSON":                     jsonType,
	"KEY_BLOCK_SIZE":           keyBlockSize,
	"KEY":                      key,
	"KEYS":                     keys,
	"KILL":                     kill,
	"LABELS":                   labels,
	"LANGUAGE":                 language,
	"LAST_BACKUP":              lastBackup,
	"LAST":                     last,
	"LASTVAL":                  lastval,
	"LEADER":                   leader,
	"LEADER_CONSTRAINTS":       leaderConstraints,
	"LEADING":                  leading,
	"LEARNER":                  learner,
	"LEARNER_CONSTRAINTS":      learnerConstraints,
	"LEARNERS":                 learners,
	"LEAVE":                    leave,
	"LEFT":                     left,
	"LESS":                     less,
	"LEVEL":                    level,
	"LIKE":                     like,
	"LIMIT":                    limit,
	"LINEAR":                   linear,
	"LINES":                    lines,
	"LIST":                     list,
	"LOAD":                     load,
	"LOCAL":                    local,
	"LOCALTIME":                localTime,
	"LOCALTIMESTAMP":           localTs,
	"LOCATION":                 location,
	"LOCK":                     lock,
	"LOCKED":                   locked,
	"LOG":                      log,
	"LOGS":                     logs,
	"LONG":                     long,
	"LONGBLOB":                 longblobType,
	"LONGTEXT":                 longtextType,
	"LOW_PRIORITY":             lowPriority,
	"MASTER":                   master,
	"MATCH":                    match,
	"MAX_CONNECTIONS_PER_HOUR": maxConnectionsPerHour,
	"MAX_IDXNUM":               max_idxnum,
	"MAX_MINUTES":              max_minutes,
	"MAX_QUERIES_PER_HOUR":     maxQueriesPerHour,
	"MAX_ROWS":                 maxRows,
	"MAX_UPDATES_PER_HOUR":     maxUpdatesPerHour,
	"MAX_USER_CONNECTIONS":     maxUserConnections,
	"MAX":                      max,
	"MAXVALUE":                 maxValue,
	"MB":                       mb,
	"MEDIUMBLOB":               mediumblobType,
	"MEDIUMINT":                mediumIntType,
	"MEDIUMTEXT":               mediumtextType,
	"MEMORY":                   memory,
	"MEMBER":                   member,
	"MERGE":                    merge,
	"METADATA":                 metadata,
	"MICROSECOND":              microsecond,
	"MIDDLEINT":                middleIntType,
	"MIN_ROWS":                 minRows,
	"MIN":                      min,
	"MINUTE_MICROSECOND":       minuteMicrosecond,
	"MINUTE_SECOND":            minuteSecond,
	"MINUTE":                   minute,
	"MINVALUE":                 minValue,
	"MOD":                      mod,
	"MODE":                     mode,
	"MODIFY":                   modify,
	"MONTH":                    month,
	"NAMES":                    names,
	"NATIONAL":                 national,
	"NATURAL":                  natural,
	"NCHAR":                    ncharType,
	"NEVER":                    never,
	"NEXT_ROW_ID":              next_row_id,
	"NEXT":                     next,
	"NEXTVAL":                  nextval,
	"NO_WRITE_TO_BINLOG":       noWriteToBinLog,
	"NO":                       no,
	"NOCACHE":                  nocache,
	"NOCYCLE":                  nocycle,
	"NODE_ID":                  nodeID,
	"NODE_STATE":               nodeState,
	"NODEGROUP":                nodegroup,
	"NOMAXVALUE":               nomaxvalue,
	"NOMINVALUE":               nominvalue,
	"NONCLUSTERED":             nonclustered,
	"NONE":                     none,
	"NOT":                      not,
	"NOW":                      now,
	"NOWAIT":                   nowait,
	"NULL":                     null,
	"NULLS":                    nulls,
	"NUMERIC":                  numericType,
	"NVARCHAR":                 nvarcharType,
	"OF":                       of,
	"OFF":                      off,
	"OFFSET":                   offset,
	"OLTP_READ_ONLY":           oltpReadOnly,
	"OLTP_READ_WRITE":          oltpReadWrite,
	"OLTP_WRITE_ONLY":          oltpWriteOnly,
	"TPCH_10":                  tpch10,
	"ON_DUPLICATE":             onDuplicate,
	"ON":                       on,
	"ONLINE":                   online,
	"ONLY":                     only,
	"OPEN":                     open,
	"OPT_RULE_BLACKLIST":       optRuleBlacklist,
	"OPTIMISTIC":               optimistic,
	"OPTIMIZE":                 optimize,
	"OPTION":                   option,
	"OPTIONAL":                 optional,
	"OPTIONALLY":               optionally,
	"OR":                       or,
	"ORDER":                    order,
	"OUT":                      out,
	"OUTER":                    outer,
	"OUTFILE":                  outfile,
	"PACK_KEYS":                packKeys,
	"PAGE":                     pageSym,
	"PARSER":                   parser,
	"PARTIAL":                  partial,
	"PARTITION":                partition,
	"PARTITIONING":             partitioning,
	"PARTITIONS":               partitions,
	"PASSWORD":                 password,
	"PAUSE":                    pause,
	"PERCENT":                  percent,
	"PER_DB":                   per_db,
	"PER_TABLE":                per_table,
	"PESSIMISTIC":              pessimistic,
	"PLACEMENT":                placement,
	"PLAN":                     plan,
	"PLAN_CACHE":               planCache,
	"PLUGINS":                  plugins,
	"POINT":                    point,
	"POLICY":                   policy,
	"POSITION":                 position,
	"PRE_SPLIT_REGIONS":        preSplitRegions,
	"PRECEDING":                preceding,
	"PREDICATE":                predicate,
	"PRECISION":                precisionType,
	"PREPARE":                  prepare,
	"PRESERVE":                 preserve,
	"PRIMARY":                  primary,
	"PRIMARY_REGION":           primaryRegion,
	"PRIVILEGES":               privileges,
	"PROCEDURE":                procedure,
	"PROCESS":                  process,
	"PROCESSED_KEYS":           processedKeys,
	"PROCESSLIST":              processlist,
	"PROFILE":                  profile,
	"PROFILES":                 profiles,
	"PROXY":                    proxy,
	"PURGE":                    purge,
	"QUARTER":                  quarter,
	"QUERIES":                  queries,
	"QUERY":                    query,
	"QUERY_LIMIT":              queryLimit,
	"QUICK":                    quick,
	"RANGE":                    rangeKwd,
	"RATE_LIMIT":               rateLimit,
	"READ":                     read,
	"READ_ONLY":                readOnly,
	"REAL":                     realType,
	"REBUILD":                  rebuild,
	"RECENT":                   recent,
	"RECOMMEND":                recommend,
	"RECOVER":                  recover,
	"RECURSIVE":                recursive,
	"REDUNDANT":                redundant,
	"REFERENCES":               references,
	"REGEXP":                   regexpKwd,
	"REGION":                   region,
	"REGIONS":                  regions,
	"RELEASE":                  release,
	"RELOAD":                   reload,
	"REMOVE":                   remove,
	"RENAME":                   rename,
	"REORGANIZE":               reorganize,
	"REPAIR":                   repair,
	"REPEAT":                   repeat,
	"REPEATABLE":               repeatable,
	"REPLACE":                  replace,
	"REPLAY":                   replay,
	"REPLAYER":                 replayer,
	"REPLICA":                  replica,
	"REPLICAS":                 replicas,
	"REPLICATION":              replication,
	"RU":                       ru,
	"RULE":                     rule,
	"REQUIRE":                  require,
	"REQUIRED":                 required,
	"RESET":                    reset,
	"RESOURCE":                 resource,
	"RESPECT":                  respect,
	"RESTART":                  restart,
	"RESTORE":                  restore,
	"RESTORES":                 restores,
	"RESTORED_TS":              restoredTS,
	"RESTRICT":                 restrict,
	"REVERSE":                  reverse,
	"REVOKE":                   revoke,
	"RIGHT":                    right,
	"RLIKE":                    rlike,
	"ROLE":                     role,
	"ROLLBACK":                 rollback,
	"ROLLUP":                   rollup,
	"ROUTINE":                  routine,
	"ROW_COUNT":                rowCount,
	"ROW_FORMAT":               rowFormat,
	"ROW":                      row,
	"ROWS":                     rows,
	"RTREE":                    rtree,
	"HYPO":                     hypo,
	"RESUME":                   resume,
	"RUN":                      run,
	"RUNNING":                  running,
	"S3":                       s3,
	"SAMPLES":                  samples,
	"SAMPLERATE":               sampleRate,
	"SAN":                      san,
	"SAVEPOINT":                savepoint,
	"SCHEDULE":                 schedule,
	"SCHEMA":                   database,
	"SCHEMAS":                  databases,
	"SECOND_MICROSECOND":       secondMicrosecond,
	"SECOND":                   second,
	"SECONDARY":                secondary,
	"SECONDARY_ENGINE":         secondaryEngine,
	"SECONDARY_LOAD":           secondaryLoad,
	"SECONDARY_UNLOAD":         secondaryUnload,
	"SECURITY":                 security,
	"SELECT":                   selectKwd,
	"SEND_CREDENTIALS_TO_TIKV": sendCredentialsToTiKV,
	"SEPARATOR":                separator,
	"SEQUENCE":                 sequence,
	"SERIAL":                   serial,
	"SERIALIZABLE":             serializable,
	"SESSION":                  session,
	"SESSION_STATES":           sessionStates,
	"SET":                      set,
	"SETVAL":                   setval,
	"SHARD_ROW_ID_BITS":        shardRowIDBits,
	"SHARE":                    share,
	"SHARED":                   shared,
	"SHOW":                     show,
	"SHUTDOWN":                 shutdown,
	"SIGNED":                   signed,
	"SIMILAR":                  similar,
	"SIMPLE":                   simple,
	"SKIP":                     skip,
	"SKIP_SCHEMA_FILES":        skipSchemaFiles,
	"SLAVE":                    slave,
	"SLOW":                     slow,
	"SMALLINT":                 smallIntType,
	"SNAPSHOT":                 snapshot,
	"SOME":                     some,
	"SOURCE":                   source,
	"SPATIAL":                  spatial,
	"SPEED":                    speed,
	"SPLIT":                    split,
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
	"SQL":                      sql,
	"SQLEXCEPTION":             sqlexception,
	"SQLSTATE":                 sqlstate,
	"SQLWARNING":               sqlwarning,
	"SSL":                      ssl,
	"STALENESS":                staleness,
	"START":                    start,
	"START_TIME":               startTime,
	"START_TS":                 startTS,
	"STARTING":                 starting,
	"STATISTICS":               statistics,
	"STATS_AUTO_RECALC":        statsAutoRecalc,
	"STATS_BUCKETS":            statsBuckets,
	"STATS_EXTENDED":           statsExtended,
	"STATS_HEALTHY":            statsHealthy,
	"STATS_HISTOGRAMS":         statsHistograms,
	"STATS_TOPN":               statsTopN,
	"STATS_META":               statsMeta,
	"STATS_LOCKED":             statsLocked,
	"HISTOGRAMS_IN_FLIGHT":     histogramsInFlight,
	"STATS_PERSISTENT":         statsPersistent,
	"STATS_SAMPLE_PAGES":       statsSamplePages,
	"STATS":                    stats,
	"STATUS":                   status,
	"STD":                      stddevPop,
	"STDDEV_POP":               stddevPop,
	"STDDEV_SAMP":              stddevSamp,
	"STDDEV":                   stddevPop,
	"STOP":                     stop,
	"STORAGE":                  storage,
	"STORED":                   stored,
	"STRAIGHT_JOIN":            straightJoin,
	"STRICT":                   strict,
	"STRICT_FORMAT":            strictFormat,
	"STRONG":                   strong,
	"SUBDATE":                  subDate,
	"SUBJECT":                  subject,
	"SUBPARTITION":             subpartition,
	"SUBPARTITIONS":            subpartitions,
	"SUBSTR":                   substring,
	"SUBSTRING":                substring,
	"SUM":                      sum,
	"SUPER":                    super,
	"SURVIVAL_PREFERENCES":     survivalPreferences,
	"SWAPS":                    swaps,
	"SWITCHES":                 switchesSym,
	"SWITCH_GROUP":             switchGroup,
	"SYSTEM":                   system,
	"SYSTEM_TIME":              systemTime,
	"TARGET":                   target,
	"TASK_TYPES":               taskTypes,
	"TABLE_CHECKSUM":           tableChecksum,
	"TABLE":                    tableKwd,
	"TABLES":                   tables,
	"TABLESAMPLE":              tableSample,
	"TABLESPACE":               tablespace,
	"TEMPORARY":                temporary,
	"TEMPTABLE":                temptable,
	"TERMINATED":               terminated,
	"TEXT":                     textType,
	"THAN":                     than,
	"THEN":                     then,
	"TIDB":                     tidb,
	"TIDB_CURRENT_TSO":         tidbCurrentTSO,
	"TIDB_JSON":                tidbJson,
	"TIFLASH":                  tiFlash,
	"TIKV_IMPORTER":            tikvImporter,
	"TIME":                     timeType,
	"TIMESTAMP":                timestampType,
	"TIMESTAMPADD":             timestampAdd,
	"TIMESTAMPDIFF":            timestampDiff,
	"TINYBLOB":                 tinyblobType,
	"TINYINT":                  tinyIntType,
	"TINYTEXT":                 tinytextType,
	"TLS":                      tls,
	"TO":                       to,
	"TOKEN_ISSUER":             tokenIssuer,
	"TOKUDB_DEFAULT":           tokudbDefault,
	"TOKUDB_FAST":              tokudbFast,
	"TOKUDB_LZMA":              tokudbLzma,
	"TOKUDB_QUICKLZ":           tokudbQuickLZ,
	"TOKUDB_SMALL":             tokudbSmall,
	"TOKUDB_SNAPPY":            tokudbSnappy,
	"TOKUDB_UNCOMPRESSED":      tokudbUncompressed,
	"TOKUDB_ZLIB":              tokudbZlib,
	"TOKUDB_ZSTD":              tokudbZstd,
	"TOP":                      top,
	"TOPN":                     topn,
	"TPCC":                     tpcc,
	"TRACE":                    trace,
	"TRADITIONAL":              traditional,
	"TRAFFIC":                  traffic,
	"TRAILING":                 trailing,
	"TRANSACTION":              transaction,
	"TRIGGER":                  trigger,
	"TRIGGERS":                 triggers,
	"TRIM":                     trim,
	"TRUE":                     trueKwd,
	"TRUNCATE":                 truncate,
	"TRUE_CARD_COST":           trueCardCost,
	"TSO":                      tsoType,
	"TTL":                      ttl,
	"TTL_ENABLE":               ttlEnable,
	"TTL_JOB_INTERVAL":         ttlJobInterval,
	"TYPE":                     tp,
	"UNBOUNDED":                unbounded,
	"UNCOMMITTED":              uncommitted,
	"UNDEFINED":                undefined,
	"UNICODE":                  unicodeSym,
	"UNION":                    union,
	"UNIQUE":                   unique,
	"UNKNOWN":                  unknown,
	"UNLOCK":                   unlock,
	"UNLIMITED":                unlimited,
	"MODERATED":                moderated,
	"UNSET":                    unset,
	"UNSIGNED":                 unsigned,
	"UNTIL":                    until,
	"UNTIL_TS":                 untilTS,
	"UPDATE":                   update,
	"USAGE":                    usage,
	"USE":                      use,
	"USER":                     user,
	"USING":                    using,
	"UTC_DATE":                 utcDate,
	"UTC_TIME":                 utcTime,
	"UTC_TIMESTAMP":            utcTimestamp,
	"UTILIZATION_LIMIT":        utilizationLimit,
	"VALIDATION":               validation,
	"VALUE":                    value,
	"VALUES":                   values,
	"VAR_POP":                  varPop,
	"VAR_SAMP":                 varSamp,
	"VARBINARY":                varbinaryType,
	"VARCHAR":                  varcharType,
	"VARCHARACTER":             varcharacter,
	"VARIABLES":                variables,
	"VARIANCE":                 varPop,
	"VARYING":                  varying,
	"VECTOR":                   vectorType,
	"VERBOSE":                  verboseType,
	"VOTER":                    voter,
	"VOTER_CONSTRAINTS":        voterConstraints,
	"VOTERS":                   voters,
	"VIEW":                     view,
	"VIRTUAL":                  virtual,
	"VISIBLE":                  visible,
	"WARNINGS":                 warnings,
	"WATCH":                    watch,
	"WEEK":                     week,
	"WEIGHT_STRING":            weightString,
	"WHEN":                     when,
	"WHERE":                    where,
	"WHILE":                    while,
	"WIDTH":                    width,
	"WITH":                     with,
	"WITHOUT":                  without,
	"WRITE":                    write,
	"WORKLOAD":                 workload,
	"X509":                     x509,
	"XOR":                      xor,
	"YEAR_MONTH":               yearMonth,
	"YEAR":                     yearType,
	"ZEROFILL":                 zerofill,
	"WAIT":                     wait,
	"FAILED_LOGIN_ATTEMPTS":    failedLoginAttempts,
	"PASSWORD_LOCK_TIME":       passwordLockTime,
	"REUSE":                    reuse,
}

// See https://dev.mysql.com/doc/refman/8.0/en/function-resolution.html for details.
// ADDDATE, SESSION_USER, SUBDATE, and SYSTEM_USER are exceptions because they are actually recognized as
// identifiers even in `create table adddate (a int)`.
var btFuncTokenMap = map[string]int{
	"BIT_AND":               builtinBitAnd,
	"BIT_OR":                builtinBitOr,
	"BIT_XOR":               builtinBitXor,
	"CAST":                  builtinCast,
	"COUNT":                 builtinCount,
	"APPROX_COUNT_DISTINCT": builtinApproxCountDistinct,
	"APPROX_PERCENTILE":     builtinApproxPercentile,
	"CURDATE":               builtinCurDate,
	"CURTIME":               builtinCurTime,
	"DATE_ADD":              builtinDateAdd,
	"DATE_SUB":              builtinDateSub,
	"EXTRACT":               builtinExtract,
	"GROUP_CONCAT":          builtinGroupConcat,
	"MAX":                   builtinMax,
	"MID":                   builtinSubstring,
	"MIN":                   builtinMin,
	"NOW":                   builtinNow,
	"POSITION":              builtinPosition,
	"STD":                   builtinStddevPop,
	"STDDEV":                builtinStddevPop,
	"STDDEV_POP":            builtinStddevPop,
	"STDDEV_SAMP":           builtinStddevSamp,
	"SUBSTR":                builtinSubstring,
	"SUBSTRING":             builtinSubstring,
	"SUM":                   builtinSum,
	"SYSDATE":               builtinSysDate,
	"TRANSLATE":             builtinTranslate,
	"TRIM":                  builtinTrim,
	"VARIANCE":              builtinVarPop,
	"VAR_POP":               builtinVarPop,
	"VAR_SAMP":              builtinVarSamp,
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
	"HASH_JOIN_BUILD":       hintHashJoinBuild,
	"HASH_JOIN_PROBE":       hintHashJoinProbe,
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
	"HYPO_INDEX":            hintHypoIndex,

	// TiDB hint names
	"AGG_TO_COP":              hintAggToCop,
	"LIMIT_TO_COP":            hintLimitToCop,
	"IGNORE_PLAN_CACHE":       hintIgnorePlanCache,
	"HASH_AGG":                hintHashAgg,
	"MPP_1PHASE_AGG":          hintMpp1PhaseAgg,
	"MPP_2PHASE_AGG":          hintMpp2PhaseAgg,
	"IGNORE_INDEX":            hintIgnoreIndex,
	"INL_HASH_JOIN":           hintInlHashJoin,
	"INDEX_HASH_JOIN":         hintIndexHashJoin,
	"NO_INDEX_HASH_JOIN":      hintNoIndexHashJoin,
	"INL_JOIN":                hintInlJoin,
	"INDEX_JOIN":              hintIndexJoin,
	"NO_INDEX_JOIN":           hintNoIndexJoin,
	"INL_MERGE_JOIN":          hintInlMergeJoin,
	"INDEX_MERGE_JOIN":        hintIndexMergeJoin,
	"NO_INDEX_MERGE_JOIN":     hintNoIndexMergeJoin,
	"MEMORY_QUOTA":            hintMemoryQuota,
	"NO_SWAP_JOIN_INPUTS":     hintNoSwapJoinInputs,
	"QUERY_TYPE":              hintQueryType,
	"READ_CONSISTENT_REPLICA": hintReadConsistentReplica,
	"READ_FROM_STORAGE":       hintReadFromStorage,
	"BROADCAST_JOIN":          hintBCJoin,
	"SHUFFLE_JOIN":            hintShuffleJoin,
	"MERGE_JOIN":              hintSMJoin,
	"NO_MERGE_JOIN":           hintNoSMJoin,
	"STREAM_AGG":              hintStreamAgg,
	"SWAP_JOIN_INPUTS":        hintSwapJoinInputs,
	"USE_INDEX_MERGE":         hintUseIndexMerge,
	"USE_INDEX":               hintUseIndex,
	"ORDER_INDEX":             hintOrderIndex,
	"NO_ORDER_INDEX":          hintNoOrderIndex,
	"USE_PLAN_CACHE":          hintUsePlanCache,
	"USE_TOJA":                hintUseToja,
	"TIME_RANGE":              hintTimeRange,
	"USE_CASCADES":            hintUseCascades,
	"NTH_PLAN":                hintNthPlan,
	"FORCE_INDEX":             hintForceIndex,
	"STRAIGHT_JOIN":           hintStraightJoin,
	"LEADING":                 hintLeading,
	"SEMI_JOIN_REWRITE":       hintSemiJoinRewrite,
	"NO_DECORRELATE":          hintNoDecorrelate,

	// TiDB hint aliases
	"TIDB_HJ":   hintHashJoin,
	"TIDB_INLJ": hintInlJoin,
	"TIDB_SMJ":  hintSMJoin,

	// Other keywords
	"OLAP":            hintOLAP,
	"OLTP":            hintOLTP,
	"TIKV":            hintTiKV,
	"TIFLASH":         hintTiFlash,
	"PARTITION":       hintPartition,
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

	for idx := offset - 1; idx >= 0; idx-- {
		if s.r.s[idx] == ' ' {
			continue
		} else if s.r.s[idx] == '.' {
			return 0
		}
		break
	}

	buf := &s.buf
	buf.Reset()
	buf.Grow(len(lit))
	data := buf.Bytes()[:len(lit)]

	for i := 0; i < len(lit); i++ {
		c := lit[i]
		if c >= 'a' && c <= 'z' {
			data[i] = c + 'A' - 'a'
		} else {
			data[i] = c
		}
	}

	checkBtFuncToken := s.r.peek() == '('
	if !checkBtFuncToken && s.sqlMode.HasIgnoreSpaceMode() {
		s.skipWhitespace()
		checkBtFuncToken = s.r.peek() == '('
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
