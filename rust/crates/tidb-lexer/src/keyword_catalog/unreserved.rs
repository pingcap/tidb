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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `unreserved` section of the TiDB SQL keyword catalog (see `keyword_catalog/mod.rs`).

use super::Keyword;

pub(super) static KEYWORDS_UNRESERVED: &[Keyword] = &[
    Keyword {
        word: "ACCOUNT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ACTION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ADD_COLUMNAR_REPLICA_ON_DEMAND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ADVISE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AFFINITY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AFTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AGAINST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AGO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ALGORITHM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ALWAYS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ANY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "APPLY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ASCII",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ATTRIBUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ATTRIBUTES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AUTOEXTEND_SIZE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AUTO_ID_CACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AUTO_INCREMENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AUTO_RANDOM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AUTO_RANDOM_BASE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AVG",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "AVG_ROW_LENGTH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BACKEND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BACKUP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BACKUPS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BDR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BEGIN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BERNOULLI",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BINDING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BINDINGS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BINDING_CACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BINLOG",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BIT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BLOCK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BOOL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BOOLEAN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BTREE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "BYTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CALIBRATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CAPTURE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CASCADED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CAUSAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CHAIN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CHARSET",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CHECKPOINT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CHECKSUM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CHECKSUM_CONCURRENCY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CIPHER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLEANUP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLIENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLIENT_ERRORS_SUMMARY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLOSE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLUSTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CLUSTERED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COALESCE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COLLATION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COLUMNAR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COLUMNS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COLUMN_FORMAT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMMENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMMIT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMMITTED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMPACT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMPRESSED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMPRESSION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMPRESSION_LEVEL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "COMPRESSION_TYPE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONCURRENCY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONFIG",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONNECTION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONSISTENCY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONSISTENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CONTEXT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CPU",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_BACKSLASH_ESCAPE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_DELIMITER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_HEADER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_NOT_NULL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_NULL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_SEPARATOR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CSV_TRIM_LAST_SEPARATORS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CURRENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "CYCLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DATA",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DATETIME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DAY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DEALLOCATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DECLARE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DEFINER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DELAY_KEY_WRITE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DIGEST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DIRECTORY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DISABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DISABLED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DISCARD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DISK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DUPLICATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "DYNAMIC",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENABLED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENCRYPTION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENCRYPTION_KEYFILE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENCRYPTION_METHOD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "END",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENFORCED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENGINE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENGINES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENGINE_ATTRIBUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ENUM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ERROR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ERRORS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ESCAPE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EVENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EVENTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EVOLVE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXCHANGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXCLUSIVE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXECUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXPANSION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXPIRE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXPLORE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "EXTENDED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FAILED_LOGIN_ATTEMPTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FAULTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FIELDS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FILE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FIRST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FIXED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FLUSH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FOLLOWING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FORMAT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FOUND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FULL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "FUNCTION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "GENERAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "GLOBAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "GRANTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HANDLER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HASH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HELP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HISTOGRAM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HISTORY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HOSTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HOUR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "HYPO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IDENTIFIED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IETF_QUOTES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IGNORE_STATS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IMPORT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IMPORTS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INCREMENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INCREMENTAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INDEXES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INSERT_METHOD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INSTANCE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INVISIBLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "INVOKER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "IPC",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ISOLATION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ISSUER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "JSON",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "KEY_BLOCK_SIZE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LABELS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LANGUAGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LAST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LASTVAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LAST_BACKUP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LESS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LEVEL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LIST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LOAD_STATS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LOCAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LOCATION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LOCKED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "LOGS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MASKING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MASTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_CONNECTIONS_PER_HOUR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_IDXNUM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_MINUTES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_QUERIES_PER_HOUR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_ROWS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_UPDATES_PER_HOUR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MAX_USER_CONNECTIONS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MB",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MEMBER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MEMORY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MERGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MICROSECOND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MINUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MINVALUE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MIN_ROWS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MODE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MODIFY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "MONTH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NAMES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NATIONAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NCHAR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NEVER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NEXT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NEXTVAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NOCACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NOCYCLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NODEGROUP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NOMAXVALUE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NOMINVALUE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NONCLUSTERED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NONE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NOWAIT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NULLS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "NVARCHAR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OFF",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OFFSET",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OLD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OLTP_READ_ONLY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OLTP_READ_WRITE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OLTP_WRITE_ONLY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ONLINE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ONLY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ON_DUPLICATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OPEN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "OPTIONAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PACK_KEYS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PAGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PAGE_CHECKSUM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PAGE_COMPRESSED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PAGE_COMPRESSION_LEVEL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PARSER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PARTIAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PARTITIONING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PARTITIONS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PASSWORD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PASSWORD_LOCK_TIME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PAUSE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PERCENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PER_DB",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PER_TABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PLUGINS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "POINT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "POLICY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PRECEDING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PREPARE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PRESERVE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PRE_SPLIT_REGIONS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PRIVILEGES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PROCESS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PROCESSLIST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PROFILE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PROFILES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PROXY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "PURGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "QUARTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "QUERIES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "QUERY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "QUICK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RATE_LIMIT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REBUILD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RECOMMEND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RECOVER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REDUNDANT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REFRESH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RELOAD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REMOVE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REORGANIZE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REPAIR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REPEATABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REPLICA",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REPLICAS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REPLICATION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REQUIRED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESOURCE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESPECT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESTART",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESTORE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESTORES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RESUME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RETAIN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RETURNING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REUSE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "REVERSE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROLLBACK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROLLUP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROUTINE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROW_COUNT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "ROW_FORMAT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RTREE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "RULE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SAN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SAVEPOINT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECOND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECONDARY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECONDARY_ENGINE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECONDARY_ENGINE_ATTRIBUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECONDARY_LOAD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECONDARY_UNLOAD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SECURITY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SEND_CREDENTIALS_TO_TIKV",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SEPARATOR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SEQUENCE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SERIAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SERIALIZABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SESSION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SETVAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SHARD_ROW_ID_BITS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SHARE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SHARED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SHUTDOWN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SIGNED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SIMPLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SKIP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SKIP_SCHEMA_FILES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SLAVE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SLOW",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SNAPSHOT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SOME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SOURCE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_BUFFER_RESULT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_CACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_NO_CACHE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_DAY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_HOUR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_MINUTE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_MONTH",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_QUARTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_SECOND",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_WEEK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SQL_TSI_YEAR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "START",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_AUTO_RECALC",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_COL_CHOICE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_COL_LIST",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_OPTIONS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_PERSISTENT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_SAMPLE_PAGES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATS_SAMPLE_RATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STATUS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STORAGE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STORAGE_CLASS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "STRICT_FORMAT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SUBJECT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SUBPARTITION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SUBPARTITIONS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SUPER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SWAPS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SWITCHES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SYSTEM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "SYSTEM_TIME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TABLES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TABLESPACE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TABLE_CHECKSUM",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TEMPORARY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TEMPTABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TEXT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "THAN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TIKV_IMPORTER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TIME",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TIMEOUT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TIMESTAMP",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TOKEN_ISSUER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TPCC",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TPCH_10",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRACE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRADITIONAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRANSACTION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRANSACTIONAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRIGGERS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TRUNCATE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TSO",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TTL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TTL_ENABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TTL_JOB_INTERVAL",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "TYPE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNBOUNDED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNCOMMITTED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNDEFINED",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNICODE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNKNOWN",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UNSET",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "USER",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "UUID",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VALIDATION",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VALUE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VARIABLES",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VECTOR",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VIEW",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "VISIBLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WAIT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WAIT_TIFLASH_READY",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WARNINGS",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WEEK",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WEIGHT_STRING",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WITHOUT",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WITH_SYS_TABLE",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "WORKLOAD",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "X509",
        reserved: false,
        section: "unreserved",
    },
    Keyword {
        word: "YEAR",
        reserved: false,
        section: "unreserved",
    },
];
