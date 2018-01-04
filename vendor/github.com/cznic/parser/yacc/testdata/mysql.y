/*
   Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/* sql_yacc.yy */

%{
package sql
import (
)
%}


%union {
    empty struct{}
    interf interface{}
    bytes []byte
    str string
    strval StrVal
    numval NumVal
    
    statement IStatement
    select_statement ISelect
    subquery *SubQuery
    table ISimpleTable
    table_list ISimpleTables
    table_ref ITable
    table_ref_list ITables
    table_to_table *TableToTable
    table_to_table_list []*TableToTable

    table_index *TableIndex
    table_index_list []*TableIndex

    spname *Spname

    expr IExpr
    exprs IExprs
    valexpr IValExpr
    valexprs IValExprs
    boolexpr IBoolExpr

    lock_type LockType
    view_tail *viewTail
    event_tail *eventTail
    trigger_tail *triggerTail
    sf_tail *sfTail
    sp_tail *spTail
    udf_tail *udfTail

    like_or_where *LikeOrWhere

    variable *Variable
    vars Vars
    var_type VarType
    life_type LifeType
}

/*
   Comments for TOKENS.
   For each token, please include in the same line a comment that contains
   the following tags:
   SQL-2003-R : Reserved keyword as per SQL-2003
   SQL-2003-N : Non Reserved keyword as per SQL-2003
   SQL-1999-R : Reserved keyword as per SQL-1999
   SQL-1999-N : Non Reserved keyword as per SQL-1999
   MYSQL      : MySQL extention (unspecified)
   MYSQL-FUNC : MySQL extention, function
   INTERNAL   : Not a real token, lex optimization
   OPERATOR   : SQL operator
   FUTURE-USE : Reserved for futur use

   This makes the code grep-able, and helps maintenance.
*/

%token  ABORT_SYM                     /* INTERNAL (used in lex) */
%token<bytes>  ACCESSIBLE_SYM
%token<bytes>  ACTION                        /* SQL-2003-N */
%token<bytes>  ADD                           /* SQL-2003-R */
%token<bytes>  ADDDATE_SYM                   /* MYSQL-FUNC */
%token<bytes>  AFTER_SYM                     /* SQL-2003-N */
%token<bytes>  AGAINST
%token<bytes>  AGGREGATE_SYM
%token<bytes>  ALGORITHM_SYM
%token<bytes>  ALL                           /* SQL-2003-R */
%token<bytes>  ALTER                         /* SQL-2003-R */
%token<bytes>  ANALYSE_SYM
%token<bytes>  ANALYZE_SYM
%token<bytes>  AND_AND_SYM                   /* OPERATOR */
%token<bytes>  AND_SYM                       /* SQL-2003-R */
%token<bytes>  ANY_SYM                       /* SQL-2003-R */
%token<bytes>  AS                            /* SQL-2003-R */
%token<bytes>  ASC                           /* SQL-2003-N */
%token<bytes>  ASCII_SYM                     /* MYSQL-FUNC */
%token<bytes>  ASENSITIVE_SYM                /* FUTURE-USE */
%token<bytes>  AT_SYM                        /* SQL-2003-R */
%token<bytes>  AUTOEXTEND_SIZE_SYM
%token<bytes>  AUTO_INC
%token<bytes>  AVG_ROW_LENGTH
%token<bytes>  AVG_SYM                       /* SQL-2003-N */
%token<bytes>  BACKUP_SYM
%token<bytes>  BEFORE_SYM                    /* SQL-2003-N */
%token<bytes>  BEGIN_SYM                     /* SQL-2003-R */
%token<bytes>  BETWEEN_SYM                   /* SQL-2003-R */
%token<bytes>  BIGINT                        /* SQL-2003-R */
%token<bytes>  BINARY                        /* SQL-2003-R */
%token<bytes>  BINLOG_SYM
%token<bytes>  BIN_NUM
%token<bytes>  BIT_AND                       /* MYSQL-FUNC */
%token<bytes>  BIT_OR                        /* MYSQL-FUNC */
%token<bytes>  BIT_SYM                       /* MYSQL-FUNC */
%token<bytes>  BIT_XOR                       /* MYSQL-FUNC */
%token<bytes>  BLOB_SYM                      /* SQL-2003-R */
%token<bytes>  BLOCK_SYM
%token<bytes>  BOOLEAN_SYM                   /* SQL-2003-R */
%token<bytes>  BOOL_SYM
%token<bytes>  BOTH                          /* SQL-2003-R */
%token<bytes>  BTREE_SYM
%token<bytes>  BY                            /* SQL-2003-R */
%token<bytes>  BYTE_SYM
%token<bytes>  CACHE_SYM
%token<bytes>  CALL_SYM                      /* SQL-2003-R */
%token<bytes>  CASCADE                       /* SQL-2003-N */
%token<bytes>  CASCADED                      /* SQL-2003-R */
%token<bytes>  CASE_SYM                      /* SQL-2003-R */
%token<bytes>  CAST_SYM                      /* SQL-2003-R */
%token<bytes>  CATALOG_NAME_SYM              /* SQL-2003-N */
%token<bytes>  CHAIN_SYM                     /* SQL-2003-N */
%token<bytes>  CHANGE
%token<bytes>  CHANGED
%token<bytes>  CHARSET
%token<bytes>  CHAR_SYM                      /* SQL-2003-R */
%token<bytes>  CHECKSUM_SYM
%token<bytes>  CHECK_SYM                     /* SQL-2003-R */
%token<bytes>  CIPHER_SYM
%token<bytes>  CLASS_ORIGIN_SYM              /* SQL-2003-N */
%token<bytes>  CLIENT_SYM
%token<bytes>  CLOSE_SYM                     /* SQL-2003-R */
%token<bytes>  COALESCE                      /* SQL-2003-N */
%token<bytes>  CODE_SYM
%token<bytes>  COLLATE_SYM                   /* SQL-2003-R */
%token<bytes>  COLLATION_SYM                 /* SQL-2003-N */
%token<bytes>  COLUMNS
%token<bytes>  COLUMN_SYM                    /* SQL-2003-R */
%token<bytes>  COLUMN_FORMAT_SYM
%token<bytes>  COLUMN_NAME_SYM               /* SQL-2003-N */
%token<bytes>  COMMENT_SYM
%token<bytes>  COMMITTED_SYM                 /* SQL-2003-N */
%token<bytes>  COMMIT_SYM                    /* SQL-2003-R */
%token<bytes>  COMPACT_SYM
%token<bytes>  COMPLETION_SYM
%token<bytes>  COMPRESSED_SYM
%token<bytes>  CONCURRENT
%token<bytes>  CONDITION_SYM                 /* SQL-2003-R, SQL-2008-R */
%token<bytes>  CONNECTION_SYM
%token<bytes>  CONSISTENT_SYM
%token<bytes>  CONSTRAINT                    /* SQL-2003-R */
%token<bytes>  CONSTRAINT_CATALOG_SYM        /* SQL-2003-N */
%token<bytes>  CONSTRAINT_NAME_SYM           /* SQL-2003-N */
%token<bytes>  CONSTRAINT_SCHEMA_SYM         /* SQL-2003-N */
%token<bytes>  CONTAINS_SYM                  /* SQL-2003-N */
%token<bytes>  CONTEXT_SYM
%token<bytes>  CONTINUE_SYM                  /* SQL-2003-R */
%token<bytes>  CONVERT_SYM                   /* SQL-2003-N */
%token<bytes>  COUNT_SYM                     /* SQL-2003-N */
%token<bytes>  CPU_SYM
%token<bytes>  CREATE                        /* SQL-2003-R */
%token<bytes>  CROSS                         /* SQL-2003-R */
%token<bytes>  CUBE_SYM                      /* SQL-2003-R */
%token<bytes>  CURDATE                       /* MYSQL-FUNC */
%token<bytes>  CURRENT_SYM                   /* SQL-2003-R */
%token<bytes>  CURRENT_USER                  /* SQL-2003-R */
%token<bytes>  CURSOR_SYM                    /* SQL-2003-R */
%token<bytes>  CURSOR_NAME_SYM               /* SQL-2003-N */
%token<bytes>  CURTIME                       /* MYSQL-FUNC */
%token<bytes>  DATABASE
%token<bytes>  DATABASES
%token<bytes>  DATAFILE_SYM
%token<bytes>  DATA_SYM                      /* SQL-2003-N */
%token<bytes>  DATETIME
%token<bytes>  DATE_ADD_INTERVAL             /* MYSQL-FUNC */
%token<bytes>  DATE_SUB_INTERVAL             /* MYSQL-FUNC */
%token<bytes>  DATE_SYM                      /* SQL-2003-R */
%token<bytes>  DAY_HOUR_SYM
%token<bytes>  DAY_MICROSECOND_SYM
%token<bytes>  DAY_MINUTE_SYM
%token<bytes>  DAY_SECOND_SYM
%token<bytes>  DAY_SYM                       /* SQL-2003-R */
%token<bytes>  DEALLOCATE_SYM                /* SQL-2003-R */
%token<bytes>  DECIMAL_NUM
%token<bytes>  DECIMAL_SYM                   /* SQL-2003-R */
%token<bytes>  DECLARE_SYM                   /* SQL-2003-R */
%token<bytes>  DEFAULT                       /* SQL-2003-R */
%token<bytes>  DEFAULT_AUTH_SYM              /* INTERNAL */
%token<bytes>  DEFINER_SYM
%token<bytes>  DELAYED_SYM
%token<bytes>  DELAY_KEY_WRITE_SYM
%token<bytes>  DELETE_SYM                    /* SQL-2003-R */
%token<bytes>  DESC                          /* SQL-2003-N */
%token<bytes>  DESCRIBE                      /* SQL-2003-R */
%token<bytes>  DES_KEY_FILE
%token<bytes>  DETERMINISTIC_SYM             /* SQL-2003-R */
%token<bytes>  DIAGNOSTICS_SYM               /* SQL-2003-N */
%token<bytes>  DIRECTORY_SYM
%token<bytes>  DISABLE_SYM
%token<bytes>  DISCARD
%token<bytes>  DISK_SYM
%token<bytes>  DISTINCT                      /* SQL-2003-R */
%token<bytes>  DIV_SYM
%token<bytes>  DOUBLE_SYM                    /* SQL-2003-R */
%token<bytes>  DO_SYM
%token<bytes>  DROP                          /* SQL-2003-R */
%token<bytes>  DUAL_SYM
%token<bytes>  DUMPFILE
%token<bytes>  DUPLICATE_SYM
%token<bytes>  DYNAMIC_SYM                   /* SQL-2003-R */
%token<bytes>  EACH_SYM                      /* SQL-2003-R */
%token<bytes>  ELSE                          /* SQL-2003-R */
%token<bytes>  ELSEIF_SYM
%token<bytes>  ENABLE_SYM
%token<bytes>  ENCLOSED
%token<bytes>  END                           /* SQL-2003-R */
%token<bytes>  ENDS_SYM
%token<bytes>  END_OF_INPUT                  /* INTERNAL */
%token<bytes>  ENGINES_SYM
%token<bytes>  ENGINE_SYM
%token<bytes>  ENUM
%token<bytes>  EQ                            /* OPERATOR */
%token<bytes>  EQUAL_SYM                     /* OPERATOR */
%token<bytes>  ERROR_SYM
%token<bytes>  ERRORS
%token<bytes>  ESCAPED
%token<bytes>  ESCAPE_SYM                    /* SQL-2003-R */
%token<bytes>  EVENTS_SYM
%token<bytes>  EVENT_SYM
%token<bytes>  EVERY_SYM                     /* SQL-2003-N */
%token<bytes>  EXCHANGE_SYM
%token<bytes>  EXECUTE_SYM                   /* SQL-2003-R */
%token<bytes>  EXISTS                        /* SQL-2003-R */
%token<bytes>  EXIT_SYM
%token<bytes>  EXPANSION_SYM
%token<bytes>  EXPIRE_SYM
%token<bytes>  EXPORT_SYM
%token<bytes>  EXTENDED_SYM
%token<bytes>  EXTENT_SIZE_SYM
%token<bytes>  EXTRACT_SYM                   /* SQL-2003-N */
%token<bytes>  FALSE_SYM                     /* SQL-2003-R */
%token<bytes>  FAST_SYM
%token<bytes>  FAULTS_SYM
%token<bytes>  FETCH_SYM                     /* SQL-2003-R */
%token<bytes>  FILE_SYM
%token<bytes>  FIRST_SYM                     /* SQL-2003-N */
%token<bytes>  FIXED_SYM
%token<bytes>  FLOAT_NUM
%token<bytes>  FLOAT_SYM                     /* SQL-2003-R */
%token<bytes>  FLUSH_SYM
%token<bytes>  FORCE_SYM
%token<bytes>  FOREIGN                       /* SQL-2003-R */
%token<bytes>  FOR_SYM                       /* SQL-2003-R */
%token<bytes>  FORMAT_SYM
%token<bytes>  FOUND_SYM                     /* SQL-2003-R */
%token<bytes>  FROM
%token<bytes>  FULL                          /* SQL-2003-R */
%token<bytes>  FULLTEXT_SYM
%token<bytes>  FUNCTION_SYM                  /* SQL-2003-R */
%token<bytes>  GE
%token<bytes>  GENERAL
%token<bytes>  GEOMETRYCOLLECTION
%token<bytes>  GEOMETRY_SYM
%token<bytes>  GET_FORMAT                    /* MYSQL-FUNC */
%token<bytes>  GET_SYM                       /* SQL-2003-R */
%token<bytes>  GLOBAL_SYM                    /* SQL-2003-R */
%token<bytes>  GRANT                         /* SQL-2003-R */
%token<bytes>  GRANTS
%token<bytes>  GROUP_SYM                     /* SQL-2003-R */
%token<bytes>  GROUP_CONCAT_SYM
%token<bytes>  GT_SYM                        /* OPERATOR */
%token<bytes>  HANDLER_SYM
%token<bytes>  HASH_SYM
%token<bytes>  HAVING                        /* SQL-2003-R */
%token<bytes>  HELP_SYM
%token<bytes>  HEX_NUM
%token<bytes>  HIGH_PRIORITY
%token<bytes>  HOST_SYM
%token<bytes>  HOSTS_SYM
%token<bytes>  HOUR_MICROSECOND_SYM
%token<bytes>  HOUR_MINUTE_SYM
%token<bytes>  HOUR_SECOND_SYM
%token<bytes>  HOUR_SYM                      /* SQL-2003-R */
%token<bytes>  IDENT
%token<bytes>  IDENTIFIED_SYM
%token<bytes>  IDENT_QUOTED
%token<bytes>  IF
%token<bytes>  IGNORE_SYM
%token<bytes>  IGNORE_SERVER_IDS_SYM
%token<bytes>  IMPORT
%token<bytes>  INDEXES
%token<bytes>  INDEX_SYM
%token<bytes>  INFILE
%token<bytes>  INITIAL_SIZE_SYM
%token<bytes>  INNER_SYM                     /* SQL-2003-R */
%token<bytes>  INOUT_SYM                     /* SQL-2003-R */
%token<bytes>  INSENSITIVE_SYM               /* SQL-2003-R */
%token<bytes>  INSERT                        /* SQL-2003-R */
%token<bytes>  INSERT_METHOD
%token<bytes>  INSTALL_SYM
%token<bytes>  INTERVAL_SYM                  /* SQL-2003-R */
%token<bytes>  INTO                          /* SQL-2003-R */
%token<bytes>  INT_SYM                       /* SQL-2003-R */
%token<bytes>  INVOKER_SYM
%token<bytes>  IN_SYM                        /* SQL-2003-R */
%token<bytes>  IO_AFTER_GTIDS                /* MYSQL, FUTURE-USE */
%token<bytes>  IO_BEFORE_GTIDS               /* MYSQL, FUTURE-USE */
%token<bytes>  IO_SYM
%token<bytes>  IPC_SYM
%token<bytes>  IS                            /* SQL-2003-R */
%token<bytes>  ISOLATION                     /* SQL-2003-R */
%token<bytes>  ISSUER_SYM
%token<bytes>  ITERATE_SYM
%token<bytes>  JOIN_SYM                      /* SQL-2003-R */
%token<bytes>  KEYS
%token<bytes>  KEY_BLOCK_SIZE
%token<bytes>  KEY_SYM                       /* SQL-2003-N */
%token<bytes>  KILL_SYM
%token<bytes>  LANGUAGE_SYM                  /* SQL-2003-R */
%token<bytes>  LAST_SYM                      /* SQL-2003-N */
%token<bytes>  LE                            /* OPERATOR */
%token<bytes>  LEADING                       /* SQL-2003-R */
%token<bytes>  LEAVES
%token<bytes>  LEAVE_SYM
%token<bytes>  LEFT                          /* SQL-2003-R */
%token<bytes>  LESS_SYM
%token<bytes>  LEVEL_SYM
%token<bytes>  LEX_HOSTNAME
%token<bytes>  LIKE                          /* SQL-2003-R */
%token<bytes>  LIMIT
%token<bytes>  LINEAR_SYM
%token<bytes>  LINES
%token<bytes>  LINESTRING
%token<bytes>  LIST_SYM
%token<bytes>  LOAD
%token<bytes>  LOCAL_SYM                     /* SQL-2003-R */
%token<bytes>  LOCATOR_SYM                   /* SQL-2003-N */
%token<bytes>  LOCKS_SYM
%token<bytes>  LOCK_SYM
%token<bytes>  LOGFILE_SYM
%token<bytes>  LOGS_SYM
%token<bytes>  LONGBLOB
%token<bytes>  LONGTEXT
%token<bytes>  LONG_NUM
%token<bytes>  LONG_SYM
%token<bytes>  LOOP_SYM
%token<bytes>  LOW_PRIORITY
%token<bytes>  LT                            /* OPERATOR */
%token<bytes>  MASTER_AUTO_POSITION_SYM
%token<bytes>  MASTER_BIND_SYM
%token<bytes>  MASTER_CONNECT_RETRY_SYM
%token<bytes>  MASTER_DELAY_SYM
%token<bytes>  MASTER_HOST_SYM
%token<bytes>  MASTER_LOG_FILE_SYM
%token<bytes>  MASTER_LOG_POS_SYM
%token<bytes>  MASTER_PASSWORD_SYM
%token<bytes>  MASTER_PORT_SYM
%token<bytes>  MASTER_RETRY_COUNT_SYM
%token<bytes>  MASTER_SERVER_ID_SYM
%token<bytes>  MASTER_SSL_CAPATH_SYM
%token<bytes>  MASTER_SSL_CA_SYM
%token<bytes>  MASTER_SSL_CERT_SYM
%token<bytes>  MASTER_SSL_CIPHER_SYM
%token<bytes>  MASTER_SSL_CRL_SYM
%token<bytes>  MASTER_SSL_CRLPATH_SYM
%token<bytes>  MASTER_SSL_KEY_SYM
%token<bytes>  MASTER_SSL_SYM
%token<bytes>  MASTER_SSL_VERIFY_SERVER_CERT_SYM
%token<bytes>  MASTER_SYM
%token<bytes>  MASTER_USER_SYM
%token<bytes>  MASTER_HEARTBEAT_PERIOD_SYM
%token<bytes>  MATCH                         /* SQL-2003-R */
%token<bytes>  MAX_CONNECTIONS_PER_HOUR
%token<bytes>  MAX_QUERIES_PER_HOUR
%token<bytes>  MAX_ROWS
%token<bytes>  MAX_SIZE_SYM
%token<bytes>  MAX_SYM                       /* SQL-2003-N */
%token<bytes>  MAX_UPDATES_PER_HOUR
%token<bytes>  MAX_USER_CONNECTIONS_SYM
%token<bytes>  MAX_VALUE_SYM                 /* SQL-2003-N */
%token<bytes>  MEDIUMBLOB
%token<bytes>  MEDIUMINT
%token<bytes>  MEDIUMTEXT
%token<bytes>  MEDIUM_SYM
%token<bytes>  MEMORY_SYM
%token<bytes>  MERGE_SYM                     /* SQL-2003-R */
%token<bytes>  MESSAGE_TEXT_SYM              /* SQL-2003-N */
%token<bytes>  MICROSECOND_SYM               /* MYSQL-FUNC */
%token<bytes>  MIGRATE_SYM
%token<bytes>  MINUTE_MICROSECOND_SYM
%token<bytes>  MINUTE_SECOND_SYM
%token<bytes>  MINUTE_SYM                    /* SQL-2003-R */
%token<bytes>  MIN_ROWS
%token<bytes>  MIN_SYM                       /* SQL-2003-N */
%token<bytes>  MODE_SYM
%token<bytes>  MODIFIES_SYM                  /* SQL-2003-R */
%token<bytes>  MODIFY_SYM
%token<bytes>  MOD_SYM                       /* SQL-2003-N */
%token<bytes>  MONTH_SYM                     /* SQL-2003-R */
%token<bytes>  MULTILINESTRING
%token<bytes>  MULTIPOINT
%token<bytes>  MULTIPOLYGON
%token<bytes>  MUTEX_SYM
%token<bytes>  MYSQL_ERRNO_SYM
%token<bytes>  NAMES_SYM                     /* SQL-2003-N */
%token<bytes>  NAME_SYM                      /* SQL-2003-N */
%token<bytes>  NATIONAL_SYM                  /* SQL-2003-R */
%token<bytes>  NATURAL                       /* SQL-2003-R */
%token<bytes>  NCHAR_STRING
%token<bytes>  NCHAR_SYM                     /* SQL-2003-R */
%token<bytes>  NDBCLUSTER_SYM
%token<bytes>  NE                            /* OPERATOR */
%token<bytes>  NEG
%token<bytes>  NEW_SYM                       /* SQL-2003-R */
%token<bytes>  NEXT_SYM                      /* SQL-2003-N */
%token<bytes>  NODEGROUP_SYM
%token<bytes>  NONE_SYM                      /* SQL-2003-R */
%token<bytes>  NOT2_SYM
%token<bytes>  NOT_SYM                       /* SQL-2003-R */
%token<bytes>  NOW_SYM
%token<bytes>  NO_SYM                        /* SQL-2003-R */
%token<bytes>  NO_WAIT_SYM
%token<bytes>  NO_WRITE_TO_BINLOG
%token<bytes>  NULL_SYM                      /* SQL-2003-R */
%token<bytes>  NUM
%token<bytes>  NUMBER_SYM                    /* SQL-2003-N */
%token<bytes>  NUMERIC_SYM                   /* SQL-2003-R */
%token<bytes>  NVARCHAR_SYM
%token<bytes>  OFFSET_SYM
%token<bytes>  OLD_PASSWORD
%token<bytes>  ON                            /* SQL-2003-R */
%token<bytes>  ONE_SYM
%token<bytes>  ONLY_SYM                      /* SQL-2003-R */
%token<bytes>  OPEN_SYM                      /* SQL-2003-R */
%token<bytes>  OPTIMIZE
%token<bytes>  OPTIONS_SYM
%token<bytes>  OPTION                        /* SQL-2003-N */
%token<bytes>  OPTIONALLY
%token<bytes>  OR2_SYM
%token<bytes>  ORDER_SYM                     /* SQL-2003-R */
%token<bytes>  OR_OR_SYM                     /* OPERATOR */
%token<bytes>  OR_SYM                        /* SQL-2003-R */
%token<bytes>  OUTER
%token<bytes>  OUTFILE
%token<bytes>  OUT_SYM                       /* SQL-2003-R */
%token<bytes>  OWNER_SYM
%token<bytes>  PACK_KEYS_SYM
%token<bytes>  PAGE_SYM
%token<bytes>  PARAM_MARKER
%token<bytes>  PARSER_SYM
%token<bytes>  PARTIAL                       /* SQL-2003-N */
%token<bytes>  PARTITION_SYM                 /* SQL-2003-R */
%token<bytes>  PARTITIONS_SYM
%token<bytes>  PARTITIONING_SYM
%token<bytes>  PASSWORD
%token<bytes>  PHASE_SYM
%token<bytes>  PLUGIN_DIR_SYM                /* INTERNAL */
%token<bytes>  PLUGIN_SYM
%token<bytes>  PLUGINS_SYM
%token<bytes>  POINT_SYM
%token<bytes>  POLYGON
%token<bytes>  PORT_SYM
%token<bytes>  POSITION_SYM                  /* SQL-2003-N */
%token<bytes>  PRECISION                     /* SQL-2003-R */
%token<bytes>  PREPARE_SYM                   /* SQL-2003-R */
%token<bytes>  PRESERVE_SYM
%token<bytes>  PREV_SYM
%token<bytes>  PRIMARY_SYM                   /* SQL-2003-R */
%token<bytes>  PRIVILEGES                    /* SQL-2003-N */
%token<bytes>  PROCEDURE_SYM                 /* SQL-2003-R */
%token<bytes>  PROCESS
%token<bytes>  PROCESSLIST_SYM
%token<bytes>  PROFILE_SYM
%token<bytes>  PROFILES_SYM
%token<bytes>  PROXY_SYM
%token<bytes>  PURGE
%token<bytes>  QUARTER_SYM
%token<bytes>  QUERY_SYM
%token<bytes>  QUICK
%token<bytes>  RANGE_SYM                     /* SQL-2003-R */
%token<bytes>  READS_SYM                     /* SQL-2003-R */
%token<bytes>  READ_ONLY_SYM
%token<bytes>  READ_SYM                      /* SQL-2003-N */
%token<bytes>  READ_WRITE_SYM
%token<bytes>  REAL                          /* SQL-2003-R */
%token<bytes>  REBUILD_SYM
%token<bytes>  RECOVER_SYM
%token<bytes>  REDOFILE_SYM
%token<bytes>  REDO_BUFFER_SIZE_SYM
%token<bytes>  REDUNDANT_SYM
%token<bytes>  REFERENCES                    /* SQL-2003-R */
%token<bytes>  REGEXP
%token<bytes>  RELAY
%token<bytes>  RELAYLOG_SYM
%token<bytes>  RELAY_LOG_FILE_SYM
%token<bytes>  RELAY_LOG_POS_SYM
%token<bytes>  RELAY_THREAD
%token<bytes>  RELEASE_SYM                   /* SQL-2003-R */
%token<bytes>  RELOAD
%token<bytes>  REMOVE_SYM
%token<bytes>  RENAME
%token<bytes>  REORGANIZE_SYM
%token<bytes>  REPAIR
%token<bytes>  REPEATABLE_SYM                /* SQL-2003-N */
%token<bytes>  REPEAT_SYM                    /* MYSQL-FUNC */
%token<bytes>  REPLACE                       /* MYSQL-FUNC */
%token<bytes>  REPLICATION
%token<bytes>  REQUIRE_SYM
%token<bytes>  RESET_SYM
%token<bytes>  RESIGNAL_SYM                  /* SQL-2003-R */
%token<bytes>  RESOURCES
%token<bytes>  RESTORE_SYM
%token<bytes>  RESTRICT
%token<bytes>  RESUME_SYM
%token<bytes>  RETURNED_SQLSTATE_SYM         /* SQL-2003-N */
%token<bytes>  RETURNS_SYM                   /* SQL-2003-R */
%token<bytes>  RETURN_SYM                    /* SQL-2003-R */
%token<bytes>  REVERSE_SYM
%token<bytes>  REVOKE                        /* SQL-2003-R */
%token<bytes>  RIGHT                         /* SQL-2003-R */
%token<bytes>  ROLLBACK_SYM                  /* SQL-2003-R */
%token<bytes>  ROLLUP_SYM                    /* SQL-2003-R */
%token<bytes>  ROUTINE_SYM                   /* SQL-2003-N */
%token<bytes>  ROWS_SYM                      /* SQL-2003-R */
%token<bytes>  ROW_FORMAT_SYM
%token<bytes>  ROW_SYM                       /* SQL-2003-R */
%token<bytes>  ROW_COUNT_SYM                 /* SQL-2003-N */
%token<bytes>  RTREE_SYM
%token<bytes>  SAVEPOINT_SYM                 /* SQL-2003-R */
%token<bytes>  SCHEDULE_SYM
%token<bytes>  SCHEMA_NAME_SYM               /* SQL-2003-N */
%token<bytes>  SECOND_MICROSECOND_SYM
%token<bytes>  SECOND_SYM                    /* SQL-2003-R */
%token<bytes>  SECURITY_SYM                  /* SQL-2003-N */
%token<bytes>  SELECT_SYM                    /* SQL-2003-R */
%token<bytes>  SENSITIVE_SYM                 /* FUTURE-USE */
%token<bytes>  SEPARATOR_SYM
%token<bytes>  SERIALIZABLE_SYM              /* SQL-2003-N */
%token<bytes>  SERIAL_SYM
%token<bytes>  SESSION_SYM                   /* SQL-2003-N */
%token<bytes>  SERVER_SYM
%token<bytes>  SERVER_OPTIONS
%token<bytes>  SET                           /* SQL-2003-R */
%token<bytes>  SET_VAR
%token<bytes>  SHARE_SYM
%token<bytes>  SHIFT_LEFT                    /* OPERATOR */
%token<bytes>  SHIFT_RIGHT                   /* OPERATOR */
%token<bytes>  SHOW
%token<bytes>  SHUTDOWN
%token<bytes>  SIGNAL_SYM                    /* SQL-2003-R */
%token<bytes>  SIGNED_SYM
%token<bytes>  SIMPLE_SYM                    /* SQL-2003-N */
%token<bytes>  SLAVE
%token<bytes>  SLOW
%token<bytes>  SMALLINT                      /* SQL-2003-R */
%token<bytes>  SNAPSHOT_SYM
%token<bytes>  SOCKET_SYM
%token<bytes>  SONAME_SYM
%token<bytes>  SOUNDS_SYM
%token<bytes>  SOURCE_SYM
%token<bytes>  SPATIAL_SYM
%token<bytes>  SPECIFIC_SYM                  /* SQL-2003-R */
%token<bytes>  SQLEXCEPTION_SYM              /* SQL-2003-R */
%token<bytes>  SQLSTATE_SYM                  /* SQL-2003-R */
%token<bytes>  SQLWARNING_SYM                /* SQL-2003-R */
%token<bytes>  SQL_AFTER_GTIDS               /* MYSQL */
%token<bytes>  SQL_AFTER_MTS_GAPS            /* MYSQL */
%token<bytes>  SQL_BEFORE_GTIDS              /* MYSQL */
%token<bytes>  SQL_BIG_RESULT
%token<bytes>  SQL_BUFFER_RESULT
%token<bytes>  SQL_CACHE_SYM
%token<bytes>  SQL_CALC_FOUND_ROWS
%token<bytes>  SQL_NO_CACHE_SYM
%token<bytes>  SQL_SMALL_RESULT
%token<bytes>  SQL_SYM                       /* SQL-2003-R */
%token<bytes>  SQL_THREAD
%token<bytes>  SSL_SYM
%token<bytes>  STARTING
%token<bytes>  STARTS_SYM
%token<bytes>  START_SYM                     /* SQL-2003-R */
%token<bytes>  STATS_AUTO_RECALC_SYM
%token<bytes>  STATS_PERSISTENT_SYM
%token<bytes>  STATS_SAMPLE_PAGES_SYM
%token<bytes>  STATUS_SYM
%token<bytes>  STDDEV_SAMP_SYM               /* SQL-2003-N */
%token<bytes>  STD_SYM
%token<bytes>  STOP_SYM
%token<bytes>  STORAGE_SYM
%token<bytes>  STRAIGHT_JOIN
%token<bytes>  STRING_SYM
%token<bytes>  SUBCLASS_ORIGIN_SYM           /* SQL-2003-N */
%token<bytes>  SUBDATE_SYM
%token<bytes>  SUBJECT_SYM
%token<bytes>  SUBPARTITIONS_SYM
%token<bytes>  SUBPARTITION_SYM
%token<bytes>  SUBSTRING                     /* SQL-2003-N */
%token<bytes>  SUM_SYM                       /* SQL-2003-N */
%token<bytes>  SUPER_SYM
%token<bytes>  SUSPEND_SYM
%token<bytes>  SWAPS_SYM
%token<bytes>  SWITCHES_SYM
%token<bytes>  SYSDATE
%token<bytes>  TABLES
%token<bytes>  TABLESPACE
%token<bytes>  TABLE_REF_PRIORITY
%token<bytes>  TABLE_SYM                     /* SQL-2003-R */
%token<bytes>  TABLE_CHECKSUM_SYM
%token<bytes>  TABLE_NAME_SYM                /* SQL-2003-N */
%token<bytes>  TEMPORARY                     /* SQL-2003-N */
%token<bytes>  TEMPTABLE_SYM
%token<bytes>  TERMINATED
%token<bytes>  TEXT_STRING
%token<bytes>  TEXT_SYM
%token<bytes>  THAN_SYM
%token<bytes>  THEN_SYM                      /* SQL-2003-R */
%token<bytes>  TIMESTAMP                     /* SQL-2003-R */
%token<bytes>  TIMESTAMP_ADD
%token<bytes>  TIMESTAMP_DIFF
%token<bytes>  TIME_SYM                      /* SQL-2003-R */
%token<bytes>  TINYBLOB
%token<bytes>  TINYINT
%token<bytes>  TINYTEXT
%token<bytes>  TO_SYM                        /* SQL-2003-R */
%token<bytes>  TRAILING                      /* SQL-2003-R */
%token<bytes>  TRANSACTION_SYM
%token<bytes>  TRIGGERS_SYM
%token<bytes>  TRIGGER_SYM                   /* SQL-2003-R */
%token<bytes>  TRIM                          /* SQL-2003-N */
%token<bytes>  TRUE_SYM                      /* SQL-2003-R */
%token<bytes>  TRUNCATE_SYM
%token<bytes>  TYPES_SYM
%token<bytes>  TYPE_SYM                      /* SQL-2003-N */
%token<bytes>  UDF_RETURNS_SYM
%token<bytes>  ULONGLONG_NUM
%token<bytes>  UNCOMMITTED_SYM               /* SQL-2003-N */
%token<bytes>  UNDEFINED_SYM
%token<bytes>  UNDERSCORE_CHARSET
%token<bytes>  UNDOFILE_SYM
%token<bytes>  UNDO_BUFFER_SIZE_SYM
%token<bytes>  UNDO_SYM                      /* FUTURE-USE */
%token<bytes>  UNICODE_SYM
%token<bytes>  UNINSTALL_SYM
%token<bytes>  UNION_SYM                     /* SQL-2003-R */
%token<bytes>  UNIQUE_SYM
%token<bytes>  UNKNOWN_SYM                   /* SQL-2003-R */
%token<bytes>  UNLOCK_SYM
%token<bytes>  UNSIGNED
%token<bytes>  UNTIL_SYM
%token<bytes>  UPDATE_SYM                    /* SQL-2003-R */
%token<bytes>  UPGRADE_SYM
%token<bytes>  USAGE                         /* SQL-2003-N */
%token<bytes>  USER                          /* SQL-2003-R */
%token<bytes>  USE_FRM
%token<bytes>  USE_SYM
%token<bytes>  USING                         /* SQL-2003-R */
%token<bytes>  UTC_DATE_SYM
%token<bytes>  UTC_TIMESTAMP_SYM
%token<bytes>  UTC_TIME_SYM
%token<bytes>  VALUES                        /* SQL-2003-R */
%token<bytes>  VALUE_SYM                     /* SQL-2003-R */
%token<bytes>  VARBINARY
%token<bytes>  VARCHAR                       /* SQL-2003-R */
%token<bytes>  VARIABLES
%token<bytes>  VARIANCE_SYM
%token<bytes>  VARYING                       /* SQL-2003-R */
%token<bytes>  VAR_SAMP_SYM
%token<bytes>  VIEW_SYM                      /* SQL-2003-N */
%token<bytes>  WAIT_SYM
%token<bytes>  WARNINGS
%token<bytes>  WEEK_SYM
%token<bytes>  WEIGHT_STRING_SYM
%token<bytes>  WHEN_SYM                      /* SQL-2003-R */
%token<bytes>  WHERE                         /* SQL-2003-R */
%token<bytes>  WHILE_SYM
%token<bytes>  WITH                          /* SQL-2003-R */
%token<bytes>  WITH_CUBE_SYM                 /* INTERNAL */
%token<bytes>  WITH_ROLLUP_SYM               /* INTERNAL */
%token<bytes>  WORK_SYM                      /* SQL-2003-N */
%token<bytes>  WRAPPER_SYM
%token<bytes>  WRITE_SYM                     /* SQL-2003-N */
%token<bytes>  X509_SYM
%token<bytes>  XA_SYM
%token<bytes>  XML_SYM
%token<bytes>  XOR
%token<bytes>  YEAR_MONTH_SYM
%token<bytes>  YEAR_SYM                      /* SQL-2003-R */
%token<bytes>  ZEROFILL

%left   JOIN_SYM INNER_SYM STRAIGHT_JOIN CROSS LEFT RIGHT
/* A dummy token to force the priority of table_ref production in a join. */
%left   TABLE_REF_PRIORITY
%left   SET_VAR
%left   OR_OR_SYM OR_SYM OR2_SYM
%left   XOR
%left   AND_SYM AND_AND_SYM
%left   BETWEEN_SYM CASE_SYM WHEN_SYM THEN_SYM ELSE
%left   EQ EQUAL_SYM GE GT_SYM LE LT NE IS LIKE REGEXP IN_SYM
%left   '|'
%left   '&'
%left   SHIFT_LEFT SHIFT_RIGHT
%left   '-' '+'
%left   '*' '/' '%' DIV_SYM MOD_SYM
%left   '^'
%left   NEG '~'
%right  NOT_SYM NOT2_SYM
%right  BINARY COLLATE_SYM
%left  INTERVAL_SYM

%start query
%type <statement> verb_clause statement


/* DDL */
%type <statement> alter create drop rename truncate

/* DML */
%type <statement> insert update delete replace call do handler load single_multi

%type <select_statement> select select_init select_init2 select_paren select_part2 select_derived2 opt_select_from query_specification select_init2_derived select_part2_derived select_paren_derived select_derived create_select 
%type <select_statement> view_select view_select_aux create_view_select create_view_select_paren query_expression_body
%type <select_statement> union_opt union_clause_opt select_derived_union union_list 

%type <subquery> subselect

/* Transaction */
%type <statement> commit lock release rollback savepoint start unlock xa 

/* DAL */
%type <statement> analyze binlog_base64_event check checksum optimize repair flush grant install uninstall kill keycache partition_entry preload reset revoke set show flush_options show_param start_option_value_list

/* Replication Statement */
%type <statement> change purge slave 

/* Prepare */
%type <statement> deallocate execute prepare

/* Compound-Statement */
%type <statement> get_diagnostics resignal_stmt signal_stmt

/* MySQL Utility Statement */
%type <statement> describe help use explanable_command

%type <bytes> ident IDENT_sys keyword keyword_sp ident_or_empty opt_wild opt_table_alias opt_db TEXT_STRING_sys ident_or_text interval interval_time_stamp TEXT_STRING_literal old_or_new_charset_name old_or_new_charset_name_or_default charset_name_or_default charset_name 

%type <interf> insert_field_spec insert_values view_or_trigger_or_sp_or_event definer_tail no_definer_tail start_option_value_list_following_option_type

%type <table> table_name_with_opt_use_partition table_ident into_table insert_table table_ident_nodb table_wild_one table_ident_opt_wild table_name table_alias_ref table_lock
%type <table_list> table_list table_lock_list opt_table_list


%type <table_ref> esc_table_ref table_ref table_factor join_table 
%type <table_ref_list> select_into select_from join_table_list derived_table_list table_alias_ref_list table_wild_list 

%type <table_to_table> table_to_table
%type <table_to_table_list> table_to_table_list

%type <table_index> assign_to_keycache assign_to_keycache_parts preload_keys_parts preload_keys
%type <table_index_list> keycache_list_or_parts keycache_list preload_list_or_parts preload_list 

%type <spname> sp_name opt_ev_rename_to

%type <empty> '.'

%type <lock_type> select_lock_type

%type <view_tail> view_tail
%type <trigger_tail> trigger_tail
%type <sp_tail> sp_tail
%type <sf_tail> sf_tail
%type <udf_tail> udf_tail
%type <event_tail> event_tail

%type <like_or_where> wild_and_where

%type <str> internal_variable_name comp_op
%type <variable> option_value_no_option_type option_value_following_option_type option_value
%type <vars> option_value_list_continued option_value_list

%type <life_type> option_type opt_var_ident_type
%type <var_type> 

%type <expr> expr set_expr_or_default
%type <exprs> expr_list
%type <boolexpr> bool_pri
%type <valexpr> predicate bit_expr simple_expr simple_ident literal param_marker variable text_literal temporal_literal NUM_literal simple_ident_q 

%%


query:
  END_OF_INPUT { SetParseTree(MySQLlex, nil) } 
| verb_clause ';' opt_end_of_input { SetParseTree(MySQLlex, $1) } 
| verb_clause END_OF_INPUT { SetParseTree(MySQLlex, $1) }
; 

opt_end_of_input:
 
| END_OF_INPUT;

verb_clause:
  statement { $$ = $1}
| begin { $$ = &Begin{} };

statement:
  alter {$$ = $1}
| analyze {$$ = $1} 
| binlog_base64_event {$$ = $1}
| call {$$ = $1} 
| change {$$ = $1}
| check {$$ = $1}
| checksum {$$ = $1}
| commit {$$ = $1}
| create {$$ = $1}
| deallocate {$$ = $1}
| delete {$$ = $1}
| describe {$$ = $1}
| do {$$ = $1}
| drop {$$ = $1}
| execute {$$ = $1}
| flush {$$ = $1}
| get_diagnostics {$$ = $1}
| grant {$$ = $1}
| handler {$$ = $1}
| help {$$ = $1}
| insert {$$ = $1}
| install {$$ = $1}
| kill {$$ = $1}
| load {$$ = $1}
| lock {$$ = $1}
| optimize {$$ = $1}
| keycache {$$ = $1}
| partition_entry {$$ = $1}
| preload {$$ = $1}
| prepare {$$ = $1}
| purge {$$ = $1}
| release {$$ = $1}
| rename {$$ = $1}
| repair {$$ = $1}
| replace {$$ = $1}
| reset {$$ = $1}
| resignal_stmt {$$ = $1}
| revoke {$$ = $1}
| rollback {$$ = $1}
| savepoint {$$ = $1}
| select {$$ = $1}
| set {$$ = $1}
| signal_stmt {$$ = $1}
| show {$$ = $1}
| slave {$$ = $1}
| start {$$ = $1}
| truncate {$$ = $1}
| uninstall {$$ = $1}
| unlock {$$ = $1}
| update {$$ = $1}
| use {$$ = $1}
| xa {$$ = $1}
;

deallocate:
  deallocate_or_drop PREPARE_SYM ident { $$ = &Deallocate{} };

deallocate_or_drop:
  DEALLOCATE_SYM
| DROP;

prepare:
  PREPARE_SYM ident FROM prepare_src { $$ = &Prepare{} };

prepare_src:
  TEXT_STRING_sys
| '@' ident_or_text;

execute:
  EXECUTE_SYM ident execute_using { $$ = &Execute{} };

execute_using:
 
| USING execute_var_list;

execute_var_list:
  execute_var_list ',' execute_var_ident
| execute_var_ident;

execute_var_ident:
  '@' ident_or_text;

help:
  HELP_SYM ident_or_text { $$ = &Help{} };

change:
  CHANGE MASTER_SYM TO_SYM master_defs { $$ = &Change{} };

master_defs:
  master_def
| master_defs ',' master_def;

master_def:
  MASTER_HOST_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_BIND_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_USER_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_PASSWORD_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_PORT_SYM EQ ulong_num
| MASTER_CONNECT_RETRY_SYM EQ ulong_num
| MASTER_RETRY_COUNT_SYM EQ ulong_num
| MASTER_DELAY_SYM EQ ulong_num
| MASTER_SSL_SYM EQ ulong_num
| MASTER_SSL_CA_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_SSL_CAPATH_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_SSL_CERT_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_SSL_CIPHER_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_SSL_KEY_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_SSL_VERIFY_SERVER_CERT_SYM EQ ulong_num
| MASTER_SSL_CRL_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_SSL_CRLPATH_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_HEARTBEAT_PERIOD_SYM EQ NUM_literal
| IGNORE_SERVER_IDS_SYM EQ '(' ignore_server_id_list ')'
| MASTER_AUTO_POSITION_SYM EQ ulong_num
| master_file_def;

ignore_server_id_list:
 
| ignore_server_id
| ignore_server_id_list ',' ignore_server_id;

ignore_server_id:
  ulong_num;

master_file_def:
  MASTER_LOG_FILE_SYM EQ TEXT_STRING_sys_nonewline
| MASTER_LOG_POS_SYM EQ ulonglong_num
| RELAY_LOG_FILE_SYM EQ TEXT_STRING_sys_nonewline
| RELAY_LOG_POS_SYM EQ ulong_num;

create:
  CREATE opt_table_options TABLE_SYM opt_if_not_exists table_ident create2 { $$ = &CreateTable{Table: $5} }
| CREATE opt_unique INDEX_SYM ident key_alg ON table_ident '(' key_list ')' normal_key_options opt_index_lock_algorithm { $$ = &CreateIndex{} }
| CREATE fulltext INDEX_SYM ident init_key_options ON table_ident '(' key_list ')' fulltext_key_options opt_index_lock_algorithm { $$ = &CreateIndex{} }
| CREATE spatial INDEX_SYM ident init_key_options ON table_ident '(' key_list ')' spatial_key_options opt_index_lock_algorithm { $$ = &CreateIndex{} }
| CREATE DATABASE opt_if_not_exists ident opt_create_database_options { $$ = &CreateDatabase{} }
| CREATE view_or_trigger_or_sp_or_event 
  { 
    switch st := $2.(type) {
        case *viewTail:
        $$ = &CreateView{View: st.View}
        case *triggerTail:
        $$ = &CreateTrigger{Trigger: st.Trigger}
        case *spTail:
        $$ = &CreateProcedure{Procedure: st.Procedure}
        case *sfTail:
        $$ = &CreateFunction{Function: st.Function}
        case *udfTail:
        $$ = &CreateUDF{Function: st.Function}
        case *eventTail:
        $$ = &CreateEvent{Event: st.Event}
        default:
        panic(__yyfmt__.Sprintf("unknow create statement:%T", st))
    }
  }
| CREATE USER clear_privileges grant_list { $$ = &CreateUser{} }
| CREATE LOGFILE_SYM GROUP_SYM logfile_group_info { $$ = &CreateLog{} }
| CREATE TABLESPACE tablespace_info { $$ = &CreateTablespace{} }
| CREATE server_def { $$ = &CreateServer{} }
;

server_def:
  SERVER_SYM ident_or_text FOREIGN DATA_SYM WRAPPER_SYM ident_or_text OPTIONS_SYM '(' server_options_list ')';

server_options_list:
  server_option
| server_options_list ',' server_option;

server_option:
  USER TEXT_STRING_sys
| HOST_SYM TEXT_STRING_sys
| DATABASE TEXT_STRING_sys
| OWNER_SYM TEXT_STRING_sys
| PASSWORD TEXT_STRING_sys
| SOCKET_SYM TEXT_STRING_sys
| PORT_SYM ulong_num;

event_tail:
  remember_name EVENT_SYM opt_if_not_exists sp_name ON SCHEDULE_SYM ev_schedule_time opt_ev_on_completion opt_ev_status opt_ev_comment DO_SYM ev_sql_stmt
  { $$ = &eventTail{Event: $4} }
;

ev_schedule_time:
  EVERY_SYM expr interval ev_starts ev_ends
| AT_SYM expr;

opt_ev_status:
 
| ENABLE_SYM
| DISABLE_SYM ON SLAVE
| DISABLE_SYM;

ev_starts:
 
| STARTS_SYM expr;

ev_ends:
 
| ENDS_SYM expr;

opt_ev_on_completion:
 
| ev_on_completion;

ev_on_completion:
  ON COMPLETION_SYM PRESERVE_SYM
| ON COMPLETION_SYM NOT_SYM PRESERVE_SYM;

opt_ev_comment:
 
| COMMENT_SYM TEXT_STRING_sys;

ev_sql_stmt:
  ev_sql_stmt_inner;

ev_sql_stmt_inner:
  sp_proc_stmt_statement
| sp_proc_stmt_return
| sp_proc_stmt_if
| case_stmt_specification
| sp_labeled_block
| sp_unlabeled_block
| sp_labeled_control
| sp_proc_stmt_unlabeled
| sp_proc_stmt_leave
| sp_proc_stmt_iterate
| sp_proc_stmt_open
| sp_proc_stmt_fetch
| sp_proc_stmt_close;

clear_privileges:
 ;

sp_name:
  ident '.' ident { $$ = &Spname{Qualifier: $1, Name: $3} }
| ident { $$ = &Spname{Name: $1} };

sp_a_chistics:
 
| sp_a_chistics sp_chistic;

sp_c_chistics:
 
| sp_c_chistics sp_c_chistic;

sp_chistic:
  COMMENT_SYM TEXT_STRING_sys
| LANGUAGE_SYM SQL_SYM
| NO_SYM SQL_SYM
| CONTAINS_SYM SQL_SYM
| READS_SYM SQL_SYM DATA_SYM
| MODIFIES_SYM SQL_SYM DATA_SYM
| sp_suid;

sp_c_chistic:
  sp_chistic
| DETERMINISTIC_SYM
| not DETERMINISTIC_SYM;

sp_suid:
  SQL_SYM SECURITY_SYM DEFINER_SYM
| SQL_SYM SECURITY_SYM INVOKER_SYM;

call:
  CALL_SYM sp_name opt_sp_cparam_list { $$ = &Call{Spname:$2} };

opt_sp_cparam_list:
 
| '(' opt_sp_cparams ')';

opt_sp_cparams:
 
| sp_cparams;

sp_cparams:
  sp_cparams ',' expr
| expr;

sp_fdparam_list:
 
| sp_fdparams;

sp_fdparams:
  sp_fdparams ',' sp_fdparam
| sp_fdparam;

sp_init_param:
 ;

sp_fdparam:
  ident sp_init_param type_with_opt_collate;

sp_pdparam_list:
 
| sp_pdparams;

sp_pdparams:
  sp_pdparams ',' sp_pdparam
| sp_pdparam;

sp_pdparam:
  sp_opt_inout sp_init_param ident type_with_opt_collate;

sp_opt_inout:
 
| IN_SYM
| OUT_SYM
| INOUT_SYM;

sp_proc_stmts:
 
| sp_proc_stmts sp_proc_stmt ';';

sp_proc_stmts1:
  sp_proc_stmt ';'
| sp_proc_stmts1 sp_proc_stmt ';';

sp_decls:
 
| sp_decls sp_decl ';';

sp_decl:
  DECLARE_SYM sp_decl_idents type_with_opt_collate sp_opt_default
| DECLARE_SYM ident CONDITION_SYM FOR_SYM sp_cond
| DECLARE_SYM sp_handler_type HANDLER_SYM FOR_SYM sp_hcond_list sp_proc_stmt
| DECLARE_SYM ident CURSOR_SYM FOR_SYM select;

sp_handler_type:
  EXIT_SYM
| CONTINUE_SYM;

sp_hcond_list:
  sp_hcond_element
| sp_hcond_list ',' sp_hcond_element;

sp_hcond_element:
  sp_hcond;

sp_cond:
  ulong_num
| sqlstate;

sqlstate:
  SQLSTATE_SYM opt_value TEXT_STRING_literal;

opt_value:
 
| VALUE_SYM;

sp_hcond:
  sp_cond
| ident
| SQLWARNING_SYM
| not FOUND_SYM
| SQLEXCEPTION_SYM;

signal_stmt:
  SIGNAL_SYM signal_value opt_set_signal_information { $$ = &Signal{} } ;

signal_value:
  ident
| sqlstate;

opt_signal_value:
 
| signal_value;

opt_set_signal_information:
 
| SET signal_information_item_list;

signal_information_item_list:
  signal_condition_information_item_name EQ signal_allowed_expr
| signal_information_item_list ',' signal_condition_information_item_name EQ signal_allowed_expr;

signal_allowed_expr:
  literal
| variable
| simple_ident;

signal_condition_information_item_name:
  CLASS_ORIGIN_SYM
| SUBCLASS_ORIGIN_SYM
| CONSTRAINT_CATALOG_SYM
| CONSTRAINT_SCHEMA_SYM
| CONSTRAINT_NAME_SYM
| CATALOG_NAME_SYM
| SCHEMA_NAME_SYM
| TABLE_NAME_SYM
| COLUMN_NAME_SYM
| CURSOR_NAME_SYM
| MESSAGE_TEXT_SYM
| MYSQL_ERRNO_SYM;

resignal_stmt:
  RESIGNAL_SYM opt_signal_value opt_set_signal_information { $$ = &Resignal{} }; 

get_diagnostics:
  GET_SYM which_area DIAGNOSTICS_SYM diagnostics_information { $$ = &Diagnostics{} } ;

which_area:
 
| CURRENT_SYM;

diagnostics_information:
  statement_information
| CONDITION_SYM condition_number condition_information;

statement_information:
  statement_information_item
| statement_information ',' statement_information_item;

statement_information_item:
  simple_target_specification EQ statement_information_item_name;

simple_target_specification:
  ident
| '@' ident_or_text;

statement_information_item_name:
  NUMBER_SYM
| ROW_COUNT_SYM;

condition_number:
  signal_allowed_expr;

condition_information:
  condition_information_item
| condition_information ',' condition_information_item;

condition_information_item:
  simple_target_specification EQ condition_information_item_name;

condition_information_item_name:
  CLASS_ORIGIN_SYM
| SUBCLASS_ORIGIN_SYM
| CONSTRAINT_CATALOG_SYM
| CONSTRAINT_SCHEMA_SYM
| CONSTRAINT_NAME_SYM
| CATALOG_NAME_SYM
| SCHEMA_NAME_SYM
| TABLE_NAME_SYM
| COLUMN_NAME_SYM
| CURSOR_NAME_SYM
| MESSAGE_TEXT_SYM
| MYSQL_ERRNO_SYM
| RETURNED_SQLSTATE_SYM;

sp_decl_idents:
  ident
| sp_decl_idents ',' ident;

sp_opt_default:
 
| DEFAULT expr;

sp_proc_stmt:
  sp_proc_stmt_statement
| sp_proc_stmt_return
| sp_proc_stmt_if
| case_stmt_specification
| sp_labeled_block
| sp_unlabeled_block
| sp_labeled_control
| sp_proc_stmt_unlabeled
| sp_proc_stmt_leave
| sp_proc_stmt_iterate
| sp_proc_stmt_open
| sp_proc_stmt_fetch
| sp_proc_stmt_close;

sp_proc_stmt_if:
  IF sp_if END IF;

sp_proc_stmt_statement:
  statement;

sp_proc_stmt_return:
  RETURN_SYM expr;

sp_proc_stmt_unlabeled:
  sp_unlabeled_control;

sp_proc_stmt_leave:
  LEAVE_SYM label_ident;

sp_proc_stmt_iterate:
  ITERATE_SYM label_ident;

sp_proc_stmt_open:
  OPEN_SYM ident;

sp_proc_stmt_fetch:
  FETCH_SYM sp_opt_fetch_noise ident INTO sp_fetch_list;

sp_proc_stmt_close:
  CLOSE_SYM ident;

sp_opt_fetch_noise:
 
| NEXT_SYM FROM
| FROM;

sp_fetch_list:
  ident
| sp_fetch_list ',' ident;

sp_if:
  expr THEN_SYM sp_proc_stmts1 sp_elseifs;

sp_elseifs:
 
| ELSEIF_SYM sp_if
| ELSE sp_proc_stmts1;

case_stmt_specification:
  simple_case_stmt
| searched_case_stmt;

simple_case_stmt:
  CASE_SYM expr simple_when_clause_list else_clause_opt END CASE_SYM;

searched_case_stmt:
  CASE_SYM searched_when_clause_list else_clause_opt END CASE_SYM;

simple_when_clause_list:
  simple_when_clause
| simple_when_clause_list simple_when_clause;

searched_when_clause_list:
  searched_when_clause
| searched_when_clause_list searched_when_clause;

simple_when_clause:
  WHEN_SYM expr THEN_SYM sp_proc_stmts1;

searched_when_clause:
  WHEN_SYM expr THEN_SYM sp_proc_stmts1;

else_clause_opt:
 
| ELSE sp_proc_stmts1;

sp_labeled_control:
  label_ident ':' sp_unlabeled_control sp_opt_label;

sp_opt_label:
 
| label_ident;

sp_labeled_block:
  label_ident ':' sp_block_content sp_opt_label;

sp_unlabeled_block:
  sp_block_content;

sp_block_content:
  BEGIN_SYM sp_decls sp_proc_stmts END;

sp_unlabeled_control:
  LOOP_SYM sp_proc_stmts1 END LOOP_SYM
| WHILE_SYM expr DO_SYM sp_proc_stmts1 END WHILE_SYM
| REPEAT_SYM sp_proc_stmts1 UNTIL_SYM expr END REPEAT_SYM;

trg_action_time:
  BEFORE_SYM
| AFTER_SYM;

trg_event:
  INSERT
| UPDATE_SYM
| DELETE_SYM;

change_tablespace_access:
  tablespace_name ts_access_mode;

change_tablespace_info:
  tablespace_name CHANGE ts_datafile change_ts_option_list;

tablespace_info:
  tablespace_name ADD ts_datafile opt_logfile_group_name tablespace_option_list;

opt_logfile_group_name:
 
| USE_SYM LOGFILE_SYM GROUP_SYM ident;

alter_tablespace_info:
  tablespace_name ADD ts_datafile alter_tablespace_option_list
| tablespace_name DROP ts_datafile alter_tablespace_option_list;

logfile_group_info:
  logfile_group_name add_log_file logfile_group_option_list;

alter_logfile_group_info:
  logfile_group_name add_log_file alter_logfile_group_option_list;

add_log_file:
  ADD lg_undofile
| ADD lg_redofile;

change_ts_option_list:
  change_ts_options;

change_ts_options:
  change_ts_option
| change_ts_options change_ts_option
| change_ts_options ',' change_ts_option;

change_ts_option:
  opt_ts_initial_size
| opt_ts_autoextend_size
| opt_ts_max_size;

tablespace_option_list:
 
| tablespace_options;

tablespace_options:
  tablespace_option
| tablespace_options tablespace_option
| tablespace_options ',' tablespace_option;

tablespace_option:
  opt_ts_initial_size
| opt_ts_autoextend_size
| opt_ts_max_size
| opt_ts_extent_size
| opt_ts_nodegroup
| opt_ts_engine
| ts_wait
| opt_ts_comment;

alter_tablespace_option_list:
 
| alter_tablespace_options;

alter_tablespace_options:
  alter_tablespace_option
| alter_tablespace_options alter_tablespace_option
| alter_tablespace_options ',' alter_tablespace_option;

alter_tablespace_option:
  opt_ts_initial_size
| opt_ts_autoextend_size
| opt_ts_max_size
| opt_ts_engine
| ts_wait;

logfile_group_option_list:
 
| logfile_group_options;

logfile_group_options:
  logfile_group_option
| logfile_group_options logfile_group_option
| logfile_group_options ',' logfile_group_option;

logfile_group_option:
  opt_ts_initial_size
| opt_ts_undo_buffer_size
| opt_ts_redo_buffer_size
| opt_ts_nodegroup
| opt_ts_engine
| ts_wait
| opt_ts_comment;

alter_logfile_group_option_list:
 
| alter_logfile_group_options;

alter_logfile_group_options:
  alter_logfile_group_option
| alter_logfile_group_options alter_logfile_group_option
| alter_logfile_group_options ',' alter_logfile_group_option;

alter_logfile_group_option:
  opt_ts_initial_size
| opt_ts_engine
| ts_wait;

ts_datafile:
  DATAFILE_SYM TEXT_STRING_sys;

lg_undofile:
  UNDOFILE_SYM TEXT_STRING_sys;

lg_redofile:
  REDOFILE_SYM TEXT_STRING_sys;

tablespace_name:
  ident;

logfile_group_name:
  ident;

ts_access_mode:
  READ_ONLY_SYM
| READ_WRITE_SYM
| NOT_SYM ACCESSIBLE_SYM;

opt_ts_initial_size:
  INITIAL_SIZE_SYM opt_equal size_number;

opt_ts_autoextend_size:
  AUTOEXTEND_SIZE_SYM opt_equal size_number;

opt_ts_max_size:
  MAX_SIZE_SYM opt_equal size_number;

opt_ts_extent_size:
  EXTENT_SIZE_SYM opt_equal size_number;

opt_ts_undo_buffer_size:
  UNDO_BUFFER_SIZE_SYM opt_equal size_number;

opt_ts_redo_buffer_size:
  REDO_BUFFER_SIZE_SYM opt_equal size_number;

opt_ts_nodegroup:
  NODEGROUP_SYM opt_equal real_ulong_num;

opt_ts_comment:
  COMMENT_SYM opt_equal TEXT_STRING_sys;

opt_ts_engine:
  opt_storage ENGINE_SYM opt_equal storage_engines;

ts_wait:
  WAIT_SYM
| NO_WAIT_SYM;

size_number:
  real_ulonglong_num
| IDENT_sys;

create2:
  '(' create2a
| opt_create_table_options opt_create_partitioning create3
| LIKE table_ident
| '(' LIKE table_ident ')';

create2a:
  create_field_list ')' opt_create_table_options opt_create_partitioning create3
| opt_create_partitioning create_select ')' union_opt;

create3:
 
| opt_duplicate opt_as create_select union_clause_opt
| opt_duplicate opt_as '(' create_select ')' union_opt;

opt_create_partitioning:
  opt_partitioning;

opt_partitioning:
 
| partitioning;

partitioning:
  PARTITION_SYM have_partitioning partition;

have_partitioning:
 ;

partition_entry:
  PARTITION_SYM partition { $$ = &Partition{} };

partition:
  BY part_type_def opt_num_parts opt_sub_part part_defs;

part_type_def:
  opt_linear KEY_SYM opt_key_algo '(' part_field_list ')'
| opt_linear HASH_SYM part_func
| RANGE_SYM part_func
| RANGE_SYM part_column_list
| LIST_SYM part_func
| LIST_SYM part_column_list;

opt_linear:
 
| LINEAR_SYM;

opt_key_algo:
 
| ALGORITHM_SYM EQ real_ulong_num;

part_field_list:
 
| part_field_item_list;

part_field_item_list:
  part_field_item
| part_field_item_list ',' part_field_item;

part_field_item:
  ident;

part_column_list:
  COLUMNS '(' part_field_list ')';

part_func:
  '(' remember_name part_func_expr remember_end ')';

sub_part_func:
  '(' remember_name part_func_expr remember_end ')';

opt_num_parts:
 
| PARTITIONS_SYM real_ulong_num;

opt_sub_part:
 
| SUBPARTITION_SYM BY opt_linear HASH_SYM sub_part_func opt_num_subparts
| SUBPARTITION_SYM BY opt_linear KEY_SYM opt_key_algo '(' sub_part_field_list ')' opt_num_subparts;

sub_part_field_list:
  sub_part_field_item
| sub_part_field_list ',' sub_part_field_item;

sub_part_field_item:
  ident;

part_func_expr:
  bit_expr;

opt_num_subparts:
 
| SUBPARTITIONS_SYM real_ulong_num;

part_defs:
 
| '(' part_def_list ')';

part_def_list:
  part_definition
| part_def_list ',' part_definition;

part_definition:
  PARTITION_SYM part_name opt_part_values opt_part_options opt_sub_partition;

part_name:
  ident;

opt_part_values:
 
| VALUES LESS_SYM THAN_SYM part_func_max
| VALUES IN_SYM part_values_in;

part_func_max:
  MAX_VALUE_SYM
| part_value_item;

part_values_in:
  part_value_item
| '(' part_value_list ')';

part_value_list:
  part_value_item
| part_value_list ',' part_value_item;

part_value_item:
  '(' part_value_item_list ')';

part_value_item_list:
  part_value_expr_item
| part_value_item_list ',' part_value_expr_item;

part_value_expr_item:
  MAX_VALUE_SYM
| bit_expr;

opt_sub_partition:
 
| '(' sub_part_list ')';

sub_part_list:
  sub_part_definition
| sub_part_list ',' sub_part_definition;

sub_part_definition:
  SUBPARTITION_SYM sub_name opt_part_options;

sub_name:
  ident_or_text;

opt_part_options:
 
| opt_part_option_list;

opt_part_option_list:
  opt_part_option_list opt_part_option
| opt_part_option;

opt_part_option:
  TABLESPACE opt_equal ident_or_text
| opt_storage ENGINE_SYM opt_equal storage_engines
| NODEGROUP_SYM opt_equal real_ulong_num
| MAX_ROWS opt_equal real_ulonglong_num
| MIN_ROWS opt_equal real_ulonglong_num
| DATA_SYM DIRECTORY_SYM opt_equal TEXT_STRING_sys
| INDEX_SYM DIRECTORY_SYM opt_equal TEXT_STRING_sys
| COMMENT_SYM opt_equal TEXT_STRING_sys;

create_select:
  SELECT_SYM select_options select_item_list opt_select_from
  {
    if $4 == nil {
        $$ = &Select{From: nil, LockType: LockType_NoLock}
    } else {
        $$ = &Select{From: $4.(*Select).From, LockType: $4.(*Select).LockType}
    }    
  } 
;

opt_as:
 
| AS;

opt_create_database_options:
 
| create_database_options;

create_database_options:
  create_database_option
| create_database_options create_database_option;

create_database_option:
  default_collation
| default_charset;

opt_table_options:
 
| table_options;

table_options:
  table_option
| table_option table_options;

table_option:
  TEMPORARY;

opt_if_not_exists:
 
| IF not EXISTS;

opt_create_table_options:
 
| create_table_options;

create_table_options_space_separated:
  create_table_option
| create_table_option create_table_options_space_separated;

create_table_options:
  create_table_option
| create_table_option create_table_options
| create_table_option ',' create_table_options;

create_table_option:
  ENGINE_SYM opt_equal storage_engines
| MAX_ROWS opt_equal ulonglong_num
| MIN_ROWS opt_equal ulonglong_num
| AVG_ROW_LENGTH opt_equal ulong_num
| PASSWORD opt_equal TEXT_STRING_sys
| COMMENT_SYM opt_equal TEXT_STRING_sys
| AUTO_INC opt_equal ulonglong_num
| PACK_KEYS_SYM opt_equal ulong_num
| PACK_KEYS_SYM opt_equal DEFAULT
| STATS_AUTO_RECALC_SYM opt_equal ulong_num
| STATS_AUTO_RECALC_SYM opt_equal DEFAULT
| STATS_PERSISTENT_SYM opt_equal ulong_num
| STATS_PERSISTENT_SYM opt_equal DEFAULT
| STATS_SAMPLE_PAGES_SYM opt_equal ulong_num
| STATS_SAMPLE_PAGES_SYM opt_equal DEFAULT
| CHECKSUM_SYM opt_equal ulong_num
| TABLE_CHECKSUM_SYM opt_equal ulong_num
| DELAY_KEY_WRITE_SYM opt_equal ulong_num
| ROW_FORMAT_SYM opt_equal row_types
| UNION_SYM opt_equal '(' opt_table_list ')'
| default_charset
| default_collation
| INSERT_METHOD opt_equal merge_insert_types
| DATA_SYM DIRECTORY_SYM opt_equal TEXT_STRING_sys
| INDEX_SYM DIRECTORY_SYM opt_equal TEXT_STRING_sys
| TABLESPACE ident
| STORAGE_SYM DISK_SYM
| STORAGE_SYM MEMORY_SYM
| CONNECTION_SYM opt_equal TEXT_STRING_sys
| KEY_BLOCK_SIZE opt_equal ulong_num;

default_charset:
  opt_default charset opt_equal charset_name_or_default;

default_collation:
  opt_default COLLATE_SYM opt_equal collation_name_or_default;

storage_engines:
  ident_or_text;

known_storage_engines:
  ident_or_text;

row_types:
  DEFAULT
| FIXED_SYM
| DYNAMIC_SYM
| COMPRESSED_SYM
| REDUNDANT_SYM
| COMPACT_SYM;

merge_insert_types:
  NO_SYM
| FIRST_SYM
| LAST_SYM;

opt_select_from:
  opt_limit_clause { $$ = nil }
| select_from select_lock_type { $$ = &Select{From: $1, LockType: $2} }
;

udf_type:
  STRING_SYM
| REAL
| DECIMAL_SYM
| INT_SYM;

create_field_list:
  field_list;

field_list:
  field_list_item
| field_list ',' field_list_item;

field_list_item:
  column_def
| key_def;

column_def:
  field_spec opt_check_constraint
| field_spec references;

key_def:
  normal_key_type opt_ident key_alg '(' key_list ')' normal_key_options
| fulltext opt_key_or_index opt_ident init_key_options '(' key_list ')' fulltext_key_options
| spatial opt_key_or_index opt_ident init_key_options '(' key_list ')' spatial_key_options
| opt_constraint constraint_key_type opt_ident key_alg '(' key_list ')' normal_key_options
| opt_constraint FOREIGN KEY_SYM opt_ident '(' key_list ')' references
| opt_constraint check_constraint;

opt_check_constraint:
 
| check_constraint;

check_constraint:
  CHECK_SYM '(' expr ')';

opt_constraint:
 
| constraint;

constraint:
  CONSTRAINT opt_ident;

field_spec:
  field_ident type opt_attribute;

type:
  int_type opt_field_length field_options
| real_type opt_precision field_options
| FLOAT_SYM float_options field_options
| BIT_SYM
| BIT_SYM field_length
| BOOL_SYM
| BOOLEAN_SYM
| char field_length opt_binary
| char opt_binary
| nchar field_length opt_bin_mod
| nchar opt_bin_mod
| BINARY field_length
| BINARY
| varchar field_length opt_binary
| nvarchar field_length opt_bin_mod
| VARBINARY field_length
| YEAR_SYM opt_field_length field_options
| DATE_SYM
| TIME_SYM type_datetime_precision
| TIMESTAMP type_datetime_precision
| DATETIME type_datetime_precision
| TINYBLOB
| BLOB_SYM opt_field_length
| spatial_type
| MEDIUMBLOB
| LONGBLOB
| LONG_SYM VARBINARY
| LONG_SYM varchar opt_binary
| TINYTEXT opt_binary
| TEXT_SYM opt_field_length opt_binary
| MEDIUMTEXT opt_binary
| LONGTEXT opt_binary
| DECIMAL_SYM float_options field_options
| NUMERIC_SYM float_options field_options
| FIXED_SYM float_options field_options
| ENUM '(' string_list ')' opt_binary
| SET '(' string_list ')' opt_binary
| LONG_SYM opt_binary
| SERIAL_SYM;

spatial_type:
  GEOMETRY_SYM
| GEOMETRYCOLLECTION
| POINT_SYM
| MULTIPOINT
| LINESTRING
| MULTILINESTRING
| POLYGON
| MULTIPOLYGON;

char:
  CHAR_SYM;

nchar:
  NCHAR_SYM
| NATIONAL_SYM CHAR_SYM;

varchar:
  char VARYING
| VARCHAR;

nvarchar:
  NATIONAL_SYM VARCHAR
| NVARCHAR_SYM
| NCHAR_SYM VARCHAR
| NATIONAL_SYM CHAR_SYM VARYING
| NCHAR_SYM VARYING;

int_type:
  INT_SYM
| TINYINT
| SMALLINT
| MEDIUMINT
| BIGINT;

real_type:
  REAL
| DOUBLE_SYM
| DOUBLE_SYM PRECISION;

float_options:
 
| field_length
| precision;

precision:
  '(' NUM ',' NUM ')';

type_datetime_precision:
 
| '(' NUM ')';

func_datetime_precision:
 
| '(' ')'
| '(' NUM ')';

field_options:
 
| field_opt_list;

field_opt_list:
  field_opt_list field_option
| field_option;

field_option:
  SIGNED_SYM
| UNSIGNED
| ZEROFILL;

field_length:
  '(' LONG_NUM ')'
| '(' ULONGLONG_NUM ')'
| '(' DECIMAL_NUM ')'
| '(' NUM ')';

opt_field_length:
 
| field_length;

opt_precision:
 
| precision;

opt_attribute:
 
| opt_attribute_list;

opt_attribute_list:
  opt_attribute_list attribute
| attribute;

attribute:
  NULL_SYM
| not NULL_SYM
| DEFAULT now_or_signed_literal
| ON UPDATE_SYM now
| AUTO_INC
| SERIAL_SYM DEFAULT VALUE_SYM
| opt_primary KEY_SYM
| UNIQUE_SYM
| UNIQUE_SYM KEY_SYM
| COMMENT_SYM TEXT_STRING_sys
| COLLATE_SYM collation_name
| COLUMN_FORMAT_SYM DEFAULT
| COLUMN_FORMAT_SYM FIXED_SYM
| COLUMN_FORMAT_SYM DYNAMIC_SYM
| STORAGE_SYM DEFAULT
| STORAGE_SYM DISK_SYM
| STORAGE_SYM MEMORY_SYM;

type_with_opt_collate:
  type opt_collate;

now:
  NOW_SYM func_datetime_precision;

now_or_signed_literal:
  now
| signed_literal;

charset:
  CHAR_SYM SET
| CHARSET;

charset_name:
  ident_or_text { $$ = $1 }
| BINARY { $$ = $1 };

charset_name_or_default:
  charset_name { $$ = $1 }
| DEFAULT { $$ = $1 };

opt_load_data_charset:
 
| charset charset_name_or_default;

old_or_new_charset_name:
  ident_or_text { $$ = $1 }
| BINARY { $$ = $1 };

old_or_new_charset_name_or_default:
  old_or_new_charset_name { $$ = $1 }
| DEFAULT { $$ = $1 };

collation_name:
  ident_or_text;

opt_collate:
 
| COLLATE_SYM collation_name_or_default;

collation_name_or_default:
  collation_name
| DEFAULT;

opt_default:
 
| DEFAULT;

ascii:
  ASCII_SYM
| BINARY ASCII_SYM
| ASCII_SYM BINARY;

unicode:
  UNICODE_SYM
| UNICODE_SYM BINARY
| BINARY UNICODE_SYM;

opt_binary:
 
| ascii
| unicode
| BYTE_SYM
| charset charset_name opt_bin_mod
| BINARY
| BINARY charset charset_name;

opt_bin_mod:
 
| BINARY;

ws_nweights:
  '(' real_ulong_num ')';

ws_level_flag_desc:
  ASC
| DESC;

ws_level_flag_reverse:
  REVERSE_SYM;

ws_level_flags:
 
| ws_level_flag_desc
| ws_level_flag_desc ws_level_flag_reverse
| ws_level_flag_reverse;

ws_level_number:
  real_ulong_num;

ws_level_list_item:
  ws_level_number ws_level_flags;

ws_level_list:
  ws_level_list_item
| ws_level_list ',' ws_level_list_item;

ws_level_range:
  ws_level_number '-' ws_level_number;

ws_level_list_or_range:
  ws_level_list
| ws_level_range;

opt_ws_levels:
 
| LEVEL_SYM ws_level_list_or_range;

opt_primary:
 
| PRIMARY_SYM;

references:
  REFERENCES table_ident opt_ref_list opt_match_clause opt_on_update_delete;

opt_ref_list:
 
| '(' ref_list ')';

ref_list:
  ref_list ',' ident
| ident;

opt_match_clause:
 
| MATCH FULL
| MATCH PARTIAL
| MATCH SIMPLE_SYM;

opt_on_update_delete:
 
| ON UPDATE_SYM delete_option
| ON DELETE_SYM delete_option
| ON UPDATE_SYM delete_option ON DELETE_SYM delete_option
| ON DELETE_SYM delete_option ON UPDATE_SYM delete_option;

delete_option:
  RESTRICT
| CASCADE
| SET NULL_SYM
| NO_SYM ACTION
| SET DEFAULT;

normal_key_type:
  key_or_index;

constraint_key_type:
  PRIMARY_SYM KEY_SYM
| UNIQUE_SYM opt_key_or_index;

key_or_index:
  KEY_SYM
| INDEX_SYM;

opt_key_or_index:
 
| key_or_index;

keys_or_index:
  KEYS
| INDEX_SYM
| INDEXES;

opt_unique:
 
| UNIQUE_SYM;

fulltext:
  FULLTEXT_SYM;

spatial:
  SPATIAL_SYM;

init_key_options:
 ;

key_alg:
  init_key_options
| init_key_options key_using_alg;

normal_key_options:
 
| normal_key_opts;

fulltext_key_options:
 
| fulltext_key_opts;

spatial_key_options:
 
| spatial_key_opts;

normal_key_opts:
  normal_key_opt
| normal_key_opts normal_key_opt;

spatial_key_opts:
  spatial_key_opt
| spatial_key_opts spatial_key_opt;

fulltext_key_opts:
  fulltext_key_opt
| fulltext_key_opts fulltext_key_opt;

key_using_alg:
  USING btree_or_rtree
| TYPE_SYM btree_or_rtree;

all_key_opt:
  KEY_BLOCK_SIZE opt_equal ulong_num
| COMMENT_SYM TEXT_STRING_sys;

normal_key_opt:
  all_key_opt
| key_using_alg;

spatial_key_opt:
  all_key_opt;

fulltext_key_opt:
  all_key_opt
| WITH PARSER_SYM IDENT_sys;

btree_or_rtree:
  BTREE_SYM
| RTREE_SYM
| HASH_SYM;

key_list:
  key_list ',' key_part order_dir
| key_part order_dir;

key_part:
  ident
| ident '(' NUM ')';

opt_ident:
 
| field_ident;

opt_component:
 
| '.' ident;

string_list:
  text_string
| string_list ',' text_string;

alter:
  ALTER opt_ignore TABLE_SYM table_ident alter_commands 
  { $$ = &AlterTable{Table: $4} }
| ALTER DATABASE ident_or_empty create_database_options 
  { $$ = &AlterDatabase{Schema: $3} }
| ALTER DATABASE ident UPGRADE_SYM DATA_SYM DIRECTORY_SYM NAME_SYM 
  { $$ = &AlterDatabase{Schema: $3} }
| ALTER PROCEDURE_SYM sp_name sp_a_chistics { $$ = &AlterProcedure{Procedure: $3} }
| ALTER FUNCTION_SYM sp_name sp_a_chistics { $$ = &AlterFunction{Function: $3} }
| ALTER view_algorithm definer_opt view_tail { $$ = &AlterView{View: $4.View, As: $4.As} }
| ALTER definer_opt view_tail { $$ = &AlterView{View: $3.View, As: $3.As} }
| ALTER definer_opt EVENT_SYM sp_name ev_alter_on_schedule_completion opt_ev_rename_to opt_ev_status opt_ev_comment opt_ev_sql_stmt { $$ = &AlterEvent{Event: $4, Rename: $6} }
| ALTER TABLESPACE alter_tablespace_info { $$ = &AlterTablespace{} }
| ALTER LOGFILE_SYM GROUP_SYM alter_logfile_group_info { $$ = &AlterLogfile{} }
| ALTER TABLESPACE change_tablespace_info { $$ = &AlterTablespace{} }
| ALTER TABLESPACE change_tablespace_access { $$ = &AlterTablespace{} }
| ALTER SERVER_SYM ident_or_text OPTIONS_SYM '(' server_options_list ')' { $$ = &AlterServer{} }
| ALTER USER clear_privileges alter_user_list { $$ = &AlterUser{} }
; 

alter_user_list:
  user PASSWORD EXPIRE_SYM
| alter_user_list ',' user PASSWORD EXPIRE_SYM;

ev_alter_on_schedule_completion:
 
| ON SCHEDULE_SYM ev_schedule_time
| ev_on_completion
| ON SCHEDULE_SYM ev_schedule_time ev_on_completion;

opt_ev_rename_to:
  { $$ = nil }
| RENAME TO_SYM sp_name { $$ = $3 };

opt_ev_sql_stmt:
 
| DO_SYM ev_sql_stmt;

ident_or_empty:
  { $$ = nil } 
| ident { $$ = $1 };

alter_commands:
 
| DISCARD TABLESPACE
| IMPORT TABLESPACE
| alter_list opt_partitioning
| alter_list remove_partitioning
| remove_partitioning
| partitioning
| add_partition_rule
| DROP PARTITION_SYM alt_part_name_list
| REBUILD_SYM PARTITION_SYM opt_no_write_to_binlog all_or_alt_part_name_list
| OPTIMIZE PARTITION_SYM opt_no_write_to_binlog all_or_alt_part_name_list opt_no_write_to_binlog
| ANALYZE_SYM PARTITION_SYM opt_no_write_to_binlog all_or_alt_part_name_list
| CHECK_SYM PARTITION_SYM all_or_alt_part_name_list opt_mi_check_type
| REPAIR PARTITION_SYM opt_no_write_to_binlog all_or_alt_part_name_list opt_mi_repair_type
| COALESCE PARTITION_SYM opt_no_write_to_binlog real_ulong_num
| TRUNCATE_SYM PARTITION_SYM all_or_alt_part_name_list
| reorg_partition_rule
| EXCHANGE_SYM PARTITION_SYM alt_part_name_item WITH TABLE_SYM table_ident have_partitioning;

remove_partitioning:
  REMOVE_SYM PARTITIONING_SYM have_partitioning;

all_or_alt_part_name_list:
  ALL
| alt_part_name_list;

add_partition_rule:
  ADD PARTITION_SYM opt_no_write_to_binlog add_part_extra;

add_part_extra:
 
| '(' part_def_list ')'
| PARTITIONS_SYM real_ulong_num;

reorg_partition_rule:
  REORGANIZE_SYM PARTITION_SYM opt_no_write_to_binlog reorg_parts_rule;

reorg_parts_rule:
 
| alt_part_name_list INTO '(' part_def_list ')';

alt_part_name_list:
  alt_part_name_item
| alt_part_name_list ',' alt_part_name_item;

alt_part_name_item:
  ident;

alter_list:
  alter_list_item
| alter_list ',' alter_list_item;

add_column:
  ADD opt_column;

alter_list_item:
  add_column column_def opt_place
| ADD key_def
| add_column '(' create_field_list ')'
| CHANGE opt_column field_ident field_spec opt_place
| MODIFY_SYM opt_column field_ident type opt_attribute opt_place
| DROP opt_column field_ident opt_restrict
| DROP FOREIGN KEY_SYM field_ident
| DROP PRIMARY_SYM KEY_SYM
| DROP key_or_index field_ident
| DISABLE_SYM KEYS
| ENABLE_SYM KEYS
| ALTER opt_column field_ident SET DEFAULT signed_literal
| ALTER opt_column field_ident DROP DEFAULT
| RENAME opt_to table_ident
| CONVERT_SYM TO_SYM charset charset_name_or_default opt_collate
| create_table_options_space_separated
| FORCE_SYM
| alter_order_clause
| alter_algorithm_option
| alter_lock_option;

opt_index_lock_algorithm:
 
| alter_lock_option
| alter_algorithm_option
| alter_lock_option alter_algorithm_option
| alter_algorithm_option alter_lock_option;

alter_algorithm_option:
  ALGORITHM_SYM opt_equal DEFAULT
| ALGORITHM_SYM opt_equal ident;

alter_lock_option:
  LOCK_SYM opt_equal DEFAULT
| LOCK_SYM opt_equal ident;

opt_column:
 
| COLUMN_SYM;

opt_ignore:
 
| IGNORE_SYM;

opt_restrict:
 
| RESTRICT
| CASCADE;

opt_place:
 
| AFTER_SYM ident
| FIRST_SYM;

opt_to:
 
| TO_SYM
| EQ
| AS;

slave:
  START_SYM SLAVE opt_slave_thread_option_list slave_until slave_connection_opts { $$ = &StartSlave{} }
| STOP_SYM SLAVE opt_slave_thread_option_list { $$ = &StopSlave{} }
;

start:
  START_SYM TRANSACTION_SYM opt_start_transaction_option_list { $$ = &StartTrans{} };

opt_start_transaction_option_list:
 
| start_transaction_option_list;

start_transaction_option_list:
  start_transaction_option
| start_transaction_option_list ',' start_transaction_option;

start_transaction_option:
  WITH CONSISTENT_SYM SNAPSHOT_SYM
| READ_SYM ONLY_SYM
| READ_SYM WRITE_SYM;

slave_connection_opts:
  slave_user_name_opt slave_user_pass_opt slave_plugin_auth_opt slave_plugin_dir_opt;

slave_user_name_opt:
 
| USER EQ TEXT_STRING_sys;

slave_user_pass_opt:
 
| PASSWORD EQ TEXT_STRING_sys;

slave_plugin_auth_opt:
 
| DEFAULT_AUTH_SYM EQ TEXT_STRING_sys;

slave_plugin_dir_opt:
 
| PLUGIN_DIR_SYM EQ TEXT_STRING_sys;

opt_slave_thread_option_list:
 
| slave_thread_option_list;

slave_thread_option_list:
  slave_thread_option
| slave_thread_option_list ',' slave_thread_option;

slave_thread_option:
  SQL_THREAD
| RELAY_THREAD;

slave_until:
 
| UNTIL_SYM slave_until_opts;

slave_until_opts:
  master_file_def
| slave_until_opts ',' master_file_def
| SQL_BEFORE_GTIDS EQ TEXT_STRING_sys
| SQL_AFTER_GTIDS EQ TEXT_STRING_sys
| SQL_AFTER_MTS_GAPS;

checksum:
  CHECKSUM_SYM table_or_tables table_list opt_checksum_type { $$ = &CheckSum{Tables: $3} } ;

opt_checksum_type:
 
| QUICK
| EXTENDED_SYM;

repair:
  REPAIR opt_no_write_to_binlog table_or_tables table_list opt_mi_repair_type { $$ = &Repair{Tables: $4} };

opt_mi_repair_type:
 
| mi_repair_types;

mi_repair_types:
  mi_repair_type
| mi_repair_type mi_repair_types;

mi_repair_type:
  QUICK
| EXTENDED_SYM
| USE_FRM;

analyze:
  ANALYZE_SYM opt_no_write_to_binlog table_or_tables table_list { $$ = &Analyze{Tables: $4} };

binlog_base64_event:
  BINLOG_SYM TEXT_STRING_sys { $$ = &Binlog{} };

check:
  CHECK_SYM table_or_tables table_list opt_mi_check_type { $$ = &Check{Tables: $3} } ;

opt_mi_check_type:
 
| mi_check_types;

mi_check_types:
  mi_check_type
| mi_check_type mi_check_types;

mi_check_type:
  QUICK
| FAST_SYM
| MEDIUM_SYM
| EXTENDED_SYM
| CHANGED
| FOR_SYM UPGRADE_SYM;

optimize:
  OPTIMIZE opt_no_write_to_binlog table_or_tables table_list { $$ = &Optimize{Tables: $4} }; 

opt_no_write_to_binlog:
 
| NO_WRITE_TO_BINLOG
| LOCAL_SYM;

rename:
  RENAME table_or_tables table_to_table_list { $$ = &RenameTable{ToList: $3} }
| RENAME USER clear_privileges rename_list { $$ = &RenameUser{} }
; 

rename_list:
  user TO_SYM user
| rename_list ',' user TO_SYM user;

table_to_table_list:
  table_to_table { $$ = []*TableToTable{$1} }
| table_to_table_list ',' table_to_table { $$ = append($1, $3) };

table_to_table:
  table_ident TO_SYM table_ident { $$ = &TableToTable{From: $1, To: $3} };

keycache:
  CACHE_SYM INDEX_SYM keycache_list_or_parts IN_SYM key_cache_name 
  { $$ = &CacheIndex{TableIndexList: $3} };

keycache_list_or_parts:
  keycache_list { $$ = $1 }
| assign_to_keycache_parts { $$ = []*TableIndex{$1} }
;

keycache_list:
  assign_to_keycache { $$ = []*TableIndex{$1} }
| keycache_list ',' assign_to_keycache { $$ = append($1, $3) };

assign_to_keycache:
  table_ident cache_keys_spec { $$ = &TableIndex{Table: $1} };

assign_to_keycache_parts:
  table_ident adm_partition cache_keys_spec { $$ = &TableIndex{Table: $1} };

key_cache_name:
  ident
| DEFAULT;

preload:
  LOAD INDEX_SYM INTO CACHE_SYM preload_list_or_parts { $$ = &LoadIndex{TableIndexList: $5} }
;

preload_list_or_parts:
  preload_keys_parts { $$ = TableIndexes{$1} }
| preload_list { $$ = $1 };

preload_list:
  preload_keys { $$ = TableIndexes{$1} }
| preload_list ',' preload_keys { $$ = append($1, $3) };

preload_keys:
  table_ident cache_keys_spec opt_ignore_leaves { $$ = &TableIndex{Table: $1} };

preload_keys_parts:
  table_ident adm_partition cache_keys_spec opt_ignore_leaves {$$ = &TableIndex{Table: $1} };

adm_partition:
  PARTITION_SYM have_partitioning '(' all_or_alt_part_name_list ')';

cache_keys_spec:
  cache_key_list_or_empty;

cache_key_list_or_empty:
 
| key_or_index '(' opt_key_usage_list ')';

opt_ignore_leaves:
 
| IGNORE_SYM LEAVES;

select:
  select_init { $$ = $1 };

select_init:
  SELECT_SYM select_init2 { $$ = $2 }
| '(' select_paren ')' union_opt 
  { 
    if $4 == nil {
        $$ = &SubQuery{SelectStatement: $2} // subquery?
    } else {
        $$ = &Union{Left: $2, Right: $4}
    }
  }
;

select_paren:
  SELECT_SYM select_part2
  { $$ = $2 }
| '(' select_paren ')'
  { $$ = &SubQuery{SelectStatement : $2} } 
;

select_paren_derived:
  SELECT_SYM select_part2_derived { $$ = $2 }
| '(' select_paren_derived ')' { $$ = $2 }
;

select_init2:
  select_part2 union_clause_opt
  {
    if $2 == nil {
        $$ = $1
    } else { // we got a right-recuse union clause
        $$ = &Union{Left: $1, Right: $2}
    }
  }
;

select_part2:
  select_options select_item_list select_into select_lock_type
  { $$ = &Select {From: $3, LockType: $4} }
;

select_into:
  opt_order_clause opt_limit_clause { $$ = nil }
| into { $$ = nil }
| select_from { $$ = $1 }
| into select_from { $$ = $2 }
| select_from into { $$ = $1 }
;

select_from:
  FROM join_table_list where_clause group_clause having_clause opt_order_clause opt_limit_clause procedure_analyse_clause
  { $$ = $2 }
| FROM DUAL_SYM where_clause opt_limit_clause { $$ = nil };

select_options:
 
| select_option_list;

select_option_list:
  select_option_list select_option
| select_option;

select_option:
  query_expression_option
| SQL_NO_CACHE_SYM
| SQL_CACHE_SYM;

select_lock_type:
  { $$ = LockType_NoLock }
| FOR_SYM UPDATE_SYM { $$ = LockType_ForUpdate }
| LOCK_SYM IN_SYM SHARE_SYM MODE_SYM { $$ = LockType_LockInShareMode };

select_item_list:
  select_item_list ',' select_item
| select_item
| '*';

select_item:
  remember_name table_wild remember_end
| remember_name expr remember_end select_alias;

remember_name:
 ;

remember_end:
 ;

select_alias:
 
| AS ident
| AS TEXT_STRING_sys
| ident
| TEXT_STRING_sys;

optional_braces:
 
| '(' ')';

expr:
  expr or expr %prec OR_SYM 
  { $$ = &OrExpr{Left: $1, Right: $3} }
| expr XOR expr %prec XOR
  { $$ = &XorExpr{Left: $1, Right: $3} }
| expr and expr %prec AND_SYM
  { $$ = &AndExpr{Left: $1, Right: $3} }
| NOT_SYM expr %prec NOT_SYM
  { $$ = &NotExpr{Expr: $2} }
| bool_pri IS TRUE_SYM %prec IS
  { $$ = &IsCheck{Operator: OP_IS_TRUE, Expr: $1} }
| bool_pri IS not TRUE_SYM %prec IS
  { $$ = &IsCheck{Operator: OP_IS_NOT_TRUE, Expr: $1} }
| bool_pri IS FALSE_SYM %prec IS
  { $$ = &IsCheck{Operator: OP_IS_FALSE, Expr: $1} }
| bool_pri IS not FALSE_SYM %prec IS
  { $$ = &IsCheck{Operator: OP_IS_NOT_FALSE, Expr: $1} }
| bool_pri IS UNKNOWN_SYM %prec IS
  { $$ = &IsCheck{Operator: OP_IS_UNKNOWN, Expr: $1} }
| bool_pri IS not UNKNOWN_SYM %prec IS
  { $$ = &IsCheck{Operator: OP_IS_NOT_UNKNOWN, Expr: $1} }
| bool_pri
  { $$ = $1 }
;

bool_pri:
  bool_pri IS NULL_SYM %prec IS
  { $$ = &NullCheck{Operator: OP_IS_NULL, Expr: $1} }
| bool_pri IS not NULL_SYM %prec IS
  { $$ = &NullCheck{Operator: OP_IS_NOT_NULL, Expr: $1} }
| bool_pri EQUAL_SYM predicate %prec EQUAL_SYM
  { $$ = &CompareExpr{Left: $1, Operator: OP_EQ, Right: $3} }
| bool_pri comp_op predicate %prec EQ
  { $$ = &CompareExpr{Left: $1, Operator: $2, Right: $3} }
| bool_pri comp_op all_or_any '(' subselect ')' %prec EQ
  { $$ = &CompareExpr{Left: $1, Operator: $2, Right: $5} }
| predicate
  { $$ = &Predicate{Expr: $1} };

predicate:
  bit_expr IN_SYM '(' subselect ')'
  { $$ = &InCond{Left: $1, Operator: OP_IN, Right: IExprs{$4}} }
| bit_expr not IN_SYM '(' subselect ')'
  { $$ = &InCond{Left: $1, Operator: OP_NOT_IN, Right: IExprs{$5}} } 
| bit_expr IN_SYM '(' expr ')'
  { $$ = &InCond{Left: $1, Operator: OP_IN, Right: IExprs{$4}} }
| bit_expr IN_SYM '(' expr ',' expr_list ')'
  { $$ = &InCond{Left: $1, Operator: OP_IN, Right: append(IExprs{$4}, $6...)} }
| bit_expr not IN_SYM '(' expr ')'
  { $$ = &InCond{Left: $1, Operator: OP_NOT_IN, Right: IExprs{$5}} }
| bit_expr not IN_SYM '(' expr ',' expr_list ')'
  { $$ = &InCond{Left: $1, Operator: OP_NOT_IN, Right: append(IExprs{$5}, $7...)} }
| bit_expr BETWEEN_SYM bit_expr AND_SYM predicate
  { $$ = &RangeCond{Left: $1, Operator: OP_BETWEEN, From: $3, To: $5} }
| bit_expr not BETWEEN_SYM bit_expr AND_SYM predicate
  { $$ = &RangeCond{Left: $1, Operator: OP_NOT_BETWEEN, From: $4, To: $6} }
| bit_expr SOUNDS_SYM LIKE bit_expr
  { $$ = &LikeCond{Left: $1, Operator: OP_SOUNDS_LIKE, Right: $4} }
| bit_expr LIKE simple_expr opt_escape
  { $$ = &LikeCond{Left: $1, Operator: OP_LIKE, Right: $3} }
| bit_expr not LIKE simple_expr opt_escape
  { $$ = &LikeCond{Left: $1, Operator: OP_NOT_LIKE, Right: $4} }
| bit_expr REGEXP bit_expr
  { $$ = &LikeCond{Left: $1, Operator: OP_REGEXP, Right: $3} }
| bit_expr not REGEXP bit_expr
  { $$ = &LikeCond{Left: $1, Operator: OP_NOT_REGEXP, Right: $4} }
| bit_expr { $$ = $1 };

bit_expr:
  bit_expr '|' bit_expr %prec '|'
  { $$ = &BinaryExpr{Left: $1, Operator: OP_BITOR, Right: $3} }
| bit_expr '&' bit_expr %prec '&'
  { $$ = &BinaryExpr{Left: $1, Operator: OP_BITAND, Right: $3} }
| bit_expr SHIFT_LEFT bit_expr %prec SHIFT_LEFT
  { $$ = &BinaryExpr{Left: $1, Operator: OP_SHIFTLEFT, Right: $3} }
| bit_expr SHIFT_RIGHT bit_expr %prec SHIFT_RIGHT
  { $$ = &BinaryExpr{Left: $1, Operator: OP_SHIFTRIGHT, Right: $3} }
| bit_expr '+' bit_expr %prec '+'
  { $$ = &BinaryExpr{Left: $1, Operator: OP_PLUS, Right: $3} }
| bit_expr '-' bit_expr %prec '-'
  { $$ = &BinaryExpr{Left: $1, Operator: OP_MINUS, Right: $3} }
| bit_expr '+' INTERVAL_SYM expr interval %prec '+'
  { $$ = &BinaryExpr{Left: $1, Operator: OP_PLUS, Right: &IntervalExpr{Expr: $4, Interval: $5}} }
| bit_expr '-' INTERVAL_SYM expr interval %prec '-'
  { $$ = &BinaryExpr{Left: $1, Operator: OP_MINUS, Right: &IntervalExpr{Expr: $4, Interval: $5}} }
| bit_expr '*' bit_expr %prec '*'
  { $$ = &BinaryExpr{Left: $1, Operator: OP_MULT, Right: $3} }
| bit_expr '/' bit_expr %prec '/'
  { $$ = &BinaryExpr{Left: $1, Operator: OP_DIV, Right: $3} }
| bit_expr '%' bit_expr %prec '%'
  { $$ = &BinaryExpr{Left: $1, Operator: OP_MOD, Right: $3} }
| bit_expr DIV_SYM bit_expr %prec DIV_SYM
  { $$ = &BinaryExpr{Left: $1, Operator: OP_DIV, Right: $3} }
| bit_expr MOD_SYM bit_expr %prec MOD_SYM
  { $$ = &BinaryExpr{Left: $1, Operator: OP_MOD, Right: $3} }
| bit_expr '^' bit_expr
  { $$ = &BinaryExpr{Left: $1, Operator: OP_BITXOR, Right: $3} }
| simple_expr
  { $$ = $1 }
;

or:
  OR_SYM
| OR2_SYM;

and:
  AND_SYM
| AND_AND_SYM;

not:
  NOT_SYM
| NOT2_SYM;

not2:
  '!'
| NOT2_SYM;

comp_op:
  EQ { $$ = OP_EQ }
| GE { $$ = OP_GE }
| GT_SYM { $$ = OP_GT }
| LE { $$ = OP_LE }
| LT { $$ = OP_LT }
| NE { $$ = OP_NE };

all_or_any:
  ALL
| ANY_SYM;

simple_expr:
  simple_ident { $$ = $1 }
| function_call_keyword { $$ = &FuncExpr{} }
| function_call_nonkeyword { $$ = &FuncExpr{} }
| function_call_generic { $$ = &FuncExpr{} }
| function_call_conflict { $$ = &FuncExpr{} }
| simple_expr COLLATE_SYM ident_or_text %prec NEG
  { $$ = &CollateExpr{Expr: $1, Collate: $3} }
| literal { $$ = $1 }
| param_marker { $$ = $1 }
| variable { $$ = $1 }
| sum_expr { $$ = &FuncExpr{} }
| simple_expr OR_OR_SYM simple_expr { $$ = &OrOrExpr{Left: $1, Right: $3} }
| '+' simple_expr %prec NEG { $$ = &UnaryExpr{Expr: $2, Operator: OP_UPLUS} }
| '-' simple_expr %prec NEG { $$ = &UnaryExpr{Expr: $2, Operator: OP_UMINUS} }
| '~' simple_expr %prec NEG { $$ = &UnaryExpr{Expr: $2, Operator: OP_TILDA} }
| not2 simple_expr %prec NEG { $$ = &UnaryExpr{Expr: $2, Operator: OP_NOT2} }
| '(' subselect ')' { $$ = &SubQuery{SelectStatement: $2} }
| '(' expr ')' { $$ = &IExprs{$2} }
| '(' expr ',' expr_list ')' { $$ = append(IExprs{$2}, $4...) }
| ROW_SYM '(' expr ',' expr_list ')' { $$ = &FuncExpr{} }
| EXISTS '(' subselect ')' { $$ = &ExistsExpr{SubQuery: $3} }
| '{' ident expr '}' { $$ = &IdentExpr{Ident: $2, Expr: $3} }
| MATCH ident_list_arg AGAINST '(' bit_expr fulltext_options ')' { $$ = &MatchExpr{} }
| BINARY simple_expr %prec NEG { $$ = &UnaryExpr{Expr: $2, Operator: OP_UBINARY} }
| CAST_SYM '(' expr AS cast_type ')' { $$ = &FuncExpr{} }
| CASE_SYM opt_expr when_list opt_else END { $$ = &FuncExpr{} }
| CONVERT_SYM '(' expr ',' cast_type ')' { $$ = &FuncExpr{} }
| CONVERT_SYM '(' expr USING charset_name ')' { $$ = &FuncExpr{} }
| DEFAULT '(' simple_ident ')' { $$ = &FuncExpr{} }
| VALUES '(' simple_ident_nospvar ')' { $$ = &FuncExpr{} }
| INTERVAL_SYM expr interval '+' expr %prec INTERVAL_SYM { $$ = &IntervalExpr{Expr: &BinaryExpr{Left: $2, Right: $5, Operator: OP_PLUS}, Interval: $3} };

function_call_keyword:
  CHAR_SYM '(' expr_list ')'
| CHAR_SYM '(' expr_list USING charset_name ')'
| CURRENT_USER optional_braces
| DATE_SYM '(' expr ')'
| DAY_SYM '(' expr ')'
| HOUR_SYM '(' expr ')'
| INSERT '(' expr ',' expr ',' expr ',' expr ')'
| INTERVAL_SYM '(' expr ',' expr ')' %prec INTERVAL_SYM
| INTERVAL_SYM '(' expr ',' expr ',' expr_list ')' %prec INTERVAL_SYM
| LEFT '(' expr ',' expr ')'
| MINUTE_SYM '(' expr ')'
| MONTH_SYM '(' expr ')'
| RIGHT '(' expr ',' expr ')'
| SECOND_SYM '(' expr ')'
| TIME_SYM '(' expr ')'
| TIMESTAMP '(' expr ')'
| TIMESTAMP '(' expr ',' expr ')'
| TRIM '(' expr ')'
| TRIM '(' LEADING expr FROM expr ')'
| TRIM '(' TRAILING expr FROM expr ')'
| TRIM '(' BOTH expr FROM expr ')'
| TRIM '(' LEADING FROM expr ')'
| TRIM '(' TRAILING FROM expr ')'
| TRIM '(' BOTH FROM expr ')'
| TRIM '(' expr FROM expr ')'
| USER '(' ')'
| YEAR_SYM '(' expr ')';

function_call_nonkeyword:
  ADDDATE_SYM '(' expr ',' expr ')'
| ADDDATE_SYM '(' expr ',' INTERVAL_SYM expr interval ')'
| CURDATE optional_braces
| CURTIME func_datetime_precision
| DATE_ADD_INTERVAL '(' expr ',' INTERVAL_SYM expr interval ')' %prec INTERVAL_SYM
| DATE_SUB_INTERVAL '(' expr ',' INTERVAL_SYM expr interval ')' %prec INTERVAL_SYM
| EXTRACT_SYM '(' interval FROM expr ')'
| GET_FORMAT '(' date_time_type ',' expr ')'
| now
| POSITION_SYM '(' bit_expr IN_SYM expr ')'
| SUBDATE_SYM '(' expr ',' expr ')'
| SUBDATE_SYM '(' expr ',' INTERVAL_SYM expr interval ')'
| SUBSTRING '(' expr ',' expr ',' expr ')'
| SUBSTRING '(' expr ',' expr ')'
| SUBSTRING '(' expr FROM expr FOR_SYM expr ')'
| SUBSTRING '(' expr FROM expr ')'
| SYSDATE func_datetime_precision
| TIMESTAMP_ADD '(' interval_time_stamp ',' expr ',' expr ')'
| TIMESTAMP_DIFF '(' interval_time_stamp ',' expr ',' expr ')'
| UTC_DATE_SYM optional_braces
| UTC_TIME_SYM func_datetime_precision
| UTC_TIMESTAMP_SYM func_datetime_precision;

function_call_conflict:
  ASCII_SYM '(' expr ')'
| CHARSET '(' expr ')'
| COALESCE '(' expr_list ')'
| COLLATION_SYM '(' expr ')'
| DATABASE '(' ')'
| IF '(' expr ',' expr ',' expr ')'
| FORMAT_SYM '(' expr ',' expr ')'
| FORMAT_SYM '(' expr ',' expr ',' expr ')'
| MICROSECOND_SYM '(' expr ')'
| MOD_SYM '(' expr ',' expr ')'
| OLD_PASSWORD '(' expr ')'
| PASSWORD '(' expr ')'
| QUARTER_SYM '(' expr ')'
| REPEAT_SYM '(' expr ',' expr ')'
| REPLACE '(' expr ',' expr ',' expr ')'
| REVERSE_SYM '(' expr ')'
| ROW_COUNT_SYM '(' ')'
| TRUNCATE_SYM '(' expr ',' expr ')'
| WEEK_SYM '(' expr ')'
| WEEK_SYM '(' expr ',' expr ')'
| WEIGHT_STRING_SYM '(' expr opt_ws_levels ')'
| WEIGHT_STRING_SYM '(' expr AS CHAR_SYM ws_nweights opt_ws_levels ')'
| WEIGHT_STRING_SYM '(' expr AS BINARY ws_nweights ')'
| WEIGHT_STRING_SYM '(' expr ',' ulong_num ',' ulong_num ',' ulong_num ')'
| geometry_function;

geometry_function:
  CONTAINS_SYM '(' expr ',' expr ')'
| GEOMETRYCOLLECTION '(' expr_list ')'
| LINESTRING '(' expr_list ')'
| MULTILINESTRING '(' expr_list ')'
| MULTIPOINT '(' expr_list ')'
| MULTIPOLYGON '(' expr_list ')'
| POINT_SYM '(' expr ',' expr ')'
| POLYGON '(' expr_list ')';

function_call_generic:
  IDENT_sys '(' opt_udf_expr_list ')'
| ident '.' ident '(' opt_expr_list ')';

fulltext_options:
  opt_natural_language_mode opt_query_expansion
| IN_SYM BOOLEAN_SYM MODE_SYM;

opt_natural_language_mode:
 
| IN_SYM NATURAL LANGUAGE_SYM MODE_SYM;

opt_query_expansion:
 
| WITH QUERY_SYM EXPANSION_SYM;

opt_udf_expr_list:
 
| udf_expr_list;

udf_expr_list:
  udf_expr
| udf_expr_list ',' udf_expr;

udf_expr:
  remember_name expr remember_end select_alias;

sum_expr:
  AVG_SYM '(' in_sum_expr ')' {}
| AVG_SYM '(' DISTINCT in_sum_expr ')'
| BIT_AND '(' in_sum_expr ')'
| BIT_OR '(' in_sum_expr ')'
| BIT_XOR '(' in_sum_expr ')'
| COUNT_SYM '(' opt_all '*' ')'
| COUNT_SYM '(' in_sum_expr ')'
| COUNT_SYM '(' DISTINCT expr_list ')'
| MIN_SYM '(' in_sum_expr ')'
| MIN_SYM '(' DISTINCT in_sum_expr ')'
| MAX_SYM '(' in_sum_expr ')'
| MAX_SYM '(' DISTINCT in_sum_expr ')'
| STD_SYM '(' in_sum_expr ')'
| VARIANCE_SYM '(' in_sum_expr ')'
| STDDEV_SAMP_SYM '(' in_sum_expr ')'
| VAR_SAMP_SYM '(' in_sum_expr ')'
| SUM_SYM '(' in_sum_expr ')'
| SUM_SYM '(' DISTINCT in_sum_expr ')'
| GROUP_CONCAT_SYM '(' opt_distinct expr_list opt_gorder_clause opt_gconcat_separator ')';

variable:
  '@' variable_aux { $$ = &Variable{} };

variable_aux:
  ident_or_text SET_VAR expr
| ident_or_text
| '@' opt_var_ident_type ident_or_text opt_component;

opt_distinct:
 
| DISTINCT;

opt_gconcat_separator:
 
| SEPARATOR_SYM text_string;

opt_gorder_clause:
 
| ORDER_SYM BY gorder_list;

gorder_list:
  gorder_list ',' order_ident order_dir
| order_ident order_dir;

in_sum_expr:
  opt_all expr;

cast_type:
  BINARY opt_field_length
| CHAR_SYM opt_field_length opt_binary
| NCHAR_SYM opt_field_length
| SIGNED_SYM
| SIGNED_SYM INT_SYM
| UNSIGNED
| UNSIGNED INT_SYM
| DATE_SYM
| TIME_SYM type_datetime_precision
| DATETIME type_datetime_precision
| DECIMAL_SYM float_options;

opt_expr_list:
 
| expr_list;

expr_list:
  expr { $$ = IExprs{$1} }
| expr_list ',' expr { $$ = append($1, $3) };

ident_list_arg:
  ident_list
| '(' ident_list ')';

ident_list:
  simple_ident
| ident_list ',' simple_ident;

opt_expr:
 
| expr;

opt_else:
 
| ELSE expr;

when_list:
  WHEN_SYM expr THEN_SYM expr
| when_list WHEN_SYM expr THEN_SYM expr;

table_ref:
  table_factor { $$ = $1 }
| join_table { $$ = $1 };

join_table_list:
  derived_table_list { $$ = $1 };

esc_table_ref:
  table_ref { $$ = $1 }
| '{' ident table_ref '}' { $$ = $3 };

derived_table_list:
  esc_table_ref { $$ = []ITable{$1} }
| derived_table_list ',' esc_table_ref { $$ = append($1, $3) };

join_table:
  table_ref normal_join table_ref %prec TABLE_REF_PRIORITY 
  { $$ = &JoinTable{Left: $1, Right: $3} }
| table_ref STRAIGHT_JOIN table_factor
  { $$ = &JoinTable{Left: $1, Right: $3} }
| table_ref normal_join table_ref ON expr
  { $$ = &JoinTable{Left: $1, Right: $3} }
| table_ref STRAIGHT_JOIN table_factor ON expr
  { $$ = &JoinTable{Left: $1, Right: $3} }
| table_ref normal_join table_ref USING '(' using_list ')'
  { $$ = &JoinTable{Left: $1, Right: $3} }
| table_ref NATURAL JOIN_SYM table_factor
  { $$ = &JoinTable{Left: $1, Right: $4} }
| table_ref LEFT opt_outer JOIN_SYM table_ref ON expr
  { $$ = &JoinTable{Left: $1, Right: $5} }
| table_ref LEFT opt_outer JOIN_SYM table_factor USING '(' using_list ')'
  { $$ = &JoinTable{Left: $1, Right: $5} }
| table_ref NATURAL LEFT opt_outer JOIN_SYM table_factor
  { $$ = &JoinTable{Left: $1, Right: $6} }
| table_ref RIGHT opt_outer JOIN_SYM table_ref ON expr
  { $$ = &JoinTable{Left: $1, Right: $5} }
| table_ref RIGHT opt_outer JOIN_SYM table_factor USING '(' using_list ')'
  { $$ = &JoinTable{Left: $1, Right: $5} }
| table_ref NATURAL RIGHT opt_outer JOIN_SYM table_factor
  { $$ = &JoinTable{Left: $1, Right: $6} }
;

normal_join:
  JOIN_SYM
| INNER_SYM JOIN_SYM
| CROSS JOIN_SYM;

opt_use_partition:
 
| use_partition;

use_partition:
  PARTITION_SYM '(' using_list ')' have_partitioning;

table_factor:
  table_ident opt_use_partition opt_table_alias opt_key_definition
  { $$ = &AliasedTable{TableOrSubQuery: $1, As: $3} }
| select_derived_init get_select_lex select_derived2
  { $$ = &AliasedTable{TableOrSubQuery: $3} }
| '(' get_select_lex select_derived_union ')' opt_table_alias
  { $$ = &AliasedTable{TableOrSubQuery: $3, As: $5} }
;

select_derived_union:
  select_derived opt_union_order_or_limit 
  { $$ = &SubQuery{SelectStatement:$1} }
| select_derived_union UNION_SYM union_option query_specification opt_union_order_or_limit { $$ = &SubQuery{SelectStatement: &Union{Left:$1, Right: $4}} }
;

select_init2_derived:
  select_part2_derived { $$ = $1 };

select_part2_derived:
  opt_query_expression_options select_item_list opt_select_from select_lock_type
  {
    if $3 == nil {
        $$ = &Select{From: nil, LockType: $4}
    } else {
        $$ = &Select{From: $3.(*Select).From, LockType: $4}
    }
  }
;

select_derived:
  get_select_lex derived_table_list { $$ = &Select{From: $2} };

select_derived2:
  select_options select_item_list opt_select_from
  { $$ = &SubQuery{SelectStatement: $3} }
;

get_select_lex:
 ;

select_derived_init:
  SELECT_SYM;

opt_outer:
 
| OUTER;

index_hint_clause:
 
| FOR_SYM JOIN_SYM
| FOR_SYM ORDER_SYM BY
| FOR_SYM GROUP_SYM BY;

index_hint_type:
  FORCE_SYM
| IGNORE_SYM;

index_hint_definition:
  index_hint_type key_or_index index_hint_clause '(' key_usage_list ')'
| USE_SYM key_or_index index_hint_clause '(' opt_key_usage_list ')';

index_hints_list:
  index_hint_definition
| index_hints_list index_hint_definition;

opt_index_hints_list:
 
| index_hints_list;

opt_key_definition:
  opt_index_hints_list;

opt_key_usage_list:
 
| key_usage_list;

key_usage_element:
  ident
| PRIMARY_SYM;

key_usage_list:
  key_usage_element
| key_usage_list ',' key_usage_element;

using_list:
  ident
| using_list ',' ident;

interval:
  interval_time_stamp { $$ = $1 }
| DAY_HOUR_SYM { $$ = $1 }
| DAY_MICROSECOND_SYM { $$ = $1 }
| DAY_MINUTE_SYM { $$ = $1 }
| DAY_SECOND_SYM { $$ = $1 }
| HOUR_MICROSECOND_SYM { $$ = $1 }
| HOUR_MINUTE_SYM { $$ = $1 }
| HOUR_SECOND_SYM { $$ = $1 }
| MINUTE_MICROSECOND_SYM { $$ = $1 }
| MINUTE_SECOND_SYM { $$ = $1 }
| SECOND_MICROSECOND_SYM { $$ = $1 }
| YEAR_MONTH_SYM { $$ = $1 }
;

interval_time_stamp:
  DAY_SYM { $$ = $1 }
| WEEK_SYM { $$ = $1 }
| HOUR_SYM { $$ = $1 }
| MINUTE_SYM { $$ = $1 }
| MONTH_SYM { $$ = $1 }
| QUARTER_SYM { $$ = $1 }
| SECOND_SYM { $$ = $1 }
| MICROSECOND_SYM { $$ = $1 }
| YEAR_SYM { $$ = $1 }
;

date_time_type:
  DATE_SYM
| TIME_SYM
| TIMESTAMP
| DATETIME;

table_alias:
 
| AS
| EQ;

opt_table_alias:
  { $$ = nil } 
| table_alias ident 
  { $$ = $2 }
;

opt_all:
 
| ALL;

where_clause:
 
| WHERE expr;

having_clause:
 
| HAVING expr;

opt_escape:
  ESCAPE_SYM simple_expr
|;

group_clause:
 
| GROUP_SYM BY group_list olap_opt;

group_list:
  group_list ',' order_ident order_dir
| order_ident order_dir;

olap_opt:
 
| WITH_CUBE_SYM
| WITH_ROLLUP_SYM;

alter_order_clause:
  ORDER_SYM BY alter_order_list;

alter_order_list:
  alter_order_list ',' alter_order_item
| alter_order_item;

alter_order_item:
  simple_ident_nospvar order_dir;

opt_order_clause:
 
| order_clause;

order_clause:
  ORDER_SYM BY order_list;

order_list:
  order_list ',' order_ident order_dir
| order_ident order_dir;

order_dir:
 
| ASC
| DESC;

opt_limit_clause_init:
 
| limit_clause;

opt_limit_clause:
 
| limit_clause;

limit_clause:
  LIMIT limit_options;

limit_options:
  limit_option
| limit_option ',' limit_option
| limit_option OFFSET_SYM limit_option;

limit_option:
  ident
| param_marker
| ULONGLONG_NUM
| LONG_NUM
| NUM;

delete_limit_clause:
 
| LIMIT limit_option;

ulong_num:
  NUM
| HEX_NUM
| LONG_NUM
| ULONGLONG_NUM
| DECIMAL_NUM
| FLOAT_NUM;

real_ulong_num:
  NUM
| HEX_NUM
| LONG_NUM
| ULONGLONG_NUM
| dec_num_error;

ulonglong_num:
  NUM
| ULONGLONG_NUM
| LONG_NUM
| DECIMAL_NUM
| FLOAT_NUM;

real_ulonglong_num:
  NUM
| ULONGLONG_NUM
| LONG_NUM
| dec_num_error;

dec_num_error:
  dec_num;

dec_num:
  DECIMAL_NUM
| FLOAT_NUM;

procedure_analyse_clause:
 
| PROCEDURE_SYM ANALYSE_SYM '(' opt_procedure_analyse_params ')';

opt_procedure_analyse_params:
 
| procedure_analyse_param
| procedure_analyse_param ',' procedure_analyse_param;

procedure_analyse_param:
  NUM;

select_var_list_init:
  select_var_list;

select_var_list:
  select_var_list ',' select_var_ident
| select_var_ident;

select_var_ident:
  '@' ident_or_text
| ident_or_text;

into:
  INTO into_destination;

into_destination:
  OUTFILE TEXT_STRING_filesystem opt_load_data_charset opt_field_term opt_line_term
| DUMPFILE TEXT_STRING_filesystem
| select_var_list_init;

do:
  DO_SYM expr_list { $$ = &Do{} };

drop:
  DROP opt_temporary table_or_tables if_exists table_list opt_restrict { $$ = &DropTables{Tables: $5} }
| DROP INDEX_SYM ident ON table_ident opt_index_lock_algorithm { $$ = &DropIndex{On: $5} }
| DROP DATABASE if_exists ident { $$ = &DropDatabase{} }
| DROP FUNCTION_SYM if_exists ident '.' ident 
  { $$ = &DropFunction{Function: &Spname{Qualifier: $4 , Name: $6}} }
| DROP FUNCTION_SYM if_exists ident
  { $$ = &DropFunction{Function: &Spname{Name: $4}} }
| DROP PROCEDURE_SYM if_exists sp_name { $$ = &DropProcedure{Procedure: $4} }
| DROP USER clear_privileges user_list { $$ = &DropUser{} }
| DROP VIEW_SYM if_exists table_list opt_restrict { $$ = &DropView{} }
| DROP EVENT_SYM if_exists sp_name { $$ = &DropEvent{Event: $4} }
| DROP TRIGGER_SYM if_exists sp_name { $$ = &DropTrigger{Trigger: $4} }
| DROP TABLESPACE tablespace_name drop_ts_options_list { $$ = &DropTablespace{} }
| DROP LOGFILE_SYM GROUP_SYM logfile_group_name drop_ts_options_list { $$ = &DropLogfile{} }
| DROP SERVER_SYM if_exists ident_or_text { $$ = &DropServer{} }
;

table_list:
  table_name { $$ = ISimpleTables{$1} }
| table_list ',' table_name { $$ = append($1, $3) };

table_name:
  table_ident { $$ = $1 };

table_name_with_opt_use_partition:
  table_ident opt_use_partition { $$ = $1 };

table_alias_ref_list:
  table_alias_ref 
  { $$ = ITables{$1} }
| table_alias_ref_list ',' table_alias_ref 
  { $$ = append($1, $3) }
;

table_alias_ref:
  table_ident_opt_wild { $$ = $1 };

if_exists:
 
| IF EXISTS;

opt_temporary:
 
| TEMPORARY;

drop_ts_options_list:
 
| drop_ts_options;

drop_ts_options:
  drop_ts_option
| drop_ts_options drop_ts_option
| drop_ts_options_list ',' drop_ts_option;

drop_ts_option:
  opt_ts_engine
| ts_wait;

insert:
  INSERT insert_lock_option opt_ignore into_table insert_field_spec opt_insert_update
  {
    $$ = &Insert{Table: $4, InsertFields: $5}
  }
;

replace:
  REPLACE replace_lock_option into_table insert_field_spec 
  { 
    $$ = &Replace{Table: $3, ReplaceFields: $4}
  }
;

insert_lock_option:
 
| LOW_PRIORITY
| DELAYED_SYM
| HIGH_PRIORITY;

replace_lock_option:
  opt_low_priority
| DELAYED_SYM;

into_table:
  INTO insert_table { $$ = $2 }
| insert_table { $$ = $1 };

insert_table:
  table_name_with_opt_use_partition { $$ = $1 };

insert_field_spec:
  insert_values { $$ = $1 }
| '(' ')' insert_values { $$ = $3 }
| '(' fields ')' insert_values { $$ = $4 }
| SET ident_eq_list { $$ = struct{}{} };

fields:
  fields ',' insert_ident
| insert_ident;

insert_values:
  VALUES values_list { $$ = struct{}{} }
| VALUE_SYM values_list { $$ = struct{}{} }
| create_select union_clause_opt
  {
    if $2 == nil {
        $$ = $1
    } else {
        $$ = &Union{Left: $1, Right: $2}
    }
  }
| '(' create_select ')' union_opt
  {
    if $4 == nil {
        $$ = $2
    } else {
        $$ = &Union{Left: $2, Right: $4}
    }
  }
;

values_list:
  values_list ',' no_braces
| no_braces;

ident_eq_list:
  ident_eq_list ',' ident_eq_value
| ident_eq_value;

ident_eq_value:
  simple_ident_nospvar equal expr_or_default;

equal:
  EQ
| SET_VAR;

opt_equal:
 
| equal;

no_braces:
  '(' opt_values ')';

opt_values:
 
| values;

values:
  values ',' expr_or_default
| expr_or_default;

expr_or_default:
  expr
| DEFAULT;

opt_insert_update:
 
| ON DUPLICATE_SYM KEY_SYM UPDATE_SYM insert_update_list;

update:
  UPDATE_SYM opt_low_priority opt_ignore join_table_list SET update_list where_clause opt_order_clause delete_limit_clause 
  { 
    $$ = &Update{Tables: $4}
  }
;

update_list:
  update_list ',' update_elem
| update_elem;

update_elem:
  simple_ident_nospvar equal expr_or_default;

insert_update_list:
  insert_update_list ',' insert_update_elem
| insert_update_elem;

insert_update_elem:
  simple_ident_nospvar equal expr_or_default;

opt_low_priority:
 
| LOW_PRIORITY;

delete:
  DELETE_SYM opt_delete_options single_multi { $$ = $3 }
;

single_multi:
  FROM table_ident opt_use_partition where_clause opt_order_clause delete_limit_clause 
  { $$ = &Delete{Tables: ITables{$2}} }
| table_wild_list FROM join_table_list where_clause 
  { $$ = &Delete{Tables: append($1, $3...)} }
| FROM table_alias_ref_list USING join_table_list where_clause
  { $$ = &Delete{Tables: append($2, $4...)} }
;

table_wild_list:
  table_wild_one { $$ = ITables{$1} }
| table_wild_list ',' table_wild_one { $$ = append($1, $3) }
;

table_wild_one:
  ident opt_wild { $$ = &SimpleTable{Name: $1, Column: $2} }
| ident '.' ident opt_wild { $$ = &SimpleTable{Qualifier: $1, Name: $3, Column: $4} }
;

opt_wild:
  { $$ = nil } 
| '.' '*' { $$ = []byte{'*'} };

opt_delete_options:
 
| opt_delete_option opt_delete_options;

opt_delete_option:
  QUICK
| LOW_PRIORITY
| IGNORE_SYM;

truncate:
  TRUNCATE_SYM opt_table_sym table_name { $$ = &TruncateTable{Table: $3} }
;

opt_table_sym:
 
| TABLE_SYM;

opt_profile_defs:
 
| profile_defs;

profile_defs:
  profile_def
| profile_defs ',' profile_def;

profile_def:
  CPU_SYM
| MEMORY_SYM
| BLOCK_SYM IO_SYM
| CONTEXT_SYM SWITCHES_SYM
| PAGE_SYM FAULTS_SYM
| IPC_SYM
| SWAPS_SYM
| SOURCE_SYM
| ALL;

opt_profile_args:
 
| FOR_SYM QUERY_SYM NUM;

show:
  SHOW show_param { $$ = $2 };

show_param:
  DATABASES wild_and_where { $$ = &ShowDatabases{LikeOrWhere: $2} }
| opt_full TABLES opt_db wild_and_where { $$ = &ShowTables{From: $3} }
| opt_full TRIGGERS_SYM opt_db wild_and_where { $$ = &ShowTriggers{From: $3} }
| EVENTS_SYM opt_db wild_and_where { $$ = &ShowEvents{From: $2} }
| TABLE_SYM STATUS_SYM opt_db wild_and_where { $$ = &ShowTableStatus{From: $3} }
| OPEN_SYM TABLES opt_db wild_and_where { $$ = &ShowOpenTables{From: $3} }
| PLUGINS_SYM { $$ = &ShowPlugins{} }
| ENGINE_SYM known_storage_engines show_engine_param { $$ = &ShowEngines{} }
| ENGINE_SYM ALL show_engine_param { $$ = &ShowEngines{} }
| opt_full COLUMNS from_or_in table_ident opt_db wild_and_where { $$ = &ShowColumns{Table: $4, From: $5} }
| master_or_binary LOGS_SYM { $$ = &ShowLogs{} }
| SLAVE HOSTS_SYM { $$ = &ShowSlaveHosts{} } 
| BINLOG_SYM EVENTS_SYM binlog_in binlog_from opt_limit_clause_init { $$ = &ShowLogEvents{} } 
| RELAYLOG_SYM EVENTS_SYM binlog_in binlog_from opt_limit_clause_init { $$ = &ShowLogEvents{} } 
| keys_or_index from_or_in table_ident opt_db where_clause 
  { $$ = &ShowIndex{Table: $3, From: $4} }
| opt_storage ENGINES_SYM { $$ = &ShowEngines{} }
| PRIVILEGES { $$ = &ShowPrivileges{} }
| COUNT_SYM '(' '*' ')' WARNINGS { $$ = &ShowWarnings{} }
| COUNT_SYM '(' '*' ')' ERRORS { $$ = &ShowErrors{} }
| WARNINGS opt_limit_clause_init { $$ = &ShowWarnings{} }
| ERRORS opt_limit_clause_init { $$ = &ShowErrors{} }
| PROFILES_SYM { $$ = &ShowProfiles{} }
| PROFILE_SYM opt_profile_defs opt_profile_args opt_limit_clause_init 
  { $$ = &ShowProfiles{} }
| opt_var_type STATUS_SYM wild_and_where { $$ = &ShowStatus{} }
| opt_full PROCESSLIST_SYM { $$ = &ShowProcessList{} }
| opt_var_type VARIABLES wild_and_where { $$ = &ShowVariables{} }
| charset wild_and_where { $$ = &ShowCharset{} }
| COLLATION_SYM wild_and_where { $$ = &ShowCollation{} }
| GRANTS { $$ = &ShowGrants{} }
| GRANTS FOR_SYM user { $$ = &ShowGrants{} }
| CREATE DATABASE opt_if_not_exists ident 
  { $$ = &ShowCreateDatabase{Schema: $4} }
| CREATE TABLE_SYM table_ident 
  { $$ = &ShowCreate{Prefix: $2, Table: $3} }
| CREATE VIEW_SYM table_ident 
  { $$ = &ShowCreate{Prefix: $2, Table: $3} }
| MASTER_SYM STATUS_SYM { $$ = &ShowMasterStatus{} } 
| SLAVE STATUS_SYM { $$ = &ShowSlaveStatus{} }
| CREATE PROCEDURE_SYM sp_name 
  { $$ = &ShowCreate{Prefix: $2, Table: $3} }
| CREATE FUNCTION_SYM sp_name 
  { $$ = &ShowCreate{Prefix: $2, Table: $3} }
| CREATE TRIGGER_SYM sp_name 
  { $$ = &ShowCreate{Prefix: $2, Table: $3} }
| PROCEDURE_SYM STATUS_SYM wild_and_where { $$ = &ShowProcedure{} }
| FUNCTION_SYM STATUS_SYM wild_and_where { $$ = &ShowFunction{} }
| PROCEDURE_SYM CODE_SYM sp_name { $$ = &ShowProcedure{Procedure: $3} }
| FUNCTION_SYM CODE_SYM sp_name { $$ = &ShowFunction{Function: $3} }
| CREATE EVENT_SYM sp_name 
  { $$ = &ShowCreate{Prefix: $2, Table: $3} }
; 

show_engine_param:
  STATUS_SYM
| MUTEX_SYM
| LOGS_SYM;

master_or_binary:
  MASTER_SYM
| BINARY;

opt_storage:
 
| STORAGE_SYM;

opt_db:
  { $$ = nil } 
| from_or_in ident { $$ = $2 };

opt_full:
 
| FULL;

from_or_in:
  FROM
| IN_SYM;

binlog_in:
 
| IN_SYM TEXT_STRING_sys;

binlog_from:
 
| FROM ulonglong_num;

wild_and_where:
  { $$ = nil } 
| LIKE TEXT_STRING_sys { $$ = &LikeOrWhere{Like: string($2)} }
| WHERE expr { $$ = nil };

describe:
  describe_command table_ident opt_describe_column 
  { $$ = &DescribeTable{Table: $2} }
| describe_command opt_extended_describe explanable_command 
  { $$ = &DescribeStmt{Stmt: $3} }
;

explanable_command:
  select { $$ = $1 }
| insert { $$ = $1 }
| replace { $$ = $1 }
| update { $$ = $1 }
| delete { $$ = $1 };

describe_command:
  DESC
| DESCRIBE;

opt_extended_describe:
 
| EXTENDED_SYM
| PARTITIONS_SYM
| FORMAT_SYM EQ ident_or_text;

opt_describe_column:
 
| text_string
| ident;

flush:
  FLUSH_SYM opt_no_write_to_binlog flush_options { $$ = $3 };

flush_options:
  table_or_tables opt_table_list opt_flush_lock { $$ = &FlushTables{Tables: $2} }
| flush_options_list { $$ = &Flush{} };

opt_flush_lock:
 
| WITH READ_SYM LOCK_SYM
| FOR_SYM EXPORT_SYM;

flush_options_list:
  flush_options_list ',' flush_option
| flush_option;

flush_option:
  ERROR_SYM LOGS_SYM
| ENGINE_SYM LOGS_SYM
| GENERAL LOGS_SYM
| SLOW LOGS_SYM
| BINARY LOGS_SYM
| RELAY LOGS_SYM
| QUERY_SYM CACHE_SYM
| HOSTS_SYM
| PRIVILEGES
| LOGS_SYM
| STATUS_SYM
| DES_KEY_FILE
| RESOURCES;

opt_table_list:
  { $$ = nil } 
| table_list { $$ = $1 };

reset:
  RESET_SYM reset_options { $$ = &Reset{} };

reset_options:
  reset_options ',' reset_option
| reset_option;

reset_option:
  SLAVE slave_reset_options
| MASTER_SYM
| QUERY_SYM CACHE_SYM;

slave_reset_options:
 
| ALL;

purge:
  PURGE purge_options { $$ = &Purge{} };

purge_options:
  master_or_binary LOGS_SYM purge_option;

purge_option:
  TO_SYM TEXT_STRING_sys
| BEFORE_SYM expr;

kill:
  KILL_SYM kill_option expr { $$ = &Kill{} };

kill_option:
 
| CONNECTION_SYM
| QUERY_SYM;

use:
  USE_SYM ident { $$ = &Use{DB: $2} };

load:
  LOAD data_or_xml load_data_lock opt_local INFILE TEXT_STRING_filesystem opt_duplicate INTO TABLE_SYM table_ident opt_use_partition opt_load_data_charset opt_xml_rows_identified_by opt_field_term opt_line_term opt_ignore_lines opt_field_or_var_spec opt_load_data_set_spec
  {
    $$ = &Load{}
  }  
;

data_or_xml:
  DATA_SYM
| XML_SYM;

opt_local:
 
| LOCAL_SYM;

load_data_lock:
 
| CONCURRENT
| LOW_PRIORITY;

opt_duplicate:
 
| REPLACE
| IGNORE_SYM;

opt_field_term:
 
| COLUMNS field_term_list;

field_term_list:
  field_term_list field_term
| field_term;

field_term:
  TERMINATED BY text_string
| OPTIONALLY ENCLOSED BY text_string
| ENCLOSED BY text_string
| ESCAPED BY text_string;

opt_line_term:
 
| LINES line_term_list;

line_term_list:
  line_term_list line_term
| line_term;

line_term:
  TERMINATED BY text_string
| STARTING BY text_string;

opt_xml_rows_identified_by:
 
| ROWS_SYM IDENTIFIED_SYM BY text_string;

opt_ignore_lines:
 
| IGNORE_SYM NUM lines_or_rows;

lines_or_rows:
  LINES
| ROWS_SYM;

opt_field_or_var_spec:
 
| '(' fields_or_vars ')'
| '(' ')';

fields_or_vars:
  fields_or_vars ',' field_or_var
| field_or_var;

field_or_var:
  simple_ident_nospvar
| '@' ident_or_text;

opt_load_data_set_spec:
 
| SET load_data_set_list;

load_data_set_list:
  load_data_set_list ',' load_data_set_elem
| load_data_set_elem;

load_data_set_elem:
  simple_ident_nospvar equal remember_name expr_or_default remember_end;

text_literal:
  TEXT_STRING { $$ = StrVal($1) }
| NCHAR_STRING { $$ = StrVal($1) }
| UNDERSCORE_CHARSET TEXT_STRING { $$ = StrVal(append(append($1, ' '), $2...)) }
| text_literal TEXT_STRING_literal { $$ = StrVal(append($1.(StrVal), $2...)) };

text_string:
  TEXT_STRING_literal
| HEX_NUM
| BIN_NUM;

param_marker:
  PARAM_MARKER { $$ = StrVal("?") };

signed_literal:
  literal
| '+' NUM_literal
| '-' NUM_literal;

literal:
  text_literal { $$ = $1 }
| NUM_literal { $$ = $1 }
| temporal_literal { $$ = $1 }
| NULL_SYM { $$ = &NullVal{} }
| FALSE_SYM { $$ = BoolVal(false) }
| TRUE_SYM { $$ = BoolVal(true) }
| HEX_NUM { $$ = HexVal($1) }
| BIN_NUM { $$ = BinVal($1) }
| UNDERSCORE_CHARSET HEX_NUM { $$ = HexVal(append(append($1, ' '), $2...)) }
| UNDERSCORE_CHARSET BIN_NUM { $$ = BinVal(append(append($1, ' '), $2...)) }
;

NUM_literal:
  NUM { $$ = NumVal($1) }
| LONG_NUM { $$ = NumVal($1) }
| ULONGLONG_NUM { $$ = NumVal($1) }
| DECIMAL_NUM { $$ = NumVal($1) }
| FLOAT_NUM { $$ = NumVal($1) };

temporal_literal:
  DATE_SYM TEXT_STRING { $$ = StrVal(append($1, $2...)) }
| TIME_SYM TEXT_STRING { $$ = StrVal(append($1, $2...)) }
| TIMESTAMP TEXT_STRING { $$ = StrVal(append($1, $2...)) }
;

insert_ident:
  simple_ident_nospvar
| table_wild;

table_wild:
  ident '.' '*'
| ident '.' ident '.' '*';

order_ident:
  expr;

simple_ident:
  ident { $$ = &SchemaObject{Column: $1} }
| simple_ident_q { $$ = $1 };

simple_ident_nospvar:
  ident
| simple_ident_q;

simple_ident_q:
  ident '.' ident { $$ = &SchemaObject{Table: $1, Column: $3} }
| '.' ident '.' ident { $$ = &SchemaObject{Table: $2, Column: $4} }
| ident '.' ident '.' ident { $$ = &SchemaObject{Schema: $1, Table: $3, Column: $5} };

field_ident:
  ident
| ident '.' ident '.' ident
| ident '.' ident
| '.' ident;

table_ident:
  ident { $$ = &SimpleTable{Name: $1} }
| ident '.' ident { $$ = &SimpleTable{Qualifier: $1, Name: $3} }
| '.' ident { $$ = &SimpleTable{Name: $2} } ;

table_ident_opt_wild:
  ident opt_wild { $$ = &SimpleTable{Name: $1, Column: $2} }
| ident '.' ident opt_wild { $$ = &SimpleTable{Qualifier: $1, Name: $3, Column: $4} }
;

table_ident_nodb:
  ident { $$ = &SimpleTable{Name: $1} };

IDENT_sys:
  IDENT { $$ = $1 }
| IDENT_QUOTED { $$ = $1 };

TEXT_STRING_sys_nonewline:
  TEXT_STRING_sys;

TEXT_STRING_sys:
  TEXT_STRING { $$ = $1} ;

TEXT_STRING_literal:
  TEXT_STRING;

TEXT_STRING_filesystem:
  TEXT_STRING;

ident:
  IDENT_sys { $$ = $1 }
| keyword { $$ = $1 };

label_ident:
  IDENT_sys
| keyword_sp;

ident_or_text:
  ident { $$ = $1 }
| TEXT_STRING_sys { $$ = $1 }
| LEX_HOSTNAME { $$ = $1 };

user:
  ident_or_text
| ident_or_text '@' ident_or_text
| CURRENT_USER optional_braces;

keyword:
  keyword_sp { $$ = $1 }
| ASCII_SYM { $$ = $1 }
| BACKUP_SYM { $$ = $1 }
| BEGIN_SYM { $$ = $1 }
| BYTE_SYM { $$ = $1 }
| CACHE_SYM { $$ = $1 }
| CHARSET { $$ = $1 }
| CHECKSUM_SYM { $$ = $1 }
| CLOSE_SYM { $$ = $1 }
| COMMENT_SYM { $$ = $1 }
| COMMIT_SYM { $$ = $1 }
| CONTAINS_SYM { $$ = $1 }
| DEALLOCATE_SYM { $$ = $1 }
| DO_SYM { $$ = $1 }
| END { $$ = $1 }
| EXECUTE_SYM { $$ = $1 }
| FLUSH_SYM { $$ = $1 }
| FORMAT_SYM { $$ = $1 }
| HANDLER_SYM { $$ = $1 }
| HELP_SYM { $$ = $1 }
| HOST_SYM { $$ = $1 }
| INSTALL_SYM { $$ = $1 }
| LANGUAGE_SYM { $$ = $1 }
| NO_SYM { $$ = $1 }
| OPEN_SYM { $$ = $1 }
| OPTIONS_SYM { $$ = $1 }
| OWNER_SYM { $$ = $1 }
| PARSER_SYM { $$ = $1 } 
| PORT_SYM { $$ = $1 }
| PREPARE_SYM { $$ = $1 }
| REMOVE_SYM { $$ = $1 }
| REPAIR { $$ = $1 }
| RESET_SYM { $$ = $1 }
| RESTORE_SYM { $$ = $1 }
| ROLLBACK_SYM { $$ = $1 }
| SAVEPOINT_SYM { $$ = $1 }
| SECURITY_SYM { $$ = $1 }
| SERVER_SYM { $$ = $1 }
| SIGNED_SYM { $$ = $1 }
| SOCKET_SYM { $$ = $1 }
| SLAVE { $$ = $1 }
| SONAME_SYM { $$ = $1 }
| START_SYM { $$ = $1 }
| STOP_SYM { $$ = $1 }
| TRUNCATE_SYM { $$ = $1 }
| UNICODE_SYM { $$ = $1 }
| UNINSTALL_SYM { $$ = $1 }
| WRAPPER_SYM { $$ = $1 }
| XA_SYM { $$ = $1 }
| UPGRADE_SYM { $$ = $1 }
;

keyword_sp:
  ACTION { $$ = $1 }
| ADDDATE_SYM { $$ = $1 }
| AFTER_SYM { $$ = $1 }
| AGAINST { $$ = $1 }
| AGGREGATE_SYM { $$ = $1 }
| ALGORITHM_SYM { $$ = $1 }
| ANALYSE_SYM { $$ = $1 }
| ANY_SYM { $$ = $1 }
| AT_SYM { $$ = $1 } 
| AUTO_INC { $$ = $1 }
| AUTOEXTEND_SIZE_SYM { $$ = $1 }
| AVG_ROW_LENGTH { $$ = $1 }
| AVG_SYM { $$ = $1 }
| BINLOG_SYM { $$ = $1 }
| BIT_SYM { $$ = $1 }
| BLOCK_SYM { $$ = $1 }
| BOOL_SYM { $$ = $1 }
| BOOLEAN_SYM { $$ = $1 }
| BTREE_SYM { $$ = $1 }
| CASCADED { $$ = $1 }
| CATALOG_NAME_SYM { $$ = $1 }
| CHAIN_SYM { $$ = $1 }
| CHANGED { $$ = $1 }
| CIPHER_SYM { $$ = $1 }
| CLIENT_SYM { $$ = $1 }
| CLASS_ORIGIN_SYM { $$ = $1 }
| COALESCE { $$ = $1 }
| CODE_SYM { $$ = $1 }
| COLLATION_SYM { $$ = $1 }
| COLUMN_NAME_SYM { $$ = $1 }
| COLUMN_FORMAT_SYM { $$ = $1 }
| COLUMNS { $$ = $1 }
| COMMITTED_SYM { $$ = $1 }
| COMPACT_SYM { $$ = $1 }
| COMPLETION_SYM { $$ = $1 }
| COMPRESSED_SYM { $$ = $1 }
| CONCURRENT { $$ = $1 }
| CONNECTION_SYM { $$ = $1 }
| CONSISTENT_SYM { $$ = $1 }
| CONSTRAINT_CATALOG_SYM { $$ = $1 }
| CONSTRAINT_SCHEMA_SYM { $$ = $1 }
| CONSTRAINT_NAME_SYM { $$ = $1 }
| CONTEXT_SYM { $$ = $1 }
| CPU_SYM { $$ = $1 }
| CUBE_SYM { $$ = $1 }
| CURRENT_SYM { $$ = $1 }
| CURSOR_NAME_SYM { $$ = $1 }
| DATA_SYM { $$ = $1 }
| DATAFILE_SYM { $$ = $1 }
| DATETIME { $$ = $1 }
| DATE_SYM { $$ = $1 }
| DAY_SYM { $$ = $1 }
| DEFAULT_AUTH_SYM { $$ = $1 }
| DEFINER_SYM { $$ = $1 }
| DELAY_KEY_WRITE_SYM { $$ = $1 }
| DES_KEY_FILE { $$ = $1 }
| DIAGNOSTICS_SYM { $$ = $1 }
| DIRECTORY_SYM { $$ = $1 }
| DISABLE_SYM { $$ = $1 }
| DISCARD { $$ = $1 }
| DISK_SYM { $$ = $1 }
| DUMPFILE { $$ = $1 }
| DUPLICATE_SYM { $$ = $1 }
| DYNAMIC_SYM { $$ = $1 }
| ENDS_SYM { $$ = $1 }
| ENUM { $$ = $1 }
| ENGINE_SYM { $$ = $1 }
| ENGINES_SYM { $$ = $1 }
| ERROR_SYM { $$ = $1 }
| ERRORS { $$ = $1 }
| ESCAPE_SYM { $$ = $1 }
| EVENT_SYM { $$ = $1 }
| EVENTS_SYM { $$ = $1 }
| EVERY_SYM { $$ = $1 }
| EXCHANGE_SYM { $$ = $1 }
| EXPANSION_SYM { $$ = $1 }
| EXPIRE_SYM { $$ = $1 }
| EXPORT_SYM { $$ = $1 }
| EXTENDED_SYM { $$ = $1 }
| EXTENT_SIZE_SYM { $$ = $1 }
| FAULTS_SYM { $$ = $1 }
| FAST_SYM { $$ = $1 }
| FOUND_SYM { $$ = $1 }
| ENABLE_SYM { $$ = $1 }
| FULL { $$ = $1 }
| FILE_SYM { $$ = $1 }
| FIRST_SYM { $$ = $1 }
| FIXED_SYM { $$ = $1 }
| GENERAL { $$ = $1 }
| GEOMETRY_SYM { $$ = $1 }
| GEOMETRYCOLLECTION { $$ = $1 }
| GET_FORMAT { $$ = $1 }
| GRANTS { $$ = $1 }
| GLOBAL_SYM { $$ = $1 }
| HASH_SYM { $$ = $1 }
| HOSTS_SYM { $$ = $1 }
| HOUR_SYM { $$ = $1 }
| IDENTIFIED_SYM { $$ = $1 }
| IGNORE_SERVER_IDS_SYM { $$ = $1 }
| INVOKER_SYM  { $$ = $1 }
| IMPORT { $$ = $1 }
| INDEXES { $$ = $1 }
| INITIAL_SIZE_SYM { $$ = $1 }
| IO_SYM { $$ = $1 }
| IPC_SYM { $$ = $1 }
| ISOLATION { $$ = $1 }
| ISSUER_SYM { $$ = $1 }
| INSERT_METHOD { $$ = $1 }
| KEY_BLOCK_SIZE { $$ = $1 }
| LAST_SYM { $$ = $1 }
| LEAVES { $$ = $1 }
| LESS_SYM { $$ = $1 }
| LEVEL_SYM { $$ = $1 }
| LINESTRING { $$ = $1 }
| LIST_SYM { $$ = $1 }
| LOCAL_SYM { $$ = $1 }
| LOCKS_SYM { $$ = $1 }
| LOGFILE_SYM { $$ = $1 }
| LOGS_SYM { $$ = $1 }
| MAX_ROWS { $$ = $1 }
| MASTER_SYM { $$ = $1 }
| MASTER_HEARTBEAT_PERIOD_SYM { $$ = $1 }
| MASTER_HOST_SYM { $$ = $1 }
| MASTER_PORT_SYM { $$ = $1 }
| MASTER_LOG_FILE_SYM { $$ = $1 }
| MASTER_LOG_POS_SYM { $$ = $1 }
| MASTER_USER_SYM { $$ = $1 }
| MASTER_PASSWORD_SYM { $$ = $1 }
| MASTER_SERVER_ID_SYM { $$ = $1 }
| MASTER_CONNECT_RETRY_SYM { $$ = $1 }
| MASTER_RETRY_COUNT_SYM { $$ = $1 }
| MASTER_DELAY_SYM { $$ = $1 }
| MASTER_SSL_SYM { $$ = $1 }
| MASTER_SSL_CA_SYM { $$ = $1 }
| MASTER_SSL_CAPATH_SYM { $$ = $1 }
| MASTER_SSL_CERT_SYM { $$ = $1 }
| MASTER_SSL_CIPHER_SYM { $$ = $1 }
| MASTER_SSL_CRL_SYM { $$ = $1 }
| MASTER_SSL_CRLPATH_SYM { $$ = $1 }
| MASTER_SSL_KEY_SYM { $$ = $1 }
| MASTER_AUTO_POSITION_SYM { $$ = $1 }
| MAX_CONNECTIONS_PER_HOUR { $$ = $1 }
| MAX_QUERIES_PER_HOUR { $$ = $1 }
| MAX_SIZE_SYM { $$ = $1 }
| MAX_UPDATES_PER_HOUR { $$ = $1 }
| MAX_USER_CONNECTIONS_SYM { $$ = $1 }
| MEDIUM_SYM { $$ = $1 }
| MEMORY_SYM { $$ = $1 }
| MERGE_SYM { $$ = $1 }
| MESSAGE_TEXT_SYM { $$ = $1 }
| MICROSECOND_SYM { $$ = $1 }
| MIGRATE_SYM { $$ = $1 }
| MINUTE_SYM { $$ = $1 }
| MIN_ROWS { $$ = $1 }
| MODIFY_SYM { $$ = $1 }
| MODE_SYM { $$ = $1 }
| MONTH_SYM { $$ = $1 }
| MULTILINESTRING { $$ = $1 }
| MULTIPOINT { $$ = $1 }
| MULTIPOLYGON { $$ = $1 }
| MUTEX_SYM { $$ = $1 }
| MYSQL_ERRNO_SYM { $$ = $1 }
| NAME_SYM { $$ = $1 }
| NAMES_SYM { $$ = $1 }
| NATIONAL_SYM { $$ = $1 }
| NCHAR_SYM { $$ = $1 }
| NDBCLUSTER_SYM { $$ = $1 }
| NEXT_SYM { $$ = $1 }
| NEW_SYM { $$ = $1 }
| NO_WAIT_SYM { $$ = $1 }
| NODEGROUP_SYM { $$ = $1 }
| NONE_SYM { $$ = $1 }
| NUMBER_SYM { $$ = $1 }
| NVARCHAR_SYM { $$ = $1 }
| OFFSET_SYM { $$ = $1 }
| OLD_PASSWORD { $$ = $1 }
| ONE_SYM { $$ = $1 }
| ONLY_SYM { $$ = $1 }
| PACK_KEYS_SYM { $$ = $1 }
| PAGE_SYM { $$ = $1 }
| PARTIAL { $$ = $1 }
| PARTITIONING_SYM { $$ = $1 }
| PARTITIONS_SYM { $$ = $1 }
| PASSWORD { $$ = $1 }
| PHASE_SYM { $$ = $1 }
| PLUGIN_DIR_SYM { $$ = $1 }
| PLUGIN_SYM { $$ = $1 }
| PLUGINS_SYM { $$ = $1 }
| POINT_SYM { $$ = $1 }
| POLYGON { $$ = $1 }
| PRESERVE_SYM { $$ = $1 }
| PREV_SYM { $$ = $1 }
| PRIVILEGES { $$ = $1 }
| PROCESS { $$ = $1 }
| PROCESSLIST_SYM { $$ = $1 }
| PROFILE_SYM { $$ = $1 }
| PROFILES_SYM { $$ = $1 }
| PROXY_SYM { $$ = $1 }
| QUARTER_SYM { $$ = $1 }
| QUERY_SYM { $$ = $1 }
| QUICK { $$ = $1 }
| READ_ONLY_SYM { $$ = $1 }
| REBUILD_SYM { $$ = $1 }
| RECOVER_SYM { $$ = $1 }
| REDO_BUFFER_SIZE_SYM { $$ = $1 }
| REDOFILE_SYM { $$ = $1 }
| REDUNDANT_SYM { $$ = $1 }
| RELAY { $$ = $1 }
| RELAYLOG_SYM { $$ = $1 }
| RELAY_LOG_FILE_SYM { $$ = $1 }
| RELAY_LOG_POS_SYM { $$ = $1 }
| RELAY_THREAD { $$ = $1 }
| RELOAD { $$ = $1 }
| REORGANIZE_SYM { $$ = $1 }
| REPEATABLE_SYM { $$ = $1 }
| REPLICATION { $$ = $1 }
| RESOURCES { $$ = $1 }
| RESUME_SYM { $$ = $1 }
| RETURNED_SQLSTATE_SYM { $$ = $1 }
| RETURNS_SYM { $$ = $1 }
| REVERSE_SYM { $$ = $1 }
| ROLLUP_SYM { $$ = $1 }
| ROUTINE_SYM { $$ = $1 }
| ROWS_SYM { $$ = $1 }
| ROW_COUNT_SYM { $$ = $1 }
| ROW_FORMAT_SYM { $$ = $1 }
| ROW_SYM { $$ = $1 }
| RTREE_SYM { $$ = $1 }
| SCHEDULE_SYM { $$ = $1 }
| SCHEMA_NAME_SYM { $$ = $1 }
| SECOND_SYM { $$ = $1 }
| SERIAL_SYM { $$ = $1 }
| SERIALIZABLE_SYM { $$ = $1 }
| SESSION_SYM { $$ = $1 }
| SIMPLE_SYM { $$ = $1 }
| SHARE_SYM { $$ = $1 }
| SHUTDOWN { $$ = $1 }
| SLOW { $$ = $1 }
| SNAPSHOT_SYM { $$ = $1 }
| SOUNDS_SYM { $$ = $1 }
| SOURCE_SYM { $$ = $1 }
| SQL_AFTER_GTIDS { $$ = $1 }
| SQL_AFTER_MTS_GAPS { $$ = $1 }
| SQL_BEFORE_GTIDS { $$ = $1 }
| SQL_CACHE_SYM { $$ = $1 }
| SQL_BUFFER_RESULT { $$ = $1 }
| SQL_NO_CACHE_SYM { $$ = $1 }
| SQL_THREAD { $$ = $1 }
| STARTS_SYM { $$ = $1 }
| STATS_AUTO_RECALC_SYM { $$ = $1 }
| STATS_PERSISTENT_SYM { $$ = $1 }
| STATS_SAMPLE_PAGES_SYM { $$ = $1 }
| STATUS_SYM { $$ = $1 }
| STORAGE_SYM { $$ = $1 }
| STRING_SYM { $$ = $1 }
| SUBCLASS_ORIGIN_SYM { $$ = $1 }
| SUBDATE_SYM { $$ = $1 }
| SUBJECT_SYM { $$ = $1 }
| SUBPARTITION_SYM { $$ = $1 }
| SUBPARTITIONS_SYM { $$ = $1 }
| SUPER_SYM { $$ = $1 }
| SUSPEND_SYM { $$ = $1 }
| SWAPS_SYM { $$ = $1 }
| SWITCHES_SYM { $$ = $1 }
| TABLE_NAME_SYM { $$ = $1 }
| TABLES { $$ = $1 }
| TABLE_CHECKSUM_SYM { $$ = $1 }
| TABLESPACE { $$ = $1 }
| TEMPORARY { $$ = $1 }
| TEMPTABLE_SYM { $$ = $1 }
| TEXT_SYM { $$ = $1 }
| THAN_SYM { $$ = $1 }
| TRANSACTION_SYM { $$ = $1 }
| TRIGGERS_SYM { $$ = $1 }
| TIMESTAMP { $$ = $1 }
| TIMESTAMP_ADD { $$ = $1 }
| TIMESTAMP_DIFF { $$ = $1 }
| TIME_SYM { $$ = $1 }
| TYPES_SYM { $$ = $1 }
| TYPE_SYM { $$ = $1 }
| UDF_RETURNS_SYM { $$ = $1 }
| FUNCTION_SYM { $$ = $1 }
| UNCOMMITTED_SYM { $$ = $1 }
| UNDEFINED_SYM { $$ = $1 }
| UNDO_BUFFER_SIZE_SYM { $$ = $1 }
| UNDOFILE_SYM { $$ = $1 }
| UNKNOWN_SYM { $$ = $1 }
| UNTIL_SYM { $$ = $1 }
| USER { $$ = $1 }
| USE_FRM { $$ = $1 }
| VARIABLES { $$ = $1 }
| VIEW_SYM { $$ = $1 } 
| VALUE_SYM { $$ = $1 }
| WARNINGS { $$ = $1 } 
| WAIT_SYM { $$ = $1 }
| WEEK_SYM { $$ = $1 }
| WORK_SYM { $$ = $1 }
| WEIGHT_STRING_SYM { $$ = $1 }
| X509_SYM { $$ = $1 }
| XML_SYM { $$ = $1 }
| YEAR_SYM { $$ = $1 }
;

set:
  SET start_option_value_list { $$ = $2 };

start_option_value_list:
  option_value_no_option_type option_value_list_continued 
  {
    if $2 == nil {
        $$ = &Set{VarList: Vars{$1}}
    } else {
        $$ = &Set{VarList: append($2, $1)}
    }
  }
| TRANSACTION_SYM transaction_characteristics { $$ = &SetTrans{} }
| option_type start_option_value_list_following_option_type 
  {
    if st, ok := $2.(*SetTrans); ok {
        $$ = st
    } else {
        tmp := $2.(Vars)
        for _, v := range tmp {
            v.Life = $1
        }

        $$ = &Set{VarList: tmp}
    }
  };

start_option_value_list_following_option_type:
  option_value_following_option_type option_value_list_continued
  { 
    tmp := Vars{$1}
    if $2 != nil {
        $$ = append(tmp, $2...)
    } else {
        $$ = tmp
    }
  }
| TRANSACTION_SYM transaction_characteristics { $$ = &SetTrans{} };

option_value_list_continued:
  { $$ = nil } 
| ',' option_value_list { $$ = $2 };

option_value_list:
  option_value { $$ = Vars{$1} }
| option_value_list ',' option_value { $$ = append($1, $3) };

option_value:
  option_type option_value_following_option_type 
  {
    $2.Life = $1
    $$ = $2
  }
| option_value_no_option_type { $$ = $1 };

option_type:
  GLOBAL_SYM { $$ = Life_Global }
| LOCAL_SYM { $$ = Life_Local }
| SESSION_SYM { $$ = Life_Session };

opt_var_type:
 
| GLOBAL_SYM
| LOCAL_SYM
| SESSION_SYM;

opt_var_ident_type:
  { $$ = Life_Unknown} 
| GLOBAL_SYM '.' { $$ = Life_Global }
| LOCAL_SYM '.' { $$ = Life_Local }
| SESSION_SYM '.' { $$ = Life_Session };

option_value_following_option_type:
  internal_variable_name equal set_expr_or_default 
  { $$ = &Variable{Type: Type_Usr, Name: $1, Value: $3} };

option_value_no_option_type:
  internal_variable_name equal set_expr_or_default 
  { $$ = &Variable{Type: Type_Usr, Name: $1, Value: $3} }
| '@' ident_or_text equal expr 
  { $$ = &Variable{Type: Type_Usr, Name: string($2), Value: $4} }
| '@' '@' opt_var_ident_type internal_variable_name equal set_expr_or_default
  { $$ = &Variable{Type: Type_Sys, Life: $3, Name: $4, Value: $6} }
| charset old_or_new_charset_name_or_default
  { $$ = &Variable{Type: Type_Sys, Name: "CHARACTER SET", Value: StrVal($2)} }
| NAMES_SYM equal expr
  { $$ = &Variable{Type: Type_Sys, Name: "NAMES", Value: $3} }
| NAMES_SYM charset_name_or_default opt_collate
  { $$ = &Variable{Type: Type_Sys, Name: "NAMES", Value: StrVal($2)} }
| PASSWORD equal text_or_password
  { $$ = &Variable{Type: Type_Sys, Name: "PASSWORD"} }
| PASSWORD FOR_SYM user equal text_or_password
  { $$ = &Variable{Type: Type_Sys, Name: "PASSWORD"} }
;

internal_variable_name:
  ident { $$ = string($1) }
| ident '.' ident { $$ = string($1) + "." + string($3)}
| DEFAULT '.' ident { $$ = "DEFAULT." + string($3) };

transaction_characteristics:
  transaction_access_mode
| isolation_level
| transaction_access_mode ',' isolation_level
| isolation_level ',' transaction_access_mode;

transaction_access_mode:
  transaction_access_mode_types;

isolation_level:
  ISOLATION LEVEL_SYM isolation_types;

transaction_access_mode_types:
  READ_SYM ONLY_SYM
| READ_SYM WRITE_SYM;

isolation_types:
  READ_SYM UNCOMMITTED_SYM
| READ_SYM COMMITTED_SYM
| REPEATABLE_SYM READ_SYM
| SERIALIZABLE_SYM;

text_or_password:
  TEXT_STRING
| PASSWORD '(' TEXT_STRING ')'
| OLD_PASSWORD '(' TEXT_STRING ')';

set_expr_or_default:
  expr { $$ = $1 }
| DEFAULT { $$ = StrVal($1) }
| ON { $$ = StrVal($1) }
| ALL { $$ = StrVal($1) }
| BINARY { $$ = StrVal($1) }
;

lock:
  LOCK_SYM table_or_tables table_lock_list { $$ = &Lock{Tables: $3} };

table_or_tables:
  TABLE_SYM
| TABLES;

table_lock_list:
  table_lock { $$ = ISimpleTables{$1} }
| table_lock_list ',' table_lock { $$ = append($1, $3) };

table_lock:
  table_ident opt_table_alias lock_option { $$ = $1 };

lock_option:
  READ_SYM
| WRITE_SYM
| LOW_PRIORITY WRITE_SYM
| READ_SYM LOCAL_SYM;

unlock:
  UNLOCK_SYM table_or_tables { $$ = &Unlock{} };

handler:
  HANDLER_SYM table_ident OPEN_SYM opt_table_alias { $$ = &Handler{} }
| HANDLER_SYM table_ident_nodb CLOSE_SYM { $$ = &Handler{} }
| HANDLER_SYM table_ident_nodb READ_SYM handler_read_or_scan where_clause opt_limit_clause { $$ = &Handler{} }

;

handler_read_or_scan:
  handler_scan_function
| ident handler_rkey_function;

handler_scan_function:
  FIRST_SYM
| NEXT_SYM;

handler_rkey_function:
  FIRST_SYM
| NEXT_SYM
| PREV_SYM
| LAST_SYM
| handler_rkey_mode '(' values ')';

handler_rkey_mode:
  EQ
| GE
| LE
| GT_SYM
| LT;

revoke:
  REVOKE clear_privileges revoke_command { $$ = &Revoke{} };

revoke_command:
  grant_privileges ON opt_table grant_ident FROM grant_list
| grant_privileges ON FUNCTION_SYM grant_ident FROM grant_list
| grant_privileges ON PROCEDURE_SYM grant_ident FROM grant_list
| ALL opt_privileges ',' GRANT OPTION FROM grant_list
| PROXY_SYM ON user FROM grant_list;

grant:
  GRANT clear_privileges grant_command { $$ = &Grant{} };

grant_command:
  grant_privileges ON opt_table grant_ident TO_SYM grant_list require_clause grant_options
| grant_privileges ON FUNCTION_SYM grant_ident TO_SYM grant_list require_clause grant_options
| grant_privileges ON PROCEDURE_SYM grant_ident TO_SYM grant_list require_clause grant_options
| PROXY_SYM ON user TO_SYM grant_list opt_grant_option;

opt_table:
 
| TABLE_SYM;

grant_privileges:
  object_privilege_list
| ALL opt_privileges;

opt_privileges:
 
| PRIVILEGES;

object_privilege_list:
  object_privilege
| object_privilege_list ',' object_privilege;

object_privilege:
  SELECT_SYM opt_column_list
| INSERT opt_column_list
| UPDATE_SYM opt_column_list
| REFERENCES opt_column_list
| DELETE_SYM
| USAGE
| INDEX_SYM
| ALTER
| CREATE
| DROP
| EXECUTE_SYM
| RELOAD
| SHUTDOWN
| PROCESS
| FILE_SYM
| GRANT OPTION
| SHOW DATABASES
| SUPER_SYM
| CREATE TEMPORARY TABLES
| LOCK_SYM TABLES
| REPLICATION SLAVE
| REPLICATION CLIENT_SYM
| CREATE VIEW_SYM
| SHOW VIEW_SYM
| CREATE ROUTINE_SYM
| ALTER ROUTINE_SYM
| CREATE USER
| EVENT_SYM
| TRIGGER_SYM
| CREATE TABLESPACE;

opt_and:
 
| AND_SYM;

require_list:
  require_list_element opt_and require_list
| require_list_element;

require_list_element:
  SUBJECT_SYM TEXT_STRING
| ISSUER_SYM TEXT_STRING
| CIPHER_SYM TEXT_STRING;

grant_ident:
  '*'
| ident '.' '*'
| '*' '.' '*'
| table_ident;

user_list:
  user
| user_list ',' user;

grant_list:
  grant_user
| grant_list ',' grant_user;

grant_user:
  user IDENTIFIED_SYM BY TEXT_STRING
| user IDENTIFIED_SYM BY PASSWORD TEXT_STRING
| user IDENTIFIED_SYM WITH ident_or_text
| user IDENTIFIED_SYM WITH ident_or_text AS TEXT_STRING_sys
| user;

opt_column_list:
 
| '(' column_list ')';

column_list:
  column_list ',' column_list_id
| column_list_id;

column_list_id:
  ident;

require_clause:
 
| REQUIRE_SYM require_list
| REQUIRE_SYM SSL_SYM
| REQUIRE_SYM X509_SYM
| REQUIRE_SYM NONE_SYM;

grant_options:
 
| WITH grant_option_list;

opt_grant_option:
 
| WITH GRANT OPTION;

grant_option_list:
  grant_option_list grant_option
| grant_option;

grant_option:
  GRANT OPTION
| MAX_QUERIES_PER_HOUR ulong_num
| MAX_UPDATES_PER_HOUR ulong_num
| MAX_CONNECTIONS_PER_HOUR ulong_num
| MAX_USER_CONNECTIONS_SYM ulong_num;

begin:
  BEGIN_SYM opt_work;

opt_work:
 
| WORK_SYM;

opt_chain:
 
| AND_SYM NO_SYM CHAIN_SYM
| AND_SYM CHAIN_SYM;

opt_release:
 
| RELEASE_SYM
| NO_SYM RELEASE_SYM;

opt_savepoint:
 
| SAVEPOINT_SYM;

commit:
  COMMIT_SYM opt_work opt_chain opt_release { $$ = &Commit{} };

rollback:
  ROLLBACK_SYM opt_work opt_chain opt_release { $$ = &Rollback{} }
| ROLLBACK_SYM opt_work TO_SYM opt_savepoint ident { $$ = &Rollback{Point: $5} }
;

savepoint:
  SAVEPOINT_SYM ident { $$ = &SavePoint{Point: $2} }
;

release:
  RELEASE_SYM SAVEPOINT_SYM ident { $$ = &Release{Point: $3} };

union_clause_opt:
  { $$ = nil } 
| union_list { $$ = $1 };

union_list:
  UNION_SYM union_option select_init { $$ = $3 }
;

union_opt:
  { $$ = nil } 
| union_list { $$ = $1 }
| union_order_or_limit { $$ = nil };

opt_union_order_or_limit:
 
| union_order_or_limit;

union_order_or_limit:
  order_or_limit;

order_or_limit:
  order_clause opt_limit_clause_init
| limit_clause;

union_option:
 
| DISTINCT
| ALL;

query_specification:
  SELECT_SYM select_init2_derived { $$ = &SubQuery{SelectStatement: $2} }
| '(' select_paren_derived ')' { $$ = &SubQuery{SelectStatement: $2} }
;

query_expression_body:
  query_specification opt_union_order_or_limit { $$ = $1 }
| query_expression_body UNION_SYM union_option query_specification opt_union_order_or_limit { $$ = &Union{Left: $1, Right: $4} };

subselect:
  subselect_start query_expression_body subselect_end { $$ = &SubQuery{SelectStatement: $2} };

subselect_start:
 ;

subselect_end:
 ;

opt_query_expression_options:
 
| query_expression_option_list;

query_expression_option_list:
  query_expression_option_list query_expression_option
| query_expression_option;

query_expression_option:
  STRAIGHT_JOIN
| HIGH_PRIORITY
| DISTINCT
| SQL_SMALL_RESULT
| SQL_BIG_RESULT
| SQL_BUFFER_RESULT
| SQL_CALC_FOUND_ROWS
| ALL;

view_or_trigger_or_sp_or_event:
  definer definer_tail { $$ = $2 }
| no_definer no_definer_tail { $$ = $2 }
| view_replace_or_algorithm definer_opt view_tail { $$ = $3 };

definer_tail:
  view_tail { $$ = $1 }
| trigger_tail { $$ = $1 }
| sp_tail { $$ = $1 }
| sf_tail { $$ = $1 }
| event_tail { $$ = $1 }
;

no_definer_tail:
  view_tail { $$ = $1 }
| trigger_tail { $$ = $1 }
| sp_tail { $$ = $1 }
| sf_tail { $$ = $1 }
| udf_tail { $$ = $1 }
| event_tail { $$ = $1 }
;

definer_opt:
  no_definer
| definer;

no_definer:
 ;

definer:
  DEFINER_SYM EQ user;

view_replace_or_algorithm:
  view_replace
| view_replace view_algorithm
| view_algorithm;

view_replace:
  OR_SYM REPLACE;

view_algorithm:
  ALGORITHM_SYM EQ UNDEFINED_SYM
| ALGORITHM_SYM EQ MERGE_SYM
| ALGORITHM_SYM EQ TEMPTABLE_SYM;

view_suid:
 
| SQL_SYM SECURITY_SYM DEFINER_SYM
| SQL_SYM SECURITY_SYM INVOKER_SYM;

view_tail:
  view_suid VIEW_SYM table_ident view_list_opt AS view_select
  { $$ = &viewTail{View: $3, As: $6} }
;

view_list_opt:
 
| '(' view_list ')';

view_list:
  ident
| view_list ',' ident;

view_select:
  view_select_aux view_check_option { $$ = $1 };

view_select_aux:
  create_view_select union_clause_opt
  {
    if $2 == nil {
        $$ = $1
    } else {
        $$ = &Union{Left: $1, Right: $2}
    }
  }
| '(' create_view_select_paren ')' union_opt
  {
    if $4 == nil {
        $$ = &ParenSelect{Select: $2}
    } else {
        $$ = &Union{Left: &ParenSelect{Select: $2}, Right: $4}
    }
  }
;

create_view_select_paren:
  create_view_select { $$ = $1 }
| '(' create_view_select_paren ')' { $$ = &ParenSelect{Select: $2} }
;

create_view_select:
  SELECT_SYM select_part2 { $$ = $2 };

view_check_option:
 
| WITH CHECK_SYM OPTION
| WITH CASCADED CHECK_SYM OPTION
| WITH LOCAL_SYM CHECK_SYM OPTION;

trigger_tail:
  TRIGGER_SYM remember_name sp_name trg_action_time trg_event ON remember_name table_ident FOR_SYM remember_name EACH_SYM ROW_SYM sp_proc_stmt
  { $$ = &triggerTail{Trigger: $3} }
;

udf_tail:
  AGGREGATE_SYM remember_name FUNCTION_SYM ident RETURNS_SYM udf_type SONAME_SYM TEXT_STRING_sys 
  { $$ = &udfTail{} }
| remember_name FUNCTION_SYM ident RETURNS_SYM udf_type SONAME_SYM TEXT_STRING_sys
  { $$ = &udfTail{} }
;

sf_tail:
  remember_name FUNCTION_SYM sp_name '(' sp_fdparam_list ')' RETURNS_SYM type_with_opt_collate sp_c_chistics sp_proc_stmt
  { $$ = &sfTail{Function: $3} }
;

sp_tail:
  PROCEDURE_SYM remember_name sp_name '(' sp_pdparam_list ')' sp_c_chistics sp_proc_stmt
  { $$ = &spTail{Procedure: $3} }
;

xa:
  XA_SYM begin_or_start xid opt_join_or_resume { $$ = &XA{} }
| XA_SYM END xid opt_suspend { $$ = &XA{} }
| XA_SYM PREPARE_SYM xid { $$ = &XA{} }
| XA_SYM COMMIT_SYM xid opt_one_phase { $$ = &XA{} }
| XA_SYM ROLLBACK_SYM xid { $$ = &XA{} }
| XA_SYM RECOVER_SYM { $$ = &XA{} }
;

xid:
  text_string
| text_string ',' text_string
| text_string ',' text_string ',' ulong_num;

begin_or_start:
  BEGIN_SYM
| START_SYM;

opt_join_or_resume:
 
| JOIN_SYM
| RESUME_SYM;

opt_one_phase:
 
| ONE_SYM PHASE_SYM;

opt_suspend:
 
| SUSPEND_SYM opt_migrate;

opt_migrate:
 
| FOR_SYM MIGRATE_SYM;

install:
  INSTALL_SYM PLUGIN_SYM ident SONAME_SYM TEXT_STRING_sys { $$ = &Install{} }
;

uninstall:
  UNINSTALL_SYM PLUGIN_SYM ident { $$ = &Uninstall{} }
;
%%
