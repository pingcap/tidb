-- Rebuild TiDB materialized-view maintenance system tables.
--
-- Target schema source:
--   cp_mv_for_master (bf681b1b662a04c01dfdbf62507af90790df453d)
--   pkg/session/bootstrap.go
--
-- Preconditions:
--   1. The cluster contains no materialized views and no materialized view logs.
--   2. Run during a maintenance window, with MV refresh/purge scheduling stopped.
--   3. Run this only when moving to the cp_mv_for_master MV implementation.
--
-- This deliberately discards all MV refresh/purge scheduling state, history, and alerts.
-- DDL statements are individually committed by TiDB; do not assume this script is atomic.

DROP TABLE IF EXISTS mysql.tidb_mview_refresh_alert;
DROP TABLE IF EXISTS mysql.tidb_mview_refresh_hist;
DROP TABLE IF EXISTS mysql.tidb_mlog_purge_hist;
DROP TABLE IF EXISTS mysql.tidb_mview_refresh_info;
DROP TABLE IF EXISTS mysql.tidb_mlog_purge_info;

CREATE TABLE mysql.tidb_mview_refresh_info (
    MVIEW_ID bigint NOT NULL,
    LAST_SUCCESS_READ_TSO bigint unsigned DEFAULT NULL,
    LAST_SUCCESS_REFRESH_END_UNIX_SECONDS bigint DEFAULT NULL,
    NEXT_REFRESH_UNIX_SECONDS bigint DEFAULT NULL,
    PRIMARY KEY(MVIEW_ID)
);

CREATE TABLE mysql.tidb_mlog_purge_info (
    MLOG_ID bigint NOT NULL,
    NEXT_PURGE_UNIX_SECONDS bigint DEFAULT NULL,
    LAST_PURGED_TSO bigint unsigned DEFAULT NULL,
    PRIMARY KEY(MLOG_ID)
);

CREATE TABLE mysql.tidb_mview_refresh_hist (
    REFRESH_JOB_ID bigint unsigned NOT NULL,
    MVIEW_ID bigint NOT NULL,
    MVIEW_SCHEMA varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
    MVIEW_NAME varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
    REFRESH_METHOD varchar(32) NOT NULL,
    REFRESH_START_TIME datetime(6) DEFAULT NULL,
    REFRESH_END_TIME datetime(6) DEFAULT NULL,
    REFRESH_DURATION_SEC decimal(18,6) DEFAULT NULL,
    REFRESH_SCHEDULE_DURATION_SEC decimal(18,6) DEFAULT NULL,
    REFRESH_STATUS varchar(16) DEFAULT NULL,
    REFRESH_ROWS bigint DEFAULT NULL,
    REFRESH_READ_TSO bigint unsigned DEFAULT NULL,
    REFRESH_COMMIT_TSO bigint unsigned DEFAULT NULL,
    REFRESH_FAILED_REASON text DEFAULT NULL,
    CANCEL_REQUEST_TIME datetime(6) DEFAULT NULL,
    CANCEL_REQUESTED_BY varchar(512) DEFAULT NULL,
    LAST_HEARTBEAT_TIME datetime(6) DEFAULT NULL,
    PRIMARY KEY(REFRESH_JOB_ID),
    KEY idx_mview_start_time (MVIEW_ID, REFRESH_START_TIME),
    KEY idx_mview_name_start_time (MVIEW_SCHEMA, MVIEW_NAME, REFRESH_START_TIME),
    KEY idx_mview_name_commit_tso (MVIEW_SCHEMA, MVIEW_NAME, REFRESH_COMMIT_TSO),
    KEY idx_mview_status_start_time (MVIEW_ID, REFRESH_STATUS, REFRESH_START_TIME),
    KEY idx_refresh_duration_sec (REFRESH_DURATION_SEC),
    KEY idx_refresh_schedule_duration_sec (REFRESH_SCHEDULE_DURATION_SEC),
    KEY idx_refresh_start_time (REFRESH_START_TIME),
    KEY idx_refresh_status_start_time (REFRESH_STATUS, REFRESH_START_TIME)
);

CREATE TABLE mysql.tidb_mview_refresh_alert (
    MVIEW_ID bigint NOT NULL,
    MVIEW_SCHEMA varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
    MVIEW_NAME varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
    ALERT_LEVEL varchar(16) DEFAULT NULL,
    REFRESH_FAILED varchar(3) DEFAULT NULL,
    LAST_SUCCESS_SNAPSHOT_TIME datetime(6) DEFAULT NULL,
    UPDATE_TIME datetime(6) DEFAULT NULL,
    PRIMARY KEY(MVIEW_ID)
);

CREATE TABLE mysql.tidb_mlog_purge_hist (
    PURGE_JOB_ID bigint unsigned NOT NULL,
    MLOG_ID bigint NOT NULL,
    BASE_TABLE_SCHEMA varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
    BASE_TABLE_NAME varchar(64) CHARSET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
    PURGE_METHOD varchar(32) NOT NULL,
    PURGE_START_TIME datetime(6) DEFAULT NULL,
    PURGE_END_TIME datetime(6) DEFAULT NULL,
    PURGE_DURATION_SEC decimal(18,6) DEFAULT NULL,
    PURGE_ROWS bigint NOT NULL,
    PURGE_STATUS varchar(16) DEFAULT NULL,
    PURGE_CUTOFF_TSO bigint unsigned DEFAULT NULL,
    PURGE_FAILED_REASON text DEFAULT NULL,
    CANCEL_REQUEST_TIME datetime(6) DEFAULT NULL,
    CANCEL_REQUESTED_BY varchar(512) DEFAULT NULL,
    LAST_HEARTBEAT_TIME datetime(6) DEFAULT NULL,
    PRIMARY KEY(PURGE_JOB_ID),
    KEY idx_mlog_start_time (MLOG_ID, PURGE_START_TIME),
    KEY idx_table_name_start_time (BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_START_TIME),
    KEY idx_mlog_status_start_time (MLOG_ID, PURGE_STATUS, PURGE_START_TIME),
    KEY idx_purge_duration_sec (PURGE_DURATION_SEC),
    KEY idx_purge_start_time (PURGE_START_TIME),
    KEY idx_purge_status_start_time (PURGE_STATUS, PURGE_START_TIME)
);
