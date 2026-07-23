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

//! Backup/restore grammar transcreated from `pkg/parser/import_brie_parser.go`.

use tidb_ast::{BrieKind, BrieOption, BrieOptionLevel, BrieOptionValue, BrieStmt};
use tidb_lexer::TokenKind;

use crate::{decode_string, PResult, Parser};

impl Parser {
    pub(crate) fn parse_brie(&mut self) -> PResult<BrieStmt> {
        let kind = if self.is_kw("BACKUP") {
            self.bump();
            BrieKind::Backup
        } else {
            self.expect_kw("RESTORE")?;
            BrieKind::Restore
        };
        if kind == BrieKind::Backup && self.is_kw("LOGS") {
            self.bump();
            self.expect_kw("TO")?;
            let storage = self.parse_brie_string()?;
            return Ok(BrieStmt {
                kind: BrieKind::StreamStart,
                schemas: Vec::new(),
                tables: Vec::new(),
                storage,
                job_id: 0,
                options: self.parse_brie_options()?,
            });
        }
        if kind == BrieKind::Restore && self.is_kw("POINT") {
            self.bump();
            self.expect_kw("FROM")?;
            let storage = self.parse_brie_string()?;
            return Ok(BrieStmt {
                kind: BrieKind::RestorePoint,
                schemas: Vec::new(),
                tables: Vec::new(),
                storage,
                job_id: 0,
                options: self.parse_brie_options()?,
            });
        }

        let mut schemas = Vec::new();
        let mut tables = Vec::new();
        if self.is_kw("DATABASE") || self.is_kw("SCHEMA") {
            self.bump();
            if self.is_op("*") {
                self.bump();
            } else {
                schemas.push(self.parse_name_or_keyword()?);
                while self.is_op(",") {
                    self.bump();
                    schemas.push(self.parse_name_or_keyword()?);
                }
            }
        } else if self.is_kw("TABLE") {
            self.bump();
            tables.push(self.parse_name_path()?);
            while self.is_op(",") {
                self.bump();
                tables.push(self.parse_name_path()?);
            }
        } else {
            return Err(self.err_here("expected BACKUP/RESTORE DATABASE or TABLE"));
        }
        self.expect_kw(if kind == BrieKind::Backup {
            "TO"
        } else {
            "FROM"
        })?;
        let storage = self.parse_brie_string()?;
        Ok(BrieStmt {
            kind,
            schemas,
            tables,
            storage,
            job_id: 0,
            options: self.parse_brie_options()?,
        })
    }

    pub(crate) fn parse_brie_control(&mut self) -> PResult<BrieStmt> {
        let kind = if self.is_kw("PAUSE") {
            BrieKind::StreamPause
        } else if self.is_kw("RESUME") {
            BrieKind::StreamResume
        } else if self.is_kw("STOP") {
            BrieKind::StreamStop
        } else {
            return Err(self.err_here("expected backup-log control"));
        };
        self.bump();
        self.expect_kw("BACKUP")?;
        self.expect_kw("LOGS")?;
        Ok(BrieStmt {
            kind,
            schemas: Vec::new(),
            tables: Vec::new(),
            storage: String::new(),
            job_id: 0,
            options: self.parse_brie_options()?,
        })
    }

    pub(crate) fn parse_purge_backup_logs(&mut self) -> PResult<BrieStmt> {
        self.expect_kw("PURGE")?;
        self.expect_kw("BACKUP")?;
        self.expect_kw("LOGS")?;
        self.expect_kw("FROM")?;
        let storage = self.parse_brie_string()?;
        Ok(BrieStmt {
            kind: BrieKind::StreamPurge,
            schemas: Vec::new(),
            tables: Vec::new(),
            storage,
            job_id: 0,
            options: self.parse_brie_options()?,
        })
    }

    pub(crate) fn parse_show_brie(&mut self) -> PResult<BrieStmt> {
        self.expect_kw("SHOW")?;
        if self.token_literal_is_at(0, "BR") {
            self.bump();
            self.expect_kw("JOB")?;
            let kind = if self.is_kw("QUERY") {
                self.bump();
                BrieKind::ShowQuery
            } else {
                BrieKind::ShowJob
            };
            return Ok(BrieStmt {
                kind,
                schemas: Vec::new(),
                tables: Vec::new(),
                storage: String::new(),
                job_id: self.parse_brie_job_id()?,
                options: Vec::new(),
            });
        }
        self.expect_token_literal("BACKUP")?;
        let kind = if self.is_kw("LOGS") {
            self.bump();
            if self.is_kw("STATUS") {
                self.bump();
                BrieKind::StreamStatus
            } else {
                self.expect_kw("METADATA")?;
                BrieKind::StreamMetadata
            }
        } else {
            self.expect_kw("METADATA")?;
            BrieKind::ShowBackupMetadata
        };
        let storage = if matches!(
            kind,
            BrieKind::StreamMetadata | BrieKind::ShowBackupMetadata
        ) {
            self.expect_kw("FROM")?;
            self.parse_brie_string()?
        } else {
            String::new()
        };
        Ok(BrieStmt {
            kind,
            schemas: Vec::new(),
            tables: Vec::new(),
            storage,
            job_id: 0,
            options: Vec::new(),
        })
    }

    pub(crate) fn parse_cancel_brie(&mut self) -> PResult<BrieStmt> {
        self.expect_kw("CANCEL")?;
        self.expect_kw("BR")?;
        self.expect_kw("JOB")?;
        Ok(BrieStmt {
            kind: BrieKind::CancelJob,
            schemas: Vec::new(),
            tables: Vec::new(),
            storage: String::new(),
            job_id: self.parse_brie_job_id()?,
            options: Vec::new(),
        })
    }

    fn parse_brie_job_id(&mut self) -> PResult<i64> {
        let token = self.bump();
        if token.kind != TokenKind::IntLit {
            return Err(self.err_here("expected BR job ID"));
        }
        token
            .text
            .parse::<i64>()
            .map_err(|_| self.err_here("expected BR job ID"))
    }

    fn parse_brie_string(&mut self) -> PResult<String> {
        let token = self.bump();
        if token.kind != TokenKind::Str {
            return Err(self.err_here("expected external-storage URL"));
        }
        Ok(decode_string(&token.text))
    }

    fn parse_brie_options(&mut self) -> PResult<Vec<BrieOption>> {
        let mut options = Vec::new();
        while self.peek().kind != TokenKind::Eof && !self.is_op(";") {
            let source_name = self.bump().text.to_ascii_uppercase();
            let name = canonical_brie_option_name(&source_name).to_string();
            if self.is_op("=") {
                self.bump();
            }
            let value = if self.peek().kind == TokenKind::Str {
                BrieOptionValue::String(decode_string(&self.bump().text))
            } else if self.is_kw("OFF") || self.is_kw("FALSE") {
                self.bump();
                brie_level_value(&name, BrieOptionLevel::Off)
            } else if self.is_kw("REQUIRED") || self.is_kw("TRUE") {
                self.bump();
                brie_level_value(&name, BrieOptionLevel::Required)
            } else if self.is_kw("OPTIONAL") {
                self.bump();
                brie_level_value(&name, BrieOptionLevel::Optional)
            } else if self.is_kw("COLUMNS") {
                self.bump();
                BrieOptionValue::CsvHeaderColumns
            } else {
                let token = self.bump();
                if token.kind != TokenKind::IntLit {
                    return Err(self.err_here("expected BRIE option value"));
                }
                let mut value = token
                    .text
                    .parse::<u64>()
                    .map_err(|_| self.err_here("expected BRIE unsigned option value"))?;
                if name == "RATE_LIMIT" && self.is_kw("MB") {
                    self.bump();
                    if self.is_op("/") {
                        self.bump();
                    }
                    self.expect_kw("SECOND")?;
                    value *= 1_048_576;
                    BrieOptionValue::RateLimitBytes(value)
                } else if (name == "SNAPSHOT" || name == "LAST_BACKUP")
                    && brie_time_unit(self.peek().text.as_str()).is_some()
                {
                    let multiplier =
                        brie_time_unit(self.bump().text.as_str()).expect("time unit was checked");
                    if self.is_kw("AGO") {
                        self.bump();
                    }
                    BrieOptionValue::MicrosecondsAgo(value * multiplier * 1_000)
                } else {
                    BrieOptionValue::Unsigned(value)
                }
            };
            options.push(BrieOption { name, value });
        }
        Ok(options)
    }
}

fn brie_level_value(name: &str, level: BrieOptionLevel) -> BrieOptionValue {
    if name == "CHECKSUM" || name == "ANALYZE" {
        BrieOptionValue::Level(level)
    } else {
        BrieOptionValue::Unsigned(match level {
            BrieOptionLevel::Off => 0,
            BrieOptionLevel::Required => 1,
            BrieOptionLevel::Optional => 2,
        })
    }
}

fn brie_time_unit(unit: &str) -> Option<u64> {
    Some(match unit.to_ascii_uppercase().as_str() {
        "MICROSECOND" => 1,
        "SECOND" => 1_000_000,
        "MINUTE" => 60_000_000,
        "HOUR" => 3_600_000_000,
        "DAY" => 86_400_000_000,
        "WEEK" => 604_800_000_000,
        "MONTH" => 2_592_000_000_000,
        "YEAR" => 31_536_000_000_000,
        _ => return None,
    })
}

fn canonical_brie_option_name(name: &str) -> &str {
    match name {
        "ENCRYPTION_KEYFILE" => "ENCRYPTION_KEY_FILE",
        "SNAPSHOT"
        | "LAST_BACKUP"
        | "RATE_LIMIT"
        | "CONCURRENCY"
        | "CHECKSUM"
        | "SEND_CREDENTIALS_TO_TIKV"
        | "CHECKPOINT"
        | "ONLINE"
        | "ANALYZE"
        | "BACKEND"
        | "ON_DUPLICATE"
        | "CSV_DELIMITER"
        | "CSV_HEADER"
        | "CSV_NULL"
        | "CSV_SEPARATOR"
        | "CSV_BACKSLASH_ESCAPE"
        | "CSV_NOT_NULL"
        | "CSV_TRIM_LAST_SEPARATORS"
        | "FULL_BACKUP_STORAGE"
        | "RESTORED_TS"
        | "START_TS"
        | "UNTIL_TS"
        | "GC_TTL"
        | "ENCRYPTION_METHOD"
        | "IGNORE_STATS"
        | "LOAD_STATS"
        | "WAIT_TIFLASH_READY"
        | "WITH_SYS_TABLE"
        | "CHECKSUM_CONCURRENCY"
        | "COMPRESSION_LEVEL"
        | "COMPRESSION_TYPE"
        | "SKIP_SCHEMA_FILES"
        | "STRICT_FORMAT"
        | "TIKV_IMPORTER"
        | "RESUME" => name,
        _ => "RATE_LIMIT",
    }
}
