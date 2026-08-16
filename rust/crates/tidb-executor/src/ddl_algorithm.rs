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

//! COMPLETE transcreation of ONE file of Go `pkg/ddl`: `ddl_algorithm.go`.
//! The rest of `pkg/ddl` is not transcreated here, so this module is a
//! complete FILE, not a complete PACKAGE. Every symbol that file declares is
//! present: [`AlterAlgorithm`], its two package-level values
//! (`instantAlgorithm`, `inplaceAlgorithm`), `getProperAlgorithm`, and
//! [`resolve_alter_algorithm`].
//!
//! It answers one question: given what the user asked for after
//! `ALGORITHM =` and which ALTER the statement performs, which online-DDL
//! algorithm actually runs -- and whether asking for the other one is an
//! error rather than a silent substitution.
//!
//! TiDB supports only INPLACE and INSTANT. Almost every ALTER is INSTANT
//! (metadata only); adding a constraint is INPLACE, which is not literally
//! in place -- DML is never blocked, but backfilling the index data costs
//! time.
//!
//! When the requested algorithm is not supported, the resolver looks for a
//! better one in the order `INSTANT > INPLACE > COPY` and returns it
//! ALONGSIDE the error, so a caller that tolerates the substitution has the
//! algorithm to use. Asking for something stronger than anything supported
//! (INSTANT on an INPLACE-only alter) yields `Default` and the error.
//!
//! # Narrowings, each named
//!
//! - `ast.AlterTableSpec` / `ast.AlterTableType`: Go dispatches on the
//!   spec's `Tp` tag. The transcreated `tidb-ast` models an ALTER clause as
//!   the payload-carrying [`AlterTableAction`] enum with no separate tag, so
//!   [`resolve_alter_algorithm`] takes the action itself and
//!   [`is_add_constraint`] recovers Go's single interesting case. Go's
//!   parser assigns `AlterTableAddConstraint` to every `ADD` of an index,
//!   primary key, unique key, foreign key, or check, which is the three
//!   Rust variants that predicate names.
//! - `ast.AlgorithmType`'s `iota` ordering: `getProperAlgorithm` compares
//!   algorithms with `<=`, so the numbering IS the semantics. The Rust
//!   [`AlterTableAlgorithm`] has the same four variants in the same order
//!   but no `Ord`, so [`algorithm_rank`] restates Go's `iota` explicitly.
//! - `dbterror.ErrAlterOperationNotSupported`: this crate carries errors as
//!   local enums rather than the `dbterror` machinery, so the failure is
//!   [`AlterAlgorithmError`], which reports errno 1846 and formats the same
//!   three `%s` arguments Go passes.

use tidb_ast::{AlterTableAction, AlterTableAlgorithm};

/// Go `ErrAlterOperationNotSupportedReason`'s errno.
pub const ERR_ALTER_OPERATION_NOT_SUPPORTED_REASON: u16 = 1846;

/// Go `dbterror.ErrAlterOperationNotSupported`, raised when the requested
/// algorithm is not one the alter operation supports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterAlgorithmError {
    /// Go's first `%s`: `ALGORITHM=<requested>`.
    pub operation: String,
    /// Go's second `%s`: `Cannot alter table by <requested>`.
    pub reason: String,
    /// Go's third `%s`: `ALGORITHM=<the alter's default>`.
    pub suggestion: String,
}

impl AlterAlgorithmError {
    /// The MySQL error number `dbterror.ClassDDL.NewStd` attaches.
    #[must_use]
    pub const fn code(&self) -> u16 {
        ERR_ALTER_OPERATION_NOT_SUPPORTED_REASON
    }
}

impl std::fmt::Display for AlterAlgorithmError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // errno.ErrAlterOperationNotSupportedReason:
        // "%s is not supported. Reason: %s. Try %s."
        write!(
            formatter,
            "{} is not supported. Reason: {}. Try {}.",
            self.operation, self.reason, self.suggestion
        )
    }
}

impl std::error::Error for AlterAlgorithmError {}

/// Go `AlterAlgorithm`: the algorithms one alter operation supports.
///
/// For now TiDB supports only `AlgorithmTypeInplace` and
/// `AlgorithmTypeInstant`. See
/// <https://dev.mysql.com/doc/refman/8.0/en/alter-table.html#alter-table-performance>.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlterAlgorithm {
    /// Go `supported`, which MUST store algorithms in the order
    /// `INSTANT, INPLACE, COPY`.
    supported: &'static [AlterTableAlgorithm],
    /// Go `defAlgorithm`, used when the alter algorithm is not given.
    def_algorithm: AlterTableAlgorithm,
}

/// Go `instantAlgorithm`.
pub const INSTANT_ALGORITHM: AlterAlgorithm = AlterAlgorithm {
    supported: &[AlterTableAlgorithm::Instant],
    def_algorithm: AlterTableAlgorithm::Instant,
};

/// Go `inplaceAlgorithm`.
pub const INPLACE_ALGORITHM: AlterAlgorithm = AlterAlgorithm {
    supported: &[AlterTableAlgorithm::Inplace],
    def_algorithm: AlterTableAlgorithm::Inplace,
};

/// Go's `ast.AlgorithmType` `iota` order, which `getProperAlgorithm`'s `<=`
/// depends on: `Default < Copy < Inplace < Instant`.
const fn algorithm_rank(algorithm: AlterTableAlgorithm) -> u8 {
    match algorithm {
        AlterTableAlgorithm::Default => 0,
        AlterTableAlgorithm::Copy => 1,
        AlterTableAlgorithm::Inplace => 2,
        AlterTableAlgorithm::Instant => 3,
    }
}

/// Go's `fmt.Sprintf("%s", ast.AlgorithmType)`, i.e. `AlgorithmType.String()`.
const fn algorithm_text(algorithm: AlterTableAlgorithm) -> &'static str {
    match algorithm {
        AlterTableAlgorithm::Default => "DEFAULT",
        AlterTableAlgorithm::Copy => "COPY",
        AlterTableAlgorithm::Inplace => "INPLACE",
        AlterTableAlgorithm::Instant => "INSTANT",
    }
}

/// Go `getProperAlgorithm`.
fn get_proper_algorithm(
    specify: AlterTableAlgorithm,
    algorithm: &AlterAlgorithm,
) -> (AlterTableAlgorithm, Option<AlterAlgorithmError>) {
    if specify == AlterTableAlgorithm::Default {
        return (algorithm.def_algorithm, None);
    }

    let mut resolved = AlterTableAlgorithm::Default;

    for supported in algorithm.supported {
        if algorithm_rank(specify) <= algorithm_rank(*supported) {
            resolved = *supported;
            break;
        }
    }

    let error = (specify != resolved).then(|| AlterAlgorithmError {
        operation: format!("ALGORITHM={}", algorithm_text(specify)),
        reason: format!("Cannot alter table by {}", algorithm_text(specify)),
        suggestion: format!("ALGORITHM={}", algorithm_text(algorithm.def_algorithm)),
    });
    (resolved, error)
}

/// Whether Go's parser would tag this clause `ast.AlterTableAddConstraint`:
/// `ADD` of an index, primary key, unique key, foreign key, or check.
#[must_use]
pub fn is_add_constraint(action: &AlterTableAction) -> bool {
    matches!(
        action,
        AlterTableAction::AddIndexConstraint(_)
            | AlterTableAction::AddForeignKey(_)
            | AlterTableAction::AddCheck(_)
    )
}

/// Go `ResolveAlterAlgorithm`: resolve the algorithm of the alter clause.
///
/// If `specify` is `Default`, the alter action's default algorithm is
/// returned. If the specified algorithm is not supported by the alter action,
/// a better one is sought in the order `INSTANT > INPLACE > COPY` and
/// returned together with [`AlterAlgorithmError`] -- so `INSTANT` may come
/// back for `specify = INPLACE`. If no valid algorithm can be chosen,
/// `Default` and the error are returned.
#[must_use]
pub fn resolve_alter_algorithm(
    action: &AlterTableAction,
    specify: AlterTableAlgorithm,
) -> (AlterTableAlgorithm, Option<AlterAlgorithmError>) {
    // For now TiDB supports only the inplace and instant algorithms.
    if is_add_constraint(action) {
        get_proper_algorithm(specify, &INPLACE_ALGORITHM)
    } else {
        get_proper_algorithm(specify, &INSTANT_ALGORITHM)
    }
}

#[cfg(test)]
mod tests {
    //! Go `pkg/ddl/ddl_algorithm_test.go`: `TestFindAlterAlgorithm`.

    use super::*;
    use tidb_ast::{DdlStmt, Stmt};

    /// Go `allAlgorithm`.
    const ALL_ALGORITHM: [AlterTableAlgorithm; 3] = [
        AlterTableAlgorithm::Copy,
        AlterTableAlgorithm::Inplace,
        AlterTableAlgorithm::Instant,
    ];

    /// Go's `testCase` struct.
    struct TestCase {
        /// Go `alterSpec`. Go builds an `ast.AlterTableSpec` literal with only
        /// its `Tp` (and, for table options, one option) set; the Rust AST has
        /// no such tag, so the clause is PARSED, which produces exactly the
        /// action Go's parser would have tagged.
        alter_clause: &'static str,
        supported_algorithm: &'static [AlterTableAlgorithm],
        expected_algorithm: &'static [AlterTableAlgorithm],
    }

    fn parse_alter_action(clause: &str) -> AlterTableAction {
        let sql = format!("ALTER TABLE t {clause}");
        let stmt = tidb_parser::parse(&sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        let Stmt::Ddl(ddl) = stmt else {
            panic!("{sql} is not a DDL statement");
        };
        let DdlStmt::AlterTable(alter) = &*ddl else {
            panic!("{sql} is not ALTER TABLE");
        };
        alter
            .actions
            .first()
            .unwrap_or_else(|| panic!("{sql} produced no action"))
            .clone()
    }

    /// Go `TestFindAlterAlgorithm`.
    #[test]
    fn find_alter_algorithm_matches_go() {
        const SUPPORTED_INSTANT: &[AlterTableAlgorithm] = &[
            AlterTableAlgorithm::Default,
            AlterTableAlgorithm::Copy,
            AlterTableAlgorithm::Inplace,
            AlterTableAlgorithm::Instant,
        ];
        const EXPECTED_INSTANT: &[AlterTableAlgorithm] = &[
            AlterTableAlgorithm::Instant,
            AlterTableAlgorithm::Instant,
            AlterTableAlgorithm::Instant,
            AlterTableAlgorithm::Instant,
        ];

        let test_cases = [
            TestCase {
                // ast.AlterTableAddConstraint
                alter_clause: "ADD INDEX i1 (c1)",
                supported_algorithm: &[
                    AlterTableAlgorithm::Default,
                    AlterTableAlgorithm::Copy,
                    AlterTableAlgorithm::Inplace,
                ],
                expected_algorithm: &[
                    AlterTableAlgorithm::Inplace,
                    AlterTableAlgorithm::Inplace,
                    AlterTableAlgorithm::Inplace,
                ],
            },
            // ast.AlterTableAddColumns
            TestCase {
                alter_clause: "ADD COLUMN c2 INT",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableDropColumn
            TestCase {
                alter_clause: "DROP COLUMN c2",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableDropPrimaryKey
            TestCase {
                alter_clause: "DROP PRIMARY KEY",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableDropIndex
            TestCase {
                alter_clause: "DROP INDEX i1",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableDropForeignKey
            TestCase {
                alter_clause: "DROP FOREIGN KEY fk1",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableRenameTable
            TestCase {
                alter_clause: "RENAME TO t2",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableRenameIndex
            TestCase {
                alter_clause: "RENAME INDEX i1 TO i2",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // Alter table options.
            // ast.AlterTableOption / ast.TableOptionShardRowID
            TestCase {
                alter_clause: "SHARD_ROW_ID_BITS = 4",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableOption / ast.TableOptionAutoIncrement
            TestCase {
                alter_clause: "AUTO_INCREMENT = 3",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableOption / ast.TableOptionComment
            TestCase {
                alter_clause: "COMMENT = 'c'",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableOption / ast.TableOptionCharset
            TestCase {
                alter_clause: "CHARACTER SET = utf8mb4",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableOption / ast.TableOptionCollate
            TestCase {
                alter_clause: "COLLATE = utf8mb4_bin",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // TODO (Go): after we support migrating the data of partitions,
            // change the cases below.
            // ast.AlterTableCoalescePartitions
            TestCase {
                alter_clause: "COALESCE PARTITION 2",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableAddPartitions
            TestCase {
                alter_clause: "ADD PARTITION PARTITIONS 2",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableDropPartition
            TestCase {
                alter_clause: "DROP PARTITION p1",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableTruncatePartition
            TestCase {
                alter_clause: "TRUNCATE PARTITION p1",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableExchangePartition
            TestCase {
                alter_clause: "EXCHANGE PARTITION p1 WITH TABLE t2",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // TODO (Go): after we support locking a table, change the case
            // below.
            // ast.AlterTableLock
            TestCase {
                alter_clause: "LOCK = NONE",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // TODO (Go): after we support changing the column type, the cases
            // below need to change.
            // ast.AlterTableModifyColumn
            TestCase {
                alter_clause: "MODIFY COLUMN c1 BIGINT",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableChangeColumn
            TestCase {
                alter_clause: "CHANGE COLUMN c1 c2 BIGINT",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
            // ast.AlterTableAlterColumn
            TestCase {
                alter_clause: "ALTER COLUMN c1 SET DEFAULT 1",
                supported_algorithm: SUPPORTED_INSTANT,
                expected_algorithm: EXPECTED_INSTANT,
            },
        ];

        for test_case in &test_cases {
            run_alter_algorithm_test_case(test_case);
        }
    }

    /// Go's `runAlterAlgorithmTestCases`.
    fn run_alter_algorithm_test_case(test_case: &TestCase) {
        let action = parse_alter_action(test_case.alter_clause);
        let unsupported: Vec<AlterTableAlgorithm> = ALL_ALGORITHM
            .into_iter()
            .filter(|algorithm| !test_case.supported_algorithm.contains(algorithm))
            .collect();

        // Test supported.
        for (position, specify) in test_case.supported_algorithm.iter().enumerate() {
            let (algorithm, error) = resolve_alter_algorithm(&action, *specify);
            if let Some(error) = error {
                assert_eq!(error.code(), ERR_ALTER_OPERATION_NOT_SUPPORTED_REASON);
            }
            assert_eq!(
                test_case.expected_algorithm[position], algorithm,
                "{}: specify {specify:?}",
                test_case.alter_clause
            );
        }

        // Test unsupported.
        for specify in unsupported {
            let (algorithm, error) = resolve_alter_algorithm(&action, specify);
            assert_eq!(AlterTableAlgorithm::Default, algorithm);
            let error = error.expect("an unsupported algorithm must raise an error");
            assert_eq!(error.code(), ERR_ALTER_OPERATION_NOT_SUPPORTED_REASON);
        }
    }
}
