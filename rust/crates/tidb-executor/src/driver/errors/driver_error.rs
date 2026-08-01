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

use super::{SchemaErrorKind, TxnErrorKind, VarErrorKind};
use crate::executor::ExecError;

/// A failure while running a SQL string through the driver.
#[derive(Debug, Clone)]
pub enum DriverError {
    /// The SQL failed to parse.
    Parse(String),
    /// The statement is not a supported `FROM`-less `SELECT`.
    Unsupported(&'static str),
    /// Like [`Self::Unsupported`], but the message names the specific
    /// statement/AST kind the refusal saw -- built at the refusal site from
    /// the parsed statement's own variant name, so the same top-level
    /// "not supported yet" wording stays diagnostic instead of generic.
    UnsupportedKind(String),
    /// Rewriting an expression or executing failed.
    Exec(ExecError),
    /// The shared catalog is unusable because a statement panicked while
    /// holding it, so its schema state may be half-written.
    CatalogPoisoned,
    /// A transaction could not be committed.
    Txn(TxnErrorKind),
    /// A session-variable statement failed.
    Var(VarErrorKind),
    /// A schema statement failed.
    Schema(SchemaErrorKind),
    /// Go `autoid.ErrAutoincReadFailed` (1467): the AUTO_INCREMENT column has
    /// no id left in its domain, which Go raises rather than reusing one.
    AutoincReadFailed,
    /// The AUTO_INCREMENT counter's home could not be read or written.
    ///
    /// Deliberately NOT `AutoincReadFailed`: Go raises 1467 only for a full
    /// domain, and answering it here would tell a user their ids had run out
    /// when every id is still available and the meta transaction simply did
    /// not land. Go surfaces the underlying storage failure instead, which is
    /// what the carried text is.
    AutoIdUnavailable(String),
    /// Go `ErrDupFieldName` (1060).
    DuplicateColumnName(String),
    /// Go `ErrDupKeyName` (1061).
    DuplicateKeyName(String),
    /// Go `ErrCantDropFieldOrKey` (1091), with the index-specific message.
    UnknownIndex(String),
    /// Go `ErrCantDropFieldOrKey` (1091).
    UnknownColumnInAlter(String),
    /// Go `ErrCantRemoveAllFields` (1090).
    CannotDropOnlyColumn {
        /// The column the statement named.
        column: String,
        /// The table it belongs to.
        table: String,
    },
    /// TiDB `ErrUnsupportedModifyColumn`-family (8200).
    UnsupportedDropIntegerPrimaryKey,
    /// Go `ErrFunctionsNoopImpl` (1235): a clause TiDB only implements as a
    /// no-op, refused unless `tidb_enable_noop_functions` allows it.
    FunctionsNoopImpl(&'static str),
    /// TiDB `ErrUnsupportedModifyColumn` (8200), carrying Go's reason text.
    UnsupportedModifyColumn(&'static str),
    /// Go `ErrBadField` (1054): the column is not in the table.
    UnknownColumnInTable {
        /// The column the statement named.
        column: String,
        /// The table it looked in.
        table: String,
    },
    /// Go `ErrBlobKeyWithoutLength` (1170).
    BlobKeyWithoutLength(String),
    /// Go `ErrWrongSubKey` / `dbterror.ErrIncorrectPrefixKey` (1089): an
    /// index key part declared a length on a type that cannot carry one, or
    /// one longer than the column. Go's message names neither.
    IncorrectPrefixKey,
    /// Go `ErrKeyPart0` (1391): an index key part declared a zero length.
    KeyPart0(String),
    /// Go `ErrTooLongKey` (1071): an index key part is longer than
    /// `MaxIndexLength`. Both numbers are BYTES.
    TooLongKey {
        /// The key part's length in bytes, already multiplied by the
        /// charset's maximum bytes per character.
        length: i64,
        /// The maximum a key part may reach.
        max: i64,
    },
    /// Go `ErrJSONUsedAsKey` (3152): a JSON column in an index.
    JsonUsedInKey(String),
    /// Go `ErrBlobCantHaveDefault` (1101): a JSON column's default.
    BlobCantHaveDefault(String),
    /// Go `ErrTruncatedWrongValue` (1292).
    TruncatedIncorrectValue {
        /// The numeric domain Go names.
        kind: &'static str,
        /// The value it could not read.
        value: String,
    },
    /// Go `ErrTruncatedWrongValueForField` (1265), value form.
    DataTruncatedValue {
        /// The column being modified.
        column: String,
        /// The value that does not fit.
        value: String,
    },
    /// Go `exeerrors.ErrSavepointNotExists` (`ErrSpDoesNotExist`, 1305):
    /// `ROLLBACK TO` or `RELEASE` named a savepoint the transaction does not
    /// hold. Carries the name AS WRITTEN -- Go matches savepoint names
    /// case-insensitively but reports back the spelling the statement used.
    SavepointNotExists(String),
    /// Go `admin.ErrAdminCheckTable` (8003): `ADMIN CHECK` found a table
    /// whose row count is not one of its indexes' entry counts. Carries Go's
    /// already-formatted detail.
    AdminCheckTable(String),
    /// Go `consistency.ErrAdminCheckInconsistent` (`ErrDataInconsistent`,
    /// 8223): `ADMIN CHECK` found a row and an index entry that disagree.
    /// Carries Go's already-formatted detail.
    DataInconsistent(String),
    /// Go `plannererrors.ErrWrongParamCount` (8112): the number of values an
    /// `EXECUTE` supplies is not the number of `?` markers the prepared
    /// statement carries. Raised by `planCachePreprocess`'s step 1, which both
    /// the SQL-level `EXECUTE` and the binary protocol reach.
    WrongParamCount,
    /// Go `plannererrors.ErrPreparedStmtNotFound` (8111): an `EXECUTE`,
    /// `DEALLOCATE PREPARE` or `DROP PREPARE` named a statement this session
    /// does not hold. Names are matched EXACTLY -- TiDB keys
    /// `PreparedStmtNameToID` by the spelling `PREPARE` used, so `MyStmt` and
    /// `mystmt` are different statements (captured).
    PreparedStmtNotFound,
    /// Go `exeerrors.ErrPrepareMulti` (8115): the text a `PREPARE` was given
    /// parsed into more than one statement.
    PrepareMulti,
    /// Go `plannererrors.ErrUnsupportedPs` (1295): a statement kind that may
    /// not be prepared at all. `GeneratePlanCacheStmtWithAST` lists them --
    /// `IMPORT INTO`, `LOAD DATA`, `PREPARE`, `EXECUTE`, `DEALLOCATE`, a
    /// non-transactional DML, and a `SELECT ... INTO OUTFILE`.
    UnsupportedPreparedStatement,
    /// Go `plannererrors.ErrWrongArguments` (1210), carrying the function
    /// name the arguments were wrong for (`ntile`).
    WrongArguments(&'static str),
    /// Go `plannererrors.ErrWindowInvalidWindowFuncUse` (3593): a window
    /// function written outside the select list / `ORDER BY`, carrying its
    /// lowercased name.
    WindowInvalidWindowFuncUse(String),
    /// Go `plannererrors.ErrWindowNoSuchWindow` (3579): an `OVER` clause named
    /// a window the `WINDOW` clause does not define.
    WindowNoSuchWindow(String),
    /// Go `plannererrors.ErrWindowCircularityInWindowGraph` (3580): a named
    /// window's `base` chain loops back on itself.
    WindowCircularity,
    /// Go `plannererrors.ErrWindowNoChildPartitioning` (3581): a window that
    /// extends another defined its own `PARTITION BY`.
    WindowNoChildPartitioning,
    /// Go `plannererrors.ErrWindowNoRedefineOrderBy` (3583): a window that
    /// extends another added an `ORDER BY` the base already has, carrying the
    /// extending window's own name (`<unnamed window>` for an inline `OVER
    /// (w ORDER BY ...)`) and the base's.
    WindowNoRedefineOrderBy {
        /// The window doing the extending, as Go reports it.
        window: String,
        /// The base window it may not inherit from.
        base: String,
    },
    /// Go `plannererrors.ErrWindowNoInheritFrame` (3582): a named window that
    /// defines a frame may not be referenced by another window, carrying its
    /// name.
    WindowNoInheritFrame(String),
    /// Go `plannererrors.ErrNotSupportedYet` (1235) as the window builder
    /// raises it, carrying the feature text Go names.
    NotSupportedYet(&'static str),
    /// Go `plannererrors.ErrWindowFrameStartIllegal` / `ErrWindowFrameIllegal`
    /// (3586): a frame bound whose offset is negative, NULL or non-integral,
    /// or a `start` bound that ranks AFTER its `end` bound.
    WindowFrameIllegal,
    /// Go `plannererrors.ErrWindowRangeFrameOrderType` (3587): a `RANGE` frame
    /// with an `N PRECEDING`/`N FOLLOWING` bound needs exactly one `ORDER BY`
    /// expression of numeric or temporal type.
    WindowRangeFrameOrderType,
    /// Go `plannererrors.ErrWindowRangeFrameTemporalType` (3588): a temporal
    /// `ORDER BY` key accepts only an `INTERVAL` bound value.
    WindowRangeFrameTemporalType,
    /// Go `plannererrors.ErrWindowRangeFrameNumericType` (3589): a numeric
    /// `ORDER BY` key rejects an `INTERVAL` bound value.
    WindowRangeFrameNumericType,
    /// Go `plannererrors.ErrIllegalReference` (1247): a `GROUP BY` item names
    /// a select-list alias whose expression has no value yet at grouping time
    /// -- an aggregate or a window function.
    IllegalReference {
        /// The alias as written.
        name: String,
        /// Go's parenthesized reason, for example
        /// `reference to group function`.
        reason: &'static str,
    },
    /// Go `plannererrors.ErrAmbiguous` (1052) naming the clause it was
    /// written in: a `NATURAL`/`USING` join raises it from
    /// `coalesceCommonColumns` when one side offers a common name twice, so
    /// there is no single column to coalesce.
    AmbiguousColumnInClause {
        /// The name as Go quotes it, which is lowercased there.
        column: String,
        /// The clause Go names, for example `from clause`.
        clause: String,
    },
    /// Go `ErrUnknownColumn` (1054) naming the clause it was written in.
    UnknownColumnInClause {
        /// The name as written.
        column: String,
        /// The clause Go names, for example `order clause`.
        clause: String,
    },
    /// Go `plannererrors.ErrBadGeneratedColumn` (3105): an `INSERT`/`UPDATE`
    /// assigned a value to a generated column. Only `DEFAULT` is permitted
    /// there, and it means "leave it to the expression".
    BadGeneratedColumn {
        /// The generated column that was written to.
        column: String,
        /// The table it belongs to.
        table: String,
    },
    /// Go `dbterror.ErrGeneratedColumnNonPrior` (3107).
    GeneratedColumnNonPrior,
    /// Go `dbterror.ErrFunctionalIndexOnField` (3762): an expression index
    /// whose expression is nothing but a column, which is a plain index
    /// written the long way. See [`crate::expression_index`].
    FunctionalIndexOnField,
    /// Go `dbterror.ErrFunctionalIndexFunctionIsNotAllowed` (3758), carrying
    /// the index name: the expression names a subquery, `values(x)`, or a
    /// variable.
    FunctionalIndexFunctionNotAllowed(String),
    /// Go `dbterror.ErrFunctionalIndexRowValueIsNotAllowed` (3800), carrying
    /// the index name.
    FunctionalIndexRowValue(String),
    /// Go `dbterror.ErrExpressionIndexCanNotRefer` (3754), carrying the index
    /// name: the expression reads an `AUTO_INCREMENT` column.
    ExpressionIndexCanNotRefer(String),
    /// Go `dbterror.ErrUnsupportedExpressionIndex` (8200): the expression
    /// calls a function outside `GAFunction4ExpressionIndex` and the server
    /// was not started with `allow-expression-index`.
    UnsafeFunctionInExpressionIndex,
    /// Go `dbterror.ErrDependentByFunctionalIndex` (3837), carrying the
    /// column name: a column an expression index reads cannot be dropped or
    /// renamed.
    DependentByFunctionalIndex(String),
    /// Go `dbterror.ErrTooLongIdent` (1059), carrying the identifier Go names.
    TooLongIdent(String),
    /// Go `dbterror.ErrWrongExprInPartitionFunc` (1486): the partition
    /// expression reads no column at all. See
    /// [`crate::ddl::table_partition`].
    PartitionWrongExprInFunc,
    /// Go `dbterror.ErrPartitionFuncNotAllowed` (1491): the partition
    /// expression does not evaluate to an integer. Reported against the
    /// CLAUSE, unlike the bare-column case, which is 1659 against the column.
    PartitionFuncWrongType,
    /// Go `dbterror.ErrTooManyPartitions` (1499).
    PartitionTooMany,
    /// Go `ast.ErrSubpartition` (1500): `SUBPARTITION BY` under a method
    /// that cannot carry it.
    PartitionSubpartition,
    /// Go `dbterror.ErrUniqueKeyNeedAllFieldsInPf` (1503), carrying the kind
    /// of key Go names (`CLUSTERED INDEX`).
    PartitionUniqueKeyNeedAllFields(String),
    /// Go `ast.ErrNoParts` (1504), carrying the noun Go counts
    /// (`partitions`): `PARTITIONS 0`.
    PartitionNoParts(&'static str),
    /// Go `dbterror.ErrSameNamePartition` (1517), carrying the repeated name.
    PartitionSameName(String),
    /// Go `dbterror.ErrPartitionFunctionIsNotAllowed` (1564): the partition
    /// expression calls something outside Go's whitelist.
    PartitionFunctionNotAllowed,
    /// Go `dbterror.ErrNotAllowedTypeInPartition` (1659), carrying the column
    /// whose type the partition expression may not read.
    PartitionFieldTypeNotAllowed(String),
    /// Go `dbterror.ErrGlobalIndexNotExplicitlySet` (8264), carrying the
    /// index name: a unique index that does not include every partitioning
    /// column, without `GLOBAL`.
    PartitionGlobalIndexNeeded(String),
    /// Go `ast.ErrPartitionWrongValues` (1480), whose argument names the
    /// method that OWNS the value clause the definition used: `VALUES LESS
    /// THAN` outside RANGE, or `VALUES IN` outside LIST.
    PartitionWrongValues {
        /// The owning method (`RANGE` or `LIST`).
        method: &'static str,
        /// The clause spelling (`VALUES LESS THAN` or `VALUES IN`).
        clause: &'static str,
    },
    /// Go `ast.ErrPartitionMaxvalue` (1481): a `MAXVALUE` bound on any
    /// partition but the last.
    PartitionMaxValueNotLast,
    /// Go `dbterror.ErrRangeNotIncreasing` (1493): `VALUES LESS THAN` bounds
    /// that do not strictly increase.
    PartitionRangeNotIncreasing,
    /// Go `ast.ErrPartitionsMustBeDefined` (1492), carrying the method: a
    /// RANGE or LIST table with no partition definitions at all.
    PartitionsMustBeDefined(&'static str),
    /// Go `dbterror.ErrPartitionConstDomain` (1563): a bound outside the
    /// partition function's domain, which is a negative bound under an
    /// unsigned expression.
    PartitionConstDomain,
    /// Go `dbterror.ErrValuesIsNotIntType` (1697), carrying the partition
    /// name: a `VALUES` bound that is not an integer.
    PartitionValuesNotInt(String),
    /// Go `table.ErrNoPartitionForGivenValue` (1526), carrying the value as
    /// Go renders it: a row no partition accepts.
    NoPartitionForValue(String),
    /// Go `table.ErrUnknownPartition` (1735), carrying the name and the
    /// table: `PARTITION (p)` naming a partition the table does not have.
    UnknownPartition {
        /// The name as written.
        partition: String,
        /// The table it was written against.
        table: String,
    },
    /// Go `dbterror.ErrUnsupportedOnGeneratedColumn` (3106), whose argument
    /// names what was attempted.
    UnsupportedOnGeneratedColumn(String),
    /// Go `dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed` (3770): a
    /// column `DEFAULT` names a function that is not on Go's whitelist for
    /// defaults, carried as `(column, function)`.
    DefaultFunctionNotAllowed(String, String),
    /// Go `plannererrors.ErrUnknownTable` (1109): a multi-table `DELETE`
    /// names a target the `FROM`/`USING` clause does not provide -- which
    /// includes naming an aliased source by its stored table name.
    UnknownTableInMultiDelete(String),
    /// Go `plannererrors.ErrWrongGroupField` (1056): a `GROUP BY` position
    /// resolves to an aggregate or window-function select field, which
    /// cannot itself be grouped on.
    WrongGroupField(String),
    /// Go `plannererrors.ErrFieldNotInGroupBy` (1055): under
    /// `ONLY_FULL_GROUP_BY`, an expression reports a column that `GROUP BY`
    /// neither pins nor functionally determines.
    FieldNotInGroupBy {
        /// The offending expression's 1-based position in its clause.
        position: usize,
        /// The clause, as Go names it: `SELECT list` or `ORDER BY`.
        clause: &'static str,
        /// The column, qualified as Go qualifies it (`db.tbl.col`).
        column: String,
    },
    /// Go `plannererrors.ErrMixOfGroupFuncAndFieldsIncompatible` (8123): the
    /// same rule for a query that aggregates with no `GROUP BY` at all, where
    /// every row collapses into one and so no bare column has a value.
    FieldNotInAggregatedQuery {
        /// The offending select field's 1-based position.
        position: usize,
        /// The column, qualified as Go qualifies it.
        column: String,
    },
    /// Go `plannererrors.ErrAggregateOrderNonAggQuery` (3029): an `ORDER BY`
    /// aggregate over a query whose select list reads a bare column, so the
    /// aggregate would apply to a result that was never aggregated. Go reports
    /// it BEFORE 8123 and regardless of whether the select list aggregates at
    /// all.
    AggregateOrderNonAggQuery {
        /// The offending `ORDER BY` item's 1-based position.
        position: usize,
    },
    /// Go `plannererrors.ErrFieldInOrderNotSelect` (3065): a `SELECT DISTINCT`
    /// orders by an expression reading a column the select list does not
    /// report, so the row the order would pick is not one the result has.
    FieldInOrderNotSelect {
        /// The offending `ORDER BY` item's 1-based position.
        position: usize,
        /// The column, qualified as Go qualifies it (`db.tbl.col`).
        column: String,
    },
    /// Go `plannererrors.ErrAggregateInOrderNotSelect` (3066): the same rule
    /// where the `ORDER BY` item is an aggregate call the select list does not
    /// contain.
    AggregateInOrderNotSelect {
        /// The offending `ORDER BY` item's 1-based position.
        position: usize,
    },
    /// Go `types.ErrInvalidDefault` (1067).
    InvalidDefault(String),
    /// Go `dbterror.ErrPrimaryCantHaveNull` (1171): a `PRIMARY KEY` column
    /// was given `DEFAULT NULL`. Go's `checkDefaultValue` tests this BEFORE
    /// the plain NOT NULL arm, so a primary key -- which is implicitly NOT
    /// NULL -- reports 1171 and not 1067.
    PrimaryCantHaveNull,
    /// Go `ErrDataTooLong` (1406).
    DataTooLong {
        /// The column written.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// Go `ErrWarnDataOutOfRange` (1264).
    DataOutOfRange {
        /// The column written.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// Go `types.ErrOverflow` (1690) as `types.overflow` phrases it:
    /// a value that does not fit the target integer type.
    ConstantOverflows {
        /// The rejected value, printed in the target's own domain.
        value: String,
        /// The type's name, as Go `types.TypeStr` prints it.
        type_name: String,
    },
    /// Go `table.ErrTruncatedWrongValueForField` (1366).
    IncorrectValue {
        /// The column type's name, as Go `types.TypeStr` prints it.
        type_name: String,
        /// The rejected value.
        value: String,
        /// The column written.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// Go `types.ErrWrongValue` (1292) as the write path reports it: the
    /// SAME message as [`Self::IncorrectValue`] under a different code.
    ///
    /// A bad temporal value is 1292, not 1366, because Go raises it from
    /// `handleZeroDatetime` with `types.ErrWrongValue` -- which is declared
    /// against `mysql.ErrTruncatedWrongValue` -- before the generic
    /// column-cast error can be reached. `completeInsertErr` then appends the
    /// column and row, which is why the two texts coincide.
    IncorrectTemporalValue {
        /// The column type's name, as Go `types.TypeStr` prints it.
        type_name: String,
        /// The rejected value.
        value: String,
        /// The column written.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// Go `ErrTruncatedWrongValueForField` (1265), row form.
    DataTruncatedAtRow {
        /// The column being modified.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// TiDB 8200: the column is covered by a composite index.
    CannotDropColumnWithCompositeIndex(String),
    /// Go `ErrWrongNumberOfColumnsInSelect` (1222).
    WrongNumberOfColumnsInSelect,
    /// Go `ErrWrongAutoKey` (1075): more than one auto column.
    WrongAutoKey,
    /// Go `ErrWrongFieldSpec` (1063): AUTO_INCREMENT on a non-integer column.
    WrongColumnSpecifier(String),
    /// Go `ErrWrongColumnName` (1166): a column name the server reserves, of
    /// which `_tidb_rowid` is the one an `ALTER TABLE ... RENAME COLUMN` can
    /// reach.
    WrongColumnName(String),
    /// Go `dbterror.ErrPKIndexCantBeInvisible` (3522): the index a statement
    /// tried to hide from the planner is the table's primary key, explicit or
    /// implicit.
    PrimaryKeyCantBeInvisible,
    /// Go `infoschema.ErrKeyNotExists` (1176): the statement named an index
    /// the table does not have. This is 1091's sibling and NOT the same
    /// error: `DROP INDEX` on a missing key is 1091, while `RENAME INDEX`,
    /// `ALTER INDEX` and `USE INDEX` on one are 1176.
    KeyNotExists {
        /// The index the statement named, in its written spelling.
        key: String,
        /// The table it looked in.
        table: String,
    },
    /// Go `ErrColumnCantNull` (1048).
    ColumnCannotBeNull(String),
    /// Go `ErrNoDefaultForField` (1364).
    NoDefaultForField(String),
    /// Go `dbterror.ErrWrongFkDef` (1239): the constraint itself is
    /// malformed, which DDL reports before any row is looked at.
    WrongFkDef {
        /// The constraint name as written, empty when it was unnamed.
        name: String,
        /// Go's `%s` reason clause.
        reason: String,
    },
    /// Go `ErrNoReferencedRow2` (1452): a child-side `INSERT`/`UPDATE` named
    /// a parent row that does not exist.
    ForeignKeyNoReferencedRow {
        /// The referencing schema and table.
        table: String,
        /// The constraint as `SHOW CREATE TABLE` would print it.
        constraint: String,
    },
    /// Go `ErrRowIsReferenced2` (1451): a parent-side `DELETE`/`UPDATE` would
    /// have orphaned a referencing row, and the constraint restricts it.
    ForeignKeyRowIsReferenced {
        /// The referencing schema and table.
        table: String,
        /// The constraint as `SHOW CREATE TABLE` would print it.
        constraint: String,
    },
    /// Go `ErrFkExceedMaxDepth` (3008): a cascade recursed deeper than
    /// MySQL's 15 levels.
    ForeignKeyCascadeTooDeep,
    /// Go `infoschema.ErrForeignKeyCannotUseVirtualColumn` (3733): a
    /// constraint names a VIRTUAL generated column on either side. A virtual
    /// column has no stored value to index or to compare a key against, so
    /// InnoDB refuses it outright.
    ForeignKeyUsesVirtualColumn {
        /// The constraint name, as written.
        foreign_key: String,
        /// The offending column.
        column: String,
    },
    /// Go `dbterror.ErrWrongFKOptionForGeneratedColumn` (3104): a referential
    /// action would WRITE a child column whose value the table computes.
    WrongFkOptionForGeneratedColumn {
        /// The clause as Go spells it back: `ON UPDATE CASCADE`,
        /// `ON DELETE SET NULL`, ...
        clause: String,
    },
    /// Go `dbterror.ErrDropIndexNeededInForeignKey` (1553).
    DropIndexNeededInForeignKey(String),
    /// Go `dbterror.ErrFkDupName` (1826): `ALTER TABLE ... ADD FOREIGN KEY`
    /// named a constraint the table already declares. Checked before the
    /// reference resolves, so it fires with `foreign_key_checks` at 0 too.
    FkDupName(String),
    /// Go `ErrDupEntry` (1062).
    DuplicateEntry {
        /// The rejected key value.
        value: String,
        /// The violated key's name.
        key: String,
    },
    /// Go `ER_SUBQUERY_NO_1_ROW` (1242): a scalar subquery produced more than
    /// one row.
    SubqueryReturnsMoreThanOneRow,
    /// Go `exeerrors.ErrMemoryExceedForQuery` (8175): the statement exceeded
    /// `tidb_mem_quota_query` under `tidb_mem_oom_action = CANCEL`.
    MemoryExceedForQuery {
        /// The connection the message names.
        conn_id: u64,
    },
    /// Go `types.ErrJSONDocumentNULLKey` (3158): `JSON_OBJECTAGG` evaluated a
    /// NULL member name.
    JsonDocumentNullKey,
    /// Go `types.ErrInvalidJSONCharset` (3144): `JSON_OBJECTAGG` evaluated a
    /// BINARY-charset key.
    InvalidJsonCharset {
        /// The rejected key argument's charset name.
        charset: String,
    },
    /// Go `typeInfer4ApproxPercentile`'s plain errors (no error class, so
    /// 1105), carrying the message text Go writes.
    ApproxPercentileArgument(&'static str),
    /// Go `typeInfer4ApproxPercentile`: `Percentage value %d is out of range
    /// [1, 100]`, carrying the integer Go printed.
    PercentageOutOfRange(i64),
    /// Go `plannererrors.ErrInvalidGroupFuncUse` (1111): `GROUPING()` written
    /// in a query that has no `WITH ROLLUP`.
    InvalidGroupFuncUse,
    /// Go `plannererrors.ErrFieldInGroupingNotGroupBy` (3602): a `GROUPING()`
    /// argument is not one of the `GROUP BY` expressions. The number Go prints
    /// is the argument's 0-based position.
    FieldInGroupingNotGroupBy(usize),
    /// Go's plain `INSERT into view` refusal, which carries no error class:
    /// `insert into view %s is not supported now`.
    InsertIntoViewUnsupported(String),
    /// Go's plain `DELETE` refusal: `delete view %s is not supported now`.
    DeleteViewUnsupported(String),
    /// Go's plain `INSERT into sequence` refusal, the sequence twin of
    /// [`DriverError::InsertIntoViewUnsupported`]. Captured:
    /// `insert into s1 values (1)` reports
    /// `insert into sequence s1 is not supported now` with no error class.
    InsertIntoSequenceUnsupported(String),
    /// Go's plain `DELETE` refusal for a sequence. Captured:
    /// `delete from s1` reports `delete sequence s1 is not supported now`.
    DeleteSequenceUnsupported(String),
    /// Go `table.ErrSequenceHasRunOut` (4135): `NEXTVAL` has nothing left and
    /// the sequence does not `CYCLE`. The string is the qualified name.
    /// Captured: `Sequence 'test.s4' has run out`. NOTE this is a DIFFERENT
    /// error from the auto-increment allocator's 1467.
    SequenceHasRunOut(String),
    /// Go `plannererrors.ErrNonUpdatableTable` (1288), which is what an
    /// `UPDATE` through a view reports.
    TableNotUpdatable(String),
    /// Go `ErrViewWrongList` (1353): the `CREATE VIEW v (...)` column list
    /// and the body's select list have different widths. A derived table's
    /// own `(c1, c2)` alias column list reports the SAME error when it does
    /// not match the subquery's width (captured).
    ViewWrongList,
    /// Go `plannererrors.ErrCTERecursiveRequiresUnion` (3573): a CTE inside a
    /// `WITH RECURSIVE` clause names itself but its body is a bare `SELECT`
    /// with no `UNION` to separate a seed from a recursive block (captured).
    CteRecursiveRequiresUnion(String),
    /// Go `plannererrors.ErrCTERecursiveRequiresNonRecursiveFirst` (3574): the
    /// self-referencing blocks are not a trailing run -- the first block names
    /// the CTE, or a non-recursive block follows a recursive one (captured).
    CteRecursiveRequiresNonRecursiveFirst(String),
    /// Go `plannererrors.ErrCTERecursiveForbidsAggregation` (3575): a recursive
    /// block aggregates (`GROUP BY` or an aggregate/window call) (captured).
    CteRecursiveForbidsAggregation(String),
    /// Go `plannererrors.ErrCTERecursiveForbiddenJoinOrder` (3577): a recursive
    /// block names the CTE other than exactly once as a plain `FROM` table --
    /// twice (a self-join), zero times after an earlier block named it, or from
    /// inside a derived table or scalar subquery (captured).
    CteRecursiveForbiddenJoinOrder(String),
    /// Go `exeerrors.ErrCTEMaxRecursionDepth` (3636): the fixpoint ran one more
    /// round than `@@cte_max_recursion_depth` allows. The payload is the round
    /// count Go reports, which is the limit PLUS ONE -- the round it refused to
    /// run (captured: a limit of `3` reports `4`, the default `1000` reports
    /// `1001`).
    CteMaxRecursionDepth(u64),
    /// Go `plannererrors.ErrInvalidLateralJoin` (3809): a `LATERAL` derived
    /// table in a join shape `buildLateralJoin` refuses. The payload is Go's
    /// own reason text, which the message interpolates.
    InvalidLateralJoin(&'static str),
    /// Go `plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or
    /// CONNECTION_ADMIN")` (1227): `KILL` of a connection logged in as a
    /// DIFFERENT user than the caller, without SUPER (or the dynamic
    /// CONNECTION_ADMIN privilege, not modelled in this tier). Killing one's
    /// own connection is always allowed regardless of privilege.
    KillAccessDenied,
    /// Go `ErrDerivedMustHaveAlias` (1248): a derived table was written
    /// without an alias. Captured from Go for both a plain `SELECT` and a
    /// view body.
    DerivedMustHaveAlias,
    /// Go `ErrCannotUser` (1396): `CREATE USER` named an account that
    /// already exists. Go quotes the account as `'user'@'host'`.
    CreateUserAlreadyExists {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
    },
    /// Go `ErrCannotUser` (1396): `DROP USER` named one or more accounts that
    /// do not exist. Go collects every missing account across the statement,
    /// rolls back (nothing is dropped), and reports them comma-joined,
    /// unquoted `user@host` each (`auth.UserIdentity.String`,
    /// `strings.Join(failedUsers, ",")`).
    DropUserMissing {
        /// The missing accounts, already formatted and comma-joined.
        accounts: String,
    },
    /// Go `ErrCannotUser` (1396): `ALTER USER ... IDENTIFIED BY` named an
    /// account that does not exist, without `IF EXISTS`. Quoted
    /// `'user'@'host'`, like CREATE USER's form and unlike DROP USER's.
    AlterUserMissing {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
    },
    /// Go `ErrWrongValue2` (1525): `PASSWORD EXPIRE INTERVAL n DAY` was
    /// written with a day count outside `1 ..= 65535`, which `loadOptions`
    /// rejects before any row is touched.
    PasswordExpireIntervalOutOfRange {
        /// The rejected day count, printed as written.
        days: i64,
    },
    /// Go `ErrMustChangePassword` (1820): the session logged in with an
    /// expired password (sandbox mode) and ran something other than the
    /// `SET PASSWORD` / `ALTER USER` it is allowed to run.
    MustChangePassword,
    /// Go `ErrCannotUser` (1396) for `RENAME USER`, whose message carries a
    /// trailing reason clause rather than just the account
    /// (captured: `... failed for nosuch@% TO x@% old did not exist`, and
    /// `... new did exist` when the target identity is taken).
    RenameUserFailed {
        /// The source account username.
        old_user: String,
        /// The source account host.
        old_host: String,
        /// The target account username.
        new_user: String,
        /// The target account host.
        new_host: String,
        /// Whether the SOURCE account was missing (as opposed to the target
        /// identity already existing), which selects the reason clause.
        old_missing: bool,
    },
    /// Go `ErrPasswordNoMatch` (1133): `SET PASSWORD FOR` named an account
    /// with no `mysql.user` row. `SET PASSWORD` does NOT reuse
    /// `ErrCannotUser` (captured).
    SetPasswordNoMatchingRow,
    /// Go `ErrPluginIsNotLoaded` (1524): `CREATE`/`ALTER USER ... IDENTIFIED
    /// WITH <plugin>` named a plugin that is neither one of Go's built-in
    /// `CREATE USER`-accepted plugins (`mysql_native_password`,
    /// `caching_sha2_password`, `tidb_sm3_password`, `auth_socket`,
    /// `tidb_auth_token`, `authentication_ldap_simple`,
    /// `authentication_ldap_sasl`) nor a registered extension auth plugin
    /// (which this tier has none of).
    PluginIsNotLoaded {
        /// The unrecognized plugin name, as written.
        plugin: String,
    },
    /// Go `ErrPasswordFormat` (1827): an `IDENTIFIED WITH <plugin> AS
    /// '<hash>'` credential is not shaped like that plugin's stored
    /// `authentication_string` -- captured: `mysql_native_password` needs
    /// exactly `*` + 40 hex digits, `caching_sha2_password`/
    /// `tidb_sm3_password` need exactly 70 bytes, and `tidb_auth_token`'s
    /// `AS` form is refused outright (Go's `encodedPassword` has no case for
    /// it, so it falls to the same `default: return "", false`).
    PasswordFormat,
    /// Go's plain `errors.Errorf("Unknown user: %s", user)` (`REVOKE` on an
    /// account that does not exist), unquoted `user@host`.
    RevokeUnknownUser {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
    },
    /// Go `ErrCantCreateUserWithGrant` (1410): `GRANT` named an account that
    /// does not exist and TiDB refuses to implicitly create one.
    GrantToUnknownUser,
    /// Go `ErrCannotUser` (1396) raised by a ROLE statement. Each of them
    /// names its own operation and formats the offending identity the way
    /// that statement's Go code does, so both travel together rather than
    /// being guessed at render time (all captured):
    /// `CREATE ROLE` quotes `'r'@'h'`, `DROP ROLE`/`GRANT ROLE`/`REVOKE ROLE`
    /// print an account bare as `u@h`, and `REVOKE ROLE` prints a missing
    /// ROLE backtick-quoted as ``\`r\`@\`h\`` (`auth.RoleIdentity.String`).
    CannotUserRole {
        /// The operation name the message reports, e.g. `CREATE ROLE`.
        operation: &'static str,
        /// The already-formatted identity (or comma-joined identities).
        target: String,
    },
    /// Go `ErrGrantRole` (3523): `GRANT <role> TO ...` named a role that has
    /// no account row at all.
    GrantUnknownRole {
        /// The role name.
        role: String,
        /// The role host.
        host: String,
    },
    /// Go `ErrRoleNotGranted` (3530): `SET ROLE`/`SET DEFAULT ROLE` named a
    /// role that is not granted DIRECTLY to the account. A role held only
    /// indirectly (granted to a role the account holds) lands here too --
    /// activation never walks the graph.
    RoleNotGranted {
        /// The role name.
        role: String,
        /// The role host.
        role_host: String,
        /// The account username.
        user: String,
        /// The account host.
        host: String,
    },
    /// Go `ErrDynamicPrivilegeNotRegistered` (3929): a `GRANT`/`REVOKE`
    /// privilege name is not one of the standard static privileges and is
    /// not a registered dynamic privilege either.
    DynamicPrivilegeNotRegistered(String),
    /// Go `exeerrors.ErrIllegalPrivilegeLevel` (3619): a DYNAMIC privilege
    /// was named at DATABASE or TABLE scope, which Go rejects before it
    /// checks whether the privilege is registered at all. `GRANT` names the
    /// offending privilege; `REVOKE` names every dynamic privilege in the
    /// statement, comma-joined.
    IllegalPrivilegeLevel(String),
    /// Go `ErrNonexistingGrant` (1141): `SHOW GRANTS FOR` an account with no
    /// grant row at all (also raised for an account that does not exist).
    NonexistingGrant {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
    },
    /// Go `ErrWrongUsage.GenWithStackByArgs("DB GRANT", "GLOBAL PRIVILEGES")`
    /// (1221): a DB-scope `GRANT`/`REVOKE` named a global-only privilege
    /// (`PROCESS`, `SUPER`, ...).
    DbGrantGlobalOnlyPriv,
    /// Go `ErrIllegalGrantForTable` (1144): a TABLE-scope `GRANT`/`REVOKE`
    /// named a privilege outside `mysql.AllTablePrivs`.
    IllegalGrantForTable,
    /// Go `ErrWrongUsage.GenWithStackByArgs("COLUMN GRANT", "NON-COLUMN
    /// PRIVILEGES")` (1221): a privilege carrying a column list is not one of
    /// `mysql.AllColumnPrivs`.
    ColumnGrantNonColumnPriv,
    /// Go's plain `errors.Errorf("Unknown column: %s", ...)` in
    /// `checkAndInitColumnPriv`: a `GRANT` named a column the table does not
    /// have. Carries the column name as written.
    UnknownGrantColumn(String),
    /// Go's plain `errors.Errorf("There is no such grant defined for user
    /// '%s' on host '%s' on database %s", ...)`: `REVOKE ... ON db.*` for an
    /// account with no `mysql.DB` row for that database at all.
    RevokeNoDbGrant {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
        /// The database named in the `REVOKE`, as written.
        database: String,
    },
    /// Go's plain `errors.Errorf("There is no such grant defined for user
    /// '%s' on host '%s' on table %s.%s", ...)`: `REVOKE ... ON db.t` for an
    /// account with no `mysql.Tables_priv` row for that table at all.
    RevokeNoTableGrant {
        /// The account username.
        user: String,
        /// The account host.
        host: String,
        /// The database named in the `REVOKE`, as written.
        database: String,
        /// The table named in the `REVOKE`, as written.
        table: String,
    },
}
