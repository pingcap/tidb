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

use std::borrow::Cow;

use super::{SchemaErrorKind, TxnErrorKind, VarErrorKind};
use crate::executor::ExecError;

/// A failure while running a SQL string through the driver.
#[derive(Debug, Clone)]
pub enum DriverError {
    /// The SQL failed to parse.
    Parse(String),
    /// A grammar-action refusal that carries its own errno, the way Go's
    /// parser raises `ast.ErrNoParts` as `[ddl:1504]` rather than yacc's
    /// 1064.
    ParseCoded {
        /// The classed errno the grammar action names.
        errno: u16,
        /// Go's message, verbatim.
        message: String,
    },
    /// The general refusal: this tier does not implement what the statement
    /// asked for, and the carried text says which part.
    ///
    /// It is raised from every layer -- an `EXPLAIN` format name, a catalog
    /// object kind, an unported `ALTER TABLE` action, an operator with no
    /// executor -- and is by far the most-constructed variant here. The
    /// doc it replaces said "not a supported `FROM`-less `SELECT`", which
    /// described the driver when a `FROM` clause was the frontier; it has not
    /// been true for any of the hundreds of sites since.
    ///
    /// Everything it refuses reaches the client as Go's catch-all 1105, so
    /// the carried text IS the whole diagnostic. Say what was refused, not
    /// that something was.
    ///
    /// [`Cow`] rather than `&'static str` because a refusal that names the
    /// AST kind it saw builds its text per call. That used to be a SECOND
    /// variant, `UnsupportedKind(String)`, which rendered byte for byte
    /// identically on adjacent lines of `to_mysql_error` -- nothing on the
    /// wire could tell the two apart, and the only difference was who owned
    /// the bytes. Build one with [`DriverError::unsupported`], which takes
    /// either.
    Unsupported(Cow<'static, str>),
    /// Go `dbterror.ErrTableOptionUnionUnsupported` (8232).
    TableOptionUnionUnsupported,
    /// Go `dbterror.ErrTableOptionInsertMethodUnsupported` (8233).
    TableOptionInsertMethodUnsupported,
    /// Go `dbterror.ErrUnknownEngine` (1286).
    UnknownStorageEngine(String),
    /// TiDB `ErrOptOnCacheTable` (8242), carrying the operation or reason.
    OperationOnCachedTable(&'static str),
    /// Go `dbterror.ErrUnsupportedAlterCacheForSysTable` (8200).
    UnsupportedAlterCacheForSystemTable,
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
    /// Go `autoid.ErrAutoRandReadFailed` (8059): the increasing portion no
    /// longer fits the table's persisted AUTO_RANDOM bit layout.
    AutoRandReadFailed,
    /// The AUTO_INCREMENT counter's home could not be read or written.
    ///
    /// Deliberately NOT `AutoincReadFailed`: Go raises 1467 only for a full
    /// domain, and answering it here would tell a user their ids had run out
    /// when every id is still available and the meta transaction simply did
    /// not land. Go surfaces the underlying storage failure instead, which is
    /// what the carried text is.
    AutoIdUnavailable(String),
    /// TiDB `ErrInvalidAutoRandom` (8216), carrying its reason text.
    InvalidAutoRandom(String),
    /// Go `ErrDupFieldName` (1060).
    DuplicateColumnName(String),
    /// Go `ErrDupKeyName` (1061).
    DuplicateKeyName(String),
    /// Go `ErrMultiplePriKey` (1068): more than one PRIMARY KEY declaration.
    MultiplePrimaryKey,
    /// Go `ErrTooBigPrecision` (1426), carrying the declared precision, the
    /// column and the type's maximum.
    TooBigPrecision {
        /// The precision the column declared.
        precision: i64,
        /// The column it was declared on.
        column: String,
        /// The type's own maximum.
        maximum: i64,
    },
    /// Go `ErrMBiggerThanD` (1427), carrying the column: `DECIMAL(M,D)` and
    /// the float family need `M >= D`.
    MBiggerThanD(String),
    /// Go `ErrDuplicatedValueInType` (1291), carrying the column, the
    /// repeated member and whether the type is ENUM or SET.
    DuplicatedValueInType {
        /// The column the type was declared on.
        column: String,
        /// The member that appears more than once.
        value: String,
        /// `ENUM` or `SET`, as Go spells it in the message.
        type_name: &'static str,
    },
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
    /// TiDB `ErrUnsupportedModifyColumn` (8200): Go `checkTypeChangeSupported`
    /// (`pkg/types/field_type.go:1569-1603`) refuses this ORIGIN/TARGET type
    /// pair outright, before any row is touched -- so it fires the same way
    /// on an empty table as on a populated one, unlike the per-row
    /// `convert_to` gate this refusal sits in front of.
    ///
    /// Go reaches this through `types.CheckModifyTypeCompatible`
    /// (`field_type.go:1515-1518`), then `pkg/ddl/modify_column.go`'s
    /// `checkModifyTypes` (`:2262-2273`) wraps that error's `.Error()` text in
    /// a SECOND `ErrUnsupportedModifyColumn`, so the real server's message is
    /// double-prefixed ("Unsupported modify column: Unsupported modify
    /// column: change from original type ... to ... is currently unsupported
    /// yet"). This variant renders the INNER (single) wrap only -- the
    /// double-wrap is Go's own accident of composition, not a text this tier
    /// promises to reproduce byte for byte; the errno and REFUSED-vs-ACCEPTED
    /// outcome are what the difftest oracle compares.
    UnsupportedModifyColumnType {
        /// `origin.CompactStr()`.
        from: String,
        /// `to.CompactStr()`.
        to: String,
    },
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
    /// Go `ErrWrongKeyColumn` (1167): the column cannot be indexed at all --
    /// a `NULL`-typed column, or a `char(0)`/`varchar(0)`/`binary(0)` whose
    /// declared width leaves nothing to key on.
    WrongKeyColumn(String),
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
        /// The domain Go names: a fixed word such as `DOUBLE` when the
        /// conversion raised the error itself, or the column's `CompactStr`
        /// (`decimal(4,1)`, `time`) when `castColumnValue` re-titled a bare
        /// `ErrTruncated`.
        kind: String,
        /// The value it could not read.
        value: String,
    },
    /// Go `ErrDataTooLong` (1406) as `ProduceStrWithSpecifiedTp` raises it,
    /// BEFORE any caller re-titles it with a column and a row. A write path
    /// that returns `table.CastValue`'s error unchanged -- an
    /// `ON DUPLICATE KEY UPDATE` assignment -- reports this form.
    DataTooLongRaw {
        /// The column's declared length.
        field_len: u64,
        /// The value's length in the same unit.
        data_len: u64,
    },
    /// Go `ErrTruncated` (1265) raised with NO arguments, which is what a
    /// failed ENUM/SET conversion returns.
    ///
    /// `castColumnValue` re-titles a bare `ErrTruncated` into
    /// `ErrTruncatedWrongVal` for every type EXCEPT SET and ENUM, so those
    /// two alone reach a caller still carrying the message TEMPLATE. A caller
    /// that formats it (`completeInsertErr`) fills in the column and the row;
    /// one that returns it unchanged prints the template verbatim, `'%s'` and
    /// `%d` included. That is TiDB's own answer, reproduced rather than
    /// tidied:
    ///
    /// ```text
    /// insert into k (id) values (1) on duplicate key update e='zz'
    ///   ERROR 1265 (01000): Data truncated for column '%s' at row %d
    /// ```
    DataTruncatedUnformatted,
    /// Go `types.ErrWrongValue` (1292) with NO column and row appended: the
    /// temporal form of the error a raw `table.CastValue` returns.
    IncorrectValueRaw {
        /// The column type's name, as Go `types.TypeStr` prints it.
        type_name: String,
        /// The rejected value.
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
    /// Go `ErrDataInconsistentMismatchIndex` (8134): an index entry names a
    /// row but an indexed column differs from that row.
    DataInconsistentMismatchIndex(String),
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
    /// Go `plannererrors.ErrWindowFrameIllegal` (3586): a frame bound whose
    /// offset is negative, NULL or non-integral, or a `start` bound that ranks
    /// AFTER its `end` bound.
    WindowFrameIllegal,
    /// Go `plannererrors.ErrWindowFrameStartIllegal` (3584): the frame's START
    /// is `UNBOUNDED FOLLOWING`, which is illegal whatever the end bound is.
    WindowFrameStartIllegal,
    /// Go `plannererrors.ErrWindowFrameEndIllegal` (3585): the frame's END is
    /// `UNBOUNDED PRECEDING`, which is illegal whatever the start bound is.
    WindowFrameEndIllegal,
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
    /// Go `dbterror.ErrGeneratedColumnFunctionIsNotAllowed` (3102).
    GeneratedColumnFunctionNotAllowed(String),
    /// Go `dbterror.ErrGeneratedColumnNonPrior` (3107).
    GeneratedColumnNonPrior,
    /// Go `dbterror.ErrWrongKeyColumnFunctionalIndex` (3761), carrying the
    /// restored expression text: `checkIndexColumn`'s first arm reached over
    /// a HIDDEN column -- the expression's result has no width to key on.
    WrongKeyColumnFunctionalIndex(String),
    /// Go `dbterror.ErrFunctionalIndexOnJSONOrGeometryFunction` (3753): an
    /// expression index whose result type is JSON. Go's message names neither
    /// the index nor the expression.
    FunctionalIndexOnJson,
    /// Go `dbterror.ErrFunctionalIndexOnBlob` (3757): an expression index
    /// whose result type is BLOB or TEXT.
    FunctionalIndexOnBlob,
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
    /// Go `expression.ErrIncorrectParameterCount` (1582), carrying the
    /// function name: the call passes a number of arguments the builtin's
    /// `baseFunctionClass{minArgs, maxArgs}` does not admit. Go raises it
    /// from `VerifyArgsWrapper` inside `illegalFunctionChecker`, so an
    /// expression index reports it BEFORE the 8200 GA gate.
    WrongParamCountToNativeFct(String),
    /// Go `dbterror.ErrDependentByFunctionalIndex` (3837), carrying the
    /// column name: a column an expression index reads cannot be dropped or
    /// renamed.
    DependentByFunctionalIndex(String),
    /// Go `dbterror.ErrDependentByGeneratedColumn` (3108), carrying the column
    /// name: a column a VISIBLE generated column reads cannot be dropped or
    /// renamed. The hidden-column sibling of this is
    /// [`Self::DependentByFunctionalIndex`], and which of the two a statement
    /// gets is decided by the first dependent column in offset order.
    DependentByGeneratedColumn(String),
    /// Go `dbterror.ErrDependentByPartitionFunctional` (3855), carrying the
    /// column name: a column the partition expression -- or the
    /// `PARTITION BY ... COLUMNS` list -- reads cannot be dropped or renamed.
    DependentByPartitionFunctional(String),
    /// Go `dbterror.ErrTooLongIdent` (1059), carrying the identifier Go names.
    TooLongIdent(String),
    /// Go `dbterror.ErrTooLongTableComment` (1628).
    TableCommentTooLong(String),
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
    /// Go `ErrPartitionMgmtOnNonpartitioned` (1505).
    PartitionManagementOnNonpartitioned,
    /// Go `dbterror.ErrDropPartitionNonExistent` (1507).
    PartitionDropNonexistent,
    /// Go `dbterror.ErrDropLastPartition` (1508).
    PartitionDropLast,
    /// Go `dbterror.ErrOnlyOnRangeListPartition` (1512).
    PartitionOnlyRangeList(&'static str),
    /// Go `dbterror.ErrUniqueKeyNeedAllFieldsInPf` (1503), carrying the kind
    /// of key Go names (`CLUSTERED INDEX`).
    PartitionUniqueKeyNeedAllFields(String),
    /// Go `ast.ErrNoParts` (1504), carrying the noun Go counts
    /// (`partitions`): `PARTITIONS 0`.
    PartitionNoParts(&'static str),
    /// Go's `getPartitionColSlices` fallthrough: `PARTITION BY KEY ()` on a
    /// table that HAS keys but none that can serve as the partitioning
    /// columns. Go raises it with a bare `errors.Errorf`, so it reaches the
    /// client as 1105.
    PartitionMetadataIncomplete,
    /// Go `table.ErrUnknownPartition` (1735) as `newPartitionedTable` raises
    /// it for metadata with no definitions (`tables/partition.go:115`).
    ///
    /// Go returns the BARE error there rather than
    /// `GenWithStackByArgs(...)`, so its two `%-.64s` placeholders reach the
    /// client unsubstituted. That is the message a real TiDB emits.
    PartitionMetadataUnknown,
    /// Go `ErrTooLongTablePartitionComment` (1629): a per-partition
    /// `COMMENT` past `MaxCommentLength`, raised only under STRICT mode.
    PartitionCommentTooLong {
        /// The partition whose comment is too long.
        name: String,
        /// Go `MaxCommentLength`.
        limit: usize,
    },
    /// Go `dbterror.ErrSameNamePartition` (1517), carrying the repeated name.
    PartitionSameName(String),
    /// Go `dbterror.ErrPartitionFunctionIsNotAllowed` (1564): the partition
    /// expression calls something outside Go's whitelist.
    PartitionFunctionNotAllowed,
    /// Go `dbterror.ErrNotAllowedTypeInPartition` (1659), carrying the column
    /// whose type the partition expression may not read.
    PartitionFieldTypeNotAllowed(String),
    /// Go `infoschema.ErrPlacementPolicyExists` (8238).
    PlacementPolicyExists(String),
    /// Go `infoschema.ErrPlacementPolicyNotExists` (8239).
    PlacementPolicyNotExists(String),
    /// Go `dbterror.ErrPlacementPolicyInUse` (8241): a policy cannot be
    /// dropped while a table or partition still names it.
    PlacementPolicyInUse(String),
    /// Go `dbterror.ErrFieldNotFoundPart` (1488): a name in a partitioning
    /// COLUMN LIST that no column answers to. The expression path reports the
    /// same mistake as 1054 instead, so the two cannot share a variant.
    PartitionFieldNotFound,
    /// Go `dbterror.ErrNullInValuesLessThan` (1566).
    PartitionNullInValuesLessThan,
    /// Go `dbterror.ErrSameNamePartitionField` (1652).
    PartitionDuplicateField(String),
    /// Go `dbterror.ErrWrongTypeColumnValue` (1654).
    PartitionColumnValueWrongType,
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
    /// Go `dbterror.ErrMultipleDefConstInListPart` (1495).
    PartitionDuplicateListValue,
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
    /// Go `table.ErrRowDoesNotMatchGivenPartitionSet` (1748): an INSERT row
    /// routes outside the partitions the statement selected.
    RowDoesNotMatchGivenPartitionSet,
    /// Go `dbterror.ErrUnsupportedOnGeneratedColumn` (3106), whose argument
    /// names what was attempted.
    UnsupportedOnGeneratedColumn(String),
    /// Go `dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed` (3770): a
    /// column `DEFAULT` names a function that is not on Go's whitelist for
    /// defaults, carried as `(column, function)`.
    DefaultFunctionNotAllowed(String, String),
    /// Go `dbterror.ErrColumnTypeUnsupportedNextValue` (8228), carrying the
    /// column name whose type cannot store a sequence default.
    UnsupportedSequenceDefaultType(String),
    /// Go `dbterror.ErrAddColumnWithSequenceAsDefault` (8230), carrying the
    /// added column name.
    AddColumnSequenceDefault(String),
    /// Go `dbterror.ErrBinlogUnsafeSystemFunction` (1674): ADD COLUMN would
    /// synthesize a node-local value for rows written before the column.
    BinlogUnsafeSystemFunction,
    /// Go `plannererrors.ErrUnknownTable` (1109): a multi-table `DELETE`
    /// names a target the `FROM`/`USING` clause does not provide -- which
    /// includes naming an aliased source by its stored table name.
    UnknownTableInMultiDelete(String),
    /// Go `plannererrors.ErrNonUpdatableTable` (1288): a write named a source
    /// that IS in the `FROM` but is not a base table -- a derived table, a
    /// CTE, a view or a sequence. Go decides this by ABSENCE from
    /// `updatableTableListResolver`'s list (`buildUpdateLists`'s
    /// `!foundListItem`) and from `collectTableName`'s `canUpdate` for
    /// `DELETE`. The second field is the statement Go names in the message,
    /// `UPDATE` or `DELETE`.
    NonUpdatableTable {
        /// The source as the statement named it: its alias once it has one.
        table: String,
        /// `UPDATE` or `DELETE`, as Go's message spells it.
        statement: &'static str,
    },
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
    /// Go `table.errGetDefaultFailed` / `ErrFieldGetDefaultFailed` (8038):
    /// persisted default metadata could not be parsed as this column's type.
    FieldGetDefaultFailed(String),
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
    /// Go `ErrWrongUsage` (1221): two SQL constructs were combined in a
    /// grammar position where TiDB refuses their interaction.
    WrongUsage {
        /// The outer construct named first in the diagnostic.
        first: &'static str,
        /// The conflicting construct named second in the diagnostic.
        second: &'static str,
    },
    /// Go `ErrWrongAutoKey` (1075): more than one auto column.
    WrongAutoKey,
    /// Go `ErrWrongFieldSpec` (1063): AUTO_INCREMENT on a non-integer column.
    WrongColumnSpecifier(String),
    /// Go `dbterror.ErrInvalidOnUpdate` (1294).
    InvalidOnUpdate(String),
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
    /// Go `dbterror.ErrFKIncompatibleColumns` (3780): a `MODIFY`/`CHANGE`
    /// moved a constrained column onto a type the column on the other side of
    /// the constraint does not share.
    FkIncompatibleColumns {
        /// The REFERENCING column's name.
        referencing: String,
        /// The REFERENCED column's name.
        referenced: String,
        /// The constraint's name.
        constraint: String,
    },
    /// Go `dbterror.ErrForeignKeyColumnCannotChange` (1832): the type is
    /// still compatible, but the change is not one a constraint survives --
    /// a narrowed non-integer width, or any decimal precision/scale move.
    ForeignKeyColumnCannotChange {
        /// The column named by the `MODIFY`, under its OLD name.
        column: String,
        /// The constraint this table declares over it.
        constraint: String,
    },
    /// Go `dbterror.ErrForeignKeyColumnCannotChangeChild` (1833): the same
    /// refusal reached from the PARENT side, where the constraint lives in
    /// another table.
    ForeignKeyColumnCannotChangeChild {
        /// The parent column named by the `MODIFY`, under its OLD name.
        column: String,
        /// The child's constraint name.
        constraint: String,
        /// The child as `schema.table`, lowercased the way Go builds it.
        child_table: String,
    },
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
    /// MySQL `ER_TOO_MANY_ROWS` (1172): `SELECT ... INTO @var` produced more
    /// than one row.
    SelectIntoMoreThanOneRow,
    /// MySQL `ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT` (1222): the `INTO` list's
    /// width differs from the select list's.
    SelectIntoColumnMismatch,
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
    /// Go `ErrConstraintNotFound` (3940): `Constraint '%s' does not exist.`
    /// Raised by `ALTER TABLE ... DROP {CHECK|CONSTRAINT} <name>` -- which is
    /// ONE action in TiDB's grammar, and names a CHECK constraint only.
    /// Measured: `ALTER TABLE c DROP CONSTRAINT fk1` where `fk1` IS a foreign
    /// key answers this error and leaves the key in place.
    CheckConstraintNotExists(String),
    /// Go's `preprocessor`'s `CREATE BINDING` check (`preprocess.go`), a plain
    /// error and so 1105: the origin and hinted statements do not normalize to
    /// the same text once their hints are erased. Both normalized texts are
    /// carried because Go prints both, and they are the whole diagnostic --
    /// the user wrote two statements that differ in more than hints.
    BindingHintedSqlMismatch {
        /// The origin statement's normalized, DB-qualified text.
        origin: String,
        /// The hinted statement's normalized, DB-qualified text.
        hinted: String,
    },
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
    /// Go `plannererrors.ErrSpecificAccessDenied` (1227), the general form:
    /// the statement needs at least one of the named privileges. The payload
    /// is Go's own argument text VERBATIM, including its capitalization
    /// quirks -- `CREATE USER` reports `"CREATE User"` while `DROP USER`
    /// reports `"CREATE USER"` (`executor/simple.go`'s `executeCreateUser`
    /// and `executeDropUser`).
    SpecificAccessDenied(String),
    /// Go `exeerrors.ErrDBaccessDenied` (1044): the caller may not reach a
    /// whole schema. Raised by `SET PASSWORD` for another account and by
    /// `SHOW GRANTS FOR <other>`, both of which name the `mysql` schema.
    DbAccessDenied {
        /// The caller's authenticated username.
        user: String,
        /// The caller's authenticated host.
        host: String,
        /// The schema the caller was refused.
        database: String,
    },
    /// Go `ErrTableaccessDenied` (1142): the caller lacks one privilege on
    /// one table. Raised by `SHOW CREATE USER FOR <other>`, which needs
    /// `SELECT` on `mysql.user`.
    TableAccessDenied {
        /// The privilege name Go prints, uppercase (`SELECT`).
        privilege: &'static str,
        /// The caller's authenticated username.
        user: String,
        /// The caller's authenticated host.
        host: String,
        /// The table the caller was refused.
        table: String,
    },
    /// Go `plannererrors.ErrPrivilegeCheckFail` (8121): a `visitInfo` entry
    /// with no statement-specific `authErr` failed, which is what a denied
    /// `GRANT`/`REVOKE` reports outside `performance_schema`
    /// (`planner/core/optimizer.go`'s `CheckPrivilege`). The payload is the
    /// privilege's Go `String()` form.
    PrivilegeCheckFail(String),
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
    /// Go `variable.ErrNotValidPassword` (1819): the supplied plaintext
    /// password violated one of the active `validate_password.*` rules.
    NotValidPassword {
        /// The policy-specific reason inserted into Go's error template.
        reason: String,
    },
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
    /// Go `ErrIllegalGrantForTable` (1144): a TABLE-scope `GRANT`/`REVOKE`
    /// named a privilege outside `mysql.AllTablePrivs`.
    IllegalGrantForTable,
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

impl DriverError {
    /// [`DriverError::Unsupported`] from either a fixed reason or one built
    /// per call.
    ///
    /// Both spellings render identically on the wire, so this is the one
    /// constructor; the borrow stays borrowed and the owned string is moved.
    #[must_use]
    pub fn unsupported(reason: impl Into<Cow<'static, str>>) -> Self {
        DriverError::Unsupported(reason.into())
    }
}
