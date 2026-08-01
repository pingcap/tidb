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

/// Why a schema statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaErrorKind {
    /// Go `infoschema.ErrDatabaseNotExists` / `ErrBadDB` (1049).
    UnknownDatabase(String),
    /// Go `infoschema.ErrTableNotExists` (1146): a statement read a table
    /// that does not exist.
    UnknownTable(String),
    /// Go `ErrTableExists` (1050).
    TableExists(String),
    /// Go `ErrBadTable` (1051): `DROP TABLE` named a table that does not
    /// exist. MySQL uses a different code and message here than for a read.
    BadTable(String),
    /// Go `ErrDBCreateExists` (1007).
    DatabaseExists(String),
    /// Go `plannererrors.ErrNoDB` (1046).
    NoDatabaseSelected,
    /// Go `ErrWrongObject` (1347): the name exists but is the other object
    /// kind -- `DROP VIEW t` / `SHOW CREATE VIEW t` on a base table, or
    /// `CREATE TABLE ... LIKE v` on a view or sequence.
    ///
    /// `expected` is the object kind Go passes as the message's third
    /// argument, which is `VIEW` for the view statements and `BASE TABLE`
    /// for `CREATE TABLE ... LIKE`. Most table statements that name a view
    /// report the name as simply unknown instead, as Go does.
    WrongObject {
        /// The db-qualified name as written.
        name: String,
        /// The object kind the statement required.
        expected: &'static str,
    },
    /// Go `plannererrors.ErrViewInvalid` (1356): the view's own query no
    /// longer runs, typically because a base table was dropped.
    ViewInvalid(String),
    /// Go `infoschema.ErrSequenceDropExists` (4139): `DROP SEQUENCE` named a
    /// sequence that does not exist. Captured: `drop sequence nosuch` reports
    /// `[schema:4139] Unknown SEQUENCE: 'test.nosuch'` -- a different code and
    /// wording from both 1146 and `DROP TABLE`'s 1051.
    UnknownSequence(String),
    /// Go `dbterror.ErrErrorOnRename` (1025): a rename named a destination
    /// schema that does not exist. Go raises it from `ddl.ExtractTblInfos`
    /// only after the source has resolved, and leaves the source table in
    /// place -- the statement moves nothing at all.
    RenameTargetDatabaseMissing {
        /// The source as `db.table`, Go's first message argument.
        from: String,
        /// The destination as `db.table`, Go's second message argument.
        to: String,
        /// The destination schema, named in the nested reason.
        database: String,
    },
    /// Go `ddl.ErrSequenceInvalidData` (4136): the option values cannot
    /// describe a sequence. Captured for `increment by 0`, `cache 0`,
    /// `minvalue 10 maxvalue 5` and `start with 1 minvalue 5`, each reporting
    /// `Sequence '<db>.<name>' values are conflicting`.
    SequenceValuesConflicting(String),
}
