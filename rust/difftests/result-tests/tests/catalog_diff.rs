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

//! The CATALOG ring: what a table's definition READS BACK as, after DDL.
//!
//! # Why this exists as its own gate
//!
//! `integration_diff` replays whole topics and onboards a topic only once the
//! WHOLE topic is at zero divergences. That rule is right for what it guards,
//! and it has one structural consequence that went unnoticed for a long time:
//! the topics that clear it are query topics. All 49 divergences it carries
//! are EXPLAIN access-path shapes and two partition row cases. Not one is a
//! catalog read, because the topics dense in `SHOW CREATE TABLE` after an
//! `ALTER` -- `ddl/*`, `infoschema/*` -- are also dense in DDL this engine
//! does not model yet, so none of them will ever reach zero as a whole, and
//! none of them will ever be onboarded there.
//!
//! Meanwhile the catalog seam is where the worst bugs have actually lived. Two
//! `pkg/ddl` seams were mined by hand and each yielded five bugs of the same
//! shape: `bit(10) DEFAULT 250` printing `DEFAULT '250'`, `binary(4) DEFAULT
//! 0x61` printing `DEFAULT ''`, a NULLABLE PRIMARY KEY, an `AUTO_INCREMENT`
//! that could not be dropped while reporting that it had been, two indexes
//! printed with the SAME NAME. In every one of them THE ROWS WERE RIGHT AND
//! THE CATALOG WAS WRONG -- and the row-shaped oracle only ever looked at
//! rows.
//!
//! So this gate cuts the corpus the other way. It replays the same topics
//! against the same engine, but it compares ONLY catalog reads (see
//! [`is_catalog_read`]), and it is therefore free to run topics whose QUERY
//! surface still diverges. Nothing here is synthesized: every expected answer
//! is TiDB's own recorded `r/<topic>.result` text, so this is an oracle and
//! not a mirror.
//!
//! # What three reverted bugs proved, including the two it did NOT catch
//!
//! An oracle that would not have caught the bugs that motivated it is not
//! worth landing, so three of the eight were reverted IN PLACE and the gate
//! re-run. Every revert was confirmed live by `tidb-session`'s own unit tests
//! going red first, so a miss here is a statement about the CORPUS and not
//! about the revert.
//!
//!  * `bit(N) DEFAULT <literal>` printing through `to_bit_literal_string`
//!    (`show::column_default_text`) -- NOT CAUGHT. Six unit tests failed and
//!    this gate did not move by one. The reason is not subtlety: the corpus
//!    contains no BIT column with a DEFAULT that is ever read back. Its only
//!    bit columns are `ddl/column_type_change`'s `bit(13)` with no default,
//!    and `ddl/db_integration`'s `int default b'1'`, which is an INT column
//!    and never reaches this branch.
//!  * The `MODIFY COLUMN` nullable primary key (`alter_table.rs`'s copy of
//!    the old column's `PRI_KEY_FLAG`) -- NOT CAUGHT, for the same reason.
//!    `ddl/modify_column` holds 133 `ALTER`s and prints no `SHOW CREATE
//!    TABLE` at all, so the read-back the bug corrupts is never recorded.
//!  * The SAME BUG CLASS at CREATE time -- a primary key that is not
//!    implicitly `NOT NULL` (`ddl.rs`'s `NOT_NULL_FLAG | PRI_KEY_FLAG`) --
//!    CAUGHT, and by all three assertions at once: matched fell 102 -> 99,
//!    divergences rose 111 -> 114, and the fingerprint moved.
//!
//! So the gate has real teeth on the seam, and the honest limit is that
//! TiDB's recording only ever asks for a table's definition where TiDB's own
//! authors thought to ask. Where it asks, a wrong catalog now fails. Where it
//! does not, the hand-built captures in `tidb-session`'s `tests_column_defaults`
//! and `tests_alter_column` remain the only cover -- which is exactly the
//! division of labour to expect, and a reason to keep writing them rather
//! than to treat this file as having replaced them.
//!
//! # Every statement still RUNS
//!
//! Only the COMPARISON is scoped. Each non-catalog statement is executed and
//! its outcome discarded, because the catalog reads later in the script are
//! reading the state those statements built -- an `ALTER` that is skipped
//! rather than run would leave the `SHOW CREATE TABLE` after it describing a
//! table that was never altered, which is precisely the shape of failure that
//! makes an instrument lie. Discarded outcomes are counted by class and the
//! totals are printed on every run.

#[path = "integration_plan_property.rs"]
mod integration_plan_property;
#[path = "mysqltest_connections.rs"]
mod mysqltest_connections;
#[path = "mysqltest_script.rs"]
mod mysqltest_script;

use std::collections::BTreeMap;
use std::fs;

use integration_plan_property::plan_statement;
use mysqltest_connections::Connections;
use mysqltest_script::{align, parse_test, split_warnings, Item, Stmt};
use tidb_datatype::Datum;
use tidb_session::{Session, StmtOutput};

/// The topics replayed for their catalog reads.
///
/// Chosen by measurement over `tests/integrationtest/t`, not by name: the
/// corpus holds 2,526 `ALTER TABLE` statements, 530 of which are followed
/// within ten statements by a catalog read, and 497 recorded `SHOW CREATE
/// TABLE` statements spread over 40 topics. The topics below are the dense
/// end of that distribution plus the two `infoschema` topics that read the
/// same metadata through a different surface.
///
/// The reason recorded with each is the catalog property it puts under the
/// gate that the others do not.
const CATALOG_TOPICS: &[(&str, &str)] = &[
    (
        "ddl/default_as_expression",
        "the column DEFAULT seam itself: 65 `SHOW CREATE TABLE` bodies over \
         columns whose default is an expression, a literal, or a function \
         call -- the seam that printed `bit(10) DEFAULT 250` as `DEFAULT '250'`",
    ),
    (
        "ddl/column_modify",
        "`MODIFY`/`CHANGE COLUMN` read back: the seam that produced a NULLABLE \
         PRIMARY KEY and an `AUTO_INCREMENT` that reported itself dropped \
         while remaining",
    ),
    (
        "ddl/column_type_change",
        "the largest ALTER topic in the suite (649 alters); its 29 catalog \
         reads are the type-change seam's own read-back",
    ),
    (
        "ddl/constraint",
        "CHECK constraints in the printed body -- the one catalog element that \
         is stored as an expression and re-rendered",
    ),
    (
        "ddl/db_integration",
        "charset/collation and table option read-back after ALTER",
    ),
    (
        "ddl/column",
        "ADD/DROP COLUMN position (`AFTER`, `FIRST`) in the printed column order",
    ),
    (
        "ddl/table_modify",
        "table-level ALTER: rename, comment, and the options printed after the \
         closing paren",
    ),
    (
        "executor/show",
        "the SHOW surface's own topic: 60 `SHOW CREATE TABLE` bodies plus \
         `SHOW COLUMNS`, which prints the same metadata through different \
         column names",
    ),
    (
        "infoschema/tables",
        "`information_schema.tables`/`.columns`: the catalog read that does \
         NOT go through the SHOW printer, so a divergence here separates a \
         wrong CATALOG from a wrong RENDERING",
    ),
    (
        "infoschema/infoschema",
        "the information_schema surface at breadth",
    ),
    (
        "generated_columns",
        "`GENERATED ALWAYS AS (...)` re-rendered from the stored expression, \
         with VIRTUAL/STORED and the column's own type",
    ),
];

/// Whether a statement is a catalog read -- a statement whose result IS the
/// stored definition of a schema object, rather than the data in it.
///
/// The four surfaces differ in what they render and agree on what they
/// describe, which is the point of comparing all of them: `SHOW CREATE TABLE`
/// prints a body that must RE-PARSE, `SHOW COLUMNS`/`DESC <table>` print the
/// same metadata as a row per column, and `information_schema` reaches the
/// stored model without the SHOW printer in the way at all.
///
/// `DESC`/`DESCRIBE` is deliberately routed through [`plan_statement`] first:
/// TiDB parses `DESC t` and `DESC SELECT ...` as one statement, and only the
/// token after the keyword decides whether the answer is a column list or a
/// query plan. A plan is not a catalog read and is left to `integration_diff`.
fn is_catalog_read(sql: &str) -> bool {
    if plan_statement(sql).is_some() {
        return false;
    }
    let sql = sql.trim_start().to_ascii_lowercase();
    let head: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    head.starts_with("show create table")
        || head.starts_with("show create view")
        || head.starts_with("show create sequence")
        || head.starts_with("show columns")
        || head.starts_with("show full columns")
        || head.starts_with("show fields")
        || head.starts_with("show index")
        || head.starts_with("show keys")
        || head.starts_with("desc ")
        || head.starts_with("describe ")
        || (head.starts_with("select") && head.contains("information_schema."))
}

/// Why a catalog read did not produce a comparable outcome.
///
/// These are the same distinctions `integration_diff` draws, kept separate
/// here so this gate's own skip totals can be watched for growth: a
/// divergence count that falls because fewer statements are EXAMINED looks
/// exactly like progress, and the only defence is publishing both numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SkipClass {
    /// The recorder rewrote or extended this statement's output, so the
    /// recording is not the statement's own result.
    RecorderRewroteOutput(&'static str),
    /// TiDB recorded an error here and this engine also refused it. Agreement
    /// on rejection is the assertion.
    BothRejected,
    /// The catalog read itself does not run here: a surface this engine does
    /// not model.
    OutOfDomain,
}

/// One topic's catalog replay outcome.
#[derive(Default)]
struct CatalogReport {
    /// Catalog reads whose full recorded text matched.
    matched: usize,
    /// Catalog reads that did not match, with the divergence text.
    divergences: Vec<String>,
    /// Catalog reads that produced no comparable outcome, by class.
    skipped: BTreeMap<String, usize>,
    /// Statements that are not catalog reads: RUN for their effect on the
    /// state the later catalog reads describe, never compared.
    ran_for_effect: usize,
}

impl CatalogReport {
    fn compared(&self) -> usize {
        self.matched + self.divergences.len()
    }

    fn catalog_reads(&self) -> usize {
        self.compared() + self.skipped.values().sum::<usize>()
    }

    fn absorb(&mut self, other: CatalogReport) {
        self.matched += other.matched;
        self.divergences.extend(other.divergences);
        self.ran_for_effect += other.ran_for_effect;
        for (class, count) in other.skipped {
            *self.skipped.entry(class).or_default() += count;
        }
    }
}

/// Renders one result cell the way the recorder writes it.
fn cell(value: &Datum) -> String {
    if value.is_null() {
        return "NULL".to_owned();
    }
    value.sql_string().unwrap_or_else(|_| value.label())
}

/// Renders a result set as the recorder does: a tab-separated header of column
/// names, then one tab-separated line per row, then split on any newline
/// EMBEDDED IN A CELL -- `SHOW CREATE TABLE`'s body is one cell containing
/// newlines and the recorder has no escape for it, so its physical lines in
/// the `.result` file are the units both sides must be compared in.
fn render(
    columns: &[(String, tidb_datatype::FieldType)],
    rows: &[Vec<Datum>],
    sorted: bool,
) -> Vec<String> {
    let mut out = vec![columns
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>()
        .join("\t")];
    out.extend(
        rows.iter()
            .map(|row| row.iter().map(cell).collect::<Vec<_>>().join("\t")),
    );
    if sorted {
        out[1..].sort();
    }
    out.iter()
        .flat_map(|line| line.split('\n').map(str::to_owned))
        .collect()
}

/// Compares one catalog read against its recorded block.
fn compare_catalog(
    session: &mut Session,
    stmt: &Stmt,
    recorded: &[String],
    report: &mut CatalogReport,
) {
    if let Some(reason) = stmt.blocker {
        // Run it anyway: the recording is not comparable, but the statement's
        // effect on the schema is real and the reads after it depend on it.
        drop(session.run_with_columns(&stmt.sql));
        *report
            .skipped
            .entry(format!("{:?}", SkipClass::RecorderRewroteOutput(reason)))
            .or_default() += 1;
        return;
    }
    // `--enable_warnings` APPENDED a `SHOW WARNINGS` block to this read's
    // rows; it did not rewrite them. Drop that half so the rows compare. The
    // warning texts themselves are gated by `integration_diff`, whose unit is
    // the statement; this tier's unit is the catalog read's rows.
    let recorded = if stmt.warnings {
        split_warnings(recorded).0
    } else {
        recorded
    };
    let recorded_error = stmt.expect_error
        || recorded
            .first()
            .is_some_and(|line| line.starts_with("Error "));
    let outcome = session.run_with_columns(&stmt.sql);
    let mut skip = |class: SkipClass| {
        *report.skipped.entry(format!("{class:?}")).or_default() += 1;
    };
    match (outcome, recorded_error) {
        (Err(_), true) => skip(SkipClass::BothRejected),
        (Err(_), false) => skip(SkipClass::OutOfDomain),
        (Ok(_), true) => report.divergences.push(format!(
            "\n--- {}\n  tidb: {}\n  rust: accepted the statement",
            stmt.sql,
            recorded.first().map_or("<error>", String::as_str)
        )),
        (Ok(StmtOutput::Rows { columns, rows }), false) => {
            let ours = render(&columns, &rows, stmt.sorted);
            let mut theirs: Vec<String> = recorded.to_vec();
            if stmt.sorted && !theirs.is_empty() {
                theirs[1..].sort();
            }
            if ours == theirs {
                report.matched += 1;
            } else {
                report.divergences.push(format!(
                    "\n--- {}\n  tidb: {}\n  rust: {}",
                    stmt.sql,
                    theirs.join(" / "),
                    ours.join(" / ")
                ));
            }
        }
        // A catalog read that returned no result set at all is not a catalog
        // read this engine answered; it is out of domain.
        (Ok(_), false) => skip(SkipClass::OutOfDomain),
    }
}

fn run_topic(topic: &str) -> Result<CatalogReport, String> {
    let topic = topic.to_owned();
    difftest::on_deep_stack(move || run_topic_on_this_stack(&topic))
}

fn run_topic_on_this_stack(topic: &str) -> Result<CatalogReport, String> {
    let dir = difftest::parser_oracle::repo_root().join("tests/integrationtest");
    let script = fs::read_to_string(dir.join(format!("t/{topic}.test")))
        .map_err(|e| format!("read t/{topic}.test: {e}"))?;
    let recorded = fs::read_to_string(dir.join(format!("r/{topic}.result")))
        .map_err(|e| format!("read r/{topic}.result: {e}"))?;
    let items = parse_test(&script)?;
    let aligned = align(&items, &recorded)?;

    let mut report = CatalogReport::default();
    let mut connections = Connections::open(topic)?;
    for (item, block) in aligned {
        let stmt = match item {
            Item::Stmt(stmt) => stmt,
            Item::Connection(cmd) => {
                connections.apply(cmd)?;
                continue;
            }
            Item::Echo(_) => continue,
        };
        if is_catalog_read(&stmt.sql) {
            compare_catalog(connections.current(), stmt, &block, &mut report);
        } else {
            // Not compared -- but RUN, because the catalog reads after it
            // describe the schema this statement leaves behind.
            drop(connections.current().run_with_columns(&stmt.sql));
            report.ran_for_effect += 1;
        }
    }
    Ok(report)
}

/// The catalog ratchet, asserted on BOTH sides.
///
/// The lower bound matters as much as the upper one here. This gate compares
/// a SUBSET of each topic, chosen by a predicate, so the cheapest way to make
/// it green is to narrow the predicate until nothing is examined -- and that
/// is indistinguishable from real progress if only the divergence count is
/// published. So the number of catalog reads MATCHED is pinned too: it may
/// only rise.
/// 111 -> 104: the `mysql` schema became a name that exists, so
/// `infoschema/infoschema`'s `rename table infoschema__infoschema.t1 to
/// mysql.t1` has somewhere to rename TO. All seven were the same shape --
/// `SELECT count(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA =
/// 'mysql' AND TABLE_NAME = 't1'` answering 0 where TiDB answers 1 -- and
/// they are genuine parity rather than a fabricated schema: Go really does
/// accept a user table in `mysql` (captured: `create table mysql.zz(a int)`
/// succeeds), so those rows exist on both sides now.
/// 111 -> 112: RANGE partitioning became measurable. `CREATE TABLE ...
/// PARTITION BY RANGE` used to be refused, so two of `executor/show`'s
/// catalog reads were out of domain; they are compared now (217 -> 219),
/// one MATCHES (106 -> 107) and one diverges.
///
/// The divergent one is `SHOW CREATE TABLE log`, and NOT for its partition
/// clause -- that clause reads back character for character, twelve
/// `VALUES LESS THAN` definitions over `MONTH(`end_time`)` and a `MAXVALUE`.
/// It diverges on two pre-existing gaps the refusal had been hiding: the
/// composite primary key prints `CLUSTERED` where TiDB prints
/// `NONCLUSTERED`, and the `AUTO_INCREMENT=505488` counter is not printed at
/// all. Both belong to the clustered-index and auto-increment surfaces, not
/// to partitioning.
/// 105 -> 102: the CREATE TABLE index-key validations landed (Go's
/// `buildIndexColumns` running `sumLength` and `checkIndexColumn`'s 1167),
/// so three catalog reads whose tables TiDB refuses to create are now
/// refused here too, and their `SHOW CREATE TABLE` divergences vanish with
/// the tables.
/// 102 -> 100: Go's `checkColumnDefaultValue` landed on `CREATE TABLE`
/// (#274). Under `sql_mode=''` an EMPTY default on a TEXT/BLOB column is a
/// warning and the default is DROPPED, which is what TiDB's own
/// `TestCheckColumnDefaultValue` records; this tier used to store the `''`
/// and print it, so `show create table text_default_text` and
/// `text_default_blob` both read back a `DEFAULT ''` TiDB does not print.
/// They now MATCH rather than vanish: compared held at 219 and matched rose
/// 117 -> 119, so no read stopped being examined.
const KNOWN_CATALOG_DIVERGENCES: usize = 100;

/// The floor on catalog reads that MATCH TiDB's recording exactly. See
/// [`KNOWN_CATALOG_DIVERGENCES`] for why a divergence ceiling alone is not a
/// gate.
///
/// 102 -> 106: prefix indexes on secondary keys are built rather than
/// refused, so `ddl/column_type_change`'s four
/// `TestChangePrefixedIndexColumnToNonPrefixOne` tables exist and their
/// `SHOW CREATE TABLE` is compared -- and matches, because `SHOW CREATE
/// TABLE` prints the declared `(n)` and `MODIFY COLUMN` clears it exactly
/// where Go's `UpdateIndexCol` does. The divergence count and fingerprint
/// did not move: all four were previously OUT OF DOMAIN, not red.
///
/// 106 -> 113: the same `mysql` schema change. Seven catalog reads went from
/// red to matched, and the floor rises with them -- which is the point of
/// having a floor at all, since a divergence count that falls while the
/// matched count falls too is statements going dark, not statements getting
/// right.
/// 106 -> 107: RANGE partitioning is built, so `executor/show`'s `thash2`
/// definition reads back and matches. See [`KNOWN_CATALOG_DIVERGENCES`] for
/// the second newly-measurable read, which does not.
const MATCHED_FLOOR: usize = 114;

/// A fingerprint over the TEXT of every carried divergence, and the reason it
/// exists is a hole this gate was CAUGHT having on its first day.
///
/// The `bit(10) DEFAULT 250` bug -- one of the eight catalog bugs that
/// motivated this file -- was reverted in place to check that the gate would
/// have caught it. It did not. Six `tidb-session` unit tests went red, so the
/// revert was certainly live, and the catalog counts did not move by one:
/// the corpus's only bit column, `ddl/column_type_change`'s `bit(13)`, was
/// ALREADY diverging for an unrelated reason, so the mutation changed the
/// divergence's TEXT while leaving the COUNT identical.
///
/// That is the general failure of a counting ratchet: it is blind inside its
/// own red set, and the red set is exactly where the unfixed bugs are, so a
/// second bug landing on an already-red statement is invisible. Hashing the
/// divergence text closes it. The hash is FNV-1a, written out here rather
/// than taken from `DefaultHasher`, because a gate constant has to mean the
/// same thing in a future toolchain.
///
/// 15_209_155_495_828_413_401 -> 5_541_979_227_473_374_610: the seven
/// `TABLE_SCHEMA = 'mysql'` counts named on [`KNOWN_CATALOG_DIVERGENCES`]
/// LEFT the set. Nothing else in it changed text -- the before/after lists
/// were diffed line for line, and those seven entries are the whole diff.
/// 15_209_155_495_828_413_401 -> 4_674_120_825_973_024_879, and the value is
/// the COMPOSITION of two units that landed together rather than either one's
/// own number. The `mysql` schema removed seven `TABLE_SCHEMA = 'mysql'`
/// counts from the set; RANGE partitioning added one (`SHOW CREATE TABLE
/// log`). Neither unit's separately-measured fingerprint (5_541_979_227_473_374_610
/// and 16_061_795_834_336_541_714) is correct on the merged tree, which is
/// exactly what this constant exists to catch: the count moved 111 -> 105 and
/// the SET moved differently again, so a count-only gate would have accepted
/// either stale value.
///
/// 4_674_120_825_973_024_879 -> 12_364_006_894_818_275_074: the count stayed
/// 105 and matched stayed at the floor, but divergence TEXTS inside the red
/// set improved -- the CREATE TABLE handle-kind fix (NONCLUSTERED honoured)
/// moved several `SHOW CREATE TABLE` and `tidb_pk_type` reads closer to the
/// recording without closing them. The clearest single witness is the
/// partitioned `log` table, which previously diverged on BOTH the clustered
/// clause and the AUTO_INCREMENT counter and now diverges ONLY on
/// `AUTO_INCREMENT=505488`. Same statements, still red, redder for fewer
/// reasons -- the count-blind ratchet could never have seen this, which is
/// why the fingerprint exists.
/// 12_364_006_894_818_275_074 -> 9_329_879_596_375_632_442: the count stayed
/// 105, and exactly TWO texts moved -- both `DESC test_gv_ddl` in
/// `ddl/column_modify`, which lost the column `a` here and no longer does.
/// `column_modify.test` runs `alter table test_gv_ddl drop column a` and
/// `alter table test_gv_ddl change column a anew int` over
/// `test_gv_ddl(a int, b int as (a+8) virtual, c int as (b+2) stored)`, and
/// the recording answers both with `Error 3108 (HY000): Column 'a' has a
/// generated column dependency.` This tier accepted them, so `a` really went
/// away and every later read of that table was short a column. Both are now
/// refused with that exact error (#202). The two DESCs stay red because `b`
/// reads back `bigint` with no `VIRTUAL GENERATED` in `Extra` -- a separate,
/// still-open gap -- so the count could not see this: the fingerprint is the
/// only gate that could.
///
/// Moved again with [`KNOWN_CATALOG_DIVERGENCES`] 102 -> 100: the two
/// `SHOW CREATE TABLE` reads that stopped printing a `DEFAULT ''` TiDB does
/// not print (`text_default_text` and `text_default_blob`) left the set, and
/// nothing else in it changed.
const CATALOG_DIVERGENCE_FINGERPRINT: u64 = 6_068_344_160_096_210_003;

/// FNV-1a over the sorted divergence texts. Sorted because the value must
/// depend on WHAT diverges and not on the order topics happen to run in.
fn fingerprint(divergences: &[String]) -> u64 {
    let mut sorted: Vec<&str> = divergences.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in sorted.join("\n").bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[test]
fn catalog_reads_match_recorded_tidb_output() {
    let mut total = CatalogReport::default();
    let mut per_topic = Vec::new();
    for (topic, _why) in CATALOG_TOPICS {
        let report = run_topic(topic).unwrap_or_else(|e| panic!("topic {topic}: {e}"));
        per_topic.push(format!(
            "{topic}: {} of {} catalog reads compared ({} matched, {} diverged), {:?}, {} statements run for effect",
            report.compared(),
            report.catalog_reads(),
            report.matched,
            report.divergences.len(),
            report.skipped,
            report.ran_for_effect
        ));
        total.absorb(report);
    }

    eprintln!(
        "catalog replay over {} topics: {} of {} catalog reads compared, {} matched, {} diverged\n  {}\nskips by class: {:?}\nstatements run for effect: {}",
        CATALOG_TOPICS.len(),
        total.compared(),
        total.catalog_reads(),
        total.matched,
        total.divergences.len(),
        per_topic.join("\n  "),
        total.skipped,
        total.ran_for_effect
    );

    if std::env::var_os("CATALOG_SHOW_DIVERGENCES").is_some() {
        eprintln!("carried catalog divergences:{}", total.divergences.join(""));
    }

    assert!(
        total.divergences.len() <= KNOWN_CATALOG_DIVERGENCES,
        "catalog divergences rose to {} (limit {}). Run with \
         CATALOG_SHOW_DIVERGENCES=1 to see them. A catalog divergence is a \
         table whose DEFINITION reads back differently from TiDB's, which the \
         row-shaped gates cannot see.",
        total.divergences.len(),
        KNOWN_CATALOG_DIVERGENCES,
    );
    assert!(
        total.divergences.len() >= KNOWN_CATALOG_DIVERGENCES,
        "catalog divergences fell to {} (recorded {}). Lower \
         KNOWN_CATALOG_DIVERGENCES to {} so the ratchet holds.",
        total.divergences.len(),
        KNOWN_CATALOG_DIVERGENCES,
        total.divergences.len(),
    );
    let seen = fingerprint(&total.divergences);
    assert_eq!(
        seen, CATALOG_DIVERGENCE_FINGERPRINT,
        "the SET of carried catalog divergences changed while the COUNT did \
         not. Something inside the already-red set now reads back differently \
         -- which is either a fix (update the fingerprint to {seen} and say \
         what it fixed) or a second bug landing on a statement that was \
         already wrong. Run with CATALOG_SHOW_DIVERGENCES=1 to diff them."
    );
    assert!(
        total.matched >= MATCHED_FLOOR,
        "catalog reads MATCHING TiDB's recording fell to {} (floor {}). \
         Divergences did not rise, so statements stopped being EXAMINED \
         rather than starting to disagree -- check the skip totals above.",
        total.matched,
        MATCHED_FLOOR,
    );
}

#[cfg(test)]
mod tests {
    use super::is_catalog_read;

    #[test]
    fn desc_of_a_table_is_a_catalog_read_and_desc_of_a_query_is_not() {
        assert!(is_catalog_read("desc t"));
        assert!(is_catalog_read("DESCRIBE db.t"));
        assert!(!is_catalog_read("desc select * from t"));
        assert!(!is_catalog_read("explain select * from t"));
    }

    #[test]
    fn the_four_catalog_surfaces_are_all_recognised() {
        assert!(is_catalog_read("SHOW CREATE TABLE t"));
        assert!(is_catalog_read("show  create   table  t"));
        assert!(is_catalog_read("show full columns from t"));
        assert!(is_catalog_read("show index from t"));
        assert!(is_catalog_read(
            "select * from information_schema.columns where table_name = 't'"
        ));
    }

    #[test]
    fn a_data_query_is_not_a_catalog_read() {
        assert!(!is_catalog_read("select * from t"));
        assert!(!is_catalog_read("alter table t add column c int"));
        // `information_schema` as a bare word in a string is not the schema.
        assert!(!is_catalog_read("select 'information_schema'"));
    }
}
