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

//! The result ring pointed at TIDB'S OWN record/replay suite.
//!
//! Replay `tests/integrationtest/t/<topic>.test` through the live Session
//! and compare with TiDB's recorded `r/<topic>.result`. Recordings are read-only:
//! every divergence in an enrolled topic fails the gate.
//!
//! # Onboarding is explicit, and the count is honest
//!
//! [`TOPICS`] is the onboarded list. It is short on purpose: a topic is
//! onboarded only once its subject matter is one this engine actually covers,
//! and every statement that does not run is counted in a NAMED skip class
//! (see [`SkipClass`]) so a green run states exactly what it proved. The
//! remaining topics are not silently skipped -- they are simply not on the
//! list yet, and [`survey_unonboarded_topics`] is the tool that ranks them.
//!
//! # A topic may drive several connections
//!
//! A topic that drives SEVERAL connections replays through
//! `mysqltest_connections`, which holds one session per connection over one
//! shared store -- read its docs for which state is shared and which is not,
//! and for why an account it cannot authenticate refuses the topic instead of
//! falling back to root.
//!
//! # The two content classes differ in kind
//!
//! Row results are directly comparable and are the bulk of the value. Plan
//! text is NOT: this tier's `EXPLAIN` printer deliberately describes the
//! executors it has rather than Go's, so an `EXPLAIN` is compared by the
//! access PROPERTY its case guards -- see `integration_plan_property`, which
//! also owns the rule for WHICH statements are plans: `EXPLAIN`, `DESCRIBE`
//! and `DESC` are one statement in TiDB's parser, and the split against
//! `DESC <table>`'s column list is made by the token after the keyword.

use difftest_result_tests::enrolled_topics;
use difftest_result_tests::mysqltest_connections;
use difftest_result_tests::mysqltest_script;
use difftest_result_tests::replay;

use std::collections::BTreeMap;
use std::fs;

use enrolled_topics::TOPICS;
use mysqltest_connections::Connections;
use mysqltest_script::{align_bytes, parse_test, recording_path, Item, Stmt};
use replay::{integrationtest_dir, TopicReport};

fn run_topic(topic: &str) -> Result<TopicReport, String> {
    replay::run_topic::<Connections>(topic)
}

/// Replays every topic, `INTEGRATION_JOBS` at a time (all cores by default),
/// and answers the reports in `topics` order.
///
/// Each topic replays in a process of its own, as each recording was made
/// against a server: a topic opens its own store, but `SET GLOBAL`, the
/// config and Go's process-wide singletons (`vardef`, infosync's label
/// rules) are process state, and topics sharing one process would read each
/// other's. `INTEGRATION_JOBS=1` replays the topics one after another.
fn run_topics(topics: &[&str]) -> Vec<Result<TopicReport, String>> {
    let jobs = std::env::var("INTEGRATION_JOBS")
        .ok()
        .and_then(|jobs| jobs.parse::<usize>().ok())
        .filter(|jobs| *jobs > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from))
        .min(topics.len().max(1));
    let reports_dir = std::env::temp_dir().join(format!("integration-diff-{}", std::process::id()));
    std::fs::create_dir_all(&reports_dir).expect("create the report directory");
    let next = std::sync::atomic::AtomicUsize::new(0);
    let reports: Vec<std::sync::Mutex<Option<Result<TopicReport, String>>>> =
        topics.iter().map(|_| std::sync::Mutex::new(None)).collect();
    std::thread::scope(|scope| {
        for _ in 0..jobs {
            scope.spawn(|| loop {
                let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(topic) = topics.get(index) else {
                    break;
                };
                let report = replay_topic_in_child(topic, &reports_dir.join(index.to_string()));
                *reports[index].lock().unwrap() = Some(report);
            });
        }
    });
    let _ = std::fs::remove_dir_all(&reports_dir);
    reports
        .into_iter()
        .map(|report| report.into_inner().unwrap().expect("every topic was replayed"))
        .collect()
}

/// Replays `topic` in a child process ([`replay_child_topic`]), which writes
/// its report to `<path>.report` and its output to `<path>.log`.
fn replay_topic_in_child(topic: &str, path: &std::path::Path) -> Result<TopicReport, String> {
    let report_path = path.with_extension("report");
    let log_path = path.with_extension("log");
    let log = std::fs::File::create(&log_path).map_err(|e| format!("create {}: {e}", log_path.display()))?;
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "--ignored", "--nocapture", "--test-threads=1", "replay_child_topic"])
        .env("INTEGRATION_CHILD_TOPIC", topic)
        .env("INTEGRATION_CHILD_REPORT", &report_path)
        .stdout(log.try_clone().map_err(|e| format!("log handle: {e}"))?)
        .stderr(log)
        .status()
        .map_err(|e| format!("spawn the replay of {topic}: {e}"))?;
    match std::fs::read_to_string(&report_path) {
        Ok(text) if status.success() => match text.strip_prefix("ERROR\t") {
            Some(error) => Err(error.to_owned()),
            None => TopicReport::decode(&text),
        },
        _ => {
            // The child's last lines name the panic.
            let output = std::fs::read_to_string(&log_path).unwrap_or_default();
            let tail: Vec<&str> = output
                .lines()
                .filter(|line| !line.starts_with('['))
                .collect();
            let tail = tail[tail.len().saturating_sub(12)..].join("\n");
            Err(format!("the replay process failed ({status}):\n{tail}"))
        }
    }
}

/// The child half of [`run_topics`]: replays `INTEGRATION_CHILD_TOPIC` and
/// writes its encoded report to `INTEGRATION_CHILD_REPORT`.
#[test]
#[ignore = "run by integrationtest_replay_matches_recorded_tidb_output, one process per topic"]
fn replay_child_topic() {
    let topic = std::env::var("INTEGRATION_CHILD_TOPIC").expect("INTEGRATION_CHILD_TOPIC");
    let path = std::env::var("INTEGRATION_CHILD_REPORT").expect("INTEGRATION_CHILD_REPORT");
    let text = match run_topic(&topic) {
        Ok(report) => report.encode(),
        Err(error) => format!("ERROR\t{error}"),
    };
    std::fs::write(&path, text).expect("write the topic report");
}

/// `INTEGRATION_TIMING=1`: where the replay's time went -- the slowest
/// topics, and the statement kinds by total time across every topic.
fn print_timing(topics: &[&str], reports: &[Result<TopicReport, String>], wall: std::time::Duration) {
    let mut by_topic: Vec<(std::time::Duration, &str)> = topics
        .iter()
        .zip(reports)
        .filter_map(|(topic, report)| report.as_ref().ok().map(|report| (report.elapsed, *topic)))
        .collect();
    let summed: std::time::Duration = by_topic.iter().map(|(elapsed, _)| *elapsed).sum();
    by_topic.sort_by(|left, right| right.cmp(left));
    let mut by_kind: BTreeMap<String, (usize, std::time::Duration)> = BTreeMap::new();
    for report in reports.iter().flatten() {
        for (kind, (count, spent)) in &report.statement_time {
            let total = by_kind.entry(kind.clone()).or_default();
            total.0 += count;
            total.1 += *spent;
        }
    }
    let mut by_kind: Vec<_> = by_kind.into_iter().collect();
    by_kind.sort_by(|left, right| right.1 .1.cmp(&left.1 .1));
    eprintln!("replay wall time {wall:.1?}, summed topic time {summed:.1?}");
    eprintln!("slowest topics:");
    for (elapsed, topic) in by_topic.iter().take(15) {
        eprintln!("  {elapsed:>9.2?} {topic}");
    }
    eprintln!("statement kinds by total time:");
    for (kind, (count, spent)) in by_kind.iter().take(20) {
        eprintln!(
            "  {spent:>9.2?} {count:>6} x {kind:<24} {:>8.2?} each",
            *spent / u32::try_from(*count).unwrap_or(u32::MAX).max(1)
        );
    }
}

/// A topic listed twice is replayed twice, and every statement it compares is
/// counted twice in the headline totals -- which is exactly what happened when
/// `session/variable` and `table/cache` were each onboarded a second time by a
/// later unit that did not notice the first entry. The ratchet never lied (a
/// duplicated topic contributes its divergences twice, so the count still only
/// moves when real behaviour moves), but the compared/matched figures did, and
/// a number that overstates what was proved is the one number this driver may
/// not get wrong. Checked before the replay so the run stops rather than
/// reporting an inflated total.
fn assert_topics_are_unique() {
    let mut seen = BTreeMap::new();
    for topic in TOPICS {
        *seen.entry(*topic).or_insert(0usize) += 1;
    }
    let dupes: Vec<_> = seen
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|(t, n)| format!("{t} ({n}x)"))
        .collect();
    assert!(
        dupes.is_empty(),
        "TOPICS lists {} topic(s) more than once: {}. Each is replayed once per \
         entry and counted once per replay, inflating the compared and matched \
         totals. Remove the extra entry.",
        dupes.len(),
        dupes.join(", ")
    );
}

#[test]
fn topics_are_listed_once_each() {
    assert_topics_are_unique();
}

/// How far the warning comparison actually reaches, stated as a number instead
/// of assumed.
///
/// [`compare`] asks `SHOW WARNINGS` for a statement ONLY when `stmt.warnings`
/// is set, and that flag comes from the `--enable_warnings` directive in the
/// `.test` script -- not from anything this engine or TiDB did. Every other
/// statement is compared on its rows alone, so a warning TiDB raises and this
/// tier does not (or the reverse) leaves the rows identical and the replay
/// calls it a match.
///
/// That is not a hypothesis. This test parses the onboarded scripts with the
/// replay's own reader and prints the current reach. The latest measured line
/// is `warning gate reaches 704 of 21636 statements across 153 topics`; it is
/// the reason a fix that adds a real warning outside those 704 can move
/// neither ratchet.
///
/// The blind spot is in the RECORDING, not in this reader, and that is the
/// part worth knowing before anyone tries to close it. mysqltest writes a
/// warning into `.result` only under `--enable_warnings` or for an explicit
/// `show warnings;` (7 more statements, compared as ordinary rows). Everywhere
/// else TiDB's warnings were never captured: `executor/analyze` records
/// nothing for `set @@session.tidb_enable_fast_analyze=1` and only shows that
/// it warns because the script asks on the next line. So there is no reader
/// change that widens this gate against the recordings we have -- comparing
/// warnings outside the printed gate needs TiDB's warnings from somewhere else, a
/// re-recording or a live server, not a better comparison.
///
/// The number is pinned so that widening or narrowing the warning gate is a
/// visible edit rather than a silent one. Raising the covered count is
/// progress; the total moving means TOPICS changed and both figures should be
/// re-read, not patched.
#[test]
fn warning_comparison_covers_only_enable_warnings_statements() {
    let dir = integrationtest_dir();
    let mut covered = 0;
    let mut total = 0;
    let mut per_topic = Vec::new();
    for topic in TOPICS {
        let script = fs::read_to_string(dir.join(format!("t/{topic}.test")))
            .unwrap_or_else(|e| panic!("read t/{topic}.test: {e}"));
        let items = parse_test(&script).unwrap_or_else(|e| panic!("parse t/{topic}.test: {e}"));
        let stmts: Vec<&Stmt> = items
            .iter()
            .filter_map(|item| match item {
                Item::Stmt(stmt) => Some(stmt),
                _ => None,
            })
            .collect();
        let warned = stmts.iter().filter(|stmt| stmt.warnings).count();
        covered += warned;
        total += stmts.len();
        if warned > 0 {
            per_topic.push(format!("{topic}: {warned} of {}", stmts.len()));
        }
    }
    eprintln!(
        "warning gate reaches {covered} of {total} statements across {} topics\n  {}",
        TOPICS.len(),
        per_topic.join("\n  ")
    );
    // Re-read, not patched. The enrollment census added 57 topics and 3,865
    // statements (6,882 + 3,865 = 10,747), and the WARNING half of the move is
    // TWO of them: `expression/noop_functions` runs 17 statements under
    // `--enable_warnings` -- it is a topic about which statements raise a
    // warning instead of an error, so a high ratio is what it is FOR -- and
    // `table/index` runs 1. 31 + 17 + 1 = 49. The other 55 new topics add
    // 3,787 statements and NOT ONE warning-gated statement, so the gate's
    // reach per topic did not change; it was extended by exactly the two
    // topics that use the directive.
    //
    // Re-read again after batch57's three enrollments (`window_function`,
    // `executor/expand`, `session/vars`), which add 225 statements
    // (10,747 + 225 = 10,972). The WARNING half of that move is ONE of the
    // three: `session/vars` runs 8 statements under `--enable_warnings`, which
    // is what a topic about variable behavior would: the warning is how a
    // variable reports that it refused or clamped a value. `window_function`
    // and `executor/expand` add 98 statements and NOT ONE warning-gated
    // statement. 49 + 8 = 57.
    //
    // The harness-alignment enrollment adds
    // `planner/core/integration_partition`, whose output line is `5 of 493`.
    // The resulting current oracle line is `warning gate reaches 62 of 11465
    // statements across 110 topics`.
    //
    // Re-read after TOPICS grew from 110 to 153 (10,171 more statements,
    // 11,465 + 10,171 = 21,636). The WARNING half of that move is SEVEN of
    // the 43 new topics, each running real `--enable_warnings` blocks whose
    // appended `SHOW WARNINGS` the replay compares: `planner/core/casetest/
    // rule/rule_join_reorder` 404 of 504 and `planner/core/casetest/hint/
    // hint` 119 of 182 -- topics about which hints a plan honors, where the
    // warning IS the answer for a hint that cannot be applied -- then
    // `executor/insert` 89, `executor/write` 14, `expression/time` 12,
    // `planner/core/indexmerge_path` 3 and `session/clustered_index` 1.
    // 62 + 404 + 119 + 89 + 14 + 12 + 3 + 1 = 704. The other 36 new topics
    // add NOT ONE warning-gated statement.
    assert_eq!(
        (covered, total),
        (704, 21636),
        "the warning gate's reach changed; re-read what it now covers rather \
         than updating this number to match"
    );
}

#[test]
fn integrationtest_replay_matches_recorded_tidb_output() {
    assert_topics_are_unique();

    let started = std::time::Instant::now();
    let reports = run_topics(TOPICS);
    let wall = started.elapsed();
    if std::env::var_os("INTEGRATION_TIMING").is_some() {
        print_timing(TOPICS, &reports, wall);
    }

    let mut total = TopicReport::default();
    let mut per_topic = Vec::new();
    for (topic, report) in TOPICS.iter().zip(reports) {
        let report = report.unwrap_or_else(|e| panic!("topic {topic}: {e}"));
        per_topic.push(format!(
            "{topic}: {} matched {:?}, {} diverged, {} skipped of {}",
            report.matched_total(),
            report.matched,
            report.divergences.len(),
            report.skipped.values().sum::<usize>(),
            report.total()
        ));
        total.divergences.extend(report.divergences);
        for (kind, count) in report.matched {
            *total.matched.entry(kind).or_default() += count;
        }
        for (class, count) in report.skipped {
            *total.skipped.entry(class).or_default() += count;
        }
    }

    eprintln!(
        "integrationtest replay over {} topics: {} of {} statements compared\n  {}\nmatches by kind: {:?}\nskips by class: {:?}",
        TOPICS.len(),
        total.matched_total() + total.divergences.len(),
        total.total(),
        per_topic.join("\n  "),
        total.matched,
        total.skipped
    );

    // Statements whose RECORDING is older than the Go on this branch, each
    // confirmed by running the statement through that Go (the local oracle):
    // Go now prints `USE_INDEX(`t`)` where the recording has `USE_INDEX(`t` )`.
    // The list must stay exact: a statement that stops diverging has to leave
    // it, and one that is not on it is a real divergence.
    const KNOWN_STALE_RECORDINGS: &[&str] = &[
        "batch on a limit 10 dry run insert into t1 select /*+ USE_INDEX(t) */ * from t;",
        "batch on a limit 10 dry run insert into t1 select /*+ USE_INDEX(t) */ * from t on duplicate key update t1.b=t.b;",
        "batch on a limit 10 dry run delete /*+ USE_INDEX(t) */ from t;",
    ];
    // EXPLAIN text is compared whole, and some plans still differ from Go's in
    // id allocation, estimates or operator choice (measured 2026-10-17: 37; 82 after
    // the JSON argument casts, the RANGE pruner, the handle ordering and derived TopN, mostly
    // index-merge paths and grouped-range ordered scans; 0 on 2026-10-25). They
    // are budgeted apart: the number may only go down, and lowering the
    // constant records a fixed plan.
    const PLAN_TEXT_BUDGET: usize = 0;
    let (plan_text, other): (Vec<&String>, Vec<&String>) = total
        .divergences
        .iter()
        .partition(|d| d.contains(difftest_result_tests::replay::PLAN_TEXT_TAG));
    eprintln!("EXPLAIN texts differing from TiDB's: {}", plan_text.len());
    if std::env::var_os("INTEGRATION_SHOW_PLAN_DIVERGENCES").is_some() {
        for divergence in &plan_text {
            eprintln!("PLAN-DIVERGENCE{divergence}");
        }
    }
    assert!(
        plan_text.len() <= PLAN_TEXT_BUDGET,
        "{} EXPLAIN texts differ from TiDB's (budget {PLAN_TEXT_BUDGET}); new ones:{}",
        plan_text.len(),
        plan_text.iter().map(|d| d.as_str()).collect::<String>()
    );
    let (stale, real): (Vec<&String>, Vec<&String>) = other.into_iter().partition(|d| {
        KNOWN_STALE_RECORDINGS
            .iter()
            .any(|sql| d.contains(&format!("] {sql}")))
    });
    let still_diverging = KNOWN_STALE_RECORDINGS
        .iter()
        .filter(|sql| stale.iter().any(|d| d.contains(&format!("] {sql}"))))
        .count();
    assert_eq!(
        still_diverging,
        KNOWN_STALE_RECORDINGS.len(),
        "a statement on KNOWN_STALE_RECORDINGS no longer diverges: remove it from the list"
    );
    assert!(
        real.is_empty(),
        "{} of {} compared statements diverge from TiDB's recording:{}",
        real.len(),
        total.matched_total() + total.divergences.len(),
        real.iter().map(|d| d.as_str()).collect::<String>()
    );
    // Rust refusing what Go answered is skipped (`OutOfDomain`), which can hide
    // a regression: it once grew from 5 to 61 with no divergence reported. The
    // budget may only go down; lowering it is the way to record a fix.
    const OUT_OF_DOMAIN_BUDGET: usize = 5;
    let out_of_domain = total
        .skipped
        .iter()
        .filter(|(class, _)| format!("{class:?}") == "OutOfDomain")
        .map(|(_, count)| *count)
        .sum::<usize>();
    assert!(
        out_of_domain <= OUT_OF_DOMAIN_BUDGET,
        "{out_of_domain} statements are out of domain (budget {OUT_OF_DOMAIN_BUDGET}): \
         rerun with INTEGRATION_SHOW_OUT_OF_DOMAIN=1 to see what Rust now refuses"
    );
}

/// Lists every topic this reader cannot ALIGN, with the cause for each.
///
/// An unaligned topic is worse than a refused one. A refused statement is
/// counted in a named [`SkipClass`] and appears in every total this driver
/// prints; a topic that does not align is not compared, not refused, and not
/// counted -- it is simply ABSENT from every number the survey reports, and
/// nothing in a green run points at it. That is the same shape as the
/// instrument bugs this ring has already found, so the inventory is kept as a
/// standing tool rather than being rediscovered each time.
///
/// This is cheap on purpose: it reads the two files and runs `parse_test` +
/// `align`, which is where alignment is decided, so it does not need to
/// execute a single statement and finishes in under a second over all topics.
///
/// ```sh
/// cargo test -p difftest-result-tests --test integration_diff -- \
///   --ignored --nocapture survey_unaligned
/// ```
///
#[test]
#[ignore = "inventory tool: lists every topic the reader cannot align, with its cause"]
fn survey_unaligned_topics() {
    let dir = integrationtest_dir();
    let mut unaligned = 0usize;
    let mut aligned = 0usize;
    for topic in all_topics() {
        let script = match fs::read_to_string(dir.join(format!("t/{topic}.test"))) {
            Ok(text) => text,
            Err(e) => {
                unaligned += 1;
                eprintln!("UNALIGNED  {topic}: read .test: {e}");
                continue;
            }
        };
        let result_path = recording_path(&dir, &topic);
        let recorded = match fs::read(&result_path) {
            Ok(bytes) => bytes,
            Err(e) => {
                unaligned += 1;
                eprintln!("UNALIGNED  {topic}: read {}: {e}", result_path.display());
                continue;
            }
        };
        match parse_test(&script).and_then(|items| {
            let count = items.len();
            align_bytes(&items, &recorded).map(|_| count)
        }) {
            Ok(count) => {
                aligned += 1;
                if std::env::var_os("INTEGRATION_SHOW_ALIGNED").is_some() {
                    eprintln!("aligned    {topic}: {count} items");
                }
            }
            Err(reason) => {
                unaligned += 1;
                eprintln!("UNALIGNED  {topic}: {reason}");
            }
        }
    }
    eprintln!("{aligned} topics align, {unaligned} do not");
}

/// Every topic in the suite, as `t/<topic>.test` relative paths without the
/// extension.
fn all_topics() -> Vec<String> {
    let root = integrationtest_dir().join("t");
    let mut topics = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(at) = stack.pop() {
        for entry in fs::read_dir(&at).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "test") {
                topics.push(
                    path.strip_prefix(&root)
                        .unwrap()
                        .with_extension("")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    topics.sort();
    topics
}

/// Replays the single topic named by `INTEGRATION_TOPIC`, printing one status
/// line. This is the survey's child process (see
/// [`survey_unonboarded_topics`]) and also the way to look at one topic by
/// hand:
///
/// ```sh
/// INTEGRATION_TOPIC=executor/join cargo test -p difftest-result-tests \
///   --test integration_diff -- --ignored --nocapture replay_one_topic
/// ```
#[test]
#[ignore = "onboarding tool: replays the one topic named by INTEGRATION_TOPIC"]
fn replay_one_topic_from_env() {
    let topics = std::env::var("INTEGRATION_TOPIC").expect("INTEGRATION_TOPIC must name a topic");
    // A comma-separated list replays its topics in order in this one process,
    // as the corpus test does, which is what exposes state one topic leaves
    // behind for the next.
    for topic in topics
        .split(',')
        .map(str::trim)
        .filter(|topic| !topic.is_empty())
    {
        replay_topic_reporting(topic);
    }
}

fn replay_topic_reporting(topic: &str) {
    let topic = topic.to_owned();
    let started = std::time::Instant::now();
    match run_topic(&topic) {
        Ok(report) => {
            eprintln!(
                "{:>5} matched {:>5} diverged of {:>5} in {:>6}ms  {topic}  {:?} {:?}",
                report.matched_total(),
                report.divergences.len(),
                report.total(),
                started.elapsed().as_millis(),
                report.matched,
                report.skipped,
            );
            if std::env::var_os("INTEGRATION_SHOW_DIVERGENCES").is_some() {
                eprintln!("{}", report.divergences.join(""));
            }
            if std::env::var_os("INTEGRATION_TIMING").is_some() {
                let mut kinds: Vec<_> = report.statement_time.iter().collect();
                kinds.sort_by(|left, right| right.1 .1.cmp(&left.1 .1));
                for (kind, (count, spent)) in kinds.iter().take(12) {
                    eprintln!("  {spent:>9.2?} {count:>6} x {kind}");
                }
            }
        }
        Err(reason) => eprintln!("UNALIGNED {topic}: {reason}"),
    }
}

/// Ranks the topics that are NOT yet onboarded by how much of each already
/// replays, so the next onboarding increment is chosen by evidence instead of
/// by name. Ignored by default: it surveys the fixture topics and is an onboarding
/// tool, not a gate.
///
/// Each topic runs in its own CHILD PROCESS, because a survey of an engine
/// under construction meets outcomes a `catch_unwind` cannot survive: a stack
/// overflow aborts the process outright, and a statement that does not
/// terminate would hang the whole sweep. Isolation turns both into one
/// reported line for one topic instead of the end of the run.
///
/// ```sh
/// cargo test -p difftest-result-tests --test integration_diff -- --ignored --nocapture survey
/// ```
///
#[test]
#[ignore = "onboarding tool: ranks fixture topics for enrollment"]
fn survey_unonboarded_topics() {
    let dir = integrationtest_dir();
    let mut topics = Vec::new();
    let mut stack = vec![dir.join("t")];
    while let Some(at) = stack.pop() {
        for entry in fs::read_dir(&at).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "test") {
                topics.push(
                    path.strip_prefix(dir.join("t"))
                        .unwrap()
                        .with_extension("")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    topics.sort();

    for topic in &topics {
        // A topic already on the gate is not a candidate, and printing it as
        // one is how `session/variable` and `table/cache` each got onboarded
        // twice: the second unit read this survey's output, saw a topic that
        // replayed clean, and added it. Naming the onboarded ones here is the
        // guard at the point where the mistake was actually made --
        // `assert_topics_are_unique` is the backstop behind it.
        if TOPICS.contains(&topic.as_str()) {
            eprintln!("ONBOARDED  {topic}");
            continue;
        }
        match replay_in_child(topic) {
            Ok(()) => {}
            Err(status) => eprintln!("{status}  {topic}"),
        }
    }
}

/// Runs one topic in a child process, returning `Err` with the outcome when the
/// child did not report for itself. The child inherits stderr, so a successful
/// replay's own status line is already on the terminal.
fn replay_in_child(topic: &str) -> Result<(), String> {
    /// A topic that hangs must not hang the sweep. Every onboarded topic
    /// replays in tens of milliseconds; a second is already three orders of
    /// magnitude of headroom.
    const BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "--nocapture",
            "--ignored",
            "replay_one_topic_from_env",
        ])
        .env("INTEGRATION_TOPIC", topic)
        .stdout(std::process::Stdio::null())
        .spawn()
        .map_err(|e| format!("SPAWN FAILED ({e})"))?;

    let deadline = std::time::Instant::now() + BUDGET;
    loop {
        match child.try_wait().map_err(|e| format!("WAIT FAILED ({e})"))? {
            Some(status) if status.success() => return Ok(()),
            Some(status) => return Err(format!("CRASHED ({status})")),
            None if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("DID NOT FINISH IN {BUDGET:?}"));
            }
            None => std::thread::sleep(std::time::Duration::from_millis(20)),
        }
    }
}
