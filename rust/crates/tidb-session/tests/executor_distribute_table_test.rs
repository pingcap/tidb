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

//! Go `pkg/executor/distribute_table_test.go`, on the mock store through the
//! session, with the info syncer's PD client replaced
//! (`infosync.SetPDHttpCliForTest`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tikv_client::pd_http;

use crate::support::testkit::*;

const SCHEDULER_NAME: &str = "balance-range-scheduler";

/// Go `MockDistributePDCli`: the scheduler jobs it holds, the calls to
/// `GetSchedulerConfig` it answers with nothing first (`skip`), the inputs
/// `CreateSchedulerWithInput` received, and the distributions it answers.
#[derive(Default)]
struct MockDistributePdCli {
    state: Mutex<MockState>,
}

#[derive(Default)]
struct MockState {
    jobs: Vec<serde_json::Map<String, Value>>,
    skip: usize,
    count: usize,
    created: Vec<HashMap<String, Value>>,
    distributions: HashMap<(Vec<u8>, Vec<u8>), pd_http::RegionDistributions>,
}

impl tidb_domain_infosync::region::PdHttpClient for MockDistributePdCli {
    fn get_region_distribution_by_key_range(
        &self,
        key_range: &pd_http::KeyRange,
        _engine: &str,
    ) -> Result<pd_http::RegionDistributions, String> {
        let state = self.state.lock().unwrap();
        state
            .distributions
            .get(&(key_range.start_key.clone(), key_range.end_key.clone()))
            .cloned()
            .ok_or_else(|| "unexpected key range".to_owned())
    }

    fn get_scheduler_config(&self, scheduler_name: &str) -> Result<Value, String> {
        assert_eq!(scheduler_name, SCHEDULER_NAME);
        let mut state = self.state.lock().unwrap();
        if state.count < state.skip {
            state.count += 1;
            return Ok(Value::Null);
        }
        Ok(Value::Array(state.jobs.iter().cloned().map(Value::Object).collect()))
    }

    fn create_scheduler_with_input(
        &self,
        name: &str,
        input: &mut HashMap<String, Value>,
    ) -> Result<(), String> {
        assert_eq!(name, SCHEDULER_NAME);
        let mut state = self.state.lock().unwrap();
        state.created.push(input.clone());
        let mut job: serde_json::Map<String, Value> = input.clone().into_iter().collect();
        job.insert("job-id".to_owned(), json!((state.jobs.len() + 1) as u64));
        job.insert("status".to_owned(), json!("pending"));
        state.jobs.push(job);
        Ok(())
    }

    fn cancel_scheduler_job(&self, scheduler_name: &str, job_id: u64) -> Result<(), String> {
        assert_eq!(scheduler_name, SCHEDULER_NAME);
        let state = self.state.lock().unwrap();
        if state.jobs.iter().any(|job| job.get("job-id").and_then(Value::as_f64) == Some(job_id as f64)) {
            return Ok(());
        }
        Err("job not found".to_owned())
    }
}

fn install(cli: &Arc<MockDistributePdCli>) -> impl FnOnce() {
    tidb_domain_infosync::region::set_pd_http_cli_for_test(Arc::clone(cli) as Arc<dyn tidb_domain_infosync::region::PdHttpClient>)
}

fn table_key(pid: i64) -> Vec<u8> {
    let mut key = Vec::new();
    tidb_codec::encode_bytes(&mut key, &tidb_tablecodec::table_key::gen_table_prefix(pid));
    key
}

/// The physical IDs of `table` (its partitions, `partitions` when named).
fn physical_ids(tk: &mut TestKit, table: &str, partitions: &[&str]) -> Vec<i64> {
    let rows = tk.must_query(&format!(
        "select partition_name, tidb_partition_id from information_schema.partitions \
         where table_schema = 'test' and table_name = '{table}' and partition_name is not null"
    ));
    let mut ids = Vec::new();
    for row in rows.rows() {
        if partitions.is_empty() || partitions.iter().any(|p| p.eq_ignore_ascii_case(&row[0])) {
            ids.push(row[1].parse().unwrap());
        }
    }
    if rows.rows().is_empty() {
        let table_id = tk.must_query(&format!(
            "select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '{table}'"
        ));
        ids.push(table_id.rows()[0][0].parse().unwrap());
    }
    ids.sort_unstable();
    ids
}

/// Go `TestShowDistributionJobs`.
#[test]
fn test_show_distribution_jobs() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");

    let cli = Arc::new(MockDistributePdCli::default());
    let recover_cli = install(&cli);
    assert!(tk.must_query("show distribution jobs").rows().is_empty());

    let now = chrono::Local::now();
    let create = now - chrono::Duration::minutes(1);
    let start = now - chrono::Duration::seconds(30);
    let job = json!({
        "job-id": 1.0,
        "alias": (["test", "test", "partition(P0,P1)"].join(".")),
        "engine": "tikv",
        "rule": "leader-scatter",
        "create": create.to_rfc3339_opts(chrono::SecondsFormat::Nanos, false),
        "start": start.to_rfc3339_opts(chrono::SecondsFormat::Nanos, false),
        "status": "finished",
        "timeout": 30 * 60 * 1_000_000_000_i64,
    });
    cli.state.lock().unwrap().jobs = vec![job.as_object().unwrap().clone()];

    tk.must_query("show distribution jobs").check([format!(
        "1 test test partition(P0,P1) tikv leader-scatter finished 30m0s {} {} <nil>",
        create.format("%Y-%m-%d %H:%M:%S"),
        start.format("%Y-%m-%d %H:%M:%S"),
    )]);

    let all = tk.must_query("show distribution jobs").rows().to_vec();
    assert_eq!(tk.must_query("show distribution jobs where `job_id`=1").rows(), all.as_slice());
    let one = tk.must_query("show distribution job 1").rows().to_vec();
    assert_eq!(tk.must_query("show distribution jobs where `job_id`=1").rows(), one.as_slice());
    let zero = tk.must_query("show distribution job 0").rows().to_vec();
    assert_eq!(tk.must_query("show distribution jobs where `job_id`=0").rows(), zero.as_slice());
    recover_cli();
}

/// Go `TestDistributeTable`.
#[test]
fn test_distribute_table() {
    let mut tk = TestKit::new();
    let database = "test";
    tk.must_exec(&format!("use {database}"));

    let cli = Arc::new(MockDistributePdCli::default());
    cli.state.lock().unwrap().skip = 1;
    let recover_cli = install(&cli);
    // The input Go's `mockCreateSchedulerWithInput` expects.
    let expected = |tk: &mut TestKit, table: &str, config: &HashMap<String, Value>, partitions: &[&str]| {
        let pids = physical_ids(tk, table, partitions);
        let mut starts: Vec<String> = Vec::new();
        let mut ends: Vec<String> = Vec::new();
        for (i, pid) in pids.iter().enumerate() {
            let (start, end) = pd_http::KeyRange::new(table_key(*pid), table_key(pid + 1)).escape_as_utf8_str();
            if i == 0 || *pid != pids[i - 1] + 1 {
                starts.push(start);
                ends.push(end);
            } else {
                *ends.last_mut().unwrap() = end;
            }
        }
        let mut config = config.clone();
        config.insert("start-key".to_owned(), json!(starts.join(",")));
        config.insert("end-key".to_owned(), json!(ends.join(",")));
        config
    };
    let last_created = |cli: &MockDistributePdCli| cli.state.lock().unwrap().created.last().cloned().unwrap();
    let table = "t1";
    let mut partition = "";

    let mut config: HashMap<String, Value> = HashMap::from([
        ("alias".to_owned(), json!([database, table, partition].join("."))),
        ("engine".to_owned(), json!("tikv")),
        ("rule".to_owned(), json!("leader-scatter")),
    ]);
    tk.must_exec(
        "create table t1 (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
		partition p0 values less than (1024),
		partition p1 values less than (2048),
		partition p2 values less than (3072)
	   );",
    );
    tk.must_query(&format!("distribute table {table} rule='leader-scatter' engine='tikv'")).check(["1"]);
    assert_eq!(last_created(&cli), expected(&mut tk, table, &config, &[]));
    // test for multi partitions
    partition = "partition(p1,p2)";
    config.insert("alias".to_owned(), json!([database, table, partition].join(".")));
    tk.must_query(&format!("distribute table {table} partition (p1,p2) rule='leader-scatter' engine='tikv'"))
        .check(["2"]);
    assert_eq!(last_created(&cli), expected(&mut tk, table, &config, &["p1", "p2"]));
    // test for unordered partitions
    tk.must_query(&format!("distribute table {table} partition (p2,p1) rule='leader-scatter' engine='tikv'"))
        .check(["3"]);
    assert_eq!(last_created(&cli), expected(&mut tk, table, &config, &["p1", "p2"]));

    // test for timeout
    partition = "partition(p0)";
    config.insert("alias".to_owned(), json!([database, table, partition].join(".")));
    config.insert("timeout".to_owned(), json!("30m"));
    tk.must_query(&format!(
        "distribute table {table} partition(p0) rule='leader-scatter' engine='tikv' timeout='30m'"
    ))
    .check(["4"]);
    assert_eq!(last_created(&cli), expected(&mut tk, table, &config, &["p0"]));

    partition = "partition(p0,p2)";
    config.insert("alias".to_owned(), json!([database, table, partition].join(".")));
    config.insert("timeout".to_owned(), json!("30m"));
    tk.must_query(&format!(
        "distribute table {table} partition(p0,p2) rule='leader-scatter' engine='tikv' timeout='30m'"
    ))
    .check(["5"]);
    assert_eq!(last_created(&cli), expected(&mut tk, table, &config, &["p0", "p2"]));

    // test for incorrect arguments
    tk.must_get_err_msg(
        &format!("distribute table {table} rule='leader-scatter' engine='tiflash'"),
        "[planner:1210]Incorrect arguments to the rule of tiflash must be learner-scatter",
    );
    tk.must_get_err_msg(
        &format!("distribute table {table} rule='leader-scatter' engine='titan'"),
        "[planner:1210]Incorrect arguments to engine must be tikv or tiflash",
    );
    tk.must_get_err_msg(
        &format!("distribute table {table} rule='witness' engine='tikv'"),
        "[planner:1210]Incorrect arguments to rule must be leader-scatter, peer-scatter or learner-scatter",
    );
    recover_cli();
}

/// Go `TestShowTableDistributions`.
#[test]
fn test_show_table_distributions() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");

    let cli = Arc::new(MockDistributePdCli::default());
    let recover_cli = install(&cli);
    let mock_distribution = |store_id: u64| pd_http::RegionDistribution {
        store_id,
        engine_type: "tikv".to_owned(),
        region_leader_count: 1,
        region_peer_count: 3,
        region_write_bytes: 100,
        region_write_keys: 10,
        region_write_query: 1,
        region_leader_read_bytes: 1000,
        region_leader_read_keys: 100,
        region_leader_read_query: 10,
        region_peer_read_keys: 10000,
        region_peer_read_bytes: 1000,
        region_peer_read_query: 100,
        ..pd_http::RegionDistribution::default()
    };
    let distributions = pd_http::RegionDistributions {
        region_distributions: vec![mock_distribution(1), mock_distribution(2)],
    };
    let mock_get_distributions = |tk: &mut TestKit, table: &str, partition: &str| {
        let ids = physical_ids(tk, table, if partition.is_empty() { &[] } else { std::slice::from_ref(&partition) });
        let id = ids[0];
        cli.state
            .lock()
            .unwrap()
            .distributions
            .insert((table_key(id), table_key(id + 1)), distributions.clone());
    };
    tk.must_exec("create table t1(a int)");
    mock_get_distributions(&mut tk, "t1", "");
    tk.must_query("show table t1 distributions").check([
        "t1 1 tikv 1 3 100 10 1 1000 100 10 1000 10000 100",
        "t1 2 tikv 1 3 100 10 1 1000 100 10 1000 10000 100",
    ]);

    // test for partition table distributions
    tk.must_exec(
        "create table tp1 (id int) PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (100),\
         PARTITION p1 VALUES LESS THAN (1000),\
         PARTITION p2 VALUES LESS THAN (10000)\
         )",
    );
    mock_get_distributions(&mut tk, "tp1", "p0");
    mock_get_distributions(&mut tk, "tp1", "p1");
    tk.must_query("show table tp1 partition(p0) distributions").check([
        "p0 1 tikv 1 3 100 10 1 1000 100 10 1000 10000 100",
        "p0 2 tikv 1 3 100 10 1 1000 100 10 1000 10000 100",
    ]);
    tk.must_query("show table tp1 partition(p0,p1) distributions").check([
        "p0 1 tikv 1 3 100 10 1 1000 100 10 1000 10000 100",
        "p0 2 tikv 1 3 100 10 1 1000 100 10 1000 10000 100",
        "p1 1 tikv 1 3 100 10 1 1000 100 10 1000 10000 100",
        "p1 2 tikv 1 3 100 10 1 1000 100 10 1000 10000 100",
    ]);
    recover_cli();
}

/// Go `TestCancelDistributionJob`.
#[test]
fn test_cancel_distribution_job() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    let cli = Arc::new(MockDistributePdCli::default());
    let recover_cli = install(&cli);
    // cancel job failed because job not found
    let err = tk.exec_to_err("cancel distribution job 1").expect("the job is unknown");
    assert!(err.error_string().contains("job not found"), "{}", err.error_string());

    // cancel job successfully
    cli.state.lock().unwrap().jobs.push(json!({"job-id": 1.0}).as_object().unwrap().clone());
    assert!(tk.exec_to_err("cancel distribution job 1").is_none());
    recover_cli();
}
