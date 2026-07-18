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

#![allow(dead_code, missing_docs)]

use std::time::Duration;

#[path = "../src/region/health_policy.rs"]
mod health_policy;
#[path = "../src/region/slow_score.rs"]
mod slow_score;
#[path = "../src/region/store_health.rs"]
mod store_health;

use health_policy::{ReplicaHealthFacts, ReplicaHealthPolicy, StoreLabel, StoreSelectionScore};
use slow_score::SlowScoreStat;
use store_health::{StoreHealth, StoreHealthDetail, StoreLoad, StoreRoutingHealth};

fn label(key: &str, value: &str) -> StoreLabel {
    StoreLabel {
        key: key.to_owned(),
        value: value.to_owned(),
    }
}

fn facts<'a>(
    labels: &'a [StoreLabel],
    is_leader: bool,
    is_learner: bool,
    attempts: u8,
    slow: bool,
) -> ReplicaHealthFacts<'a> {
    ReplicaHealthFacts {
        store_id: 1,
        labels,
        is_leader,
        is_learner,
        attempts,
        reported_busy: false,
        health: StoreHealthDetail {
            client_side_slow_score: if slow { 80 } else { 1 },
            tikv_side_slow_score: 1,
        },
        load: StoreLoad::default(),
    }
}

#[test]
fn score_precedence_and_source_calculate_score_matrix_are_exact() {
    assert!(StoreSelectionScore::NOT_SLOW > StoreSelectionScore::LABEL_MATCHES);
    assert!(StoreSelectionScore::LABEL_MATCHES > StoreSelectionScore::PREFER_LEADER);
    assert!(StoreSelectionScore::PREFER_LEADER > StoreSelectionScore::NORMAL_PEER);
    assert!(StoreSelectionScore::NORMAL_PEER > StoreSelectionScore::NOT_ATTEMPTED);

    let local = [label("zone", "west")];
    let remote = [label("zone", "east")];
    struct Case<'a> {
        policy: ReplicaHealthPolicy,
        facts: ReplicaHealthFacts<'a>,
        expected: u8,
    }
    let cases = [
        Case {
            policy: ReplicaHealthPolicy::default(),
            facts: facts(&local, true, false, 0, false),
            expected: 8 | 16 | 1,
        },
        Case {
            policy: ReplicaHealthPolicy::default(),
            facts: facts(&local, false, false, 0, false),
            expected: 8 | 2 | 16 | 1,
        },
        Case {
            policy: ReplicaHealthPolicy::default(),
            facts: facts(&local, true, false, 0, true),
            expected: 8 | 1,
        },
        Case {
            policy: ReplicaHealthPolicy::default(),
            facts: facts(&local, false, false, 0, true),
            expected: 8 | 2 | 1,
        },
        Case {
            policy: ReplicaHealthPolicy {
                try_leader: true,
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&local, true, false, 0, true),
            expected: 8 | 2 | 1,
        },
        Case {
            policy: ReplicaHealthPolicy {
                prefer_leader: true,
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&local, true, false, 0, true),
            expected: 8 | 2 | 1,
        },
        Case {
            policy: ReplicaHealthPolicy {
                learner_only: true,
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&local, false, false, 0, true),
            expected: 8 | 1,
        },
        Case {
            policy: ReplicaHealthPolicy {
                labels: vec![label("zone", "east")],
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&local, false, false, 0, true),
            expected: 2 | 1,
        },
        Case {
            policy: ReplicaHealthPolicy {
                try_leader: true,
                labels: vec![label("zone", "east")],
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&local, true, false, 0, true),
            expected: 4 | 1,
        },
        Case {
            policy: ReplicaHealthPolicy {
                try_leader: true,
                labels: vec![label("zone", "east")],
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&local, false, false, 0, true),
            expected: 2 | 1,
        },
        Case {
            policy: ReplicaHealthPolicy {
                prefer_leader: true,
                labels: vec![label("zone", "east")],
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&remote, true, false, 0, true),
            expected: 8 | 2 | 1,
        },
        Case {
            policy: ReplicaHealthPolicy {
                learner_only: true,
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&local, false, true, 1, false),
            expected: 8 | 2 | 16,
        },
        Case {
            policy: ReplicaHealthPolicy {
                stores: vec![2],
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&local, false, false, 1, false),
            expected: 2 | 16,
        },
        Case {
            policy: ReplicaHealthPolicy {
                prefer_leader: true,
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&local, true, false, 1, false),
            expected: 8 | 4 | 16,
        },
        Case {
            policy: ReplicaHealthPolicy {
                prefer_leader: true,
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&local, false, false, 1, false),
            expected: 8 | 2 | 16,
        },
        Case {
            policy: ReplicaHealthPolicy {
                prefer_leader: true,
                ..ReplicaHealthPolicy::default()
            },
            facts: facts(&local, false, false, 1, true),
            expected: 8 | 2,
        },
    ];

    for (index, case) in cases.into_iter().enumerate() {
        assert_eq!(
            case.policy.score(case.facts).bits(),
            case.expected,
            "score case {index}"
        );
    }
}

#[test]
fn label_matching_requires_every_pair_and_falls_back_instead_of_failing() {
    let labels = [label("zone", "west"), label("disk", "ssd")];
    let policy = ReplicaHealthPolicy {
        labels: vec![label("zone", "west"), label("disk", "ssd")],
        ..ReplicaHealthPolicy::default()
    };
    assert_eq!(
        policy.score(facts(&labels, false, false, 0, false)).bits(),
        27
    );

    let partial = [label("zone", "west")];
    assert_eq!(
        policy.score(facts(&partial, false, false, 0, false)).bits(),
        19
    );
}

#[test]
fn optimistic_wait_decays_and_busy_comparison_is_strict() {
    let mut load = StoreLoad::default();
    load.update(Duration::from_millis(500), Duration::from_millis(10));
    assert_eq!(
        load.estimated_wait(Duration::from_millis(210)),
        Duration::from_millis(300)
    );
    assert_eq!(
        load.estimated_wait(Duration::from_millis(510)),
        Duration::ZERO
    );

    load.update(Duration::from_millis(50), Duration::from_secs(1));
    let policy = ReplicaHealthPolicy {
        busy_threshold: Duration::from_millis(50),
        ..ReplicaHealthPolicy::default()
    };
    let mut equal = facts(&[], false, false, 0, false);
    equal.load = load;
    assert!(policy.is_candidate(equal, Duration::from_secs(1)));

    load.update(Duration::from_millis(51), Duration::from_secs(1));
    let mut greater = equal;
    greater.load = load;
    assert!(!policy.is_candidate(greater, Duration::from_secs(1)));

    let leader = facts(&[], true, false, 0, false);
    assert!(!policy.is_candidate(leader, Duration::from_secs(1)));
}

#[test]
fn positive_busy_estimates_update_load_and_zero_marks_slow() {
    let mut routing = StoreRoutingHealth::default();
    routing.observe_server_busy(500, Duration::from_secs(1));
    assert_eq!(
        routing.load.estimated_wait(Duration::from_millis(1_200)),
        Duration::from_millis(300)
    );
    assert!(!routing.health.is_slow());

    routing.observe_server_busy(0, Duration::from_secs(2));
    assert!(routing.health.is_slow());
    assert_eq!(routing.health.detail().client_side_slow_score, 100);
}

#[test]
fn five_idle_replica_source_cases_preserve_no_invalidation_fallback() {
    let now = Duration::from_secs(1);
    let policy = ReplicaHealthPolicy {
        busy_threshold: Duration::from_millis(50),
        ..ReplicaHealthPolicy::default()
    };
    let mut loads = [StoreLoad::default(); 3];
    let make_facts = |index: usize, busy: bool, loads: &[StoreLoad; 3]| ReplicaHealthFacts {
        store_id: index as u64 + 1,
        labels: &[],
        is_leader: index == 0,
        is_learner: false,
        attempts: 0,
        reported_busy: busy,
        health: StoreHealthDetail {
            client_side_slow_score: 1,
            tikv_side_slow_score: 1,
        },
        load: loads[index],
    };

    // Source case 1: writes do not enter this health policy; their selector
    // remains the ordinary leader path. The pure leaf therefore begins with
    // the first read diversion and always filters the leader while enabled.
    let first = [
        make_facts(0, true, &loads),
        make_facts(1, false, &loads),
        make_facts(2, false, &loads),
    ];
    assert_eq!(policy.select(&first, now, 0), Some(1));

    // Source case 2: a 500 ms leader estimate and 800 ms first-follower
    // estimate rotate to the other follower without backoff.
    loads[0].update(Duration::from_millis(500), now);
    loads[1].update(Duration::from_millis(800), now);
    let second = [
        make_facts(0, true, &loads),
        make_facts(1, true, &loads),
        make_facts(2, false, &loads),
    ];
    assert_eq!(policy.select(&second, now, 0), Some(2));

    // Source case 3: after the final 150 ms estimate every replica is busy.
    // Returning None is the typed no-idle result; RegionCache remains valid.
    loads[2].update(Duration::from_millis(150), now);
    let all_busy = [
        make_facts(0, true, &loads),
        make_facts(1, true, &loads),
        make_facts(2, true, &loads),
    ];
    assert_eq!(policy.select(&all_busy, now, 0), None);

    // Source cases 4 and 5 vary leader changes/errors, but retain the same
    // invariant: no-idle is not region exhaustion, and clearing the threshold
    // is a request-local transition before the current leader is retried.
    // A later request has no request-local busy flags. After 120 ms, the
    // 150 ms store is below threshold while the 500/800 ms stores remain busy.
    let later = now + Duration::from_millis(120);
    let fresh_request = [
        make_facts(0, false, &loads),
        make_facts(1, false, &loads),
        make_facts(2, false, &loads),
    ];
    assert_eq!(policy.select(&fresh_request, later, 0), Some(2));
}

#[test]
fn thirty_second_request_immediately_marks_client_slow() {
    let mut health = StoreHealth::default();
    health.tick(Duration::ZERO);
    health.record_client_duration(Duration::from_secs(30));
    assert!(health.is_slow());
    assert_eq!(health.detail().client_side_slow_score, 100);
}

#[test]
fn source_slow_score_rising_and_falling_latency_stays_below_threshold() {
    let mut score = SlowScoreStat::default();
    assert!(!score.is_slow());
    score.record(Duration::from_millis(1));
    score.tick();
    assert!(!score.is_slow());
    for millis in 2..=100 {
        score.record(Duration::from_millis(millis));
        if millis % 5 == 0 {
            score.tick();
            assert!(!score.is_slow());
        }
    }
    for millis in (2..=100).rev() {
        score.record(Duration::from_millis(millis));
        if millis % 5 == 0 {
            score.tick();
            assert!(!score.is_slow());
        }
    }
    score.mark_already_slow();
    assert!(score.is_slow());
}

#[test]
fn tikv_score_rate_limit_threshold_and_linear_decay_match_source() {
    let mut health = StoreHealth::default();
    let start = Duration::from_secs(1);
    assert_eq!(health.detail().tikv_side_slow_score, 0);
    health.tick(start);
    assert!(!health.is_slow());

    assert!(health.update_tikv_score(50, start + Duration::from_millis(200)));
    assert!(!health.is_slow());
    assert!(!health.update_tikv_score(100, start + Duration::from_millis(250)));
    assert_eq!(health.detail().tikv_side_slow_score, 50);
    assert!(health.update_tikv_score(100, start + Duration::from_millis(400)));
    assert!(health.is_slow());

    health.tick(start + Duration::from_secs(120) + Duration::from_millis(400));
    assert_eq!(health.detail().tikv_side_slow_score, 60);
    assert!(!health.is_slow());
    health.tick(start + Duration::from_secs(300) + Duration::from_millis(400));
    assert_eq!(health.detail().tikv_side_slow_score, 1);
}

#[test]
fn unchanged_tikv_score_above_one_refreshes_decay_clock() {
    let mut health = StoreHealth::default();
    assert!(health.update_tikv_score(90, Duration::from_secs(1)));
    assert!(health.update_tikv_score(90, Duration::from_secs(2)));
    health.tick(Duration::from_secs(16));
    assert_eq!(health.detail().tikv_side_slow_score, 90);
    health.tick(Duration::from_secs(17));
    assert_eq!(health.detail().tikv_side_slow_score, 85);
}

#[test]
fn prefer_leader_filters_slow_followers_but_not_slow_leader() {
    let policy = ReplicaHealthPolicy {
        prefer_leader: true,
        ..ReplicaHealthPolicy::default()
    };
    assert!(!policy.is_candidate(facts(&[], false, false, 0, true), Duration::ZERO));
    assert!(policy.is_candidate(facts(&[], true, false, 0, true), Duration::ZERO));
}
