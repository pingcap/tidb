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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Exact ports of every resource-group row in Go's parser and AST restore
//! tests. The source inventory is intentionally table-shaped so additions to
//! either Go table have one obvious Rust landing point.

use super::*;

fn assert_parser_case(sql: &str, expected: Option<&str>) {
    match expected {
        Some(expected) => assert_eq!(r(sql), expected, "source SQL: {sql}"),
        None => assert!(parse(sql).is_err(), "source SQL unexpectedly parsed: {sql}"),
    }
}

#[test]
fn go_parser_test_resource_group_cases() {
    let cases = [
        (r#"create resource group x cpu ='8c'"#, None),
        (r#"create resource group x region ='us, 3'"#, None),
        (
            r#"create resource group x cpu='8c', io_read_bandwidth='2GB/s', io_write_bandwidth='200MB/s'"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=2000"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 2000"#),
        ),
        (
            r#"create resource group x ru_per_sec=200000"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 200000"#),
        ),
        (
            r#"create resource group x ru_per_sec=UNLIMITED"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED"#),
        ),
        (
            r#"create resource group x ru_per_sec=unlimited"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED"#),
        ),
        (r#"create resource group x ru_per_sec='check'"#, None),
        (r#"create resource group x followers=0"#, None),
        (r#"create resource group x burstable=true"#, None),
        (r#"create resource group x burstable=false"#, None),
        (r#"create resource group x burstable=disable"#, None),
        (
            r#"create resource group x ru_per_sec=1000, burstable"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, BURSTABLE = MODERATED"#),
        ),
        (
            r#"create resource group x burstable, ru_per_sec=2000"#,
            Some(r#"CREATE RESOURCE GROUP `x` BURSTABLE = MODERATED, RU_PER_SEC = 2000"#),
        ),
        (
            r#"create resource group x ru_per_sec=3000 burstable"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 3000, BURSTABLE = MODERATED"#),
        ),
        (
            r#"create resource group x burstable ru_per_sec=4000"#,
            Some(r#"CREATE RESOURCE GROUP `x` BURSTABLE = MODERATED, RU_PER_SEC = 4000"#),
        ),
        (
            r#"create resource group x BURSTABLE = UNLIMITED ru_per_sec=4000"#,
            Some(r#"CREATE RESOURCE GROUP `x` BURSTABLE = UNLIMITED, RU_PER_SEC = 4000"#),
        ),
        (
            r#"create resource group x BURSTABLE = MODERATED ru_per_sec=4000"#,
            Some(r#"CREATE RESOURCE GROUP `x` BURSTABLE = MODERATED, RU_PER_SEC = 4000"#),
        ),
        (
            r#"create resource group x BURSTABLE = OFF ru_per_sec=4000"#,
            Some(r#"CREATE RESOURCE GROUP `x` BURSTABLE = OFF, RU_PER_SEC = 4000"#),
        ),
        (
            r#"create resource group x ru_per_sec=20, priority=LOW, burstable"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 20, PRIORITY = LOW, BURSTABLE = MODERATED"#,
            ),
        ),
        (
            r#"create resource group default ru_per_sec=20, priority=LOW, burstable"#,
            Some(
                r#"CREATE RESOURCE GROUP `default` RU_PER_SEC = 20, PRIORITY = LOW, BURSTABLE = MODERATED"#,
            ),
        ),
        (
            r#"create resource group default ru_per_sec=UNLIMITED, priority=LOW, burstable"#,
            Some(
                r#"CREATE RESOURCE GROUP `default` RU_PER_SEC = UNLIMITED, PRIORITY = LOW, BURSTABLE = MODERATED"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000"#),
        ),
        (
            r#"create resource group x ru_per_sec=1000 burstable=unlimited"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, BURSTABLE = UNLIMITED"#),
        ),
        (
            r#"create resource group x ru_per_sec=1000 burstable=off"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, BURSTABLE = OFF"#),
        ),
        (
            r#"create resource group x ru_per_sec=1000 burstable=moderated"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, BURSTABLE = MODERATED"#),
        ),
        (
            r#"create resource group x burstable=unlimited, ru_per_sec=2000"#,
            Some(r#"CREATE RESOURCE GROUP `x` BURSTABLE = UNLIMITED, RU_PER_SEC = 2000"#),
        ),
        (
            r#"create resource group x burstable=off, ru_per_sec=2000"#,
            Some(r#"CREATE RESOURCE GROUP `x` BURSTABLE = OFF, RU_PER_SEC = 2000"#),
        ),
        (
            r#"create resource group x burstable=moderated, ru_per_sec=2000"#,
            Some(r#"CREATE RESOURCE GROUP `x` BURSTABLE = MODERATED, RU_PER_SEC = 2000"#),
        ),
        (
            r#"create resource group x ru_per_sec=1000 ,burstable=unlimited"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, BURSTABLE = UNLIMITED"#),
        ),
        (
            r#"create resource group x ru_per_sec=1000 ,burstable=off"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, BURSTABLE = OFF"#),
        ),
        (
            r#"create resource group x ru_per_sec=1000 ,burstable=moderated"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, BURSTABLE = MODERATED"#),
        ),
        (
            r#"create resource group x ru_per_sec=1000 , priority=LOW,burstable=unlimited"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, PRIORITY = LOW, BURSTABLE = UNLIMITED"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 , priority=LOW,burstable=off"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, PRIORITY = LOW, BURSTABLE = OFF"#),
        ),
        (
            r#"create resource group x ru_per_sec=1000 , priority=LOW,burstable=moderated"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, PRIORITY = LOW, BURSTABLE = MODERATED"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=UNLIMITED , priority=LOW,burstable=unlimited"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED, PRIORITY = LOW, BURSTABLE = UNLIMITED"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=UNLIMITED , priority=LOW,burstable=off"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED, PRIORITY = LOW, BURSTABLE = OFF"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=UNLIMITED , priority=LOW,burstable=moderated"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED, PRIORITY = LOW, BURSTABLE = MODERATED"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED '10s' ACTION DRYRUN)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' ACTION = DRYRUN)"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED '10m' ACTION COOLDOWN)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10m' ACTION = COOLDOWN)"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT=(ACTION KILL EXEC_ELAPSED='10m')"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (ACTION = KILL EXEC_ELAPSED = '10m')"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED '10s' WATCH=SIMILAR DURATION '10m' ACTION COOLDOWN)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' WATCH = SIMILAR DURATION = '10m' ACTION = COOLDOWN)"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT (EXEC_ELAPSED "10s" ACTION COOLDOWN WATCH EXACT DURATION='10m')"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' ACTION = COOLDOWN WATCH = EXACT DURATION = '10m')"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT (EXEC_ELAPSED '9s' ACTION COOLDOWN WATCH EXACT)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '9s' ACTION = COOLDOWN WATCH = EXACT DURATION = UNLIMITED)"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT (EXEC_ELAPSED '8s' ACTION COOLDOWN WATCH EXACT DURATION = UNLIMITED)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '8s' ACTION = COOLDOWN WATCH = EXACT DURATION = UNLIMITED)"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT (EXEC_ELAPSED '7s' ACTION COOLDOWN WATCH EXACT DURATION UNLIMITED)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '7s' ACTION = COOLDOWN WATCH = EXACT DURATION = UNLIMITED)"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT (EXEC_ELAPSED '7s' ACTION COOLDOWN WATCH EXACT DURATION 'UNLIMITED')"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '7s' ACTION = COOLDOWN WATCH = EXACT DURATION = UNLIMITED)"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED '10s' RU 100 ACTION DRYRUN)"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' RU = 100 ACTION = DRYRUN)"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED '10s' PROCESSED_KEYS 100 ACTION DRYRUN)"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' PROCESSED_KEYS = 100 ACTION = DRYRUN)"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED '10s' WATCH SIMILAR DURATION '10m' ACTION COOLDOWN)"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' WATCH = SIMILAR DURATION = '10m' ACTION = COOLDOWN)"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED '10s' ACTION COOLDOWN WATCH EXACT DURATION '10m')"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' ACTION = COOLDOWN WATCH = EXACT DURATION = '10m')"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 background = (task_types='')"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, BACKGROUND = (TASK_TYPES = '')"#),
        ),
        (
            r#"create resource group x ru_per_sec=1000 background = (UTILIZATION_LIMIT=50)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, BACKGROUND = (UTILIZATION_LIMIT = 50)"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 background = (UTILIZATION_LIMIT="NAN")"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 background (task_types='br,lightning')"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, BACKGROUND = (TASK_TYPES = 'br,lightning')"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 background (task_types='br,lightning',utilization_limit=50)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, BACKGROUND = (TASK_TYPES = 'br,lightning', UTILIZATION_LIMIT = 50)"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT (EXEC_ELAPSED "10s" ACTION COOLDOWN WATCH EXACT DURATION='10m')  background (task_types 'br,lightning')"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' ACTION = COOLDOWN WATCH = EXACT DURATION = '10m'), BACKGROUND = (TASK_TYPES = 'br,lightning')"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT (EXEC_ELAPSED "10s" ACTION COOLDOWN WATCH PLAN DURATION='10m')  background (task_types 'br,lightning')"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' ACTION = COOLDOWN WATCH = PLAN DURATION = '10m'), BACKGROUND = (TASK_TYPES = 'br,lightning')"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT (EXEC_ELAPSED "10s" ACTION COOLDOWN WATCH PLAN DURATION='10m')  background (task_types 'br,lightning', utilization_limit 10)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' ACTION = COOLDOWN WATCH = PLAN DURATION = '10m'), BACKGROUND = (TASK_TYPES = 'br,lightning', UTILIZATION_LIMIT = 10)"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=UNLIMITED QUERY_LIMIT (EXEC_ELAPSED "10s" ACTION COOLDOWN WATCH PLAN DURATION='10m')  background (task_types 'br,lightning')"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED, QUERY_LIMIT = (EXEC_ELAPSED = '10s' ACTION = COOLDOWN WATCH = PLAN DURATION = '10m'), BACKGROUND = (TASK_TYPES = 'br,lightning')"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT = (EXEC_ELAPSED '10s')"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s')"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY=(EXEC_ELAPSED '10s')"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT=EXEC_ELAPSED '10s'"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT = (EXEC_ELAPSED '10s'"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 LIMIT=(EXEC_ELAPSED '10s')"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT = (EXEC_ELAPSED '10s' ACTION DRYRUN ACTION KILL)"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT = (PROCESSED_KEYS=100)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (PROCESSED_KEYS = 100)"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY=(PROCESSED_KEYS 100)"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT=PROCESSED_KEYS 100"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT = (PROCESSED_KEYS 100"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 LIMIT=(PROCESSED_KEYS 100)"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT = (PROCESSED_KEYS 100 ACTION DRYRUN ACTION KILL)"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT = (RU=100)"#,
            Some(r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (RU = 100)"#),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY=(RU 100)"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT=RU 100"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT = (RU 100"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 LIMIT=(RU 100)"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT = (RU 100 ACTION DRYRUN ACTION KILL)"#,
            None,
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT = (EXEC_ELAPSED='10s' PROCESSED_KEYS=100)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' PROCESSED_KEYS = 100)"#,
            ),
        ),
        (
            r#"create resource group x ru_per_sec=1000 QUERY_LIMIT = (EXEC_ELAPSED='10s', PROCESSED_KEYS=100, RU=100)"#,
            Some(
                r#"CREATE RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' PROCESSED_KEYS = 100 RU = 100)"#,
            ),
        ),
        (r#"alter resource group x cpu ='8c'"#, None),
        (r#"alter resource group x region ='us, 3'"#, None),
        (r#"alter resource group x burstable=true"#, None),
        (r#"alter resource group x burstable=false"#, None),
        (r#"alter resource group x burstable=disable"#, None),
        (
            r#"alter resource group default priority = high"#,
            Some(r#"ALTER RESOURCE GROUP `default` PRIORITY = HIGH"#),
        ),
        (
            r#"alter resource group x cpu='8c', io_read_bandwidth='2GB/s', io_write_bandwidth='200MB/s'"#,
            None,
        ),
        (
            r#"alter resource group x ru_per_sec=1000"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000"#),
        ),
        (
            r#"alter resource group x ru_per_sec=2000, BURSTABLE"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 2000, BURSTABLE = MODERATED"#),
        ),
        (
            r#"alter resource group x ru_per_sec=UNLIMITED"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED"#),
        ),
        (
            r#"alter resource group x ru_per_sec=UNLIMITED, BURSTABLE"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED, BURSTABLE = MODERATED"#),
        ),
        (
            r#"alter resource group x ru_per_sec=unlimited, BURSTABLE"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED, BURSTABLE = MODERATED"#),
        ),
        (
            r#"alter resource group x ru_per_sec='check', BURSTABLE"#,
            None,
        ),
        (
            r#"alter resource group x BURSTABLE, ru_per_sec=3000"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = MODERATED, RU_PER_SEC = 3000"#),
        ),
        (
            r#"alter resource group x BURSTABLE ru_per_sec=4000"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = MODERATED, RU_PER_SEC = 4000"#),
        ),
        (
            r#"alter resource group x ru_per_sec=2000, BURSTABLE=unlimited"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 2000, BURSTABLE = UNLIMITED"#),
        ),
        (
            r#"alter resource group x ru_per_sec=2000, BURSTABLE=moderated"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 2000, BURSTABLE = MODERATED"#),
        ),
        (
            r#"alter resource group x ru_per_sec=2000, BURSTABLE=off"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 2000, BURSTABLE = OFF"#),
        ),
        (
            r#"alter resource group x ru_per_sec=UNLIMITED"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED"#),
        ),
        (
            r#"alter resource group x ru_per_sec=UNLIMITED, BURSTABLE=unlimited"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED, BURSTABLE = UNLIMITED"#),
        ),
        (
            r#"alter resource group x ru_per_sec=UNLIMITED, BURSTABLE=moderated"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED, BURSTABLE = MODERATED"#),
        ),
        (
            r#"alter resource group x ru_per_sec=unlimited, BURSTABLE=off"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED, BURSTABLE = OFF"#),
        ),
        (
            r#"alter resource group x ru_per_sec='check', BURSTABLE"#,
            None,
        ),
        (
            r#"alter resource group x ru_per_sec=2000, BURSTABLE=yes"#,
            None,
        ),
        (
            r#"alter resource group x BURSTABLE=unlimited, ru_per_sec=3000"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = UNLIMITED, RU_PER_SEC = 3000"#),
        ),
        (
            r#"alter resource group x BURSTABLE=moderated, ru_per_sec=3000"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = MODERATED, RU_PER_SEC = 3000"#),
        ),
        (
            r#"alter resource group x BURSTABLE=off, ru_per_sec=3000"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = OFF, RU_PER_SEC = 3000"#),
        ),
        (
            r#"alter resource group x BURSTABLE=unlimited ru_per_sec=4000"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = UNLIMITED, RU_PER_SEC = 4000"#),
        ),
        (
            r#"alter resource group x BURSTABLE=moderated ru_per_sec=4000"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = MODERATED, RU_PER_SEC = 4000"#),
        ),
        (
            r#"alter resource group x BURSTABLE=off ru_per_sec=4000"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = OFF, RU_PER_SEC = 4000"#),
        ),
        (
            r#"alter resource group x BURSTABLE"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = MODERATED"#),
        ),
        (
            r#"alter resource group x ru_per_sec=200000 BURSTABLE"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 200000, BURSTABLE = MODERATED"#),
        ),
        (r#"alter resource group x followers=0"#, None),
        (
            r#"alter resource group x ru_per_sec=20 priority=MID BURSTABLE"#,
            None,
        ),
        (
            r#"alter resource group x BURSTABLE=unlimited"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = UNLIMITED"#),
        ),
        (
            r#"alter resource group x BURSTABLE=moderated"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = MODERATED"#),
        ),
        (
            r#"alter resource group x BURSTABLE=off"#,
            Some(r#"ALTER RESOURCE GROUP `x` BURSTABLE = OFF"#),
        ),
        (
            r#"alter resource group x ru_per_sec=200000 BURSTABLE=unlimited"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 200000, BURSTABLE = UNLIMITED"#),
        ),
        (
            r#"alter resource group x ru_per_sec=200000 BURSTABLE=moderated"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 200000, BURSTABLE = MODERATED"#),
        ),
        (
            r#"alter resource group x ru_per_sec=200000 BURSTABLE=off"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 200000, BURSTABLE = OFF"#),
        ),
        (r#"alter resource group x followers=0"#, None),
        (r#"alter resource group x ru_per_sec=20 priority=MID"#, None),
        (
            r#"alter resource group x ru_per_sec=20 priority=HIGH BURSTABLE=unlimited"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 20, PRIORITY = HIGH, BURSTABLE = UNLIMITED"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=20 priority=HIGH BURSTABLE=moderated"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 20, PRIORITY = HIGH, BURSTABLE = MODERATED"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=20 priority=HIGH BURSTABLE=off"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 20, PRIORITY = HIGH, BURSTABLE = OFF"#),
        ),
        (
            r#"alter resource group x QUERY_LIMIT=NULL"#,
            Some(r#"ALTER RESOURCE GROUP `x` QUERY_LIMIT = NULL"#),
        ),
        (
            r#"alter resource group x QUERY_LIMIT=()"#,
            Some(r#"ALTER RESOURCE GROUP `x` QUERY_LIMIT = NULL"#),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED '10s' ACTION DRYRUN)"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' ACTION = DRYRUN)"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT=()"#,
            Some(r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = NULL"#),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED '10m' ACTION COOLDOWN)"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10m' ACTION = COOLDOWN)"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT=( ACTION KILL EXEC_ELAPSED '10m')"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (ACTION = KILL EXEC_ELAPSED = '10m')"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED '10s' WATCH SIMILAR DURATION '10m' ACTION COOLDOWN)"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' WATCH = SIMILAR DURATION = '10m' ACTION = COOLDOWN)"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED '10s' ACTION COOLDOWN WATCH EXACT DURATION '10m')"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s' ACTION = COOLDOWN WATCH = EXACT DURATION = '10m')"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=UNLIMITED QUERY_LIMIT=(EXEC_ELAPSED '10s' ACTION COOLDOWN WATCH EXACT DURATION '10m')"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = UNLIMITED, QUERY_LIMIT = (EXEC_ELAPSED = '10s' ACTION = COOLDOWN WATCH = EXACT DURATION = '10m')"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT = (EXEC_ELAPSED '10s')"#,
            Some(
                r#"ALTER RESOURCE GROUP `x` RU_PER_SEC = 1000, QUERY_LIMIT = (EXEC_ELAPSED = '10s')"#,
            ),
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT EXEC_ELAPSED '10s'"#,
            None,
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT = (EXEC_ELAPSED '10s' ACTION DRYRUN ACTION KILL)"#,
            None,
        ),
        (
            r#"alter resource group x ru_per_sec=1000 QUERY_LIMIT = (EXEC_ELAPSED '10s' ACTION DRYRUN WATCH SIMILAR DURATION '10m' ACTION COOLDOWN)"#,
            None,
        ),
        (
            r#"alter resource group x background=()"#,
            Some(r#"ALTER RESOURCE GROUP `x` BACKGROUND = NULL"#),
        ),
        (
            r#"alter resource group x background NULL"#,
            Some(r#"ALTER RESOURCE GROUP `x` BACKGROUND = NULL"#),
        ),
        (
            r#"alter resource group default priority=low background = ( task_types "ttl" )"#,
            Some(
                r#"ALTER RESOURCE GROUP `default` PRIORITY = LOW, BACKGROUND = (TASK_TYPES = 'ttl')"#,
            ),
        ),
        (
            r#"alter resource group default burstable=unlimited background ( task_types = 'a,b,c' )"#,
            Some(
                r#"ALTER RESOURCE GROUP `default` BURSTABLE = UNLIMITED, BACKGROUND = (TASK_TYPES = 'a,b,c')"#,
            ),
        ),
        (
            r#"alter resource group default burstable=moderated background ( task_types = 'a,b,c' )"#,
            Some(
                r#"ALTER RESOURCE GROUP `default` BURSTABLE = MODERATED, BACKGROUND = (TASK_TYPES = 'a,b,c')"#,
            ),
        ),
        (
            r#"alter resource group default burstable=off background ( task_types = 'a,b,c' )"#,
            Some(
                r#"ALTER RESOURCE GROUP `default` BURSTABLE = OFF, BACKGROUND = (TASK_TYPES = 'a,b,c')"#,
            ),
        ),
        (
            r#"alter resource group default burstable=unlimited background ( utilization_limit = 20 )"#,
            Some(
                r#"ALTER RESOURCE GROUP `default` BURSTABLE = UNLIMITED, BACKGROUND = (UTILIZATION_LIMIT = 20)"#,
            ),
        ),
        (
            r#"alter resource group default burstable=moderated background ( utilization_limit = 20 )"#,
            Some(
                r#"ALTER RESOURCE GROUP `default` BURSTABLE = MODERATED, BACKGROUND = (UTILIZATION_LIMIT = 20)"#,
            ),
        ),
        (
            r#"alter resource group default burstable=off background ( utilization_limit = 20 )"#,
            Some(
                r#"ALTER RESOURCE GROUP `default` BURSTABLE = OFF, BACKGROUND = (UTILIZATION_LIMIT = 20)"#,
            ),
        ),
        (
            r#"alter resource group default burstable=unlimited background ( task_types = 'a,b,c', utilization_limit = 20 )"#,
            Some(
                r#"ALTER RESOURCE GROUP `default` BURSTABLE = UNLIMITED, BACKGROUND = (TASK_TYPES = 'a,b,c', UTILIZATION_LIMIT = 20)"#,
            ),
        ),
        (
            r#"alter resource group default burstable=moderated background ( task_types = 'a,b,c', utilization_limit = 20 )"#,
            Some(
                r#"ALTER RESOURCE GROUP `default` BURSTABLE = MODERATED, BACKGROUND = (TASK_TYPES = 'a,b,c', UTILIZATION_LIMIT = 20)"#,
            ),
        ),
        (
            r#"alter resource group default burstable=off background ( task_types = 'a,b,c', utilization_limit = 20 )"#,
            Some(
                r#"ALTER RESOURCE GROUP `default` BURSTABLE = OFF, BACKGROUND = (TASK_TYPES = 'a,b,c', UTILIZATION_LIMIT = 20)"#,
            ),
        ),
        (
            r#"alter resource group default burstable=unlimited background ( utilization_limit = 'abc' )"#,
            None,
        ),
        (
            r#"alter resource group default burstable=moderated background ( utilization_limit = 'abc' )"#,
            None,
        ),
        (
            r#"alter resource group default burstable=off background ( utilization_limit = 'abc' )"#,
            None,
        ),
        (
            r#"drop resource group x;"#,
            Some(r#"DROP RESOURCE GROUP `x`"#),
        ),
        (
            r#"drop resource group DEFAULT;"#,
            Some(r#"DROP RESOURCE GROUP `DEFAULT`"#),
        ),
        (
            r#"drop resource group if exists x;"#,
            Some(r#"DROP RESOURCE GROUP IF EXISTS `x`"#),
        ),
        (r#"drop resource group x,y"#, None),
        (r#"drop resource group if exists x,y"#, None),
    ];
    assert_eq!(cases.len(), 158);
    for (sql, expected) in cases {
        assert_parser_case(sql, expected);
    }
}

#[test]
fn go_ast_resource_group_ddl_restore_cases() {
    let cases = [
        (
            r#"CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500"#,
            r#"CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500"#,
        ),
        (
            r#"CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = UNLIMITED"#,
            r#"CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = UNLIMITED"#,
        ),
        (
            r#"CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 BURSTABLE"#,
            r#"CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, BURSTABLE = MODERATED"#,
        ),
        (
            r#"CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 BURSTABLE=UNLIMITED"#,
            r#"CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, BURSTABLE = UNLIMITED"#,
        ),
        (
            r#"CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 BURSTABLE=MODERATED"#,
            r#"CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, BURSTABLE = MODERATED"#,
        ),
        (
            r#"CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 BURSTABLE=OFF"#,
            r#"CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, BURSTABLE = OFF"#,
        ),
        (
            r#"CREATE RESOURCE GROUP IF NOT EXISTS rg2 RU_PER_SEC = 600"#,
            r#"CREATE RESOURCE GROUP IF NOT EXISTS `rg2` RU_PER_SEC = 600"#,
        ),
        (
            r#"CREATE RESOURCE GROUP IF NOT EXISTS rg3 RU_PER_SEC = 100 PRIORITY = HIGH"#,
            r#"CREATE RESOURCE GROUP IF NOT EXISTS `rg3` RU_PER_SEC = 100, PRIORITY = HIGH"#,
        ),
        (
            r#"CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 QUERY_LIMIT=(EXEC_ELAPSED='60s', ACTION=COOLDOWN)"#,
            r#"CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, QUERY_LIMIT = (EXEC_ELAPSED = '60s' ACTION = COOLDOWN)"#,
        ),
        (
            r#"CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 500 QUERY_LIMIT=(ACTION=SWITCH_GROUP(rg2))"#,
            r#"CREATE RESOURCE GROUP IF NOT EXISTS `rg1` RU_PER_SEC = 500, QUERY_LIMIT = (ACTION = SWITCH_GROUP(`rg2`))"#,
        ),
        (
            r#"ALTER RESOURCE GROUP rg1 QUERY_LIMIT=(EXEC_ELAPSED='60s', ACTION=KILL, WATCH=SIMILAR DURATION='10m')"#,
            r#"ALTER RESOURCE GROUP `rg1` QUERY_LIMIT = (EXEC_ELAPSED = '60s' ACTION = KILL WATCH = SIMILAR DURATION = '10m')"#,
        ),
        (
            r#"ALTER RESOURCE GROUP rg1 QUERY_LIMIT=(EXEC_ELAPSED='1m', ACTION=SWITCH_GROUP(rg2), WATCH=SIMILAR DURATION='10m')"#,
            r#"ALTER RESOURCE GROUP `rg1` QUERY_LIMIT = (EXEC_ELAPSED = '1m' ACTION = SWITCH_GROUP(`rg2`) WATCH = SIMILAR DURATION = '10m')"#,
        ),
        (
            r#"ALTER RESOURCE GROUP rg1 QUERY_LIMIT=NULL"#,
            r#"ALTER RESOURCE GROUP `rg1` QUERY_LIMIT = NULL"#,
        ),
        (
            r#"ALTER RESOURCE GROUP `default` BACKGROUND=(TASK_TYPES='br,ddl')"#,
            r#"ALTER RESOURCE GROUP `default` BACKGROUND = (TASK_TYPES = 'br,ddl')"#,
        ),
        (
            r#"ALTER RESOURCE GROUP `default` BACKGROUND=NULL"#,
            r#"ALTER RESOURCE GROUP `default` BACKGROUND = NULL"#,
        ),
        (
            r#"ALTER RESOURCE GROUP `default` BACKGROUND=(TASK_TYPES='')"#,
            r#"ALTER RESOURCE GROUP `default` BACKGROUND = (TASK_TYPES = '')"#,
        ),
        (
            r#"ALTER RESOURCE GROUP rg1 RU_PER_SEC=UNLIMITED"#,
            r#"ALTER RESOURCE GROUP `rg1` RU_PER_SEC = UNLIMITED"#,
        ),
        (
            r#"ALTER RESOURCE GROUP rg1 RU_PER_SEC=500"#,
            r#"ALTER RESOURCE GROUP `rg1` RU_PER_SEC = 500"#,
        ),
        (
            r#"ALTER RESOURCE GROUP rg1 BURSTABLE=UNLIMITED"#,
            r#"ALTER RESOURCE GROUP `rg1` BURSTABLE = UNLIMITED"#,
        ),
        (
            r#"ALTER RESOURCE GROUP rg1 BURSTABLE=MODERATED"#,
            r#"ALTER RESOURCE GROUP `rg1` BURSTABLE = MODERATED"#,
        ),
        (
            r#"ALTER RESOURCE GROUP rg1 BURSTABLE=OFF"#,
            r#"ALTER RESOURCE GROUP `rg1` BURSTABLE = OFF"#,
        ),
    ];
    assert_eq!(cases.len(), 21);
    for (sql, expected) in cases {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn source_zero_values_and_list_boundaries_remain_visible() {
    assert_eq!(r("create resource group rg"), "CREATE RESOURCE GROUP `rg`");
    assert_eq!(
        r("alter resource group rg ru_per_sec burstable="),
        "ALTER RESOURCE GROUP `rg` RU_PER_SEC = 0, BURSTABLE = OFF"
    );
    assert_eq!(
        r("alter resource group rg query_limit=(exec_elapsed watch exact)"),
        "ALTER RESOURCE GROUP `rg` QUERY_LIMIT = (EXEC_ELAPSED = '' WATCH = EXACT DURATION = UNLIMITED)"
    );
    assert_eq!(
        r("alter resource group rg background=(task_types"),
        "ALTER RESOURCE GROUP `rg` BACKGROUND = (TASK_TYPES = '')"
    );
    assert!(parse("alter resource group rg query_limit=(exec_elapsed").is_err());
    assert!(parse("alter resource group rg priority=low priority=high").is_err());
    assert!(
        parse("alter resource group rg background=(task_types='br', task_types='ddl')").is_err()
    );

    // All three Go statement parsers read the name with `next()` rather than
    // an identifier production.
    assert_eq!(
        r("drop resource group DEFAULT"),
        "DROP RESOURCE GROUP `DEFAULT`"
    );
    assert_eq!(
        r("drop resource group if exists 'rg 1'"),
        "DROP RESOURCE GROUP IF EXISTS `rg 1`"
    );
    assert_eq!(r("drop resource group 1"), "DROP RESOURCE GROUP `1`");
    assert!(parse("drop resource group rg1, rg2").is_err());
}

#[test]
fn resource_group_payload_is_typed_end_to_end() {
    let Stmt::Ddl(ddl) = parse(
        "alter resource group if exists rg query_limit=(ru=7 action=switch_group(slow) watch=plan duration=unlimited) background=(task_types='ddl', utilization_limit=25)",
    )
    .expect("resource group statement parses")
    else {
        panic!("expected DDL envelope");
    };
    let tidb_ast::DdlStmt::AlterResourceGroup(statement) = ddl.as_ref() else {
        panic!("expected typed ALTER RESOURCE GROUP");
    };
    assert!(statement.if_exists);
    assert_eq!(statement.name, "rg");
    assert!(matches!(
        statement.options.as_slice(),
        [
            tidb_ast::ResourceGroupOption::QueryLimit(runaway),
            tidb_ast::ResourceGroupOption::Background(background)
        ] if matches!(
            runaway.as_slice(),
            [
                tidb_ast::ResourceGroupRunawayOption::Rule(
                    tidb_ast::ResourceGroupRunawayRule::RequestUnit(7)
                ),
                tidb_ast::ResourceGroupRunawayOption::Action(
                    tidb_ast::ResourceGroupRunawayAction::SwitchGroup(name)
                ),
                tidb_ast::ResourceGroupRunawayOption::Watch(
                    tidb_ast::ResourceGroupRunawayWatch {
                        watch_type: tidb_ast::ResourceGroupRunawayWatchType::Plan,
                        duration: None,
                    }
                )
            ] if name == "slow"
        ) && matches!(
            background.as_slice(),
            [
                tidb_ast::ResourceGroupBackgroundOption::TaskTypes(tasks),
                tidb_ast::ResourceGroupBackgroundOption::UtilizationLimit(25)
            ] if tasks == "ddl"
        )
    ));
}
