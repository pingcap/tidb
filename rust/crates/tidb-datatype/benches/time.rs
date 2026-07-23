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

//! Executable equivalents of the benchmarks in `pkg/types/time_test.go`.

use std::hint::black_box;
use std::time::Instant;

use tidb_datatype::{parse_date_format, parse_datetime, CoreTime, MySqlDuration, Time, TimeType};

const ITERATIONS: usize = 10_000;

fn measure(name: &str, mut operation: impl FnMut()) {
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        operation();
    }
    println!("{name}: {:?}", started.elapsed());
}

fn main() {
    if cfg!(test) {
        return;
    }

    let timezone = chrono_tz::UTC;
    let time = Time::new(
        CoreTime::from_date(2017, 1, 18, 0, 0, 0, 0),
        TimeType::Timestamp,
        0,
    )
    .expect("static benchmark time");
    measure("BenchmarkFormat", || {
        black_box(
            black_box(time)
                .date_format("%Y-%m-%d %H:%i:%s")
                .expect("static date format"),
        );
    });

    let duration = MySqlDuration::new(12, 30, 59, 0, 6).expect("static duration");
    measure("BenchmarkTimeAdd", || {
        black_box(
            black_box(time)
                .add_duration(black_box(duration))
                .expect("static time addition"),
        );
    });

    let comparison = [
        (
            parse_datetime("2011-10-10 11:11:11", &timezone, true, false)
                .unwrap()
                .time,
            parse_datetime("2011-10-10 11:11:11", &timezone, true, false)
                .unwrap()
                .time,
        ),
        (
            parse_datetime("2011-10-10 11:11:11.123456", &timezone, true, false)
                .unwrap()
                .time,
            parse_datetime("2011-10-10 11:11:11.1", &timezone, true, false)
                .unwrap()
                .time,
        ),
    ];
    measure("BenchmarkTimeCompare", || {
        for (left, right) in comparison {
            black_box(black_box(left).compare(black_box(right)));
        }
    });

    for (name, input) in [
        ("date basic", "2011-12-13"),
        ("date internal", "20111213"),
        ("datetime basic", "2011-12-13 14:15:16"),
        ("datetime internal", "20111213141516"),
        ("datetime basic frac", "2011-12-13 14:15:16.123456"),
        (
            "datetime repeated delimiters",
            "2011---12---13 14::15::16..123456",
        ),
    ] {
        measure(&format!("BenchmarkParseDateFormat/{name}"), || {
            black_box(parse_date_format(black_box(input)));
        });
    }

    for (name, input) in [
        ("without timezone", "2020-10-10T10:10:10"),
        ("with timezone", "2020-10-10T10:10:10Z+08:00"),
    ] {
        measure(&format!("BenchmarkParseDatetimeFormat/{name}"), || {
            black_box(
                parse_datetime(black_box(input), &timezone, true, false).expect("static datetime"),
            );
        });
    }

    for (name, input, format) in [
        (
            "yyyyMMdd hhmmss ffff",
            "31/05/2016 12:34:56.1234",
            "%d/%m/%Y %H:%i:%S.%f",
        ),
        (
            "percent-r ddMMyyyy",
            "04:13:56 AM 13/05/2019",
            "%r %d/%c/%Y",
        ),
        ("percent-T ddMMyyyy", " 4:13:56 13/05/2019", "%T %d/%c/%Y"),
    ] {
        measure(&format!("BenchmarkStrToDate/{name}"), || {
            black_box(
                Time::str_to_date(black_box(input), black_box(format), false, &timezone)
                    .expect("static STR_TO_DATE input"),
            );
        });
    }
}
