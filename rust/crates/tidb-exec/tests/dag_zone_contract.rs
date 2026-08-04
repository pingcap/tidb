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

//! The ONE `DAGRequest` time-zone contract, shared by every node type that
//! stamps one.
//!
//! Go writes the pair in exactly one shape, at every construction site:
//!
//! ```go
//! dagReq.TimeZoneName, dagReq.TimeZoneOffset =
//!     timeutil.Zone(ctx.GetSessionVars().Location())
//! ```
//!
//! and `timeutil.Zone` returns `loc.String()` as the name. The rows below were
//! MEASURED by running that function on this branch (`timeutil.ParseTimeZone`
//! then `timeutil.Zone`, system TZ `Asia/Shanghai`):
//!
//! ```text
//! 'UTC'           -> name "UTC"           offset 0
//! '+08:00'        -> name ""              offset 28800
//! '-06:30'        -> name ""              offset -23400
//! '+00:00'        -> name ""              offset 0
//! 'Asia/Shanghai' -> name "Asia/Shanghai" offset 28800
//! ```
//!
//! The empty name is the load-bearing row. `timeutil.ParseTimeZone`'s
//! `+HH:MM` branch builds `time.FixedZone("", ofst)`, so an offset zone has no
//! name at all; TiKV prefers a non-empty name and falls back to the offset,
//! and `"+08:00"` sent AS a name is a name no zone database can load. One
//! stamper sent exactly that, and only its own mock asserted it, so nothing
//! else in the tree could notice.
//!
//! Every stamper is pinned through this one function so the two cannot drift:
//! a site that stopped deriving its pair from
//! `SessionTimeZone::dag_zone` fails here whichever site it was.

use tidb_datatype::SessionTimeZone;

/// Runs `stamp` — a node type's DAG-request construction, reduced to the
/// `(TimeZoneName, TimeZoneOffset)` pair it puts on the wire — over the
/// measured Go rows.
///
/// `site` names the stamper in the failure message, because the assertion is
/// shared and the failure is not.
pub fn assert_go_dag_zone_contract(
    site: &str,
    mut stamp: impl FnMut(&SessionTimeZone) -> (String, i64),
) {
    let fixed = |name: &str, offset_secs: i32| SessionTimeZone::Fixed {
        name: name.to_owned(),
        offset_secs,
    };
    let rows: Vec<(SessionTimeZone, (String, i64))> = vec![
        (SessionTimeZone::utc(), ("UTC".to_owned(), 0)),
        (fixed("+08:00", 8 * 3600), (String::new(), 8 * 3600)),
        (
            fixed("-06:30", -(6 * 3600 + 30 * 60)),
            (String::new(), -(6 * 3600 + 30 * 60)),
        ),
        // `+00:00` is an OFFSET zone that happens to be zero, not `UTC`: Go
        // builds it with `FixedZone("", 0)` like any other offset.
        (fixed("+00:00", 0), (String::new(), 0)),
        (
            SessionTimeZone::Named(chrono_tz::Tz::Asia__Shanghai),
            ("Asia/Shanghai".to_owned(), 8 * 3600),
        ),
    ];
    for (zone, expected) in rows {
        assert_eq!(
            stamp(&zone),
            expected,
            "{site} must stamp {zone:?} the way Go's timeutil.Zone does"
        );
    }
}
