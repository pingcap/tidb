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

//! `ToTiDBErr` over the GC safe-point check's failure: the SQL error keeps
//! the client error's text, Go's local times included, under TiKV's 9006.

use tidb_store_driver_error::driver_error::{to_tidb_driver_error, ConvertedDriverError};
use tidb_txnkv::VisibilityError;

#[test]
fn the_sql_error_carries_gos_local_times() {
    let (start_ts, safe_point) = (248_175_289_958_400_000, 469_896_279_291_330_560);
    let error = VisibilityError::TxnAbortedByGc {
        txn_start_ts: start_ts,
        txn_safe_point: safe_point,
    };
    let expected = error.to_string();
    let ConvertedDriverError::Terror(converted) =
        to_tidb_driver_error(&error.into_storage_driver_error())
    else {
        panic!("a GC-aborted read must use the TiKV error catalog")
    };
    assert_eq!(converted.code().value(), 9006);
    assert_eq!(converted.message(), expected);
}
