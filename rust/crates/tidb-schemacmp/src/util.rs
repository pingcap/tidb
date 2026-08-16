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

//! Go `pkg/util/schemacmp/util.go`: MySQL type-code ordering helpers.

use tidb_mysql::types::{TypeBlob, TypeInt24, TypeShort, TypeTinyBlob};

/// Go `compareMySQLIntegerType`: compares two MySQL integer types, returning
/// -1 if `a < b`, 0 if `a == b` and 1 if `a > b`.
pub(crate) fn compare_mysql_integer_type(a: u8, b: u8) -> i32 {
    if a == b {
        return 0;
    }

    // TypeTiny(1) < TypeShort(2) < TypeInt24(9) < TypeLong(3) < TypeLonglong(8)
    if a == TypeInt24 {
        if b <= TypeShort {
            return 1;
        }
        return -1;
    }
    if b == TypeInt24 {
        if a <= TypeShort {
            return -1;
        }
        return 1;
    }
    if a < b {
        return -1;
    }
    1
}

/// Go `compareMySQLBlobType`: compares two MySQL blob types, returning -1 if
/// `a < b`, 0 if `a == b` and 1 if `a > b`.
pub(crate) fn compare_mysql_blob_type(a: u8, b: u8) -> i32 {
    if a == b {
        return 0;
    }

    // TypeTinyBlob(0xf9, 249) < TypeBlob(0xfc, 252) < TypeMediumBlob(0xfa, 250) < TypeLongBlob(0xfb, 251)
    if a == TypeBlob {
        if b == TypeTinyBlob {
            return 1;
        }
        return -1;
    }
    if b == TypeBlob {
        if a == TypeTinyBlob {
            return -1;
        }
        return 1;
    }
    if a < b {
        return -1;
    }
    1
}
