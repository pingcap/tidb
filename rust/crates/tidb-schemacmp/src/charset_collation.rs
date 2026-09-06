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

//! Go `pkg/util/schemacmp/charset_collation.go`: character-set and collation
//! lattices.

use std::any::Any;

use tidb_datatype::GoString;
use tidb_mysql::to_lowercase;

use crate::lattice::{
    incompatible_charset_error, incompatible_collation_error, type_mismatch_error,
    IncompatibleError, Lattice, Value,
};

// Go `pkg/parser/charset` constants used by this file. `UTF8Charset` and
// `UTF8MB4Charset` already exist in `tidb_mysql::charset`; the remaining two
// literals carry the same values as Go's `CharsetUTF8MB3`/`CharsetLatin1`.
const CHARSET_UTF8: &str = tidb_mysql::charset::UTF8Charset;
const CHARSET_UTF8MB4: &str = tidb_mysql::charset::UTF8MB4Charset;
const CHARSET_UTF8MB3: &str = "utf8mb3";
const CHARSET_LATIN1: &str = "latin1";

/// Go's unexported `charsetLattice` struct, returned by [`charset`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CharsetLattice {
    value: String,
}

/// Go `Charset`: a lattice for comparing/joining character sets. Currently it
/// supports the ordering: latin1 < utf8mb4 and utf8(utf8mb3) < utf8mb4. Other
/// charsets are only comparable when identical.
#[must_use]
pub fn charset(cs: &str) -> CharsetLattice {
    let mut normalized = to_lowercase(cs);
    if normalized == CHARSET_UTF8MB3 {
        normalized = CHARSET_UTF8.to_owned();
    }
    CharsetLattice { value: normalized }
}

impl Lattice for CharsetLattice {
    fn unwrap(&self) -> Value {
        Value::Str(GoString::from(self.value.as_str()))
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        let Some(b) = other.as_any().downcast_ref::<Self>() else {
            return Err(type_mismatch_error(self, other));
        };

        if self.value == b.value {
            return Ok(0);
        }
        // This must be compatible with ddl.checkModifyCharsetAndCollation().
        if self.value == CHARSET_UTF8MB4 && (b.value == CHARSET_UTF8 || b.value == CHARSET_LATIN1) {
            return Ok(1);
        }
        if b.value == CHARSET_UTF8MB4
            && (self.value == CHARSET_UTF8 || self.value == CHARSET_LATIN1)
        {
            return Ok(-1);
        }
        Err(incompatible_charset_error(&self.value, &b.value))
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        let Some(b) = other.as_any().downcast_ref::<Self>() else {
            return Err(type_mismatch_error(self, other));
        };

        match self.compare(b) {
            Ok(cmp) => {
                if cmp >= 0 {
                    Ok(self.clone_lattice())
                } else {
                    Ok(b.clone_lattice())
                }
            }
            Err(error) => {
                // Currently the only special case is Join(latin1, utf8) = utf8mb4
                if (self.value == CHARSET_UTF8 && b.value == CHARSET_LATIN1)
                    || (self.value == CHARSET_LATIN1 && b.value == CHARSET_UTF8)
                {
                    return Ok(Box::new(charset(CHARSET_UTF8MB4)));
                }
                Err(error)
            }
        }
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.charsetLattice"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(self.clone())
    }
}

/// Go's unexported `collationLattice` struct, returned by [`collation`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollationLattice {
    charset: CharsetLattice,
    /// The part after `<charset>_` in the normalized collation name. For
    /// collations without an underscore, suffix is empty and
    /// charset==collation.
    suffix: String,
}

/// Go `Collation`: a lattice for comparing/joining collations.
///
/// It supports the ordering:
///   - `latin1_<suffix>` < `utf8mb4_<suffix>`
///   - `utf8_<suffix>` < `utf8mb4_<suffix>`
///
/// (same suffix only).
///
/// Other collations are only comparable when identical.
#[must_use]
pub fn collation(co: &str) -> CollationLattice {
    let (charset_name, suffix) = co.split_once('_').unwrap_or((co, ""));
    CollationLattice {
        charset: charset(charset_name),
        suffix: to_lowercase(suffix),
    }
}

impl CollationLattice {
    /// Go `unwrapString`: the normalized collation spelling.
    fn unwrap_string(&self) -> String {
        if self.suffix.is_empty() {
            return self.charset.value.clone();
        }
        format!("{}_{}", self.charset.value, self.suffix)
    }
}

impl Lattice for CollationLattice {
    fn unwrap(&self) -> Value {
        Value::Str(GoString::from(self.unwrap_string()))
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        let Some(b) = other.as_any().downcast_ref::<Self>() else {
            return Err(type_mismatch_error(self, other));
        };

        if self.suffix != b.suffix {
            return Err(incompatible_collation_error(
                &self.unwrap_string(),
                &b.unwrap_string(),
            ));
        }

        self.charset.compare(&b.charset)
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        let Some(b) = other.as_any().downcast_ref::<Self>() else {
            return Err(type_mismatch_error(self, other));
        };

        let error = match self.compare(b) {
            Ok(cmp) => {
                if cmp >= 0 {
                    return Ok(self.clone_lattice());
                }
                return Ok(b.clone_lattice());
            }
            Err(error) => error,
        };

        // If suffix differs, the join doesn't exist (keep the original error).
        if self.suffix != b.suffix {
            return Err(error);
        }

        // When suffix matches, delegate to charset join to handle
        // incomparable-but-joinable pairs.
        let Ok(join_charset) = self.charset.join(&b.charset) else {
            return Err(error);
        };
        let joined = join_charset
            .as_any()
            .downcast_ref::<CharsetLattice>()
            .expect("charset join returns a charset lattice")
            .clone();
        Ok(Box::new(Self {
            charset: joined,
            suffix: self.suffix.clone(),
        }))
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.collationLattice"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(self.clone())
    }
}
