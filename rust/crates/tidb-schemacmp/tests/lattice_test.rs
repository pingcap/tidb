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

//! Go `pkg/util/schemacmp/lattice_test.go` near 1:1.

use std::any::Any;
use std::collections::BTreeMap;
use std::rc::Rc;

use tidb_datatype::GoString;
use tidb_mysql::types::{
    TypeBlob, TypeInt24, TypeLong, TypeLongBlob, TypeLonglong, TypeMediumBlob, TypeSet, TypeShort,
    TypeTiny, TypeTinyBlob,
};
use tidb_schemacmp::{
    equality_singleton, field_tp, map_lattice, maybe, singleton, BitSet, Bool, Byte, Equality,
    IncompatibleError, Int64, Lattice, LatticeMap, StringList, Tuple, Uint, Value,
};

/// Go `eqBytes`: a sample type used for testing `EqualitySingleton`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct EqBytes(Vec<u8>);

impl Equality for EqBytes {
    fn equals(&self, other: &dyn Equality) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|b| self == b)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Go `uintMap`: a sample type used for testing `Map`.
#[derive(Clone, Debug, Default)]
struct UintMap(BTreeMap<String, u64>);

impl LatticeMap for UintMap {
    fn new_empty(&self) -> Box<dyn LatticeMap> {
        Box::new(Self::default())
    }

    fn insert(&mut self, key: &str, value: Box<dyn Lattice>) {
        let value = value
            .as_any()
            .downcast_ref::<Uint>()
            .expect("a uintMap stores Uint values");
        self.0.insert(key.to_owned(), value.0);
    }

    fn get(&self, key: &str) -> Option<Box<dyn Lattice>> {
        self.0
            .get(key)
            .map(|value| Box::new(Uint(*value)) as Box<dyn Lattice>)
    }

    fn for_each(
        &self,
        action: &mut dyn FnMut(&str, &dyn Lattice) -> Result<(), IncompatibleError>,
    ) -> Result<(), IncompatibleError> {
        for (key, value) in &self.0 {
            action(key, &Uint(*value))?;
        }
        Ok(())
    }

    fn compare_with_nil(&self, _value: &dyn Lattice) -> Result<i32, IncompatibleError> {
        Ok(1)
    }

    fn join_with_nil(
        &self,
        value: &dyn Lattice,
    ) -> Result<Option<Box<dyn Lattice>>, IncompatibleError> {
        Ok(Some(value.clone_lattice()))
    }

    fn should_delete_incompatible_join(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_map(&self) -> Box<dyn LatticeMap> {
        Box::new(self.clone())
    }
}

fn eq_bytes(text: &str) -> Rc<EqBytes> {
    Rc::new(EqBytes(text.as_bytes().to_vec()))
}

fn uint_map(entries: &[(&str, u64)]) -> Box<dyn Lattice> {
    map_lattice(Box::new(UintMap(
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), *value))
            .collect(),
    )))
}

fn string_list(values: &[&str]) -> Box<dyn Lattice> {
    Box::new(StringList(
        values.iter().map(|v| GoString::from(*v)).collect(),
    ))
}

struct Case {
    a: Box<dyn Lattice>,
    b: Box<dyn Lattice>,
    compare_result: i32,
    compare_error: &'static str,
    join: Option<Box<dyn Lattice>>,
    join_error: &'static str,
}

fn ok(
    a: Box<dyn Lattice>,
    b: Box<dyn Lattice>,
    compare_result: i32,
    join: Box<dyn Lattice>,
) -> Case {
    Case {
        a,
        b,
        compare_result,
        compare_error: "",
        join: Some(join),
        join_error: "",
    }
}

fn err(
    a: Box<dyn Lattice>,
    b: Box<dyn Lattice>,
    compare_error: &'static str,
    join_error: &'static str,
) -> Case {
    Case {
        a,
        b,
        compare_result: 0,
        compare_error,
        join: None,
        join_error,
    }
}

fn cmp_err_join(
    a: Box<dyn Lattice>,
    b: Box<dyn Lattice>,
    compare_error: &'static str,
    join: Box<dyn Lattice>,
) -> Case {
    Case {
        a,
        b,
        compare_result: 0,
        compare_error,
        join: Some(join),
        join_error: "",
    }
}

fn assert_lattice_eq(actual: &dyn Lattice, expected: &dyn Lattice) {
    assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
}

fn assert_regexp(pattern: &str, error: &IncompatibleError) {
    let re = regex::Regex::new(pattern).expect("the Go test pattern compiles");
    assert!(
        re.is_match(&error.to_string()),
        "error {error:?} does not match {pattern:?}"
    );
}

// Go `TestCompatibilities`.
#[test]
#[allow(non_upper_case_globals)]
fn test_compatibilities() {
    let test_cases = vec![
        ok(
            Box::new(Bool(false)),
            Box::new(Bool(false)),
            0,
            Box::new(Bool(false)),
        ),
        ok(
            Box::new(Bool(false)),
            Box::new(Bool(true)),
            -1,
            Box::new(Bool(true)),
        ),
        ok(
            Box::new(Bool(true)),
            Box::new(Bool(true)),
            0,
            Box::new(Bool(true)),
        ),
        ok(
            singleton(Value::Int(123)),
            singleton(Value::Int(123)),
            0,
            singleton(Value::Int(123)),
        ),
        err(
            singleton(Value::Int(123)),
            singleton(Value::Int(2468)),
            r"distinct singletons.*",
            r"distinct singletons.*",
        ),
        cmp_err_join(
            Box::new(BitSet(0b010110)),
            Box::new(BitSet(0b110001)),
            r"non-inclusive bit sets.*",
            Box::new(BitSet(0b110111)),
        ),
        ok(
            Box::new(BitSet(0xffff_ffff)),
            Box::new(BitSet(0)),
            1,
            Box::new(BitSet(0xffff_ffff)),
        ),
        ok(
            Box::new(BitSet(0b10001)),
            Box::new(BitSet(0b11011)),
            -1,
            Box::new(BitSet(0b11011)),
        ),
        ok(
            Box::new(BitSet(0x522)),
            Box::new(BitSet(0x522)),
            0,
            Box::new(BitSet(0x522)),
        ),
        ok(
            Box::new(Byte(123)),
            Box::new(Byte(123)),
            0,
            Box::new(Byte(123)),
        ),
        ok(
            Box::new(Byte(1)),
            Box::new(Byte(23)),
            -1,
            Box::new(Byte(23)),
        ),
        ok(
            Box::new(Byte(123)),
            Box::new(Byte(45)),
            1,
            Box::new(Byte(123)),
        ),
        cmp_err_join(
            Box::new(Tuple(vec![Box::new(Byte(123)), Box::new(Bool(false))])),
            Box::new(Tuple(vec![Box::new(Byte(67)), Box::new(Bool(true))])),
            r"at tuple index 1: combining contradicting orders.*",
            Box::new(Tuple(vec![Box::new(Byte(123)), Box::new(Bool(true))])),
        ),
        ok(
            Box::new(Tuple(vec![])),
            Box::new(Tuple(vec![])),
            0,
            Box::new(Tuple(vec![])),
        ),
        err(
            Box::new(Tuple(vec![
                singleton(Value::Int(6)),
                singleton(Value::Int(7)),
            ])),
            Box::new(Tuple(vec![
                singleton(Value::Int(6)),
                singleton(Value::Int(8)),
            ])),
            r"at tuple index 1: distinct singletons.*",
            r"at tuple index 1: distinct singletons.*",
        ),
        err(
            Box::new(Tuple(vec![])),
            Box::new(Tuple(vec![Box::new(Bool(false))])),
            r"tuple length mismatch.*",
            r"tuple length mismatch.*",
        ),
        err(
            Box::new(Bool(false)),
            singleton(Value::Bool(false)),
            r"type mismatch.*",
            r"type mismatch.*",
        ),
        err(
            maybe(Some(singleton(Value::Int(123)))),
            maybe(Some(singleton(Value::Int(678)))),
            r"distinct singletons.*",
            r"distinct singletons.*",
        ),
        ok(
            maybe(Some(Box::new(Byte(111)))),
            maybe(Some(Box::new(Byte(222)))),
            -1,
            maybe(Some(Box::new(Byte(222)))),
        ),
        ok(
            maybe(None),
            maybe(Some(singleton(Value::Int(135)))),
            -1,
            maybe(Some(singleton(Value::Int(135)))),
        ),
        ok(maybe(None), maybe(None), 0, maybe(None)),
        err(
            Box::new(Bool(false)),
            maybe(Some(Box::new(Bool(false)))),
            r"type mismatch.*",
            r"type mismatch.*",
        ),
        ok(
            string_list(&["one", "two", "three"]),
            string_list(&["one", "two", "three", "four", "five"]),
            -1,
            string_list(&["one", "two", "three", "four", "five"]),
        ),
        err(
            string_list(&["one", "two", "three"]),
            string_list(&["two", "three"]),
            r"at string list index 0: distinct values.*",
            r"at string list index 0: distinct values.*",
        ),
        err(
            string_list(&["a", "b", "c"]),
            string_list(&["a", "e", "i", "o", "u"]),
            r"at string list index 1: distinct values.*",
            r"at string list index 1: distinct values.*",
        ),
        ok(string_list(&[]), string_list(&[]), 0, string_list(&[])),
        ok(
            equality_singleton(eq_bytes("abcdef")),
            equality_singleton(eq_bytes("abcdef")),
            0,
            equality_singleton(eq_bytes("abcdef")),
        ),
        err(
            equality_singleton(eq_bytes("abcdef")),
            equality_singleton(eq_bytes("ABCDEF")),
            r"distinct singletons.*",
            r"distinct singletons.*",
        ),
        err(
            equality_singleton(eq_bytes("abcdef")),
            singleton(Value::Equality(eq_bytes("ABCDEF"))),
            r"type mismatch.*",
            r"type mismatch.*",
        ),
        ok(
            Box::new(Int64(234)),
            Box::new(Int64(-5)),
            1,
            Box::new(Int64(234)),
        ),
        ok(
            Box::new(Uint(665_544)),
            Box::new(Uint(765)),
            1,
            Box::new(Uint(665_544)),
        ),
        cmp_err_join(
            uint_map(&[("a", 123), ("b", 678), ("c", 456)]),
            uint_map(&[("a", 234), ("b", 567), ("d", 789)]),
            r".*combining contradicting orders.*",
            uint_map(&[("a", 234), ("b", 678), ("c", 456), ("d", 789)]),
        ),
        ok(
            uint_map(&[("a", 123), ("b", 678), ("c", 456)]),
            uint_map(&[("a", 1), ("c", 4)]),
            1,
            uint_map(&[("a", 123), ("b", 678), ("c", 456)]),
        ),
        // TypeTiny compare/join with other integer types.
        ok(
            field_tp(TypeTiny),
            field_tp(TypeTiny),
            0,
            field_tp(TypeTiny),
        ),
        ok(
            field_tp(TypeTiny),
            field_tp(TypeShort),
            -1,
            field_tp(TypeShort),
        ),
        ok(
            field_tp(TypeTiny),
            field_tp(TypeInt24),
            -1,
            field_tp(TypeInt24),
        ),
        ok(
            field_tp(TypeTiny),
            field_tp(TypeLong),
            -1,
            field_tp(TypeLong),
        ),
        ok(
            field_tp(TypeTiny),
            field_tp(TypeLonglong),
            -1,
            field_tp(TypeLonglong),
        ),
        // TypeShort compare/join with other integer types.
        ok(
            field_tp(TypeShort),
            field_tp(TypeTiny),
            1,
            field_tp(TypeShort),
        ),
        ok(
            field_tp(TypeShort),
            field_tp(TypeShort),
            0,
            field_tp(TypeShort),
        ),
        ok(
            field_tp(TypeShort),
            field_tp(TypeInt24),
            -1,
            field_tp(TypeInt24),
        ),
        ok(
            field_tp(TypeShort),
            field_tp(TypeLong),
            -1,
            field_tp(TypeLong),
        ),
        ok(
            field_tp(TypeShort),
            field_tp(TypeLonglong),
            -1,
            field_tp(TypeLonglong),
        ),
        // TypeInt24 compare/join with other integer types.
        ok(
            field_tp(TypeInt24),
            field_tp(TypeTiny),
            1,
            field_tp(TypeInt24),
        ),
        ok(
            field_tp(TypeInt24),
            field_tp(TypeShort),
            1,
            field_tp(TypeInt24),
        ),
        ok(
            field_tp(TypeInt24),
            field_tp(TypeInt24),
            0,
            field_tp(TypeInt24),
        ),
        ok(
            field_tp(TypeInt24),
            field_tp(TypeLong),
            -1,
            field_tp(TypeLong),
        ),
        ok(
            field_tp(TypeInt24),
            field_tp(TypeLonglong),
            -1,
            field_tp(TypeLonglong),
        ),
        // TypeLong compare/join with other integer types.
        ok(
            field_tp(TypeLong),
            field_tp(TypeTiny),
            1,
            field_tp(TypeLong),
        ),
        ok(
            field_tp(TypeLong),
            field_tp(TypeShort),
            1,
            field_tp(TypeLong),
        ),
        ok(
            field_tp(TypeLong),
            field_tp(TypeInt24),
            1,
            field_tp(TypeLong),
        ),
        ok(
            field_tp(TypeLong),
            field_tp(TypeLong),
            0,
            field_tp(TypeLong),
        ),
        ok(
            field_tp(TypeLong),
            field_tp(TypeLonglong),
            -1,
            field_tp(TypeLonglong),
        ),
        // TypeLonglong compare/join with other integer types.
        ok(
            field_tp(TypeLonglong),
            field_tp(TypeTiny),
            1,
            field_tp(TypeLonglong),
        ),
        ok(
            field_tp(TypeLonglong),
            field_tp(TypeShort),
            1,
            field_tp(TypeLonglong),
        ),
        ok(
            field_tp(TypeLonglong),
            field_tp(TypeInt24),
            1,
            field_tp(TypeLonglong),
        ),
        ok(
            field_tp(TypeLonglong),
            field_tp(TypeLong),
            1,
            field_tp(TypeLonglong),
        ),
        ok(
            field_tp(TypeLonglong),
            field_tp(TypeLonglong),
            0,
            field_tp(TypeLonglong),
        ),
        // TypeTinyBlob compare/join with other blob types.
        ok(
            field_tp(TypeTinyBlob),
            field_tp(TypeTinyBlob),
            0,
            field_tp(TypeTinyBlob),
        ),
        ok(
            field_tp(TypeTinyBlob),
            field_tp(TypeBlob),
            -1,
            field_tp(TypeBlob),
        ),
        ok(
            field_tp(TypeTinyBlob),
            field_tp(TypeMediumBlob),
            -1,
            field_tp(TypeMediumBlob),
        ),
        ok(
            field_tp(TypeTinyBlob),
            field_tp(TypeLongBlob),
            -1,
            field_tp(TypeLongBlob),
        ),
        // TypeBlob compare/join with other blob types.
        ok(
            field_tp(TypeBlob),
            field_tp(TypeTinyBlob),
            1,
            field_tp(TypeBlob),
        ),
        ok(
            field_tp(TypeBlob),
            field_tp(TypeBlob),
            0,
            field_tp(TypeBlob),
        ),
        ok(
            field_tp(TypeBlob),
            field_tp(TypeMediumBlob),
            -1,
            field_tp(TypeMediumBlob),
        ),
        ok(
            field_tp(TypeBlob),
            field_tp(TypeLongBlob),
            -1,
            field_tp(TypeLongBlob),
        ),
        // TypeMediumBlob compare/join with other blob types.
        ok(
            field_tp(TypeMediumBlob),
            field_tp(TypeTinyBlob),
            1,
            field_tp(TypeMediumBlob),
        ),
        ok(
            field_tp(TypeMediumBlob),
            field_tp(TypeBlob),
            1,
            field_tp(TypeMediumBlob),
        ),
        ok(
            field_tp(TypeMediumBlob),
            field_tp(TypeMediumBlob),
            0,
            field_tp(TypeMediumBlob),
        ),
        ok(
            field_tp(TypeMediumBlob),
            field_tp(TypeLongBlob),
            -1,
            field_tp(TypeLongBlob),
        ),
        // TypeLongBlob compare/join with other blob types.
        ok(
            field_tp(TypeLongBlob),
            field_tp(TypeTinyBlob),
            1,
            field_tp(TypeLongBlob),
        ),
        ok(
            field_tp(TypeLongBlob),
            field_tp(TypeBlob),
            1,
            field_tp(TypeLongBlob),
        ),
        ok(
            field_tp(TypeLongBlob),
            field_tp(TypeMediumBlob),
            1,
            field_tp(TypeLongBlob),
        ),
        ok(
            field_tp(TypeLongBlob),
            field_tp(TypeLongBlob),
            0,
            field_tp(TypeLongBlob),
        ),
        // type mismatch or incompatible.
        err(
            field_tp(TypeLong),
            singleton(Value::Bool(false)),
            r"type mismatch.*",
            r"type mismatch.*",
        ),
        err(
            field_tp(TypeLong),
            field_tp(TypeSet),
            r"incompatible mysql type.*",
            r"incompatible mysql type.*",
        ),
    ];

    for tc in &test_cases {
        let compare = tc.a.compare(tc.b.as_ref());
        if !tc.compare_error.is_empty() {
            assert_regexp(tc.compare_error, &compare.unwrap_err());
        } else {
            assert_eq!(compare.unwrap(), tc.compare_result);
        }

        let compare = tc.b.compare(tc.a.as_ref());
        if !tc.compare_error.is_empty() {
            assert_regexp(tc.compare_error, &compare.unwrap_err());
        } else {
            assert_eq!(compare.unwrap(), -tc.compare_result);
        }

        let join = tc.a.join(tc.b.as_ref());
        if !tc.join_error.is_empty() {
            assert_regexp(tc.join_error, &join.unwrap_err());
        } else {
            assert_lattice_eq(
                join.unwrap().as_ref(),
                tc.join.as_ref().expect("a join expectation").as_ref(),
            );
        }

        let join = tc.b.join(tc.a.as_ref());
        if !tc.join_error.is_empty() {
            assert_regexp(tc.join_error, &join.unwrap_err());
        } else {
            let join = join.unwrap();
            assert_lattice_eq(
                join.as_ref(),
                tc.join.as_ref().expect("a join expectation").as_ref(),
            );

            let cmp = join.compare(tc.a.as_ref()).unwrap();
            assert!(cmp >= 0);

            let cmp = join.compare(tc.b.as_ref()).unwrap();
            assert!(cmp >= 0);
        }
    }
}
