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

//! Go `github.com/pingcap/failpoint` and `pkg/testkit/testfailpoint`.
//!
//! A failpoint is named as failpoint-ctl names Go's: the package path plus
//! the `Inject` name (`github.com/pingcap/tidb/pkg/store/copr/x`), so a Go
//! test's `failpoint.Enable(path, terms)` ports verbatim. Terms are Go's
//! (`return(true)`, `3*return("x")`, `pause`, `panic`, `sleep(10)`). A build
//! without the `failpoints` feature (Go without failpoint-ctl) compiles every
//! failpoint out.

/// Go `failpoint.Value`: the term's `return` argument.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value(Option<String>);

impl Value {
    /// Go `val.(bool)`.
    pub fn bool(&self) -> bool {
        match self.0.as_deref() {
            Some("true") => true,
            Some("false") => false,
            other => panic!("failpoint value {other:?} is not a bool"),
        }
    }

    /// Go `val.(int)`.
    pub fn int(&self) -> i64 {
        self.0
            .as_deref()
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("failpoint value {:?} is not an int", self.0))
    }

    /// Go `v, ok := val.(int)`.
    pub fn as_int(&self) -> Option<i64> {
        self.0.as_deref().and_then(|value| value.parse().ok())
    }

    /// Go `val.(string)`.
    pub fn string(&self) -> String {
        self.0
            .as_deref()
            .and_then(|value| value.strip_prefix('"')?.strip_suffix('"'))
            .unwrap_or_else(|| panic!("failpoint value {:?} is not a string", self.0))
            .to_owned()
    }
}

/// Go `failpoint.Eval(name)`: the value when the failpoint fires.
#[inline]
pub fn eval(name: &str) -> Option<Value> {
    if cfg!(feature = "failpoints") {
        fail::eval(name, Value)
    } else {
        None
    }
}

/// Go `failpoint.Inject(name, f)`.
#[inline]
pub fn inject(name: &str, f: impl FnOnce(Value)) {
    if let Some(value) = eval(name) {
        f(value);
    }
}

/// Go `failpoint.Enable(name, terms)`.
///
/// # Errors
/// The terms do not parse.
pub fn enable(name: &str, terms: &str) -> Result<(), String> {
    assert!(
        cfg!(feature = "failpoints"),
        "failpoint {name} is enabled in a build without tidb-failpoint/failpoints"
    );
    fail::cfg(name, terms)
}

/// Go `testfailpoint.EnableCall(t, name, fn)`: `fn` runs where the
/// failpoint is evaluated (Go `failpoint.InjectCall`).
///
/// # Errors
/// The failpoint cannot be configured.
pub fn enable_call(name: &str, f: impl Fn() + Send + Sync + 'static) -> Result<(), String> {
    assert!(
        cfg!(feature = "failpoints"),
        "failpoint {name} is enabled in a build without tidb-failpoint/failpoints"
    );
    fail::cfg_callback(name, f)
}

/// Go `failpoint.List()` with each failpoint's terms (Go `Status`).
#[must_use]
pub fn list() -> Vec<(String, String)> {
    fail::list()
}

/// Go `failpoint.Disable(name)`.
pub fn disable(name: &str) {
    fail::remove(name);
}

/// Go `testfailpoint.Enable(t, name, terms)`: the failpoint is disabled when
/// the guard drops (Go's `t.Cleanup`).
#[must_use]
pub fn enable_for_test(name: &str, terms: &str) -> Guard {
    enable(name, terms).unwrap_or_else(|error| panic!("enable failpoint {name}: {error}"));
    Guard(name.to_owned())
}

/// Disables its failpoint on drop.
pub struct Guard(String);

impl Drop for Guard {
    fn drop(&mut self) {
        disable(&self.0);
    }
}

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    use super::*;

    #[test]
    fn terms_and_values_are_gos() {
        let name = "github.com/pingcap/tidb/pkg/failpoint/x";
        assert_eq!(eval(name), None);
        let _guard = enable_for_test(name, "2*return(\"a\")");
        assert_eq!(eval(name).map(|value| value.string()), Some("a".to_owned()));
        assert_eq!(eval(name).map(|value| value.string()), Some("a".to_owned()));
        assert_eq!(eval(name), None);
        enable(name, "return(true)").unwrap();
        assert!(eval(name).unwrap().bool());
        enable(name, "return(-3)").unwrap();
        assert_eq!(eval(name).unwrap().int(), -3);
        disable(name);
        assert_eq!(eval(name), None);
    }
}
