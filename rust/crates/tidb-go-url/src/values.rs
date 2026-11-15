//! Go `net/url`'s `Values`, `ParseQuery` and `Values.Encode`.

use crate::escape::{query_escape, query_unescape};
use crate::Error;
use std::collections::BTreeMap;

/// Go `url.Values`. Go's map has no order; this one iterates by key, the
/// order `Values.Encode` sorts into.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Values(pub BTreeMap<Vec<u8>, Vec<Vec<u8>>>);

impl Values {
    /// Go `Values.Get`: the first value of `key`, or empty.
    pub fn get(&self, key: impl AsRef<[u8]>) -> &[u8] {
        self.0
            .get(key.as_ref())
            .and_then(|values| values.first())
            .map_or(b"", Vec::as_slice)
    }

    /// Go `Values.Set`.
    pub fn set(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.0.insert(key.into(), vec![value.into()]);
    }

    /// Go `Values.Add`.
    pub fn add(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.0.entry(key.into()).or_default().push(value.into());
    }

    /// Go `Values.Del`.
    pub fn del(&mut self, key: impl AsRef<[u8]>) {
        self.0.remove(key.as_ref());
    }

    /// Go `Values.Has`.
    pub fn has(&self, key: impl AsRef<[u8]>) -> bool {
        self.0.contains_key(key.as_ref())
    }

    /// Go `Values.Encode`: `key=value` pairs sorted by key.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for (key, values) in &self.0 {
            let key = query_escape(key);
            for value in values {
                if !buf.is_empty() {
                    buf.push(b'&');
                }
                buf.extend_from_slice(&key);
                buf.push(b'=');
                buf.extend(query_escape(value));
            }
        }
        buf
    }
}

/// Go `defaultMaxParams`. Go's `urlmaxqueryparams` GODEBUG override has no
/// counterpart in this process.
const DEFAULT_MAX_PARAMS: usize = 10000;

/// Go `url.ParseQuery`: every valid pair, and the first decoding error.
pub fn parse_query(query: impl AsRef<[u8]>) -> (Values, Option<Error>) {
    let mut values = Values::default();
    let err = parse_query_into(&mut values, query.as_ref());
    (values, err)
}

/// Go `parseQuery`.
fn parse_query_into(m: &mut Values, mut query: &[u8]) -> Option<Error> {
    if query.iter().filter(|&&c| c == b'&').count() + 1 > DEFAULT_MAX_PARAMS {
        return Some(Error::Other(
            "number of URL query parameters exceeded limit".into(),
        ));
    }
    let mut err = None;
    while !query.is_empty() {
        let key;
        (key, query) = match query.iter().position(|&c| c == b'&') {
            Some(i) => (&query[..i], &query[i + 1..]),
            None => (query, &b""[..]),
        };
        if key.contains(&b';') {
            // Go overwrites any earlier error here.
            err = Some(Error::Other("invalid semicolon separator in query".into()));
            continue;
        }
        if key.is_empty() {
            continue;
        }
        let (key, value) = match key.iter().position(|&c| c == b'=') {
            Some(i) => (&key[..i], &key[i + 1..]),
            None => (key, &b""[..]),
        };
        let key = match query_unescape(key) {
            Ok(key) => key,
            Err(e) => {
                err.get_or_insert(e);
                continue;
            }
        };
        let value = match query_unescape(value) {
            Ok(value) => value,
            Err(e) => {
                err.get_or_insert(e);
                continue;
            }
        };
        m.add(key, value);
    }
    err
}
