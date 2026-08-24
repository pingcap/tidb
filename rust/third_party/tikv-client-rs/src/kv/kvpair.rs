// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::str;

#[cfg(test)]
use proptest_derive::Arbitrary;

use super::HexRepr;
use super::Key;
use super::Value;
use crate::proto::kvrpcpb;

/// A key/value pair.
///
/// # Examples
/// ```rust
/// # use tikv_client::{Key, Value, KvPair};
/// let key = "key".to_owned();
/// let value = "value".to_owned();
/// let constructed = KvPair::new(key.clone(), value.clone());
/// let from_tuple = KvPair::from((key, value));
/// assert_eq!(constructed, from_tuple);
/// ```
///
/// Many functions which accept a `KvPair` accept an `Into<KvPair>`, which means all of the above
/// types (Like a `(Key, Value)`) can be passed directly to those functions.
#[derive(Default, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(test, derive(Arbitrary))]
pub struct KvPair(pub Key, pub Value);

impl KvPair {
    /// Create a new `KvPair`.
    #[inline]
    pub fn new(key: impl Into<Key>, value: impl Into<Value>) -> Self {
        KvPair(key.into(), value.into())
    }

    /// Immutably borrow the `Key` part of the `KvPair`.
    #[inline]
    pub fn key(&self) -> &Key {
        &self.0
    }

    /// Immutably borrow the `Value` part of the `KvPair`.
    #[inline]
    pub fn value(&self) -> &Value {
        &self.1
    }

    /// Consume `self` and return the `Key` part.
    #[inline]
    pub fn into_key(self) -> Key {
        self.0
    }

    /// Consume `self` and return the `Value` part.
    #[inline]
    pub fn into_value(self) -> Value {
        self.1
    }

    /// Mutably borrow the `Key` part of the `KvPair`.
    #[inline]
    pub fn key_mut(&mut self) -> &mut Key {
        &mut self.0
    }

    /// Mutably borrow the `Value` part of the `KvPair`.
    #[inline]
    pub fn value_mut(&mut self) -> &mut Value {
        &mut self.1
    }

    /// Set the `Key` part of the `KvPair`.
    #[inline]
    pub fn set_key(&mut self, k: impl Into<Key>) {
        self.0 = k.into();
    }

    /// Set the `Value` part of the `KvPair`.
    #[inline]
    pub fn set_value(&mut self, v: impl Into<Value>) {
        self.1 = v.into();
    }
}

impl<K, V> From<(K, V)> for KvPair
where
    K: Into<Key>,
    V: Into<Value>,
{
    fn from((k, v): (K, V)) -> Self {
        KvPair(k.into(), v.into())
    }
}

impl From<KvPair> for (Key, Value) {
    fn from(pair: KvPair) -> Self {
        (pair.0, pair.1)
    }
}

impl From<KvPair> for Key {
    fn from(pair: KvPair) -> Self {
        pair.0
    }
}

impl From<kvrpcpb::KvPair> for KvPair {
    fn from(pair: kvrpcpb::KvPair) -> Self {
        KvPair(Key::from(pair.key), pair.value)
    }
}

impl From<KvPair> for kvrpcpb::KvPair {
    fn from(pair: KvPair) -> Self {
        let mut result = kvrpcpb::KvPair::default();
        let (key, value) = pair.into();
        result.key = key.into();
        result.value = value;
        result
    }
}

impl AsRef<Key> for KvPair {
    fn as_ref(&self) -> &Key {
        &self.0
    }
}

impl AsRef<Value> for KvPair {
    fn as_ref(&self) -> &Value {
        &self.1
    }
}

impl fmt::Debug for KvPair {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let KvPair(key, value) = self;
        match str::from_utf8(value) {
            Ok(s) => write!(f, "KvPair({}, {:?})", HexRepr(&key.0), s),
            Err(_) => write!(f, "KvPair({}, {})", HexRepr(&key.0), HexRepr(value)),
        }
    }
}
