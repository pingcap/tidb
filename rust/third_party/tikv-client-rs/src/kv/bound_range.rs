// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;
use std::cmp::Eq;
use std::cmp::PartialEq;
use std::ops::Bound;
use std::ops::Range;
use std::ops::RangeBounds;
use std::ops::RangeFrom;
use std::ops::RangeFull;
use std::ops::RangeInclusive;
use std::ops::RangeTo;
use std::ops::RangeToInclusive;

#[cfg(test)]
use proptest_derive::Arbitrary;

use super::Key;
use crate::proto::kvrpcpb;

/// A struct for expressing ranges. This type is semi-opaque and is not really meant for users to
/// deal with directly. Most functions which operate on ranges will accept any types which
/// implement `Into<BoundRange>`.
///
/// In TiKV, keys are an ordered sequence of bytes. This means we can have ranges over those
/// bytes. Eg `001` is before `010`.
///
/// **Minimum key**: there is the minimum key: empty key. So a range may not be unbounded below.
/// The unbounded lower bound in a [`Range`](Range) will be converted to an empty key.
///
/// **Maximum key**: There is no limit of the maximum key. When an empty key is used as the upper bound, it means upper unbounded.
/// The unbounded upper bound in a [`Range`](Range). The range covering all keys is just `Key::EMPTY..`.
///
/// **But, you should not need to worry about all this:** Most functions which operate
/// on ranges will accept any types which implement `Into<BoundRange>`.
/// Common range types like `a..b`, `a..=b` has implemented `Into<BoundRange>`where `a` and `b`
/// `impl Into<Key>`. You can implement `Into<BoundRange>` for your own types by using `try_from`.
/// It means all of the following types in the example can be passed directly to those functions.
///
/// # Examples
/// ```rust
/// # use std::ops::{Range, RangeInclusive, RangeTo, RangeToInclusive, RangeFrom, RangeFull, Bound};
/// # use std::convert::TryInto;
/// # use tikv_client::{Key, BoundRange};
///
/// let explict_range: Range<Key> = Range { start: Key::from("Rust".to_owned()), end: Key::from("TiKV".to_owned()) };
/// let from_explict_range: BoundRange = explict_range.into();
///
/// let range: Range<String> = "Rust".to_owned().."TiKV".to_owned();
/// let from_range: BoundRange = range.into();
/// assert_eq!(from_explict_range, from_range);
///
/// let range: RangeInclusive<String> = "Rust".to_owned()..="TiKV".to_owned();
/// let from_range: BoundRange = range.into();
/// assert_eq!(
///     from_range,
///     (Bound::Included(Key::from("Rust".to_owned())), Bound::Included(Key::from("TiKV".to_owned()))),
/// );
///
/// let range_from: RangeFrom<String> = "Rust".to_owned()..;
/// let from_range_from: BoundRange = range_from.into();
/// assert_eq!(
///     from_range_from,
///     (Bound::Included(Key::from("Rust".to_owned())), Bound::Unbounded),
/// );
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(Arbitrary))]
pub struct BoundRange {
    pub from: Bound<Key>,
    pub to: Bound<Key>,
}

impl BoundRange {
    /// Create a new BoundRange.
    ///
    /// The caller must ensure that `from` is not `Unbounded`.
    pub fn new(from: Bound<Key>, to: Bound<Key>) -> BoundRange {
        BoundRange { from, to }
    }

    /// Create a new BoundRange bounded below by `from` and unbounded above.
    pub fn range_from(from: Key) -> BoundRange {
        BoundRange {
            from: Bound::Included(from),
            to: Bound::Unbounded,
        }
    }

    /// Ranges used in scanning TiKV have a particularity to them.
    ///
    /// The **start** of a scan is inclusive, unless appended with an '\0', then it is exclusive.
    ///
    /// The **end** of a scan is exclusive, unless appended with an '\0', then it is inclusive.
    ///
    /// # Examples
    /// ```rust
    /// use tikv_client::{BoundRange, Key, IntoOwnedRange};
    /// // Exclusive
    /// let range = "a".."z";
    /// assert_eq!(
    ///     BoundRange::from(range.into_owned()).into_keys(),
    ///     (Key::from("a".to_owned()), Some(Key::from("z".to_owned()))),
    /// );
    /// // Inclusive
    /// let range = "a"..="z";
    /// assert_eq!(
    ///     BoundRange::from(range.into_owned()).into_keys(),
    ///     (Key::from("a".to_owned()), Some(Key::from("z\0".to_owned()))),
    /// );
    /// // Open right
    /// let range = "a".to_owned()..;
    /// assert_eq!(
    ///     BoundRange::from(range).into_keys(),
    ///     (Key::from("a".to_owned()), None),
    /// );
    /// // Left open right exclusive
    /// let range = .."z";
    /// assert_eq!(
    ///     BoundRange::from(range.into_owned()).into_keys(),
    ///     (Key::from("".to_owned()), Some(Key::from("z".to_owned()))),
    /// );
    /// // Left open right inclusive
    /// let range = ..="z";
    /// assert_eq!(
    ///     BoundRange::from(range.into_owned()).into_keys(),
    ///     (Key::from("".to_owned()), Some(Key::from("z\0".to_owned()))),
    /// );
    /// // Full range
    /// let range = ..;
    /// assert_eq!(
    ///     BoundRange::from(range).into_keys(),
    ///     (Key::from("".to_owned()), None),
    /// );
    // ```
    pub fn into_keys(self) -> (Key, Option<Key>) {
        let start = match self.from {
            Bound::Included(v) => v,
            Bound::Excluded(v) => v.next_key(),
            Bound::Unbounded => Key::EMPTY,
        };
        let end = match self.to {
            Bound::Included(v) => Some(v.next_key()),
            Bound::Excluded(v) => Some(v),
            Bound::Unbounded => None,
        };
        (start, end)
    }
}

impl RangeBounds<Key> for BoundRange {
    // clippy will act differently on nightly and stable, so we allow `needless_match` here.
    #[allow(clippy::needless_match)]
    fn start_bound(&self) -> Bound<&Key> {
        match &self.from {
            Bound::Included(f) => Bound::Included(f),
            Bound::Excluded(f) => Bound::Excluded(f),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&Key> {
        match &self.to {
            Bound::Included(t) => {
                if t.is_empty() {
                    Bound::Unbounded
                } else {
                    Bound::Included(t)
                }
            }
            Bound::Excluded(t) => {
                if t.is_empty() {
                    Bound::Unbounded
                } else {
                    Bound::Excluded(t)
                }
            }
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

// FIXME `==` should not `clone`
impl<T: Into<Key> + Clone> PartialEq<(Bound<T>, Bound<T>)> for BoundRange {
    fn eq(&self, other: &(Bound<T>, Bound<T>)) -> bool {
        self.from == convert_to_bound_key(other.0.clone())
            && self.to == convert_to_bound_key(other.1.clone())
    }
}

impl<T: Into<Key>> From<Range<T>> for BoundRange {
    fn from(other: Range<T>) -> BoundRange {
        BoundRange::new(
            Bound::Included(other.start.into()),
            Bound::Excluded(other.end.into()),
        )
    }
}

impl<T: Into<Key>> From<RangeFrom<T>> for BoundRange {
    fn from(other: RangeFrom<T>) -> BoundRange {
        BoundRange::new(Bound::Included(other.start.into()), Bound::Unbounded)
    }
}

impl<T: Into<Key>> From<RangeTo<T>> for BoundRange {
    fn from(other: RangeTo<T>) -> BoundRange {
        BoundRange::new(Bound::Unbounded, Bound::Excluded(other.end.into()))
    }
}

impl<T: Into<Key>> From<RangeInclusive<T>> for BoundRange {
    fn from(other: RangeInclusive<T>) -> BoundRange {
        let (start, end) = other.into_inner();
        BoundRange::new(Bound::Included(start.into()), Bound::Included(end.into()))
    }
}

impl<T: Into<Key>> From<RangeToInclusive<T>> for BoundRange {
    fn from(other: RangeToInclusive<T>) -> BoundRange {
        BoundRange::new(Bound::Unbounded, Bound::Included(other.end.into()))
    }
}

impl From<RangeFull> for BoundRange {
    fn from(_other: RangeFull) -> BoundRange {
        BoundRange::new(Bound::Unbounded, Bound::Unbounded)
    }
}

impl<T: Into<Key>> From<(T, Option<T>)> for BoundRange {
    fn from(other: (T, Option<T>)) -> BoundRange {
        let to = match other.1 {
            None => Bound::Unbounded,
            Some(to) => to.into().into_upper_bound(),
        };

        BoundRange::new(other.0.into().into_lower_bound(), to)
    }
}

impl<T: Into<Key>> From<(T, T)> for BoundRange {
    fn from(other: (T, T)) -> BoundRange {
        BoundRange::new(
            other.0.into().into_lower_bound(),
            other.1.into().into_upper_bound(),
        )
    }
}

impl<T: Into<Key> + Eq> From<(Bound<T>, Bound<T>)> for BoundRange {
    fn from(bounds: (Bound<T>, Bound<T>)) -> BoundRange {
        BoundRange::new(
            convert_to_bound_key(bounds.0),
            convert_to_bound_key(bounds.1),
        )
    }
}

impl From<BoundRange> for kvrpcpb::KeyRange {
    fn from(bound_range: BoundRange) -> Self {
        let (start, end) = bound_range.into_keys();
        let mut range = kvrpcpb::KeyRange::default();
        range.start_key = start.into();
        range.end_key = end.unwrap_or_default().into();
        range
    }
}

impl From<kvrpcpb::KeyRange> for BoundRange {
    fn from(range: kvrpcpb::KeyRange) -> Self {
        let start_key = Key::from(range.start_key);
        let end_key = Key::from(range.end_key);
        BoundRange::new(start_key.into_lower_bound(), end_key.into_upper_bound())
    }
}

/// A convenience trait for converting ranges of borrowed types into a `BoundRange`.
///
/// # Examples
/// ```rust
/// # use tikv_client::{IntoOwnedRange, BoundRange};
/// # use std::ops::*;
/// let r1: Range<&str> = "s".."e";
/// let r1: BoundRange = r1.into_owned();
///
/// let r2: RangeFrom<&str> = "start"..;
/// let r2: BoundRange = r2.into_owned();
///
/// let r3: RangeInclusive<&str> = "s"..="e";
/// let r3: BoundRange = r3.into_owned();
///
/// let r4: RangeTo<&str> = .."z";
/// let r4: BoundRange = r4.into_owned();
///
/// let k1: Vec<u8> = "start".to_owned().into_bytes();
/// let k2: Vec<u8> = "end".to_owned().into_bytes();
/// let r4: BoundRange = (&k1, &k2).into_owned();
/// let r5: BoundRange = (&k1, None).into_owned();
/// let r6: BoundRange = (&k1, Some(&k2)).into_owned();
/// ```
pub trait IntoOwnedRange {
    /// Transform a borrowed range of some form into an owned `BoundRange`.
    fn into_owned(self) -> BoundRange;
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange for Range<&U> {
    fn into_owned(self) -> BoundRange {
        From::from(Range {
            start: self.start.to_owned(),
            end: self.end.to_owned(),
        })
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange for RangeFrom<&U> {
    fn into_owned(self) -> BoundRange {
        From::from(RangeFrom {
            start: self.start.to_owned(),
        })
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange for RangeTo<&U> {
    fn into_owned(self) -> BoundRange {
        From::from(RangeTo {
            end: self.end.to_owned(),
        })
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange
    for RangeInclusive<&U>
{
    fn into_owned(self) -> BoundRange {
        let (from, to) = self.into_inner();
        From::from(RangeInclusive::new(from.to_owned(), to.to_owned()))
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange
    for RangeToInclusive<&U>
{
    fn into_owned(self) -> BoundRange {
        From::from(RangeToInclusive {
            end: self.end.to_owned(),
        })
    }
}

impl IntoOwnedRange for RangeFull {
    fn into_owned(self) -> BoundRange {
        From::from(self)
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange for (&U, Option<&U>) {
    fn into_owned(self) -> BoundRange {
        From::from((self.0.to_owned(), self.1.map(|u| u.to_owned())))
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange for (&U, &U) {
    fn into_owned(self) -> BoundRange {
        From::from((self.0.to_owned(), self.1.to_owned()))
    }
}

fn convert_to_bound_key<K: Into<Key>>(b: Bound<K>) -> Bound<Key> {
    match b {
        Bound::Included(k) => Bound::Included(k.into()),
        Bound::Excluded(k) => Bound::Excluded(k.into()),
        Bound::Unbounded => Bound::Unbounded,
    }
}
