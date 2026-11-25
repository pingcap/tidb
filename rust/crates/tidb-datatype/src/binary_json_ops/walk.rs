// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go BinaryJSON.Walk and extractToCallback over validated binary cells.

use std::collections::HashSet;
use std::ops::Range;

use smallvec::SmallVec;

use super::normalize_index;
use crate::binary_json::{BinaryJSONRef, JSONContainer};
use crate::json_path::{JSONPathLocation, format_concrete_path};
use crate::{
    BinaryJSONError, JSON_TYPE_CODE_ARRAY, JSON_TYPE_CODE_OBJECT, JSONPathArraySelection,
    JSONPathExpression, JSONPathLeg,
};

#[derive(Clone, Copy)]
enum Visit<'a> {
    Subtree,
    Select(&'a [JSONPathLeg]),
}

pub(super) fn visit_binary<'a>(
    root: BinaryJSONRef<'a>,
    paths: &[JSONPathExpression],
    visit_subtrees: bool,
    emit: impl FnMut(&[JSONPathLocation<'a>], BinaryJSONRef<'a>) -> bool,
) -> Result<(), BinaryJSONError> {
    // One fixed-depth selector reaches disjoint roots. Recursive descent
    // or multiple selectors can reach a path that was already visited.
    let may_overlap = paths.len() > 1
        || paths
            .iter()
            .any(|path| path.legs().contains(&JSONPathLeg::DoubleAsterisk));
    let mut walker = Walker {
        path: SmallVec::new(),
        seen: (visit_subtrees && may_overlap).then(HashSet::new),
        visit_subtrees,
        emit,
    };
    if paths.is_empty() {
        walker.visit(root, Visit::Subtree)?;
    } else {
        for path in paths {
            if walker.visit(root, Visit::Select(path.legs()))? {
                break;
            }
        }
    }
    Ok(())
}

struct Walker<'a, F> {
    path: SmallVec<[JSONPathLocation<'a>; 8]>,
    seen: Option<HashSet<String>>,
    visit_subtrees: bool,
    emit: F,
}

impl<'a, F> Walker<'a, F>
where
    F: FnMut(&[JSONPathLocation<'a>], BinaryJSONRef<'a>) -> bool,
{
    fn visit(
        &mut self,
        value: BinaryJSONRef<'a>,
        action: Visit<'_>,
    ) -> Result<bool, BinaryJSONError> {
        match action {
            Visit::Subtree => {
                if let Some(seen) = &mut self.seen {
                    if !seen.insert(format_concrete_path(&self.path)) {
                        return Ok(false);
                    }
                }
                if (self.emit)(&self.path, value) {
                    return Ok(true);
                }
                self.children(value, action)
            }
            Visit::Select([]) if self.visit_subtrees => self.visit(value, Visit::Subtree),
            Visit::Select([]) => Ok((self.emit)(&self.path, value)),
            Visit::Select([JSONPathLeg::Key(key), remain @ ..])
                if value.type_code == JSON_TYPE_CODE_OBJECT =>
            {
                let next = Visit::Select(remain);
                if key == "*" {
                    return self.children(value, next);
                }
                let object = value.container()?;
                let (index, found) = object.key_position(key.as_bytes())?;
                if found {
                    self.child(object, index, object_location(object, index)?, next)
                } else {
                    Ok(false)
                }
            }
            Visit::Select([JSONPathLeg::Array(selection), remain @ ..])
                if value.type_code == JSON_TYPE_CODE_ARRAY =>
            {
                let array = value.container()?;
                let next = Visit::Select(remain);
                for index in walk_indices(selection, array.count) {
                    let leg = JSONPathLocation::Index(walk_index(selection, index));
                    if self.child(array, index, leg, next)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Visit::Select([JSONPathLeg::DoubleAsterisk, remain @ ..]) => {
                if self.visit(value, Visit::Select(remain))? {
                    return Ok(true);
                }
                self.children(value, action)
            }
            Visit::Select(_) => Ok(false),
        }
    }

    fn children(
        &mut self,
        value: BinaryJSONRef<'a>,
        action: Visit<'_>,
    ) -> Result<bool, BinaryJSONError> {
        match value.type_code {
            JSON_TYPE_CODE_ARRAY => {
                let array = value.container()?;
                for index in 0..array.count {
                    if self.child(array, index, JSONPathLocation::Index(index as i64), action)? {
                        return Ok(true);
                    }
                }
            }
            JSON_TYPE_CODE_OBJECT => {
                let object = value.container()?;
                for index in 0..object.count {
                    if self.child(object, index, object_location(object, index)?, action)? {
                        return Ok(true);
                    }
                }
            }
            _ => {}
        }
        Ok(false)
    }

    fn child(
        &mut self,
        parent: JSONContainer<'a>,
        index: usize,
        leg: JSONPathLocation<'a>,
        action: Visit<'_>,
    ) -> Result<bool, BinaryJSONError> {
        let child = parent.child(index)?;
        self.path.push(leg);
        let stop = self.visit(child, action);
        self.path.pop();
        stop
    }
}

fn object_location(
    object: JSONContainer<'_>,
    index: usize,
) -> Result<JSONPathLocation<'_>, BinaryJSONError> {
    let key = simdutf8::basic::from_utf8(object.key(index)?)
        .map_err(|_| BinaryJSONError::InvalidBinary)?;
    Ok(JSONPathLocation::Key(key))
}

pub(super) fn walk_indices(selection: &JSONPathArraySelection, length: usize) -> Range<usize> {
    match selection {
        JSONPathArraySelection::Asterisk => 0..length,
        JSONPathArraySelection::Index(index) => {
            normalize_index(*index, length).map_or(0..0, |index| index..index + 1)
        }
        JSONPathArraySelection::Range { start, end } if length != 0 => {
            let Some(start) = normalize_index(*start, length) else {
                return 0..0;
            };
            let end = if *end < 0 {
                let Some(end) = normalize_index(*end, length) else {
                    return 0..0;
                };
                end
            } else {
                usize::try_from(*end).unwrap_or(usize::MAX).min(length - 1)
            };
            start..end + 1
        }
        JSONPathArraySelection::Range { .. } => 0..0,
    }
}

pub(super) fn walk_index(selection: &JSONPathArraySelection, actual: usize) -> i64 {
    match selection {
        // Go's callback retains the requested index spelling, including last-N.
        JSONPathArraySelection::Index(requested) => *requested,
        _ => actual as i64,
    }
}
