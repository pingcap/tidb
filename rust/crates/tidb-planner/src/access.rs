// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/planner/core/access/access_obj.go`.

use std::fmt;

use tidb_proto::tipb;
use tipb::access_object::AccessObject as PbAccessObjectKind;

/// Go `ScanAccessObject`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScanAccessObject {
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Accessed indexes.
    pub indexes: Vec<IndexAccess>,
    /// Accessed static partitions.
    pub partitions: Vec<String>,
}

impl ScanAccessObject {
    /// Go `NormalizedString`.
    #[must_use]
    pub fn normalized_string(&self) -> String {
        self.render(true)
    }

    fn render(&self, normalized: bool) -> String {
        let mut output = String::new();
        if !self.table.is_empty() {
            output.push_str("table:");
            output.push_str(&self.table);
        }
        if !self.partitions.is_empty() {
            output.push_str(", partition:");
            if normalized {
                output.push('?');
            } else {
                output.push_str(&self.partitions.join(","));
            }
        }
        for index in &self.indexes {
            if index.is_clustered_index {
                output.push_str(", clustered index:");
            } else {
                output.push_str(", index:");
            }
            output.push_str(&index.name);
            output.push('(');
            output.push_str(&index.cols.join(", "));
            output.push(')');
        }
        output
    }

    fn to_pb(&self) -> tipb::ScanAccessObject {
        tipb::ScanAccessObject {
            database: self.database.clone(),
            table: self.table.clone(),
            indexes: self.indexes.iter().map(IndexAccess::to_pb).collect(),
            partitions: self.partitions.clone(),
        }
    }
}

impl fmt::Display for ScanAccessObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.render(false))
    }
}

/// Go `IndexAccess`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexAccess {
    /// Index name.
    pub name: String,
    /// Index column names.
    pub cols: Vec<String>,
    /// Whether this is the clustered primary index.
    pub is_clustered_index: bool,
}

impl IndexAccess {
    /// Go `ToPB` for a non-nil receiver.
    #[must_use]
    pub fn to_pb(&self) -> tipb::IndexAccess {
        tipb::IndexAccess {
            name: self.name.clone(),
            cols: self.cols.clone(),
            is_clustered_index: self.is_clustered_index,
        }
    }
}

/// Go `OtherAccessObject`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OtherAccessObject(pub String);

impl OtherAccessObject {
    /// Go `NormalizedString`.
    #[must_use]
    pub fn normalized_string(&self) -> String {
        self.0.clone()
    }
}

impl fmt::Display for OtherAccessObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Go `DynamicPartitionAccessObject`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DynamicPartitionAccessObject {
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Whether every partition is accessed.
    pub all_partitions: bool,
    /// Accessed partition names.
    pub partitions: Vec<String>,
    /// Display error; protobuf output leaves this slot as its zero value.
    pub error: String,
}

impl fmt::Display for DynamicPartitionAccessObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.error.is_empty() {
            return formatter.write_str(&self.error);
        }
        if self.all_partitions {
            formatter.write_str("partition:all")
        } else if self.partitions.is_empty() {
            formatter.write_str("partition:dual")
        } else {
            write!(formatter, "partition:{}", self.partitions.join(","))
        }
    }
}

/// Go `DynamicPartitionAccessObjects`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DynamicPartitionAccessObjects(pub Vec<DynamicPartitionAccessObject>);

impl DynamicPartitionAccessObjects {
    /// Go `NormalizedString`, which deliberately equals `String`.
    #[must_use]
    pub fn normalized_string(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for DynamicPartitionAccessObjects {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, access) in self.0.iter().enumerate() {
            if index != 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{access}")?;
            if self.0.len() != 1 {
                write!(formatter, " of {}", access.table)?;
            }
        }
        Ok(())
    }
}

/// The closed Rust equivalent of Go's `base.AccessObject` implementors from
/// this package.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessObject {
    /// Scan access.
    Scan(ScanAccessObject),
    /// Other string access.
    Other(OtherAccessObject),
    /// Dynamic partition access.
    DynamicPartitions(DynamicPartitionAccessObjects),
}

impl AccessObject {
    /// Calls the source implementor's `NormalizedString`.
    #[must_use]
    pub fn normalized_string(&self) -> String {
        match self {
            Self::Scan(object) => object.normalized_string(),
            Self::Other(object) => object.normalized_string(),
            Self::DynamicPartitions(object) => object.normalized_string(),
        }
    }

    /// Calls the source implementor's `SetIntoPB`.
    pub fn set_into_pb(&self, operator: &mut tipb::ExplainOperator) {
        let access_object = match self {
            Self::Scan(object) => Some(PbAccessObjectKind::ScanObject(object.to_pb())),
            Self::Other(object) if object.0.is_empty() => None,
            Self::Other(object) => Some(PbAccessObjectKind::OtherObject(object.0.clone())),
            Self::DynamicPartitions(objects) if objects.0.is_empty() => None,
            Self::DynamicPartitions(objects) => {
                let objects = objects
                    .0
                    .iter()
                    .map(|object| {
                        if object.error.is_empty() {
                            tipb::DynamicPartitionAccessObject {
                                database: object.database.clone(),
                                table: object.table.clone(),
                                all_partitions: object.all_partitions,
                                partitions: object.partitions.clone(),
                            }
                        } else {
                            tipb::DynamicPartitionAccessObject::default()
                        }
                    })
                    .collect();
                Some(PbAccessObjectKind::DynamicPartitionObjects(
                    tipb::DynamicPartitionAccessObjects { objects },
                ))
            }
        };
        if let Some(access_object) = access_object {
            operator.access_objects = vec![tipb::AccessObject {
                access_object: Some(access_object),
            }];
        }
    }
}

impl fmt::Display for AccessObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scan(object) => object.fmt(formatter),
            Self::Other(object) => object.fmt(formatter),
            Self::DynamicPartitions(object) => object.fmt(formatter),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_strings_and_pb_match_go() {
        let object = ScanAccessObject {
            database: "test".to_owned(),
            table: "t".to_owned(),
            indexes: vec![
                IndexAccess {
                    name: "idx".to_owned(),
                    cols: vec!["a".to_owned(), "b".to_owned()],
                    is_clustered_index: false,
                },
                IndexAccess {
                    name: "PRIMARY".to_owned(),
                    cols: vec!["a".to_owned()],
                    is_clustered_index: true,
                },
            ],
            partitions: vec!["p0".to_owned(), "p1".to_owned()],
        };
        assert_eq!(
            object.to_string(),
            "table:t, partition:p0,p1, index:idx(a, b), clustered index:PRIMARY(a)"
        );
        assert_eq!(
            object.normalized_string(),
            "table:t, partition:?, index:idx(a, b), clustered index:PRIMARY(a)"
        );
        let mut operator = tipb::ExplainOperator::default();
        AccessObject::Scan(object).set_into_pb(&mut operator);
        let Some(PbAccessObjectKind::ScanObject(scan)) =
            operator.access_objects[0].access_object.as_ref()
        else {
            panic!("scan access object")
        };
        assert_eq!(scan.database, "test");
        assert_eq!(scan.indexes.len(), 2);
    }

    #[test]
    fn other_and_dynamic_objects_match_empty_error_and_multi_table_branches() {
        let mut operator = tipb::ExplainOperator::default();
        AccessObject::Other(OtherAccessObject::default()).set_into_pb(&mut operator);
        assert!(operator.access_objects.is_empty());
        let other = AccessObject::Other(OtherAccessObject("cte:x".to_owned()));
        assert_eq!(other.normalized_string(), "cte:x");
        other.set_into_pb(&mut operator);
        let Some(PbAccessObjectKind::OtherObject(other)) =
            operator.access_objects[0].access_object.as_ref()
        else {
            panic!("other access object")
        };
        assert_eq!(other, "cte:x");

        let single = DynamicPartitionAccessObjects(vec![DynamicPartitionAccessObject {
            table: "t".to_owned(),
            partitions: vec!["p0".to_owned(), "p1".to_owned()],
            ..DynamicPartitionAccessObject::default()
        }]);
        assert_eq!(single.to_string(), "partition:p0,p1");
        assert_eq!(single.normalized_string(), "partition:p0,p1");

        let dynamic = DynamicPartitionAccessObjects(vec![
            DynamicPartitionAccessObject {
                table: "t1".to_owned(),
                all_partitions: true,
                ..DynamicPartitionAccessObject::default()
            },
            DynamicPartitionAccessObject {
                table: "t2".to_owned(),
                error: "partition error".to_owned(),
                ..DynamicPartitionAccessObject::default()
            },
        ]);
        assert_eq!(
            dynamic.to_string(),
            "partition:all of t1, partition error of t2"
        );
        let object = AccessObject::DynamicPartitions(dynamic);
        object.set_into_pb(&mut operator);
        let Some(PbAccessObjectKind::DynamicPartitionObjects(dynamic)) =
            operator.access_objects[0].access_object.as_ref()
        else {
            panic!("dynamic access object")
        };
        assert_eq!(dynamic.objects.len(), 2);
        assert_eq!(dynamic.objects[0].table, "t1");
        assert_eq!(
            dynamic.objects[1],
            tipb::DynamicPartitionAccessObject::default()
        );
        assert_eq!(
            DynamicPartitionAccessObject::default().to_string(),
            "partition:dual"
        );

        let old = operator.access_objects.clone();
        AccessObject::DynamicPartitions(DynamicPartitionAccessObjects::default())
            .set_into_pb(&mut operator);
        assert_eq!(operator.access_objects, old);
    }
}
