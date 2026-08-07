/// Go `splitRangeInt64Max`.
#[must_use]
pub fn split_range_int64_max(count: i64) -> Vec<(String, String)> {
    assert!(count >= 0, "negative split range count");
    let mut ranges = Vec::with_capacity(count as usize);
    // Go reaches this division after successfully allocating a zero-length
    // slice, so count zero intentionally panics here.
    let batch = 9_999_999_999_999_999_999_u64 / count as u64;
    for index in 0..count as u64 {
        let start = batch * index;
        let end = batch * (index + 1);
        ranges.push((
            if index == 0 {
                "0".to_owned()
            } else {
                format!("{start:019}")
            },
            format!("{end:019}"),
        ));
    }
    ranges
}

/// Go `IterAllTables`: streams every table field over bounded database-key
/// ranges using between one and fifteen independent snapshots. The callback is
/// serialized exactly as Go's `mu.Lock` region, while decoding and scanning
/// remain concurrent.
pub fn iter_all_tables<S, C, F>(
    store: &S,
    start_ts: u64,
    concurrency: i32,
    cancelled: &C,
    visit: F,
) -> Result<()>
where
    S: MetaSnapshotStore,
    C: Fn() -> bool + Sync,
    F: FnMut(&TableInfo) -> Result<()> + Send,
{
    let concurrency = concurrency.clamp(1, 15);
    let ranges = split_range_int64_max(i64::from(concurrency));
    let callback = Mutex::new(visit);
    let stop = AtomicBool::new(false);
    let first_error = Mutex::new(None::<MetaError>);

    // Go creates and tags every snapshot in the parent goroutine before
    // starting the corresponding worker.
    let snapshots: Vec<_> = (0..concurrency)
        .map(|_| {
            let mut snapshot = store.snapshot(start_ts);
            snapshot.mark_internal_meta_request();
            snapshot
        })
        .collect();

    std::thread::scope(|scope| {
        for (mut snapshot, (range_start, range_end)) in snapshots.into_iter().zip(ranges) {
            let callback = &callback;
            let stop = &stop;
            let first_error = &first_error;
            scope.spawn(move || {
                let worker = catch_unwind(AssertUnwindSafe(|| {
                    let mut logical_start = b"DB:".to_vec();
                    tidb_codec::encode_bytes(&mut logical_start, range_start.as_bytes());
                    let mut logical_end = b"DB:".to_vec();
                    tidb_codec::encode_bytes(&mut logical_end, range_end.as_bytes());
                    let encoded_start = structure::encode_hash_data_key_prefix(&logical_start);
                    let encoded_end = structure::encode_hash_data_key_prefix(&logical_end);

                    snapshot.iterate_range(
                        &encoded_start,
                        &encoded_end,
                        &mut |encoded_key, encoded_value| {
                            // An error from another worker cancels this worker;
                            // the originating error remains the one returned.
                            if stop.load(Ordering::Acquire) {
                                return Ok(());
                            }
                            if cancelled() {
                                return Err(MetaError::Cancelled);
                            }
                            // Go deliberately skips malformed unrelated keys
                            // inside the bounded raw range.
                            let Ok((database_key, field)) =
                                structure::decode_hash_data_key(encoded_key)
                            else {
                                return Ok(());
                            };
                            if !field.starts_with(key::TABLE_PREFIX.as_bytes()) {
                                return Ok(());
                            }
                            let database_id = key::parse_db_key(&database_key)?;
                            let table = value::parse_table_info(encoded_value, database_id)?;
                            let mut callback = callback.lock().map_err(|_| {
                                MetaError::Storage("IterAllTables callback mutex poisoned".into())
                            })?;
                            callback(&table)
                        },
                    )
                }));

                let result = match worker {
                    Ok(result) => result,
                    Err(_) => Err(MetaError::Storage(
                        "panic recovered in IterAllTables worker".into(),
                    )),
                };
                if let Err(error) = result {
                    stop.store(true, Ordering::Release);
                    let mut first = first_error.lock().expect("error mutex is never exposed");
                    if first.is_none() {
                        *first = Some(error);
                    }
                }
            });
        }
    });

    first_error
        .into_inner()
        .expect("error mutex is never exposed")
        .map_or(Ok(()), Err)
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Go `isTableInfoMustLoad` with an explicit filter list.
#[must_use]
pub fn table_info_must_load_with_filters(
    mut json: &[u8],
    check_foreign_keys_in_order: bool,
    filters: &[MustLoadFilterAttr<'_>],
) -> bool {
    if check_foreign_keys_in_order {
        let foreign_key = find_bytes(json, FOREIGN_KEY_ATTRIBUTES_NIL)
            .or_else(|| find_bytes(json, FOREIGN_KEY_ATTRIBUTES_ZERO));
        let Some(index) = foreign_key else {
            return true;
        };
        json = &json[index..];
    }
    for filter in filters {
        let Some(index) = find_bytes(json, filter.attr) else {
            if filter.load_if_missing {
                return true;
            }
            continue;
        };
        if !filter.load_if_missing {
            return true;
        }
        json = &json[index..];
    }
    false
}

/// Go `IsTableInfoMustLoad`.
#[must_use]
pub fn table_info_must_load(json: &[u8]) -> bool {
    table_info_must_load_with_filters(json, true, TABLE_INFO_MUST_LOAD_FILTERS)
}

/// Go `Unescape`; replacements are deliberately ordered.
#[must_use]
pub fn unescape_name(value: &str) -> String {
    value.replace(r#"\""#, r#"""#).replace(r#"\\"#, r#"\"#)
}

/// Byte-string form of Go `Unescape`. Go strings can contain invalid UTF-8,
/// so the fast metadata path returns byte keys rather than silently rejecting
/// bytes that `regexp` and `strings.ReplaceAll` accept.
#[must_use]
pub fn unescape_name_bytes(value: &[u8]) -> Vec<u8> {
    fn replace_all(input: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
        let mut output = Vec::with_capacity(input.len());
        let mut rest = input;
        while let Some(index) = rest.windows(from.len()).position(|window| window == from) {
            output.extend_from_slice(&rest[..index]);
            output.extend_from_slice(to);
            rest = &rest[index + from.len()..];
        }
        output.extend_from_slice(rest);
        output
    }

    let quotes = replace_all(value, br#"\""#, br#"""#);
    replace_all(&quotes, br#"\\"#, br#"\"#)
}

/// Go `FastUnmarshalTableNameInfo` over the partial-JSON extractor.
pub fn fast_unmarshal_table_name_info(data: &[u8]) -> Result<TableNameInfo> {
    let members = extract_top_level_members(data, TABLE_NAME_INFO_FIELDS)
        .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
    let id = serde_json::from_str::<i64>(members["id"].get())
        .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
    struct SourceName(String);
    impl<'de> Deserialize<'de> for SourceName {
        fn deserialize<D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> std::result::Result<Self, D::Error> {
            struct SourceNameVisitor;
            impl<'de> serde::de::Visitor<'de> for SourceNameVisitor {
                type Value = SourceName;

                fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    formatter.write_str("a two-field CI string object")
                }

                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> std::result::Result<Self::Value, A::Error> {
                    let Some(_first_key) = map.next_key::<String>()? else {
                        return Err(serde::de::Error::custom("unexpected name field in JSON"));
                    };
                    // Go takes token 2, the first value, without checking the
                    // first key's spelling.
                    let first_value = map.next_value::<String>()?;
                    let Some(_second_key) = map.next_key::<String>()? else {
                        return Err(serde::de::Error::custom("unexpected name field in JSON"));
                    };
                    let second_value = map.next_value::<serde_json::Value>()?;
                    if second_value.is_array() || second_value.is_object() {
                        return Err(serde::de::Error::custom("unexpected name field in JSON"));
                    }
                    if map.next_key::<serde::de::IgnoredAny>()?.is_some() {
                        return Err(serde::de::Error::custom("unexpected name field in JSON"));
                    }
                    Ok(SourceName(first_value))
                }
            }
            deserializer.deserialize_map(SourceNameVisitor)
        }
    }
    let SourceName(original) = serde_json::from_str(members["name"].get())
        .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
    Ok(TableNameInfo {
        id,
        name: CiString::new(&original),
    })
}

/// Go `ExtractSchemaAndTableNameFromJob`.
pub fn extract_schema_and_table_name_from_job(data: &[u8]) -> Result<(String, String)> {
    let members = extract_top_level_members(data, JOB_EXTRACT_FIELDS)
        .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
    let schema = serde_json::from_str::<String>(members["schema_name"].get())
        .map_err(|_| MetaError::InvalidJson("unexpected name field in JSON".to_owned()))?;
    let table = serde_json::from_str::<String>(members["table_name"].get())
        .map_err(|_| MetaError::InvalidJson("unexpected name field in JSON".to_owned()))?;
    Ok((schema, table))
}

/// Go `IsJobMatch`, including the source expression's `&&`/`||` precedence.
pub fn job_matches(
    job: &[u8],
    schema_names: &BTreeSet<String>,
    table_names: &BTreeSet<String>,
) -> Result<bool> {
    if schema_names.is_empty() && table_names.is_empty() {
        return Ok(true);
    }
    let (schema_name, table_name) = extract_schema_and_table_name_from_job(job)?;
    Ok(
        ((schema_names.is_empty() || schema_names.contains(&schema_name))
            && table_names.is_empty())
            || table_names.contains(&table_name),
    )
}

/// Go `DefaultGroupMeta4Test`.
#[must_use]
pub fn default_resource_group_for_test() -> Arc<ResourceGroupInfo> {
    Arc::clone(DEFAULT_RESOURCE_GROUP.get_or_init(|| {
        Arc::new(ResourceGroupInfo {
            settings: Some(Box::new(ResourceGroupSettings {
                ru_rate: i32::MAX as u64,
                priority: MEDIUM_PRIORITY_VALUE,
                burst_limit: -1,
                ..ResourceGroupSettings::default()
            })),
            id: DEFAULT_RESOURCE_GROUP_ID,
            name: CiString::new("default"),
            state: SchemaState::PUBLIC,
        })
    }))
}

fn encode_resource_group(group: &ResourceGroupInfo) -> Result<Vec<u8>> {
    tidb_model::serde_helpers::to_go_json(group)
        .map_err(|error| MetaError::InvalidJson(error.to_string()))
}

fn decode_resource_group(encoded: &[u8]) -> Result<ResourceGroupInfo> {
    serde_json::from_slice(encoded).map_err(|error| MetaError::InvalidJson(error.to_string()))
}

/// Go `GetOldestSchemaVersion`.
pub fn oldest_schema_version(reader: &mut impl MvccReader) -> Result<i64> {
    let info = reader
        .mvcc_by_encoded_key(&key::schema_version_kv_key(), u64::MAX)?
        .ok_or(MetaError::NoSchemaVersionWrite)?;
    let write = info.writes.last().ok_or(MetaError::NoSchemaVersionWrite)?;
    value::parse_int_value(&write.short_value)
}

fn check_global_id(generated: i64) -> Result<()> {
    if generated > MAX_USER_GLOBAL_ID {
        return Err(MetaError::GlobalIdExceedsLimit {
            generated,
            limit: MAX_USER_GLOBAL_ID,
        });
    }
    Ok(())
}

fn go_fixed_two(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "+Inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_owned()
    } else {
        format!("{value:.2}")
    }
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn is_zero_f64(value: &f64) -> bool {
    *value == 0.0
}

fn go_zero_time() -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(
        NaiveDate::from_ymd_opt(1, 1, 1)
            .expect("Go zero date")
            .and_hms_nano_opt(0, 0, 0, 0)
            .expect("Go zero time"),
        Utc,
    )
}

fn serialize_go_time<S: serde::Serializer>(
    value: &DateTime<Utc>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

fn deserialize_go_time<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<DateTime<Utc>, D::Error> {
    let value = String::deserialize(deserializer)?;
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(serde::de::Error::custom)
}

#[cfg(test)]
#[path = "meta_go_lockdown.rs"]
mod meta_go_lockdown;
