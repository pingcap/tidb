// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `metering_sdk/writer/metering`.

use crate::{storage::ObjectStorageProvider, MeteringData};
use std::io::Write;
use std::sync::Arc;
use tidb_gocontext::Context;

/// Go `DefaultSharedPoolID`.
pub const DEFAULT_SHARED_POOL_ID: &str = "default-shared-pool";
/// Go SDK `config.Config` options consumed by the writer.
#[derive(Clone, Default)]
pub struct Config {
    pub overwrite_existing: bool,
    pub page_size_bytes: i64,
}
/// Go `MeteringWriter`.
pub struct MeteringWriter {
    provider: Arc<dyn ObjectStorageProvider>,
    config: Config,
    shared_pool_id: String,
}
#[derive(serde::Serialize)]
struct PageMeteringData<'a> {
    timestamp: i64,
    category: &'a str,
    self_id: &'a str,
    shared_pool_id: &'a str,
    part: usize,
    data: &'a [std::collections::BTreeMap<String, serde_json::Value>],
}
impl MeteringWriter {
    /// Go `NewMeteringWriterFromConfig`.
    pub fn new_from_config(
        provider: Arc<dyn ObjectStorageProvider>,
        config: Config,
        metering_config: &crate::config::MeteringConfig,
    ) -> Self {
        let pool = if metering_config.shared_pool_id.is_empty() {
            DEFAULT_SHARED_POOL_ID
        } else {
            &metering_config.shared_pool_id
        };
        Self::new_with_shared_pool(provider, config, pool)
    }
    /// Go `NewMeteringWriterWithSharedPool`.
    pub fn new_with_shared_pool(
        provider: Arc<dyn ObjectStorageProvider>,
        config: Config,
        pool: &str,
    ) -> Self {
        Self {
            provider,
            config,
            shared_pool_id: pool.into(),
        }
    }
    /// Go `MeteringWriter.Write`, including `ValidateSelfID` and `ValidateTimestamp`.
    pub fn write(&self, ctx: &Context, data: &mut MeteringData) -> Result<(), String> {
        if data.shared_pool_id.is_empty() {
            data.shared_pool_id = self.shared_pool_id.clone();
        }
        if data.shared_pool_id.is_empty() {
            return Err("SharedPoolID is required and cannot be empty".into());
        }
        if data.self_id.is_empty() {
            return Err("self ID cannot be empty".into());
        }
        if data.self_id.contains('-') {
            return Err(format!(
                "self_id cannot contain dash character: {}",
                data.self_id
            ));
        }
        if data.timestamp <= 0 {
            return Err("timestamp must be positive".into());
        }
        if data.timestamp % 60 != 0 {
            return Err("timestamp must be a minute-level timestamp (divisible by 60)".into());
        }
        if self.config.page_size_bytes <= 0 {
            return self.write_page_data(ctx, data, 0, &data.data);
        }
        let mut start = 0;
        let mut current_size = 0_i64;
        let mut part = 0;
        for (index, item) in data.data.iter().enumerate() {
            let size = tidb_stringutil::go_json_marshal(item).len() as i64;
            if index > start && current_size + size > self.config.page_size_bytes {
                self.write_page_data(ctx, data, part, &data.data[start..index])?;
                start = index;
                current_size = 0;
                part += 1;
            }
            current_size += size;
        }
        if start < data.data.len() {
            self.write_page_data(ctx, data, part, &data.data[start..])?;
        }
        Ok(())
    }
    /// Go `MeteringWriter.writePageData`.
    fn write_page_data(
        &self,
        ctx: &Context,
        data: &MeteringData,
        part: usize,
        items: &[std::collections::BTreeMap<String, serde_json::Value>],
    ) -> Result<(), String> {
        let path = format!(
            "metering/ru/{}/{}/{}/{}-{part}.json.gz",
            data.timestamp, data.category, data.shared_pool_id, data.self_id
        );
        if !self.config.overwrite_existing
            && self
                .provider
                .exists(ctx, &path)
                .map_err(|error| format!("failed to check if file exists: {error}"))?
        {
            return Err(format!("file already exists: {path}"));
        }
        let page = PageMeteringData {
            timestamp: data.timestamp,
            category: &data.category,
            self_id: &data.self_id,
            shared_pool_id: &data.shared_pool_id,
            part,
            data: items,
        };
        let json = tidb_stringutil::go_json_marshal(&page);
        // Go's gzip stream has a zero mtime. Each call owns its compressor so concurrent
        // writes cannot observe another write's buffer (Go protects reuse with a mutex).
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gzip.write_all(json.as_bytes())
            .map_err(|error| format!("failed to compress data: {error}"))?;
        let compressed = gzip
            .finish()
            .map_err(|error| format!("failed to compress data: {error}"))?;
        self.provider
            .upload(ctx, &path, &compressed)
            .map_err(|error| format!("failed to upload page data: {error}"))
    }
    /// Go `MeteringWriter.Close`; each native compressor has already been finalized.
    pub fn close(&self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    /// Go `NewMeteringWriter`: local provider test fixture.
    fn fixture() -> (
        tempfile::TempDir,
        Arc<dyn ObjectStorageProvider>,
        crate::config::MeteringConfig,
    ) {
        let root = tempfile::tempdir().unwrap();
        let config = crate::config::new_from_uri(&format!(
            "localfs://{}?prefix=capture&shared-pool-id=pool",
            root.path().display()
        ))
        .unwrap();
        let provider = crate::storage::new_object_storage_provider(&config).unwrap();
        (root, provider, config)
    }
    /// Go `MeteringWriter.Write`: its test metering envelope.
    fn data() -> MeteringData {
        MeteringData {
            self_id: "server_1".into(),
            timestamp: 120,
            category: "dxf".into(),
            shared_pool_id: String::new(),
            data: vec![std::collections::BTreeMap::from([
                ("row_count".into(), 12.into()),
                ("task_id".into(), 7.into()),
            ])],
        }
    }
    /// Go `MeteringWriter.Write`: decompress the stored test object.
    fn read(provider: &dyn ObjectStorageProvider, path: &str) -> String {
        let bytes = provider.download(&Context::background(), path).unwrap();
        let mut gz = flate2::read::GzDecoder::new(bytes.as_slice());
        let mut json = String::new();
        gz.read_to_string(&mut json).unwrap();
        json
    }
    /// Go `TestMeterSDKLocalReadback`, captured under `zzprobe/o15_meter_sdk`.
    #[test]
    fn test_meter_sdk_local_readback() {
        let (_root, provider, config) = fixture();
        let writer = MeteringWriter::new_from_config(provider.clone(), Config::default(), &config);
        let ctx = Context::background();
        let mut data = data();
        writer.write(&ctx, &mut data).unwrap();
        let path = "metering/ru/120/dxf/pool/server_1-0.json.gz";
        assert_eq!(
            read(provider.as_ref(), path),
            r#"{"timestamp":120,"category":"dxf","self_id":"server_1","shared_pool_id":"pool","part":0,"data":[{"row_count":12,"task_id":7}]}"#
        );
        assert_eq!(
            writer.write(&ctx, &mut data).unwrap_err(),
            format!("file already exists: {path}")
        );
        let overwrite = MeteringWriter::new_from_config(
            provider.clone(),
            Config {
                overwrite_existing: true,
                page_size_bytes: 0,
            },
            &config,
        );
        data.data[0].insert("row_count".into(), 13.into());
        overwrite.write(&ctx, &mut data).unwrap();
        assert!(read(provider.as_ref(), path).contains("\"row_count\":13"));
        writer.close().unwrap();
        writer.close().unwrap();
    }
    /// Go SDK `TestMeteringDataValidation` and `TestValidateSelfID`.
    #[test]
    fn test_metering_data_validation() {
        let (_root, provider, config) = fixture();
        let writer = MeteringWriter::new_from_config(provider, Config::default(), &config);
        let mut data = data();
        let ctx = Context::background();
        for timestamp in [0, -60] {
            data.timestamp = timestamp;
            assert_eq!(
                writer.write(&ctx, &mut data).unwrap_err(),
                "timestamp must be positive"
            );
        }
        data.timestamp = 121;
        assert_eq!(
            writer.write(&ctx, &mut data).unwrap_err(),
            "timestamp must be a minute-level timestamp (divisible by 60)"
        );
        data.timestamp = 120;
        data.self_id = "bad-id".into();
        assert_eq!(
            writer.write(&ctx, &mut data).unwrap_err(),
            "self_id cannot contain dash character: bad-id"
        );
        data.self_id.clear();
        assert_eq!(
            writer.write(&ctx, &mut data).unwrap_err(),
            "self ID cannot be empty"
        );
    }
    /// Go SDK `TestMeteringWriterPagination`.
    #[test]
    fn test_metering_writer_pagination() {
        let (_root, provider, config) = fixture();
        let writer = MeteringWriter::new_from_config(
            provider.clone(),
            Config {
                page_size_bytes: 1,
                overwrite_existing: false,
            },
            &config,
        );
        let ctx = Context::background();
        let mut data = data();
        data.data.extend(data.data.clone());
        writer.write(&ctx, &mut data).unwrap();
        assert_eq!(provider.list(&ctx, "metering/").unwrap().len(), 2);
        for part in 0..2 {
            let value: serde_json::Value = serde_json::from_str(&read(
                provider.as_ref(),
                &format!("metering/ru/120/dxf/pool/server_1-{part}.json.gz"),
            ))
            .unwrap();
            assert_eq!(value["part"], part);
            assert_eq!(value["data"].as_array().unwrap().len(), 1);
        }
        data.self_id = "empty".into();
        data.data.clear();
        writer.write(&ctx, &mut data).unwrap();
        assert_eq!(provider.list(&ctx, "metering/").unwrap().len(), 2);
    }
    /// Go SDK `TestMeteringWriterConcurrency`.
    #[test]
    fn test_metering_writer_concurrency() {
        let (_root, provider, config) = fixture();
        let writer = Arc::new(MeteringWriter::new_from_config(
            provider.clone(),
            Config::default(),
            &config,
        ));
        std::thread::scope(|scope| {
            for i in 0..10 {
                let writer = writer.clone();
                scope.spawn(move || {
                    for j in 0..5 {
                        let mut data = data();
                        data.self_id = format!("server{i}{j}");
                        writer.write(&Context::background(), &mut data).unwrap();
                    }
                });
            }
        });
        assert_eq!(
            provider
                .list(&Context::background(), "metering/")
                .unwrap()
                .len(),
            50
        );
    }
    /// Go SDK `TestSharedPoolIDRequired` and `TestDefaultSharedPoolID`.
    #[test]
    fn test_shared_pool_id_required() {
        let (_root, provider, mut config) = fixture();
        let mut data = data();
        let writer = MeteringWriter::new_with_shared_pool(provider.clone(), Config::default(), "");
        assert_eq!(
            writer.write(&Context::background(), &mut data).unwrap_err(),
            "SharedPoolID is required and cannot be empty"
        );
        config.shared_pool_id.clear();
        let writer = MeteringWriter::new_from_config(provider, Config::default(), &config);
        writer.write(&Context::background(), &mut data).unwrap();
        assert_eq!(data.shared_pool_id, DEFAULT_SHARED_POOL_ID);
    }
}
