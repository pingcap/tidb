// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `br/pkg/metautil/metafile.go`: physical backup metadata writing.
use crate::local_storage::LocalStorage;
use prost::Message;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use tidb_gocontext::Context;
use tidb_proto::backup::{BackupMeta, CipherInfo, File, MetaFile, Schema};

/// Go `backuppb.BackupSchemaVersion`, from the pinned kvproto module.
pub const BACKUP_SCHEMA_VERSION: u32 = 1;
/// Go `MetaFileSize`.
pub const META_FILE_SIZE: usize = 128 * 1024 * 1024;
/// Go `MaxBatchSize`.
const MAX_BATCH_SIZE: usize = 1024;

/// Go `AppendOp`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendOp {
    /// Go `AppendMetaFile`.
    MetaFile,
    /// Go `AppendDataFile`.
    DataFile,
    /// Go `AppendSchema`.
    Schema,
    /// Go `AppendDDL`.
    Ddl,
}
impl AppendOp {
    /// Go `AppendOp.name`.
    fn name(self) -> &'static str {
        match self {
            Self::MetaFile => "metafile",
            Self::DataFile => "datafile",
            Self::Schema => "schema",
            Self::Ddl => "ddl",
        }
    }
}
/// Go `MetaWriter.Send`'s typed payloads; data files stay in adjacent batches.
pub enum MetaItem {
    /// Go `AppendMetaFile` payload.
    MetaFile(File),
    /// Go `AppendDataFile` payload.
    DataFile(Vec<File>),
    /// Go `AppendSchema` payload.
    Schema(Schema),
    /// Go `AppendDDL` payload.
    Ddl(Vec<u8>),
}
impl MetaItem {
    /// Go `AppendOp.appendFile`'s payload type assertion.
    fn op(&self) -> AppendOp {
        match self {
            Self::MetaFile(_) => AppendOp::MetaFile,
            Self::DataFile(_) => AppendOp::DataFile,
            Self::Schema(_) => AppendOp::Schema,
            Self::Ddl(_) => AppendOp::Ddl,
        }
    }
}

/// Go `sizedMetaFile`.
pub struct SizedMetaFile {
    root: MetaFile,
    data_file_size: u64,
    size: usize,
    item_num: usize,
    size_limit: usize,
}
impl SizedMetaFile {
    /// Go `NewSizedMetaFile`.
    pub fn new(size_limit: usize) -> Self {
        Self {
            root: MetaFile::default(),
            data_file_size: 0,
            size: 0,
            item_num: 0,
            size_limit,
        }
    }
    /// Go `sizedMetaFile.append` and `AppendOp.appendFile`. Size excludes
    /// enclosing protobuf tags, and the limit is strictly greater-than.
    pub fn append(&mut self, item: MetaItem) -> bool {
        match item {
            MetaItem::MetaFile(file) => {
                self.size += file.encoded_len();
                self.item_num += 1;
                self.root.meta_files.push(file);
            }
            MetaItem::DataFile(files) => {
                for file in &files {
                    self.size += file.encoded_len();
                    self.data_file_size = self.data_file_size.wrapping_add(file.size);
                }
                self.item_num += files.len();
                self.root.data_files.extend(files);
            }
            MetaItem::Schema(schema) => {
                self.size += schema.encoded_len();
                self.item_num += 1;
                self.root.schemas.push(schema);
            }
            MetaItem::Ddl(ddl) => {
                self.size += ddl.len();
                self.item_num += 1;
                self.root.ddls.push(ddl);
            }
        }
        self.size > self.size_limit
    }
}

/// Go `Encrypt`. Indexed metafiles store IVs in their File records; the
/// top-level backupmeta prepends the IV to its encrypted bytes.
pub fn encrypt(content: &[u8], cipher: Option<&CipherInfo>) -> Result<(Vec<u8>, Vec<u8>), String> {
    use tidb_proto::encryptionpb::EncryptionMethod;
    let Some(cipher) = cipher.filter(|_| !content.is_empty()) else {
        return Ok((content.to_vec(), Vec::new()));
    };
    match EncryptionMethod::try_from(cipher.cipher_type) {
        Ok(EncryptionMethod::Plaintext) => Ok((content.to_vec(), Vec::new())),
        Ok(
            EncryptionMethod::Aes128Ctr | EncryptionMethod::Aes192Ctr | EncryptionMethod::Aes256Ctr,
        ) => {
            let mut iv = vec![0; 16];
            getrandom::fill(&mut iv).map_err(|e| e.to_string())?;
            let data = tidb_encrypt::aes_encrypt_with_ctr(content, &cipher.cipher_key, &iv)
                .map_err(|e| e.to_string())?;
            Ok((data, iv))
        }
        _ => Err("cipher type invalid [BR:Common:ErrInvalidArgument]invalid argument".into()),
    }
}

/// Go `MetaWriter`'s mutable state, shared with its single writing worker.
struct WriterState {
    storage: LocalStorage,
    use_v2_meta: bool,
    backup_meta: BackupMeta,
    metafiles: SizedMetaFile,
    sequence: u64,
    flushed_item_num: usize,
    meta_file_name: String,
    cipher: Option<CipherInfo>,
    total_data_file_size: u64,
    total_meta_file_size: u64,
    error: Option<String>,
}
impl WriterState {
    /// Go `MetaWriter.fillMetasV1`.
    fn fill_metas_v1(&mut self, op: AppendOp) {
        match op {
            AppendOp::DataFile => self
                .backup_meta
                .files
                .clone_from(&self.metafiles.root.data_files),
            AppendOp::Schema => {
                self.backup_meta
                    .schemas
                    .clone_from(&self.metafiles.root.schemas);
                self.add_stats_size();
            }
            AppendOp::Ddl => self.backup_meta.ddls = merge_ddls(&self.metafiles.root.ddls),
            AppendOp::MetaFile => panic!("unsupported op type"),
        }
        // Go V1 deliberately retains the buffer between operation phases.
        self.flushed_item_num += self.metafiles.item_num;
    }
    /// Go `MetaWriter.fillMetasV1` / `flushMetasV2`'s statistics accounting.
    fn add_stats_size(&mut self) {
        for schema in &self.metafiles.root.schemas {
            for index in &schema.stats_index {
                self.total_meta_file_size = self.total_meta_file_size.wrapping_add(index.size_enc);
            }
        }
    }
    /// Go `MetaWriter.flushMetasV2`.
    fn flush_metas_v2(&mut self, op: AppendOp) -> Result<(), String> {
        let empty = match op {
            AppendOp::Schema => self.metafiles.root.schemas.is_empty(),
            AppendOp::DataFile => self.metafiles.root.data_files.is_empty(),
            AppendOp::Ddl => self.metafiles.root.ddls.is_empty(),
            AppendOp::MetaFile => panic!("unsupported op type"),
        };
        if empty {
            return Ok(());
        }
        if op == AppendOp::Schema {
            self.add_stats_size();
        }
        let content = self.metafiles.root.encode_to_vec();
        self.total_data_file_size = self
            .total_data_file_size
            .wrapping_add(self.metafiles.data_file_size);
        self.sequence += 1;
        let name = format!("backupmeta.{}.{:09}", op.name(), self.sequence);
        let (encrypted, iv) = encrypt(&content, self.cipher.as_ref())?;
        self.total_meta_file_size = self
            .total_meta_file_size
            .wrapping_add(encrypted.len() as u64);
        self.storage.write_file(&name, &encrypted)?;
        let file = File {
            name,
            sha256: Sha256::digest(&content).to_vec(),
            size: content.len() as u64,
            cipher_iv: iv,
            ..Default::default()
        };
        let index = match op {
            AppendOp::Schema => &mut self.backup_meta.schema_index,
            AppendOp::DataFile => &mut self.backup_meta.file_index,
            AppendOp::Ddl => &mut self.backup_meta.ddl_indexes,
            AppendOp::MetaFile => unreachable!(),
        };
        index.get_or_insert_default().meta_files.push(file);
        self.flushed_item_num += self.metafiles.item_num;
        self.metafiles = SizedMetaFile::new(self.metafiles.size_limit);
        Ok(())
    }
    /// Go `MetaWriter.ArchiveSize`.
    fn archive_size(&self) -> u64 {
        self.backup_meta
            .files
            .iter()
            .fold(self.total_data_file_size, |sum, f| sum.wrapping_add(f.size))
    }
}

/// Go `MetaWriter`: one bounded worker per operation phase.
pub struct MetaWriter {
    state: Arc<Mutex<WriterState>>,
    sender: Option<mpsc::SyncSender<MetaItem>>,
    worker: Option<JoinHandle<()>>,
}
impl MetaWriter {
    /// Go `NewMetaWriter`.
    pub fn new(
        storage: LocalStorage,
        size_limit: usize,
        use_v2: bool,
        name: &str,
        cipher: Option<CipherInfo>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(WriterState {
                storage,
                use_v2_meta: use_v2,
                backup_meta: BackupMeta {
                    ddls: b"[]".to_vec(),
                    backup_schema_version: BACKUP_SCHEMA_VERSION,
                    ..Default::default()
                },
                metafiles: SizedMetaFile::new(size_limit),
                sequence: 0,
                flushed_item_num: 0,
                meta_file_name: if name.is_empty() {
                    "backupmeta".into()
                } else {
                    name.into()
                },
                cipher,
                total_data_file_size: 0,
                total_meta_file_size: 0,
                error: None,
            })),
            sender: None,
            worker: None,
        }
    }
    /// Go `MetaWriter.Update`.
    pub fn update(&self, f: impl FnOnce(&mut BackupMeta)) {
        f(&mut self.state.lock().unwrap().backup_meta);
    }
    /// Go `MetaWriter.StartWriteMetasAsync`.
    pub fn start_write_metas_async(&mut self, ctx: &Context, op: AppendOp) {
        assert!(
            self.worker.is_none(),
            "finish the previous metadata phase first"
        );
        let (sender, receiver) = mpsc::sync_channel::<MetaItem>(MAX_BATCH_SIZE);
        self.sender = Some(sender);
        let state = self.state.clone();
        let ctx = ctx.clone();
        {
            let mut state = state.lock().unwrap();
            state.error = None;
            state.flushed_item_num = 0;
        }
        self.worker = Some(std::thread::spawn(move || {
            loop {
                if ctx.err().is_some() {
                    break;
                }
                let item = match receiver.recv_timeout(std::time::Duration::from_millis(10)) {
                    Ok(item) => item,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                };
                assert_eq!(item.op(), op);
                let mut state = state.lock().unwrap();
                let flush = state.metafiles.append(item);
                if state.use_v2_meta && flush {
                    if let Err(error) = state.flush_metas_v2(op) {
                        state.error = Some(error);
                        break;
                    }
                }
            }
        }));
    }
    /// Go `MetaWriter.Send`.
    pub fn send(&self, item: MetaItem) -> Result<(), String> {
        self.sender
            .as_ref()
            .expect("start metadata phase first")
            .send(item)
            .map_err(|_| {
                self.state
                    .lock()
                    .unwrap()
                    .error
                    .clone()
                    .unwrap_or_else(|| "metadata writer closed".into())
            })
    }
    /// Go `MetaWriter.FinishWriteMetas`.
    pub fn finish_write_metas(&mut self, _ctx: &Context, op: AppendOp) -> Result<(), String> {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            worker.join().expect("metadata writer panicked");
        }
        let mut state = self.state.lock().unwrap();
        if let Some(error) = &state.error {
            return Err(error.clone());
        }
        if state.use_v2_meta {
            state.flush_metas_v2(op)
        } else {
            state.fill_metas_v1(op);
            Ok(())
        }
    }
    /// Go `MetaWriter.FlushBackupMeta`.
    pub fn flush_backup_meta(&self, _ctx: &Context) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        state.backup_meta.version = i32::from(state.use_v2_meta);
        state.backup_meta.backup_schema_version = state
            .backup_meta
            .backup_schema_version
            .max(BACKUP_SCHEMA_VERSION);
        state.backup_meta.backup_size = state
            .total_meta_file_size
            .wrapping_add(state.archive_size())
            .wrapping_add(state.backup_meta.encoded_len() as u64);
        let (encrypted, mut iv) =
            encrypt(&state.backup_meta.encode_to_vec(), state.cipher.as_ref())?;
        iv.extend_from_slice(&encrypted);
        state.storage.write_file(&state.meta_file_name, &iv)
    }
    /// Go `MetaWriter.ArchiveSize`.
    pub fn archive_size(&self) -> u64 {
        self.state.lock().unwrap().archive_size()
    }
    /// Go `MetaWriter.MetaFilesSize`.
    pub fn meta_files_size(&self) -> u64 {
        self.state.lock().unwrap().total_meta_file_size
    }
    /// Go `MetaWriter.Backupmeta`.
    pub fn backupmeta(&self) -> BackupMeta {
        self.state.lock().unwrap().backup_meta.clone()
    }
}
impl Drop for MetaWriter {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
/// Go `mergeDDLs`.
pub(super) fn merge_ddls(ddls: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = vec![b'['];
    for (i, ddl) in ddls.iter().enumerate() {
        if i != 0 {
            bytes.push(b',');
        }
        bytes.extend(ddl);
    }
    bytes.push(b']');
    bytes
}

#[cfg(test)]
mod tests;
