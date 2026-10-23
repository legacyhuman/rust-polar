//! Small, single-process redb store. Database writes are deliberately
//! synchronous; callers should run them on the dedicated writer thread planned
//! by the service layer rather than on an async executor.

use std::fs;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};

use crate::model::{Measurement, MeasurementContext};
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};

pub const DEFAULT_CACHE_BYTES: usize = 8 * 1024 * 1024;
pub const MIN_CACHE_BYTES: usize = 1024 * 1024;
pub const MAX_CACHE_BYTES: usize = 256 * 1024 * 1024;
const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_RECORD_BATCH_ROWS: usize = 1000;
const MAX_RECORD_BATCH_BYTES: usize = 512 * 1024;
const MAX_MEASUREMENT_BATCH_ROWS: usize = 1000;
const MAX_MEASUREMENT_BATCH_BYTES: usize = 512 * 1024;
const MAX_MEASUREMENT_PAGE_ROWS: usize = 1000;
const MAX_MEASUREMENT_PAGE_BYTES: usize = 1024 * 1024;
const SCHEMA_VERSION: &str = "3";
const META: TableDefinition<&str, &str> = TableDefinition::new("meta");
const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("users");
const SOURCES: TableDefinition<&str, &[u8]> = TableDefinition::new("sources");
const METRICS: TableDefinition<&str, &[u8]> = TableDefinition::new("metrics");
const ARCHIVES: TableDefinition<&str, &[u8]> = TableDefinition::new("archives");
const ARCHIVE_STATUS: TableDefinition<&str, &str> = TableDefinition::new("archive_status");
const RECORDS: TableDefinition<&str, &[u8]> = TableDefinition::new("records");
const SERIES: TableDefinition<&str, &[u8]> = TableDefinition::new("series");
const EXTERNAL_INDEX: TableDefinition<&str, &[u8]> = TableDefinition::new("external_index");
const DAILY: TableDefinition<&str, &[u8]> = TableDefinition::new("daily");
const SYNC_STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("sync_state");
const PARENT_ACTIVE_VERSION: TableDefinition<&str, &[u8]> =
    TableDefinition::new("parent_active_version");
const PARENT_STAGING_VERSION: TableDefinition<&str, &[u8]> =
    TableDefinition::new("parent_staging_version");
const PARENT_NEXT_VERSION: TableDefinition<&str, &[u8]> =
    TableDefinition::new("parent_next_version");

fn validate_cache_bytes(cache_bytes: usize) -> Result<(), StorageError> {
    if (MIN_CACHE_BYTES..=MAX_CACHE_BYTES).contains(&cache_bytes) {
        Ok(())
    } else {
        Err(StorageError::CacheBytesOutOfRange {
            value: cache_bytes,
            minimum: MIN_CACHE_BYTES,
            maximum: MAX_CACHE_BYTES,
        })
    }
}

fn configured_cache_bytes(
    value: Result<String, std::env::VarError>,
) -> Result<usize, StorageError> {
    match value {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|_| StorageError::InvalidCacheBytes(value)),
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_CACHE_BYTES),
        Err(std::env::VarError::NotUnicode(value)) => Err(StorageError::InvalidCacheBytes(
            value.to_string_lossy().into_owned(),
        )),
    }
}

/// Durable handle for a parent replacement being prepared in bounded writes.
/// A handle can be reconstructed after restart by calling
/// [`Store::begin_parent_measurement_replacement`] again with the same parent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParentMeasurementReplacement {
    parent_id: String,
    generation: u64,
}

/// Open the local store, creating the initial versioned schema if needed.
pub struct Store {
    db: Database,
    database_path: PathBuf,
    archive_path: PathBuf,
}

/// Inclusive/exclusive UTC time bounds and a keyset cursor for a series query.
/// `context = None` scans all contexts for the selected user/source/metric;
/// specifying a context also lets redb seek directly into the time range.
pub struct MeasurementQuery<'a> {
    pub user_id: &'a str,
    pub source: &'a str,
    pub metric: &'a str,
    pub context: Option<&'a MeasurementContext>,
    pub from_utc_micros: i64,
    pub to_utc_micros: i64,
    pub limit: usize,
    /// Opaque key returned by the previous page. It is valid only for the same
    /// query dimensions and range.
    pub after: Option<&'a str>,
}

/// One bounded page of measurements. `next_cursor` is the last returned key;
/// each page has its own redb snapshot, so concurrent updates may affect later
/// pages.
#[derive(serde::Serialize)]
pub struct MeasurementPage {
    pub rows: Vec<Measurement>,
    pub next_cursor: Option<String>,
}

impl Store {
    /// Open the store using `RUST_POLAR_DB_CACHE_BYTES` when set, otherwise
    /// use the conservative 8 MiB default. The environment setting is read at
    /// open time so CLI commands and the resident service use the same config.
    pub fn open(data_dir: &Path) -> Result<Self, StorageError> {
        let cache_bytes = configured_cache_bytes(std::env::var("RUST_POLAR_DB_CACHE_BYTES"))?;
        Self::open_with_cache_bytes(data_dir, cache_bytes)
    }

    /// Open the store with an explicit redb page cache size.
    pub fn open_with_cache_bytes(
        data_dir: &Path,
        cache_bytes: usize,
    ) -> Result<Self, StorageError> {
        validate_cache_bytes(cache_bytes)?;
        fs::create_dir_all(data_dir)?;
        restrict_directory(data_dir)?;
        let parent = data_dir
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_directory(parent)?;
        let database_path = data_dir.join("health.redb");
        let archive_path = data_dir.join("archive");
        fs::create_dir_all(&archive_path)?;
        sync_directory(data_dir)?;
        let db = Database::builder()
            .set_cache_size(cache_bytes)
            .create(&database_path)?;
        restrict_file(&database_path)?;
        sync_directory(data_dir)?;

        let store = Self {
            db,
            database_path,
            archive_path,
        };
        store.initialize_schema()?;
        Ok(store)
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }
    pub fn archive_path(&self) -> &Path {
        &self.archive_path
    }

    /// Create a consistent, redb-snapshot backup plus its immutable archive
    /// files. The destination must not exist and must be outside this data
    /// directory. Secrets in `credentials/` are intentionally excluded.
    pub fn backup_to(&self, destination: &Path) -> Result<(), StorageError> {
        if destination.exists() {
            return Err(StorageError::InvalidBackupDestination);
        }
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let parent = fs::canonicalize(parent)?;
        let name = destination
            .file_name()
            .ok_or(StorageError::InvalidBackupDestination)?;
        let destination = parent.join(name);
        let source_root = fs::canonicalize(
            self.database_path
                .parent()
                .ok_or(StorageError::InvalidBackupDestination)?,
        )?;
        if destination.starts_with(&source_root) || source_root.starts_with(&destination) {
            return Err(StorageError::InvalidBackupDestination);
        }
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let staging = parent.join(format!(
            ".{}.backup-{}-{nonce}.tmp",
            name.to_string_lossy(),
            std::process::id()
        ));
        fs::create_dir(&staging)?;
        restrict_directory(&staging)?;
        let result = self.write_backup_contents(&staging);
        if let Err(error) = result {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }
        fs::rename(&staging, &destination)?;
        sync_directory(&parent)?;
        Ok(())
    }

    fn write_backup_contents(&self, destination: &Path) -> Result<(), StorageError> {
        let archive_destination = destination.join("archive");
        fs::create_dir(&archive_destination)?;
        restrict_directory(&archive_destination)?;

        let target = Store::open(destination)?;
        let snapshot = self.db.begin_read()?;
        copy_string_table(&snapshot, &target.db, META)?;
        copy_string_table(&snapshot, &target.db, ARCHIVE_STATUS)?;
        copy_bytes_table(&snapshot, &target.db, USERS)?;
        copy_bytes_table(&snapshot, &target.db, SOURCES)?;
        copy_bytes_table(&snapshot, &target.db, METRICS)?;
        copy_bytes_table(&snapshot, &target.db, ARCHIVES)?;
        copy_bytes_table(&snapshot, &target.db, RECORDS)?;
        copy_bytes_table(&snapshot, &target.db, SERIES)?;
        copy_bytes_table(&snapshot, &target.db, EXTERNAL_INDEX)?;
        copy_bytes_table(&snapshot, &target.db, DAILY)?;
        copy_bytes_table(&snapshot, &target.db, SYNC_STATE)?;
        copy_bytes_table(&snapshot, &target.db, PARENT_ACTIVE_VERSION)?;
        copy_bytes_table(&snapshot, &target.db, PARENT_STAGING_VERSION)?;
        copy_bytes_table(&snapshot, &target.db, PARENT_NEXT_VERSION)?;
        let schema_version = target.schema_version()?;
        drop(snapshot);
        drop(target);

        copy_archive_tree(&self.archive_path, &archive_destination)?;
        let manifest = BackupManifest {
            format_version: 1,
            schema_version,
            created_at: time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| "unknown".to_owned()),
            includes_credentials: false,
        };
        let manifest_path = destination.join("backup.json");
        let mut manifest_file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&manifest_path)?;
        serde_json::to_writer(&mut manifest_file, &manifest)?;
        use std::io::Write as _;
        manifest_file.write_all(b"\n")?;
        manifest_file.sync_all()?;
        restrict_file(&manifest_path)?;
        sync_tree_directories(destination)?;
        Ok(())
    }

    /// Restore a backup into a new data directory. This verifies its manifest,
    /// opens the source database, verifies every archived response, then makes
    /// a new consistent snapshot at the destination.
    pub fn restore_backup(backup: &Path, destination: &Path) -> Result<(), StorageError> {
        let manifest: BackupManifest =
            serde_json::from_slice(&fs::read(backup.join("backup.json"))?)?;
        if manifest.format_version != 1 || manifest.schema_version != SCHEMA_VERSION {
            return Err(StorageError::UnsupportedSchema(manifest.schema_version));
        }
        let source = Store::open(backup)?;
        let verification = crate::archive::verify_all(source.archive_path())
            .map_err(|error| StorageError::Io(std::io::Error::other(error)))?;
        if verification.failed != 0 {
            return Err(StorageError::InvalidBackupArchive(verification.failed));
        }
        source.backup_to(destination)
    }

    fn initialize_schema(&self) -> Result<(), StorageError> {
        let mut write = self.db.begin_write()?;
        write
            .set_durability(Durability::Immediate)
            .map_err(StorageError::SetDurability)?;
        {
            let mut meta = write.open_table(META)?;
            let version = meta
                .get("schema_version")?
                .map(|value| value.value().to_owned());
            match version {
                Some(version) if version == "1" || version == "2" => {
                    // Version 2 added per-receipt normalization status.
                    // Version 3 adds parent-version pointers; existing rows
                    // remain the implicit legacy generation until a parent is
                    // explicitly replaced through the staged API.
                    meta.insert("schema_version", SCHEMA_VERSION)?;
                }
                Some(version) if version != SCHEMA_VERSION => {
                    return Err(StorageError::UnsupportedSchema(version));
                }
                Some(_) => {}
                None => {
                    meta.insert("schema_version", SCHEMA_VERSION)?;
                }
            }
            drop(meta);
            let _ = write.open_table(USERS)?;
            let _ = write.open_table(SOURCES)?;
            let _ = write.open_table(METRICS)?;
            let _ = write.open_table(ARCHIVES)?;
            let _ = write.open_table(ARCHIVE_STATUS)?;
            let _ = write.open_table(RECORDS)?;
            let _ = write.open_table(SERIES)?;
            let _ = write.open_table(EXTERNAL_INDEX)?;
            let _ = write.open_table(DAILY)?;
            let _ = write.open_table(SYNC_STATE)?;
            let _ = write.open_table(PARENT_ACTIVE_VERSION)?;
            let _ = write.open_table(PARENT_STAGING_VERSION)?;
            let _ = write.open_table(PARENT_NEXT_VERSION)?;
        }
        write.commit()?;
        Ok(())
    }

    /// Store a serialized archive receipt as pending normalization. Repeating
    /// the same receipt id is idempotent when the bytes match.
    pub fn register_archive<T: serde::Serialize>(
        &self,
        receipt_id: &str,
        value: &T,
    ) -> Result<(), StorageError> {
        let bytes = serde_json::to_vec(value)?;
        let mut write = self.db.begin_write()?;
        write
            .set_durability(Durability::Immediate)
            .map_err(StorageError::SetDurability)?;
        {
            let mut table = write.open_table(ARCHIVES)?;
            if let Some(existing) = table.get(receipt_id)? {
                if existing.value() != bytes.as_slice() {
                    return Err(StorageError::ArchiveReceiptConflict(receipt_id.to_owned()));
                }
            } else {
                table.insert(receipt_id, bytes.as_slice())?;
            }
        }
        {
            let mut status = write.open_table(ARCHIVE_STATUS)?;
            if status.get(receipt_id)?.is_none() {
                status.insert(receipt_id, "pending")?;
            }
        }
        write.commit()?;
        Ok(())
    }

    /// Record durable archive processing state independently from the source
    /// receipt. `pending` receipts are safe to replay after interruption.
    pub fn set_archive_state(&self, receipt_id: &str, state: &str) -> Result<(), StorageError> {
        if receipt_id.is_empty() || receipt_id.len() > 256 || state.len() > 64 {
            return Err(StorageError::InvalidSyncStateKey);
        }
        let mut write = self.db.begin_write()?;
        write
            .set_durability(Durability::Immediate)
            .map_err(StorageError::SetDurability)?;
        {
            let mut table = write.open_table(ARCHIVE_STATUS)?;
            table.insert(receipt_id, state)?;
        }
        write.commit()?;
        Ok(())
    }

    /// Return a bounded list of archive receipts awaiting normalization.
    pub fn pending_archive_ids(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        if limit == 0 || limit > MAX_MEASUREMENT_PAGE_ROWS {
            return Err(StorageError::InvalidMeasurementPageLimit);
        }
        let read = self.db.begin_read()?;
        let archives = read.open_table(ARCHIVES)?;
        let statuses = read.open_table(ARCHIVE_STATUS)?;
        let mut result = Vec::new();
        for entry in archives.iter()? {
            let (key, _) = entry?;
            let id = key.value();
            let state = statuses.get(id)?.map(|value| value.value().to_owned());
            if state.as_deref().unwrap_or("pending") == "pending" {
                result.push(id.to_owned());
                if result.len() == limit {
                    break;
                }
            }
        }
        Ok(result)
    }

    pub fn archive_count(&self) -> Result<u64, StorageError> {
        let read = self.db.begin_read()?;
        let table = read.open_table(ARCHIVES)?;
        Ok(table.len()?)
    }

    pub fn record_count(&self) -> Result<u64, StorageError> {
        let read = self.db.begin_read()?;
        let table = read.open_table(RECORDS)?;
        Ok(table.len()?)
    }

    pub fn measurement_count(&self) -> Result<u64, StorageError> {
        let read = self.db.begin_read()?;
        let table = read.open_table(SERIES)?;
        Ok(table.len()?)
    }

    /// Store a bounded JSON value under a deterministic sync-state key.
    /// Replacements are durable before this method returns.
    pub fn put_sync_state<T: serde::Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError> {
        if key.is_empty() || key.len() > 512 {
            return Err(StorageError::InvalidSyncStateKey);
        }
        let bytes = serde_json::to_vec(value)?;
        if bytes.len() > 64 * 1024 {
            return Err(StorageError::SyncStateTooLarge(bytes.len()));
        }
        let mut write = self.db.begin_write()?;
        write
            .set_durability(Durability::Immediate)
            .map_err(StorageError::SetDurability)?;
        {
            let mut table = write.open_table(SYNC_STATE)?;
            table.insert(key, bytes.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    pub fn get_sync_state(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let read = self.db.begin_read()?;
        let table = read.open_table(SYNC_STATE)?;
        Ok(table.get(key)?.map(|value| value.value().to_vec()))
    }

    /// Return a bounded, key-ordered page of sync state for local status APIs.
    pub fn sync_state_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        if limit == 0 || limit > MAX_MEASUREMENT_PAGE_ROWS {
            return Err(StorageError::InvalidMeasurementPageLimit);
        }
        let read = self.db.begin_read()?;
        let table = read.open_table(SYNC_STATE)?;
        let entries = match after {
            Some(cursor) => table.range::<&str>((Excluded(cursor), Unbounded))?,
            None => table.range::<&str>((Unbounded::<&str>, Unbounded::<&str>))?,
        };
        let mut result = Vec::with_capacity(limit.min(128));
        for entry in entries {
            let (key, value) = entry?;
            let value = value.value();
            if value.len() > 64 * 1024 {
                return Err(StorageError::SyncStateTooLarge(value.len()));
            }
            result.push((key.value().to_owned(), value.to_vec()));
            if result.len() == limit {
                break;
            }
        }
        Ok(result)
    }

    /// Insert or replace one idempotently keyed record payload. The payload is
    /// one bounded provider record, never a complete API response body.
    pub fn put_record(&self, key: &str, payload: &[u8]) -> Result<(), StorageError> {
        self.put_records(&[(key, payload)])
    }

    /// Atomically insert or replace a bounded set of provider records. Batch
    /// size is capped at 1000 rows or 512 KiB, whichever comes first. A single
    /// record may use the full 4 MiB per-record allowance.
    pub fn put_records(&self, records: &[(&str, &[u8])]) -> Result<(), StorageError> {
        if records.len() > MAX_RECORD_BATCH_ROWS {
            return Err(StorageError::RecordBatchTooLarge);
        }
        let mut encoded_bytes = 0usize;
        for (key, payload) in records {
            if payload.len() > MAX_RECORD_BYTES {
                return Err(StorageError::RecordTooLarge(payload.len()));
            }
            encoded_bytes = encoded_bytes
                .saturating_add(key.len())
                .saturating_add(payload.len());
        }
        if records.len() > 1 && encoded_bytes > MAX_RECORD_BATCH_BYTES {
            return Err(StorageError::RecordBatchTooLarge);
        }
        let mut write = self.db.begin_write()?;
        write
            .set_durability(Durability::Immediate)
            .map_err(StorageError::SetDurability)?;
        {
            let mut table = write.open_table(RECORDS)?;
            for (key, payload) in records {
                table.insert(*key, *payload)?;
            }
        }
        write.commit()?;
        Ok(())
    }

    pub fn get_record(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let read = self.db.begin_read()?;
        let table = read.open_table(RECORDS)?;
        Ok(table.get(key)?.map(|value| value.value().to_vec()))
    }

    /// Atomically insert or replace a bounded batch of normalized measurements.
    /// Stable measurement ids are indexed separately so replay is idempotent,
    /// including when a correction moves a measurement to another timestamp.
    pub fn put_measurements(&self, measurements: &[Measurement]) -> Result<(), StorageError> {
        self.put_measurements_in_parent_generation(measurements, None)
    }

    fn put_measurements_in_parent_generation(
        &self,
        measurements: &[Measurement],
        replacement: Option<&ParentMeasurementReplacement>,
    ) -> Result<(), StorageError> {
        if measurements.len() > MAX_MEASUREMENT_BATCH_ROWS {
            return Err(StorageError::MeasurementBatchTooLarge);
        }

        let mut prepared = Vec::with_capacity(measurements.len());
        let mut encoded_bytes = 0usize;
        for measurement in measurements {
            if let Some(replacement) = replacement
                && measurement.parent_id.as_ref().map(|id| id.as_str())
                    != Some(replacement.parent_id.as_str())
            {
                return Err(StorageError::ParentReplacementMismatch);
            }
            let key = measurement_key(measurement)?;
            let legacy_index_key = measurement_index_key(measurement);
            let parent_index_bytes = measurement.parent_id.as_ref().map_or(0, |parent| {
                parent_index_key(parent.as_str(), &key).len()
                    + parent_version_index_key(parent.as_str(), 1, measurement.id.as_str()).len()
                    + 18 // reserve room for the generation suffix on the series key
            });
            let value = serde_json::to_vec(measurement)?;
            if value.len() > MAX_RECORD_BYTES {
                return Err(StorageError::RecordTooLarge(value.len()));
            }
            encoded_bytes = encoded_bytes
                .saturating_add(key.len())
                .saturating_add(legacy_index_key.len())
                .saturating_add(parent_index_bytes)
                .saturating_add(value.len());
            if encoded_bytes > MAX_MEASUREMENT_BATCH_BYTES {
                return Err(StorageError::MeasurementBatchTooLarge);
            }
            prepared.push((
                key,
                legacy_index_key,
                value,
                measurement.parent_id.clone(),
                measurement.id.clone(),
            ));
        }

        let mut write = self.db.begin_write()?;
        write
            .set_durability(Durability::Immediate)
            .map_err(StorageError::SetDurability)?;
        {
            let active_versions = write.open_table(PARENT_ACTIVE_VERSION)?;
            let staging_versions = write.open_table(PARENT_STAGING_VERSION)?;
            let mut series = write.open_table(SERIES)?;
            let mut external_index = write.open_table(EXTERNAL_INDEX)?;
            if let Some(replacement) = replacement {
                let staging = staging_versions
                    .get(replacement.parent_id.as_str())?
                    .map(|value| decode_generation(value.value()))
                    .transpose()?
                    .ok_or(StorageError::ParentReplacementNotStaging)?;
                if staging != replacement.generation {
                    return Err(StorageError::ParentReplacementNotStaging);
                }
            }
            for (base_key, legacy_index_key, value, parent_id, measurement_id) in prepared {
                let generation = if let Some(replacement) = replacement {
                    Some(replacement.generation)
                } else if let Some(parent_id) = &parent_id {
                    active_versions
                        .get(parent_id.as_str())?
                        .map(|value| decode_generation(value.value()))
                        .transpose()?
                } else {
                    None
                };
                let key = generation.map_or_else(
                    || base_key.clone(),
                    |generation| versioned_measurement_key(&base_key, generation),
                );
                let index_key = match (&parent_id, generation) {
                    (Some(parent_id), Some(generation)) => parent_version_index_key(
                        parent_id.as_str(),
                        generation,
                        measurement_id.as_str(),
                    ),
                    _ => legacy_index_key,
                };
                let parent_key = parent_id
                    .as_ref()
                    .map(|parent| parent_index_key(parent.as_str(), &key));
                let previous_key = external_index
                    .get(index_key.as_str())?
                    .map(|previous| {
                        String::from_utf8(previous.value().to_vec())
                            .map_err(|_| StorageError::CorruptMeasurementIndex)
                    })
                    .transpose()?;
                if let Some(previous_key) = previous_key {
                    if let Some(previous_row) = series.get(previous_key.as_str())? {
                        let previous_measurement: Measurement =
                            serde_json::from_slice(previous_row.value())?;
                        if let Some(previous_parent) = &previous_measurement.parent_id {
                            let old_parent_key =
                                parent_index_key(previous_parent.as_str(), &previous_key);
                            if Some(old_parent_key.as_str()) != parent_key.as_deref() {
                                external_index.remove(old_parent_key.as_str())?;
                            }
                        }
                    }
                    if previous_key != key {
                        series.remove(previous_key.as_str())?;
                    }
                }
                series.insert(key.as_str(), value.as_slice())?;
                external_index.insert(index_key.as_str(), key.as_bytes())?;
                if let Some(parent_key) = parent_key {
                    external_index.insert(parent_key.as_str(), key.as_bytes())?;
                }
                if let (Some(parent_id), Some(generation)) = (&parent_id, generation) {
                    let version_index = parent_version_index_key(
                        parent_id.as_str(),
                        generation,
                        measurement_id.as_str(),
                    );
                    external_index.insert(version_index.as_str(), key.as_bytes())?;
                }
            }
        }
        write.commit()?;
        Ok(())
    }

    /// Start or resume a parent replacement. The durable staging generation is
    /// reused after restart so the caller can safely replay its source batches.
    pub fn begin_parent_measurement_replacement(
        &self,
        parent_id: &str,
    ) -> Result<ParentMeasurementReplacement, StorageError> {
        let mut write = self.db.begin_write()?;
        write
            .set_durability(Durability::Immediate)
            .map_err(StorageError::SetDurability)?;
        let mut staging = write.open_table(PARENT_STAGING_VERSION)?;
        let mut next = write.open_table(PARENT_NEXT_VERSION)?;
        let generation = if let Some(value) = staging.get(parent_id)? {
            decode_generation(value.value())?
        } else {
            let generation = next
                .get(parent_id)?
                .map(|value| decode_generation(value.value()))
                .transpose()?
                .unwrap_or(1);
            let following = generation
                .checked_add(1)
                .ok_or(StorageError::ParentVersionOverflow)?;
            staging.insert(parent_id, &generation.to_be_bytes()[..])?;
            next.insert(parent_id, &following.to_be_bytes()[..])?;
            generation
        };
        drop(next);
        drop(staging);
        write.commit()?;
        Ok(ParentMeasurementReplacement {
            parent_id: parent_id.to_owned(),
            generation,
        })
    }

    /// Write one bounded batch into an invisible generation. Repeating an
    /// identical batch is idempotent; conflicting source corrections are
    /// handled by aborting the staged generation and starting it again.
    pub fn stage_parent_measurements(
        &self,
        replacement: &ParentMeasurementReplacement,
        measurements: &[Measurement],
    ) -> Result<(), StorageError> {
        self.put_measurements_in_parent_generation(measurements, Some(replacement))
    }

    /// Atomically make a fully staged generation visible. `expected_rows` is
    /// checked against the durable staged index so an incomplete stream cannot
    /// be activated accidentally. Readers hold one redb snapshot and therefore
    /// see either the old or the new generation.
    pub fn activate_parent_measurement_replacement(
        &self,
        replacement: &ParentMeasurementReplacement,
        expected_rows: usize,
    ) -> Result<(), StorageError> {
        let mut write = self.db.begin_write()?;
        write
            .set_durability(Durability::Immediate)
            .map_err(StorageError::SetDurability)?;
        {
            let staging = write.open_table(PARENT_STAGING_VERSION)?;
            let generation = staging
                .get(replacement.parent_id.as_str())?
                .map(|value| decode_generation(value.value()))
                .transpose()?
                .ok_or(StorageError::ParentReplacementNotStaging)?;
            if generation != replacement.generation {
                return Err(StorageError::ParentReplacementNotStaging);
            }
        }
        let version_prefix =
            parent_version_index_prefix(replacement.parent_id.as_str(), replacement.generation);
        let staged_rows = {
            let external_index = write.open_table(EXTERNAL_INDEX)?;
            let mut count = 0usize;
            for entry in external_index.range(version_prefix.as_str()..)? {
                let (key, _) = entry?;
                if !key.value().starts_with(&version_prefix) {
                    break;
                }
                count = count.saturating_add(1);
            }
            count
        };
        if staged_rows != expected_rows {
            return Err(StorageError::ParentReplacementIncomplete {
                expected: expected_rows,
                staged: staged_rows,
            });
        }
        {
            let mut active = write.open_table(PARENT_ACTIVE_VERSION)?;
            active.insert(
                replacement.parent_id.as_str(),
                &replacement.generation.to_be_bytes()[..],
            )?;
        }
        {
            let mut staging = write.open_table(PARENT_STAGING_VERSION)?;
            staging.remove(replacement.parent_id.as_str())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Abandon an incomplete staged generation and remove its rows in bounded
    /// chunks. If interrupted, call again with the same handle; the staging
    /// pointer is cleared only after the last row has been removed.
    pub fn discard_staged_parent_measurement_replacement(
        &self,
        replacement: &ParentMeasurementReplacement,
        max_rows: usize,
    ) -> Result<usize, StorageError> {
        if max_rows == 0 {
            return Ok(0);
        }
        let max_rows = max_rows.min(MAX_MEASUREMENT_BATCH_ROWS);
        let mut write = self.db.begin_write()?;
        write
            .set_durability(Durability::Immediate)
            .map_err(StorageError::SetDurability)?;
        let staging_generation = write
            .open_table(PARENT_STAGING_VERSION)?
            .get(replacement.parent_id.as_str())?
            .map(|value| decode_generation(value.value()))
            .transpose()?
            .ok_or(StorageError::ParentReplacementNotStaging)?;
        if staging_generation != replacement.generation {
            return Err(StorageError::ParentReplacementNotStaging);
        }
        let prefix = parent_index_prefix(&replacement.parent_id);
        let mut staged = {
            let external_index = write.open_table(EXTERNAL_INDEX)?;
            let mut staged = Vec::with_capacity(max_rows.min(256));
            for entry in external_index.range(prefix.as_str()..)? {
                let (index, value) = entry?;
                if !index.value().starts_with(&prefix) {
                    break;
                }
                let key = String::from_utf8(value.value().to_vec())
                    .map_err(|_| StorageError::CorruptMeasurementIndex)?;
                if measurement_generation_from_key(&key) == Some(replacement.generation) {
                    staged.push((index.value().to_owned(), key));
                    if staged.len() > max_rows {
                        break;
                    }
                }
            }
            staged
        };
        let has_more = staged.len() > max_rows;
        staged.truncate(max_rows);
        {
            let mut series = write.open_table(SERIES)?;
            let mut external_index = write.open_table(EXTERNAL_INDEX)?;
            for (parent_index, key) in &staged {
                let measurement = series
                    .get(key.as_str())?
                    .map(|row| serde_json::from_slice::<Measurement>(row.value()))
                    .transpose()?;
                if let Some(measurement) = measurement {
                    let version_index = parent_version_index_key(
                        &replacement.parent_id,
                        replacement.generation,
                        measurement.id.as_str(),
                    );
                    external_index.remove(version_index.as_str())?;
                    series.remove(key.as_str())?;
                }
                external_index.remove(parent_index.as_str())?;
            }
        }
        if !has_more {
            write
                .open_table(PARENT_STAGING_VERSION)?
                .remove(replacement.parent_id.as_str())?;
        }
        let removed = staged.len();
        write.commit()?;
        Ok(removed)
    }

    /// Remove obsolete/abandoned rows for one parent in bounded transactions.
    /// The active and current staging generations are retained. Repeating the
    /// call continues cleanup safely after interruption.
    pub fn cleanup_parent_measurement_versions(
        &self,
        parent_id: &str,
        max_rows: usize,
    ) -> Result<usize, StorageError> {
        if max_rows == 0 {
            return Ok(0);
        }
        let max_rows = max_rows.min(MAX_MEASUREMENT_BATCH_ROWS);
        let mut write = self.db.begin_write()?;
        write
            .set_durability(Durability::Immediate)
            .map_err(StorageError::SetDurability)?;
        let active_generation = write
            .open_table(PARENT_ACTIVE_VERSION)?
            .get(parent_id)?
            .map(|value| decode_generation(value.value()))
            .transpose()?;
        let staging_generation = write
            .open_table(PARENT_STAGING_VERSION)?
            .get(parent_id)?
            .map(|value| decode_generation(value.value()))
            .transpose()?;
        let prefix = parent_index_prefix(parent_id);
        let stale = {
            let external_index = write.open_table(EXTERNAL_INDEX)?;
            let mut stale = Vec::with_capacity(max_rows.min(256));
            for entry in external_index.range(prefix.as_str()..)? {
                let (index, value) = entry?;
                if !index.value().starts_with(&prefix) {
                    break;
                }
                let key = String::from_utf8(value.value().to_vec())
                    .map_err(|_| StorageError::CorruptMeasurementIndex)?;
                let generation = measurement_generation_from_key(&key);
                let is_active = generation == active_generation
                    || (active_generation.is_none() && generation.is_none());
                let is_staging = staging_generation.is_some() && generation == staging_generation;
                if is_active || is_staging {
                    continue;
                }
                stale.push((index.value().to_owned(), key));
                if stale.len() == max_rows {
                    break;
                }
            }
            stale
        };
        if stale.is_empty() {
            return Ok(0);
        }
        {
            let mut series = write.open_table(SERIES)?;
            let mut external_index = write.open_table(EXTERNAL_INDEX)?;
            for (parent_index, key) in &stale {
                let measurement = series
                    .get(key.as_str())?
                    .map(|row| serde_json::from_slice::<Measurement>(row.value()))
                    .transpose()?;
                if let Some(measurement) = measurement {
                    let id_index = if let (Some(parent), Some(generation)) =
                        (&measurement.parent_id, measurement_generation_from_key(key))
                    {
                        parent_version_index_key(
                            parent.as_str(),
                            generation,
                            measurement.id.as_str(),
                        )
                    } else {
                        measurement_index_key(&measurement)
                    };
                    if external_index
                        .get(id_index.as_str())?
                        .is_some_and(|value| value.value() == key.as_bytes())
                    {
                        external_index.remove(id_index.as_str())?;
                    }
                    series.remove(key.as_str())?;
                }
                external_index.remove(parent_index.as_str())?;
            }
        }
        let removed = stale.len();
        write.commit()?;
        Ok(removed)
    }

    /// Remove the series rows belonging to one parent document and their
    /// stable-ID index entries. Call before replaying a corrected parent's
    /// current children so samples omitted by the correction do not remain
    /// queryable. A parent-prefix index avoids scanning unrelated history.
    pub fn remove_measurements_for_parent(&self, parent_id: &str) -> Result<usize, StorageError> {
        let mut write = self.db.begin_write()?;
        write
            .set_durability(Durability::Immediate)
            .map_err(StorageError::SetDurability)?;
        let mut children = {
            let external_index = write.open_table(EXTERNAL_INDEX)?;
            let mut children = Vec::new();
            let prefix = parent_index_prefix(parent_id);
            for entry in external_index.range(prefix.as_str()..)? {
                let (key, value) = entry?;
                if !key.value().starts_with(&prefix) {
                    break;
                }
                let series_key = String::from_utf8(value.value().to_vec())
                    .map_err(|_| StorageError::CorruptMeasurementIndex)?;
                children.push((series_key, key.value().to_owned()));
            }
            children
        };
        // Databases created before the parent index was introduced may still
        // contain rows without index entries. Rebuild the missing links for
        // this parent on demand, then subsequent replacements use the narrow
        // prefix range above.
        if children.is_empty() {
            let series = write.open_table(SERIES)?;
            for entry in series.iter()? {
                let (key, value) = entry?;
                let measurement: Measurement = serde_json::from_slice(value.value())?;
                if measurement
                    .parent_id
                    .as_ref()
                    .is_some_and(|parent| parent.as_str() == parent_id)
                {
                    children.push((
                        key.value().to_owned(),
                        parent_index_key(parent_id, key.value()),
                    ));
                }
            }
        }
        if children.is_empty() {
            return Ok(0);
        }
        {
            let mut series = write.open_table(SERIES)?;
            let mut external_index = write.open_table(EXTERNAL_INDEX)?;
            for (key, parent_key) in &children {
                let row = series
                    .get(key.as_str())?
                    .ok_or(StorageError::CorruptMeasurementIndex)?;
                let measurement: Measurement = serde_json::from_slice(row.value())?;
                drop(row);
                let index_key = measurement_index_key(&measurement);
                series.remove(key.as_str())?;
                external_index.remove(index_key.as_str())?;
                external_index.remove(parent_key.as_str())?;
            }
        }
        write.commit()?;
        Ok(children.len())
    }

    /// Read a bounded time-range page using the ordered series key and an
    /// opaque keyset cursor. UTC range semantics are half-open `[from, to)`.
    pub fn measurement_page(
        &self,
        query: MeasurementQuery<'_>,
    ) -> Result<MeasurementPage, StorageError> {
        if query.limit == 0 || query.limit > MAX_MEASUREMENT_PAGE_ROWS {
            return Err(StorageError::InvalidMeasurementPageLimit);
        }
        if query.from_utc_micros >= query.to_utc_micros {
            return Err(StorageError::InvalidMeasurementRange);
        }

        let base_prefix = format!(
            "{}~{}~{}~",
            hex_component(query.user_id),
            hex_component(query.source),
            hex_component(query.metric),
        );
        let context_prefix = query
            .context
            .map(|context| format!("{}{}~", base_prefix, context_key(context)));
        let prefix = context_prefix.as_deref().unwrap_or(&base_prefix);

        let lower = context_prefix.as_ref().map(|context| {
            format!(
                "{}{:016x}~",
                context,
                ordered_timestamp(query.from_utc_micros)
            )
        });
        let upper = context_prefix.as_ref().map(|context| {
            // 'g' sorts after every hex digit allowed in the id component, so
            // this exclusive bound excludes all rows at `to_utc_micros`.
            format!(
                "{}{:016x}~g",
                context,
                ordered_timestamp(query.to_utc_micros)
            )
        });

        if let Some(cursor) = query.after {
            if !cursor.starts_with(prefix) {
                return Err(StorageError::InvalidMeasurementCursor);
            }
            if let Some(upper) = upper.as_deref()
                && cursor >= upper
            {
                return Err(StorageError::InvalidMeasurementCursor);
            }
        }

        let read = self.db.begin_read()?;
        let series = read.open_table(SERIES)?;
        let active_versions = read.open_table(PARENT_ACTIVE_VERSION)?;
        let mut rows = Vec::with_capacity(query.limit.min(128));
        let mut next_cursor = None;
        let mut page_bytes = 0usize;
        let mut has_more = false;

        macro_rules! read_entries {
            ($entries:expr, $entry_prefix:expr, $entry_upper:expr) => {
                for entry in $entries {
                    let (key_value, value) = entry?;
                    let key = key_value.value();
                    if !key.starts_with($entry_prefix) {
                        break;
                    }
                    if let Some(entry_upper) = $entry_upper
                        && key >= entry_upper
                    {
                        break;
                    }

                    let encoded = value.value();
                    let measurement: Measurement = serde_json::from_slice(encoded)?;
                    if let Some(parent_id) = &measurement.parent_id {
                        let active_generation = active_versions
                            .get(parent_id.as_str())?
                            .map(|value| decode_generation(value.value()))
                            .transpose()?;
                        if measurement_generation_from_key(key) != active_generation {
                            continue;
                        }
                    }
                    let Some(timestamp) = measurement.time.start.utc_micros() else {
                        return Err(StorageError::CorruptMeasurementTime);
                    };
                    if timestamp < query.from_utc_micros || timestamp >= query.to_utc_micros {
                        continue;
                    }

                    if rows.len() == query.limit {
                        has_more = true;
                        break;
                    }
                    let row_bytes = key.len().saturating_add(encoded.len());
                    if page_bytes.saturating_add(row_bytes) > MAX_MEASUREMENT_PAGE_BYTES {
                        if rows.is_empty() {
                            return Err(StorageError::MeasurementRowTooLarge);
                        }
                        has_more = true;
                        break;
                    }
                    page_bytes += row_bytes;
                    next_cursor = Some(key.to_owned());
                    rows.push(measurement);
                }
            };
        }

        if let (Some(context_prefix), Some(lower), Some(upper)) = (
            context_prefix.as_deref(),
            lower.as_deref(),
            upper.as_deref(),
        ) {
            let start = query.after.map_or((Included(lower), Unbounded), |cursor| {
                if cursor >= lower {
                    (Excluded(cursor), Excluded(upper))
                } else {
                    (Included(lower), Excluded(upper))
                }
            });
            let entries = series.range::<&str>(start)?;
            read_entries!(entries, context_prefix, Some(upper));
        } else {
            // With no context filter the series key groups contexts before
            // timestamps. Seek directly into the requested timestamp range in
            // each known context instead of scanning years of unrelated rows.
            // Unknown provider contexts share a prefix; scan that tail as a
            // compatibility fallback and retain every value.
            let mut context_keys = vec![
                "cardio_load",
                "daily",
                "exercise",
                "nightly_recharge",
                "nightscout",
                "profile",
                "sleep_wise",
                "sleep",
                "unknown_",
            ];
            context_keys.sort_unstable();
            for context_key in context_keys {
                let context_prefix = format!("{base_prefix}{context_key}");
                if context_key == "unknown_" {
                    let end = format!("{base_prefix}unknown`");
                    if query.after.is_some_and(|cursor| cursor >= end.as_str()) {
                        continue;
                    }
                    let start = query
                        .after
                        .map_or(Included(context_prefix.as_str()), |cursor| {
                            if cursor >= context_prefix.as_str() {
                                Excluded(cursor)
                            } else {
                                Included(context_prefix.as_str())
                            }
                        });
                    let entries = series.range::<&str>((start, Excluded(end.as_str())))?;
                    read_entries!(entries, context_prefix.as_str(), Some(end.as_str()));
                } else {
                    let exact_prefix = format!("{context_prefix}~");
                    let lower = format!(
                        "{exact_prefix}{:016x}~",
                        ordered_timestamp(query.from_utc_micros)
                    );
                    let upper = format!(
                        "{exact_prefix}{:016x}~g",
                        ordered_timestamp(query.to_utc_micros)
                    );
                    if query.after.is_some_and(|cursor| cursor >= upper.as_str()) {
                        continue;
                    }
                    let start = query.after.map_or(Included(lower.as_str()), |cursor| {
                        if cursor >= lower.as_str() {
                            Excluded(cursor)
                        } else {
                            Included(lower.as_str())
                        }
                    });
                    // Hold the strings until range() consumes the bound.
                    let entries = series.range::<&str>((start, Excluded(upper.as_str())))?;
                    read_entries!(entries, exact_prefix.as_str(), Some(upper.as_str()));
                }
                if has_more {
                    break;
                }
            }
        }

        Ok(MeasurementPage {
            rows,
            next_cursor: has_more.then_some(next_cursor).flatten(),
        })
    }

    /// Read a bounded page of records ordered by their stable string key.
    pub fn record_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let read = self.db.begin_read()?;
        let table = read.open_table(RECORDS)?;
        let mut page = Vec::with_capacity(limit.min(1000));
        let mut page_bytes = 0usize;
        let entries = match after {
            Some(cursor) => table.range::<&str>((Excluded(cursor), Unbounded))?,
            None => table.range::<&str>((Unbounded::<&str>, Unbounded::<&str>))?,
        };
        for entry in entries {
            let (key, value) = entry?;
            let key = key.value();
            let value = value.value();
            if value.len() > MAX_RECORD_BYTES {
                return Err(StorageError::RecordTooLarge(value.len()));
            }
            let row_bytes = key.len().saturating_add(value.len());
            if !page.is_empty() && page_bytes.saturating_add(row_bytes) > MAX_PAGE_BYTES {
                break;
            }
            page_bytes += row_bytes;
            page.push((key.to_owned(), value.to_vec()));
            if page.len() >= limit {
                break;
            }
        }
        Ok(page)
    }

    pub fn schema_version(&self) -> Result<String, StorageError> {
        let read = self.db.begin_read()?;
        let table = read.open_table(META)?;
        let value = table
            .get("schema_version")?
            .ok_or(StorageError::MissingSchemaVersion)?;
        Ok(value.value().to_owned())
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
struct BackupManifest {
    format_version: u32,
    schema_version: String,
    created_at: String,
    includes_credentials: bool,
}

fn copy_bytes_table(
    source: &redb::ReadTransaction,
    destination: &Database,
    definition: TableDefinition<&str, &[u8]>,
) -> Result<(), StorageError> {
    let table = source.open_table(definition)?;
    let mut batch = Vec::<(String, Vec<u8>)>::new();
    let mut batch_bytes = 0usize;
    for entry in table.iter()? {
        let (key, value) = entry?;
        let key = key.value().to_owned();
        let value = value.value().to_vec();
        batch_bytes = batch_bytes
            .saturating_add(key.len())
            .saturating_add(value.len());
        batch.push((key, value));
        if batch.len() >= MAX_RECORD_BATCH_ROWS || batch_bytes >= MAX_RECORD_BATCH_BYTES {
            write_bytes_batch(destination, definition, &batch)?;
            batch.clear();
            batch_bytes = 0;
        }
    }
    if !batch.is_empty() {
        write_bytes_batch(destination, definition, &batch)?;
    }
    Ok(())
}

fn write_bytes_batch(
    destination: &Database,
    definition: TableDefinition<&str, &[u8]>,
    batch: &[(String, Vec<u8>)],
) -> Result<(), StorageError> {
    let mut write = destination.begin_write()?;
    write
        .set_durability(Durability::Immediate)
        .map_err(StorageError::SetDurability)?;
    {
        let mut table = write.open_table(definition)?;
        for (key, value) in batch {
            table.insert(key.as_str(), value.as_slice())?;
        }
    }
    write.commit()?;
    Ok(())
}

fn copy_string_table(
    source: &redb::ReadTransaction,
    destination: &Database,
    definition: TableDefinition<&str, &str>,
) -> Result<(), StorageError> {
    let table = source.open_table(definition)?;
    let mut batch = Vec::<(String, String)>::new();
    let mut batch_bytes = 0usize;
    for entry in table.iter()? {
        let (key, value) = entry?;
        let key = key.value().to_owned();
        let value = value.value().to_owned();
        batch_bytes = batch_bytes
            .saturating_add(key.len())
            .saturating_add(value.len());
        batch.push((key, value));
        if batch.len() >= MAX_RECORD_BATCH_ROWS || batch_bytes >= MAX_RECORD_BATCH_BYTES {
            write_string_batch(destination, definition, &batch)?;
            batch.clear();
            batch_bytes = 0;
        }
    }
    if !batch.is_empty() {
        write_string_batch(destination, definition, &batch)?;
    }
    Ok(())
}

fn write_string_batch(
    destination: &Database,
    definition: TableDefinition<&str, &str>,
    batch: &[(String, String)],
) -> Result<(), StorageError> {
    let mut write = destination.begin_write()?;
    write
        .set_durability(Durability::Immediate)
        .map_err(StorageError::SetDurability)?;
    {
        let mut table = write.open_table(definition)?;
        for (key, value) in batch {
            table.insert(key.as_str(), value.as_str())?;
        }
    }
    write.commit()?;
    Ok(())
}

fn copy_archive_tree(source: &Path, destination: &Path) -> Result<(), StorageError> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let from = entry.path();
        let to = destination.join(entry.file_name());
        if kind.is_dir() {
            fs::create_dir(&to)?;
            restrict_directory(&to)?;
            copy_archive_tree(&from, &to)?;
        } else if kind.is_file()
            && from.extension().and_then(|extension| extension.to_str()) != Some("part")
        {
            fs::copy(&from, &to)?;
            restrict_file(&to)?;
            fs::File::open(&to)?.sync_all()?;
        }
    }
    sync_directory(destination)?;
    Ok(())
}

fn sync_tree_directories(root: &Path) -> Result<(), StorageError> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_tree_directories(&entry.path())?;
        }
    }
    sync_directory(root)?;
    Ok(())
}

#[cfg(unix)]
fn restrict_directory(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    fs::File::open(path)?.sync_all()
}

fn measurement_key(measurement: &Measurement) -> Result<String, StorageError> {
    let start = measurement
        .time
        .start
        .utc_micros()
        .ok_or(StorageError::UnresolvedMeasurementTime)?;
    if let Some(end) = &measurement.time.end {
        let end = end
            .utc_micros()
            .ok_or(StorageError::UnresolvedMeasurementTime)?;
        if end < start {
            return Err(StorageError::InvalidMeasurementInterval);
        }
    }
    Ok(format!(
        "{}~{}~{}~{}~{:016x}~{}",
        hex_component(measurement.provenance.user_id.as_str()),
        hex_component(measurement.provenance.source.as_str()),
        hex_component(measurement.metric.as_str()),
        context_key(&measurement.context),
        ordered_timestamp(start),
        hex_component(measurement.id.as_str()),
    ))
}

fn measurement_index_key(measurement: &Measurement) -> String {
    measurement_index_key_parts(
        measurement.provenance.user_id.as_str(),
        measurement.provenance.source.as_str(),
        measurement.id.as_str(),
    )
}

fn measurement_index_key_parts(user_id: &str, source: &str, id: &str) -> String {
    format!(
        "measurement:{}:{}:{}",
        hex_component(user_id),
        hex_component(source),
        hex_component(id),
    )
}

fn versioned_measurement_key(base_key: &str, generation: u64) -> String {
    format!("{base_key}~v{generation:016x}")
}

fn measurement_generation_from_key(key: &str) -> Option<u64> {
    let (_, suffix) = key.rsplit_once("~v")?;
    (suffix.len() == 16)
        .then(|| u64::from_str_radix(suffix, 16).ok())
        .flatten()
}

fn parent_version_index_prefix(parent_id: &str, generation: u64) -> String {
    format!(
        "parent-version:{}:{generation:016x}:",
        hex_component(parent_id)
    )
}

fn parent_version_index_key(parent_id: &str, generation: u64, measurement_id: &str) -> String {
    format!(
        "{}{}",
        parent_version_index_prefix(parent_id, generation),
        hex_component(measurement_id)
    )
}

fn decode_generation(bytes: &[u8]) -> Result<u64, StorageError> {
    let generation: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::CorruptParentVersion)?;
    Ok(u64::from_be_bytes(generation))
}

fn parent_index_prefix(parent_id: &str) -> String {
    format!("parent:{}:", hex_component(parent_id))
}

fn parent_index_key(parent_id: &str, series_key: &str) -> String {
    format!(
        "{}{}",
        parent_index_prefix(parent_id),
        hex_component(series_key)
    )
}

fn context_key(context: &MeasurementContext) -> String {
    match context {
        MeasurementContext::Daily => "daily".into(),
        MeasurementContext::Sleep => "sleep".into(),
        MeasurementContext::Exercise => "exercise".into(),
        MeasurementContext::NightlyRecharge => "nightly_recharge".into(),
        MeasurementContext::SleepWise => "sleep_wise".into(),
        MeasurementContext::CardioLoad => "cardio_load".into(),
        MeasurementContext::Profile => "profile".into(),
        MeasurementContext::Nightscout => "nightscout".into(),
        MeasurementContext::Unknown(value) => format!("unknown_{}", hex_component(value)),
    }
}

/// Hex encoding is delimiter-safe and preserves bytewise lexical ordering.
fn hex_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len().saturating_mul(2));
    for byte in value.bytes() {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

/// Flip the sign bit so lexicographic big-endian hexadecimal order matches
/// signed i64 chronological order, including dates before the Unix epoch.
fn ordered_timestamp(unix_micros: i64) -> u64 {
    (unix_micros as u64) ^ (1_u64 << 63)
}

#[cfg(not(unix))]
fn restrict_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_file(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn restrict_file(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    Redb(redb::Error),
    Database(redb::DatabaseError),
    Table(redb::TableError),
    Storage(redb::StorageError),
    Transaction(redb::TransactionError),
    Commit(redb::CommitError),
    SetDurability(redb::SetDurabilityError),
    Json(serde_json::Error),
    UnsupportedSchema(String),
    MissingSchemaVersion,
    ArchiveReceiptConflict(String),
    RecordTooLarge(usize),
    RecordBatchTooLarge,
    MeasurementBatchTooLarge,
    InvalidMeasurementPageLimit,
    InvalidMeasurementRange,
    InvalidMeasurementCursor,
    UnresolvedMeasurementTime,
    InvalidMeasurementInterval,
    CorruptMeasurementIndex,
    CorruptMeasurementTime,
    MeasurementRowTooLarge,
    ParentReplacementMismatch,
    ParentReplacementNotStaging,
    ParentReplacementIncomplete {
        expected: usize,
        staged: usize,
    },
    ParentVersionOverflow,
    CorruptParentVersion,
    InvalidSyncStateKey,
    SyncStateTooLarge(usize),
    InvalidBackupDestination,
    InvalidBackupArchive(u64),
    InvalidCacheBytes(String),
    CacheBytesOutOfRange {
        value: usize,
        minimum: usize,
        maximum: usize,
    },
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "storage I/O error: {e}"),
            Self::Redb(e) => write!(f, "redb error: {e}"),
            Self::Database(e) => write!(f, "redb database error: {e}"),
            Self::Table(e) => write!(f, "redb table error: {e}"),
            Self::Storage(e) => write!(f, "redb storage error: {e}"),
            Self::Transaction(e) => write!(f, "redb transaction error: {e}"),
            Self::Commit(e) => write!(f, "redb commit error: {e}"),
            Self::SetDurability(e) => write!(f, "redb durability error: {e}"),
            Self::Json(e) => write!(f, "storage JSON error: {e}"),
            Self::UnsupportedSchema(v) => write!(f, "unsupported database schema version {v}"),
            Self::MissingSchemaVersion => f.write_str("database schema version is missing"),
            Self::ArchiveReceiptConflict(id) => write!(
                f,
                "archive receipt {id} already exists with different content"
            ),
            Self::RecordTooLarge(size) => {
                write!(f, "record size {size} exceeds the 4 MiB limit")
            }
            Self::RecordBatchTooLarge => {
                f.write_str("record batch exceeds the 1000-row or 512 KiB limit")
            }
            Self::MeasurementBatchTooLarge => write!(
                f,
                "measurement batch exceeds {MAX_MEASUREMENT_BATCH_ROWS} rows or {MAX_MEASUREMENT_BATCH_BYTES} encoded bytes"
            ),
            Self::InvalidMeasurementPageLimit => write!(
                f,
                "measurement page limit must be between 1 and {MAX_MEASUREMENT_PAGE_ROWS}"
            ),
            Self::InvalidMeasurementRange => {
                f.write_str("measurement range must be a non-empty UTC half-open interval")
            }
            Self::InvalidMeasurementCursor => {
                f.write_str("measurement cursor does not belong to this query")
            }
            Self::UnresolvedMeasurementTime => {
                f.write_str("measurement series rows require UTC-resolved times")
            }
            Self::InvalidMeasurementInterval => {
                f.write_str("measurement interval end precedes its start")
            }
            Self::CorruptMeasurementIndex => {
                f.write_str("measurement id index contains an invalid series key")
            }
            Self::CorruptMeasurementTime => {
                f.write_str("stored series measurement has no UTC-resolved start")
            }
            Self::MeasurementRowTooLarge => {
                f.write_str("one measurement row exceeds the bounded page byte limit")
            }
            Self::ParentReplacementMismatch => {
                f.write_str("staged measurement belongs to a different parent")
            }
            Self::ParentReplacementNotStaging => {
                f.write_str("parent replacement generation is not the active staging version")
            }
            Self::ParentReplacementIncomplete { expected, staged } => write!(
                f,
                "parent replacement has {staged} staged rows; {expected} were expected"
            ),
            Self::ParentVersionOverflow => f.write_str("parent version counter overflowed"),
            Self::CorruptParentVersion => {
                f.write_str("parent version metadata is not an eight-byte generation")
            }
            Self::InvalidSyncStateKey => {
                f.write_str("sync state key must contain between 1 and 512 bytes")
            }
            Self::SyncStateTooLarge(size) => {
                write!(f, "sync state value size {size} exceeds 64 KiB")
            }
            Self::InvalidBackupDestination => f.write_str(
                "backup destination must be a new directory outside the live data directory",
            ),
            Self::InvalidBackupArchive(failed) => {
                write!(f, "backup contains {failed} invalid archive object(s)")
            }
            Self::InvalidCacheBytes(value) => write!(
                f,
                "RUST_POLAR_DB_CACHE_BYTES must be an integer between {MIN_CACHE_BYTES} and {MAX_CACHE_BYTES}; got {value:?}"
            ),
            Self::CacheBytesOutOfRange {
                value,
                minimum,
                maximum,
            } => write!(
                f,
                "redb cache size must be between {minimum} and {maximum} bytes; got {value}"
            ),
        }
    }
}
impl std::error::Error for StorageError {}
impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<redb::Error> for StorageError {
    fn from(e: redb::Error) -> Self {
        Self::Redb(e)
    }
}
impl From<redb::DatabaseError> for StorageError {
    fn from(e: redb::DatabaseError) -> Self {
        Self::Database(e)
    }
}
impl From<redb::TableError> for StorageError {
    fn from(e: redb::TableError) -> Self {
        Self::Table(e)
    }
}
impl From<redb::StorageError> for StorageError {
    fn from(e: redb::StorageError) -> Self {
        Self::Storage(e)
    }
}
impl From<redb::TransactionError> for StorageError {
    fn from(e: redb::TransactionError) -> Self {
        Self::Transaction(e)
    }
}
impl From<redb::CommitError> for StorageError {
    fn from(e: redb::CommitError) -> Self {
        Self::Commit(e)
    }
}
impl From<serde_json::Error> for StorageError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Aggregation, DataQuality, EventTime, MetricId, NumericValue, Provenance, SourceId,
        StableId, TimeInterval, Unit, UserId,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "rust-polar-storage-{}-{nonce}-{}",
                std::process::id(),
                NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn redb_cache_configuration_defaults_and_validates_bounds() {
        assert_eq!(DEFAULT_CACHE_BYTES, 8 * 1024 * 1024);
        assert_eq!(
            configured_cache_bytes(Err(std::env::VarError::NotPresent)).unwrap(),
            DEFAULT_CACHE_BYTES
        );
        assert_eq!(
            configured_cache_bytes(Ok("2097152".to_owned())).unwrap(),
            2 * 1024 * 1024
        );
        assert!(matches!(
            configured_cache_bytes(Ok("many".to_owned())),
            Err(StorageError::InvalidCacheBytes(_))
        ));
        assert!(validate_cache_bytes(MIN_CACHE_BYTES).is_ok());
        assert!(validate_cache_bytes(MAX_CACHE_BYTES).is_ok());
        assert!(matches!(
            validate_cache_bytes(MIN_CACHE_BYTES - 1),
            Err(StorageError::CacheBytesOutOfRange { .. })
        ));
        assert!(matches!(
            validate_cache_bytes(MAX_CACHE_BYTES + 1),
            Err(StorageError::CacheBytesOutOfRange { .. })
        ));
    }

    #[test]
    fn store_opens_with_explicit_cache_configuration() {
        let temp = TestDir::new();
        let store = Store::open_with_cache_bytes(&temp.0, 2 * 1024 * 1024).unwrap();
        assert_eq!(store.schema_version().unwrap(), "3");
    }

    #[test]
    fn initializes_versioned_schema_and_round_trips_archive_receipt() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();
        assert_eq!(store.schema_version().unwrap(), "3");
        let value = serde_json::json!({
            "receipt_id": "r-1",
            "content_hash": "hash",
            "status": "pending"
        });
        store.register_archive("r-1", &value).unwrap();
        store.register_archive("r-1", &value).unwrap();
        assert_eq!(store.archive_count().unwrap(), 1);
        assert_eq!(store.pending_archive_ids(10).unwrap(), vec!["r-1"]);
        store.set_archive_state("r-1", "processed").unwrap();
        assert!(store.pending_archive_ids(10).unwrap().is_empty());
        drop(store);

        let reopened = Store::open(&temp.0).unwrap();
        assert_eq!(reopened.archive_count().unwrap(), 1);
        assert_eq!(reopened.schema_version().unwrap(), "3");
    }

    #[test]
    fn upgrades_schema_one_by_adding_archive_processing_state() {
        let temp = TestDir::new();
        fs::create_dir_all(&temp.0).unwrap();
        let db = Database::create(temp.0.join("health.redb")).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut meta = write.open_table(META).unwrap();
            meta.insert("schema_version", "1").unwrap();
        }
        write.commit().unwrap();
        drop(db);
        let store = Store::open(&temp.0).unwrap();
        assert_eq!(store.schema_version().unwrap(), "3");
        assert!(store.pending_archive_ids(10).unwrap().is_empty());
    }

    #[test]
    fn sync_state_is_durable_replaceable_and_paged() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();
        store
            .put_sync_state(
                "polar/activity",
                &serde_json::json!({"through": "2026-10-20"}),
            )
            .unwrap();
        store
            .put_sync_state("polar/sleep", &serde_json::json!({"through": "2026-10-21"}))
            .unwrap();
        store
            .put_sync_state(
                "polar/activity",
                &serde_json::json!({"through": "2026-10-21"}),
            )
            .unwrap();
        let saved: serde_json::Value =
            serde_json::from_slice(&store.get_sync_state("polar/activity").unwrap().unwrap())
                .unwrap();
        assert_eq!(saved["through"], "2026-10-21");
        let first = store.sync_state_page(None, 1).unwrap();
        assert_eq!(first[0].0, "polar/activity");
        assert_eq!(
            store.sync_state_page(Some(&first[0].0), 1).unwrap()[0].0,
            "polar/sleep"
        );
        assert!(matches!(
            store.put_sync_state("", &serde_json::Value::Null),
            Err(StorageError::InvalidSyncStateKey)
        ));
    }

    #[test]
    fn rejects_receipt_idempotency_conflict() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();
        store.register_archive("r-1", &"first").unwrap();
        assert!(matches!(
            store.register_archive("r-1", &"second"),
            Err(StorageError::ArchiveReceiptConflict(_))
        ));
    }

    #[test]
    fn backup_restore_preserves_redb_records_measurements_and_archives() {
        let temp = TestDir::new();
        let source_dir = temp.0.join("source");
        let backup_dir = temp.0.join("backup");
        let restored_dir = temp.0.join("restored");
        let store = Store::open(&source_dir).unwrap();
        store
            .put_record("polar/user-profile/self", br#"{"first_name":"Ada"}"#)
            .unwrap();
        store
            .put_measurements(&[measurement("point-1", 10, "70")])
            .unwrap();
        let archive = crate::archive::ArchiveStore::new(store.archive_path().to_path_buf());
        let receipt = archive
            .store_body(
                "polar",
                "test",
                Default::default(),
                200,
                "2026-10-21T00:00:00Z",
                std::io::Cursor::new(b"{}".as_slice()),
            )
            .unwrap();
        store
            .register_archive(&receipt.receipt_id, &receipt)
            .unwrap();

        store.backup_to(&backup_dir).unwrap();
        assert!(matches!(
            store.backup_to(&backup_dir),
            Err(StorageError::InvalidBackupDestination)
        ));
        drop(store);

        let backup = Store::open(&backup_dir).unwrap();
        assert_eq!(backup.record_count().unwrap(), 1);
        assert_eq!(backup.measurement_count().unwrap(), 1);
        assert_eq!(backup.archive_count().unwrap(), 1);
        assert_eq!(
            crate::archive::verify_all(backup.archive_path())
                .unwrap()
                .failed,
            0
        );
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(backup_dir.join("backup.json")).unwrap())
                .unwrap();
        assert_eq!(manifest["includes_credentials"], false);
        drop(backup);

        Store::restore_backup(&backup_dir, &restored_dir).unwrap();
        let restored = Store::open(&restored_dir).unwrap();
        assert_eq!(restored.record_count().unwrap(), 1);
        assert_eq!(restored.measurement_count().unwrap(), 1);
        assert_eq!(restored.archive_count().unwrap(), 1);
        assert_eq!(
            restored
                .get_record("polar/user-profile/self")
                .unwrap()
                .unwrap(),
            br#"{"first_name":"Ada"}"#
        );
    }

    #[test]
    fn bounds_record_size_and_record_page_memory() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();
        let too_large = vec![0_u8; MAX_RECORD_BYTES + 1];
        assert!(matches!(
            store.put_record("too-large", &too_large),
            Err(StorageError::RecordTooLarge(_))
        ));
        let payload = vec![b'x'; 3 * 1024 * 1024];
        store.put_record("a", &payload).unwrap();
        store.put_record("b", &payload).unwrap();
        let page = store.record_page(None, 1000).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].0, "a");
    }

    #[test]
    fn provider_record_batches_are_atomic_and_bounded() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();
        store.put_records(&[("a", b"one"), ("b", b"two")]).unwrap();
        assert_eq!(store.record_count().unwrap(), 2);

        let payload = vec![b'x'; 300 * 1024];
        assert!(matches!(
            store.put_records(&[("c", payload.as_slice()), ("d", payload.as_slice())]),
            Err(StorageError::RecordBatchTooLarge)
        ));
        assert_eq!(store.record_count().unwrap(), 2);
    }

    fn measurement(id: &str, timestamp: i64, value: &str) -> Measurement {
        Measurement {
            schema_version: 1,
            id: StableId::new(id),
            provenance: Provenance {
                user_id: UserId::new("test-user"),
                source: SourceId::new("polar"),
                resource: "continuous-heart-rate".into(),
                provider_external_id: None,
                source_device_id: Some("loop".into()),
                fetched_at_utc_micros: 1_800_000_000_000_000,
                archive_ref: None,
                revision: None,
                quality: DataQuality::ProviderReported,
            },
            metric: MetricId::new("heart_rate"),
            value: NumericValue::new(value).unwrap(),
            unit: Unit::BeatsPerMinute,
            time: TimeInterval::point(EventTime::utc(timestamp)),
            aggregation: Aggregation::Mean,
            context: MeasurementContext::Daily,
            parent_id: None,
        }
    }

    fn heart_rate_query<'a>(
        from_utc_micros: i64,
        to_utc_micros: i64,
        limit: usize,
        after: Option<&'a str>,
    ) -> MeasurementQuery<'a> {
        MeasurementQuery {
            user_id: "test-user",
            source: "polar",
            metric: "heart_rate",
            context: Some(&MeasurementContext::Daily),
            from_utc_micros,
            to_utc_micros,
            limit,
            after,
        }
    }

    fn exercise_sample(id: &str, timestamp: i64, value: &str, parent: &str) -> Measurement {
        let mut measurement = measurement(id, timestamp, value);
        measurement.context = MeasurementContext::Exercise;
        measurement.parent_id = Some(StableId::new(parent));
        measurement
    }

    fn exercise_query<'a>(
        from_utc_micros: i64,
        to_utc_micros: i64,
        limit: usize,
        after: Option<&'a str>,
    ) -> MeasurementQuery<'a> {
        MeasurementQuery {
            user_id: "test-user",
            source: "polar",
            metric: "heart_rate",
            context: Some(&MeasurementContext::Exercise),
            from_utc_micros,
            to_utc_micros,
            limit,
            after,
        }
    }

    #[test]
    fn context_free_range_queries_seek_each_context_and_page_across_contexts() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();
        let mut rows = Vec::new();
        for (context, values) in [
            (
                MeasurementContext::Daily,
                vec![(5, "d-old"), (15, "d-15"), (25, "d-25"), (35, "d-new")],
            ),
            (
                MeasurementContext::Exercise,
                vec![(12, "e-12"), (22, "e-22")],
            ),
            (
                MeasurementContext::Unknown("future".into()),
                vec![(18, "u-18")],
            ),
        ] {
            for (timestamp, id) in values {
                let mut row = measurement(id, timestamp, "70");
                row.context = context.clone();
                rows.push(row);
            }
        }
        store.put_measurements(&rows).unwrap();
        let query_page = |after: Option<&str>| {
            store.measurement_page(MeasurementQuery {
                user_id: "test-user",
                source: "polar",
                metric: "heart_rate",
                context: None,
                from_utc_micros: 10,
                to_utc_micros: 30,
                limit: 2,
                after,
            })
        };

        let first = query_page(None).unwrap();
        assert_eq!(
            first
                .rows
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["d-15", "d-25"]
        );
        let second = query_page(first.next_cursor.as_deref()).unwrap();
        assert_eq!(
            second
                .rows
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["e-12", "e-22"]
        );
        let third = query_page(second.next_cursor.as_deref()).unwrap();
        assert_eq!(
            third
                .rows
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["u-18"]
        );
        assert!(third.next_cursor.is_none());
    }

    #[test]
    fn stores_synthetic_series_in_bounded_batches_and_pages_by_time_key() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();

        // Generate rather than retain the complete backfill in memory. The
        // writer additionally rejects batches above both row and byte caps.
        let mut start = 0_i64;
        for batch_start in (0..1505).step_by(100) {
            let batch_end = (batch_start + 100).min(1505);
            let batch: Vec<_> = (batch_start..batch_end)
                .map(|index| {
                    measurement(
                        &format!("sample-{index:04}"),
                        index as i64 * 300_000_000,
                        &format!("{}", 55 + index % 40),
                    )
                })
                .collect();
            assert!(batch.len() <= MAX_MEASUREMENT_BATCH_ROWS);
            store.put_measurements(&batch).unwrap();
            start = batch_end as i64 * 300_000_000;
        }

        let page_one = store
            .measurement_page(heart_rate_query(0, start, 1000, None))
            .unwrap();
        assert_eq!(page_one.rows.len(), 1000);
        assert!(page_one.next_cursor.is_some());

        let page_two = store
            .measurement_page(heart_rate_query(
                0,
                start,
                1000,
                page_one.next_cursor.as_deref(),
            ))
            .unwrap();
        assert_eq!(page_two.rows.len(), 505);
        assert!(page_two.next_cursor.is_none());
        assert_eq!(
            page_two.rows.last().unwrap().time.start.utc_micros(),
            Some(start - 300_000_000)
        );
    }

    #[test]
    fn removing_corrected_parent_samples_cleans_rows_and_stable_id_index() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();
        let parent = StableId::new("exercise-1");
        let mut first = measurement("exercise-sample-0", 10, "100");
        first.parent_id = Some(parent.clone());
        first.context = MeasurementContext::Exercise;
        let mut second = measurement("exercise-sample-1", 20, "110");
        second.parent_id = Some(parent.clone());
        second.context = MeasurementContext::Exercise;
        let other = measurement("unrelated-sample", 30, "120");
        store.put_measurements(&[first, second, other]).unwrap();

        assert_eq!(
            store
                .remove_measurements_for_parent(parent.as_str())
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .remove_measurements_for_parent(parent.as_str())
                .unwrap(),
            0
        );
        assert_eq!(store.measurement_count().unwrap(), 1);
        assert!(
            store
                .measurement_page(heart_rate_query(0, 100, 10, None))
                .unwrap()
                .rows
                .iter()
                .any(|row| row.id.as_str() == "unrelated-sample")
        );

        // The old stable IDs can be inserted again after their index entries
        // were removed, which is what a corrected exercise replay requires.
        let mut corrected = measurement("exercise-sample-0", 40, "115");
        corrected.parent_id = Some(parent);
        corrected.context = MeasurementContext::Exercise;
        store.put_measurements(&[corrected]).unwrap();
        assert_eq!(store.measurement_count().unwrap(), 2);
    }

    #[test]
    fn replacing_measurement_parent_updates_parent_index() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();
        let first_parent = StableId::new("exercise-old");
        let second_parent = StableId::new("exercise-new");
        let mut original = measurement("reassigned-sample", 10, "100");
        original.parent_id = Some(first_parent.clone());
        original.context = MeasurementContext::Exercise;
        store.put_measurements(&[original.clone()]).unwrap();

        original.parent_id = Some(second_parent.clone());
        original.value = NumericValue::new("101").unwrap();
        store.put_measurements(&[original]).unwrap();

        assert_eq!(
            store
                .remove_measurements_for_parent(first_parent.as_str())
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .remove_measurements_for_parent(second_parent.as_str())
                .unwrap(),
            1
        );
        assert_eq!(store.measurement_count().unwrap(), 0);
    }

    #[test]
    fn staged_parent_replacement_survives_restart_and_switches_atomically() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();
        let parent = "exercise-staged";
        store
            .put_measurements(&[
                exercise_sample("sample-a", 10, "100", parent),
                exercise_sample("sample-b", 20, "110", parent),
            ])
            .unwrap();

        let replacement = store.begin_parent_measurement_replacement(parent).unwrap();
        store
            .stage_parent_measurements(
                &replacement,
                &[exercise_sample("sample-a", 30, "120", parent)],
            )
            .unwrap();
        let visible_before = store
            .measurement_page(exercise_query(0, 100, 10, None))
            .unwrap()
            .rows;
        assert_eq!(visible_before.len(), 2);
        assert!(visible_before.iter().all(|row| row.value.as_str() != "120"));
        drop(store);

        // A restart before activation leaves the old generation visible and
        // returns the same durable staging token. Replaying its batch is safe.
        let reopened = Store::open(&temp.0).unwrap();
        let resumed = reopened
            .begin_parent_measurement_replacement(parent)
            .unwrap();
        assert_eq!(resumed, replacement);
        reopened
            .stage_parent_measurements(&resumed, &[exercise_sample("sample-a", 30, "120", parent)])
            .unwrap();
        assert!(matches!(
            reopened.activate_parent_measurement_replacement(&resumed, 2),
            Err(StorageError::ParentReplacementIncomplete {
                expected: 2,
                staged: 1
            })
        ));
        assert_eq!(
            reopened
                .measurement_page(exercise_query(0, 100, 10, None))
                .unwrap()
                .rows
                .len(),
            2
        );

        // The corrected exercise has fewer samples. Activation is one small
        // pointer update; old rows become invisible together while cleanup is
        // still pending.
        reopened
            .activate_parent_measurement_replacement(&resumed, 1)
            .unwrap();
        drop(reopened);
        let after_activation_restart = Store::open(&temp.0).unwrap();
        let visible_after = after_activation_restart
            .measurement_page(exercise_query(0, 100, 10, None))
            .unwrap()
            .rows;
        assert_eq!(visible_after.len(), 1);
        assert_eq!(visible_after[0].id.as_str(), "sample-a");
        assert_eq!(visible_after[0].value.as_str(), "120");

        assert_eq!(
            after_activation_restart
                .cleanup_parent_measurement_versions(parent, 1)
                .unwrap(),
            1
        );
        assert_eq!(
            after_activation_restart
                .cleanup_parent_measurement_versions(parent, 1)
                .unwrap(),
            1
        );
        assert_eq!(
            after_activation_restart
                .cleanup_parent_measurement_versions(parent, 1)
                .unwrap(),
            0
        );
        assert_eq!(
            after_activation_restart
                .measurement_page(exercise_query(0, 100, 10, None))
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn replacement_is_idempotent_even_when_corrected_time_moves() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();
        let original = measurement("stable-point", 10, "70");
        store
            .put_measurements(std::slice::from_ref(&original))
            .unwrap();
        store.put_measurements(&[original]).unwrap();

        let corrected = measurement("stable-point", 20, "71");
        store.put_measurements(&[corrected]).unwrap();

        let page = store
            .measurement_page(heart_rate_query(0, 100, 10, None))
            .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].value.as_str(), "71");
        assert_eq!(page.rows[0].time.start.utc_micros(), Some(20));
    }
}
