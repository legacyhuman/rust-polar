//! Crash-safe, content-addressed storage for source API response bodies.
//!
//! The caller owns HTTP streaming and must pass a bounded reader. This module
//! never buffers a complete response: it hashes while copying through gzip to
//! a temporary file, then publishes the object and its receipt atomically.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

const FORMAT_VERSION: u32 = 1;
const COPY_BUFFER_BYTES: usize = 64 * 1024;
pub const DEFAULT_MIN_FREE_BYTES: u64 = 512 * 1024 * 1024;

/// Removes an unpublished temporary file on every early-return path. Rename
/// publication consumes the path, so the final removal is then a harmless
/// no-op. This also covers failures while creating/verifying the object path.
struct RemoveFileOnDrop(PathBuf);

impl Drop for RemoveFileOnDrop {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Redacted request parameters. Callers must omit credentials and personal
/// secrets before calling [`ArchiveStore::store_body`].
pub type RequestMetadata = BTreeMap<String, String>;

/// Stable reference to one fetch event. Multiple receipts may reference the
/// same content-addressed object when a response body repeats.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArchiveReceipt {
    pub receipt_id: String,
    pub content_hash: String,
    pub source: String,
    pub resource: String,
    pub request: RequestMetadata,
    pub status: u16,
    pub fetched_at: String,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
}

/// Result of a body verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Verification {
    pub content_hash: String,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
}

/// Counts returned by the `archive verify` command.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VerifySummary {
    pub verified: u64,
    pub failed: u64,
}

/// Bounded-memory summary of old temporary and unreferenced object files.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArchiveDiagnostics {
    pub stale_parts: u64,
    pub stale_part_bytes: u64,
    pub orphan_objects: u64,
    pub orphan_object_bytes: u64,
}

/// One issue delivered to a diagnostic callback. The scanner creates one
/// value at a time and does not retain issue paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveIssue {
    pub kind: ArchiveIssueKind,
    pub path: PathBuf,
    pub bytes: u64,
    pub modified: SystemTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchiveIssueKind {
    StalePart,
    OrphanObject,
}

/// A dry-run cleanup plan. Its fields are private so deletion can only follow
/// a successful `preview_cleanup` call. Apply it while no writer is publishing
/// archive objects; redb ownership normally provides that single-process rule.
#[derive(Clone, Debug)]
pub struct CleanupPlan {
    cutoff: SystemTime,
    summary: ArchiveDiagnostics,
    fingerprint: [u8; 32],
}

impl CleanupPlan {
    pub fn summary(&self) -> ArchiveDiagnostics {
        self.summary
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CleanupResult {
    pub removed_parts: u64,
    pub removed_part_bytes: u64,
    pub removed_objects: u64,
    pub removed_object_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Descriptor {
    format_version: u32,
    receipt_id: String,
    content_hash: String,
    source: String,
    resource: String,
    request: RequestMetadata,
    status: u16,
    fetched_at: String,
    uncompressed_bytes: u64,
    compressed_bytes: u64,
}

/// Filesystem-backed archive. `root` is expected to be inside a private data
/// directory owned by the current user.
#[derive(Clone, Debug)]
pub struct ArchiveStore {
    root: PathBuf,
    min_free_bytes: u64,
    #[cfg(test)]
    fault: Option<TestFault>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestFault {
    BodyWriteAfter(u64),
    FileSync,
    ObjectRename,
    ReceiptSync,
    ReceiptRename,
}

#[cfg(test)]
type FaultPoint = TestFault;
#[cfg(not(test))]
type FaultPoint = NoFault;

#[cfg(not(test))]
struct NoFault;

macro_rules! archive_fault {
    ($fault:ident) => {{
        #[cfg(test)]
        {
            TestFault::$fault
        }
        #[cfg(not(test))]
        {
            NoFault
        }
    }};
}

#[cfg(test)]
struct FailAfterWriter<'a> {
    inner: &'a mut File,
    remaining: u64,
}

#[cfg(test)]
impl Write for FailAfterWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::from(io::ErrorKind::StorageFull));
        }
        let allowed = usize::try_from(self.remaining.min(buf.len() as u64)).unwrap_or(buf.len());
        let written = self.inner.write(&buf[..allowed])?;
        self.remaining = self.remaining.saturating_sub(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn compress_body<R: Read, W: Write>(
    mut reader: R,
    output: &mut W,
) -> Result<(String, u64), ArchiveError> {
    let mut compressed = GzEncoder::new(output, Compression::default());
    let mut hasher = blake3::Hasher::new();
    let mut uncompressed_bytes = 0_u64;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        uncompressed_bytes = uncompressed_bytes
            .checked_add(count as u64)
            .ok_or(ArchiveError::SizeOverflow)?;
        compressed.write_all(&buffer[..count])?;
    }
    compressed.finish()?;
    Ok((hasher.finalize().to_hex().to_string(), uncompressed_bytes))
}

impl ArchiveStore {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            min_free_bytes: DEFAULT_MIN_FREE_BYTES,
            #[cfg(test)]
            fault: None,
        }
    }

    /// Configure the minimum free-space reserve required before an archive
    /// response begins writing. Mid-stream ENOSPC still aborts and removes the
    /// unpublished temporary object.
    pub fn with_min_free_bytes(mut self, bytes: u64) -> Self {
        self.min_free_bytes = bytes;
        self
    }

    /// Store one already-decoded HTTP response body.
    ///
    /// The object is published first, then a durable receipt descriptor. The
    /// caller can register that receipt as `pending` in redb after this returns.
    /// If the process stops between publication and DB registration,
    /// [`recover_sidecars`](Self::recover_sidecars) finds the receipt.
    pub fn store_body<R: Read>(
        &self,
        source: impl Into<String>,
        resource: impl Into<String>,
        request: RequestMetadata,
        status: u16,
        fetched_at: impl Into<String>,
        body: R,
    ) -> Result<ArchiveReceipt, ArchiveError> {
        self.ensure_layout()?;
        let available = available_space_bytes(&self.root)?;
        if available < self.min_free_bytes {
            return Err(ArchiveError::InsufficientFreeSpace {
                available_bytes: available,
                required_bytes: self.min_free_bytes,
            });
        }
        let source = source.into();
        let resource = resource.into();
        let fetched_at = fetched_at.into();
        let receipt_id = new_id();
        let temp_path = self.root.join("tmp").join(format!("{receipt_id}.part"));
        let mut temp_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp_path)?;
        let _cleanup_temp = RemoveFileOnDrop(temp_path.clone());

        let write_result = (|| {
            #[cfg(test)]
            let (content_hash, uncompressed_bytes) =
                if let Some(TestFault::BodyWriteAfter(limit)) = self.fault {
                    let mut writer = FailAfterWriter {
                        inner: &mut temp_file,
                        remaining: limit,
                    };
                    compress_body(body, &mut writer)?
                } else {
                    compress_body(body, &mut temp_file)?
                };
            #[cfg(not(test))]
            let (content_hash, uncompressed_bytes) = compress_body(body, &mut temp_file)?;

            temp_file.flush()?;
            self.sync_file(&temp_file, archive_fault!(FileSync))?;
            let compressed_bytes = temp_file.metadata()?.len();
            Ok::<_, ArchiveError>((content_hash, uncompressed_bytes, compressed_bytes))
        })();

        /*
         * Keep the compressed stream helper generic so tests can inject a
         * storage write failure after a precise number of bytes without
         * substituting a failing HTTP/body reader (which exercises a
         * different failure boundary).
         */
        let (content_hash, uncompressed_bytes, mut compressed_bytes) = match write_result {
            Ok(result) => result,
            Err(error) => {
                drop(temp_file);
                return Err(error);
            }
        };
        drop(temp_file);

        let object_path = self.object_path(&content_hash);
        let object_dir = object_path.parent().expect("object path has parent");
        fs::create_dir_all(object_dir)?;
        if object_path.exists() {
            // Do not trust a pre-existing content-addressed file blindly.
            let existing = self.verify_object(&object_path, &content_hash)?;
            compressed_bytes = existing.compressed_bytes;
            fs::remove_file(&temp_path)?;
        } else {
            self.rename(&temp_path, &object_path, archive_fault!(ObjectRename))?;
            sync_directory(object_dir)?;
            sync_directory(&self.root.join("objects"))?;
        }

        let descriptor = Descriptor {
            format_version: FORMAT_VERSION,
            receipt_id: receipt_id.clone(),
            content_hash: content_hash.clone(),
            source: source.clone(),
            resource: resource.clone(),
            request,
            status,
            fetched_at: fetched_at.clone(),
            uncompressed_bytes,
            compressed_bytes,
        };
        self.publish_descriptor(&descriptor)?;

        Ok(receipt_from(&descriptor))
    }

    #[cfg(test)]
    fn sync_file(&self, file: &File, fault: FaultPoint) -> Result<(), ArchiveError> {
        if self.fault == Some(fault) {
            return Err(ArchiveError::Io(io::Error::other(
                "injected archive fsync failure",
            )));
        }
        file.sync_all()?;
        Ok(())
    }

    #[cfg(not(test))]
    fn sync_file(&self, file: &File, _fault: FaultPoint) -> Result<(), ArchiveError> {
        file.sync_all()?;
        Ok(())
    }

    #[cfg(test)]
    fn rename(&self, from: &Path, to: &Path, fault: FaultPoint) -> Result<(), ArchiveError> {
        if self.fault == Some(fault) {
            return Err(ArchiveError::Io(io::Error::other(
                "injected archive rename failure",
            )));
        }
        fs::rename(from, to)?;
        Ok(())
    }

    #[cfg(not(test))]
    fn rename(&self, from: &Path, to: &Path, _fault: FaultPoint) -> Result<(), ArchiveError> {
        fs::rename(from, to)?;
        Ok(())
    }

    /// Open and decompress a receipt's original response body.
    pub fn read(&self, receipt: &ArchiveReceipt) -> Result<Box<dyn Read>, ArchiveError> {
        let descriptor = self.read_descriptor(&receipt.receipt_id)?;
        validate_receipt(receipt, &descriptor)?;
        let file = File::open(self.object_path(&descriptor.content_hash))?;
        Ok(Box::new(GzDecoder::new(BufReader::new(file))))
    }

    /// Verify the descriptor and stream the complete compressed object through
    /// BLAKE3. The response body itself is never loaded into memory.
    pub fn verify(&self, receipt: &ArchiveReceipt) -> Result<Verification, ArchiveError> {
        let descriptor = self.read_descriptor(&receipt.receipt_id)?;
        validate_receipt(receipt, &descriptor)?;
        let path = self.object_path(&descriptor.content_hash);
        let result = self.verify_object(&path, &descriptor.content_hash)?;
        if result.uncompressed_bytes != descriptor.uncompressed_bytes
            || result.compressed_bytes != descriptor.compressed_bytes
        {
            return Err(ArchiveError::DescriptorMismatch(receipt.receipt_id.clone()));
        }
        Ok(result)
    }

    /// Return all durable descriptors not yet necessarily registered in redb.
    /// The caller can idempotently insert these as pending archive records.
    pub fn recover_sidecars(&self) -> Result<Vec<ArchiveReceipt>, ArchiveError> {
        let mut receipts = self.receipts()?.collect::<Result<Vec<_>, _>>()?;
        receipts.sort_by(|a, b| a.receipt_id.cmp(&b.receipt_id));
        Ok(receipts)
    }

    /// Stream durable descriptors in filesystem iteration order. This avoids
    /// retaining the full receipt catalog in memory during replay on small
    /// devices. The order is intentionally unspecified; replay is idempotent.
    pub fn receipts(
        &self,
    ) -> Result<impl Iterator<Item = Result<ArchiveReceipt, ArchiveError>>, ArchiveError> {
        let dir = self.root.join("receipts");
        if !dir.exists() {
            return Ok(Box::new(std::iter::empty())
                as Box<dyn Iterator<Item = Result<ArchiveReceipt, ArchiveError>>>);
        }
        let entries = fs::read_dir(dir)?;
        Ok(Box::new(entries.filter_map(|entry| match entry {
            Err(error) => Some(Err(ArchiveError::Io(error))),
            Ok(entry) => {
                let path = entry.path();
                match entry.file_type() {
                    Err(error) => return Some(Err(ArchiveError::Io(error))),
                    Ok(file_type) if !file_type.is_file() => return None,
                    Ok(_) => {}
                }
                let is_json = path.extension().and_then(|x| x.to_str()) == Some("json");
                if !is_json {
                    return None;
                }
                Some(read_descriptor_path(&path).map(|descriptor| receipt_from(&descriptor)))
            }
        })))
    }

    /// Stream a report of `.part` files and complete objects with no receipt.
    /// Only files at least `older_than` old are reported. Memory use stays
    /// bounded regardless of archive size; the callback receives one path at
    /// a time and should stream output rather than retain every issue.
    pub fn diagnose<F>(
        &self,
        older_than: std::time::Duration,
        mut report: F,
    ) -> Result<ArchiveDiagnostics, ArchiveError>
    where
        F: FnMut(&ArchiveIssue),
    {
        let cutoff = age_cutoff(older_than)?;
        Ok(scan_candidates(&self.root, cutoff, &mut report)?.0)
    }

    /// Dry-run a cleanup. Deletion is available only by passing the returned
    /// plan to `apply_cleanup`; the plan captures a bounded fingerprint and
    /// the explicit age cutoff, not a list of paths.
    pub fn preview_cleanup(
        &self,
        older_than: std::time::Duration,
    ) -> Result<CleanupPlan, ArchiveError> {
        let cutoff = age_cutoff(older_than)?;
        let (summary, fingerprint) = scan_candidates(&self.root, cutoff, &mut |_| {})?;
        Ok(CleanupPlan {
            cutoff,
            summary,
            fingerprint,
        })
    }

    /// Apply a prior dry-run. The candidate set is rescanned first; if it
    /// changed, no files are removed. Callers must ensure no archive writer is
    /// active during apply, since the filesystem and redb cannot share an
    /// atomic transaction.
    pub fn apply_cleanup(&self, plan: &CleanupPlan) -> Result<CleanupResult, ArchiveError> {
        let (current, fingerprint) = scan_candidates(&self.root, plan.cutoff, &mut |_| {})?;
        if current != plan.summary || fingerprint != plan.fingerprint {
            return Err(ArchiveError::CleanupPlanStale);
        }

        let mut removed = CleanupResult::default();
        let mut remove_error = None;
        scan_candidates(&self.root, plan.cutoff, &mut |issue| {
            // This second pass is bounded. Errors are collected outside the
            // visitor through a separate path below; never follow symlinks.
            if remove_error.is_some() {
                return;
            }
            match fs::symlink_metadata(&issue.path) {
                Ok(metadata)
                    if metadata.file_type().is_file()
                        && metadata.len() == issue.bytes
                        && metadata.modified().ok() == Some(issue.modified) =>
                {
                    match fs::remove_file(&issue.path) {
                        Ok(()) => match issue.kind {
                            ArchiveIssueKind::StalePart => {
                                removed.removed_parts += 1;
                                removed.removed_part_bytes += issue.bytes;
                            }
                            ArchiveIssueKind::OrphanObject => {
                                removed.removed_objects += 1;
                                removed.removed_object_bytes += issue.bytes;
                            }
                        },
                        Err(error) => remove_error = Some(error),
                    }
                }
                Ok(_) => {
                    remove_error = Some(io::Error::other(
                        "cleanup candidate changed after preflight",
                    ))
                }
                Err(error) => remove_error = Some(error),
            }
        })?;
        if let Some(error) = remove_error {
            return Err(ArchiveError::Io(error));
        }
        Ok(removed)
    }

    fn ensure_layout(&self) -> Result<(), ArchiveError> {
        fs::create_dir_all(&self.root)?;
        fs::create_dir_all(self.root.join("objects"))?;
        fs::create_dir_all(self.root.join("receipts"))?;
        fs::create_dir_all(self.root.join("tmp"))?;
        restrict_directory(&self.root)?;
        restrict_directory(&self.root.join("objects"))?;
        restrict_directory(&self.root.join("receipts"))?;
        restrict_directory(&self.root.join("tmp"))?;
        sync_directory(&self.root)?;
        Ok(())
    }

    fn object_path(&self, hash: &str) -> PathBuf {
        self.root
            .join("objects")
            .join(hash.get(..2).unwrap_or("invalid"))
            .join(format!("{hash}.gz"))
    }

    fn descriptor_path(&self, receipt_id: &str) -> PathBuf {
        self.root
            .join("receipts")
            .join(format!("{receipt_id}.json"))
    }

    fn publish_descriptor(&self, descriptor: &Descriptor) -> Result<(), ArchiveError> {
        let path = self.descriptor_path(&descriptor.receipt_id);
        let temp = self
            .root
            .join("tmp")
            .join(format!("{}.receipt.part", descriptor.receipt_id));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        let publish = (|| {
            serde_json::to_writer(&mut file, descriptor)?;
            file.write_all(b"\n")?;
            file.flush()?;
            self.sync_file(&file, archive_fault!(ReceiptSync))?;
            self.rename(&temp, &path, archive_fault!(ReceiptRename))?;
            sync_directory(path.parent().expect("receipt path has parent"))?;
            sync_directory(&self.root)?;
            Ok::<_, ArchiveError>(())
        })();
        if publish.is_err() {
            drop(file);
            let _ = fs::remove_file(temp);
        }
        publish
    }

    fn read_descriptor(&self, receipt_id: &str) -> Result<Descriptor, ArchiveError> {
        // Restrict path interpolation to IDs generated by this module.
        if receipt_id.is_empty()
            || !receipt_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(ArchiveError::InvalidReceiptId);
        }
        let descriptor = read_descriptor_path(&self.descriptor_path(receipt_id))?;
        validate_descriptor(&descriptor)?;
        Ok(descriptor)
    }

    fn verify_object(
        &self,
        path: &Path,
        expected_hash: &str,
    ) -> Result<Verification, ArchiveError> {
        let file = File::open(path)?;
        let compressed_bytes = file.metadata()?.len();
        let mut decoder = GzDecoder::new(BufReader::new(file));
        let mut hasher = blake3::Hasher::new();
        let mut uncompressed_bytes = 0_u64;
        let mut buffer = [0_u8; COPY_BUFFER_BYTES];
        loop {
            let count = decoder.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
            uncompressed_bytes = uncompressed_bytes
                .checked_add(count as u64)
                .ok_or(ArchiveError::SizeOverflow)?;
        }
        let actual = hasher.finalize().to_hex().to_string();
        if actual != expected_hash {
            return Err(ArchiveError::HashMismatch {
                expected: expected_hash.to_owned(),
                actual,
            });
        }
        Ok(Verification {
            content_hash: actual,
            uncompressed_bytes,
            compressed_bytes,
        })
    }
}

fn age_cutoff(older_than: std::time::Duration) -> Result<SystemTime, ArchiveError> {
    if older_than.is_zero() {
        return Err(ArchiveError::InvalidAgeThreshold);
    }
    Ok(SystemTime::now()
        .checked_sub(older_than)
        .unwrap_or(UNIX_EPOCH))
}

/// Enumerate candidates with bounded memory. Reference checking deliberately
/// streams descriptors once per object rather than building an unbounded set
/// of all hashes; this trades CPU for predictable RAM on small devices.
fn scan_candidates<F>(
    root: &Path,
    cutoff: SystemTime,
    report: &mut F,
) -> Result<(ArchiveDiagnostics, [u8; 32]), ArchiveError>
where
    F: FnMut(&ArchiveIssue),
{
    let mut summary = ArchiveDiagnostics::default();
    let mut fingerprint = blake3::Hasher::new();

    let tmp_dir = root.join("tmp");
    if tmp_dir.exists() {
        for entry in fs::read_dir(tmp_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file()
                || entry.path().extension().and_then(|ext| ext.to_str()) != Some("part")
            {
                continue;
            }
            let metadata = fs::symlink_metadata(entry.path())?;
            let modified = metadata.modified()?;
            if modified <= cutoff {
                record_issue(
                    &mut summary,
                    &mut fingerprint,
                    report,
                    ArchiveIssue {
                        kind: ArchiveIssueKind::StalePart,
                        path: entry.path(),
                        bytes: metadata.len(),
                        modified,
                    },
                )?;
            }
        }
    }

    let objects = root.join("objects");
    if objects.exists() {
        for prefix in fs::read_dir(&objects)? {
            let prefix = prefix?;
            if !prefix.file_type()?.is_dir() {
                continue;
            }
            let prefix_name = prefix.file_name();
            let prefix_name = prefix_name.to_str().unwrap_or_default();
            for entry in fs::read_dir(prefix.path())? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let file_name = entry.file_name();
                let Some(hash) = file_name.to_str().and_then(|name| name.strip_suffix(".gz"))
                else {
                    continue;
                };
                if hash.len() != 64
                    || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                    || hash.get(..2) != Some(prefix_name)
                {
                    continue;
                }

                let metadata = fs::symlink_metadata(entry.path())?;
                let modified = metadata.modified()?;
                if modified > cutoff || has_receipt_reference(root, hash)? {
                    continue;
                }
                record_issue(
                    &mut summary,
                    &mut fingerprint,
                    report,
                    ArchiveIssue {
                        kind: ArchiveIssueKind::OrphanObject,
                        path: entry.path(),
                        bytes: metadata.len(),
                        modified,
                    },
                )?;
            }
        }
    }

    Ok((summary, *fingerprint.finalize().as_bytes()))
}

fn has_receipt_reference(root: &Path, hash: &str) -> Result<bool, ArchiveError> {
    let store = ArchiveStore::new(root.to_path_buf());
    for receipt in store.receipts()? {
        if receipt?.content_hash == hash {
            return Ok(true);
        }
    }
    Ok(false)
}

fn record_issue<F>(
    summary: &mut ArchiveDiagnostics,
    fingerprint: &mut blake3::Hasher,
    report: &mut F,
    issue: ArchiveIssue,
) -> Result<(), ArchiveError>
where
    F: FnMut(&ArchiveIssue),
{
    let size = issue.bytes;
    match issue.kind {
        ArchiveIssueKind::StalePart => {
            summary.stale_parts = summary
                .stale_parts
                .checked_add(1)
                .ok_or(ArchiveError::SizeOverflow)?;
            summary.stale_part_bytes = summary
                .stale_part_bytes
                .checked_add(size)
                .ok_or(ArchiveError::SizeOverflow)?;
            fingerprint.update(&[0]);
        }
        ArchiveIssueKind::OrphanObject => {
            summary.orphan_objects = summary
                .orphan_objects
                .checked_add(1)
                .ok_or(ArchiveError::SizeOverflow)?;
            summary.orphan_object_bytes = summary
                .orphan_object_bytes
                .checked_add(size)
                .ok_or(ArchiveError::SizeOverflow)?;
            fingerprint.update(&[1]);
        }
    }
    let path = issue.path.as_os_str().as_encoded_bytes();
    fingerprint.update(&(path.len() as u64).to_le_bytes());
    fingerprint.update(path);
    fingerprint.update(&issue.bytes.to_le_bytes());
    let modified = issue
        .modified
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    fingerprint.update(&modified.as_secs().to_le_bytes());
    fingerprint.update(&modified.subsec_nanos().to_le_bytes());
    report(&issue);
    Ok(())
}

/// Verify every durable receipt under an archive directory. An unreadable
/// descriptor or missing/corrupt object increments `failed`; independent
/// receipts are still checked so the command reports the full result.
pub fn verify_all(archive_path: &Path) -> Result<VerifySummary, ArchiveError> {
    let store = ArchiveStore::new(archive_path.to_path_buf());
    let receipts_dir = archive_path.join("receipts");
    if !receipts_dir.exists() {
        return Ok(VerifySummary::default());
    }

    let mut summary = VerifySummary::default();
    for entry in fs::read_dir(receipts_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file()
            || entry.path().extension().and_then(|ext| ext.to_str()) != Some("json")
        {
            continue;
        }
        match read_descriptor_path(&entry.path()) {
            Ok(descriptor) => {
                let receipt = receipt_from(&descriptor);
                if store.verify(&receipt).is_ok() {
                    summary.verified += 1;
                } else {
                    summary.failed += 1;
                }
            }
            Err(_) => summary.failed += 1,
        }
    }
    Ok(summary)
}

fn read_descriptor_path(path: &Path) -> Result<Descriptor, ArchiveError> {
    let descriptor: Descriptor = serde_json::from_reader(BufReader::new(File::open(path)?))?;
    if descriptor.format_version != FORMAT_VERSION {
        return Err(ArchiveError::UnsupportedFormat(descriptor.format_version));
    }
    validate_descriptor(&descriptor)?;
    Ok(descriptor)
}

fn validate_descriptor(descriptor: &Descriptor) -> Result<(), ArchiveError> {
    if descriptor.receipt_id.is_empty()
        || !descriptor
            .receipt_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(ArchiveError::InvalidReceiptId);
    }
    if descriptor.content_hash.len() != 64
        || !descriptor
            .content_hash
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
    {
        return Err(ArchiveError::InvalidContentHash);
    }
    Ok(())
}

fn validate_receipt(receipt: &ArchiveReceipt, descriptor: &Descriptor) -> Result<(), ArchiveError> {
    if receipt.receipt_id != descriptor.receipt_id
        || receipt.content_hash != descriptor.content_hash
        || receipt.source != descriptor.source
        || receipt.resource != descriptor.resource
        || receipt.request != descriptor.request
        || receipt.status != descriptor.status
        || receipt.fetched_at != descriptor.fetched_at
        || receipt.uncompressed_bytes != descriptor.uncompressed_bytes
        || receipt.compressed_bytes != descriptor.compressed_bytes
    {
        return Err(ArchiveError::DescriptorMismatch(receipt.receipt_id.clone()));
    }
    Ok(())
}

fn receipt_from(descriptor: &Descriptor) -> ArchiveReceipt {
    ArchiveReceipt {
        receipt_id: descriptor.receipt_id.clone(),
        content_hash: descriptor.content_hash.clone(),
        source: descriptor.source.clone(),
        resource: descriptor.resource.clone(),
        request: descriptor.request.clone(),
        status: descriptor.status,
        fetched_at: descriptor.fetched_at.clone(),
        uncompressed_bytes: descriptor.uncompressed_bytes,
        compressed_bytes: descriptor.compressed_bytes,
    }
}

fn new_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seq = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("{:x}-{:x}-{:x}", std::process::id(), nanos, seq)
}

fn sync_directory(path: &Path) -> io::Result<()> {
    // Directory fsync is supported on Unix. On other targets, file fsync and
    // atomic same-filesystem rename still provide the portable core behavior.
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn available_space_bytes(path: &Path) -> io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;

    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    // SAFETY: `path` is a valid NUL-terminated path and `stats` points to
    // initialized writable storage for the duration of the call.
    unsafe {
        let mut stats = std::mem::zeroed::<libc::statvfs>();
        if libc::statvfs(path.as_ptr(), &mut stats) != 0 {
            return Err(io::Error::last_os_error());
        }
        let blocks = stats.f_bavail as u64;
        let block_size = stats.f_frsize as u64;
        Ok(blocks.saturating_mul(block_size))
    }
}

#[cfg(not(unix))]
fn available_space_bytes(_path: &Path) -> io::Result<u64> {
    // The first release targets macOS and Linux. Other platforms retain the
    // streaming ENOSPC cleanup behavior but do not get a reserve preflight.
    Ok(u64::MAX)
}

#[cfg(unix)]
fn restrict_directory(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn restrict_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[derive(Debug)]
pub enum ArchiveError {
    Io(io::Error),
    Json(serde_json::Error),
    HashMismatch {
        expected: String,
        actual: String,
    },
    DescriptorMismatch(String),
    UnsupportedFormat(u32),
    InvalidReceiptId,
    InvalidContentHash,
    SizeOverflow,
    InvalidAgeThreshold,
    CleanupPlanStale,
    InsufficientFreeSpace {
        available_bytes: u64,
        required_bytes: u64,
    },
}

impl std::fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "archive I/O error: {e}"),
            Self::Json(e) => write!(f, "invalid archive descriptor: {e}"),
            Self::HashMismatch { expected, actual } => write!(
                f,
                "archive hash mismatch: expected {expected}, got {actual}"
            ),
            Self::DescriptorMismatch(id) => {
                write!(f, "archive receipt descriptor mismatch for {id}")
            }
            Self::UnsupportedFormat(version) => {
                write!(f, "unsupported archive descriptor version {version}")
            }
            Self::InvalidReceiptId => f.write_str("invalid archive receipt id"),
            Self::InvalidContentHash => f.write_str("invalid archive content hash"),
            Self::SizeOverflow => f.write_str("archive body size overflow"),
            Self::InvalidAgeThreshold => {
                f.write_str("archive age threshold must be greater than zero")
            }
            Self::CleanupPlanStale => {
                f.write_str("archive changed after cleanup preview; create a new plan")
            }
            Self::InsufficientFreeSpace {
                available_bytes,
                required_bytes,
            } => write!(
                f,
                "archive needs at least {required_bytes} bytes free; filesystem has {available_bytes}"
            ),
        }
    }
}

impl std::error::Error for ArchiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Json(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ArchiveError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for ArchiveError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::time::Duration;

    struct FailsAfter {
        bytes: &'static [u8],
        sent: bool,
    }

    impl Read for FailsAfter {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.sent {
                return Err(io::Error::other("injected interrupted response body"));
            }
            self.sent = true;
            let count = self.bytes.len().min(buffer.len());
            buffer[..count].copy_from_slice(&self.bytes[..count]);
            Ok(count)
        }
    }

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("rust-polar-archive-test-{}", new_id()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn metadata(page: &str) -> RequestMetadata {
        BTreeMap::from([("page".to_owned(), page.to_owned())])
    }

    fn make_old(path: &Path) {
        let file = OpenOptions::new().write(true).open(path).unwrap();
        file.set_times(fs::FileTimes::new().set_modified(UNIX_EPOCH))
            .unwrap();
    }

    #[test]
    fn streamed_body_round_trips_and_verifies() {
        let dir = TestDir::new();
        let store = ArchiveStore::new(dir.0.join("archive"));
        let body = (0..400_000).map(|n| (n % 251) as u8).collect::<Vec<_>>();
        let receipt = store
            .store_body(
                "polar",
                "activity",
                metadata("1"),
                200,
                "2026-10-21T12:00:00Z",
                Cursor::new(body.clone()),
            )
            .unwrap();

        let mut actual = Vec::new();
        store
            .read(&receipt)
            .unwrap()
            .read_to_end(&mut actual)
            .unwrap();
        assert_eq!(actual, body);
        let verification = store.verify(&receipt).unwrap();
        assert_eq!(verification.uncompressed_bytes, 400_000);
        assert_eq!(verification.content_hash, receipt.content_hash);
    }

    #[test]
    fn free_space_reserve_rejects_before_creating_a_partial_archive() {
        let dir = TestDir::new();
        let store = ArchiveStore::new(dir.0.join("archive")).with_min_free_bytes(u64::MAX);
        let error = store
            .store_body(
                "polar",
                "activities",
                RequestMetadata::new(),
                200,
                "2026-10-21T00:00:00Z",
                io::Cursor::new(b"{}".as_slice()),
            )
            .unwrap_err();
        assert!(matches!(error, ArchiveError::InsufficientFreeSpace { .. }));
        assert_eq!(fs::read_dir(dir.0.join("archive/tmp")).unwrap().count(), 0);
    }

    #[test]
    fn interrupted_body_discards_unpublished_archive_and_receipt() {
        let dir = TestDir::new();
        let store = ArchiveStore::new(dir.0.join("archive"));
        let reader = FailsAfter {
            bytes: b"first chunk made it to gzip",
            sent: false,
        };

        assert!(matches!(
            store.store_body("polar", "activity", metadata("1"), 200, "t", reader),
            Err(ArchiveError::Io(_))
        ));
        assert_eq!(fs::read_dir(store.root.join("tmp")).unwrap().count(), 0);
        assert_eq!(store.receipts().unwrap().count(), 0);
        assert_eq!(fs::read_dir(store.root.join("objects")).unwrap().count(), 0);
    }

    #[test]
    fn enospc_during_archive_write_removes_unpublished_files() {
        let dir = TestDir::new();
        let mut store = ArchiveStore::new(dir.0.join("archive"));
        store.fault = Some(TestFault::BodyWriteAfter(8));

        let error = store
            .store_body(
                "polar",
                "activity",
                metadata("1"),
                200,
                "t",
                Cursor::new(vec![b'x'; 256 * 1024]),
            )
            .unwrap_err();

        let ArchiveError::Io(error) = error else {
            panic!("expected injected ENOSPC, got {error:?}");
        };
        assert_eq!(error.kind(), io::ErrorKind::StorageFull);
        assert_eq!(fs::read_dir(store.root.join("tmp")).unwrap().count(), 0);
        assert_eq!(store.receipts().unwrap().count(), 0);
        assert_eq!(fs::read_dir(store.root.join("objects")).unwrap().count(), 0);
    }

    #[test]
    fn file_sync_failure_removes_partial_object_and_receipt() {
        let dir = TestDir::new();
        let mut store = ArchiveStore::new(dir.0.join("archive"));
        store.fault = Some(TestFault::FileSync);

        let result = store.store_body(
            "polar",
            "activity",
            metadata("1"),
            200,
            "t",
            Cursor::new(b"complete body".to_vec()),
        );

        assert!(matches!(result, Err(ArchiveError::Io(_))));
        assert_eq!(fs::read_dir(store.root.join("tmp")).unwrap().count(), 0);
        assert_eq!(store.receipts().unwrap().count(), 0);
        assert_eq!(fs::read_dir(store.root.join("objects")).unwrap().count(), 0);
    }

    #[test]
    fn object_rename_failure_does_not_publish_or_leak_temporary_body() {
        let dir = TestDir::new();
        let mut store = ArchiveStore::new(dir.0.join("archive"));
        store.fault = Some(TestFault::ObjectRename);

        let result = store.store_body(
            "polar",
            "activity",
            metadata("1"),
            200,
            "t",
            Cursor::new(b"complete body".to_vec()),
        );

        assert!(matches!(result, Err(ArchiveError::Io(_))));
        assert_eq!(fs::read_dir(store.root.join("tmp")).unwrap().count(), 0);
        assert_eq!(store.receipts().unwrap().count(), 0);
        assert_eq!(fs::read_dir(store.root.join("objects")).unwrap().count(), 1);
        let prefix = fs::read_dir(store.root.join("objects"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(fs::read_dir(prefix.path()).unwrap().count(), 0);
    }

    #[test]
    fn receipt_rename_failure_leaves_only_diagnosable_orphan_object() {
        let dir = TestDir::new();
        let mut store = ArchiveStore::new(dir.0.join("archive"));
        store.fault = Some(TestFault::ReceiptRename);

        let result = store.store_body(
            "polar",
            "activity",
            metadata("1"),
            200,
            "t",
            Cursor::new(b"complete body".to_vec()),
        );

        assert!(matches!(result, Err(ArchiveError::Io(_))));
        assert_eq!(fs::read_dir(store.root.join("tmp")).unwrap().count(), 0);
        assert_eq!(store.receipts().unwrap().count(), 0);
        let prefix = fs::read_dir(store.root.join("objects"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let object = fs::read_dir(prefix)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert!(object.is_file());
        make_old(&object);
        let diagnostics = store.diagnose(Duration::from_secs(60), |_| {}).unwrap();
        assert_eq!(diagnostics.orphan_objects, 1);
    }

    #[test]
    fn identical_bodies_keep_distinct_fetch_provenance() {
        let dir = TestDir::new();
        let store = ArchiveStore::new(dir.0.join("archive"));
        let first = store
            .store_body(
                "polar",
                "exercises",
                metadata("1"),
                200,
                "t1",
                Cursor::new(b"same body".to_vec()),
            )
            .unwrap();
        let second = store
            .store_body(
                "polar",
                "exercises",
                metadata("2"),
                200,
                "t2",
                Cursor::new(b"same body".to_vec()),
            )
            .unwrap();

        assert_eq!(first.content_hash, second.content_hash);
        assert_ne!(first.receipt_id, second.receipt_id);
        assert_eq!(first.request.get("page").map(String::as_str), Some("1"));
        assert_eq!(second.request.get("page").map(String::as_str), Some("2"));
        assert_eq!(store.recover_sidecars().unwrap(), vec![first, second]);
    }

    #[test]
    fn verify_detects_object_corruption() {
        let dir = TestDir::new();
        let store = ArchiveStore::new(dir.0.join("archive"));
        let receipt = store
            .store_body(
                "polar",
                "sleep",
                metadata("1"),
                200,
                "t",
                Cursor::new(b"body".to_vec()),
            )
            .unwrap();
        let object = store.object_path(&receipt.content_hash);
        let mut bytes = fs::read(&object).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0x01;
        fs::write(object, bytes).unwrap();

        assert!(matches!(
            store.verify(&receipt),
            Err(ArchiveError::Io(_)) | Err(ArchiveError::HashMismatch { .. })
        ));
        let summary = verify_all(&store.root).unwrap();
        assert_eq!(
            summary,
            VerifySummary {
                verified: 0,
                failed: 1
            }
        );
    }

    #[test]
    fn verify_rejects_valid_gzip_with_the_wrong_content_hash() {
        let dir = TestDir::new();
        let store = ArchiveStore::new(dir.0.join("archive"));
        let receipt = store
            .store_body(
                "polar",
                "sleep",
                metadata("1"),
                200,
                "t",
                Cursor::new(b"expected payload".to_vec()),
            )
            .unwrap();

        let wrong_payload = b"different but valid gzip payload";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(wrong_payload).unwrap();
        fs::write(
            store.object_path(&receipt.content_hash),
            encoder.finish().unwrap(),
        )
        .unwrap();

        assert!(matches!(
            store.verify(&receipt),
            Err(ArchiveError::HashMismatch { .. })
        ));
    }

    #[test]
    fn corrupted_existing_content_addressed_object_does_not_leak_temp_body() {
        let dir = TestDir::new();
        let store = ArchiveStore::new(dir.0.join("archive"));
        let body = b"same payload for the original object";
        let receipt = store
            .store_body(
                "polar",
                "activity",
                metadata("1"),
                200,
                "t1",
                Cursor::new(body.as_slice()),
            )
            .unwrap();
        let object = store.object_path(&receipt.content_hash);
        fs::write(&object, b"not gzip").unwrap();

        assert!(matches!(
            store.store_body(
                "polar",
                "activity",
                metadata("2"),
                200,
                "t2",
                Cursor::new(body.as_slice()),
            ),
            Err(ArchiveError::Io(_))
        ));
        assert_eq!(fs::read_dir(store.root.join("tmp")).unwrap().count(), 0);
    }

    #[test]
    fn sidecars_recover_after_database_registration_gap() {
        let dir = TestDir::new();
        let archive = dir.0.join("archive");
        let store = ArchiveStore::new(archive);
        let receipt = store
            .store_body(
                "polar",
                "profile",
                metadata("self"),
                200,
                "t",
                Cursor::new(b"{}".to_vec()),
            )
            .unwrap();

        assert_eq!(store.recover_sidecars().unwrap(), vec![receipt.clone()]);
        assert_eq!(
            verify_all(&store.root).unwrap(),
            VerifySummary {
                verified: 1,
                failed: 0
            }
        );
    }

    #[test]
    fn diagnostics_stream_stale_parts_and_orphan_objects() {
        let dir = TestDir::new();
        let store = ArchiveStore::new(dir.0.join("archive"));
        let receipt = store
            .store_body(
                "polar",
                "activity",
                metadata("1"),
                200,
                "t",
                Cursor::new(b"orphan".to_vec()),
            )
            .unwrap();
        fs::remove_file(store.descriptor_path(&receipt.receipt_id)).unwrap();

        let part = store.root.join("tmp").join("abandoned.part");
        fs::write(&part, b"partial response").unwrap();
        make_old(&part);
        make_old(&store.object_path(&receipt.content_hash));

        let mut reported = Vec::new();
        let summary = store
            .diagnose(Duration::from_secs(60), |issue| {
                reported.push((issue.kind, issue.bytes));
            })
            .unwrap();
        assert_eq!(summary.stale_parts, 1);
        assert_eq!(summary.orphan_objects, 1);
        assert_eq!(reported.len(), 2);
        assert!(matches!(
            store.diagnose(Duration::ZERO, |_| {}),
            Err(ArchiveError::InvalidAgeThreshold)
        ));
    }

    #[test]
    fn cleanup_requires_preview_and_rejects_stale_plan() {
        let dir = TestDir::new();
        let store = ArchiveStore::new(dir.0.join("archive"));
        let part = store.root.join("tmp").join("old.part");
        fs::create_dir_all(part.parent().unwrap()).unwrap();
        fs::write(&part, b"old partial").unwrap();
        make_old(&part);

        let plan = store.preview_cleanup(Duration::from_secs(60)).unwrap();
        assert_eq!(plan.summary().stale_parts, 1);

        let added = store.root.join("tmp").join("added.part");
        fs::write(&added, b"another old partial").unwrap();
        make_old(&added);
        assert!(matches!(
            store.apply_cleanup(&plan),
            Err(ArchiveError::CleanupPlanStale)
        ));
        assert!(part.exists());
        assert!(added.exists());

        let fresh_plan = store.preview_cleanup(Duration::from_secs(60)).unwrap();
        let result = store.apply_cleanup(&fresh_plan).unwrap();
        assert_eq!(result.removed_parts, 2);
        assert!(!part.exists());
        assert!(!added.exists());
    }
}
