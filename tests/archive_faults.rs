use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use rust_polar::archive::{ArchiveError, ArchiveStore};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

struct InterruptedBody {
    sent_prefix: bool,
}

impl Read for InterruptedBody {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.sent_prefix {
            return Err(io::Error::other("simulated interrupted response"));
        }
        self.sent_prefix = true;
        let prefix = b"partial response body";
        output[..prefix.len()].copy_from_slice(prefix);
        Ok(prefix.len())
    }
}

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rust-polar-archive-fault-integration-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
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
fn interrupted_stream_keeps_existing_archive_and_discards_partial_fetch() {
    let dir = TestDir::new();
    let archive = dir.0.join("archive");
    let store = ArchiveStore::new(archive.clone()).with_min_free_bytes(0);
    let existing = store
        .store_body(
            "polar",
            "profile",
            BTreeMap::new(),
            200,
            "before failure",
            io::Cursor::new(b"previous complete response".to_vec()),
        )
        .unwrap();

    let error = store.store_body(
        "polar",
        "activity",
        BTreeMap::new(),
        200,
        "interrupted",
        InterruptedBody { sent_prefix: false },
    );
    assert!(matches!(error, Err(ArchiveError::Io(_))));

    assert_eq!(store.recover_sidecars().unwrap(), vec![existing.clone()]);
    store.verify(&existing).unwrap();
    assert_eq!(fs::read_dir(archive.join("tmp")).unwrap().count(), 0);
    assert_eq!(fs::read_dir(archive.join("receipts")).unwrap().count(), 1);
    let object_count = fs::read_dir(archive.join("objects"))
        .unwrap()
        .map(|prefix| fs::read_dir(prefix.unwrap().path()).unwrap().count())
        .sum::<usize>();
    assert_eq!(object_count, 1);
}
