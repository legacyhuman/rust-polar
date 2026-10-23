//! Private, atomic on-disk storage for the Polar OAuth client and token.
//!
//! The file is intentionally separate from redb and response archives: secrets
//! must never be indexed or included in ordinary archive backups/logs.

use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const FORMAT_VERSION: u32 = 1;
const MAX_CREDENTIAL_BYTES: u64 = 64 * 1024;

#[derive(Clone, Serialize, Deserialize)]
pub struct PolarCredentials {
    format_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub token_type: Option<String>,
    pub expires_in: Option<u64>,
    pub user_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_id: Option<String>,
    pub updated_at: String,
}

pub struct NewPolarCredentials {
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub token_type: Option<String>,
    pub expires_in: Option<u64>,
    pub user_id: String,
    pub member_id: Option<String>,
    pub updated_at: String,
}

impl std::fmt::Debug for PolarCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolarCredentials")
            .field("format_version", &self.format_version)
            .field("client_id", &self.client_id.as_ref().map(|_| "[redacted]"))
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "[redacted]"),
            )
            .field("access_token", &"[redacted]")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[redacted]"),
            )
            .field("token_type", &self.token_type)
            .field("expires_in", &self.expires_in)
            .field("user_id", &self.user_id)
            .field("member_id", &self.member_id)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

impl PolarCredentials {
    pub fn new(input: NewPolarCredentials) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            client_id: input.client_id,
            client_secret: input.client_secret,
            access_token: input.access_token,
            refresh_token: input.refresh_token,
            token_type: input.token_type,
            expires_in: input.expires_in,
            user_id: input.user_id,
            member_id: input.member_id,
            updated_at: input.updated_at,
        }
    }

    pub fn load(data_dir: &Path) -> Result<Option<Self>, CredentialError> {
        let path = Self::path(data_dir);
        let file = match OpenOptions::new().read(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > MAX_CREDENTIAL_BYTES {
            return Err(CredentialError::InvalidFile);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(CredentialError::PermissionsTooOpen);
            }
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_CREDENTIAL_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_CREDENTIAL_BYTES {
            return Err(CredentialError::InvalidFile);
        }
        let credentials: Self = serde_json::from_slice(&bytes)?;
        if credentials.format_version != FORMAT_VERSION
            || credentials.access_token.is_empty()
            || credentials.user_id.is_empty()
            || credentials.client_id.is_some() != credentials.client_secret.is_some()
        {
            return Err(CredentialError::InvalidFile);
        }
        Ok(Some(credentials))
    }

    pub fn save(&self, data_dir: &Path) -> Result<(), CredentialError> {
        let directory = data_dir.join("credentials");
        fs::create_dir_all(&directory)?;
        restrict_directory(&directory)?;
        let final_path = Self::path(data_dir);
        let temp_path = directory.join(format!("polar-{}.part", std::process::id()));
        let bytes = serde_json::to_vec(self)?;
        if bytes.len() as u64 > MAX_CREDENTIAL_BYTES {
            return Err(CredentialError::InvalidFile);
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        restrict_open_options(&mut options);
        let mut file = options.open(&temp_path)?;
        let result = (|| {
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temp_path, &final_path)?;
            sync_directory(&directory)?;
            Ok::<_, std::io::Error>(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        result?;
        restrict_file(&final_path)?;
        Ok(())
    }

    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("credentials").join("polar.json")
    }
}

#[cfg(unix)]
fn restrict_directory(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn restrict_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_open_options(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn restrict_open_options(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn restrict_file(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn restrict_file(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    fs::File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[derive(Debug)]
pub enum CredentialError {
    Io(std::io::Error),
    Json(serde_json::Error),
    InvalidFile,
    PermissionsTooOpen,
}

impl std::fmt::Display for CredentialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "credential file I/O error: {error}"),
            Self::Json(error) => write!(f, "credential file JSON error: {error}"),
            Self::InvalidFile => {
                f.write_str("credential file is invalid or exceeds the size limit")
            }
            Self::PermissionsTooOpen => f.write_str("credential file permissions must be 0600"),
        }
    }
}

impl std::error::Error for CredentialError {}
impl From<std::io::Error> for CredentialError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<serde_json::Error> for CredentialError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_atomic_private_and_redacted() {
        let root =
            std::env::temp_dir().join(format!("rust-polar-credentials-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let credentials = PolarCredentials::new(NewPolarCredentials {
            client_id: Some("client-id".into()),
            client_secret: Some("client-secret-value".into()),
            access_token: "access-token-value".into(),
            refresh_token: None,
            token_type: Some("Bearer".into()),
            expires_in: Some(3600),
            user_id: "12345".into(),
            member_id: Some("local-member".into()),
            updated_at: "2026-10-21T12:00:00Z".into(),
        });
        credentials.save(&root).unwrap();
        let debug = format!("{credentials:?}");
        assert!(!debug.contains("client-secret-value"));
        assert!(!debug.contains("access-token-value"));
        let loaded = PolarCredentials::load(&root).unwrap().unwrap();
        assert_eq!(loaded.user_id, "12345");
        assert_eq!(loaded.refresh_token, None);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(PolarCredentials::path(&root))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn token_import_credentials_load_without_oauth_client_secrets() {
        let root = std::env::temp_dir().join(format!(
            "rust-polar-imported-credentials-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let credentials = PolarCredentials::new(NewPolarCredentials {
            client_id: None,
            client_secret: None,
            access_token: "imported-access-token".into(),
            refresh_token: None,
            token_type: Some("Bearer".into()),
            expires_in: None,
            user_id: "provider-user-42".into(),
            member_id: None,
            updated_at: "2026-10-21T12:00:00Z".into(),
        });

        credentials.save(&root).unwrap();
        let loaded = PolarCredentials::load(&root).unwrap().unwrap();
        assert_eq!(loaded.client_id, None);
        assert_eq!(loaded.client_secret, None);
        assert_eq!(loaded.user_id, "provider-user-42");
        assert_eq!(loaded.member_id, None);
        let stored = fs::read_to_string(PolarCredentials::path(&root)).unwrap();
        assert!(!stored.contains("client_id"));
        assert!(!stored.contains("client_secret"));
        assert!(!stored.contains("member_id"));

        let _ = fs::remove_dir_all(root);
    }
}
