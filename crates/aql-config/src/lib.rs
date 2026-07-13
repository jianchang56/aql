//! Private, versioned storage for named AQL source profiles.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CONFIG_SCHEMA_VERSION: &str = "aql-config-v1";
pub const MAX_CONFIG_BYTES: u64 = 1024 * 1024;
pub const MAX_PROFILES: usize = 64;
pub const MAX_SOURCES_PER_PROFILE: usize = 16;

const CONFIG_FILE: &str = "config.toml";
const LOCK_FILE: &str = ".aql-config-v1.lock";
const OWNERSHIP_FILE: &str = "OWNED_BY_AQL";
const OWNERSHIP_MARKER: &[u8] = b"aql-config-owned-v1\n";
const TEMP_PREFIX: &str = ".config-building-";
const TEMP_SUFFIX: &str = ".toml";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("AQL config is missing")]
    Missing,
    #[error("AQL config root has unsafe permissions, type, or path components")]
    UnsafeRoot,
    #[error("AQL config root overlaps protected data")]
    RootOverlap,
    #[error("AQL config ownership marker is missing or invalid")]
    InvalidOwnershipMarker,
    #[error("AQL config contains unknown files")]
    UnknownFile,
    #[error("AQL config schema or contents are invalid")]
    InvalidConfig,
    #[error("AQL config schema is unsupported")]
    UnsupportedSchema,
    #[error("AQL profile name is invalid")]
    InvalidProfileName,
    #[error("AQL profile already exists")]
    ProfileExists,
    #[error("AQL profile is missing")]
    ProfileMissing,
    #[error("AQL profile source is invalid")]
    InvalidSource,
    #[error("AQL config writer is already active")]
    LockHeld,
    #[error("AQL config changed during the operation")]
    StateChanged,
    #[error("AQL config I/O failed")]
    Io(#[from] std::io::Error),
    #[error("AQL config platform operation failed")]
    Platform(#[from] rustix::io::Errno),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileSource {
    pub adapter_id: String,
    pub source_root: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub name: String,
    pub sources: Vec<ProfileSource>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    schema_version: String,
    profiles: Vec<Profile>,
}

impl Default for ConfigFile {
    fn default() -> Self {
        Self {
            schema_version: CONFIG_SCHEMA_VERSION.to_string(),
            profiles: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

pub struct ConfigStore {
    root: PathBuf,
    directory: Arc<OwnedFd>,
    identity: FileIdentity,
}

pub struct ConfigWriteLock {
    directory: Arc<OwnedFd>,
    file: File,
    released: bool,
}

impl ConfigStore {
    pub fn create(root: &Path, protected_roots: &[PathBuf]) -> Result<Self, ConfigError> {
        let prospective = prospective_root(root)?;
        validate_no_overlap(&prospective, protected_roots)?;
        let (root, directory, created) = open_or_create_private_root(&prospective)?;
        let stat = rustix::fs::fstat(&directory)?;
        validate_private_directory(&stat)?;
        if created {
            ensure_ownership_marker(&directory)?;
        } else {
            ensure_existing_ownership_marker(&directory)?;
        }
        let store = Self {
            root,
            directory: Arc::new(directory),
            identity: identity(&stat),
        };
        store.validate_known_entries(true)?;
        Ok(store)
    }

    pub fn open_existing(root: &Path, protected_roots: &[PathBuf]) -> Result<Self, ConfigError> {
        let root = canonicalize_no_symlink(root).map_err(|error| match error {
            ConfigError::Io(ref value) if value.kind() == std::io::ErrorKind::NotFound => {
                ConfigError::Missing
            }
            ConfigError::Platform(value) if value == rustix::io::Errno::NOENT => {
                ConfigError::Missing
            }
            other => other,
        })?;
        validate_no_overlap(&root, protected_roots)?;
        let directory = open_directory_chain(&root)?;
        let stat = rustix::fs::fstat(&directory)?;
        validate_private_directory(&stat)?;
        ensure_existing_ownership_marker(&directory)?;
        let store = Self {
            root,
            directory: Arc::new(directory),
            identity: identity(&stat),
        };
        store.validate_known_entries(false)?;
        Ok(store)
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn list(&self) -> Result<Vec<Profile>, ConfigError> {
        Ok(self.read_config()?.profiles)
    }

    pub fn get(&self, name: &str) -> Result<Profile, ConfigError> {
        validate_profile_name(name)?;
        self.read_config()?
            .profiles
            .into_iter()
            .find(|profile| profile.name == name)
            .ok_or(ConfigError::ProfileMissing)
    }

    pub fn get_validated(
        &self,
        name: &str,
        protected_roots: &[PathBuf],
    ) -> Result<Profile, ConfigError> {
        let profile = self.get(name)?;
        validate_profile(&profile, &self.root, protected_roots)?;
        Ok(profile)
    }

    pub fn acquire_write_lock(&self) -> Result<ConfigWriteLock, ConfigError> {
        self.validate_identity()?;
        let descriptor = rustix::fs::openat(
            &self.directory,
            LOCK_FILE,
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(0o600),
        )?;
        validate_private_file(&rustix::fs::fstat(&descriptor)?)?;
        if let Err(error) = rustix::fs::flock(
            &descriptor,
            rustix::fs::FlockOperation::NonBlockingLockExclusive,
        ) {
            if error == rustix::io::Errno::WOULDBLOCK {
                return Err(ConfigError::LockHeld);
            }
            return Err(error.into());
        }
        let mut file: File = descriptor.into();
        file.set_len(0)?;
        writeln!(file, "pid={}", std::process::id())?;
        file.sync_all()?;
        let lock = ConfigWriteLock {
            directory: self.directory.clone(),
            file,
            released: false,
        };
        self.remove_abandoned_temps()?;
        self.validate_known_entries(true)?;
        Ok(lock)
    }

    pub fn add(
        &self,
        profile: Profile,
        protected_roots: &[PathBuf],
        lock: ConfigWriteLock,
    ) -> Result<(), ConfigError> {
        self.validate_lock(&lock)?;
        validate_profile(&profile, &self.root, protected_roots)?;
        let mut config = self.read_config_or_default()?;
        if config.profiles.iter().any(|item| item.name == profile.name) {
            return Err(ConfigError::ProfileExists);
        }
        if config.profiles.len() >= MAX_PROFILES {
            return Err(ConfigError::InvalidConfig);
        }
        config.profiles.push(profile);
        config
            .profiles
            .sort_by(|left, right| left.name.cmp(&right.name));
        self.publish_config(&config)?;
        lock.release()?;
        Ok(())
    }

    pub fn remove(&self, name: &str, lock: ConfigWriteLock) -> Result<(), ConfigError> {
        self.validate_lock(&lock)?;
        validate_profile_name(name)?;
        let mut config = self.read_config()?;
        let original = config.profiles.len();
        config.profiles.retain(|profile| profile.name != name);
        if config.profiles.len() == original {
            return Err(ConfigError::ProfileMissing);
        }
        self.publish_config(&config)?;
        lock.release()?;
        Ok(())
    }

    fn validate_lock(&self, lock: &ConfigWriteLock) -> Result<(), ConfigError> {
        if lock.released || !Arc::ptr_eq(&self.directory, &lock.directory) {
            return Err(ConfigError::StateChanged);
        }
        self.validate_identity()
    }

    fn validate_identity(&self) -> Result<(), ConfigError> {
        let current = rustix::fs::fstat(&self.directory)?;
        validate_private_directory(&current)?;
        if identity(&current) != self.identity {
            return Err(ConfigError::StateChanged);
        }
        let path = open_directory_chain(&self.root)?;
        let path_stat = rustix::fs::fstat(&path)?;
        validate_private_directory(&path_stat)?;
        if identity(&path_stat) != self.identity {
            return Err(ConfigError::StateChanged);
        }
        Ok(())
    }

    fn read_config_or_default(&self) -> Result<ConfigFile, ConfigError> {
        match self.read_config() {
            Ok(config) => Ok(config),
            Err(ConfigError::Missing) => Ok(ConfigFile::default()),
            Err(error) => Err(error),
        }
    }

    fn read_config(&self) -> Result<ConfigFile, ConfigError> {
        self.validate_identity()?;
        let descriptor = rustix::fs::openat(
            &self.directory,
            CONFIG_FILE,
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|error| {
            if error == rustix::io::Errno::NOENT {
                ConfigError::Missing
            } else {
                ConfigError::Platform(error)
            }
        })?;
        let stat = rustix::fs::fstat(&descriptor)?;
        validate_private_file(&stat)?;
        if stat.st_size < 0 || stat.st_size as u64 > MAX_CONFIG_BYTES {
            return Err(ConfigError::InvalidConfig);
        }
        let mut bytes = Vec::with_capacity(stat.st_size as usize);
        let mut file: File = descriptor.into();
        Read::by_ref(&mut file)
            .take(MAX_CONFIG_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(ConfigError::InvalidConfig);
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| ConfigError::InvalidConfig)?;
        let config: ConfigFile = toml::from_str(text).map_err(|_| ConfigError::InvalidConfig)?;
        validate_config(&config)?;
        self.validate_identity()?;
        Ok(config)
    }

    fn publish_config(&self, config: &ConfigFile) -> Result<(), ConfigError> {
        validate_config(config)?;
        let encoded = toml::to_string(config).map_err(|_| ConfigError::InvalidConfig)?;
        if encoded.len() as u64 > MAX_CONFIG_BYTES {
            return Err(ConfigError::InvalidConfig);
        }
        self.validate_identity()?;
        let existing = match rustix::fs::statat(
            &self.directory,
            CONFIG_FILE,
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Ok(stat) => {
                validate_private_file(&stat)?;
                Some(identity(&stat))
            }
            Err(error) if error == rustix::io::Errno::NOENT => None,
            Err(error) => return Err(error.into()),
        };
        let temporary_name = format!("{TEMP_PREFIX}{:032x}{TEMP_SUFFIX}", rand::random::<u128>());
        let descriptor = rustix::fs::openat(
            &self.directory,
            temporary_name.as_str(),
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(0o600),
        )?;
        let mut guard = TemporaryFile {
            directory: self.directory.clone(),
            name: temporary_name.clone(),
            committed: false,
        };
        let mut file: File = descriptor.into();
        file.write_all(encoded.as_bytes())?;
        file.sync_all()?;
        self.validate_identity()?;
        match (
            existing,
            rustix::fs::statat(
                &self.directory,
                CONFIG_FILE,
                rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
            ),
        ) {
            (None, Err(error)) if error == rustix::io::Errno::NOENT => {
                rustix::fs::renameat_with(
                    &self.directory,
                    temporary_name.as_str(),
                    &self.directory,
                    CONFIG_FILE,
                    rustix::fs::RenameFlags::NOREPLACE,
                )?;
            }
            (Some(expected), Ok(actual)) => {
                validate_private_file(&actual)?;
                if identity(&actual) != expected {
                    return Err(ConfigError::StateChanged);
                }
                rustix::fs::renameat(
                    &self.directory,
                    temporary_name.as_str(),
                    &self.directory,
                    CONFIG_FILE,
                )?;
            }
            _ => return Err(ConfigError::StateChanged),
        }
        rustix::fs::fsync(&self.directory)?;
        guard.committed = true;
        Ok(())
    }

    fn remove_abandoned_temps(&self) -> Result<(), ConfigError> {
        self.validate_identity()?;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(ConfigError::UnknownFile);
            };
            if is_temporary_name(name) {
                let stat = rustix::fs::statat(
                    &self.directory,
                    name,
                    rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
                )?;
                validate_private_file(&stat)?;
                rustix::fs::unlinkat(&self.directory, name, rustix::fs::AtFlags::empty())?;
            }
        }
        rustix::fs::fsync(&self.directory)?;
        self.validate_identity()
    }

    fn validate_known_entries(&self, allow_missing_config: bool) -> Result<(), ConfigError> {
        self.validate_identity()?;
        let mut config_seen = false;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(ConfigError::UnknownFile);
            };
            if !matches!(name, CONFIG_FILE | LOCK_FILE | OWNERSHIP_FILE) && !is_temporary_name(name)
            {
                return Err(ConfigError::UnknownFile);
            }
            let stat =
                rustix::fs::statat(&self.directory, name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)?;
            validate_private_file(&stat)?;
            config_seen |= name == CONFIG_FILE;
        }
        self.validate_identity()?;
        if !allow_missing_config && !config_seen {
            return Err(ConfigError::Missing);
        }
        Ok(())
    }
}

impl ConfigWriteLock {
    pub fn release(mut self) -> Result<(), ConfigError> {
        self.file.sync_all()?;
        rustix::fs::flock(&self.file, rustix::fs::FlockOperation::Unlock)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for ConfigWriteLock {
    fn drop(&mut self) {
        if !self.released {
            let _ = rustix::fs::flock(&self.file, rustix::fs::FlockOperation::Unlock);
        }
    }
}

struct TemporaryFile {
    directory: Arc<OwnedFd>,
    name: String,
    committed: bool,
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = rustix::fs::unlinkat(
                &self.directory,
                self.name.as_str(),
                rustix::fs::AtFlags::empty(),
            );
        }
    }
}

fn validate_config(config: &ConfigFile) -> Result<(), ConfigError> {
    if config.schema_version != CONFIG_SCHEMA_VERSION {
        return Err(ConfigError::UnsupportedSchema);
    }
    if config.profiles.len() > MAX_PROFILES {
        return Err(ConfigError::InvalidConfig);
    }
    let mut names = std::collections::BTreeSet::new();
    for profile in &config.profiles {
        if !names.insert(profile.name.as_str()) {
            return Err(ConfigError::InvalidConfig);
        }
        validate_profile_shape(profile)?;
    }
    Ok(())
}

fn validate_profile(
    profile: &Profile,
    config_root: &Path,
    protected_roots: &[PathBuf],
) -> Result<(), ConfigError> {
    validate_profile_shape(profile)?;
    let mut roots: Vec<PathBuf> = Vec::with_capacity(profile.sources.len());
    for source in &profile.sources {
        let canonical = canonicalize_no_symlink(&source.source_root)?;
        if paths_overlap(&canonical, config_root)
            || protected_roots
                .iter()
                .any(|root| paths_overlap(&canonical, root))
            || roots.iter().any(|root| paths_overlap(&canonical, root))
        {
            return Err(ConfigError::RootOverlap);
        }
        roots.push(canonical);
    }
    Ok(())
}

fn validate_profile_shape(profile: &Profile) -> Result<(), ConfigError> {
    validate_profile_name(&profile.name)?;
    if profile.sources.is_empty() || profile.sources.len() > MAX_SOURCES_PER_PROFILE {
        return Err(ConfigError::InvalidSource);
    }
    for source in &profile.sources {
        if !matches!(
            source.adapter_id.as_str(),
            "claude-code" | "codex" | "kimi-code" | "opencode"
        ) || !source.source_root.is_absolute()
        {
            return Err(ConfigError::InvalidSource);
        }
    }
    Ok(())
}

pub fn validate_profile_name(name: &str) -> Result<(), ConfigError> {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 64
        || !bytes[0].is_ascii_lowercase()
        || !bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
    {
        return Err(ConfigError::InvalidProfileName);
    }
    Ok(())
}

fn validate_no_overlap(root: &Path, protected_roots: &[PathBuf]) -> Result<(), ConfigError> {
    if protected_roots
        .iter()
        .any(|other| paths_overlap(root, other))
    {
        Err(ConfigError::RootOverlap)
    } else {
        Ok(())
    }
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn prospective_root(path: &Path) -> Result<PathBuf, ConfigError> {
    if !path.is_absolute() {
        return Err(ConfigError::UnsafeRoot);
    }
    if path.exists() {
        return canonicalize_no_symlink(path);
    }
    let parent = path.parent().ok_or(ConfigError::UnsafeRoot)?;
    let parent = canonicalize_no_symlink(parent)?;
    let name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or(ConfigError::UnsafeRoot)?;
    Ok(parent.join(name))
}

fn canonicalize_no_symlink(path: &Path) -> Result<PathBuf, ConfigError> {
    drop(open_directory_chain(path)?);
    let canonical = fs::canonicalize(path)?;
    Ok(canonical)
}

fn open_or_create_private_root(path: &Path) -> Result<(PathBuf, OwnedFd, bool), ConfigError> {
    let parent = path.parent().ok_or(ConfigError::UnsafeRoot)?;
    let name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or(ConfigError::UnsafeRoot)?;
    let parent_directory = open_directory_chain(parent)?;
    let created = match rustix::fs::mkdirat(
        &parent_directory,
        name,
        rustix::fs::Mode::from_raw_mode(0o700),
    ) {
        Ok(()) => true,
        Err(error) if error == rustix::io::Errno::EXIST => false,
        Err(error) => return Err(error.into()),
    };
    let directory = rustix::fs::openat(
        &parent_directory,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    validate_private_directory(&rustix::fs::fstat(&directory)?)?;
    Ok((path.to_path_buf(), directory, created))
}

fn open_directory_chain(path: &Path) -> Result<OwnedFd, ConfigError> {
    if !path.is_absolute() {
        return Err(ConfigError::UnsafeRoot);
    }
    let mut directory = rustix::fs::openat(
        rustix::fs::CWD,
        "/",
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                directory = rustix::fs::openat(
                    &directory,
                    name,
                    rustix::fs::OFlags::RDONLY
                        | rustix::fs::OFlags::DIRECTORY
                        | rustix::fs::OFlags::NOFOLLOW
                        | rustix::fs::OFlags::CLOEXEC,
                    rustix::fs::Mode::empty(),
                )?;
            }
            _ => return Err(ConfigError::UnsafeRoot),
        }
    }
    Ok(directory)
}

fn ensure_ownership_marker(directory: &OwnedFd) -> Result<(), ConfigError> {
    match rustix::fs::openat(
        directory,
        OWNERSHIP_FILE,
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::EXCL
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::from_raw_mode(0o600),
    ) {
        Ok(descriptor) => {
            let mut file: File = descriptor.into();
            file.write_all(OWNERSHIP_MARKER)?;
            file.sync_all()?;
            rustix::fs::fsync(directory)?;
        }
        Err(error) if error == rustix::io::Errno::EXIST => {
            ensure_existing_ownership_marker(directory)?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn ensure_existing_ownership_marker(directory: &OwnedFd) -> Result<(), ConfigError> {
    let descriptor = rustix::fs::openat(
        directory,
        OWNERSHIP_FILE,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| {
        if error == rustix::io::Errno::NOENT {
            ConfigError::InvalidOwnershipMarker
        } else {
            ConfigError::Platform(error)
        }
    })?;
    validate_private_file(&rustix::fs::fstat(&descriptor)?)?;
    let mut marker = Vec::new();
    let mut file: File = descriptor.into();
    Read::by_ref(&mut file)
        .take(OWNERSHIP_MARKER.len() as u64 + 1)
        .read_to_end(&mut marker)?;
    if marker != OWNERSHIP_MARKER {
        return Err(ConfigError::InvalidOwnershipMarker);
    }
    Ok(())
}

fn validate_private_directory(stat: &rustix::fs::Stat) -> Result<(), ConfigError> {
    if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::Directory
        || stat.st_mode & 0o077 != 0
    {
        return Err(ConfigError::UnsafeRoot);
    }
    Ok(())
}

fn validate_private_file(stat: &rustix::fs::Stat) -> Result<(), ConfigError> {
    if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::RegularFile
        || stat.st_mode & 0o077 != 0
    {
        return Err(ConfigError::UnsafeRoot);
    }
    Ok(())
}

fn identity(stat: &rustix::fs::Stat) -> FileIdentity {
    FileIdentity {
        device: stat.st_dev as u64,
        inode: stat.st_ino,
    }
}

fn is_temporary_name(name: &str) -> bool {
    name.strip_prefix(TEMP_PREFIX)
        .and_then(|value| value.strip_suffix(TEMP_SUFFIX))
        .is_some_and(|value| {
            value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{PermissionsExt, symlink};

    use super::*;

    fn root() -> PathBuf {
        let temporary = fs::canonicalize(std::env::temp_dir()).expect("temp root canonicalizes");
        let root = temporary.join(format!(
            "aql-config-test-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).expect("test parent is created");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("test parent is private");
        root
    }

    fn source(parent: &Path, name: &str) -> PathBuf {
        let source = parent.join(name);
        fs::create_dir(&source).expect("synthetic source is created");
        source
    }

    fn profile(source_root: PathBuf) -> Profile {
        Profile {
            name: "daily".to_string(),
            sources: vec![ProfileSource {
                adapter_id: "codex".to_string(),
                source_root,
            }],
        }
    }

    #[test]
    fn profile_name_and_shape_are_exact() {
        for valid in ["a", "daily", "a-1_b"] {
            assert!(validate_profile_name(valid).is_ok());
        }
        for invalid in ["", "A", "1a", "a/b", "a b", "é"] {
            assert!(validate_profile_name(invalid).is_err());
        }
        let too_many = Profile {
            name: "daily".to_string(),
            sources: (0..=MAX_SOURCES_PER_PROFILE)
                .map(|index| ProfileSource {
                    adapter_id: "codex".to_string(),
                    source_root: PathBuf::from(format!("/synthetic/{index}")),
                })
                .collect(),
        };
        assert!(matches!(
            validate_profile_shape(&too_many),
            Err(ConfigError::InvalidSource)
        ));
        let unknown_adapter = Profile {
            name: "daily".to_string(),
            sources: vec![ProfileSource {
                adapter_id: "unknown".to_string(),
                source_root: PathBuf::from("/synthetic"),
            }],
        };
        assert!(matches!(
            validate_profile_shape(&unknown_adapter),
            Err(ConfigError::InvalidSource)
        ));
        let claude = Profile {
            name: "claude".to_string(),
            sources: vec![ProfileSource {
                adapter_id: "claude-code".to_string(),
                source_root: PathBuf::from("/synthetic/claude"),
            }],
        };
        assert!(validate_profile_shape(&claude).is_ok());
    }

    #[test]
    fn existing_unowned_or_symlinked_config_root_is_never_claimed() {
        let root = root();
        let unowned = root.join("unowned");
        fs::create_dir(&unowned).expect("unowned directory creates");
        fs::set_permissions(&unowned, fs::Permissions::from_mode(0o700))
            .expect("unowned directory is private");
        assert!(matches!(
            ConfigStore::create(&unowned, &[]),
            Err(ConfigError::InvalidOwnershipMarker)
        ));
        assert!(!unowned.join(OWNERSHIP_FILE).exists());

        let real_parent = root.join("real-parent");
        fs::create_dir(&real_parent).expect("real parent creates");
        fs::set_permissions(&real_parent, fs::Permissions::from_mode(0o700))
            .expect("real parent is private");
        let linked_parent = root.join("linked-parent");
        symlink(&real_parent, &linked_parent).expect("parent symlink creates");
        assert!(ConfigStore::create(&linked_parent.join("config"), &[]).is_err());
        assert!(!real_parent.join("config").exists());
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn store_is_private_atomic_and_deterministic() {
        let root = root();
        let source_root = source(&root, "source");
        let config_root = root.join("config");
        let store = ConfigStore::create(&config_root, &[]).expect("config store creates");
        let lock = store.acquire_write_lock().expect("writer locks");
        store
            .add(profile(source_root), &[], lock)
            .expect("profile is added");
        assert_eq!(store.list().expect("profiles list").len(), 1);
        let bytes = fs::read(config_root.join(CONFIG_FILE)).expect("config is readable");
        assert!(bytes.starts_with(b"schema_version = \"aql-config-v1\""));
        assert_eq!(
            fs::metadata(config_root.join(CONFIG_FILE))
                .expect("config metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let lock = store.acquire_write_lock().expect("writer locks");
        store.remove("daily", lock).expect("profile removes");
        assert!(store.list().expect("profiles list").is_empty());
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn store_rejects_overlap_duplicate_unknown_schema_and_limits() {
        let root = root();
        let source_root = source(&root, "source");
        let protected = source(&root, "protected");
        let config_root = root.join("config");
        let store = ConfigStore::create(&config_root, std::slice::from_ref(&protected))
            .expect("config store creates");
        let lock = store.acquire_write_lock().expect("writer locks");
        assert!(matches!(
            store.add(profile(protected.clone()), &[protected], lock),
            Err(ConfigError::RootOverlap)
        ));
        let lock = store.acquire_write_lock().expect("writer locks");
        store
            .add(profile(source_root.clone()), &[], lock)
            .expect("profile adds");
        let lock = store.acquire_write_lock().expect("writer locks");
        assert!(matches!(
            store.add(profile(source_root), &[], lock),
            Err(ConfigError::ProfileExists)
        ));
        fs::write(
            config_root.join(CONFIG_FILE),
            "schema_version = \"future\"\nprofiles = []\n",
        )
        .expect("future config writes");
        fs::set_permissions(
            config_root.join(CONFIG_FILE),
            fs::Permissions::from_mode(0o600),
        )
        .expect("future config is private");
        assert!(matches!(store.list(), Err(ConfigError::UnsupportedSchema)));
        fs::write(config_root.join("unknown"), b"user file").expect("unknown file writes");
        assert!(matches!(
            ConfigStore::open_existing(&config_root, &[]),
            Err(ConfigError::UnknownFile)
        ));
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn symlink_permissions_lock_and_root_replacement_fail_closed() {
        let root = root();
        let config_root = root.join("config");
        let store = ConfigStore::create(&config_root, &[]).expect("config store creates");
        let first = store.acquire_write_lock().expect("first writer locks");
        assert!(matches!(
            store.acquire_write_lock(),
            Err(ConfigError::LockHeld)
        ));
        first.release().expect("first writer unlocks");

        let outside = root.join("outside");
        fs::write(&outside, b"outside").expect("outside file writes");
        symlink(&outside, config_root.join(CONFIG_FILE)).expect("config symlink creates");
        assert!(store.list().is_err());
        fs::remove_file(config_root.join(CONFIG_FILE)).expect("symlink removes");

        fs::set_permissions(&config_root, fs::Permissions::from_mode(0o777))
            .expect("permissions change");
        assert!(matches!(store.list(), Err(ConfigError::UnsafeRoot)));
        fs::set_permissions(&config_root, fs::Permissions::from_mode(0o700))
            .expect("permissions restore");

        let moved = root.join("moved");
        fs::rename(&config_root, &moved).expect("config root moves");
        fs::create_dir(&config_root).expect("replacement root creates");
        fs::set_permissions(&config_root, fs::Permissions::from_mode(0o700))
            .expect("replacement root is private");
        assert!(matches!(store.list(), Err(ConfigError::StateChanged)));
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn oversized_and_abandoned_temporary_files_fail_or_recover_safely() {
        let root = root();
        let config_root = root.join("config");
        let store = ConfigStore::create(&config_root, &[]).expect("config store creates");
        let oversized = File::create(config_root.join(CONFIG_FILE)).expect("config creates");
        fs::set_permissions(
            config_root.join(CONFIG_FILE),
            fs::Permissions::from_mode(0o600),
        )
        .expect("config is private");
        oversized
            .set_len(MAX_CONFIG_BYTES + 1)
            .expect("config grows");
        assert!(matches!(store.list(), Err(ConfigError::InvalidConfig)));
        fs::remove_file(config_root.join(CONFIG_FILE)).expect("oversized config removes");
        let abandoned = config_root.join(format!("{TEMP_PREFIX}{:032x}{TEMP_SUFFIX}", 1));
        fs::write(&abandoned, b"partial").expect("abandoned temp writes");
        fs::set_permissions(&abandoned, fs::Permissions::from_mode(0o600))
            .expect("abandoned temp is private");
        let lock = store.acquire_write_lock().expect("writer recovers temp");
        assert!(!abandoned.exists());
        lock.release().expect("writer unlocks");
        fs::remove_dir_all(root).expect("test root is removed");
    }
}
