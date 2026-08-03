//! Deterministic AQL archive, verification, installation, and uninstall tooling.
//!
//! Release archives contain an exact allowlist, canonical manifest, fixed
//! ownership metadata, deterministic gzip/tar headers, and no source-adjacent
//! writes. Verification always precedes installation.

#![deny(missing_docs)]

use aql_fs::{FileIdentity, identity};
use cap_std::fs::{Dir, Metadata as CapMetadata, OpenOptions as CapOpenOptions};
use flate2::{Compression, Decompress, FlushDecompress, GzBuilder, Status};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use tar::{Archive, Builder, EntryType, Header};

/// Public package name embedded in release manifests and archive roots.
pub const PACKAGE: &str = "aql";
const BUILD_PACKAGE: &str = "aql";
/// Exact release payload and required Unix mode for each installed file.
pub const PAYLOAD: [(&str, u32); 9] = [
    ("bin/aql", 0o755),
    ("share/bash-completion/completions/aql", 0o644),
    ("share/doc/aql/LICENSE", 0o644),
    ("share/doc/aql/README.md", 0o644),
    ("share/doc/aql/compatibility.md", 0o644),
    ("share/doc/aql/privacy-threat-model.md", 0o644),
    ("share/fish/vendor_completions.d/aql.fish", 0o644),
    ("share/man/man1/aql.1", 0o644),
    ("share/zsh/site-functions/_aql", 0o644),
];
const MAX_ARCHIVE: u64 = 256 * 1024 * 1024;
const MAX_EXPANDED: u64 = 512 * 1024 * 1024;

/// Result type used by release operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Sanitized release-tool error suitable for CLI display.
#[derive(Debug)]
pub struct Error(String);
impl Error {
    /// Creates a release error from a non-sensitive message.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self(value.to_string())
    }
}
impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self(value.to_string())
    }
}
fn fail<T>(value: impl Into<String>) -> Result<T> {
    Err(Error::new(value))
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct Metadata {
    canonical_schema: String,
    config_schema: String,
    package: String,
    target: String,
    version: String,
}
#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct Entry {
    mode: String,
    path: String,
    sha256: String,
    size: usize,
}
#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct Manifest {
    build_metadata: Metadata,
    entries: Vec<Entry>,
    package: String,
    schema_version: String,
    source_date_epoch: u32,
    target: String,
    version: String,
}
#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct Uninstall {
    files: Vec<String>,
    schema_version: String,
    target: String,
    version: String,
}

/// Verified archive digest, payload, and canonical manifest.
pub struct Verified {
    /// Lowercase SHA-256 digest of the complete archive.
    pub digest: String,
    /// Verified payload bytes keyed by allowlisted relative path.
    pub payload: BTreeMap<String, Vec<u8>>,
    /// Canonical serialized release manifest.
    pub manifest: Vec<u8>,
}

/// Validates a numeric `MAJOR.MINOR.PATCH` release version.
pub fn validate_version(value: &str) -> Result<()> {
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
    {
        Ok(())
    } else {
        fail("version must be numeric MAJOR.MINOR.PATCH")
    }
}
/// Validates one of AQL's four supported release target names.
pub fn validate_target(value: &str) -> Result<()> {
    if matches!(
        value,
        "aarch64-linux" | "aarch64-macos" | "x86_64-linux" | "x86_64-macos"
    ) {
        Ok(())
    } else {
        fail("unsupported release target")
    }
}
fn metadata(version: &str, target: &str) -> Metadata {
    Metadata {
        canonical_schema: "aql-canonical-v0".into(),
        config_schema: "aql-databases-v1".into(),
        package: BUILD_PACKAGE.into(),
        target: target.into(),
        version: version.into(),
    }
}
fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut out = serde_json::to_vec(value)?;
    out.push(b'\n');
    Ok(out)
}
fn manifest(
    version: &str,
    target: &str,
    epoch: u32,
    payload: &BTreeMap<String, Vec<u8>>,
) -> Result<Manifest> {
    if payload.len() != PAYLOAD.len() || PAYLOAD.iter().any(|(p, _)| !payload.contains_key(*p)) {
        return fail("payload allowlist mismatch");
    }
    Ok(Manifest {
        build_metadata: metadata(version, target),
        entries: PAYLOAD
            .iter()
            .map(|(p, m)| {
                let bytes = &payload[*p];
                Entry {
                    mode: format!("{m:04o}"),
                    path: (*p).into(),
                    sha256: hex::encode(Sha256::digest(bytes)),
                    size: bytes.len(),
                }
            })
            .collect(),
        package: PACKAGE.into(),
        schema_version: "aql-release-v1".into(),
        source_date_epoch: epoch,
        target: target.into(),
        version: version.into(),
    })
}
fn local(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let text = path.to_string_lossy();
    if text == "-" || text.contains("://") {
        return fail("input must be a local file");
    }
    let mut options = CapOpenOptions::new();
    options.read(true);
    let mut file = aql_fs::open_ambient_file(path, options)?.into_std();
    let meta = file.metadata()?;
    if !meta.is_file() || meta.len() > limit {
        return fail("input is not a bounded regular file");
    }
    let mut out = Vec::new();
    Read::by_ref(&mut file)
        .take(limit + 1)
        .read_to_end(&mut out)?;
    if out.len() as u64 != meta.len() {
        return fail("input changed while reading");
    }
    Ok(out)
}
fn binary(binary: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let out = Command::new(binary)
        .args(args)
        .stdin(Stdio::null())
        .output()?;
    if !out.status.success() {
        return fail(format!(
            "binary command failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(out.stdout)
}

/// Builds a deterministic `tar.gz` release archive in memory.
///
/// The binary's structured version metadata must match `version` and `target`.
/// Inputs are restricted to the fixed [`PAYLOAD`] allowlist.
pub fn build(
    repository: &Path,
    binary_path: &Path,
    version: &str,
    target: &str,
    epoch: u32,
) -> Result<Vec<u8>> {
    validate_version(version)?;
    validate_target(target)?;
    let binary_path = binary_path.canonicalize()?;
    let actual: Metadata =
        serde_json::from_slice(&binary(&binary_path, &["version", "--output", "json"])?)?;
    if actual != metadata(version, target) {
        return fail("binary metadata does not match release");
    }
    let payload = BTreeMap::from([
        ("bin/aql".into(), local(&binary_path, MAX_ARCHIVE)?),
        (
            "share/bash-completion/completions/aql".into(),
            binary(&binary_path, &["completions", "bash"])?,
        ),
        (
            "share/doc/aql/LICENSE".into(),
            local(&repository.join("LICENSE"), MAX_ARCHIVE)?,
        ),
        (
            "share/doc/aql/README.md".into(),
            local(&repository.join("README.md"), MAX_ARCHIVE)?,
        ),
        (
            "share/doc/aql/compatibility.md".into(),
            local(&repository.join("docs/compatibility.md"), MAX_ARCHIVE)?,
        ),
        (
            "share/doc/aql/privacy-threat-model.md".into(),
            local(
                &repository.join("docs/privacy-threat-model.md"),
                MAX_ARCHIVE,
            )?,
        ),
        (
            "share/fish/vendor_completions.d/aql.fish".into(),
            binary(&binary_path, &["completions", "fish"])?,
        ),
        (
            "share/man/man1/aql.1".into(),
            binary(&binary_path, &["man"])?,
        ),
        (
            "share/zsh/site-functions/_aql".into(),
            binary(&binary_path, &["completions", "zsh"])?,
        ),
    ]);
    let mut members = payload;
    members.insert(
        "manifest.json".into(),
        canonical(&manifest(version, target, epoch, &members)?)?,
    );
    let top = format!("{PACKAGE}-{version}-{target}");
    let mut tar = Vec::new();
    {
        let mut builder = Builder::new(&mut tar);
        for (path, bytes) in members {
            let mut header = Header::new_ustar();
            header.set_size(bytes.len() as u64);
            header.set_mode(if path == "manifest.json" {
                0o644
            } else {
                PAYLOAD
                    .iter()
                    .find(|(p, _)| *p == path)
                    .map(|v| v.1)
                    .ok_or_else(|| Error::new("unexpected payload"))?
            });
            header.set_mtime(epoch.into());
            header.set_uid(0);
            header.set_gid(0);
            header.set_entry_type(EntryType::Regular);
            header.set_username("")?;
            header.set_groupname("")?;
            header.set_cksum();
            builder.append_data(&mut header, format!("{top}/{path}"), bytes.as_slice())?;
        }
        builder.finish()?;
    }
    let mut gzip = GzBuilder::new()
        .mtime(epoch)
        .write(Vec::new(), Compression::best());
    gzip.write_all(&tar)?;
    Ok(gzip.finish()?)
}

/// Verifies an archive's digest, encoding, manifest, paths, modes, and payload.
///
/// No archive content is returned unless every entry passes validation.
pub fn verify(path: &Path, expected: &str, version: &str, target: &str) -> Result<Verified> {
    validate_version(version)?;
    validate_target(target)?;
    if expected.len() != 64
        || !expected
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return fail("invalid SHA-256");
    }
    let bytes = local(path, MAX_ARCHIVE)?;
    let digest = hex::encode(Sha256::digest(&bytes));
    if digest != expected {
        return fail("SHA-256 mismatch");
    }
    if bytes.len() < 10 || bytes[..2] != [0x1f, 0x8b] || bytes[3] & 0x1e != 0 {
        return fail("invalid deterministic gzip header");
    }
    let epoch = u32::from_le_bytes(
        bytes[4..8]
            .try_into()
            .map_err(|_| Error::new("truncated gzip"))?,
    );
    let tar_bytes = decode_gzip(&bytes)?;
    let top = format!("{PACKAGE}-{version}-{target}");
    let mut expected_paths = PAYLOAD
        .iter()
        .map(|(p, _)| format!("{top}/{p}"))
        .chain([format!("{top}/manifest.json")])
        .collect::<Vec<_>>();
    expected_paths.sort();
    let mut paths = Vec::new();
    let mut payload = BTreeMap::new();
    let mut manifest_bytes = None;
    let mut archive = Archive::new(tar_bytes.as_slice());
    for item in archive.entries()? {
        let mut item = item?;
        let header = item.header();
        let path = item.path()?.into_owned();
        let text = path
            .to_str()
            .ok_or_else(|| Error::new("non-UTF8 archive path"))?
            .to_owned();
        paths.push(text.clone());
        if path.is_absolute()
            || path.components().any(|c| c == Component::ParentDir)
            || header.entry_type() != EntryType::Regular
            || header.uid()? != 0
            || header.gid()? != 0
            || header.mtime()? != u64::from(epoch)
            || header
                .username()
                .map_err(|_| Error::new("archive username is not UTF-8"))?
                .is_some_and(|value| !value.is_empty())
            || header
                .groupname()
                .map_err(|_| Error::new("archive group name is not UTF-8"))?
                .is_some_and(|value| !value.is_empty())
        {
            return fail("unsafe archive metadata");
        }
        let relative = text
            .strip_prefix(&format!("{top}/"))
            .ok_or_else(|| Error::new("invalid archive root"))?;
        let mode = if relative == "manifest.json" {
            0o644
        } else {
            PAYLOAD
                .iter()
                .find(|(p, _)| *p == relative)
                .map(|v| v.1)
                .ok_or_else(|| Error::new("unexpected entry"))?
        };
        if header.mode()? != mode {
            return fail("unsafe archive mode");
        }
        let size = header.size()?;
        let mut content = Vec::new();
        item.by_ref().take(size + 1).read_to_end(&mut content)?;
        if content.len() as u64 != size {
            return fail("entry size mismatch");
        }
        if relative == "manifest.json" {
            manifest_bytes = Some(content);
        } else if payload.insert(relative.into(), content).is_some() {
            return fail("duplicate entry");
        }
    }
    if paths != expected_paths {
        return fail("archive entries are unsorted, missing, duplicated or unexpected");
    }
    let manifest_bytes = manifest_bytes.ok_or_else(|| Error::new("missing manifest"))?;
    let parsed: Manifest = serde_json::from_slice(&manifest_bytes)?;
    if canonical(&parsed)? != manifest_bytes
        || parsed != manifest(version, target, epoch, &payload)?
    {
        return fail("invalid manifest");
    }
    Ok(Verified {
        digest,
        payload,
        manifest: manifest_bytes,
    })
}

fn decode_gzip(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = Decompress::new_gzip(15);
    let mut output = Vec::new();
    let mut input_offset = 0usize;
    loop {
        let mut chunk = [0u8; 64 * 1024];
        let input_before = decoder.total_in();
        let output_before = decoder.total_out();
        let status = decoder
            .decompress(&bytes[input_offset..], &mut chunk, FlushDecompress::None)
            .map_err(|error| Error::new(format!("invalid gzip stream: {error}")))?;
        input_offset += usize::try_from(decoder.total_in() - input_before)
            .map_err(|_| Error::new("gzip input offset overflow"))?;
        let written = usize::try_from(decoder.total_out() - output_before)
            .map_err(|_| Error::new("gzip output offset overflow"))?;
        output.extend_from_slice(&chunk[..written]);
        if output.len() as u64 > MAX_EXPANDED {
            return fail("expanded archive exceeds the size limit");
        }
        if status == Status::StreamEnd {
            break;
        }
        if input_before == decoder.total_in() && output_before == decoder.total_out() {
            return fail("truncated gzip stream");
        }
    }
    if input_offset != bytes.len() {
        return fail("trailing or concatenated gzip stream");
    }
    Ok(output)
}

/// Atomically publishes a new local file with the requested Unix mode.
///
/// Publication fails if `path` already exists.
pub fn publish(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::new("output has no parent"))?;
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::Builder::new()
        .prefix(".aql-release-")
        .tempfile_in(parent)?;
    aql_fs::set_file_mode(temp.as_file(), mode)?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    fs::hard_link(temp.path(), path)?;
    // Sync through the shared primitive: directory handles cannot be opened
    // with `File::open` on every supported platform (notably Windows).
    aql_fs::sync_dir(&open_directory(&parent.canonicalize()?)?)?;
    Ok(())
}

#[derive(Clone)]
struct Identity(PathBuf, FileIdentity);
#[derive(Clone, Copy)]
struct ManagedFileIdentity(FileIdentity);

fn prefix(path: &Path, exists: bool) -> Result<Vec<Identity>> {
    if !path.is_absolute()
        || path.parent().is_none()
        || path
            .components()
            .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
    {
        return fail("prefix must be normalized and absolute");
    }
    let home = std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
        .ok_or_else(|| Error::new("user home is required"))?;
    let data = std::env::var_os("XDG_DATA_HOME")
        .or_else(|| std::env::var_os("LOCALAPPDATA"))
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share"));
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .or_else(|| std::env::var_os("APPDATA"))
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".config"));
    let state = std::env::var_os("AQL_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            if cfg!(target_os = "macos") {
                home.join("Library/Application Support/aql")
            } else {
                data.join("aql")
            }
        });
    let aql_config = std::env::var_os("AQL_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| config.join("aql"));
    for root in [&home, &data, &config, &state, &aql_config] {
        validate_environment_root(root)?;
    }
    for protected in protected_roots(&home, &data, state, aql_config) {
        if path.starts_with(&protected) || protected.starts_with(path) {
            return fail("prefix overlaps protected data");
        }
    }
    let mut ancestors = path.ancestors().collect::<Vec<_>>();
    ancestors.reverse();
    let mut ids = Vec::new();
    for (index, current) in ancestors.iter().enumerate() {
        if current.parent().is_none() {
            continue;
        }
        match fs::symlink_metadata(current) {
            Ok(m) => {
                if m.file_type().is_symlink() || !m.is_dir() {
                    return fail("prefix chain is unsafe");
                }
                ids.push(Identity(
                    (*current).to_path_buf(),
                    aql_fs::directory_identity(current)?,
                ));
                if !exists && index + 1 == ancestors.len() {
                    return fail("prefix already exists");
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    && !exists
                    && index + 1 == ancestors.len() =>
            {
                break;
            }
            Err(_) => return fail("prefix parent is missing"),
        }
    }
    let parent = path
        .parent()
        .ok_or_else(|| Error::new("prefix has no parent"))?;
    let parent_dir = aql_fs::open_absolute_dir(parent)?;
    let parent_metadata = parent_dir.dir_metadata()?;
    if !aql_fs::owned_by_current_user(&parent_metadata)
        || aql_fs::mode(&parent_metadata).is_some_and(|mode| mode & 0o022 != 0)
    {
        return fail("prefix parent is not private and owned");
    }
    Ok(ids)
}

fn validate_environment_root(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return fail("environment data roots must be normalized and absolute");
    }
    Ok(())
}

fn protected_roots(home: &Path, data: &Path, state: PathBuf, aql_config: PathBuf) -> Vec<PathBuf> {
    vec![
        home.join(".claude"),
        home.join(".codex"),
        home.join(".kimi-code"),
        data.join("opencode"),
        state,
        aql_config,
    ]
}
fn revalidate(ids: &[Identity]) -> Result<()> {
    for Identity(path, expected) in ids {
        let m = fs::symlink_metadata(path)?;
        if m.file_type().is_symlink()
            || !m.is_dir()
            || aql_fs::directory_identity(path)? != *expected
        {
            return fail("prefix identity changed");
        }
    }
    Ok(())
}

fn open_directory(path: &Path) -> Result<Dir> {
    Ok(aql_fs::open_absolute_dir(path)?)
}

fn relative_parent(root: &Dir, relative: &Path) -> Result<(Dir, OsString)> {
    Ok(aql_fs::open_parent(root, relative)?)
}

fn read_managed_file(
    root: &Dir,
    relative: &Path,
    limit: u64,
) -> Result<(Vec<u8>, CapMetadata, ManagedFileIdentity)> {
    let (parent, name) = relative_parent(root, relative)?;
    let mut options = CapOpenOptions::new();
    options.read(true);
    let descriptor = aql_fs::open_file(&parent, Path::new(&name), options)?;
    let before = descriptor.metadata()?;
    if !before.is_file() || !aql_fs::owned_by_current_user(&before) || before.len() > limit {
        return fail("unsafe installed file");
    }
    let expected_identity = identity(&before);
    let mut file = descriptor.into_std();
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    let mut reopen_options = CapOpenOptions::new();
    reopen_options.read(true);
    let reopened = aql_fs::open_file(&parent, Path::new(&name), reopen_options)?;
    if identity(&reopened.metadata()?) != expected_identity
        || after.len() != before.len()
        || bytes.len() as u64 != after.len()
    {
        return fail("installed file changed while reading");
    }
    Ok((bytes, before, ManagedFileIdentity(expected_identity)))
}
fn files() -> Vec<String> {
    let mut out = PAYLOAD
        .iter()
        .map(|v| v.0.to_owned())
        .chain(["manifest.json".into(), "UNINSTALL_MANIFEST".into()])
        .collect::<Vec<_>>();
    out.sort();
    out
}

/// Verifies and atomically installs an archive into a new destination prefix.
///
/// When `plan` is true, validation runs without creating the destination.
pub fn install(
    archive: &Path,
    digest: &str,
    version: &str,
    target: &str,
    destination: &Path,
    plan: bool,
) -> Result<()> {
    let release = verify(archive, digest, version, target)?;
    let ids = prefix(destination, false)?;
    if plan {
        return Ok(());
    }
    let parent = destination
        .parent()
        .ok_or_else(|| Error::new("prefix has no parent"))?;
    let stage = tempfile::Builder::new()
        .prefix(".aql-install-")
        .tempdir_in(parent)?;
    aql_fs::set_mode(stage.path(), 0o700)?;
    for (relative, bytes) in &release.payload {
        let path = stage.path().join(relative);
        fs::create_dir_all(path.parent().ok_or_else(|| Error::new("invalid payload"))?)?;
        let mode = PAYLOAD
            .iter()
            .find(|v| v.0 == relative)
            .map(|v| v.1)
            .ok_or_else(|| Error::new("unexpected payload"))?;
        let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
        aql_fs::set_file_mode(&f, mode)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    let mut manifest_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(stage.path().join("manifest.json"))?;
    aql_fs::set_file_mode(&manifest_file, 0o644)?;
    manifest_file.write_all(&release.manifest)?;
    manifest_file.sync_all()?;
    drop(manifest_file);
    let uninstall = Uninstall {
        files: files(),
        schema_version: "aql-uninstall-v1".into(),
        target: target.into(),
        version: version.into(),
    };
    let path = stage.path().join("UNINSTALL_MANIFEST");
    let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
    aql_fs::set_file_mode(&f, 0o600)?;
    f.write_all(&canonical(&uninstall)?)?;
    f.sync_all()?;
    drop(f);
    let mut staged_directories = PAYLOAD
        .iter()
        .filter_map(|(relative, _)| Path::new(relative).parent())
        .flat_map(|path| path.ancestors().filter(|item| !item.as_os_str().is_empty()))
        .collect::<Vec<_>>();
    staged_directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    staged_directories.dedup();
    for directory in staged_directories {
        aql_fs::sync_dir(&aql_fs::open_absolute_dir(&stage.path().join(directory))?)?;
    }
    aql_fs::sync_dir(&aql_fs::open_absolute_dir(stage.path())?)?;
    aql_fs::set_mode(stage.path(), 0o755)?;
    revalidate(&ids)?;
    let parent_descriptor = open_directory(parent)?;
    let current_parent_identity = aql_fs::identity(&parent_descriptor.dir_metadata()?);
    let expected_parent = ids
        .last()
        .ok_or_else(|| Error::new("prefix parent identity is missing"))?;
    if current_parent_identity != expected_parent.1 {
        return fail("prefix parent changed before publication");
    }
    let stage_name = stage
        .path()
        .file_name()
        .ok_or_else(|| Error::new("staging name is missing"))?;
    let destination_name = destination
        .file_name()
        .ok_or_else(|| Error::new("prefix name is missing"))?;
    let _ = (stage_name, destination_name);
    aql_fs::rename_ambient_noreplace(stage.path(), destination)
        .map_err(|e| Error::new(format!("atomic publish failed: {e}")))?;
    let _ = stage.keep();
    aql_fs::sync_dir(&parent_descriptor)?;
    revalidate(&ids)
}

/// Removes a verified AQL installation without touching foreign or replaced files.
pub fn uninstall(destination: &Path) -> Result<bool> {
    let ids = prefix(destination, true)?;
    let root = open_directory(destination)?;
    let root_stat = root.dir_metadata()?;
    let root_identity = ids
        .last()
        .ok_or_else(|| Error::new("prefix identity is missing"))?;
    if identity(&root_stat) != root_identity.1 {
        return fail("install prefix changed before uninstall");
    }
    let mut manifest_options = CapOpenOptions::new();
    manifest_options.read(true);
    let manifest_descriptor =
        aql_fs::open_file(&root, Path::new("UNINSTALL_MANIFEST"), manifest_options)?;
    let manifest_stat = manifest_descriptor.metadata()?;
    if !manifest_stat.is_file()
        || !aql_fs::owned_by_current_user(&manifest_stat)
        || aql_fs::mode(&manifest_stat).is_some_and(|mode| mode & 0o077 != 0)
        || manifest_stat.len() > 1024 * 1024
    {
        return fail("unsafe uninstall manifest");
    }
    let mut manifest_file = manifest_descriptor.into_std();
    let mut bytes = Vec::new();
    Read::by_ref(&mut manifest_file)
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if manifest_stat.len() as usize != bytes.len() {
        return fail("uninstall manifest changed while reading");
    }
    let parsed: Uninstall = serde_json::from_slice(&bytes)?;
    if canonical(&parsed)? != bytes
        || parsed.schema_version != "aql-uninstall-v1"
        || parsed.files != files()
    {
        return fail("invalid uninstall allowlist");
    }
    let mut identities = BTreeMap::new();
    identities.insert(
        "UNINSTALL_MANIFEST".to_string(),
        ManagedFileIdentity(identity(&manifest_stat)),
    );
    let (installed_manifest_bytes, installed_manifest_stat, installed_manifest_identity) =
        read_managed_file(&root, Path::new("manifest.json"), 1024 * 1024)?;
    if aql_fs::mode(&installed_manifest_stat).is_some_and(|mode| mode & 0o777 != 0o644) {
        return fail("installed manifest mode changed");
    }
    let installed_manifest: Manifest = serde_json::from_slice(&installed_manifest_bytes)?;
    if canonical(&installed_manifest)? != installed_manifest_bytes
        || installed_manifest.schema_version != "aql-release-v1"
        || installed_manifest.package != PACKAGE
        || installed_manifest.version != parsed.version
        || installed_manifest.target != parsed.target
        || installed_manifest.build_metadata != metadata(&parsed.version, &parsed.target)
        || installed_manifest.entries.len() != PAYLOAD.len()
    {
        return fail("invalid installed manifest");
    }
    identities.insert("manifest.json".to_string(), installed_manifest_identity);
    let entries = installed_manifest
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    if entries.len() != PAYLOAD.len() {
        return fail("invalid installed manifest entries");
    }
    for (relative, mode) in PAYLOAD {
        let entry = entries
            .get(relative)
            .ok_or_else(|| Error::new("installed manifest entry is missing"))?;
        if entry.mode != format!("{mode:04o}") || entry.size as u64 > MAX_ARCHIVE {
            return fail("installed manifest entry is invalid");
        }
        let (content, stat, identity) = read_managed_file(&root, Path::new(relative), MAX_ARCHIVE)?;
        if aql_fs::mode(&stat).is_some_and(|actual| actual & 0o777 != mode)
            || content.len() != entry.size
            || hex::encode(Sha256::digest(&content)) != entry.sha256
        {
            return fail("installed file no longer matches the release manifest");
        }
        identities.insert(relative.to_string(), identity);
    }
    revalidate(&ids)?;
    for relative in &parsed.files {
        let (parent, name) = relative_parent(&root, Path::new(relative))?;
        let stat = parent.symlink_metadata(&name)?;
        let expected = identities
            .get(relative)
            .ok_or_else(|| Error::new("installed file identity is missing"))?;
        if !stat.is_file() || identity(&stat) != expected.0 {
            return fail("installed file was replaced");
        }
    }
    for relative in parsed.files.iter().rev() {
        let (parent, name) = relative_parent(&root, Path::new(relative))?;
        let stat = parent.symlink_metadata(&name)?;
        let expected = identities
            .get(relative)
            .ok_or_else(|| Error::new("installed file identity is missing"))?;
        if !stat.is_file() || identity(&stat) != expected.0 {
            return fail("installed file changed during uninstall");
        }
        parent.remove_file(&name)?;
    }
    let mut dirs = PAYLOAD
        .iter()
        .flat_map(|v| {
            let mut out = Vec::new();
            let mut p = Path::new(v.0).parent();
            while let Some(x) = p {
                if x.as_os_str().is_empty() {
                    break;
                }
                out.push(x.to_owned());
                p = x.parent();
            }
            out
        })
        .collect::<Vec<_>>();
    dirs.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
    dirs.dedup();
    for dir in dirs {
        if let Ok((parent, name)) = relative_parent(&root, &dir) {
            let _ = parent.remove_dir(name);
        }
    }
    drop(root);
    let parent = open_directory(
        destination
            .parent()
            .ok_or_else(|| Error::new("prefix has no parent"))?,
    )?;
    let destination_name = destination
        .file_name()
        .ok_or_else(|| Error::new("prefix name is missing"))?;
    match parent.remove_dir(destination_name) {
        Ok(()) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => Ok(true),
        Err(error) => Err(Error::new(error.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn symlink_file(source: &Path, target: &Path) -> std::io::Result<()> {
        std::os::unix::fs::symlink(source, target)
    }

    #[cfg(windows)]
    fn symlink_file(source: &Path, target: &Path) -> std::io::Result<()> {
        std::os::windows::fs::symlink_file(source, target)
    }

    fn synthetic_path(value: &str) -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(r"C:\synthetic").join(value)
        } else {
            PathBuf::from("/synthetic").join(value)
        }
    }

    fn archive(entries: &[(String, Vec<u8>, u32, EntryType)]) -> Vec<u8> {
        let mut tar = Vec::new();
        {
            let mut builder = Builder::new(&mut tar);
            for (path, bytes, mode, kind) in entries {
                let mut header = Header::new_ustar();
                header.set_size(if *kind == EntryType::Regular {
                    bytes.len() as u64
                } else {
                    0
                });
                header.set_mode(*mode);
                header.set_mtime(1);
                header.set_uid(0);
                header.set_gid(0);
                header.set_entry_type(*kind);
                header.set_cksum();
                builder
                    .append_data(&mut header, path, bytes.as_slice())
                    .expect("append archive entry");
            }
            builder.finish().expect("finish tar");
        }
        let mut writer = GzBuilder::new()
            .mtime(1)
            .write(Vec::new(), Compression::best());
        writer.write_all(&tar).expect("write tar");
        writer.finish().expect("finish gzip")
    }

    fn valid_entries() -> Vec<(String, Vec<u8>, u32, EntryType)> {
        let payload = PAYLOAD
            .iter()
            .map(|(path, _)| {
                (
                    (*path).to_owned(),
                    format!("synthetic:{path}\n").into_bytes(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let manifest =
            canonical(&manifest("1.2.3", "aarch64-macos", 1, &payload).expect("manifest"))
                .expect("canonical manifest");
        let top = "aql-1.2.3-aarch64-macos";
        let mut entries = payload
            .into_iter()
            .map(|(path, bytes)| {
                let mode = PAYLOAD.iter().find(|item| item.0 == path).expect("mode").1;
                (format!("{top}/{path}"), bytes, mode, EntryType::Regular)
            })
            .chain([(
                format!("{top}/manifest.json"),
                manifest,
                0o644,
                EntryType::Regular,
            )])
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
    }

    fn write_archive(directory: &Path, name: &str, bytes: &[u8]) -> (PathBuf, String) {
        let path = directory.join(name);
        fs::write(&path, bytes).expect("write archive");
        let digest = hex::encode(Sha256::digest(bytes));
        (path, digest)
    }

    #[test]
    fn validation_is_ascii_and_closed() {
        assert!(validate_version("1.2.3").is_ok());
        assert!(validate_version("1.２.3").is_err());
        assert!(validate_target("x86_64-linux").is_ok());
        assert!(validate_target("windows").is_err());
    }

    #[test]
    fn gzip_requires_one_complete_stream() {
        let mut writer = GzBuilder::new()
            .mtime(1)
            .write(Vec::new(), Compression::best());
        writer.write_all(b"synthetic").expect("write gzip");
        let gzip = writer.finish().expect("finish gzip");
        assert_eq!(decode_gzip(&gzip).expect("valid gzip"), b"synthetic");
        let mut trailing = gzip.clone();
        trailing.extend_from_slice(b"trailing");
        assert!(decode_gzip(&trailing).is_err());
        let mut concatenated = gzip.clone();
        concatenated.extend_from_slice(&gzip);
        assert!(decode_gzip(&concatenated).is_err());
    }

    #[test]
    fn archive_verification_rejects_adversarial_shapes() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let entries = valid_entries();
        let valid = archive(&entries);
        let (path, digest) = write_archive(temporary.path(), "valid.tar.gz", &valid);
        verify(&path, &digest, "1.2.3", "aarch64-macos").expect("valid archive");
        assert!(verify(&path, &"0".repeat(64), "1.2.3", "aarch64-macos").is_err());

        let mut duplicate = entries.clone();
        duplicate.insert(1, duplicate[0].clone());
        let mut unsafe_mode = entries.clone();
        unsafe_mode[0].2 = 0o777;
        let mut symlink = entries.clone();
        symlink[0].3 = EntryType::Symlink;
        for (name, malicious) in [
            ("duplicate", duplicate),
            ("unsafe-mode", unsafe_mode),
            ("symlink", symlink),
        ] {
            let bytes = archive(&malicious);
            let (path, digest) = write_archive(temporary.path(), name, &bytes);
            assert!(verify(&path, &digest, "1.2.3", "aarch64-macos").is_err());
        }
        let link = temporary.path().join("archive-link");
        symlink_file(&path, &link).expect("archive symlink");
        assert!(verify(&link, &digest, "1.2.3", "aarch64-macos").is_err());
    }

    #[test]
    fn install_and_uninstall_preserve_foreign_files() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let parent = temporary
            .path()
            .canonicalize()
            .expect("canonical temporary");
        aql_fs::set_mode(&parent, 0o700).expect("private parent");
        let bytes = archive(&valid_entries());
        let (archive_path, digest) = write_archive(&parent, "release.tar.gz", &bytes);
        let destination = parent.join("installed");
        install(
            &archive_path,
            &digest,
            "1.2.3",
            "aarch64-macos",
            &destination,
            false,
        )
        .expect("install");
        fs::write(destination.join("foreign"), b"foreign\n").expect("foreign file");
        assert!(uninstall(&destination).expect("uninstall retains foreign file"));
        assert_eq!(
            fs::read(destination.join("foreign")).expect("foreign remains"),
            b"foreign\n"
        );
    }

    #[test]
    fn uninstall_refuses_replaced_managed_files() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let parent = temporary
            .path()
            .canonicalize()
            .expect("canonical temporary");
        aql_fs::set_mode(&parent, 0o700).expect("private parent");
        let bytes = archive(&valid_entries());
        let (archive_path, digest) = write_archive(&parent, "release.tar.gz", &bytes);
        let destination = parent.join("installed");
        install(
            &archive_path,
            &digest,
            "1.2.3",
            "aarch64-macos",
            &destination,
            false,
        )
        .expect("install");
        fs::write(destination.join("bin/aql"), b"foreign replacement\n")
            .expect("replace managed file");

        assert!(uninstall(&destination).is_err());
        assert_eq!(
            fs::read(destination.join("bin/aql")).expect("replacement remains"),
            b"foreign replacement\n"
        );
    }

    #[test]
    fn install_prefixes_cannot_overlap_any_agent_store() {
        let home = synthetic_path("home");
        let data = synthetic_path("data");
        let roots = protected_roots(
            &home,
            &data,
            synthetic_path("state/aql"),
            synthetic_path("config/aql"),
        );
        for protected in [
            home.join(".claude"),
            home.join(".codex"),
            home.join(".kimi-code"),
            data.join("opencode"),
        ] {
            assert!(roots.contains(&protected));
        }
        assert!(validate_environment_root(Path::new("relative/home")).is_err());
        assert!(validate_environment_root(&synthetic_path("../home")).is_err());
        assert!(validate_environment_root(&home).is_ok());
    }
}
