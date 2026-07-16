//! Cross-platform capability-based filesystem primitives used by AQL.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Component, Path, PathBuf};

use cap_fs_ext::{DirExt, FollowSymlinks, MetadataExt as _, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, File, Metadata, OpenOptions};

/// Stable identity of one open filesystem object.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    /// Returns the filesystem device or volume identifier.
    #[must_use]
    pub fn device(self) -> u64 {
        self.device
    }

    /// Returns the filesystem inode or file-index identifier.
    #[must_use]
    pub fn inode(self) -> u64 {
        self.inode
    }
}

/// Returns a cross-platform identity for capability metadata.
#[must_use]
pub fn identity(metadata: &Metadata) -> FileIdentity {
    FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

/// Opens a directory without following symlinks and returns its identity.
pub fn directory_identity(path: &Path) -> io::Result<FileIdentity> {
    let directory = open_dir(path)?;
    Ok(identity(&directory.dir_metadata()?))
}

/// Opens a regular file for reading without following symlinks and returns its identity.
pub fn file_identity(path: &Path) -> io::Result<FileIdentity> {
    let mut options = OpenOptions::new();
    options.read(true);
    let file = open_ambient_file(path, options)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "path is not a regular file",
        ));
    }
    Ok(identity(&metadata))
}

/// Opens a normalized absolute directory without following any path-component symlink.
pub fn open_absolute_dir(path: &Path) -> io::Result<Dir> {
    let path = normalize_absolute_path(path);
    let (root, components) = split_absolute(&path)?;
    let mut directory = Dir::open_ambient_dir(root, cap_std::ambient_authority())?;
    for component in components {
        directory = directory.open_dir_nofollow(component)?;
    }
    Ok(directory)
}

/// Opens an absolute directory, creating missing components without following
/// symlinks and applying the requested private mode where supported.
pub fn open_or_create_absolute_dir(path: &Path, mode: u32) -> io::Result<Dir> {
    let path = normalize_absolute_path(path);
    let (root, components) = split_absolute(&path)?;
    let mut directory = Dir::open_ambient_dir(root, cap_std::ambient_authority())?;
    for component in components {
        match directory.open_dir_nofollow(component) {
            Ok(next) => directory = next,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut builder = cap_std::fs::DirBuilder::new();
                set_dir_builder_mode(&mut builder, mode);
                match directory.create_dir_with(component, &builder) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
                directory = directory.open_dir_nofollow(component)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(directory)
}

/// Opens a normalized directory, relative paths being anchored at the current directory.
pub fn open_dir(path: &Path) -> io::Result<Dir> {
    if path.is_absolute() {
        open_absolute_dir(path)
    } else {
        let mut directory = Dir::open_ambient_dir(".", cap_std::ambient_authority())?;
        for component in normal_components(path)? {
            directory = directory.open_dir_nofollow(component)?;
        }
        Ok(directory)
    }
}

/// Opens a normalized relative directory below an existing directory capability.
pub fn open_relative_dir(root: &Dir, path: &Path) -> io::Result<Dir> {
    let mut directory = root.try_clone()?;
    for component in normal_components(path)? {
        directory = directory.open_dir_nofollow(component)?;
    }
    Ok(directory)
}

/// Opens a file below a directory capability without following any symlink component.
pub fn open_file(root: &Dir, path: &Path, mut options: OpenOptions) -> io::Result<File> {
    let (parent, name) = open_parent(root, path)?;
    options.follow(FollowSymlinks::No);
    parent.open_with(name, &options)
}

/// Opens an ambient file without following any symlink component.
pub fn open_ambient_file(path: &Path, options: OpenOptions) -> io::Result<File> {
    let parent = path.parent().filter(|value| !value.as_os_str().is_empty());
    let name = path
        .file_name()
        .filter(|value| !value.is_empty())
        .ok_or_else(invalid_path)?;
    let directory = match parent {
        Some(parent) => open_dir(parent)?,
        None => open_dir(Path::new("."))?,
    };
    open_file(&directory, Path::new(name), options)
}

/// Opens the parent directory and returns the final component of a relative path.
pub fn open_parent(root: &Dir, path: &Path) -> io::Result<(Dir, OsString)> {
    let components = normal_components(path)?;
    let (name, parents) = components.split_last().ok_or_else(invalid_path)?;
    let mut directory = root.try_clone()?;
    for component in parents {
        directory = directory.open_dir_nofollow(component)?;
    }
    Ok((directory, (*name).to_os_string()))
}

/// Flushes directory metadata to stable storage where the platform supports it.
pub fn sync_dir(directory: &Dir) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::fd::AsFd as _;

        let descriptor = rustix::fs::openat(
            directory.as_fd(),
            ".",
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(io::Error::from)?;
        rustix::fs::fsync(descriptor).map_err(io::Error::from)
    }
    #[cfg(not(unix))]
    {
        let _ = directory;
        Ok(())
    }
}

/// Sets Unix permission bits and is a no-op on platforms without Unix modes.
pub fn set_mode(path: &Path, mode: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
        Ok(())
    }
}

/// Sets Unix permission bits on an open file and is a no-op elsewhere.
pub fn set_file_mode(file: &std::fs::File, mode: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(mode))
    }
    #[cfg(not(unix))]
    {
        let _ = (file, mode);
        Ok(())
    }
}

/// Applies Unix creation mode to capability open options and is a no-op elsewhere.
pub fn set_open_mode(options: &mut OpenOptions, mode: u32) {
    #[cfg(unix)]
    {
        use cap_std::fs::OpenOptionsExt as _;
        options.mode(mode);
    }
    #[cfg(not(unix))]
    {
        let _ = (options, mode);
    }
}

fn set_dir_builder_mode(builder: &mut cap_std::fs::DirBuilder, mode: u32) {
    #[cfg(unix)]
    {
        use cap_std::fs::DirBuilderExt as _;
        builder.mode(mode);
    }
    #[cfg(not(unix))]
    {
        let _ = (builder, mode);
    }
}

/// Returns Unix permission bits when available.
#[must_use]
pub fn mode(metadata: &Metadata) -> Option<u32> {
    #[cfg(unix)]
    {
        use cap_std::fs::PermissionsExt as _;
        Some(metadata.permissions().mode())
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        None
    }
}

/// Returns whether metadata belongs to the current user where ownership is available.
#[must_use]
pub fn owned_by_current_user(metadata: &Metadata) -> bool {
    #[cfg(unix)]
    {
        use cap_fs_ext::OsMetadataExt as _;
        metadata.uid() == rustix::process::getuid().as_raw()
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        true
    }
}

/// Atomically renames a directory entry while refusing to replace an existing target.
pub fn rename_noreplace(from_dir: &Dir, from: &Path, to_dir: &Dir, to: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::fd::AsFd as _;
        rustix::fs::renameat_with(
            from_dir.as_fd(),
            from,
            to_dir.as_fd(),
            to,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(io::Error::from)
    }
    #[cfg(not(unix))]
    {
        from_dir.rename(from, to_dir, to)
    }
}

/// Atomically renames an ambient path while refusing to replace an existing target.
pub fn rename_ambient_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    let from_parent = open_dir(from.parent().ok_or_else(invalid_path)?)?;
    let to_parent = open_dir(to.parent().ok_or_else(invalid_path)?)?;
    let from_name = from.file_name().ok_or_else(invalid_path)?;
    let to_name = to.file_name().ok_or_else(invalid_path)?;
    rename_noreplace(
        &from_parent,
        Path::new(from_name),
        &to_parent,
        Path::new(to_name),
    )
}

fn split_absolute(path: &Path) -> io::Result<(PathBuf, Vec<&OsStr>)> {
    if !path.is_absolute() {
        return Err(invalid_path());
    }
    let mut root = PathBuf::new();
    let mut names = Vec::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => root.push(prefix.as_os_str()),
            Component::RootDir => root.push(Path::new(std::path::MAIN_SEPARATOR_STR)),
            Component::Normal(name) => names.push(name),
            Component::CurDir | Component::ParentDir => return Err(invalid_path()),
        }
    }
    if root.as_os_str().is_empty() {
        return Err(invalid_path());
    }
    Ok((root, names))
}

#[cfg(target_os = "macos")]
fn normalize_absolute_path(path: &Path) -> PathBuf {
    for (alias, target) in [("/var", "/private/var"), ("/tmp", "/private/tmp")] {
        let alias = Path::new(alias);
        if let Ok(relative) = path.strip_prefix(alias) {
            return Path::new(target).join(relative);
        }
    }
    path.to_path_buf()
}

#[cfg(not(target_os = "macos"))]
fn normalize_absolute_path(path: &Path) -> PathBuf {
    path.to_path_buf()
}

fn normal_components(path: &Path) -> io::Result<Vec<&OsStr>> {
    let mut names = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(name) => names.push(name),
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                return Err(invalid_path());
            }
        }
    }
    Ok(names)
}

fn invalid_path() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "path is not normalized")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_absolute_and_relative_directories_without_parent_traversal() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        std::fs::create_dir(temporary.path().join("child")).expect("child directory");
        open_absolute_dir(temporary.path()).expect("absolute directory opens");
        open_or_create_absolute_dir(&temporary.path().join("created/nested"), 0o700)
            .expect("missing absolute directories are created");
        assert!(temporary.path().join("created/nested").is_dir());
        let root = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())
            .expect("temporary capability");
        sync_dir(&root).expect("directory metadata sync succeeds");
        open_relative_dir(&root, Path::new("child")).expect("relative directory opens");
        assert!(open_relative_dir(&root, Path::new("../escape")).is_err());
    }
}
