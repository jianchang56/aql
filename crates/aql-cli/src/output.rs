use super::*;
use std::io::{Seek, SeekFrom};

pub(super) enum TransactionalOutput {
    File(SecureOutputFile),
    Stdout(fs::File),
}

impl TransactionalOutput {
    pub(super) fn create(
        path: Option<&std::path::Path>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match path {
            Some(path) => Ok(Self::File(SecureOutputFile::create(path)?)),
            None => Ok(Self::Stdout(tempfile::tempfile()?)),
        }
    }

    pub(super) fn writer(&mut self) -> &mut fs::File {
        match self {
            Self::File(output) => output.writer(),
            Self::Stdout(output) => output,
        }
    }

    pub(super) fn publish(
        mut self,
        cancellation: &CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match &mut self {
            Self::File(_) => {}
            Self::Stdout(spool) => {
                spool.flush()?;
                spool.seek(SeekFrom::Start(0))?;
                let mut stdout = io::stdout().lock();
                let mut buffer = [0_u8; 64 * 1024];
                loop {
                    let count = spool.read(&mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    if let Err(error) = stdout.write_all(&buffer[..count]) {
                        if error.kind() == io::ErrorKind::BrokenPipe {
                            cancellation.cancel();
                            return Ok(());
                        }
                        return Err(error.into());
                    }
                }
                if let Err(error) = stdout.flush() {
                    if error.kind() == io::ErrorKind::BrokenPipe {
                        cancellation.cancel();
                        return Ok(());
                    }
                    return Err(error.into());
                }
            }
        }
        if let Self::File(output) = self {
            output.commit()?;
        }
        Ok(())
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
pub(super) struct SecureOutputFile {
    directory: std::os::fd::OwnedFd,
    directory_path: PathBuf,
    directory_identity: FileIdentity,
    target_name: std::ffi::OsString,
    temporary_name: std::ffi::OsString,
    file: fs::File,
    committed: bool,
}

#[cfg(unix)]
impl SecureOutputFile {
    pub(super) fn create(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        use rustix::fs::{AtFlags, Mode, OFlags};

        let target_name = path
            .file_name()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| invalid_argument("output target must have a file name"))?
            .to_os_string();
        let directory_path = output_directory_path(path);
        let directory = open_output_directory(directory_path)?;
        let directory_stat = rustix::fs::fstat(&directory)?;
        let directory_identity = identity(&directory_stat);
        match rustix::fs::statat(&directory, &target_name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(_) => return Err(already_exists("output target already exists").into()),
            Err(error) if error == rustix::io::Errno::NOENT => {}
            Err(error) => return Err(error.into()),
        }
        let temporary_name =
            std::ffi::OsString::from(format!(".aql-output-{:016x}.tmp", rand::random::<u64>()));
        let temporary = rustix::fs::openat(
            &directory,
            &temporary_name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
        )?;
        Ok(Self {
            directory,
            directory_path: directory_path.to_path_buf(),
            directory_identity,
            target_name,
            temporary_name,
            file: temporary.into(),
            committed: false,
        })
    }

    pub(super) fn writer(&mut self) -> &mut fs::File {
        &mut self.file
    }

    pub(super) fn commit(mut self) -> Result<(), Box<dyn std::error::Error>> {
        use rustix::fs::{AtFlags, RenameFlags};

        self.file.flush()?;
        self.file.sync_all()?;
        let current_directory = {
            let directory = open_output_directory(&self.directory_path)?;
            rustix::fs::fstat(&directory)?
        };
        if identity(&current_directory) != self.directory_identity {
            return Err(state_integrity("output target directory changed during write").into());
        }
        match rustix::fs::statat(
            &self.directory,
            &self.target_name,
            AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Ok(_) => return Err(already_exists("output target appeared during write").into()),
            Err(error) if error == rustix::io::Errno::NOENT => {}
            Err(error) => return Err(error.into()),
        }
        rustix::fs::renameat_with(
            &self.directory,
            &self.temporary_name,
            &self.directory,
            &self.target_name,
            RenameFlags::NOREPLACE,
        )?;
        rustix::fs::fsync(&self.directory)?;
        self.committed = true;
        Ok(())
    }
}

#[cfg(unix)]
fn output_directory_path(path: &std::path::Path) -> &std::path::Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."))
}

#[cfg(unix)]
fn open_output_directory(
    path: &std::path::Path,
) -> Result<std::os::fd::OwnedFd, Box<dyn std::error::Error>> {
    use rustix::fs::{CWD, Mode, OFlags, openat};
    use std::path::Component;

    let path = normalize_output_directory(path);
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut directory = if path.is_absolute() {
        openat(CWD, "/", flags, Mode::empty())?
    } else {
        openat(CWD, ".", flags, Mode::empty())?
    };
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => {
                directory = openat(&directory, name, flags, Mode::empty())?;
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(
                    invalid_argument("output path must not contain parent traversal").into(),
                );
            }
        }
    }
    Ok(directory)
}

#[cfg(target_os = "macos")]
fn normalize_output_directory(path: &std::path::Path) -> std::path::PathBuf {
    for (alias, target) in [("/var", "/private/var"), ("/tmp", "/private/tmp")] {
        let alias = std::path::Path::new(alias);
        if let Ok(relative) = path.strip_prefix(alias) {
            return std::path::Path::new(target).join(relative);
        }
    }
    path.to_path_buf()
}

#[cfg(not(target_os = "macos"))]
fn normalize_output_directory(path: &std::path::Path) -> std::path::PathBuf {
    path.to_path_buf()
}

#[cfg(unix)]
impl Drop for SecureOutputFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = rustix::fs::unlinkat(
                &self.directory,
                &self.temporary_name,
                rustix::fs::AtFlags::empty(),
            );
        }
    }
}

#[cfg(unix)]
fn identity(stat: &rustix::fs::Stat) -> FileIdentity {
    FileIdentity {
        device: stat.st_dev as u64,
        inode: stat.st_ino,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_parent_resolves_to_current_directory() {
        assert_eq!(
            output_directory_path(std::path::Path::new("result.json")),
            std::path::Path::new(".")
        );
        let directory = open_output_directory(std::path::Path::new(""))
            .expect("an empty relative parent resolves to the current directory");
        let _ = directory;
    }
}

#[cfg(not(unix))]
pub(super) struct SecureOutputFile;

#[cfg(not(unix))]
impl SecureOutputFile {
    pub(super) fn create(_path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        Err(unsupported("secure file output is not supported on this platform").into())
    }

    pub(super) fn writer(&mut self) -> &mut fs::File {
        unreachable!()
    }

    pub(super) fn commit(self) -> Result<(), Box<dyn std::error::Error>> {
        unreachable!()
    }
}
