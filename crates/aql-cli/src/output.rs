use super::*;

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
            .ok_or("output target must have a file name")?
            .to_os_string();
        let directory_path = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let directory = rustix::fs::openat(
            rustix::fs::CWD,
            directory_path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let directory_stat = rustix::fs::fstat(&directory)?;
        let directory_identity = identity(&directory_stat);
        match rustix::fs::statat(&directory, &target_name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(_) => return Err("output target already exists".into()),
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
        let current_directory =
            rustix::fs::statat(rustix::fs::CWD, &self.directory_path, AtFlags::empty())?;
        if identity(&current_directory) != self.directory_identity {
            return Err("output target directory changed during write".into());
        }
        match rustix::fs::statat(
            &self.directory,
            &self.target_name,
            AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Ok(_) => return Err("output target appeared during write".into()),
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

#[cfg(not(unix))]
pub(super) struct SecureOutputFile;

#[cfg(not(unix))]
impl SecureOutputFile {
    pub(super) fn create(_path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        Err("secure file output is not supported on this platform".into())
    }

    pub(super) fn writer(&mut self) -> &mut fs::File {
        unreachable!()
    }

    pub(super) fn commit(self) -> Result<(), Box<dyn std::error::Error>> {
        unreachable!()
    }
}
