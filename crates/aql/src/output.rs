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

pub(super) struct SecureOutputFile {
    directory: cap_std::fs::Dir,
    directory_path: PathBuf,
    directory_identity: aql_fs::FileIdentity,
    target_name: std::ffi::OsString,
    temporary_name: std::ffi::OsString,
    file: fs::File,
    committed: bool,
}

impl SecureOutputFile {
    pub(super) fn create(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let target_name = path
            .file_name()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| invalid_argument("output target must have a file name"))?
            .to_os_string();
        let directory_path = output_directory_path(path);
        let directory = open_output_directory(directory_path)?;
        let directory_identity = aql_fs::identity(&directory.dir_metadata()?);
        match directory.symlink_metadata(&target_name) {
            Ok(_) => return Err(already_exists("output target already exists").into()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let temporary_name =
            std::ffi::OsString::from(format!(".aql-output-{:016x}.tmp", rand::random::<u64>()));
        let mut options = cap_std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        aql_fs::set_open_mode(&mut options, 0o600);
        let temporary =
            aql_fs::open_file(&directory, std::path::Path::new(&temporary_name), options)?
                .into_std();
        Ok(Self {
            directory,
            directory_path: directory_path.to_path_buf(),
            directory_identity,
            target_name,
            temporary_name,
            file: temporary,
            committed: false,
        })
    }

    pub(super) fn writer(&mut self) -> &mut fs::File {
        &mut self.file
    }

    pub(super) fn commit(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.file.flush()?;
        self.file.sync_all()?;
        let current_directory = {
            let directory = open_output_directory(&self.directory_path)?;
            aql_fs::identity(&directory.dir_metadata()?)
        };
        if current_directory != self.directory_identity {
            return Err(state_integrity("output target directory changed during write").into());
        }
        match self.directory.symlink_metadata(&self.target_name) {
            Ok(_) => return Err(already_exists("output target appeared during write").into()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        aql_fs::rename_noreplace(
            &self.directory,
            std::path::Path::new(&self.temporary_name),
            &self.directory,
            std::path::Path::new(&self.target_name),
        )?;
        aql_fs::sync_dir(&self.directory)?;
        self.committed = true;
        Ok(())
    }
}

fn output_directory_path(path: &std::path::Path) -> &std::path::Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."))
}

fn open_output_directory(
    path: &std::path::Path,
) -> Result<cap_std::fs::Dir, Box<dyn std::error::Error>> {
    let path = normalize_output_directory(path);
    Ok(aql_fs::open_dir(&path)?)
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

impl Drop for SecureOutputFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.directory.remove_file(&self.temporary_name);
        }
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
