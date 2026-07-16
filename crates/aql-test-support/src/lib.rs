//! Deterministic synthetic fixture generators for every supported Agent format.
//!
//! Fixtures contain only reserved synthetic values and are safe for adapter,
//! security, and performance tests. Generators replace only their explicit
//! output directory and never inspect real Agent data.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod claude;
mod codex;
mod kimi;
mod opencode;

use std::fs;
use std::path::Path;

pub use claude::generate_claude;
pub use codex::generate_codex;
pub use kimi::generate_kimi;
pub use opencode::generate_opencode;

/// Fallible result returned by fixture generators.
pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn reset_output(output: &Path) -> TestResult {
    if output.as_os_str().is_empty() || output.parent().is_none() {
        return Err("fixture output must have a parent directory".into());
    }
    match fs::symlink_metadata(output) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err("fixture output cannot be a symlink".into());
        }
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(output)?,
        Ok(_) => return Err("fixture output must be a directory path".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::create_dir_all(output)?;
    Ok(())
}

fn write_private(path: &Path, contents: impl AsRef<[u8]>) -> TestResult {
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;

    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path)?;
    file.write_all(contents.as_ref())?;
    file.flush()?;
    Ok(())
}

#[cfg(unix)]
fn make_private_tree(root: &Path) -> TestResult {
    use std::os::unix::fs::PermissionsExt;

    for entry in walk(root)? {
        let metadata = fs::symlink_metadata(&entry)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        let mode = if metadata.is_dir() { 0o700 } else { 0o600 };
        fs::set_permissions(entry, fs::Permissions::from_mode(mode))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn make_private_tree(_root: &Path) -> TestResult {
    Ok(())
}

#[cfg(unix)]
fn walk(root: &Path) -> TestResult<Vec<std::path::PathBuf>> {
    let mut result = vec![root.to_path_buf()];
    let mut index = 0;
    while index < result.len() {
        let path = result[index].clone();
        index += 1;
        if !fs::symlink_metadata(&path)?.is_dir() {
            continue;
        }
        for entry in fs::read_dir(path)? {
            result.push(entry?.path());
        }
    }
    Ok(result)
}

#[cfg(unix)]
fn symlink_file(source: &Path, target: &Path) -> TestResult {
    std::os::unix::fs::symlink(source, target)?;
    Ok(())
}

#[cfg(windows)]
fn symlink_file(source: &Path, target: &Path) -> TestResult {
    std::os::windows::fs::symlink_file(source, target)?;
    Ok(())
}

#[cfg(unix)]
fn symlink_dir(source: &Path, target: &Path) -> TestResult {
    std::os::unix::fs::symlink(source, target)?;
    Ok(())
}

#[cfg(windows)]
fn symlink_dir(source: &Path, target: &Path) -> TestResult {
    std::os::windows::fs::symlink_dir(source, target)?;
    Ok(())
}

fn copy_tree(source: &Path, target: &Path) -> TestResult {
    fs::create_dir_all(target)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source_path)?;
        if metadata.is_dir() {
            copy_tree(&source_path, &target_path)?;
        } else if metadata.is_file() {
            fs::copy(&source_path, &target_path)?;
        } else {
            return Err("fixture tree copy accepts only regular files and directories".into());
        }
    }
    Ok(())
}
