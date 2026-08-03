use super::*;

pub(super) fn safe_directory(path: &Path, stage: &str) -> Result<fs::Metadata, AdapterError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => AdapterError::NotFound {
            stage: stage.to_string(),
        },
        _ => AdapterError::PermissionDenied {
            stage: stage.to_string(),
        },
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(AdapterError::UnsupportedFormat {
            stage: stage.to_string(),
        });
    }
    Ok(metadata)
}

pub(super) fn safe_file(path: &Path, stage: &str) -> Result<fs::Metadata, AdapterError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => AdapterError::NotFound {
            stage: stage.to_string(),
        },
        _ => AdapterError::PermissionDenied {
            stage: stage.to_string(),
        },
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(AdapterError::UnsupportedFormat {
            stage: stage.to_string(),
        });
    }
    Ok(metadata)
}

pub(super) fn optional_file_identity(
    path: &Path,
    stage: &str,
) -> Result<Option<FileIdentity>, AdapterError> {
    aql_fs::optional_file_identity(path).map_err(|error| match error {
        aql_fs::OptionalFileError::NotRegularFile => AdapterError::UnsupportedFormat {
            stage: stage.to_string(),
        },
        aql_fs::OptionalFileError::IdentityUnavailable => AdapterError::SnapshotUnavailable,
        aql_fs::OptionalFileError::Io(_) => AdapterError::PermissionDenied {
            stage: stage.to_string(),
        },
    })
}

pub(super) fn table_columns(
    connection: &Connection,
    table: &str,
) -> Result<BTreeSet<String>, AdapterError> {
    let sql = match table {
        "session" => "PRAGMA table_info(session)",
        "message" => "PRAGMA table_info(message)",
        "part" => "PRAGMA table_info(part)",
        _ => {
            return Err(AdapterError::Internal {
                stage: "opencode_schema_table".to_string(),
            });
        }
    };
    let mut statement = connection
        .prepare(sql)
        .map_err(|_| AdapterError::UnsupportedFormat {
            stage: "opencode_schema_query".to_string(),
        })?;
    statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|_| AdapterError::UnsupportedFormat {
            stage: "opencode_schema_query".to_string(),
        })?
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|_| AdapterError::UnsupportedFormat {
            stage: "opencode_schema_value".to_string(),
        })
}
