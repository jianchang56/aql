use super::*;

pub(super) fn canonical_or_prospective(path: &std::path::Path) -> io::Result<PathBuf> {
    match path.canonicalize() {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
            let name = path.file_name().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "path has no final component")
            })?;
            Ok(parent.canonicalize()?.join(name))
        }
        Err(error) => Err(error),
    }
}

pub(super) fn configured_database_to_inputs(
    database: ConfiguredDatabase,
) -> Result<SourceInputs, Box<dyn std::error::Error>> {
    let source_specs = database
        .members
        .into_iter()
        .map(|source| {
            let root = source
                .root
                .to_str()
                .ok_or("database member path is invalid")?;
            Ok(format!("{}={root}", source.adapter_id))
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    Ok(SourceInputs {
        source_specs,
        skip_unavailable: false,
    })
}

struct DatabaseCandidate {
    name: &'static str,
    adapter_id: &'static str,
    root: PathBuf,
}

fn database_candidates() -> Result<Vec<DatabaseCandidate>, Box<dyn std::error::Error>> {
    let home = std::env::var_os("HOME")
        .or_else(|| {
            cfg!(windows)
                .then(|| std::env::var_os("USERPROFILE"))
                .flatten()
        })
        .map(PathBuf::from)
        .ok_or_else(|| state_unavailable("HOME is not set"))?;
    if !home.is_absolute() {
        return Err(state_unavailable("HOME must be absolute for database discovery").into());
    }
    let data_home = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            cfg!(windows)
                .then(|| std::env::var_os("LOCALAPPDATA").map(PathBuf::from))
                .flatten()
        })
        .unwrap_or_else(|| home.join(".local/share"));
    if !data_home.is_absolute() {
        return Err(
            state_unavailable("XDG_DATA_HOME must be absolute for database discovery").into(),
        );
    }
    Ok(database_candidates_for(&home, &data_home))
}

fn database_candidates_for(
    home: &std::path::Path,
    data_home: &std::path::Path,
) -> Vec<DatabaseCandidate> {
    vec![
        DatabaseCandidate {
            name: "claude",
            adapter_id: "claude-code",
            root: home.join(".claude"),
        },
        DatabaseCandidate {
            name: "codex",
            adapter_id: "codex",
            root: home.join(".codex"),
        },
        DatabaseCandidate {
            name: "kimi",
            adapter_id: "kimi-code",
            root: home.join(".kimi-code"),
        },
        DatabaseCandidate {
            name: "opencode",
            adapter_id: "opencode",
            root: data_home.join("opencode"),
        },
    ]
}

pub(super) fn read_adapter(
    adapter_id: &str,
    installation_salt: &[u8],
) -> Result<Arc<dyn AgentAdapter>, Box<dyn std::error::Error>> {
    match adapter_id {
        "claude-code" => Ok(Arc::new(ClaudeCodeAdapter::new(installation_salt.to_vec()))),
        "codex" => Ok(Arc::new(CodexAdapter::new(installation_salt.to_vec()))),
        "kimi-code" => Ok(Arc::new(KimiCodeAdapter::new(installation_salt.to_vec()))),
        "opencode" => Ok(Arc::new(OpenCodeAdapter::new(installation_salt.to_vec()))),
        _ => Err(invalid_argument("unknown source adapter").into()),
    }
}

pub(super) fn bind_sources(
    inputs: SourceInputs,
    installation_salt: Vec<u8>,
) -> Result<Vec<FederatedSource>, Box<dyn std::error::Error>> {
    let parsed = parse_source_specs_with_policy(inputs.source_specs, inputs.skip_unavailable)?;
    let mut bound = Vec::new();
    let mut source_ids = std::collections::BTreeSet::new();
    for source in parsed {
        let adapter = read_adapter(&source.adapter_id, &installation_salt)?;
        let probe = match adapter.probe(&ProbeRequest {
            data_root: source.root.to_string_lossy().into_owned(),
        }) {
            Ok(probe) => probe,
            Err(_) if inputs.skip_unavailable => continue,
            Err(error) => return Err(error.into()),
        };
        if probe.manifests.is_empty() {
            if inputs.skip_unavailable {
                continue;
            }
            return Err(source_unavailable("probe returned no compatible source").into());
        }
        for manifest in probe.manifests {
            if !source_ids.insert(manifest.source_id.clone()) {
                return Err(state_integrity("duplicate source identity").into());
            }
            bound.push(FederatedSource {
                adapter: adapter.clone(),
                manifest,
            });
        }
    }
    if bound.is_empty() {
        return Err(source_unavailable("probe returned no compatible source").into());
    }
    Ok(bound)
}

pub(super) fn parse_source_specs(
    source_specs: Vec<String>,
) -> Result<Vec<ParsedSource>, Box<dyn std::error::Error>> {
    // `database add` canonicalizes each explicitly supplied member once;
    // stored members are revalidated nofollow on every query bind.
    parse_source_specs_inner(source_specs, false, false)
}

fn parse_source_specs_with_policy(
    source_specs: Vec<String>,
    skip_unavailable: bool,
) -> Result<Vec<ParsedSource>, Box<dyn std::error::Error>> {
    parse_source_specs_inner(source_specs, skip_unavailable, true)
}

fn parse_source_specs_inner(
    source_specs: Vec<String>,
    skip_unavailable: bool,
    revalidate_no_symlink: bool,
) -> Result<Vec<ParsedSource>, Box<dyn std::error::Error>> {
    if source_specs.is_empty() {
        return Err(invalid_argument("database must contain at least one member").into());
    }
    if source_specs.len() > 16 {
        return Err(invalid_argument("database member count exceeds the supported limit").into());
    }
    let mut raw = Vec::with_capacity(source_specs.len());
    for spec in source_specs {
        let (adapter_id, root) = spec.split_once('=').ok_or_else(|| {
            invalid_argument("database member must use adapter=/absolute/path syntax")
        })?;
        if !matches!(
            adapter_id,
            "claude-code" | "codex" | "kimi-code" | "opencode"
        ) {
            return Err(invalid_argument("unknown database member adapter").into());
        }
        if root.is_empty() {
            return Err(invalid_argument("database member path cannot be empty").into());
        }
        raw.push((adapter_id.to_string(), PathBuf::from(root)));
    }

    let mut result = Vec::with_capacity(raw.len());
    for (adapter_id, root) in raw {
        if !root.is_absolute() {
            return Err(invalid_argument("database member path must be absolute").into());
        }
        let canonical = if revalidate_no_symlink {
            match canonicalize_member_root_no_symlink(&root) {
                Ok(canonical) => canonical,
                Err(_) if skip_unavailable => continue,
                Err(error) => return Err(error),
            }
        } else {
            fs::canonicalize(&root)
                .map_err(|_| source_unavailable("database member path is unavailable"))?
        };
        result.push(ParsedSource {
            adapter_id,
            root,
            canonical_root: canonical,
        });
    }
    for (index, left) in result.iter().enumerate() {
        for right in result.iter().skip(index + 1) {
            if left.canonical_root == right.canonical_root
                || left.canonical_root.starts_with(&right.canonical_root)
                || right.canonical_root.starts_with(&left.canonical_root)
            {
                return Err(
                    invalid_argument("duplicate or overlapping database member roots").into(),
                );
            }
        }
    }
    Ok(result)
}

/// Revalidates a normalized database member root without following any path
/// component symlink. Configured roots were canonicalized when they were
/// stored, while built-in roots are fixed normalized paths, so query binding
/// can retain the validated path without a second ambient canonicalization.
fn canonicalize_member_root_no_symlink(
    root: &std::path::Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    aql_fs::open_absolute_dir(root)
        .map_err(|_| source_unavailable("database member path is unavailable"))?;
    Ok(root.to_path_buf())
}

pub(super) fn configured_database_inputs(
    name: &str,
) -> Result<Option<SourceInputs>, Box<dyn std::error::Error>> {
    // `all` is reserved for explicit federation and is never a configured
    // database, so its lookup is empty by definition.
    if name == "all" {
        return Ok(None);
    }
    aql_config::validate_database_name(name)?;
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    let config_root = aql_config_root()?;
    match fs::symlink_metadata(&config_root) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    let store = match ConfigStore::open_existing(&config_root, std::slice::from_ref(&state_root)) {
        Ok(store) => store,
        Err(ConfigError::Missing) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !store.list()?.iter().any(|database| database.name == name) {
        return Ok(None);
    }
    let database = store.get_validated(name, std::slice::from_ref(&state_root))?;
    Ok(Some(configured_database_to_inputs(database)?))
}

fn candidate_is_compatible(
    candidate: &DatabaseCandidate,
    deadline: Instant,
    salt: &[u8],
) -> Result<bool, Box<dyn std::error::Error>> {
    if Instant::now() >= deadline {
        return Err(deadline_exceeded("database discovery timed out").into());
    }
    let metadata = match fs::symlink_metadata(&candidate.root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Ok(false),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(false);
    }
    let adapter = read_adapter(candidate.adapter_id, salt)?;
    Ok(adapter
        .probe(&ProbeRequest {
            data_root: candidate.root.to_string_lossy().into_owned(),
        })
        .is_ok_and(|probe| !probe.manifests.is_empty()))
}

/// Validates the public database-selection grammar shared by the CLI and the
/// interactive shell: lowercase ASCII letters, digits, `_` or `-`.
pub(super) fn validate_database_selection_name(name: &str) -> Result<(), CliError> {
    let normalized = name.to_ascii_lowercase();
    if name != normalized
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(invalid_argument(
            "database name must use lowercase ASCII letters, digits, '_' or '-'",
        ));
    }
    Ok(())
}

pub(super) fn resolve_database_inputs(
    name: &str,
) -> Result<SourceInputs, Box<dyn std::error::Error>> {
    validate_database_selection_name(name)?;
    if let Some(inputs) = configured_database_inputs(name)? {
        return Ok(inputs);
    }
    let candidates = database_candidates()?;
    if name == "all" {
        return Ok(SourceInputs {
            source_specs: candidates
                .iter()
                .map(|candidate| {
                    format!(
                        "{}={}",
                        candidate.adapter_id,
                        candidate.root.to_string_lossy()
                    )
                })
                .collect(),
            skip_unavailable: true,
        });
    }
    let candidate = candidates
        .into_iter()
        .find(|candidate| candidate.name == name)
        .ok_or_else(|| database_not_found("unknown database; run SHOW DATABASES"))?;
    Ok(SourceInputs {
        source_specs: vec![format!(
            "{}={}",
            candidate.adapter_id,
            candidate.root.to_string_lossy()
        )],
        skip_unavailable: false,
    })
}

pub(super) fn aql_config_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let root = if let Some(path) = std::env::var_os("AQL_CONFIG_HOME") {
        PathBuf::from(path)
    } else if let Some(path) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(path).join("aql")
    } else {
        PathBuf::from(std::env::var_os("HOME").ok_or_else(|| state_unavailable("HOME is not set"))?)
            .join(".config/aql")
    };
    if !root.is_absolute() {
        return Err(invalid_argument("AQL config root must be absolute").into());
    }
    Ok(root)
}

fn add_configured_database(
    name: String,
    source_specs: Vec<String>,
    acknowledge_persistent_path: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !acknowledge_persistent_path {
        return Err(invalid_argument("database add requires --acknowledge-persistent-path").into());
    }
    aql_config::validate_database_name(&name)?;
    let config_root = aql_config_root()?;
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    let parsed = parse_source_specs(source_specs)?;
    let mut protected = Vec::with_capacity(parsed.len() + 1);
    protected.push(state_root.clone());
    protected.extend(parsed.iter().map(|item| item.canonical_root.clone()));
    let database = ConfiguredDatabase {
        name: name.clone(),
        members: parsed
            .into_iter()
            .map(|item| DatabaseMember {
                adapter_id: item.adapter_id,
                root: item.canonical_root,
            })
            .collect(),
    };
    let store = ConfigStore::create(&config_root, &protected)?;
    let lock = store.acquire_write_lock()?;
    store.add(database, std::slice::from_ref(&state_root), lock)?;
    println!("database={name}");
    println!("status=added");
    Ok(())
}

fn show_configured_database(
    name: String,
    access: Vec<Access>,
) -> Result<(), Box<dyn std::error::Error>> {
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    let store = ConfigStore::open_existing(&aql_config_root()?, std::slice::from_ref(&state_root))?;
    let database = store.get_validated(&name, std::slice::from_ref(&state_root))?;
    let path_access = access_grant(&access).path;
    println!("database={}", database.name);
    println!("members={}", database.members.len());
    for (index, source) in database.members.iter().enumerate() {
        println!("member.{}.adapter={}", index + 1, source.adapter_id);
        if path_access {
            println!(
                "member.{}.root={}",
                index + 1,
                source.root.to_string_lossy()
            );
        } else {
            println!("member.{}.root=masked", index + 1);
        }
    }
    if path_access && !io::stdout().is_terminal() {
        eprintln!("warning=Path access was granted for non-terminal output");
    }
    Ok(())
}

fn remove_configured_database(name: String) -> Result<(), Box<dyn std::error::Error>> {
    aql_config::validate_database_name(&name)?;
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    let store = ConfigStore::open_existing(&aql_config_root()?, std::slice::from_ref(&state_root))?;
    let lock = store.acquire_write_lock()?;
    store.remove(&name, lock)?;
    println!("database={name}");
    println!("status=removed");
    Ok(())
}

pub(super) fn execute_database_command(
    command: DatabaseCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        DatabaseCommand::List => {
            let names = available_database_names()?;
            println!("databases={}", names.len());
            for name in names {
                println!("database={name}");
            }
            Ok(())
        }
        DatabaseCommand::Discover => discover_sources(),
        DatabaseCommand::Add {
            name,
            member,
            acknowledge_persistent_path,
        } => {
            let members = member
                .into_iter()
                .map(|member| {
                    let (agent, path) = member.split_once('=').ok_or_else(|| {
                        invalid_argument("database member must use AGENT=/absolute/path syntax")
                    })?;
                    if agent.is_empty() || path.is_empty() {
                        return Err(invalid_argument(
                            "database member agent and path cannot be empty",
                        )
                        .into());
                    }
                    Ok((agent.to_string(), PathBuf::from(path)))
                })
                .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
            let source = members
                .into_iter()
                .map(
                    |(agent, path)| -> Result<String, Box<dyn std::error::Error>> {
                        let adapter = match agent.as_str() {
                            "claude" | "claude-code" => "claude-code",
                            "codex" => "codex",
                            "kimi" | "kimi-code" => "kimi-code",
                            "opencode" => "opencode",
                            _ => {
                                return Err(invalid_argument(
                                    "unknown database agent; use claude, codex, kimi or opencode",
                                )
                                .into());
                            }
                        };
                        let path = path
                            .to_str()
                            .ok_or_else(|| invalid_argument("database path is not valid UTF-8"))?;
                        Ok(format!("{adapter}={path}"))
                    },
                )
                .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
            add_configured_database(name, source, acknowledge_persistent_path)
        }
        DatabaseCommand::Show { name, access } => {
            if configured_database_inputs(&name)?.is_some() {
                return show_configured_database(name, access);
            }
            let path_access = access_grant(&access).path;
            if name == "all" {
                println!("database=all");
                println!("kind=explicit-federation");
                println!(
                    "members={}",
                    available_database_names()?
                        .into_iter()
                        .filter(|database| database != "all")
                        .collect::<Vec<_>>()
                        .join(",")
                );
                return Ok(());
            }
            let candidate = database_candidates()?
                .into_iter()
                .find(|candidate| candidate.name == name)
                .ok_or_else(|| database_not_found("unknown database"))?;
            let deadline = Instant::now()
                .checked_add(Duration::from_secs(5))
                .ok_or_else(|| invalid_argument("database discovery timeout is invalid"))?;
            let salt: [u8; 32] = rand::random();
            println!("database={name}");
            println!("agent={}", candidate.adapter_id);
            println!(
                "status={}",
                if candidate_is_compatible(&candidate, deadline, &salt)? {
                    "compatible"
                } else {
                    "unavailable"
                }
            );
            if path_access {
                println!("path={}", candidate.root.to_string_lossy());
                if !io::stdout().is_terminal() {
                    eprintln!("warning=Path access was granted for non-terminal output");
                }
            } else {
                println!("path=masked");
            }
            Ok(())
        }
        DatabaseCommand::Remove { name } => remove_configured_database(name),
    }
}

pub(super) fn discover_sources() -> Result<(), Box<dyn std::error::Error>> {
    let candidates = database_candidates()?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or_else(|| invalid_argument("discovery timeout is invalid"))?;
    let ephemeral_salt: [u8; 32] = rand::random();
    let mut results = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if Instant::now() >= deadline {
            return Err(deadline_exceeded("source discovery timed out").into());
        }
        let status = match fs::symlink_metadata(&candidate.root) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => "missing",
            Err(_) => "incompatible",
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                "incompatible"
            }
            Ok(_) => {
                let adapter = read_adapter(candidate.adapter_id, &ephemeral_salt)?;
                match adapter.probe(&ProbeRequest {
                    data_root: candidate.root.to_string_lossy().into_owned(),
                }) {
                    Ok(probe) if !probe.manifests.is_empty() => "compatible",
                    _ => "incompatible",
                }
            }
        };
        results.push((candidate.name, status));
    }
    if Instant::now() >= deadline {
        return Err(deadline_exceeded("source discovery timed out").into());
    }
    for (database, status) in results {
        println!("database={database} status={status}");
    }
    Ok(())
}

pub(super) fn configured_database_names() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    let config_root = aql_config_root()?;
    match fs::symlink_metadata(&config_root) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    match ConfigStore::open_existing(&config_root, std::slice::from_ref(&state_root)) {
        Ok(store) => Ok(store
            .list()?
            .into_iter()
            .map(|database| database.name)
            .collect()),
        Err(ConfigError::Missing) => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn available_database_names() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or_else(|| invalid_argument("database discovery timeout is invalid"))?;
    let salt: [u8; 32] = rand::random();
    let mut names = configured_database_names()?
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut built_in_count = 0_usize;
    for candidate in database_candidates()? {
        if candidate_is_compatible(&candidate, deadline, &salt)? {
            names.insert(candidate.name.to_string());
            built_in_count += 1;
        }
    }
    if built_in_count > 0 {
        names.insert("all".to_string());
    }
    Ok(names.into_iter().collect())
}

pub(super) fn database_is_available(name: &str) -> Result<bool, Box<dyn std::error::Error>> {
    if configured_database_inputs(name)?.is_some() {
        return Ok(true);
    }
    let candidates = database_candidates()?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or_else(|| invalid_argument("database discovery timeout is invalid"))?;
    let salt: [u8; 32] = rand::random();
    if name == "all" {
        for candidate in &candidates {
            if candidate_is_compatible(candidate, deadline, &salt)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    let Some(candidate) = candidates
        .into_iter()
        .find(|candidate| candidate.name == name)
    else {
        return Ok(false);
    };
    candidate_is_compatible(&candidate, deadline, &salt)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symlink_dir(source: &std::path::Path, target: &std::path::Path) -> io::Result<()> {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(source, target)
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_dir(source, target)
        }
    }

    #[test]
    fn reserved_federation_name_never_resolves_to_a_configured_database() {
        assert!(
            configured_database_inputs("all")
                .expect("reserved federation name resolves without configured-database I/O")
                .is_none()
        );
    }

    #[test]
    fn optional_federation_members_skip_missing_roots() {
        let missing =
            std::env::temp_dir().join(format!("aql-missing-source-{:016x}", rand::random::<u64>()));
        let spec = format!("codex={}", missing.to_string_lossy());
        assert!(parse_source_specs(vec![spec.clone()]).is_err());
        assert!(
            parse_source_specs_with_policy(vec![spec], true)
                .expect("optional members parse")
                .is_empty()
        );
    }

    #[test]
    fn query_members_reject_symlink_roots_before_probe() {
        let root = std::env::temp_dir().join(format!(
            "aql-symlinked-member-{:016x}",
            rand::random::<u64>()
        ));
        fs::create_dir(&root).expect("create synthetic member parent");
        let target = root.join("target");
        fs::create_dir(&target).expect("create symlink target");
        let link = root.join("linked-root");
        symlink_dir(&target, &link).expect("create member root symlink");
        // Canonicalization resolves the link, so rejection must happen earlier.
        assert!(fs::canonicalize(&link).is_ok());
        let spec = || format!("codex={}", link.to_string_lossy());

        let Err(error) = parse_source_specs_with_policy(vec![spec()], false) else {
            panic!("symlinked member root must be rejected on the query path");
        };
        assert_eq!(error.to_string(), "database member path is unavailable");

        // `database add` still canonicalizes an explicitly supplied member once.
        assert!(parse_source_specs(vec![spec()]).is_ok());

        // Federation skips the incompatible member instead of following it.
        assert!(
            parse_source_specs_with_policy(vec![spec()], true)
                .expect("optional members parse")
                .is_empty()
        );

        // A real directory passes the nofollow revalidation.
        let real = format!("codex={}", target.to_string_lossy());
        assert!(parse_source_specs_with_policy(vec![real], false).is_ok());
        fs::remove_dir_all(&root).expect("clean synthetic member parent");
    }

    #[test]
    fn discovery_candidates_are_fixed_direct_children_of_known_roots() {
        let home = std::env::temp_dir().join("aql-synthetic-home");
        let data_home = std::env::temp_dir().join("aql-synthetic-data-home");
        let candidates = database_candidates_for(&home, &data_home);
        // Discovery probes exactly this fixed candidate set and never
        // enumerates anything else below HOME or the data home.
        let observed: Vec<(&str, &str)> = candidates
            .iter()
            .map(|candidate| (candidate.name, candidate.adapter_id))
            .collect();
        assert_eq!(
            observed,
            [
                ("claude", "claude-code"),
                ("codex", "codex"),
                ("kimi", "kimi-code"),
                ("opencode", "opencode")
            ]
        );
        for candidate in &candidates {
            // Every candidate is exactly one fixed component below a known
            // root, keeping discovery bounded and non-recursive.
            let parent = candidate
                .root
                .parent()
                .expect("candidate root has a parent");
            assert!(
                parent == home || parent == data_home,
                "candidate root {} is not a direct child of a known root",
                candidate.root.display()
            );
        }
    }
}
