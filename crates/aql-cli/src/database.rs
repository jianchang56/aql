use super::*;

pub(super) fn profile_to_source_inputs(
    profile: Profile,
) -> Result<SourceInputs, Box<dyn std::error::Error>> {
    let source_specs = profile
        .sources
        .into_iter()
        .map(|source| {
            let root = source
                .source_root
                .to_str()
                .ok_or("profile source path is invalid")?;
            Ok(format!("{}={root}", source.adapter_id))
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    Ok(SourceInputs {
        data_root: None,
        source_specs,
    })
}

struct DatabaseCandidate {
    name: &'static str,
    adapter_id: &'static str,
    root: PathBuf,
}

fn database_candidates() -> Result<Vec<DatabaseCandidate>, Box<dyn std::error::Error>> {
    let home = PathBuf::from(std::env::var_os("HOME").ok_or("HOME is not set")?);
    if !home.is_absolute() {
        return Err("HOME must be absolute for database discovery".into());
    }
    let data_home = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share"));
    if !data_home.is_absolute() {
        return Err("XDG_DATA_HOME must be absolute for database discovery".into());
    }
    Ok(vec![
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
    ])
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
        _ => Err("unknown source adapter".into()),
    }
}

pub(super) fn bind_sources(
    data_root: Option<PathBuf>,
    source_specs: Vec<String>,
    installation_salt: Vec<u8>,
) -> Result<Vec<FederatedSource>, Box<dyn std::error::Error>> {
    let parsed = parse_sources(data_root, source_specs)?;
    let mut bound = Vec::new();
    let mut source_ids = std::collections::BTreeSet::new();
    for source in parsed {
        let adapter = read_adapter(&source.adapter_id, &installation_salt)?;
        let probe = adapter.probe(&ProbeRequest {
            data_root: source.root.to_string_lossy().into_owned(),
        })?;
        if probe.manifests.is_empty() {
            return Err("probe returned no compatible source".into());
        }
        for manifest in probe.manifests {
            if !source_ids.insert(manifest.source_id.clone()) {
                return Err("duplicate source identity".into());
            }
            bound.push(FederatedSource {
                adapter: adapter.clone(),
                manifest,
            });
        }
    }
    Ok(bound)
}

pub(super) fn parse_sources(
    data_root: Option<PathBuf>,
    source_specs: Vec<String>,
) -> Result<Vec<ParsedSource>, Box<dyn std::error::Error>> {
    if data_root.is_some() && !source_specs.is_empty() {
        return Err("--data-root cannot be combined with --source".into());
    }
    let raw = if let Some(root) = data_root {
        vec![("codex".to_string(), root, false)]
    } else {
        if source_specs.is_empty() {
            return Err("at least one source is required".into());
        }
        if source_specs.len() > 16 {
            return Err("source count exceeds the supported limit".into());
        }
        let mut parsed = Vec::with_capacity(source_specs.len());
        for spec in source_specs {
            let (adapter_id, root) = spec
                .split_once('=')
                .ok_or("source must use adapter=/absolute/path syntax")?;
            if !matches!(
                adapter_id,
                "claude-code" | "codex" | "kimi-code" | "opencode"
            ) {
                return Err("unknown source adapter".into());
            }
            if root.is_empty() {
                return Err("source path cannot be empty".into());
            }
            parsed.push((adapter_id.to_string(), PathBuf::from(root), true));
        }
        parsed
    };

    let mut result = Vec::with_capacity(raw.len());
    for (adapter_id, root, require_absolute) in raw {
        if require_absolute && !root.is_absolute() {
            return Err("source path must be absolute".into());
        }
        let canonical = fs::canonicalize(&root).map_err(|_| "source path is unavailable")?;
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
                return Err("duplicate or overlapping source roots".into());
            }
        }
    }
    Ok(result)
}

pub(super) fn resolve_source_inputs(
    data_root: Option<PathBuf>,
    source_specs: Vec<String>,
    profile_name: Option<String>,
    database_name: Option<String>,
) -> Result<SourceInputs, Box<dyn std::error::Error>> {
    if let Some(database) = database_name {
        if data_root.is_some() || !source_specs.is_empty() || profile_name.is_some() {
            return Err(
                "--database cannot be combined with --data-root, --source or --profile".into(),
            );
        }
        return resolve_database_inputs(&database);
    }
    if profile_name.is_none() {
        if data_root.is_none() && source_specs.is_empty() {
            return Err("at least one explicit source or --profile is required".into());
        }
        return Ok(SourceInputs {
            data_root,
            source_specs,
        });
    }
    if data_root.is_some() || !source_specs.is_empty() {
        return Err("--profile cannot be combined with --data-root or --source".into());
    }
    let name = profile_name.ok_or("profile selection is invalid")?;
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    let store = ConfigStore::open_existing(&aql_config_root()?, std::slice::from_ref(&state_root))?;
    let profile = store.get_validated(&name, std::slice::from_ref(&state_root))?;
    profile_to_source_inputs(profile)
}

pub(super) fn resolve_single_source_root(
    data_root: Option<PathBuf>,
    source_specs: Vec<String>,
    profile_name: Option<String>,
    database_name: Option<String>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let inputs = resolve_source_inputs(data_root, source_specs, profile_name, database_name)?;
    let mut parsed = parse_sources(inputs.data_root, inputs.source_specs)?;
    if parsed.len() != 1 {
        return Err("this index operation requires a database with exactly one source".into());
    }
    Ok(parsed.remove(0).canonical_root)
}

pub(super) fn profile_source_inputs(
    name: &str,
) -> Result<Option<SourceInputs>, Box<dyn std::error::Error>> {
    aql_config::validate_profile_name(name)?;
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
    if !store.list()?.iter().any(|profile| profile.name == name) {
        return Ok(None);
    }
    let profile = store.get_validated(name, std::slice::from_ref(&state_root))?;
    Ok(Some(profile_to_source_inputs(profile)?))
}

fn candidate_is_compatible(
    candidate: &DatabaseCandidate,
    deadline: Instant,
    salt: &[u8],
) -> Result<bool, Box<dyn std::error::Error>> {
    if Instant::now() >= deadline {
        return Err("database discovery timed out".into());
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

pub(super) fn resolve_database_inputs(
    name: &str,
) -> Result<SourceInputs, Box<dyn std::error::Error>> {
    let normalized = name.to_ascii_lowercase();
    if name != normalized
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err("database name must use lowercase ASCII letters, digits, '_' or '-'".into());
    }
    if let Some(inputs) = profile_source_inputs(name)? {
        return Ok(inputs);
    }
    let candidates = database_candidates()?;
    if name == "all" {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(5))
            .ok_or("database discovery timeout is invalid")?;
        let salt: [u8; 32] = rand::random();
        let mut source_specs = Vec::new();
        for candidate in &candidates {
            if candidate_is_compatible(candidate, deadline, &salt)? {
                source_specs.push(format!(
                    "{}={}",
                    candidate.adapter_id,
                    candidate.root.to_string_lossy()
                ));
            }
        }
        if source_specs.is_empty() {
            return Err("database 'all' has no compatible local Agent data".into());
        }
        return Ok(SourceInputs {
            data_root: None,
            source_specs,
        });
    }
    let candidate = candidates
        .into_iter()
        .find(|candidate| candidate.name == name)
        .ok_or("unknown database; run SHOW DATABASES")?;
    Ok(SourceInputs {
        data_root: None,
        source_specs: vec![format!(
            "{}={}",
            candidate.adapter_id,
            candidate.root.to_string_lossy()
        )],
    })
}

pub(super) fn aql_config_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let root = if let Some(path) = std::env::var_os("AQL_CONFIG_HOME") {
        PathBuf::from(path)
    } else if let Some(path) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(path).join("aql")
    } else {
        PathBuf::from(std::env::var_os("HOME").ok_or("HOME is not set")?).join(".config/aql")
    };
    if !root.is_absolute() {
        return Err("AQL config root must be absolute".into());
    }
    Ok(root)
}

pub(super) fn execute_profile_command(
    command: ProfileCommand,
    noun: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config_root = aql_config_root()?;
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    match command {
        ProfileCommand::Add {
            name,
            source,
            acknowledge_persistent_path,
        } => {
            if !acknowledge_persistent_path {
                return Err("profile add requires --acknowledge-persistent-path".into());
            }
            aql_config::validate_profile_name(&name)?;
            let parsed = parse_sources(None, source)?;
            let mut protected = Vec::with_capacity(parsed.len() + 1);
            protected.push(state_root.clone());
            protected.extend(parsed.iter().map(|item| item.canonical_root.clone()));
            let profile = Profile {
                name: name.clone(),
                sources: parsed
                    .into_iter()
                    .map(|item| ProfileSource {
                        adapter_id: item.adapter_id,
                        source_root: item.canonical_root,
                    })
                    .collect(),
            };
            let store = ConfigStore::create(&config_root, &protected)?;
            let lock = store.acquire_write_lock()?;
            store.add(profile, std::slice::from_ref(&state_root), lock)?;
            println!("{noun}={name}");
            println!("status=added");
        }
        ProfileCommand::List => {
            let profiles =
                match ConfigStore::open_existing(&config_root, std::slice::from_ref(&state_root)) {
                    Ok(store) => store.list()?,
                    Err(ConfigError::Missing) => Vec::new(),
                    Err(error) => return Err(error.into()),
                };
            println!("{noun}s={}", profiles.len());
            for profile in profiles {
                let adapters = profile
                    .sources
                    .iter()
                    .map(|source| source.adapter_id.as_str())
                    .collect::<Vec<_>>()
                    .join(",");
                println!(
                    "{noun}={} sources={} adapters={adapters}",
                    profile.name,
                    profile.sources.len()
                );
            }
        }
        ProfileCommand::Show { name, access } => {
            let store =
                ConfigStore::open_existing(&config_root, std::slice::from_ref(&state_root))?;
            let profile = store.get_validated(&name, std::slice::from_ref(&state_root))?;
            let path_access = access_grant(&access).path;
            println!("{noun}={}", profile.name);
            println!("sources={}", profile.sources.len());
            for (index, source) in profile.sources.iter().enumerate() {
                println!("source.{}.adapter={}", index + 1, source.adapter_id);
                if path_access {
                    println!(
                        "source.{}.root={}",
                        index + 1,
                        source.source_root.to_string_lossy()
                    );
                } else {
                    println!("source.{}.root=masked", index + 1);
                }
            }
            if path_access && !io::stdout().is_terminal() {
                eprintln!("warning=Path access was granted for non-terminal output");
            }
        }
        ProfileCommand::Remove { name } => {
            aql_config::validate_profile_name(&name)?;
            let store =
                ConfigStore::open_existing(&config_root, std::slice::from_ref(&state_root))?;
            let lock = store.acquire_write_lock()?;
            store.remove(&name, lock)?;
            println!("{noun}={name}");
            println!("status=removed");
        }
    }
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
            agent,
            path,
            acknowledge_persistent_path,
        } => {
            let members = if member.is_empty() {
                if agent.len() != path.len() {
                    return Err(
                        "database add requires the same number of --agent and --path values".into(),
                    );
                }
                agent.into_iter().zip(path).collect::<Vec<_>>()
            } else {
                member
                    .into_iter()
                    .map(|member| {
                        let (agent, path) = member
                            .split_once('=')
                            .ok_or("database member must use AGENT=/absolute/path syntax")?;
                        if agent.is_empty() || path.is_empty() {
                            return Err("database member agent and path cannot be empty".into());
                        }
                        Ok((agent.to_string(), PathBuf::from(path)))
                    })
                    .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?
            };
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
                                return Err(
                                    "unknown database agent; use claude, codex, kimi or opencode"
                                        .into(),
                                );
                            }
                        };
                        let path = path.to_str().ok_or("database path is not valid UTF-8")?;
                        Ok(format!("{adapter}={path}"))
                    },
                )
                .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
            execute_profile_command(
                ProfileCommand::Add {
                    name,
                    source,
                    acknowledge_persistent_path,
                },
                "database",
            )
        }
        DatabaseCommand::Show { name, access } => {
            if profile_source_inputs(&name)?.is_some() {
                return execute_profile_command(ProfileCommand::Show { name, access }, "database");
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
                .ok_or("unknown database")?;
            let deadline = Instant::now()
                .checked_add(Duration::from_secs(5))
                .ok_or("database discovery timeout is invalid")?;
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
        DatabaseCommand::Remove { name } => {
            execute_profile_command(ProfileCommand::Remove { name }, "database")
        }
    }
}

pub(super) fn execute_source_command(
    command: SourcesCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        SourcesCommand::Discover => discover_sources(),
    }
}

pub(super) fn discover_sources() -> Result<(), Box<dyn std::error::Error>> {
    let candidates = database_candidates()?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or("discovery timeout is invalid")?;
    let ephemeral_salt: [u8; 32] = rand::random();
    let mut results = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if Instant::now() >= deadline {
            return Err("source discovery timed out".into());
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
        return Err("source discovery timed out".into());
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
            .map(|profile| profile.name)
            .collect()),
        Err(ConfigError::Missing) => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn available_database_names() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or("database discovery timeout is invalid")?;
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
    if profile_source_inputs(name)?.is_some() {
        return Ok(true);
    }
    let candidates = database_candidates()?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or("database discovery timeout is invalid")?;
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
