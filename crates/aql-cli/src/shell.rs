use super::*;

pub(super) fn drain_shell_statements(buffer: &mut String) -> Result<Vec<String>, &'static str> {
    if buffer.len() > 64 * 1024 {
        return Err("statement exceeds the fixed 64 KiB limit");
    }
    let bytes = buffer.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0_usize;
    let mut index = 0_usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut line_comment = false;
    let mut block_comment = false;
    while index < bytes.len() {
        if line_comment {
            if bytes[index] == b'\n' {
                line_comment = false;
            }
            index += 1;
            continue;
        }
        if block_comment {
            if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                block_comment = false;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        if single_quote {
            if bytes[index] == b'\'' {
                if bytes.get(index + 1) == Some(&b'\'') {
                    index += 2;
                } else {
                    single_quote = false;
                    index += 1;
                }
            } else {
                index += 1;
            }
            continue;
        }
        if double_quote {
            if bytes[index] == b'"' {
                if bytes.get(index + 1) == Some(&b'"') {
                    index += 2;
                } else {
                    double_quote = false;
                    index += 1;
                }
            } else {
                index += 1;
            }
            continue;
        }
        match bytes[index] {
            b'\'' => single_quote = true,
            b'"' => double_quote = true,
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                line_comment = true;
                index += 1;
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                block_comment = true;
                index += 1;
            }
            b';' => {
                let statement = buffer[start..index].trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    if start > 0 {
        buffer.drain(..start);
    }
    Ok(statements)
}

pub(super) fn shell_words(statement: &str) -> Vec<String> {
    let mut command = statement.trim_start();
    loop {
        if let Some(rest) = command.strip_prefix("--") {
            command = rest
                .split_once('\n')
                .map_or("", |(_, remainder)| remainder)
                .trim_start();
            continue;
        }
        if let Some(rest) = command.strip_prefix("/*") {
            command = rest
                .split_once("*/")
                .map_or("", |(_, remainder)| remainder)
                .trim_start();
            continue;
        }
        break;
    }
    command
        .split_ascii_whitespace()
        .map(|word| word.to_ascii_uppercase())
        .collect()
}

pub(super) fn query_type_name(data_type: QueryDataType) -> &'static str {
    match data_type {
        QueryDataType::Text => "TEXT",
        QueryDataType::Int64 => "BIGINT",
        QueryDataType::Bool => "BOOLEAN",
        QueryDataType::Timestamp => "TIMESTAMP",
        QueryDataType::Json => "JSON",
    }
}

pub(super) fn access_class_name(access: AccessClass) -> &'static str {
    match access {
        AccessClass::Safe => "SAFE",
        AccessClass::Path => "PATH",
        AccessClass::Content => "CONTENT",
        AccessClass::ToolInput => "TOOL_INPUT",
        AccessClass::ToolOutput => "TOOL_OUTPUT",
        AccessClass::Secret => "SECRET",
    }
}

const SQL_EXAMPLES: [(&str, &str); 4] = [
    (
        "sessions-by-model",
        "SELECT model, COUNT(*) AS sessions\nFROM sessions\nGROUP BY model\nORDER BY sessions DESC;",
    ),
    (
        "token-usage",
        "SELECT agent_id, SUM(total_tokens) AS total_tokens\nFROM usage\nGROUP BY agent_id\nORDER BY total_tokens DESC;",
    ),
    (
        "recent-sessions",
        "SELECT session_id, agent_id, model, updated_at\nFROM sessions\nORDER BY updated_at DESC\nLIMIT 20;",
    ),
    (
        "recent-tools",
        "SELECT agent_id, tool_name, started_at\nFROM tool_calls\nORDER BY started_at DESC\nLIMIT 20;",
    ),
];

pub(super) fn render_schema(
    table: Option<String>,
    output: SchemaOutput,
) -> Result<(), Box<dyn std::error::Error>> {
    let selected = if let Some(table) = table {
        let table = table.to_ascii_lowercase();
        vec![
            QUERY_SCHEMAS
                .iter()
                .find(|schema| schema.name == table)
                .ok_or("unknown canonical table")?,
        ]
    } else {
        QUERY_SCHEMAS.iter().collect::<Vec<_>>()
    };
    match output {
        SchemaOutput::Table => {
            println!("table\tcolumn\ttype\tnullable\taccess");
            for schema in selected {
                for column in schema.columns {
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        schema.name,
                        column.name,
                        query_type_name(column.data_type),
                        if column.nullable { "YES" } else { "NO" },
                        access_class_name(column.access),
                    );
                }
            }
        }
        SchemaOutput::Json => {
            let tables = selected
                .into_iter()
                .map(|schema| {
                    serde_json::json!({
                        "name": schema.name,
                        "columns": schema.columns.iter().map(|column| serde_json::json!({
                            "name": column.name,
                            "type": query_type_name(column.data_type),
                            "nullable": column.nullable,
                            "access": access_class_name(column.access).to_ascii_lowercase(),
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect::<Vec<_>>();
            println!("{}", serde_json::to_string(&tables)?);
        }
    }
    Ok(())
}

pub(super) fn render_schema_list(output: SchemaOutput) -> Result<(), Box<dyn std::error::Error>> {
    match output {
        SchemaOutput::Table => {
            println!("table");
            for schema in QUERY_SCHEMAS {
                println!("{}", schema.name);
            }
        }
        SchemaOutput::Json => println!(
            "{}",
            serde_json::to_string(
                &QUERY_SCHEMAS
                    .iter()
                    .map(|schema| schema.name)
                    .collect::<Vec<_>>()
            )?
        ),
    }
    Ok(())
}

pub(super) fn render_examples(name: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(name) = name {
        let sql = SQL_EXAMPLES
            .iter()
            .find_map(|(candidate, sql)| (*candidate == name).then_some(*sql))
            .ok_or("unknown example; run `aql examples`")?;
        println!("{sql}");
    } else {
        println!("example");
        for (name, _) in SQL_EXAMPLES {
            println!("{name}");
        }
    }
    Ok(())
}

pub(super) fn grant_shell_access(
    words: &[String],
    access: &mut Vec<Access>,
) -> Result<(), &'static str> {
    let grant = match words {
        [grant, class, for_word, session]
            if grant == "GRANT" && for_word == "FOR" && session == "SESSION" =>
        {
            match class.as_str() {
                "CONTENT" => Access::Content,
                "PATH" => Access::Path,
                _ => return Err("expected CONTENT, PATH, TOOL INPUT or TOOL OUTPUT"),
            }
        }
        [grant, tool, direction, for_word, session]
            if grant == "GRANT" && tool == "TOOL" && for_word == "FOR" && session == "SESSION" =>
        {
            match direction.as_str() {
                "INPUT" => Access::ToolInput,
                "OUTPUT" => Access::ToolOutput,
                _ => return Err("expected TOOL INPUT or TOOL OUTPUT"),
            }
        }
        _ => return Err("use GRANT <class> FOR SESSION"),
    };
    if !access.contains(&grant) {
        access.push(grant);
    }
    Ok(())
}

struct ShellHelper {
    candidates: Vec<String>,
}

impl ShellHelper {
    fn new(databases: &[String]) -> Self {
        let mut candidates = vec![
            "SELECT",
            "WITH",
            "EXPLAIN",
            "SHOW",
            "DATABASES",
            "TABLES",
            "ACCESS",
            "STATUS",
            "USE",
            "DESCRIBE",
            "GRANT",
            "REVOKE",
            "CONTENT",
            "PATH",
            "TOOL",
            "INPUT",
            "OUTPUT",
            "FOR",
            "SESSION",
            "HELP",
            "EXIT",
            "QUIT",
            "FROM",
            "WHERE",
            "GROUP",
            "BY",
            "ORDER",
            "LIMIT",
            "JOIN",
            "ON",
            "AS",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        candidates.extend(QUERY_SCHEMAS.iter().map(|schema| schema.name.to_owned()));
        candidates.extend(
            QUERY_SCHEMAS
                .iter()
                .flat_map(|schema| schema.columns.iter().map(|column| column.name.to_owned())),
        );
        candidates.extend(databases.iter().cloned());
        candidates.sort();
        candidates.dedup();
        Self { candidates }
    }
}

impl Helper for ShellHelper {}
impl Validator for ShellHelper {}
impl Highlighter for ShellHelper {}
impl Hinter for ShellHelper {
    type Hint = String;
}
impl Completer for ShellHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        position: usize,
        _context: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let start = line[..position]
            .rfind(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
            .map_or(0, |index| index + 1);
        let prefix = line[start..position].to_ascii_lowercase();
        let matches = self
            .candidates
            .iter()
            .filter(|candidate| candidate.to_ascii_lowercase().starts_with(&prefix))
            .map(|candidate| Pair {
                display: candidate.clone(),
                replacement: candidate.clone(),
            })
            .collect();
        Ok((start, matches))
    }
}

pub(super) async fn run_shell(
    initial_database: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err("interactive shell requires terminal stdin and stdout".into());
    }
    let mut selected_database = None;
    if let Some(database) = initial_database {
        let database = database.to_ascii_lowercase();
        if !database_is_available(&database)? {
            return Err("unknown or unavailable database; run SHOW DATABASES".into());
        }
        selected_database = Some(database);
    }
    let mut access = Vec::new();
    let mut buffer = String::new();
    let databases = available_database_names()?;
    let editor_config = rustyline::Config::builder()
        .max_history_size(100)?
        .history_ignore_space(true)
        .build();
    let mut editor =
        rustyline::Editor::<ShellHelper, rustyline::history::DefaultHistory>::with_config(
            editor_config,
        )?;
    editor.set_helper(Some(ShellHelper::new(&databases)));
    for line in shell_welcome(&databases, selected_database.as_deref()) {
        println!("{line}");
    }
    loop {
        let prompt = if buffer.is_empty() {
            shell_prompt(selected_database.as_deref(), &access)
        } else {
            "      -> ".to_owned()
        };
        let line = match editor.readline(&prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                buffer.clear();
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!();
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        buffer.push_str(&line);
        buffer.push('\n');
        let statements = match drain_shell_statements(&mut buffer) {
            Ok(statements) => statements,
            Err(error) => {
                eprintln!("ERROR: {error}");
                buffer.clear();
                continue;
            }
        };
        for statement in statements {
            let _ = editor.add_history_entry(statement.as_str());
            let words = shell_words(&statement);
            match words.as_slice() {
                [show, databases] if show == "SHOW" && databases == "DATABASES" => {
                    println!("database");
                    for database in available_database_names()? {
                        println!("{database}");
                    }
                }
                [use_word, database] if use_word == "USE" => {
                    let database = database.to_ascii_lowercase();
                    if !database_is_available(&database)? {
                        eprintln!(
                            "ERROR: Unknown or unavailable database '{database}'. Run SHOW DATABASES;"
                        );
                    } else {
                        selected_database = Some(database.clone());
                        println!("Database changed to {database}");
                    }
                }
                [show, tables] if show == "SHOW" && tables == "TABLES" => {
                    println!("table");
                    for schema in QUERY_SCHEMAS {
                        println!("{}", schema.name);
                    }
                }
                [show, access_word] if show == "SHOW" && access_word == "ACCESS" => {
                    println!("access");
                    if access.is_empty() {
                        println!("none");
                    } else {
                        for grant in &access {
                            println!("{}", format!("{grant:?}").to_ascii_lowercase());
                        }
                    }
                }
                [show, status] if show == "SHOW" && status == "STATUS" => {
                    println!(
                        "database={} access_grants={} timeout=30s max_records=100000 history=persistent:false",
                        selected_database.as_deref().unwrap_or("none"),
                        access.len(),
                    );
                }
                [describe, table] if describe == "DESCRIBE" || describe == "DESC" => {
                    let table = table.to_ascii_lowercase();
                    if let Some(schema) = QUERY_SCHEMAS.iter().find(|schema| schema.name == table) {
                        println!("column\ttype\tnullable\taccess");
                        for column in schema.columns {
                            println!(
                                "{}\t{}\t{}\t{}",
                                column.name,
                                query_type_name(column.data_type),
                                if column.nullable { "YES" } else { "NO" },
                                access_class_name(column.access)
                            );
                        }
                    } else {
                        eprintln!("ERROR: Unknown table '{table}'. Run SHOW TABLES;");
                    }
                }
                [revoke, all, for_word, session]
                    if revoke == "REVOKE"
                        && all == "ALL"
                        && for_word == "FOR"
                        && session == "SESSION" =>
                {
                    access.clear();
                    println!("Session access revoked");
                }
                [first, ..] if first == "GRANT" => match grant_shell_access(&words, &mut access) {
                    Ok(()) => println!("Session access granted"),
                    Err(error) => eprintln!("ERROR: {error}"),
                },
                [exit] if exit == "EXIT" || exit == "QUIT" => return Ok(()),
                [help] if help == "HELP" => {
                    println!(
                        "SHOW DATABASES; | USE <database>; | SHOW TABLES; | DESCRIBE <table>;"
                    );
                    println!(
                        "SHOW ACCESS; | SHOW STATUS; | GRANT <class> FOR SESSION; | REVOKE ALL FOR SESSION;"
                    );
                    println!("SELECT ...; | WITH ... SELECT ...; | EXPLAIN SELECT ...; | EXIT;");
                }
                [first, ..] if first == "SELECT" || first == "WITH" || first == "EXPLAIN" => {
                    let Some(database) = selected_database.clone() else {
                        eprintln!(
                            "ERROR: No database selected. Run SHOW DATABASES; and USE <database>;"
                        );
                        continue;
                    };
                    let query = Cli {
                        error_format: ErrorFormat::Text,
                        quiet: false,
                        command: Some(Command::Query {
                            database,
                            output: Output::Table,
                            output_file: None,
                            access: access.clone(),
                            param: Vec::new(),
                            limits: ExecutionLimits {
                                max_output_bytes: 64 * 1024 * 1024,
                                timeout: Duration::from_secs(30),
                            },
                            diagnostics: false,
                            shell_summary: true,
                            sql: Some(statement),
                            file: None,
                            stdin: false,
                        }),
                    };
                    if let Err(error) = Box::pin(run(query)).await {
                        eprintln!("{}", shell_query_error(error.as_ref()));
                    }
                }
                _ => {
                    eprintln!(
                        "ERROR: Expected HELP, SHOW, USE, DESCRIBE, GRANT, REVOKE, SELECT, EXPLAIN or EXIT"
                    )
                }
            }
        }
    }
}

pub(super) fn shell_query_error(error: &(dyn std::error::Error + 'static)) -> String {
    if let Some(aql_engine_datafusion::QueryError::AccessDenied(access)) =
        error.downcast_ref::<aql_engine_datafusion::QueryError>()
    {
        let grant = match *access {
            "content" => "CONTENT",
            "path" => "PATH",
            "tool-input" => "TOOL INPUT",
            "tool-output" => "TOOL OUTPUT",
            _ => return format!("ERROR: {error}"),
        };
        return format!(
            "ERROR: Query requires {access} access. Run GRANT {grant} FOR SESSION; only if it is genuinely needed."
        );
    }
    format!("ERROR: {error}")
}

pub(super) fn shell_prompt(selected_database: Option<&str>, access: &[Access]) -> String {
    let grants = if access.is_empty() {
        "safe".to_string()
    } else {
        access
            .iter()
            .map(|grant| match grant {
                Access::Path => "path",
                Access::Content => "content",
                Access::ToolInput => "tool-input",
                Access::ToolOutput => "tool-output",
            })
            .collect::<Vec<_>>()
            .join("+")
    };
    format!("aql[{}|{grants}]> ", selected_database.unwrap_or("none"))
}

pub(super) fn shell_welcome(databases: &[String], selected_database: Option<&str>) -> Vec<String> {
    let mut lines = vec![
        "AQL interactive shell. End statements with ';'.".to_string(),
        format!("Known databases: {}", databases.join(", ")),
    ];
    if let Some(database) = selected_database {
        lines.push(format!("Selected database: {database}"));
        lines.push("Next: SELECT * FROM sessions LIMIT 10;".to_string());
    } else {
        lines.extend([
            "1. SHOW DATABASES;".to_string(),
            "2. USE <database>;".to_string(),
            "3. SELECT * FROM sessions LIMIT 10;".to_string(),
        ]);
    }
    lines
}
