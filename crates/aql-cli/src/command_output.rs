use super::*;

pub(super) fn generated_command() -> clap::Command {
    Cli::command().name("aql")
}

pub(super) fn build_metadata() -> serde_json::Value {
    serde_json::json!({
        "canonical_schema": "aql-canonical-v0",
        "config_schema": CONFIG_SCHEMA_VERSION,
        "package": env!("CARGO_PKG_NAME"),
        "target": format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
        "version": env!("CARGO_PKG_VERSION"),
    })
}

pub(super) fn render_version(output: VersionOutput) -> Result<String, Box<dyn std::error::Error>> {
    let metadata = build_metadata();
    Ok(match output {
        VersionOutput::Json => serde_json::to_string(&metadata)?,
        VersionOutput::Text => format!(
            "aql {}\ntarget={}\ncanonical_schema={}\nconfig_schema={}",
            metadata["version"]
                .as_str()
                .ok_or("missing build version")?,
            metadata["target"].as_str().ok_or("missing build target")?,
            metadata["canonical_schema"]
                .as_str()
                .ok_or("missing canonical schema")?,
            metadata["config_schema"]
                .as_str()
                .ok_or("missing config schema")?,
        ),
    })
}

pub(super) fn render_completions(shell: CompletionShell) -> Vec<u8> {
    let mut command = generated_command();
    let mut rendered = Vec::new();
    clap_complete::generate(
        clap_complete::Shell::from(shell),
        &mut command,
        "aql",
        &mut rendered,
    );
    rendered
}

pub(super) fn render_manpage() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut rendered = Vec::new();
    clap_mangen::Man::new(generated_command()).render(&mut rendered)?;
    Ok(rendered)
}
