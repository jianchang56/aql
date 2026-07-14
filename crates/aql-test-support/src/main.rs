use std::path::Path;

fn main() {
    if let Err(error) = run() {
        eprintln!("error={error}");
        std::process::exit(1);
    }
}

fn run() -> aql_test_support::TestResult {
    let mut arguments = std::env::args().skip(1);
    let kind = arguments
        .next()
        .ok_or("usage: aql-test-support <claude|codex|kimi|opencode> <output> [count]")?;
    let output = arguments.next().ok_or("fixture output is required")?;
    match kind.as_str() {
        "codex" => {
            let count = arguments
                .next()
                .map(|value| value.parse::<usize>())
                .transpose()?
                .unwrap_or(1_000_000);
            aql_test_support::generate_codex(Path::new(&output), count)?;
        }
        "claude" | "claude-code" => aql_test_support::generate_claude(Path::new(&output))?,
        "kimi" | "kimi-code" => aql_test_support::generate_kimi(Path::new(&output))?,
        "opencode" => aql_test_support::generate_opencode(Path::new(&output))?,
        _ => return Err("unknown fixture kind".into()),
    }
    println!("generated=complete");
    Ok(())
}
