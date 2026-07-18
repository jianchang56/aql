use std::fs;
use std::path::Path;

const GENERATORS: [&str; 4] = ["claude", "codex", "kimi", "opencode"];
const SCHEMA_ASSETS: [&str; 2] = ["codex-schema.sql", "opencode-schema.sql"];

// Fixture generators must contain only reserved synthetic values: a real-home
// prefix, email domain, private key, or token shape in any generator means
// real Agent data contaminated the committed fixtures. The literal fragments
// are concatenated so this guard does not trip itself or sibling scans.
#[test]
fn all_fixture_generators_contain_only_synthetic_literals() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let forbidden_patterns = [
        ["/", "Users", "/"].concat(),
        ["@", "example.com"].concat(),
        ["BEGIN ", "PRIVATE KEY"].concat(),
        ["s", "k", "-"].concat(),
    ];
    let mut scanned = Vec::new();
    for generator in GENERATORS {
        let source = fs::read_to_string(root.join("src").join(format!("{generator}.rs")))
            .expect("fixture generator must be readable");
        assert!(
            source.contains("Synthetic"),
            "{generator} fixture generator lost its synthetic markers"
        );
        scanned.push((generator, source));
    }
    for asset in SCHEMA_ASSETS {
        let source = fs::read_to_string(root.join("assets").join(asset))
            .expect("fixture schema asset must be readable");
        scanned.push((asset, source));
    }
    for (name, source) in scanned {
        for forbidden in &forbidden_patterns {
            assert!(
                !source.contains(forbidden),
                "{name} fixture source contains forbidden literal {forbidden}"
            );
        }
    }
}
