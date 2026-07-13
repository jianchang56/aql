use aql_release::{Error, PAYLOAD, build, install, publish, uninstall, validate_version, verify};
use clap::{Parser, Subcommand};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Build {
        #[arg(long)]
        binary: PathBuf,
        #[arg(long)]
        output_dir: PathBuf,
        #[arg(long)]
        target: String,
        #[arg(long)]
        version: String,
    },
    Verify {
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        expected_sha256: String,
        #[arg(long)]
        target: String,
        #[arg(long)]
        version: String,
    },
    Install {
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        expected_sha256: String,
        #[arg(long)]
        prefix: PathBuf,
        #[arg(long)]
        target: String,
        #[arg(long)]
        version: String,
        #[arg(long)]
        plan: bool,
    },
    Uninstall {
        #[arg(long)]
        prefix: PathBuf,
    },
    Formula {
        #[arg(long)]
        version: String,
        #[arg(long)]
        base_url: String,
        #[arg(long)]
        homepage: String,
        #[arg(long)]
        aarch64_macos_sha256: String,
        #[arg(long)]
        x86_64_macos_sha256: String,
        #[arg(long)]
        aarch64_linux_sha256: String,
        #[arg(long)]
        x86_64_linux_sha256: String,
        #[arg(long)]
        output: PathBuf,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error={error}");
        std::process::exit(1);
    }
}

fn run() -> aql_release::Result<()> {
    match Args::parse().command {
        Commands::Build {
            binary,
            output_dir,
            target,
            version,
        } => {
            let epoch = std::env::var("SOURCE_DATE_EPOCH")
                .unwrap_or_else(|_| "1".into())
                .parse::<u32>()
                .map_err(|_| Error::new("SOURCE_DATE_EPOCH must fit u32"))?;
            let bytes = build(&root()?, &binary, &version, &target, epoch)?;
            let name = format!("aql-{version}-{target}.tar.gz");
            let digest = hex::encode(Sha256::digest(&bytes));
            publish(&output_dir.join(&name), &bytes, 0o644)?;
            publish(
                &output_dir.join(format!("{name}.sha256")),
                format!("{digest}  {name}\n").as_bytes(),
                0o644,
            )?;
            println!("archive={}", output_dir.join(name).display());
            println!("sha256={digest}");
        }
        Commands::Verify {
            archive,
            expected_sha256,
            target,
            version,
        } => {
            let result = verify(&archive, &expected_sha256, &version, &target)?;
            println!("archive={}", archive.canonicalize()?.display());
            println!("sha256={}", result.digest);
            println!("version={version}");
            println!("target={target}");
            println!("status=verified");
        }
        Commands::Install {
            archive,
            expected_sha256,
            prefix,
            target,
            version,
            plan,
        } => {
            install(&archive, &expected_sha256, &version, &target, &prefix, plan)?;
            println!("prefix={}", prefix.display());
            if plan {
                println!("sha256={expected_sha256}");
                println!("files={}", PAYLOAD.len() + 2);
                println!("status=planned");
            } else {
                println!("status=installed");
            }
        }
        Commands::Uninstall { prefix } => {
            let retained = uninstall(&prefix)?;
            println!("prefix={}", prefix.display());
            println!(
                "status={}",
                if retained {
                    "managed-files-removed"
                } else {
                    "uninstalled"
                }
            );
            if retained {
                println!(
                    "warning=prefix retained because it contains files outside the uninstall allowlist"
                );
            }
        }
        Commands::Formula {
            version,
            base_url,
            homepage,
            aarch64_macos_sha256,
            x86_64_macos_sha256,
            aarch64_linux_sha256,
            x86_64_linux_sha256,
            output,
        } => {
            validate_version(&version)?;
            url(&base_url)?;
            url(&homepage)?;
            for value in [
                &aarch64_macos_sha256,
                &x86_64_macos_sha256,
                &aarch64_linux_sha256,
                &x86_64_linux_sha256,
            ] {
                digest(value)?;
            }
            let mut formula =
                std::fs::read_to_string(root()?.join("packaging/homebrew/aql.rb.in"))?;
            let replacements = BTreeMap::from([
                ("@AARCH64_LINUX_SHA256@", aarch64_linux_sha256.as_str()),
                ("@AARCH64_MACOS_SHA256@", aarch64_macos_sha256.as_str()),
                ("@BASE_URL@", base_url.trim_end_matches('/')),
                ("@HOMEPAGE@", homepage.trim_end_matches('/')),
                ("@VERSION@", version.as_str()),
                ("@X86_64_LINUX_SHA256@", x86_64_linux_sha256.as_str()),
                ("@X86_64_MACOS_SHA256@", x86_64_macos_sha256.as_str()),
            ]);
            for (from, to) in replacements {
                formula = formula.replace(from, to);
            }
            if formula.contains('@') {
                return Err(Error::new("unresolved formula placeholder"));
            }
            publish(&output, formula.as_bytes(), 0o644)?;
            println!("formula={}", output.canonicalize()?.display());
            println!("status=generated-not-published");
        }
    }
    Ok(())
}

fn root() -> aql_release::Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| Error::new("repository root not found"))
}
fn digest(value: &str) -> aql_release::Result<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(Error::new("invalid SHA-256"))
    }
}
fn url(value: &str) -> aql_release::Result<()> {
    if value.starts_with("https://")
        && !value
            .bytes()
            .any(|b| matches!(b, b'\r' | b'\n' | b'?' | b'#'))
    {
        Ok(())
    } else {
        Err(Error::new(
            "URL must be fixed HTTPS without query or fragment",
        ))
    }
}
