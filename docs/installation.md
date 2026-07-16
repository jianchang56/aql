# 安装、升级与卸载

## 从源码构建

要求 Rust `1.96.1`：

```bash
cargo build --locked --release -p aql
```

二进制位于：

```text
target/release/aql
```

也可以安装到 Cargo bin：

```bash
cargo install --locked --path crates/aql
```

AQL 不自动使用 sudo、不修改 shell 配置、不从网络下载依赖外的可执行文件。

## 验证源码工作区

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo xtask verify
```

## 本地确定性 release

`aql-release` 负责 build manifest、archive、验证、安装、卸载和 Formula 生成。发布内容只包含 allowlist 中的 AQL 文件。下面以 Apple Silicon macOS 和版本 `0.1.0` 为例；其他平台替换 `TARGET`。

```bash
SOURCE_DATE_EPOCH=1 cargo build --locked --release -p aql -p aql-release
TARGET=aarch64-macos

target/release/aql-release build \
  --binary target/release/aql \
  --output-dir ./dist \
  --version 0.1.0 \
  --target "$TARGET"

SHA256=$(awk '{print $1}' \
  "./dist/aql-0.1.0-$TARGET.tar.gz.sha256")

target/release/aql-release verify \
  --archive "./dist/aql-0.1.0-$TARGET.tar.gz" \
  --expected-sha256 "$SHA256" \
  --version 0.1.0 \
  --target "$TARGET"
```

Release workflow 使用固定 SHA 的 GitHub Actions、locked Cargo 构建、四个平台 target、独立 build/publish 权限和 draft=false 发布。

## 安装 release archive

先验证 archive，再安装到新的版本化 prefix。不要覆盖已有 prefix：

```bash
target/release/aql-release install \
  --archive "./dist/aql-0.1.0-$TARGET.tar.gz" \
  --expected-sha256 "$SHA256" \
  --version 0.1.0 \
  --target "$TARGET" \
  --prefix "$HOME/.local/aql/0.1.0"
```

把 `bin/aql` 链接或加入 PATH 的动作由用户明确执行。安装器不会写 Agent 数据、shell rc 或系统目录。

## 升级

升级使用新 prefix：

```text
$HOME/.local/aql/0.1.0
$HOME/.local/aql/0.2.0
```

验证新版本后再切换用户自己管理的 PATH/symlink。配置数据库由 `aql database` 管理并使用私有 `aql-databases-v1` schema。

## 卸载

```bash
target/release/aql-release uninstall \
  --prefix "$HOME/.local/aql/0.1.0"
```

卸载器只删除 manifest 中仍与安装 digest 匹配的文件；被替换的文件或 foreign files 会导致 fail closed。

卸载二进制不会自动删除配置数据库和 installation salt。用户如需删除，应先确认目录是 AQL-owned 且不与任何 Agent root 重叠。

## Homebrew Formula

[`packaging/homebrew/aql.rb.in`](../packaging/homebrew/aql.rb.in) 是发布工具使用的模板，包含尚未替换的版本、URL 和 SHA256 占位符，不能直接交给 Homebrew。正式发布时，Release workflow 会验证四个平台的 archive，生成完整的 `aql.rb` 并将它上传到同一个 GitHub Release。

安装指定版本时，下载该 Release 中生成的 Formula：

```bash
VERSION=0.1.0
curl -fL \
  "https://github.com/jianchang56/aql/releases/download/v$VERSION/aql.rb" \
  -o aql.rb
brew install --formula ./aql.rb
aql --version
```

卸载：

```bash
brew uninstall aql
```

下面的命令用于发布者从四个平台的已验证 archive 手动生成 Formula，不是普通用户的安装命令：

```bash
target/release/aql-release formula \
  --version 0.1.0 \
  --base-url https://github.com/OWNER/REPO/releases/download/v0.1.0 \
  --homepage https://github.com/OWNER/REPO \
  --aarch64-macos-sha256 "$AARCH64_MACOS_SHA256" \
  --x86-64-macos-sha256 "$X86_64_MACOS_SHA256" \
  --aarch64-linux-sha256 "$AARCH64_LINUX_SHA256" \
  --x86-64-linux-sha256 "$X86_64_LINUX_SHA256" \
  --output ./dist/aql.rb
```

Formula 只引用已验证 archive 和固定 SHA256。
