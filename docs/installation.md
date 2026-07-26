# 安装、升级与卸载

## 选择安装方式

| 方式 | 平台 | 当前状态 |
|---|---|---|
| Cargo | macOS / Linux / Windows | 当前可用；从源码构建 |
| Homebrew Formula | macOS / Linux | 首个正式 Release 发布后启用 |
| Bash archive | macOS / Linux | 首个正式 Release 发布后启用 |

当前 GitHub Releases 尚无正式资产。不要运行指向 `releases/latest` 的安装命令；在首个 tag 完成四个平台构建和发布前，请使用 Cargo 源码安装。

预编译 Release 的目标矩阵是 macOS/Linux 的 `aarch64/x86_64`。Windows 已支持核心 CLI、配置与文件输出，但暂不在预编译矩阵中。

## 推荐：让 AI 安装

```text
当前还没有正式 AQL Release。请使用 Rust 1.97.0 和 locked 依赖从 GitHub 源码安装，不要使用 sudo，不要修改 shell 配置，完成后运行 aql --version。
```

## 当前可用：使用 Cargo 从源码安装

需要 Git、rustup 和 Rust `1.97.0`。macOS / Linux：

```bash
git clone https://github.com/jianchang56/aql.git
cd aql
rustup toolchain install 1.97.0
cargo +1.97.0 install --locked --path crates/aql
aql --version
```

Windows PowerShell：

```powershell
git clone https://github.com/jianchang56/aql.git
Set-Location aql
rustup toolchain install 1.97.0
cargo +1.97.0 install --locked --path crates/aql
aql --version
```

Cargo 通常把程序安装到 `~/.cargo/bin`。如果 `aql` 不在 PATH 中，请先从该目录直接运行二进制验证安装，再由用户明确决定是否调整 PATH。AQL 不自动使用 `sudo`，也不修改 `.zshrc`、`.bashrc` 或 PowerShell 配置。

## 下一步：安装 AQL Skill

安装 CLI 后，推荐立即把 Skill 安装到正在使用的 Agent。它让 Agent 默认先检查数据库和 schema，再生成显式、只读且有界的查询：

```text
请从 GitHub 仓库 jianchang56/aql 安装完整的 aql Skill。优先使用 skills CLI 全局安装到当前 Agent；不要只复制 SKILL.md，安装后确认你能识别 $aql。
```

支持 `skills` CLI 时，可以直接运行：

```bash
npx --yes skills add jianchang56/aql --skill aql --global --yes
```

如果环境没有 `npx`，请克隆仓库后复制完整的 `skills/aql` 目录到对应 Agent 的 Skill 目录。

## 预编译 Release 发布后

Release workflow 会在全部 archive 验证成功后发布以下资产：

- `aql-<version>-<target>.tar.gz`
- 每个 archive 对应的 `.sha256`
- 引用四个固定 SHA256 的 `aql.rb`

正式资产存在后，已安装 Homebrew 的 macOS / Linux 用户可以下载该 Release 中的 `aql.rb`，再运行 `brew install --formula ./aql.rb`。网站会在实际 Release 存在后启用对应的一行安装命令。

没有 Homebrew 时，下载当前平台的 archive 与 checksum，验证 SHA256 后再安装到新的版本化目录。验证二进制时使用绝对路径，不依赖 `~/.local/bin` 是否已经加入 PATH。

发布后的完整流程形状如下；将 `<version>` 替换为已经存在的正式版本：

```bash
set -euo pipefail

VERSION=<version>
REPO=https://github.com/jianchang56/aql

case "$(uname -s):$(uname -m)" in
  Darwin:arm64) TARGET=aarch64-macos ;;
  Darwin:x86_64) TARGET=x86_64-macos ;;
  Linux:aarch64|Linux:arm64) TARGET=aarch64-linux ;;
  Linux:x86_64) TARGET=x86_64-linux ;;
  *) echo "Unsupported platform" >&2; exit 1 ;;
esac

ARCHIVE="aql-$VERSION-$TARGET.tar.gz"
BASE="$REPO/releases/download/v$VERSION"
curl -fLO "$BASE/$ARCHIVE"
curl -fLO "$BASE/$ARCHIVE.sha256"

EXPECTED=$(awk '{print $1}' "$ARCHIVE.sha256")
if command -v sha256sum >/dev/null 2>&1; then
  ACTUAL=$(sha256sum "$ARCHIVE" | awk '{print $1}')
else
  ACTUAL=$(shasum -a 256 "$ARCHIVE" | awk '{print $1}')
fi
test "$EXPECTED" = "$ACTUAL"

PREFIX="$HOME/.local/aql/$VERSION"
mkdir -p "$PREFIX" "$HOME/.local/bin"
tar -xzf "$ARCHIVE" -C "$PREFIX" --strip-components=1
ln -s "$PREFIX/bin/aql" "$HOME/.local/bin/aql"
"$HOME/.local/bin/aql" --version
```

`ln -s` 在目标已存在时会失败，不会静默覆盖现有安装。如果 `~/.local/bin` 不在 PATH 中，由用户明确添加。

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

## 发布者生成 Homebrew Formula

[`packaging/homebrew/aql.rb.in`](../packaging/homebrew/aql.rb.in) 是发布工具使用的模板，包含尚未替换的版本、URL 和 SHA256 占位符，不能直接交给 Homebrew。正式发布时，Release workflow 会验证四个平台的 archive，生成完整的 `aql.rb` 并将它上传到同一个 GitHub Release。

下面的命令用于发布者从四个平台的已验证 archive 手动生成 Formula：

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
