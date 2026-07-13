# AQL 安装、升级与卸载

当前支持 macOS 和 Linux。Windows 暂缓。仓库没有远程自动安装器，也不会修改 shell 配置或调用 `sudo`。

## GitHub Release 与 Homebrew

推送经过验证的 `vMAJOR.MINOR.PATCH` tag 后，release workflow 会构建四个平台的确定性归档：

```text
aql-VERSION-aarch64-linux.tar.gz
aql-VERSION-aarch64-macos.tar.gz
aql-VERSION-x86_64-linux.tar.gz
aql-VERSION-x86_64-macos.tar.gz
```

每个归档都有独立 `.sha256`，并附带按四个平台 checksum 生成的 `aql.rb`。Release 在全部归档、校验和与 Formula 完成前保持 draft。仓库发布后可以使用对应 Release 页面中的 Formula：

```bash
brew install --formula ./aql.rb
```

项目不提供 `curl | sh` 安装方式。

## 从源码构建

要求：

- Rust `1.88.0`（仓库的 `rust-toolchain.toml` 会固定版本）
- Git
- macOS 或 Linux

```bash
cd aql
cargo build --locked --release -p aql-cli
./target/release/aql --version
```

也可以安装到 Cargo 的 bin 目录：

```bash
cargo install --locked --path crates/aql-cli
```

将程序复制到现有 `PATH` 目录是用户自己的选择，例如：

```bash
mkdir -p "$HOME/.local/bin"
install -m 755 target/release/aql "$HOME/.local/bin/aql"
```

AQL 不会自动编辑 `PATH`。确认 `$HOME/.local/bin` 已由你的 shell 配置管理。

## 自动补全和 man page

```bash
aql completions bash > ./aql.bash
aql completions zsh > ./_aql
aql completions fish > ./aql.fish
aql man > ./aql.1
man ./aql.1
```

生成物来自 public CLI tree，不包含隐藏测试参数、真实路径或 SQL history。

## 构建可验证的本地 release

Rust release 工具只处理本地文件，不下载或发布任何内容：

```bash
SOURCE_DATE_EPOCH=1 cargo build --locked --release -p aql-cli

# macOS Apple Silicon；其他平台使用 version --output json 中的 target 值
TARGET=aarch64-macos

SOURCE_DATE_EPOCH=1 cargo run --locked -p aql-release -- build \
  --binary target/release/aql \
  --output-dir ./target/local-release \
  --version 0.1.0 \
  --target "$TARGET"

SHA256=$(awk '{print $1}' \
  "./target/local-release/aql-0.1.0-$TARGET.tar.gz.sha256")

cargo run --locked -p aql-release -- verify \
  --archive "./target/local-release/aql-0.1.0-$TARGET.tar.gz" \
  --expected-sha256 "$SHA256" \
  --version 0.1.0 \
  --target "$TARGET"
```

校验覆盖 SHA-256、target/version、manifest、entry allowlist、权限、traversal、symlink/hardlink、duplicate entry 和 gzip trailing data。

## 从本地 release 安装

安装 prefix 必须是新的绝对路径，父目录由当前用户拥有：

```bash
mkdir -p "$HOME/.local/aql"
chmod 700 "$HOME/.local/aql"

cargo run --locked -p aql-release -- install \
  --archive "./target/local-release/aql-0.1.0-$TARGET.tar.gz" \
  --expected-sha256 "$SHA256" \
  --version 0.1.0 \
  --target "$TARGET" \
  --prefix "$HOME/.local/aql/0.1.0" \
  --plan

cargo run --locked -p aql-release -- install \
  --archive "./target/local-release/aql-0.1.0-$TARGET.tar.gz" \
  --expected-sha256 "$SHA256" \
  --version 0.1.0 \
  --target "$TARGET" \
  --prefix "$HOME/.local/aql/0.1.0"

"$HOME/.local/aql/0.1.0/bin/aql" --version
```

installer 拒绝 URL、stdin archive、已有 prefix、symlink/unsafe parent、checksum mismatch 和 Agent/AQL data root overlap。

## 升级

升级使用新的版本化 prefix，不覆盖旧安装：

```bash
cargo run --locked -p aql-release -- install \
  --archive "/local/path/aql-0.2.0-$TARGET.tar.gz" \
  --expected-sha256 "<64-lowercase-hex>" \
  --version 0.2.0 \
  --target "$TARGET" \
  --prefix "$HOME/.local/aql/0.2.0"
```

验证新版本后，由用户手动切换 launcher 或 PATH。旧版本不会自动删除。

## 卸载

```bash
cargo run --locked -p aql-release -- uninstall \
  --prefix "$HOME/.local/aql/0.1.0"
```

uninstaller 只删除安装 manifest 中的固定文件。它不会删除：

- AQL config/profile
- AQL index
- Action audit/state
- Agent 数据
- prefix 中的未知文件

存在未知文件时，prefix 会被保留并输出 warning。
