# Linera 2.0 清单式 GitHub 发布与自动更新设计

## 目标

用 Linera 2.0 完全替换 GitHub `danchelam/linera-market` 的旧业务代码，并保留现有机器在启动时从 GitHub 自动更新的能力。发布与更新必须避免上传账号、密码、状态、日志、截图和诊断资料，且禁止强制推送。

## 远程结构

GitHub `main` 替换为：

```text
linera_runner.py
version.json
publish.py
README.md
Linera2.0/
  requirements.txt
  linera2/
    __init__.py
    __main__.py
    account_support.py
    auto_page.py
    auto_runtime.py
    auto_session.py
    auto_tracking.py
    cli.py
    hubstudio.py
    readiness.py
    runtime.py
    store.py
    wallet_support.py
    wallet_recovery.py
    webapp.py
  templates/
```

`linera_runner.py` 继续使用旧文件名，作为从旧 Runner 迁移到新版整包更新器的桥接。旧 `linera_task.py`、`base_module.py` 和 `test_full_flow.py` 从远程删除。

## 自包含运行时

Linera 2.0 不再导入父目录 `base_module.py`。

- `account_support.py` 承担账号表读取和日志输出。
- `wallet_support.py` 承担 Linera 2.0 实际需要的 OKX 解锁和确认辅助能力。
- 钱包密码仅从 `OKX_WALLET_PASSWORD` 环境变量或 `Linera2.0/local_config.json` 读取。
- `local_config.json` 是本地私有文件，不进入 Git、发布清单或删除清单。
- 第一次迁移时，新 Runner 先从本机旧 `base_module.py` 读取必要的钱包配置并写入私有配置，然后才允许删除旧文件。迁移失败时不删除旧配置来源。

`hubshuju.xlsx` 继续是本地文件。新版默认先在 Linera 2.0 目录查找，再向上兼容查找旧位置；更新器不覆盖或删除它。

## `version.json` 协议

清单保留旧 Runner 能识别的 `runner_version`，并新增第二版协议：

```json
{
  "schema_version": 2,
  "runner_version": "2026.07.15.1",
  "app_version": "2026.07.15.1",
  "task_version": "",
  "base_version": "",
  "entrypoint": "Linera2.0/linera2",
  "files": [
    {
      "path": "Linera2.0/linera2/runtime.py",
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  ],
  "remove": [
    "linera_task.py",
    "base_module.py",
    "test_full_flow.py"
  ]
}
```

`files` 只允许发布白名单中的相对路径。`remove` 只允许精确列出的已知旧文件，不接受通配符或目录递归删除。

## 发布流程

`publish.py` 执行以下顺序：

1. 检查工作树，将发布范围限制在新 Runner、发布器、说明文档和 Linera 2.0 运行文件。
2. 运行敏感内容扫描。匹配钱包密码、完整钱包地址、Cookie、Authorization 头或私钥样式时立即中止。
3. 运行发布器测试和 Linera 2.0 全量回归测试。
4. 生成当日递增版本号，计算白名单文件的 SHA-256，原子写入 `version.json`。
5. `git fetch origin main`，仅当 `origin/main` 是当前提交的祖先时继续。
6. 精确 `git add` 发布文件并 `git rm` 已确认的旧文件，不使用 `git add .`。
7. 创建发布提交并执行 `git push origin HEAD:main`。任何拒绝都终止，禁止 `--force`。
8. 从 GitHub Raw 重新下载 `version.json`，校验版本并抽查清单文件哈希。远程验证失败时报告发布未验证，不显示成功。

## 客户端更新流程

### 从旧 Runner 迁移

1. 旧 Runner 按原逻辑读取 `runner_version`。
2. 旧 Runner 从原 Raw URL 下载被重写的 `linera_runner.py` 并重启。
3. 新 Runner 迁移本地私有配置，然后执行清单式整包更新。

### 清单式更新

1. 下载并验证 `version.json` 协议。
2. 拒绝绝对路径、`..`、驱动器路径、白名单外目录和重复冲突路径。
3. 比较本地哈希，只下载内容不同的文件到临时目录。
4. 所有下载文件的 SHA-256 全部通过后，为待替换文件建立备份。
5. 原子替换文件。替换中途失败时，回滚已替换文件。
6. 替换成功且私有配置迁移成功后，才按 `remove` 精确删除旧文件。
7. 启动 Linera 2.0 入口。

## 本地资料保护

以下内容永不进入发布清单，也不进入删除清单：

- `hubshuju.xlsx`
- `Linera2.0/local_config.json`
- `readiness_status.json`
- `auto_sessions.json`
- `alerts.json`
- `*_readiness_*.json`
- `*_auto_*.json`
- `*.log`
- `*.png`
- 诊断目录和截图目录
- Cookie、授权头、完整钱包地址、响应正文和密码

## 错误处理

- 清单不可用：保留本地已安装版本并继续启动。
- 下载失败或哈希不匹配：不替换任何文件。
- 替换失败：回滚已替换文件，保留备份与明确日志。
- 私有配置迁移失败：不删除旧 `base_module.py`，不启动需要钱包密码的写操作。
- 发布时远程分支超前：中止并要求人工合并，不强推。
- GitHub 推送成功但 Raw 验证失败：报告“已推送、远程未验证”，不误报完整成功。

## 测试与验收

自动测试覆盖：

- 清单生成、版本递增和 SHA-256。
- 运行数据、私有配置和敏感文件排除。
- 路径穿越、非白名单路径和重复路径拒绝。
- 哈希错误、下载中断时零替换。
- 多文件替换中途失败时完整回滚。
- 本地状态、账号表和私有配置保留。
- 旧 Runner 能读取的兼容字段和新 Runner 协议解析。
- 账号读取、日志和 OKX 辅助能力迁移后的行为回归。
- Linera 2.0 现有全量测试。

集成验收：

1. 使用本地临时 HTTP 源进行一次旧版到新版的模拟更新。
2. 确认私有文件保留，旧业务文件仅在迁移成功后删除。
3. 执行一次真实 `git push origin HEAD:main`。
4. 从 GitHub Raw 验证第二版清单、版本号和抽查文件哈希。

## 非目标

- 不发布账号表或任何运行状态。
- 不保留旧 Linera 业务流程。
- 不使用 GitHub Release 或 ZIP 包作为更新主通道。
- 不自动强制推送或改写远程历史。
