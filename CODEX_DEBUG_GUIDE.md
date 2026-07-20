# Linera2.0 另一台电脑 Codex 调试指南

这份文档供目标电脑上的 Codex 使用。目标是先确认安装和账号状态，再进行一次受控的
测试网完整流程验证。不要一开始就批量运行或修改交易参数。

## 一、项目与入口

- 项目根目录应包含 `linera_runner.exe`、`hubshuju.xlsx` 和 `Linera2.0/`。
- 正式入口是根目录的 `linera_runner.exe`。
- Python 备用入口是根目录的 `linera_runner.py`。
- 禁止运行已废弃的 `linera_task.py`、`base_module.py` 或 `test_full_flow.py`。
- 当前公开运行包版本为 `2026.07.17.2`。

开始前先阅读：

1. `README.md`
2. `CODEX_HANDOFF.md`
3. `Linera2.0/README.md`

## 二、安全边界

Codex 可以确认文件是否存在，但不得读取、输出、复制到对话或上传以下内容：

- `hubshuju.xlsx` 的账号数据
- `Linera2.0/local_config.json` 的钱包密码
- Cookie、Authorization、私钥、助记词
- 完整钱包地址和网络响应正文
- `auto_sessions.json`、`readiness_status.json`、`alerts.json` 中不必要的私人明细
- 日志和截图中的敏感字段

不得使用 `git add .`，不得 force-push，不得关闭或删除 HubStudio 浏览器环境。
诊断期间不得擅自增加下注金额、目标轮次或并发数。

## 三、目标电脑的准备工作

1. 等旧脚本当前轮次结束后关闭旧 EXE，HubStudio 窗口保持打开。
2. 备份原项目文件夹。
3. 只替换根目录的 `linera_runner.exe`。
4. 保留目标电脑原有的账号表、钱包配置和状态文件。
5. 确认 HubStudio 已启动，本地 API 地址为 `http://127.0.0.1:6873`。
6. 确认 Excel 中的账号编号与 HubStudio 环境编号一致。

当前交付 EXE 可用以下命令计算哈希：

```powershell
Get-FileHash .\linera_runner.exe -Algorithm SHA256
```

当前构建的 SHA-256 应为：

```text
F280B6640F14B5609839BF59A30C0CEB3E4D0999566A4605B037819294554B94
```

## 四、分阶段调试

### 阶段 1：只读状态检测

先不要使用 `--auto-session`：

```powershell
.\linera_runner.exe --web --workers 1 --timeout 60
```

打开 `http://127.0.0.1:5060`，确认每个账号显示钱包连接、Coins、后端和 Ride UI
状态。此阶段不得点击 Connect、Auto、HIGHER 或 LOWER。

重点判断：

- `ready`：可以进入下一阶段。
- `wallet_disconnected`：先检查钱包扩展和当前浏览器环境。
- `wallet_syncing`：等待链数据同步，不要立即批量刷新。
- `backend_unavailable`：记录脱敏状态与时间，不要输出响应正文。
- `page_loading`：确认目标页面和 Ride 组件是否完整加载。
- `insufficient_balance`：钱包已同步，但当前账号不能执行任务。
- `browser_unreachable`：检查 HubStudio API、窗口编号和 CDP 端口。

### 阶段 2：单账号一轮完整流程

由操作者准备一个只包含测试账号的本地 Excel，例如 `hubshuju_test.xlsx`。Codex 不得
展示其内容。然后执行：

```powershell
.\linera_runner.exe --accounts .\hubshuju_test.xlsx --web --auto-session `
  --integration-target 1 --workers 1 --auto-timeout 1200
```

这一步只允许一个账号、一个完整轮次。需要确认：钱包连接、Auto 配置、HIGHER/LOWER
操作、轮次完成和 Auto 停止都被正确识别。若出现钱包确认或 Auto-sign，可按现有流程
处理；不得扩大金额或轮次。

### 阶段 3：恢复日常配置

只有阶段 2 完整成功后，才可以使用正式账号表：

```powershell
.\linera_runner.exe --web --auto-session --workers 1
```

先保持单线程。连续完成多个账号且没有异常后，再由操作者决定是否增加并发。

## 五、异常时怎么处理

发生异常时，Codex 应先停止扩大操作，并只报告：

- 脱敏账号编号
- 状态枚举
- 简短原因
- 最近日志文件路径
- 截图路径
- 建议的下一步

不要在对话里粘贴 Cookie、授权头、完整钱包地址、密码或后端响应正文。

如果出现 `auto_still_running=true`，立即人工检查浏览器中的 Auto 状态，不得通过关闭
浏览器来掩盖问题。只有钱包确认、Auto-sign、Pause、Stop 等现有恢复动作可以执行；
任何会增加交易金额或轮次的修复都必须先征得操作者确认。

## 六、修改代码后的验证

如果必须修改代码，先说明根因、修改文件和影响范围。完成后执行：

```powershell
Set-Location .\Linera2.0
python -m unittest discover -s tests -v
python -m compileall -q linera2
```

然后回到项目根目录检查：

```powershell
Set-Location ..
git diff --check
git status --short
```

只提交明确修改的公开代码和文档。不得提交账号表、钱包配置、状态 JSON、日志、截图、
EXE、构建目录或其他本机产物。

## 七、给目标电脑 Codex 的首条指令

可以直接把下面这段话发给 Codex：

```text
请先完整阅读 README.md、CODEX_HANDOFF.md、CODEX_DEBUG_GUIDE.md 和
Linera2.0/README.md。先检查目录结构和运行环境，不要读取或输出账号表、钱包密码、
Cookie、授权头、完整钱包地址或响应正文。先执行只读状态检测，确认 ready 后再按文档
进行单账号一轮集成验证。未经我确认，不得增加下注金额、轮次或并发，不得关闭
HubStudio 浏览器窗口，不得使用 git add . 或 force-push。遇到异常只报告脱敏摘要、
日志路径和截图路径。
```
