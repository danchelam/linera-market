# Linera 2.0 账号就绪与测试网 Auto 会话

默认模式只检查账号是否可以开始任务。程序会读取钱包、Coins、Ride UI 和 Linera worker 响应，不会刷新、连接钱包、确认弹窗或点击业务按钮。

## 运行

由于目录名包含点，请先进入 2.0 目录：

```powershell
Set-Location .\Linera2.0
python -m linera2
```

默认从父目录的 `hubshuju.xlsx` 读取账号，单并发检测，每个账号最多观察 60 秒。

```powershell
python -m linera2 --workers 3 --timeout 60
```

启动 Web 状态页：

```powershell
python -m linera2 --web --workers 3
```

浏览器访问 `http://127.0.0.1:5060`。结构化状态保存在本目录的 `readiness_status.json`，钱包地址只保存脱敏形式。

## 测试网 Auto 会话

只有显式传入 `--auto-session` 才会点击网站的测试网交易控件。每个账号按 UTC 日期最多完成一次：HIGHER 1 Coin、LOWER 1 Coin，每天首次运行时随机生成并保存 4–7 个完整轮次目标。

若首次就绪检测为 `wallet_disconnected`，`--auto-session` 会先复用父目录的
OKX 解锁能力，并通过当前 Linera 页面完成一次 Connect/登录确认；随后重新执行
完整就绪检测。只有第二次结果为 `ready` 才会进入 Auto。恢复流程不会重启浏览器
或循环刷新；父钱包解锁在 provider 尚未注入时可能临时导航到外部页，适配层随后
返回原 Linera 页面。未传 `--auto-session` 时仍是完全只读检测，不会连接钱包。

```powershell
# 每日 Auto 会话；首次运行建议单并发并打开状态页
python -m linera2 --auto-session --workers 1 --web

# 仅供明确授权的单账号人工集成验证
python -m linera2 --auto-session --integration-target 1 --workers 1
```

会话状态单独保存在 `auto_sessions.json`。当天已是 `completed` 会直接跳过。若 Web/API 显示 `auto_still_running=true`，表示自动停止未确认成功，需要立即人工检查该浏览器窗口；程序不会通过关闭浏览器来掩盖仍在运行的 Auto。

排查钱包时可以直接调用 `linera2.wallet_recovery.ensure_wallet_connected()` 做
钱包专项验证。该接口只负责解锁、Connect 和登录确认，不配置 Auto，也不下注；
验证完成后应再调用 `check_account_ready()` 确认钱包、后端、Coins 和 Ride UI。

## 状态说明

- `ready`：钱包、链数据、Coins 和 Ride UI 均正常，且 Coins 大于 0。
- `wallet_disconnected`：未识别到已连接钱包。
- `wallet_syncing`：钱包地址存在，但链响应或 Coins 尚未同步。
- `backend_unavailable`：Linera worker 返回 HTTP 或业务错误。
- `page_loading`：后端已正常，但 Ride 操作区尚未加载。
- `insufficient_balance`：同步成功，但 Coins 为 0。
- `browser_unreachable` / `page_unavailable`：HubStudio CDP 或目标页面不可用。

## 测试

```powershell
python -m unittest discover -s tests -v
```

## 安装与安全更新

从项目根目录运行根级 `linera_runner.py`。它会读取 `version.json`，在安全校验通过后下载新的 Linera2 包；下载或校验失败时继续使用最后一次成功安装的本地包。首次启动会将旧版本可读取的本地密码迁移到 `Linera2.0/local_config.json`，也可以只设置环境变量：

```powershell
$env:OKX_WALLET_PASSWORD = "your-local-wallet-password"
python .\linera_runner.py --web --auto-session --workers 1
```

账号文件 `hubshuju.xlsx` 必须由操作者自行放入项目根目录。密码、Cookie、授权头、状态 JSON、截图和日志都属于本机数据，已被忽略规则排除，不应上传。

维护者发布前使用：

```powershell
python .\publish.py --dry-run
python .\publish.py
```

`--dry-run` 只运行测试并打印固定发布清单。正式发布会生成 schema 2 的 `version.json` 和文件哈希，检查 GitHub 远端后再进行 fast-forward 推送；发布器从不 force-push，也不会把本地账号或钱包数据纳入清单。
