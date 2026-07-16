# Linera2.0 给另一台电脑 Codex 的交接说明

## 先做什么

先阅读 `README.md` 和 `Linera2.0/README.md`。本项目的启动入口是根目录的
`linera_runner.py` 或打包后的 `linera_runner.exe`，不要直接运行旧的
`linera_task.py`。

## 本机文件边界

以下文件只属于本机，禁止读取内容、输出内容、提交或上传：

- `hubshuju.xlsx`
- `Linera2.0/local_config.json`
- `Linera2.0/auto_sessions.json`
- `Linera2.0/readiness_status.json`
- `alerts.json`、日志、截图、Cookie、授权头和完整钱包地址

账号编号必须同时匹配 Excel 和 HubStudio 环境编号。HubStudio API 默认是
`http://127.0.0.1:6873`。

## EXE 迁移

第一次迁移时，把开发电脑生成的 `dist/linera_runner.exe` 复制到目标项目根目录，
只替换旧 EXE，不替换本地账号和钱包文件。启动前最好等待当前交易轮次完成。

```powershell
.\linera_runner.exe --web --workers 1
```

确认状态正常后再启用自动会话：

```powershell
.\linera_runner.exe --web --auto-session --workers 1
```

以后 EXE 启动时会从 GitHub 获取 `version.json`，只更新公开的 `Linera2.0` 运行包，
失败时继续使用最后一次成功安装的版本。它不会主动重启 HubStudio 浏览器窗口。

## Python 备用启动方式

如果 EXE 太旧或无法更新，可以在项目根目录使用 Python：

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r .\Linera2.0\requirements.txt
$env:OKX_WALLET_PASSWORD = "只保存在本机的密码"
.\.venv\Scripts\python.exe .\linera_runner.py --web --workers 1
```

先单线程验证，再逐步增加并发。任何修改前先说明影响范围，并运行：

```powershell
Set-Location .\Linera2.0
python -m unittest discover -s tests -v
```
