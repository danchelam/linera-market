# Linera 2.0

这是 Linera 测试网自动化项目。公开代码、运行器和模板可以从 GitHub 更新；账号表、钱包密码和运行记录只保留在本机。

## 在另一台电脑安装

1. 安装 Python 3.12、HubStudio 和已配置好的 OKX Wallet 扩展，并准备好对应的浏览器窗口。
2. 下载仓库中的 `linera_runner.py`、`version.json` 和 `Linera2.0/` 目录到同一目录。将本地的 `hubshuju.xlsx` 放在项目根目录；它不会上传到 GitHub。
3. 在项目根目录安装依赖并启动：

```powershell
python -m pip install -r .\Linera2.0\requirements.txt
$env:OKX_WALLET_PASSWORD = "your-local-wallet-password"
python .\linera_runner.py --web --auto-session --workers 1
```

也可以把密码写入本机的 `Linera2.0/local_config.json`，不要把该文件提交或发送给别人。账号 ID 必须与 HubStudio 窗口和 Excel 中的记录一致。

首次启动会把旧版本可读取的本地密码迁移到 `Linera2.0/local_config.json`（若不存在则继续使用环境变量）。更新失败时会启动最后一次成功安装的本地包，不会留下半套更新。运行器不会主动重启已打开的浏览器窗口。

## 发布更新

先在本机完成测试，再查看将要上传的清单：

```powershell
python .\publish.py --dry-run
python .\publish.py
```

发布器只选择固定的运行时清单，生成带 SHA-256 校验值的 `version.json`，并在推送前检查敏感内容。它只允许 fast-forward 更新，绝不 force-push；账号表、密码、Cookie、授权头、截图、日志和状态 JSON 均不会上传。

## 本地验证

```powershell
Set-Location .\Linera2.0
python -m unittest discover -s tests -v
```

## 打包版 EXE 的更新

如果另一台电脑使用 `linera_runner.exe`，第一次迁移建议用当前源码重新打包一次：

```powershell
python -m pip install pyinstaller
python .\build.py
```

把 `dist\linera_runner.exe` 复制过去后，只替换旧的 EXE。不要覆盖该电脑本地的
`hubshuju.xlsx`、`Linera2.0\local_config.json`、`auto_sessions.json` 或状态文件。
以后启动 EXE 时，它会自动读取 GitHub 清单，更新 `Linera2.0` 运行包并安全重启；
不需要每次重新打包。若当前仍有交易轮次，先等待轮次结束再重启 EXE，HubStudio
浏览器窗口可以保持打开。

## 给另一台电脑 Codex 的交接

让 Codex 先阅读本文件和 `Linera2.0\README.md`，再开始操作。它只能修改公开代码，
不得读取、输出或上传账号表、钱包配置、Cookie、授权头、日志、截图和运行状态。
