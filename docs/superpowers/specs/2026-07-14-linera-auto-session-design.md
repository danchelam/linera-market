# Linera 2.0 测试网 Auto 会话设计

## 目标

在账号就绪后，使用网站内置 Auto 功能执行测试网双向交易。Higher 和 Lower 固定各投入 1 Coin，每个账号每天随机运行 4～7 个完整轮次，达到目标后停止 Auto、等待最后一轮结算并记录 Coins 变化。

## 会话边界

- 每个账号按 UTC 日期每天最多完成一次自动测试会话。
- 每日目标轮数在 4～7 之间生成一次并持久化；进程重启不得重新随机。
- 单轮只有同时出现一条 Higher 和一条 Lower 记录才计数，名义 Stake 固定为 2 Coins。
- 总名义测试量为 8～14 Coins，不以输赢或余额下降作为停止条件。
- 单次会话硬超时为 20 分钟；余额不足 2 Coins、钱包/后端失联或页面异常时提前停止。

## 执行流程

1. 调用现有 `check_account_ready`。仅当状态为 `ready` 且 Coins ≥ 2 时继续。
2. 读取当天 `auto_sessions.json`：已完成则跳过；未完成则复用已保存的目标轮数与历史基线。
3. 若页面进入时已显示 `AUTO ON`，先点击 `Stop` 并等待开放仓位结算，防止把人工或上次遗留会话混入本次计数。
4. 监听 Linera worker 的 GraphQL 请求，保存启动 Auto 前已查询的 `resolutions.entry(key: N)` 数字键集合和当前 History 摘要为基线，并在启动 Auto 前持久化会话状态。
5. 点击 `Auto / bet every round`，将 `Higher coins` 和 `Lower coins` 都填为 `1`，点击 `Start Auto · 2 coins / round`。
6. 同时出现 `AUTO ON`、`Pause`、`Stop` 后进入运行状态；否则判定启动失败。
7. 每 2 秒读取 History，并同步收集 Linera worker GraphQL 请求中的 `resolutions.entry(key: N)` 数字键。仅当出现大于启动基线的新 resolution key，且 History 中同时存在本次会话新增的 Higher、Lower 记录时计为一轮；Live 状态可以计入，同一 resolution key 只能计数一次。DOM History 不提供轮次时间或 ID，因此不能单独作为轮次键。
8. 第 N 个目标轮次的双向记录出现后立即点击 `Stop`，防止下一轮继续下注。
9. 最多等待 3 分钟，直到最后一轮不再是 Live 且开放仓位为空；随后读取结束 Coins 并标记当天完成。

## 状态与持久化

会话状态为 `waiting`、`configuring`、`running`、`stopping`、`settling`、`completed`、`failed`。每个账号保存：UTC 日期、目标轮数、已计轮数、开始/结束 Coins、累计 Stake、基线 resolution keys、已计 resolution keys、History 基线摘要、开始/结束时间和失败原因。

状态写入 `Linera2.0/auto_sessions.json`，采用原子替换，且与 `readiness_status.json` 分离。Web API 在现有账号状态中增加 `session_state`、`target_rounds`、`completed_rounds`、`start_coins`、`current_coins`、`nominal_stake` 和 `net_change`。

## 定位与成功标志

- 打开配置：按钮文本包含 `Auto` 和 `bet every round`。
- 金额输入：`input[aria-label="Higher coins"]` 与 `input[aria-label="Lower coins"]`。
- 启动：按钮文本匹配 `Start Auto`。
- 运行成功：`AUTO ON`、`Pause`、`Stop` 同时可见。
- 停止：点击 `Stop` 后 `AUTO ON` 消失，且不再产生新的轮次键。
- History 计数不得使用请求次数、刷新次数或单条交易数量；worker 请求只提取 resolution key，不记录 URL 参数、请求头或响应正文。

## 异常处理

- 任何失败退出前，只要检测到 `AUTO ON` 就尝试点击一次 `Stop`。
- 停止失败时状态为 `failed`，日志明确提示人工检查，不能关闭浏览器掩盖仍在运行的 Auto。
- 中途进程重启后先停止遗留 Auto，使用已持久化的基线和轮次键重新核对 History，再决定是否继续剩余轮次。
- 页面显示钱包断开、worker 后端错误或 Coins 不可读时停止新增下注；保留当前会话以供下一次恢复。
- 所有日志只保存脱敏钱包地址，不保存 Cookie、授权头、完整地址或响应正文。

## 测试与验收

- 目标轮数只在首次创建会话时随机，重启后保持不变且始终位于 4～7。
- Higher/Lower 同轮成对后只增加一次；单边记录、重复记录和旧 History 不计数。
- 达到目标后只点击一次 Stop，并等待最后一轮结算。
- 余额不足、就绪失败、启动失败、超时和遗留 Auto 都产生明确状态。
- 环境 `625421671` 进行单账号集成测试：固定目标 1 轮，确认输入 1+1、启动、出现双向记录、停止及最终 Coins 记录；测试后 Auto 必须为关闭状态。
