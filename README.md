# Amazon Connect Agent Event Stream → ASAPP start/stop 过滤规则

目的：消费 Amazon Connect **Agent Event Stream**（Kinesis Data Streams），在坐席与客户真正通话时通知 ASAPP `start-streaming`，在 hold / 结束时通知 `stop-streaming`。

## 数据链路

```
Connect 实例 → Data streaming → Agent events → Kinesis Data Stream → Lambda → ASAPP
```

- 每条 Kinesis record 的 `data` 是 Base64 编码的 JSON。
- 事件以 **坐席** 为中心，`Contacts[]` 是该坐席当前所有 contact，可能为空或多条。

## 过滤链（按顺序，前一层不通过即丢弃）

### 第 1 层：事件级

| Field | 保留 | 丢弃 |
|---|---|---|
| `EventType` | `STATE_CHANGE` | `HEART_BEAT` `LOGIN` `LOGOUT` |

### 第 2 层：contact 级

对 `PreviousAgentSnapshot.Contacts[]` 与 `CurrentAgentSnapshot.Contacts[]` 按 `ContactId` 配对，取并集遍历。

| Field | 保留 | 丢弃 |
|---|---|---|
| `Channel` | `VOICE` | `CHAT` `TASKS` |
| `InitiationMethod` | `INBOUND` `TRANSFER` `QUEUE_TRANSFER` | `OUTBOUND` `EXTERNAL_OUTBOUND` `API` `MONITOR` `CAMPAIGN_PREVIEW` `DISCONNECT` `WEBRTC_API` `CALLBACK` `AGENT_REPLY` `FLOW` |

`CALLBACK` / `WEBRTC_API` 按业务需要可移入保留列。使用白名单，新增类型默认不触发。

### 第 3 层：状态跳变

`State` 全部取值（官方）：`INCOMING | PENDING | CONNECTING | CONNECTED | CONNECTED_ONHOLD | MISSED | PAUSED | REJECTED | ERROR | ENDED`（`PAUSED` 仅 Task）。

| Previous.State | Current.State | 动作 |
|---|---|---|
| ≠ Current | `CONNECTED` | **start**（首次接通或 resume） |
| ≠ Current | `CONNECTED_ONHOLD` | **stop**（hold） |
| `CONNECTED` / `CONNECTED_ONHOLD` | `ENDED` 或 contact 消失 | **stop** |
| = Current | = Previous | 忽略（坐席改状态、换路由等） |
| 任意 | `INCOMING` `CONNECTING` `PENDING` `MISSED` `REJECTED` `ERROR` `PAUSED` | 忽略 |

## 动作携带字段

| Field | 用途 |
|---|---|
| `ContactId` | 当前段标识 |
| `InitialContactId` | 转接后关联原通话；为空时等于 ContactId |
| `Contacts[].StateStartTimestamp` | 去重 / 乱序保护，只接受更新的时间戳（ISO 8601，如 `2026-09-14T16:07:45.812Z`） |
| `CurrentAgentSnapshot.Configuration.Username` | 坐席 ID |
| `AgentARN` | 备用坐席标识 |

## 转接说明

- 每个 ContactId 独立处理。Warm transfer 时接收坐席的新 contact（`TRANSFER`）在客户仍 hold 期间会 start，转写的是两位坐席的交接对话；第一版接受此代价，把 `InitialContactId` 传给 ASAPP 用于关联。
- 若需避免，可在 `TRANSFER` contact 进入 `CONNECTED` 时查原通话状态，为 `CONNECTED_ONHOLD` 则延迟 start。

## 幂等与乱序

- Kinesis 同分片保序，同坐席事件落同分片；至少一次投递可能重复。
- 每个 contact 在 DynamoDB 记录 `lastTs`，条件写 `lastTs < StateStartTimestamp` 才执行动作。

## 官方文档

- Agent Event Stream 数据模型：https://docs.aws.amazon.com/connect/latest/adminguide/agent-event-stream-model.html
- Agent Event Stream 开启：https://docs.aws.amazon.com/connect/latest/adminguide/agent-event-streams.html
- Contact Events（EventBridge，无语音 hold 事件）：https://docs.aws.amazon.com/connect/latest/adminguide/contact-events.html
- Start media streaming：https://docs.aws.amazon.com/connect/latest/adminguide/start-media-streaming.html
- Invoke Lambda 支持的 flow 类型：https://docs.aws.amazon.com/connect/latest/adminguide/invoke-lambda-function-block.html
- ASAPP 接入：https://docs.asapp.com/ai-productivity/ai-transcribe/amazon-connect

字段名与取值来自官方页面；第 3 层动作映射与转接处理为设计决策，需用测试通话验证。
