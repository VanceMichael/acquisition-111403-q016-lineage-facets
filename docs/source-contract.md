# 来源资料约定

作业血缘通知收件器。已有最终摘要登记，事件源提供局部OpenLineage风格事件。只采集作业和资料关系，不调度计算。

## 交互与身份

GET /run-summary为最终摘要；GET /events?offset=0返回next及events。event含eventId、eventTime、eventType、run.runId、job.namespace/name、inputs/outputs、facets。数据集键为namespace/name；dataset.facets是静态资料，inputFacets/outputFacets只作用本次输入输出；每个分面带producer/schemaURL。eventType是START/RUNNING/COMPLETE/FAIL/ABORT，事件可以迟到，不能按到达顺序倒退终态。缺席分面不删除已有分面，同名分面不同schemaURL不得盲合并。

## 外部变化

正常资料在 `fixtures/source.mjs`。同一来源可能处于这些外部阶段：rerun、schema-conflict。集成测试通过来源实例的`setPhase`切换外部状态，这不是线上业务接口。阶段内同一URL的响应可以改变，其余URL保留正常资料；来源的请求轨迹在实例的`requests`中可查。控制方法不对网络开放。未知地址返回404，非法请求返回非成功状态。

## 身份与分面归类

收件器把三类对象分开建档，键均带类型前缀：

| 对象 | 键 | 说明 |
| --- | --- | --- |
| 作业定义 | `job:<namespace>/<name>` | 跨运行不变 |
| 运行实例 | `run:<namespace>/<name>/<runId>` | 一次具体运行；失败重跑产生新的运行键 |
| 数据集 | `dataset:<namespace>/<name>` | 跨运行共享的资料对象 |

数据集上的 `facets` 只存静态资料分面；每次运行的输入分面与输出分面分别挂在输入关系（`input:<run>:<dataset>`，consumed）和输出关系（`output:<run>:<dataset>`，produced）上，互不覆盖。分面键由分面类别、名称、producer 和 `schemaURL` 共同组成，不同 producer 对同名分面的视图各自保留。同名但 `schemaURL` 不同的分面会并列保留，并在 `conflicts` 中列出全部结构声明、producer 与来源凭据，而不是任选一个合并。同一分面键下重复上报的值按 `eventTime`（相同则 eventId）取较新者作为当前值，同时每个不同载荷都作为 `observations` 保留其事件、页面与原文摘要。

## 终态与缺席语义

- 迟到事件仍会合并其输入输出分面，但不回退已完成运行的终态：终态只被更晚（按 `eventTime`，同刻按 `eventId`）的终态替换。START/RUNNING 永远不会覆盖终态。
- 一次通知没带某个字段或分面，视为“本次未提供”，不删除已收到的内容。
- 失败运行的输出记录在自己的运行实例下；先前运行已收到的真实输出不会被覆盖或抹去。
- 同一事件（`eventId`）重复到达只接纳一次。

## 接纳边界

`POST /collect` 先取 `/run-summary`，再从已存续读位置读取 `/events` 分页。每页响应原始字节按摘要存入归档，并与 `receiptId` 关联；被接纳的事件同时记录 `eventId`、`pageUrl`、`receiptId`、`rawHash` 和接收时间，因此每条血缘都能追到具体运行实例、具体事件、具体页面和原始通知字节。原始页可用 `GET /raw?hash=<rawHash>` 取回。

只有一整轮分页读完，存续读位置才前移；分页失败或结构无效时把断点存下来并回报错误，后续 `POST /collect` 从断点继续。上一轮读完后下一次 `POST /collect` 会从第一页重新扫描，以接纳新发布或补发的页；已接纳事件按 `eventId` 去重，因此重扫不会重复入库。`POST /collect?from=start` 强制丢弃已存断点、从头重扫。

`GET /coverage` 逐运行列出还缺哪些证据：`terminal`（未收到 COMPLETE/FAIL/ABORT）、`summary`（未收到最终摘要）、`input-facets`/`output-facets`（某数据集本次输入或输出分面未到）、`dataset-facets`（静态资料分面未到），`pendingPages` 表示分页尚未读完。`totals` 给出汇总计数。`GET /status` 同时返回读位置、已接纳/已拒绝事件与完整覆盖情况。

## 归档

`var/archive/index.json` 为版本 2，保存资料记录、原文凭据、事件原始页、已接纳事件、被拒绝事件与存续读位置；`objects/` 按 SHA-256 保存原始响应字节，打开归档时逐份校验。旧版仅含最终摘要的版本 1 索引会迁移为“只有摘要、尚未收到事件证据”的状态，不会凭空补造事件。来源文件、接收记录和对外查询不共享内存对象。未声明的协议能力不作推断；格式子集以本页和实际来源为准。
