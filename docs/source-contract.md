# 来源资料约定

作业血缘通知收件器。事件源提供局部OpenLineage风格事件，收件器按事件持续合成作业与资料关系，不调度计算。

## 交互与身份

GET /run-summary为来源整理的最终摘要（旧收件边界仍保留）；GET /events?offset=0返回next及events，收件器沿next逐页接纳，分页失败或未读尽时登记缺口并保留已收部分，之后可继续补收。event含eventId、eventTime、eventType、run.runId、job.namespace/name、inputs/outputs、facets。

身份互相独立、不得共用：作业身份为`job:<namespace>/<name>`，运行实例为`run:<runId>`，数据集为`dataset:<namespace>/<name>`，运行与数据集之间的关系为`edge:<runId>:<IN|OUT>:<namespace>/<name>`。数据集键为namespace/name；dataset.facets是静态资料，挂在数据集身份下跨运行累积；inputFacets/outputFacets只作用本次输入输出，挂在关系身份下，两者作用域不同、互不覆盖；每个分面带producer/schemaURL。eventType是START/RUNNING/COMPLETE/FAIL/ABORT，事件可以迟到，不能按到达顺序倒退终态：终态只被更晚eventTime的终态替换，迟到的START/RUNNING不覆盖终态。缺席分面不删除已有分面，未发送的字段不清空旧字段；同名分面不同schemaURL不得盲合并，全部变体保留并标记facet-schema-conflict，交人工裁决。同eventId但内容不同的通知进入隔离区，不覆盖已接纳事实。

## 外部变化

正常资料在 `fixtures/source.mjs`。同一来源可能处于这些外部阶段：rerun、schema-conflict。集成测试通过来源实例的`setPhase`切换外部状态，这不是线上业务接口。阶段内同一URL的响应可以改变，其余URL保留正常资料；来源的请求轨迹在实例的`requests`中可查，控制方法不对网络开放。未知地址返回404，非法请求返回非成功状态。

## 接纳边界与可恢复性

接纳是只追加且幂等的：原始事件字节按sha256摘要存入objects，事件日志、隔离区和分页缺口写入index.json（版本2，旧版本1索引可迁移打开）；重开归档时对交换原文、事件原文、隔离原文逐份校验，任一被篡改即拒绝打开。重放同一批事件按receiptId跳过，不产生重复版本。

失败重跑使用新的runId，是独立运行实例；上次成功运行的真实输出、失败前已产生的部分输出都各自挂在对应运行的关系与分面证据下，不会被抹去。每条血缘记录都带evidence（eventId、eventTime、receiptId），可经`GET /raw/:hash`取回原始通知字节。

缺终态、缺分面、分页缺口、隔离通知、分面结构冲突都会进入待补证据清单（`GET /pending`与lineage视图的pending），并以中文说明还有哪些证据没到；证据补齐后对应待补项自动收敛，历史缺口作为已发生事实保留。未声明的协议能力不作推断；格式子集以本页和实际来源为准。
