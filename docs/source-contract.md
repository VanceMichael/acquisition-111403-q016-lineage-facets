# 来源资料约定

作业血缘通知收件器。已有最终摘要登记，事件源提供局部OpenLineage风格事件。只采集作业和资料关系，不调度计算。

## 交互与身份

GET /run-summary为最终摘要；GET /events?offset=0返回next及events。event含eventId、eventTime、eventType、run.runId、job.namespace/name、inputs/outputs、facets。数据集键为namespace/name；dataset.facets是静态资料，inputFacets/outputFacets只作用本次输入输出；每个分面带producer/schemaURL。eventType是START/RUNNING/COMPLETE/FAIL/ABORT，事件可以迟到，不能按到达顺序倒退终态。缺席分面不删除已有分面，同名分面不同schemaURL不得盲合并。

## 外部变化

正常资料在 `fixtures/source.mjs`。同一来源可能处于这些外部阶段：rerun、schema-conflict。集成测试通过来源实例的`setPhase`切换外部状态，这不是线上业务接口。阶段内同一URL的响应可以改变，其余URL保留正常资料；来源的请求轨迹在实例的`requests`中可查，控制方法不对网络开放。未知地址返回404，非法请求返回非成功状态。

## 已有收件边界

当前`src/reader.mjs`只收取/run-summary提供的已整理资料。归档按资料标识和内容摘要区分版本，原文按响应字节摘要独立存储，打开归档时逐份校验。来源文件、接收记录和对外查询不共享内存对象。未声明的协议能力不作推断；格式子集以本页和实际来源为准。
