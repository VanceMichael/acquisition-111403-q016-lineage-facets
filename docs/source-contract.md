# 来源资料约定

作业血缘通知收件器。已有最终摘要登记，事件源提供局部OpenLineage风格事件。只采集作业和资料关系，不调度计算。

## 交互与身份

GET /run-summary为最终摘要；GET /events?offset=0返回next及events。event含eventId、eventTime、eventType、run.runId、job.namespace/name、inputs/outputs、facets。数据集键为namespace/name；dataset.facets是静态资料，inputFacets/outputFacets只作用本次输入输出；每个分面带producer/schemaURL。eventType是START/RUNNING/COMPLETE/FAIL/ABORT，事件可以迟到，不能按到达顺序倒退终态。缺席分面不删除已有分面，同名分面不同schemaURL不得盲合并。

## 外部变化

正常资料在 `fixtures/source.mjs`。同一来源可能处于这些外部阶段：rerun、schema-conflict、open-run、open-run-closed。集成测试通过来源实例的`setPhase`切换外部状态，这不是线上业务接口。阶段内同一URL的响应可以改变，其余URL保留正常资料；来源的请求轨迹在实例的`requests`中可查，控制方法不对网络开放。未知地址返回404，非法请求返回非成功状态。

## 收件边界

/run-summary的单份登记保留为原有能力。事件流经`POST /collect-events`接收：原始事件页先按字节摘要入归档，事件再进入持久日志（接纳边界），最后投影为可查询血缘；投影滞后、丢失或损坏时从日志重放重建，投影可信性以其声明的日志前缀摘要校验。作业、运行、数据集分别使用`job:`/`run:`/`dataset:`身份；dataset.facets静态资料与inputFacets/outputFacets本次分面分开保存；同schemaURL的分面按字段合并（缺席字段保留旧值，字段先后以eventTime为准），不同schemaURL的同名分面作为冲突变体并列保留。运行状态只前进不倒退，终态之后的迟到事件只留证据；失败运行的部分输出挂在该运行边上，不替代最近一次成功运行的产出。缺终态、缺开始、分面结构冲突、终态冲突、事件流未收完等情况通过`GET /gaps`告知，可再次接收继续补收。来源文件、接收记录和对外查询不共享内存对象。未声明的协议能力不作推断；格式子集以本页和实际来源为准。
