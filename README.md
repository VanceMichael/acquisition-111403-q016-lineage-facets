# 作业血缘事件的分面增量接入

作业血缘通知收件器。接收合作计算服务分批送达的 START/RUNNING/COMPLETE/FAIL/ABORT 局部事件，按事件持续合成作业、运行实例、数据集与分面血缘；只采集作业和资料关系，不调度计算。

## 运行

使用 Node.js 22，依赖由 package-lock.json 固定。安装后可执行：

```sh
npm ci --ignore-scripts --no-audit --no-fund
npm test
npm run demo
npm start
```

服务启动本地来源与收件接口，默认端口分别为4011和4010。`POST /collect` 沿事件流逐页接纳当前可收事件并合成；`GET /lineage` 查看可查询血缘（作业/运行/数据集分身份、分面带事件凭据、附待补证据），`GET /pending` 只看待补与待裁决证据，`GET /raw/:hash` 按摘要取回原始通知字节，`GET /receipts` 查看交换凭据，`GET /records` 查看旧摘要登记，`GET /health` 查看服务状态。`PORT`、`SOURCE_PORT`与`DATA_DIR`可分别设置端口和归档目录。

```sh
docker build -t lineage-facets .
docker run --rm --network none lineage-facets npm test
docker run --rm --network none lineage-facets npm run demo
docker run --rm -p 4010:4010 lineage-facets
```

## 资料组织

`provider/`是独立本地来源，`fixtures/`记录合成资料和来源阶段（normal/rerun/schema-conflict）。`src/reader.mjs`负责事件流分页接纳（`collectEvents`）并保留旧的最终摘要领取（`collect`）；`src/lineage.mjs`是无状态的持续合成纯函数，处理身份分离、终态不倒退、分面冲突与待补证据；`src/archive.mjs`保存只追加的事件日志、隔离区、分页缺口和按摘要校验的原文；`src/canonical.mjs`提供稳定序列化；`src/source.mjs`限定来源地址并记录响应字节。`test/`覆盖事件接纳、来源边界、原文完整性、重跑保留、冲突分面、缺口恢复和服务联通。运行不依赖外网、数据库服务或真实设备。

## 关键语义

- 作业、运行实例、数据集各自身份独立；静态资料分面（dataset.facets）与本次输入输出分面（inputFacets/outputFacets）作用域不同，互不覆盖。
- 事件可迟到：终态只被更晚的终态替换，迟到的 START/RUNNING 不会让终态倒退；事件按 eventTime 与接纳顺序裁决，不按到达顺序。
- 未发送的字段不等于清空旧资料；同名分面的结构声明（producer/schemaURL）不同时保留全部变体并标记冲突。
- 失败重跑是新的运行实例，上次真实输出与本次失败前的部分输出都各自留证，不会互相抹去。
- 原始通知与可查询血缘之间有可恢复的接纳边界：原文按字节摘要独立存储、重开逐份校验；缺终态、缺分面或分页暂不可读时可以继续补收，待补证据清单说明还缺什么。

完整来源约定见 `docs/source-contract.md`。项目只访问本地合成资料，不携带真实身份、密钥或业务数据。

## 快速查看

`npm run demo`启动实际来源和收件服务，依次演示正常接纳、幂等重放、rerun 失败重跑和 schema-conflict 分面冲突，并打印血缘视图、待补证据与原文凭据，临时归档在退出时清理。需要保留数据时用`npm start`及独立`DATA_DIR`。
