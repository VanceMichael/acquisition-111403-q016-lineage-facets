# 作业血缘事件的分面增量接入

作业血缘通知收件器。已有最终摘要登记，事件源提供局部OpenLineage风格事件。只采集作业和资料关系，不调度计算。

## 运行

使用 Node.js 22，依赖由 package-lock.json 固定。安装后可执行：

```sh
npm ci --ignore-scripts --no-audit --no-fund
npm test
npm run demo
npm start
```

服务启动本地来源与收件接口，默认端口分别为4011和4010。`POST /collect` 接收当前最终摘要和 `/events` 分页事件，并把新接纳的事件合入血缘；`GET /records`查看最终摘要资料，`GET /lineage`查看作业/运行/数据集/关系与分面冲突，`GET /coverage`查看还缺哪些证据，`GET /status`查看读位置与接纳统计，`GET /receipts`、`GET /events`与`GET /raw?hash=`查看原始通知，`GET /health`查看服务状态。`PORT`、`SOURCE_PORT`与`DATA_DIR`可分别设置端口和归档目录。

同一 `runId` 的作业定义、运行实例与数据集分别建模；静态资料分面与本次输入/输出分面分开存放；同名但 `schemaURL` 不同的分面并列保留并标为冲突；缺席字段不删除旧分面；迟到事件不回退终态；失败重跑产生新的运行实例，不覆盖上次输出。分页未读完或某页失败时保留可续读边界，`POST /collect` 会从断点继续补收，`POST /collect?from=start` 可重扫全部事件。

```sh
docker build -t lineage-facets .
docker run --rm --network none lineage-facets npm test
docker run --rm --network none lineage-facets npm run demo
docker run --rm -p 4010:4010 lineage-facets
```

## 资料组织

`provider/`是独立本地来源，`fixtures/`记录合成资料和来源阶段，`src/reader.mjs`负责摘要与事件分页的增量收件，`src/lineage.mjs`把事件合成为作业/运行/数据集血缘并计算覆盖与冲突，`src/source.mjs`限定来源地址并记录响应字节，`src/archive.mjs`保存按摘要校验的原文、事件证据和续读边界。`test/`覆盖收件、来源边界、原文完整性、分面归类、终态排序、冲突保留、断点续读和补收。运行不依赖外网、数据库服务或真实设备。

完整来源约定见 `docs/source-contract.md`。项目只访问本地合成资料，不携带真实身份、密钥或业务数据。

## 快速查看

`npm run demo`启动实际来源和收件服务、执行一次摘要与事件收件，并打印查询结果、血缘、覆盖缺口及原文凭据，临时归档在退出时清理。正常完成时间小于90秒；需要保留数据时用`npm start`及独立`DATA_DIR`。演示反映当前产品能力，不代表全部来源资料已被接纳；`/coverage`会明确列出未到证据。
