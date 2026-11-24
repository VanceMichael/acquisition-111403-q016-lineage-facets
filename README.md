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

服务启动本地来源与收件接口，默认端口分别为4011和4010。`POST /collect` 领取当前单份资料（最终摘要登记，原有能力）；`POST /collect-events` 从来源事件流收取开始、运行中、结束通知并增量合成血缘；`GET /lineage` 查看合成后的作业、运行、数据集血缘及事件来源索引；`GET /gaps` 查看仍未到齐的证据；`GET /records`查看结构化记录，`GET /receipts`查看原文凭据，`GET /health`查看服务状态。`PORT`、`SOURCE_PORT`与`DATA_DIR`可分别设置端口和归档目录。示例领取不会自动遍历其他来源资料。

```sh
docker build -t lineage-facets .
docker run --rm --network none lineage-facets npm test
docker run --rm --network none lineage-facets npm run demo
docker run --rm -p 4010:4010 lineage-facets
```

## 资料组织

`provider/`是独立本地来源，`fixtures/`记录合成资料和来源阶段，`src/reader.mjs`负责现有单份收件，`src/events.mjs`负责事件流分页接收（中途失败保留已收部分，从断点续收），`src/lineage.mjs`负责身份、分面合并与运行状态机的纯函数合成，`src/journal.mjs`维护可恢复的事件日志与投影边界，`src/source.mjs`限定来源地址并记录响应字节，`src/archive.mjs`保存按摘要校验的原文与资料版本。`test/`覆盖现有收件、事件合成、缺口报告、崩溃恢复、来源边界、原文完整性和服务联通。运行不依赖外网、数据库服务或真实设备。

完整来源约定见 `docs/source-contract.md`。项目只访问本地合成资料，不携带真实身份、密钥或业务数据。

## 快速查看

`npm run demo`启动实际来源和收件服务、执行事件接收与增量合成、再模拟一次失败重跑，打印血缘、当前有效产出与缺失证据，临时归档在退出时清理。正常完成时间小于90秒；需要保留数据时用`npm start`及独立`DATA_DIR`。演示反映当前产品能力，不代表全部来源资料已被接纳。
