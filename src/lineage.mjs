// 作业血缘的增量合成：身份、分面合并、运行状态机与缺失证据报告。
// 本模块为纯函数，不触碰磁盘与网络，便于独立测试。

const RANK = {START: 1, RUNNING: 2, COMPLETE: 3, FAIL: 3, ABORT: 3};
export const EVENT_TYPES = Object.freeze(Object.keys(RANK));
const TERMINAL = new Set(['COMPLETE', 'FAIL', 'ABORT']);
const FACET_META = new Set(['producer', 'schemaURL']);

const isRecord = value => value !== null && typeof value === 'object' && !Array.isArray(value);

// 作业定义、运行实例、数据集是三种身份，键空间互不共用。
export const jobKeyOf = job => `job:${job.namespace}/${job.name}`;
export const runKeyOf = run => `run:${run.runId}`;
export const datasetKeyOf = dataset => `dataset:${dataset.namespace}/${dataset.name}`;

export function createState() {
  return {jobs: {}, runs: {}, datasets: {}};
}

export function validateEvent(event) {
  if (!isRecord(event)) throw new Error('事件必须是对象');
  if (typeof event.eventId !== 'string' || !event.eventId) throw new Error('事件缺少 eventId');
  if (!EVENT_TYPES.includes(event.eventType)) throw new Error(`事件 ${event.eventId} 的类型无效: ${event.eventType}`);
  if (typeof event.eventTime !== 'number' || !Number.isFinite(event.eventTime)) throw new Error(`事件 ${event.eventId} 缺少有效的 eventTime`);
  if (!isRecord(event.run) || typeof event.run.runId !== 'string' || !event.run.runId) throw new Error(`事件 ${event.eventId} 缺少 run.runId`);
  if (!isRecord(event.job) || typeof event.job.namespace !== 'string' || !event.job.namespace || typeof event.job.name !== 'string' || !event.job.name) throw new Error(`事件 ${event.eventId} 缺少 job.namespace/job.name`);
  for (const side of ['inputs', 'outputs']) {
    if (event[side] === undefined) continue;
    if (!Array.isArray(event[side])) throw new Error(`事件 ${event.eventId} 的 ${side} 必须是数组`);
    for (const dataset of event[side]) {
      if (!isRecord(dataset) || typeof dataset.namespace !== 'string' || !dataset.namespace || typeof dataset.name !== 'string' || !dataset.name) {
        throw new Error(`事件 ${event.eventId} 的 ${side} 含有缺少 namespace/name 的数据集`);
      }
    }
  }
  return true;
}

// 分面合并：同一 schemaURL 视为同一结构声明，按字段合并，某次没发送的字段保留旧值，
// 字段先后以 eventTime 为准；不同 schemaURL 的同名分面并列保留为冲突变体，不盲合并。
function mergeFacets(container, facets, context) {
  if (!isRecord(facets)) return;
  for (const [name, facet] of Object.entries(facets)) {
    if (!isRecord(facet)) continue;
    const schemaURL = typeof facet.schemaURL === 'string' ? facet.schemaURL : null;
    const slot = container[name] ??= {variants: []};
    let variant = slot.variants.find(candidate => candidate.schemaURL === schemaURL);
    if (!variant) {
      variant = {schemaURL, producer: null, fields: {}, fieldTimes: {}, events: [], updated: null};
      slot.variants.push(variant);
    }
    if (typeof facet.producer === 'string') variant.producer = facet.producer;
    for (const [field, value] of Object.entries(facet)) {
      if (FACET_META.has(field)) continue;
      const known = variant.fieldTimes[field];
      if (known === undefined || context.eventTime >= known) {
        variant.fields[field] = value;
        variant.fieldTimes[field] = context.eventTime;
      }
    }
    if (!variant.events.includes(context.eventId)) variant.events.push(context.eventId);
    variant.updated = variant.updated === null ? context.eventTime : Math.max(variant.updated, context.eventTime);
  }
}

export function applyEvent(state, event) {
  validateEvent(event);
  const jobKey = jobKeyOf(event.job);
  const runKey = runKeyOf(event.run);
  const job = state.jobs[jobKey] ??= {key: jobKey, kind: 'job', namespace: event.job.namespace, name: event.job.name, facets: {}, runs: [], events: []};
  const run = state.runs[runKey] ??= {key: runKey, kind: 'run', runId: event.run.runId, job: jobKey, state: null, terminal: false, seenStart: false, startedAt: null, endedAt: null, facets: {}, inputs: {}, outputs: {}, events: [], notes: []};
  if (run.job !== jobKey && !run.notes.some(note => note.kind === '作业归属冲突')) {
    run.notes.push({kind: '作业归属冲突', detail: `运行 ${run.runId} 已归属 ${run.job}，又收到 ${jobKey} 的事件`, eventId: event.eventId});
  }
  if (!job.runs.includes(run.runId)) job.runs.push(run.runId);
  if (!job.events.includes(event.eventId)) job.events.push(event.eventId);
  if (!run.events.includes(event.eventId)) run.events.push(event.eventId);

  // 状态机只前进不倒退：终态之后的迟到事件只留证据，不改变已确认的状态；
  // 两个不同终态先后到达时保留先到的终态并记录冲突。
  const incoming = event.eventType;
  if (run.terminal) {
    if (TERMINAL.has(incoming) && incoming !== run.state) {
      run.notes.push({kind: '终态冲突', detail: `运行 ${run.runId} 已终态 ${run.state}，又收到 ${incoming}，保留先到的终态`, eventId: event.eventId});
    } else if (!TERMINAL.has(incoming)) {
      run.notes.push({kind: '迟到事件', detail: `运行 ${run.runId} 已终态 ${run.state}，迟到的 ${incoming} 不改变状态`, eventId: event.eventId});
    }
  } else if (run.state === null || RANK[incoming] > RANK[run.state]) {
    run.state = incoming;
    if (TERMINAL.has(incoming)) {
      run.terminal = true;
      run.endedAt = event.eventTime;
    }
  }
  if (incoming === 'START') {
    run.seenStart = true;
    run.startedAt = run.startedAt === null ? event.eventTime : Math.min(run.startedAt, event.eventTime);
  }

  const context = {eventId: event.eventId, eventTime: event.eventTime};
  mergeFacets(job.facets, event.job.facets, context);
  mergeFacets(run.facets, event.run.facets, context);
  for (const [side, edgeFacets] of [['inputs', 'inputFacets'], ['outputs', 'outputFacets']]) {
    for (const dataset of event[side] ?? []) {
      const datasetKey = datasetKeyOf(dataset);
      const node = state.datasets[datasetKey] ??= {key: datasetKey, kind: 'dataset', namespace: dataset.namespace, name: dataset.name, facets: {}, events: []};
      if (!node.events.includes(event.eventId)) node.events.push(event.eventId);
      // 静态资料分面挂在数据集身份上，本次输入/输出分面挂在运行与数据集的边上，互不覆盖。
      mergeFacets(node.facets, dataset.facets, context);
      const edge = run[side][datasetKey] ??= {dataset: datasetKey, facets: {}};
      mergeFacets(edge.facets, dataset[edgeFacets], context);
    }
  }
  return run;
}

function viewFacets(container) {
  const view = {};
  for (const [name, slot] of Object.entries(container)) {
    view[name] = {
      conflict: slot.variants.length > 1,
      variants: slot.variants.map(variant => ({
        schemaURL: variant.schemaURL,
        producer: variant.producer,
        fields: structuredClone(variant.fields),
        events: [...variant.events],
        updated: variant.updated
      }))
    };
  }
  return view;
}

const viewEdges = edges => Object.values(edges).map(edge => ({dataset: edge.dataset, facets: viewFacets(edge.facets)}));

export function toView(state) {
  const jobs = Object.values(state.jobs).map(job => ({
    key: job.key,
    namespace: job.namespace,
    name: job.name,
    facets: viewFacets(job.facets),
    runs: [...job.runs],
    events: [...job.events]
  }));
  const runs = Object.values(state.runs).map(run => ({
    key: run.key,
    runId: run.runId,
    job: run.job,
    state: run.state,
    terminal: run.terminal,
    startedAt: run.startedAt,
    endedAt: run.endedAt,
    facets: viewFacets(run.facets),
    inputs: viewEdges(run.inputs),
    outputs: viewEdges(run.outputs),
    events: [...run.events],
    notes: structuredClone(run.notes)
  }));
  const datasets = Object.values(state.datasets).map(dataset => {
    const producedBy = [];
    const consumedBy = [];
    for (const run of Object.values(state.runs)) {
      if (run.outputs[dataset.key]) producedBy.push({run: run.runId, job: run.job, state: run.state, at: run.endedAt});
      if (run.inputs[dataset.key]) consumedBy.push({run: run.runId, job: run.job, state: run.state, at: run.endedAt});
    }
    // 当前有效产出只认最近一次 COMPLETE 的运行；失败运行的部分输出留在它自己的边上，不顶替。
    const completed = Object.values(state.runs)
      .filter(run => run.state === 'COMPLETE' && run.outputs[dataset.key])
      .sort((left, right) => (right.endedAt ?? 0) - (left.endedAt ?? 0));
    const latest = completed[0] ?? null;
    return {
      key: dataset.key,
      namespace: dataset.namespace,
      name: dataset.name,
      facets: viewFacets(dataset.facets),
      producedBy,
      consumedBy,
      latestCompletedOutput: latest
        ? {run: latest.runId, job: latest.job, at: latest.endedAt, facets: viewFacets(latest.outputs[dataset.key].facets)}
        : null
    };
  });
  return {jobs, runs, datasets};
}

// 向使用者报告仍未到齐的证据：缺终态、缺开始、分面结构冲突、终态冲突、事件流未收完。
export function computeGaps(state, cursor) {
  const gaps = [];
  for (const run of Object.values(state.runs)) {
    if (!run.terminal) gaps.push({kind: '缺终态事件', run: run.runId, job: run.job, state: run.state ?? '未知', detail: `运行 ${run.runId} 尚未收到终态事件，可继续补收`});
    if (!run.seenStart) gaps.push({kind: '缺开始事件', run: run.runId, job: run.job, detail: `运行 ${run.runId} 尚未收到 START 事件`});
    for (const note of run.notes) {
      if (note.kind === '终态冲突') gaps.push({kind: '终态冲突', run: run.runId, job: run.job, detail: note.detail});
    }
  }
  const facetConflicts = (owner, container) => {
    for (const [name, slot] of Object.entries(container)) {
      if (slot.variants.length > 1) {
        gaps.push({kind: '分面结构冲突', subject: owner, facet: name, schemaURLs: slot.variants.map(variant => variant.schemaURL), detail: `${owner} 的分面 ${name} 存在 ${slot.variants.length} 种结构声明，已分别保留`});
      }
    }
  };
  for (const job of Object.values(state.jobs)) facetConflicts(job.key, job.facets);
  for (const run of Object.values(state.runs)) {
    facetConflicts(run.key, run.facets);
    for (const edge of Object.values(run.inputs)) facetConflicts(`${run.key}/inputs/${edge.dataset}`, edge.facets);
    for (const edge of Object.values(run.outputs)) facetConflicts(`${run.key}/outputs/${edge.dataset}`, edge.facets);
  }
  for (const dataset of Object.values(state.datasets)) facetConflicts(dataset.key, dataset.facets);
  if (cursor && !cursor.passComplete) gaps.push({kind: '事件流未收完', next: cursor.next ?? null, detail: '来源事件尚未收完，可再次接收继续补收'});
  return gaps;
}
