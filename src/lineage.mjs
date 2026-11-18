// 作业血缘的持续合成（纯函数）。
// 身份分离：作业(job)、运行实例(run)、数据集(dataset)各自有独立标识；
// 静态资料分面(dataset.facets)与本次输入输出分面(inputFacets/outputFacets)分属不同作用域，互不覆盖。
// 合成只依据事件日志，不持有可变状态；原始事件的字节凭据留在归档层。

export const EVENT_TYPES = Object.freeze(['START', 'RUNNING', 'COMPLETE', 'FAIL', 'ABORT']);
const TERMINAL_TYPES = new Set(['COMPLETE', 'FAIL', 'ABORT']);
const RANK = {START: 1, RUNNING: 2, COMPLETE: 3, FAIL: 3, ABORT: 3};

export const isTerminal = type => TERMINAL_TYPES.has(type);
const rankOf = type => RANK[type] ?? 0;

const text = (value, key) => (typeof value?.[key] === 'string' && value[key].length > 0 ? value[key] : undefined);
const isPlainObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const enc = value => encodeURIComponent(String(value));

export const idForJob = job => `job:${enc(job.namespace)}/${enc(job.name)}`;
export const idForRun = runId => `run:${enc(runId)}`;
export const idForDataset = dataset => `dataset:${enc(dataset.namespace)}/${enc(dataset.name)}`;
export const idForEdge = (runId, direction, dataset) =>
  `edge:${enc(runId)}:${direction}:${enc(dataset.namespace)}/${enc(dataset.name)}`;
const idForFacet = (scopeId, facet, producer, schemaURL) =>
  `facet:${scopeId}:${enc(facet)}#${enc(producer)}|${enc(schemaURL)}`;

// 归一化事件时间：数字（毫秒时间戳）或 ISO 字符串均可；无效返回 null。
export function normalizeEventTime(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.length) {
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
}

// 检查单条事件是否可接纳；不可接纳时返回原因及尽力提取的身份线索，供隔离区登记。
export function inspectEvent(value) {
  const hints = {};
  const fail = reason => ({ok: false, reason, hints});
  if (!isPlainObject(value)) return fail('事件不是 JSON 对象');
  hints.eventId = text(value, 'eventId');
  hints.eventType = text(value, 'eventType');
  hints.runId = text(value.run, 'runId');
  const eventTime = normalizeEventTime(value.eventTime);
  if (eventTime !== null) hints.eventTime = eventTime;
  if (!hints.eventId) return fail('eventId 缺失或不是非空字符串');
  if (!EVENT_TYPES.includes(hints.eventType)) return fail(`eventType 不在约定范围: ${String(value.eventType)}`);
  if (eventTime === null) return fail('eventTime 缺失或无法解析');
  if (!hints.runId) return fail('run.runId 缺失或不是非空字符串');
  const namespace = text(value.job, 'namespace');
  const name = text(value.job, 'name');
  if (!namespace || !name) return fail('job.namespace/job.name 缺失');
  hints.job = {namespace, name};
  for (const direction of ['inputs', 'outputs']) {
    const entries = value[direction];
    if (entries === undefined) continue;
    if (!Array.isArray(entries)) return fail(`${direction} 不是数组`);
    for (const [index, entry] of entries.entries()) {
      if (!isPlainObject(entry) || !text(entry, 'namespace') || !text(entry, 'name')) {
        return fail(`${direction}[${index}] 缺少 namespace/name`);
      }
      for (const facetKey of ['facets', direction === 'inputs' ? 'inputFacets' : 'outputFacets']) {
        if (entry[facetKey] !== undefined && !isPlainObject(entry[facetKey])) {
          return fail(`${direction}[${index}].${facetKey} 不是对象`);
        }
      }
    }
  }
  return {ok: true, reason: null, hints};
}

function emptyFacetGroup() {
  return {conflicted: false, variants: new Map()};
}

// 把一个分面并入作用域分组。同名分面按 (producer, schemaURL) 区分结构声明：
// 声明不同则保留为冲突变体，绝不盲合并；本次未发送的字段不会触碰旧资料。
function absorbFacet(groupStore, facetName, facet, scopeId, observation, issues) {
  if (!isPlainObject(facet)) {
    issues.push(`分面 ${facetName} 不是对象，已忽略`);
    return {invalid: true, facetName};
  }
  const producer = text(facet, 'producer');
  const schemaURL = text(facet, 'schemaURL');
  if (!producer || !schemaURL) {
    issues.push(`分面 ${facetName} 缺少 producer/schemaURL，已忽略`);
    return {invalid: true, facetName};
  }
  let group = groupStore.get(facetName);
  if (!group) {
    group = emptyFacetGroup();
    groupStore.set(facetName, group);
  }
  const variantKey = `${producer}|${schemaURL}`;
  let variant = group.variants.get(variantKey);
  if (!variant) {
    variant = {
      id: idForFacet(scopeId, facetName, producer, schemaURL),
      facet: facetName, producer, schemaURL,
      payload: {}, observations: []
    };
    group.variants.set(variantKey, variant);
  }
  if (group.variants.size > 1) group.conflicted = true;
  // 只写入本次实际发送的字段，缺失字段不解释为清空。
  for (const [key, fieldValue] of Object.entries(facet)) {
    if (key === 'producer' || key === 'schemaURL') continue;
    variant.payload[key] = fieldValue;
  }
  variant.observations.push(observation);
  return {invalid: false, facetName, variant, group};
}

function facetGroupRecords(groupStore, scopeId, scopeKind, records) {
  for (const [facetName, group] of [...groupStore.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    for (const variant of [...group.variants.values()].sort((a, b) => a.id.localeCompare(b.id))) {
      records.push({
        id: variant.id, kind: 'facet', scopeKind, scopeId,
        facet: variant.facet, producer: variant.producer, schemaURL: variant.schemaURL,
        conflicted: group.conflicted,
        payload: variant.payload,
        evidence: variant.observations.map(({eventId, receiptId, eventTime}) => ({eventId, receiptId, eventTime}))
      });
    }
  }
}

// 由只追加的事件日志合成可查询血缘。输入：
// journal     已接纳事件（含 receiptId 与原始载荷）
// quarantined 被隔离的不可解析通知
// gaps        分页缺口与未读尾指针
export function synthesizeLineage({journal = [], quarantined = [], gaps = []} = {}) {
  const jobs = new Map();
  const runs = new Map();
  const datasets = new Map();
  const records = [];
  const pending = [];

  const ordered = [...journal]
    .map((entry, seq) => ({entry, seq}))
    .sort((a, b) => (a.entry.eventTime - b.entry.eventTime) || (a.seq - b.seq));

  for (const {entry, seq} of ordered) {
    const event = entry.payload;
    const jobKey = `${event.job.namespace}\u0000${event.job.name}`;
    if (!jobs.has(jobKey)) {
      jobs.set(jobKey, {id: idForJob(event.job), namespace: event.job.namespace, name: event.job.name, runIds: []});
    }
    const job = jobs.get(jobKey);

    let run = runs.get(entry.runId);
    if (!run) {
      run = {
        id: idForRun(entry.runId), runId: entry.runId, jobId: job.id,
        events: [], edges: new Map(), conflictReasons: []
      };
      runs.set(entry.runId, run);
      job.runIds.push(entry.runId);
    } else if (run.jobId !== job.id) {
      run.conflictReasons.push(`事件 ${entry.eventId} 声称作业 ${job.id}，与运行已登记作业 ${run.jobId} 不一致`);
    }

    const stateCandidate = {eventId: entry.eventId, eventType: entry.eventType, eventTime: entry.eventTime, receiptId: entry.receiptId, seq};
    const current = run.state;
    if (!current) {
      run.state = stateCandidate;
    } else {
      const currentTerminal = isTerminal(current.eventType);
      const candidateTerminal = isTerminal(entry.eventType);
      if (candidateTerminal && currentTerminal) {
        // 终态之间：更晚事件时间为准，同时间按接纳顺序；不依赖到达顺序。
        if (entry.eventTime > current.eventTime || (entry.eventTime === current.eventTime && seq > current.seq)) {
          run.state = stateCandidate;
        }
      } else if (candidateTerminal) {
        run.state = stateCandidate; // 终态覆盖非终态
      } else if (!currentTerminal) {
        if (rankOf(entry.eventType) > rankOf(current.eventType)) run.state = stateCandidate;
      }
      // 迟到的 START/RUNNING 不能让终态倒退：候选非终态、当前终态时直接保留。
    }
    run.events.push(stateCandidate);

    const observation = {
      eventId: entry.eventId, eventTime: entry.eventTime, receiptId: entry.receiptId, runId: entry.runId
    };

    for (const direction of ['inputs', 'outputs']) {
      const entries = event[direction];
      if (!Array.isArray(entries)) continue;
      const directionCode = direction === 'inputs' ? 'IN' : 'OUT';
      const scopedKey = direction === 'inputs' ? 'inputFacets' : 'outputFacets';
      for (const datasetEntry of entries) {
        const datasetKey = `${datasetEntry.namespace}\u0000${datasetEntry.name}`;
        let dataset = datasets.get(datasetKey);
        if (!dataset) {
          dataset = {
            id: idForDataset(datasetEntry), namespace: datasetEntry.namespace, name: datasetEntry.name,
            staticFacets: new Map(), producedBy: [], consumedBy: []
          };
          datasets.set(datasetKey, dataset);
        }
        const edgeKey = `${entry.runId}\u0000${directionCode}\u0000${datasetKey}`;
        let edge = run.edges.get(edgeKey);
        if (!edge) {
          edge = {
            id: idForEdge(entry.runId, directionCode, datasetEntry),
            runId: entry.runId, direction: directionCode, datasetId: dataset.id,
            scopedFacets: new Map(), observations: [], issues: []
          };
          run.edges.set(edgeKey, edge);
          if (directionCode === 'OUT' && !dataset.producedBy.includes(run.id)) dataset.producedBy.push(run.id);
          if (directionCode === 'IN' && !dataset.consumedBy.includes(run.id)) dataset.consumedBy.push(run.id);
        }
        edge.observations.push(observation);

        // 静态资料分面：挂在数据集身份下，跨运行累积，不与本次分面互盖。
        if (isPlainObject(datasetEntry.facets)) {
          for (const [facetName, facet] of Object.entries(datasetEntry.facets)) {
            const result = absorbFacet(dataset.staticFacets, facetName, facet, dataset.id, observation, edge.issues);
            if (result.invalid) pending.push({kind: 'facet-invalid', scope: 'dataset', scopeId: dataset.id, facet: result.facetName, eventId: entry.eventId, receiptId: entry.receiptId});
          }
        }
        // 本次输入/输出分面：挂在运行-数据集关系下，只作用本次。
        if (isPlainObject(datasetEntry[scopedKey])) {
          for (const [facetName, facet] of Object.entries(datasetEntry[scopedKey])) {
            const result = absorbFacet(edge.scopedFacets, facetName, facet, edge.id, observation, edge.issues);
            if (result.invalid) pending.push({kind: 'facet-invalid', scope: 'edge', scopeId: edge.id, facet: result.facetName, eventId: entry.eventId, receiptId: entry.receiptId});
          }
        }
      }
    }
  }

  // 确定性顺序输出结构化记录。
  for (const job of [...jobs.values()].sort((a, b) => a.id.localeCompare(b.id))) {
    records.push({id: job.id, kind: 'job', namespace: job.namespace, name: job.name, runIds: [...job.runIds].sort()});
  }
  for (const run of [...runs.values()].sort((a, b) => a.id.localeCompare(b.id))) {
    const edges = [...run.edges.values()].sort((a, b) => a.id.localeCompare(b.id));
    records.push({
      id: run.id, kind: 'run', runId: run.runId, jobId: run.jobId,
      state: run.state ? run.state.eventType : null,
      stateEvidence: run.state ? {eventId: run.state.eventId, eventTime: run.state.eventTime, receiptId: run.state.receiptId} : null,
      terminal: run.state ? isTerminal(run.state.eventType) : false,
      edgeIds: edges.map(edge => edge.id),
      eventIds: run.events.map(item => item.eventId),
      conflictReasons: run.conflictReasons
    });
    if (!run.state || !isTerminal(run.state.eventType)) {
      pending.push({
        kind: 'run-terminal-missing',
        runId: run.runId, jobId: run.jobId,
        observedState: run.state ? run.state.eventType : null,
        lastEventTime: run.events.length ? run.events[run.events.length - 1].eventTime : null,
        message: `运行 ${run.runId} 尚未收到终态事件（COMPLETE/FAIL/ABORT），现有结论可被后续证据补充`
      });
    }
    for (const edge of edges) {
      records.push({
        id: edge.id, kind: 'edge', runId: edge.runId, direction: edge.direction, datasetId: edge.datasetId,
        evidence: edge.observations.map(({eventId, receiptId, eventTime}) => ({eventId, receiptId, eventTime})),
        issues: edge.issues
      });
      facetGroupRecords(edge.scopedFacets, edge.id, 'run-io', records);
      for (const [facetName, group] of edge.scopedFacets) {
        if (group.conflicted) {
          pending.push({
            kind: 'facet-schema-conflict', scope: 'run-io', scopeId: edge.id, facet: facetName,
            schemaURLs: [...group.variants.keys()].map(key => key.split('|')[1]).sort(),
            message: `关系 ${edge.id} 的同名分面 ${facetName} 存在不同结构声明，变体已全部保留，需人工裁决`
          });
        }
      }
    }
  }
  for (const dataset of [...datasets.values()].sort((a, b) => a.id.localeCompare(b.id))) {
    records.push({
      id: dataset.id, kind: 'dataset', namespace: dataset.namespace, name: dataset.name,
      producedBy: [...dataset.producedBy].sort(), consumedBy: [...dataset.consumedBy].sort()
    });
    facetGroupRecords(dataset.staticFacets, dataset.id, 'dataset-static', records);
    for (const [facetName, group] of dataset.staticFacets) {
      if (group.conflicted) {
        pending.push({
          kind: 'facet-schema-conflict', scope: 'dataset-static', scopeId: dataset.id, facet: facetName,
          schemaURLs: [...group.variants.keys()].map(key => key.split('|')[1]).sort(),
          message: `数据集 ${dataset.id} 的静态分面 ${facetName} 存在不同结构声明，变体已全部保留，需人工裁决`
        });
      }
    }
  }
  for (const entry of [...journal].sort((a, b) => a.receiptId.localeCompare(b.receiptId))) {
    records.push({
      id: `admission:${entry.receiptId}`, kind: 'admission',
      receiptId: entry.receiptId, eventId: entry.eventId, eventType: entry.eventType,
      eventTime: entry.eventTime, runId: entry.runId, sourceUrl: entry.sourceUrl,
      eventHash: entry.eventHash, admittedAt: entry.admittedAt
    });
  }
  for (const item of [...quarantined].sort((a, b) => a.receiptId.localeCompare(b.receiptId))) {
    records.push({id: `quarantine:${item.receiptId}`, kind: 'quarantine', ...item});
    pending.push({
      kind: 'malformed-event', receiptId: item.receiptId, reason: item.reason,
      sourceUrl: item.sourceUrl, eventId: item.eventId ?? null,
      message: `原始通知无法解析（${item.reason}），原文已留证，待来源补发合规证据`
    });
  }
  for (const gap of gaps) {
    records.push({id: `gap:${gap.kind}:${gap.url}`, kind: 'gap', ...gap});
    if (gap.kind === 'tail') {
      if (!gap.clearedAt) {
        pending.push({kind: 'event-stream-open', url: gap.url, attemptedAt: gap.attemptedAt,
          message: `事件流仍有后续页未读（尾指针 ${gap.url}），可能还有未到证据`});
      }
    } else {
      pending.push({kind: 'event-page-unavailable', url: gap.url, status: gap.status ?? null, attemptedAt: gap.attemptedAt,
        message: `事件页 ${gap.url} 暂不可读（${gap.status ?? gap.message}），缺口已登记，可继续补收`});
    }
  }

  const facetCount = records.filter(record => record.kind === 'facet').length;
  const summary = {
    jobs: jobs.size,
    runs: runs.size,
    datasets: datasets.size,
    edges: [...runs.values()].reduce((total, run) => total + run.edges.size, 0),
    facetVariants: facetCount,
    admissions: journal.length,
    quarantined: quarantined.length,
    gaps: gaps.length,
    pending: pending.length
  };
  return {
    generatedFrom: journal.map(entry => entry.receiptId).sort(),
    summary,
    jobs: [...jobs.values()].map(({runIds, ...job}) => ({...job, runIds: [...runIds].sort()})).sort((a, b) => a.id.localeCompare(b.id)),
    runs: [...runs.values()].sort((a, b) => a.id.localeCompare(b.id)).map(run => ({
      id: run.id, runId: run.runId, jobId: run.jobId,
      state: run.state?.eventType ?? null,
      stateEvidence: run.state ? {eventId: run.state.eventId, eventTime: run.state.eventTime, receiptId: run.state.receiptId} : null,
      terminal: run.state ? isTerminal(run.state.eventType) : false,
      edges: [...run.edges.values()].sort((a, b) => a.id.localeCompare(b.id)).map(edge => ({
        id: edge.id, direction: edge.direction, datasetId: edge.datasetId,
        facets: [...edge.scopedFacets.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([facetName, group]) => ({
          facet: facetName, conflicted: group.conflicted,
          variants: [...group.variants.values()].map(variant => ({
            producer: variant.producer, schemaURL: variant.schemaURL, payload: variant.payload,
            evidence: variant.observations.map(({eventId, receiptId}) => ({eventId, receiptId}))
          }))
        })),
        evidence: edge.observations.map(({eventId, receiptId}) => ({eventId, receiptId})),
        issues: edge.issues
      })),
      conflictReasons: run.conflictReasons
    })),
    datasets: [...datasets.values()].sort((a, b) => a.id.localeCompare(b.id)).map(dataset => ({
      id: dataset.id, namespace: dataset.namespace, name: dataset.name,
      producedBy: [...dataset.producedBy].sort(), consumedBy: [...dataset.consumedBy].sort(),
      facets: [...dataset.staticFacets.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([facetName, group]) => ({
        facet: facetName, conflicted: group.conflicted,
        variants: [...group.variants.values()].map(variant => ({
          producer: variant.producer, schemaURL: variant.schemaURL, payload: variant.payload,
          evidence: variant.observations.map(({eventId, receiptId}) => ({eventId, receiptId}))
        }))
      }))
    })),
    records,
    pending
  };
}
