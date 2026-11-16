import {createHash} from 'node:crypto';

const TERMINAL_TYPES = new Set(['COMPLETE', 'FAIL', 'ABORT']);
const EVENT_TYPES = new Set(['START', 'RUNNING', 'COMPLETE', 'FAIL', 'ABORT']);
const PROGRESS_RANK = {START: 0, RUNNING: 1};

const digestOf = value => createHash('sha256').update(value).digest('hex');

const segment = value => encodeURIComponent(String(value ?? ''));

export const jobIdOf = job => `job:${segment(job?.namespace)}/${segment(job?.name)}`;
export const runIdOf = (job, runId) => `run:${segment(job?.namespace)}/${segment(job?.name)}/${segment(runId)}`;
export const datasetIdOf = dataset => `dataset:${segment(dataset?.namespace)}/${segment(dataset?.name)}`;

export function createLineage() {
  return {jobs: {}, runs: {}, datasets: {}, relations: {}};
}

function requireText(value, field) {
  if (typeof value !== 'string' || !value) throw new Error(`事件缺少 ${field}`);
  return value;
}

export function normalizeEvent(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new Error('事件必须是对象');
  const eventId = requireText(raw.eventId, 'eventId');
  const eventType = requireText(raw.eventType, 'eventType');
  if (!EVENT_TYPES.has(eventType)) throw new Error(`事件 ${eventId} 的 eventType 不受支持: ${eventType}`);
  if (typeof raw.eventTime !== 'number' || !Number.isFinite(raw.eventTime)) throw new Error(`事件 ${eventId} 缺少 eventTime`);
  const runId = requireText(raw.run?.runId, 'run.runId');
  const job = {namespace: requireText(raw.job?.namespace, 'job.namespace'), name: requireText(raw.job?.name, 'job.name')};
  const list = value => {
    if (value === undefined) return [];
    if (!Array.isArray(value)) throw new Error(`事件 ${eventId} 的输入输出必须是数组`);
    return value;
  };
  for (const [field, datasets] of [['inputs', list(raw.inputs)], ['outputs', list(raw.outputs)]]) {
    for (const dataset of datasets) {
      requireText(dataset?.namespace, `${field}.namespace`);
      requireText(dataset?.name, `${field}.name`);
    }
  }
  return {eventId, eventType, eventTime: raw.eventTime, runId, job, inputs: list(raw.inputs), outputs: list(raw.outputs)};
}

function ensureJob(lineage, job, envelope) {
  const id = jobIdOf(job);
  const record = lineage.jobs[id] || (lineage.jobs[id] = {
    id, kind: 'job', namespace: job.namespace, name: job.name,
    firstEventTime: envelope.eventTime ?? null, lastEventTime: envelope.eventTime ?? null, eventIds: []
  });
  if (envelope.eventTime !== undefined && envelope.eventTime !== null) {
    if (record.firstEventTime === null || envelope.eventTime < record.firstEventTime) record.firstEventTime = envelope.eventTime;
    if (record.lastEventTime === null || envelope.eventTime > record.lastEventTime) record.lastEventTime = envelope.eventTime;
  }
  if (envelope.eventId && !record.eventIds.includes(envelope.eventId)) record.eventIds.push(envelope.eventId);
  return record;
}

function ensureRun(lineage, job, runId, envelope) {
  const jobId = jobIdOf(job);
  const id = runIdOf(job, runId);
  const record = lineage.runs[id] || (lineage.runs[id] = {
    id, kind: 'run', runId, jobId, job: {...job},
    status: null, statusEventId: null, statusEventTime: null, terminal: false,
    firstEventTime: null, lastEventTime: null, eventIds: [], eventTypes: [],
    timeline: [], inputs: [], outputs: [], summary: null
  });
  if (envelope.eventId && !record.eventIds.includes(envelope.eventId)) {
    record.eventIds.push(envelope.eventId);
    if (envelope.eventType) {
      if (!record.eventTypes.includes(envelope.eventType)) record.eventTypes.push(envelope.eventType);
      record.timeline.push({eventId: envelope.eventId, eventType: envelope.eventType, eventTime: envelope.eventTime ?? null});
    }
  }
  if (envelope.eventTime !== undefined && envelope.eventTime !== null) {
    if (record.firstEventTime === null || envelope.eventTime < record.firstEventTime) record.firstEventTime = envelope.eventTime;
    if (record.lastEventTime === null || envelope.eventTime > record.lastEventTime) record.lastEventTime = envelope.eventTime;
  }
  return record;
}

function ensureDataset(lineage, dataset, envelope) {
  const id = datasetIdOf(dataset);
  const record = lineage.datasets[id] || (lineage.datasets[id] = {
    id, kind: 'dataset', namespace: dataset.namespace, name: dataset.name,
    firstEventTime: envelope.eventTime ?? null, lastEventTime: envelope.eventTime ?? null,
    eventIds: [], facets: [], inputOf: [], outputOf: []
  });
  if (envelope.eventTime !== undefined && envelope.eventTime !== null) {
    if (record.firstEventTime === null || envelope.eventTime < record.firstEventTime) record.firstEventTime = envelope.eventTime;
    if (record.lastEventTime === null || envelope.eventTime > record.lastEventTime) record.lastEventTime = envelope.eventTime;
  }
  if (envelope.eventId && !record.eventIds.includes(envelope.eventId)) record.eventIds.push(envelope.eventId);
  return record;
}

function ensureRelation(lineage, kind, run, dataset) {
  const id = `${kind}:${run.id}:${dataset.id}`;
  const record = lineage.relations[id] || (lineage.relations[id] = {
    id, kind, relation: kind === 'output' ? 'produced' : 'consumed',
    runId: run.id, jobId: run.jobId, datasetId: dataset.id,
    dataset: {namespace: dataset.namespace, name: dataset.name},
    firstEventTime: null, lastEventTime: null, eventIds: [],
    summaryEvidence: false, facets: []
  });
  return record;
}

function facetKey(facetClass, name, producer, schemaURL) {
  // Producer and schemaURL both participate: one producer's update must not overwrite
  // another producer's view of the same named facet.
  return `${facetClass}|${segment(name)}|${segment(producer ?? '')}|${segment(schemaURL ?? '')}`;
}

function addFacetGroup(owner, facetClass, facets, envelope) {
  if (!facets || typeof facets !== 'object' || Array.isArray(facets)) return;
  for (const [name, value] of Object.entries(facets)) {
    // A missing facet in a later notification says nothing about facets already received.
    if (value === undefined || value === null) continue;
    const facet = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
    const producer = typeof facet.producer === 'string' ? facet.producer : null;
    const schemaURL = typeof facet.schemaURL === 'string' ? facet.schemaURL : null;
    const key = facetKey(facetClass, name, producer, schemaURL);
    const eventTime = envelope.eventTime ?? null;
    const eventId = envelope.eventId ?? null;
    const revision = digestOf(JSON.stringify(value));
    let entry = owner.facets.find(existing => existing.key === key);
    if (!entry) {
      entry = {
        key, name, facetClass, producer, schemaURL,
        producers: producer ? [producer] : [],
        revision,
        value: structuredClone(value),
        eventId, eventTime,
        receiptId: envelope.receiptId ?? null,
        pageUrl: envelope.pageUrl ?? null,
        rawHash: envelope.rawHash ?? null,
        sourceEventIds: eventId ? [eventId] : [],
        observations: [],
        firstEventTime: eventTime, lastEventTime: eventTime
      };
      owner.facets.push(entry);
    } else {
      if (producer && !entry.producers.includes(producer)) entry.producers.push(producer);
      if (eventId && !entry.sourceEventIds.includes(eventId)) entry.sourceEventIds.push(eventId);
      if (isNewer({eventTime, eventId}, entry)) {
        entry.value = structuredClone(value);
        entry.revision = revision;
        entry.producer = producer ?? entry.producer;
        entry.eventId = eventId;
        entry.eventTime = eventTime;
        entry.receiptId = envelope.receiptId ?? null;
        entry.pageUrl = envelope.pageUrl ?? null;
        entry.rawHash = envelope.rawHash ?? null;
      }
      if (eventTime !== null && (entry.firstEventTime === null || eventTime < entry.firstEventTime)) entry.firstEventTime = eventTime;
      if (eventTime !== null && (entry.lastEventTime === null || eventTime > entry.lastEventTime)) entry.lastEventTime = eventTime;
    }
    // Every distinct payload stays recoverable with the event, page and original bytes
    // that carried it, even after a newer value becomes the current one.
    const observation = entry.observations.find(item => item.revision === revision);
    if (observation) {
      if (eventId && !observation.eventIds.includes(eventId)) observation.eventIds.push(eventId);
      if (eventTime !== null && (observation.lastEventTime === null || eventTime > observation.lastEventTime)) observation.lastEventTime = eventTime;
    } else {
      entry.observations.push({
        revision,
        value: structuredClone(value),
        eventId, eventTime,
        receiptId: envelope.receiptId ?? null,
        pageUrl: envelope.pageUrl ?? null,
        rawHash: envelope.rawHash ?? null,
        eventIds: eventId ? [eventId] : [],
        firstEventTime: eventTime, lastEventTime: eventTime
      });
    }
  }
}

function isNewer(candidate, reference) {
  const left = candidate.eventTime ?? -Infinity;
  const right = reference.eventTime ?? -Infinity;
  if (left !== right) return left > right;
  return (candidate.eventId ?? '') > (reference.eventId ?? '');
}

function shouldReplaceStatus(run, eventType, eventTime, eventId) {
  if (TERMINAL_TYPES.has(run.status)) {
    // A late START/RUNNING may still contribute facets, but never rolls the run back.
    if (!TERMINAL_TYPES.has(eventType)) return false;
    if (eventTime !== run.statusEventTime) return eventTime > run.statusEventTime;
    return eventId > (run.statusEventId || '');
  }
  if (TERMINAL_TYPES.has(eventType)) return true;
  // Progress only moves forward, no matter which order the notifications arrive in.
  const candidate = PROGRESS_RANK[eventType];
  const current = run.status === null ? -1 : PROGRESS_RANK[run.status];
  if (candidate === undefined || current === undefined) return false;
  if (candidate !== current) return candidate > current;
  if (run.status === null) return true;
  if (eventTime !== run.statusEventTime) return eventTime > run.statusEventTime;
  return eventId > (run.statusEventId || '');
}

function commitEvent(owner, envelope) {
  if (envelope.eventTime === null) return;
  if (owner.firstEventTime === null || envelope.eventTime < owner.firstEventTime) owner.firstEventTime = envelope.eventTime;
  if (owner.lastEventTime === null || envelope.eventTime > owner.lastEventTime) owner.lastEventTime = envelope.eventTime;
  if (envelope.eventId && !owner.eventIds.includes(envelope.eventId)) owner.eventIds.push(envelope.eventId);
}

function applySide(lineage, run, datasets, side, envelope) {
  for (const dataset of datasets) {
    const datasetRecord = ensureDataset(lineage, dataset, envelope);
    const relation = ensureRelation(lineage, side, run, datasetRecord);
    commitEvent(relation, envelope);
    const bucket = side === 'input' ? run.inputs : run.outputs;
    if (!bucket.includes(datasetRecord.id)) bucket.push(datasetRecord.id);
    const roleBucket = side === 'input' ? datasetRecord.inputOf : datasetRecord.outputOf;
    if (!roleBucket.includes(run.id)) roleBucket.push(run.id);

    // Static material about the dataset and facets about this particular input/output
    // are deliberately kept in different buckets.
    addFacetGroup(datasetRecord, 'dataset', dataset.facets, envelope);
    addFacetGroup(relation, side, side === 'input' ? dataset.inputFacets : dataset.outputFacets, envelope);
  }
}

export function applyEvent(lineage, envelope) {
  const event = normalizeEvent(envelope.event);
  const evidence = {
    eventId: event.eventId,
    eventType: event.eventType,
    eventTime: event.eventTime,
    receiptId: envelope.receiptId ?? null,
    pageUrl: envelope.pageUrl ?? null,
    rawHash: envelope.rawHash ?? null
  };
  const job = ensureJob(lineage, event.job, evidence);
  const run = ensureRun(lineage, event.job, event.runId, evidence);
  if (shouldReplaceStatus(run, event.eventType, event.eventTime, event.eventId)) {
    run.status = event.eventType;
    run.terminal = TERMINAL_TYPES.has(event.eventType);
    run.statusEventId = event.eventId;
    run.statusEventTime = event.eventTime;
  }
  applySide(lineage, run, event.inputs, 'input', evidence);
  applySide(lineage, run, event.outputs, 'output', evidence);
  return run;
}

export function applySummary(lineage, record) {
  if (!record || (record.kind && record.kind !== 'run-summary')) return null;
  if (!record.runId || !record.job?.namespace || !record.job?.name) return null;
  const envelope = {
    eventId: null, eventTime: null,
    receiptId: record.receiptId ?? null,
    pageUrl: record.pageUrl ?? null,
    rawHash: record.rawHash ?? null
  };
  ensureJob(lineage, record.job, envelope);
  const run = ensureRun(lineage, record.job, record.runId, envelope);
  const outputs = Array.isArray(record.outputs) ? record.outputs : [];
  run.summary = {
    received: true,
    recordId: record.id,
    source: record.source ?? '/run-summary',
    receiptId: record.receiptId ?? null,
    pageUrl: record.pageUrl ?? null,
    rawHash: record.rawHash ?? null,
    outputs: structuredClone(outputs)
  };
  for (const dataset of outputs) {
    if (!dataset?.namespace || !dataset?.name) continue;
    const datasetRecord = ensureDataset(lineage, dataset, envelope);
    const relation = ensureRelation(lineage, 'output', run, datasetRecord);
    relation.summaryEvidence = true;
    if (!run.outputs.includes(datasetRecord.id)) run.outputs.push(datasetRecord.id);
    if (!datasetRecord.outputOf.includes(run.id)) datasetRecord.outputOf.push(run.id);
    addFacetGroup(datasetRecord, 'dataset', dataset.facets, {...envelope, source: record.source});
    addFacetGroup(relation, 'output', dataset.outputFacets, {...envelope, source: record.source});
  }
  return run;
}

function facetConflicts(scope, owner, facets) {
  const groups = new Map();
  for (const facet of facets) {
    const key = `${facet.facetClass}|${facet.name}`;
    const group = groups.get(key) || [];
    group.push(facet);
    groups.set(key, group);
  }
  const conflicts = [];
  for (const group of groups.values()) {
    const schemaURLs = [...new Set(group.map(facet => facet.schemaURL ?? '未声明'))];
    if (schemaURLs.length < 2) continue;
    conflicts.push({
      scope,
      runId: owner.runId ?? null,
      datasetId: owner.datasetId ?? owner.id,
      facetClass: group[0].facetClass,
      facetName: group[0].name,
      schemaURLs,
      producers: [...new Set(group.flatMap(facet => facet.producers?.length ? facet.producers : ['未声明']))],
      variants: group.map(facet => facet.key),
      message: `同名分面“${group[0].name}”存在 ${schemaURLs.length} 种结构声明，已分别保留`
    });
  }
  return conflicts;
}

export function conflictsOf(lineage) {
  const conflicts = [];
  for (const dataset of Object.values(lineage.datasets)) conflicts.push(...facetConflicts('dataset', dataset, dataset.facets));
  for (const relation of Object.values(lineage.relations)) conflicts.push(...facetConflicts(relation.kind, relation, relation.facets));
  return conflicts;
}

export function coverageOf(lineage, cursor = null) {
  const runs = [];
  const pending = [];
  for (const run of Object.values(lineage.runs)) {
    const missing = [];
    if (!run.terminal) missing.push({code: 'terminal', message: '尚未收到终态事件（COMPLETE/FAIL/ABORT）'});
    if (!run.summary) missing.push({code: 'summary', message: '尚未收到最终作业摘要（/run-summary）'});
    for (const relation of Object.values(lineage.relations).filter(item => item.runId === run.id)) {
      const sideFacets = relation.facets.some(facet => facet.facetClass === relation.kind);
      if (!sideFacets) {
        missing.push({
          code: `${relation.kind}-facets`, datasetId: relation.datasetId, relationId: relation.id,
          message: `尚未收到该数据集本次${relation.kind === 'input' ? '输入' : '输出'}分面`
        });
      }
    }
    // Static dataset material is independent of this run's input/output facets, so a
    // run can be missing it even when both sides reported their own facets.
    for (const datasetId of new Set([...run.inputs, ...run.outputs])) {
      const dataset = lineage.datasets[datasetId];
      if (dataset && !dataset.facets.length) {
        missing.push({code: 'dataset-facets', datasetId, message: '尚未收到该数据集的静态资料分面'});
      }
    }
    const entry = {
      runId: run.id, runInstanceId: run.runId, jobId: run.jobId,
      status: run.status, terminal: run.terminal,
      receivedEventTypes: [...run.eventTypes],
      missing
    };
    runs.push(entry);
    for (const item of missing) pending.push({scope: 'run', id: run.id, ...item});
  }
  const datasets = Object.values(lineage.datasets).map(dataset => ({
    datasetId: dataset.id, namespace: dataset.namespace, name: dataset.name,
    facetCount: dataset.facets.length,
    inputOf: [...dataset.inputOf], outputOf: [...dataset.outputOf],
    missing: dataset.facets.length ? [] : [{code: 'dataset-facets', message: '尚未收到该数据集的静态资料分面'}]
  }));
  for (const dataset of datasets) for (const item of dataset.missing) pending.push({scope: 'dataset', id: dataset.datasetId, ...item});
  const conflicts = conflictsOf(lineage);
  return {
    cursor: structuredClone(cursor),
    pendingPages: Boolean(cursor?.next),
    runs,
    datasets,
    conflicts,
    pendingEvidence: pending,
    totals: {
      runs: runs.length,
      terminalRuns: runs.filter(run => run.terminal).length,
      runsWithMissingEvidence: runs.filter(run => run.missing.length).length,
      datasets: datasets.length,
      pendingEvidence: pending.length,
      conflicts: conflicts.length
    }
  };
}

export function describeLineage(lineage, cursor = null) {
  return {
    jobs: structuredClone(lineage.jobs),
    runs: Object.values(structuredClone(lineage.runs)),
    datasets: Object.values(structuredClone(lineage.datasets)),
    relations: Object.values(structuredClone(lineage.relations)),
    conflicts: conflictsOf(lineage),
    coverage: coverageOf(lineage, cursor)
  };
}
