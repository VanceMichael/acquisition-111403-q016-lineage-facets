import {createHash, randomUUID} from 'node:crypto';
import {mkdir, readFile, rename, open} from 'node:fs/promises';
import {join} from 'node:path';
import {applyEvent, applySummary, coverageOf, createLineage, describeLineage, normalizeEvent} from './lineage.mjs';

export const digest = bytes => createHash('sha256').update(bytes).digest('hex');

const EMPTY_CURSOR = () => ({next: null, pagesRead: 0, lastPage: null});

export function receiptIdOf(exchange) {
  return digest(Buffer.from(exchange.url + ':' + exchange.status + ':' + digest(exchange.bytes)));
}

async function replace(path, bytes) {
  const temporary = path + '.' + randomUUID() + '.tmp';
  const handle = await open(temporary, 'wx', 0o600);
  try { await handle.writeFile(bytes); await handle.sync(); } finally { await handle.close(); }
  await rename(temporary, path);
}

function emptyIndex() {
  return {
    version: 2,
    records: [],
    exchanges: [],
    rawEvents: [],
    accepted: [],
    rejected: [],
    cursor: EMPTY_CURSOR()
  };
}

// Version 1 only stored the final summary. Keeping those records makes an existing
// archive readable without inventing the event evidence it never received. The old
// reader used the plain run id as the record id, so the missing runId is recovered
// from it rather than guessed.
function migrate(index) {
  if (index.version === 2) return index;
  if (index.version !== 1 || !Array.isArray(index.records) || !Array.isArray(index.exchanges)) {
    throw new Error('归档索引结构无效');
  }
  const records = index.records.map(record => ({
    ...record,
    kind: record.kind ?? 'run-summary',
    runId: record.runId ?? (typeof record.id === 'string' && !record.id.includes(':') ? record.id : null),
    source: record.source ?? '/run-summary'
  }));
  return {...emptyIndex(), records, exchanges:index.exchanges};
}

function validate(index) {
  if (index.version !== 2) throw new Error('归档索引结构无效');
  for (const field of ['records', 'exchanges', 'rawEvents', 'accepted', 'rejected']) {
    if (!Array.isArray(index[field])) throw new Error('归档索引结构无效');
  }
  if (!index.cursor || typeof index.cursor !== 'object') throw new Error('归档索引结构无效');
  for (const exchange of index.exchanges) {
    if (!/^[a-f0-9]{64}$/.test(exchange.hash)) throw new Error('归档摘要无效');
  }
}

function replay(index) {
  const lineage = createLineage();
  for (const record of index.records) applySummary(lineage, record);
  const accepted = index.accepted.map(entry => entry.eventId);
  const acceptedSet = new Set(accepted);
  for (const raw of index.rawEvents) {
    for (const event of raw.events) {
      if (!acceptedSet.has(event.eventId)) continue;
      applyEvent(lineage, {
        event,
        eventTime: event.eventTime,
        eventId: event.eventId,
        eventType: event.eventType,
        receiptId: raw.receiptId,
        pageUrl: raw.pageUrl,
        rawHash: raw.rawHash
      });
    }
  }
  return lineage;
}

export async function openArchive(directory) {
  await mkdir(join(directory, 'objects'), {recursive:true});
  const indexPath = join(directory, 'index.json');
  let index = emptyIndex();
  try { index = JSON.parse(await readFile(indexPath, 'utf8')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  index = migrate(index);
  validate(index);
  for (const exchange of index.exchanges) {
    if (!/^[a-f0-9]{64}$/.test(exchange.hash)) throw new Error('归档摘要无效');
    const bytes = await readFile(join(directory, 'objects', exchange.hash));
    if (digest(bytes) !== exchange.hash) throw new Error('归档原文校验失败');
  }
  let lineage = replay(index);
  let queue = Promise.resolve();

  const snapshot = () => ({
    cursor: structuredClone(index.cursor),
    lineage: describeLineage(lineage, index.cursor),
    coverage: coverageOf(lineage, index.cursor),
    accepted: structuredClone(index.accepted.map(entry => entry.eventId)),
    rejected: structuredClone(index.rejected),
    exchangeCount: index.exchanges.length
  });

  return {
    records: () => structuredClone(index.records),
    exchanges: () => structuredClone(index.exchanges),
    rawEvents: () => structuredClone(index.rawEvents),
    cursor: () => structuredClone(index.cursor),
    lineage: () => describeLineage(lineage, index.cursor),
    coverage: () => coverageOf(lineage, index.cursor),
    snapshot,
    raw: async hash => {
      if (!/^[a-f0-9]{64}$/.test(hash)) throw new Error('原文摘要无效');
      const bytes = await readFile(join(directory, 'objects', hash));
      if (digest(bytes) !== hash) throw new Error('原文校验失败');
      return bytes;
    },
    commit(records, exchanges, {events = [], pages = [], cursor = null} = {}) {
      const operation = queue.then(async () => {
        const next = structuredClone(index);
        const result = {records:0, exchanges:0, events:0, duplicates:0, rejected:0, cursor:structuredClone(next.cursor)};
        for (const exchange of exchanges) {
          const hash = digest(exchange.bytes);
          await replace(join(directory, 'objects', hash), exchange.bytes);
          const receiptId = receiptIdOf(exchange);
          if (!next.exchanges.some(existing => existing.receiptId === receiptId)) {
            next.exchanges.push({receiptId,url:exchange.url,status:exchange.status,contentType:exchange.contentType,hash});
            result.exchanges += 1;
          }
        }
        // Only pages that were actually parsed become event evidence; a failed or
        // malformed response keeps its receipt but is not treated as received data.
        for (const page of pages) {
          if (!page?.rawHash) continue;
          if (!next.rawEvents.some(raw => raw.rawHash === page.rawHash && raw.pageUrl === page.url)) {
            next.rawEvents.push({
              rawHash:page.rawHash, receiptId:page.receiptId ?? null, pageUrl:page.url,
              next:page.next ?? null, count:page.count ?? null, events:[]
            });
          }
        }
        for (const record of records) {
          if (typeof record.id !== 'string' || !record.id) throw new Error('资料必须有非空标识');
          const revision = digest(Buffer.from(JSON.stringify(record)));
          if (!next.records.some(existing => existing.id === record.id && existing.revision === revision)) {
            next.records.push({...record, revision});
            result.records += 1;
          }
        }
        const knownEvents = new Set(next.accepted.map(entry => entry.eventId));
        const receivedAt = new Date().toISOString();
        for (const envelope of events) {
          let event;
          try {
            event = normalizeEvent(envelope.event);
          } catch (error) {
            next.rejected.push({
              receiptId: envelope.receiptId ?? null,
              pageUrl: envelope.pageUrl ?? null,
              rawHash: envelope.rawHash ?? null,
              message: error.message
            });
            result.rejected += 1;
            continue;
          }
          if (knownEvents.has(event.eventId)) {
            result.duplicates += 1;
            continue;
          }
          knownEvents.add(event.eventId);
          const raw = next.rawEvents.find(item => item.rawHash === envelope.rawHash)
            || (() => {
              const created = {rawHash:envelope.rawHash ?? null, receiptId:envelope.receiptId ?? null, pageUrl:envelope.pageUrl ?? null, events:[]};
              next.rawEvents.push(created);
              return created;
            })();
          raw.events.push(structuredClone(envelope.event));
          next.accepted.push({
            eventId: event.eventId,
            eventType: event.eventType,
            eventTime: event.eventTime,
            runId: event.runId,
            receivedAt,
            receiptId: envelope.receiptId ?? null,
            pageUrl: envelope.pageUrl ?? null,
            rawHash: envelope.rawHash ?? null
          });
          result.events += 1;
        }
        next.cursor = {
          next: cursor?.next ?? null,
          pagesRead: (index.cursor.pagesRead ?? 0) + (cursor?.pages ?? 0),
          lastPage: cursor?.lastPage ?? index.cursor.lastPage ?? null
        };
        await replace(indexPath, Buffer.from(JSON.stringify(next, null, 2)));
        index = next;
        lineage = replay(index);
        result.cursor = structuredClone(next.cursor);
        return result;
      });
      queue = operation.catch(() => {});
      return operation;
    }
  };
}
