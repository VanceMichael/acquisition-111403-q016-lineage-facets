import {createHash} from 'node:crypto';
import {receiptIdOf} from './archive.mjs';
import {applyEvent, applySummary, runIdOf} from './lineage.mjs';

const EVENT_START = '/events?offset=0';

export async function collect(client) {
  const exchangeIndex = client.exchanges.length;
  const document = await client('/run-summary');
  if (!document || typeof document !== 'object') throw new Error('最终摘要结构无效');
  const exchange = client.exchanges[exchangeIndex];
  return [{
    id: runIdOf(document.job, document.runId),
    kind: 'run-summary',
    runId: document.runId,
    job: document.job,
    outputs: Array.isArray(document.outputs) ? document.outputs : [],
    source: '/run-summary',
    receiptId: exchange ? receiptIdOf(exchange) : null,
    pageUrl: exchange?.url ?? null,
    rawHash: exchange ? createHash('sha256').update(exchange.bytes).digest('hex') : null
  }];
}

function eventTarget(client, path) {
  const target = new URL(path, client.origin);
  if (target.origin !== new URL(client.origin).origin) throw new Error('事件分页地址超出来源范围');
  if (target.pathname !== '/events') throw new Error('事件分页地址不在 /events');
  return target;
}

function pageEnvelope(client, path, index) {
  const target = eventTarget(client, path);
  const exchange = client.exchanges[index];
  if (!exchange || exchange.url !== target.href) throw new Error('事件页缺少原文凭据');
  return {
    receiptId: receiptIdOf(exchange),
    pageUrl: exchange.url,
    rawHash: createHash('sha256').update(exchange.bytes).digest('hex')
  };
}

// Reads from the stored boundary. A null boundary means "scan from the beginning":
// events already accepted are merged idempotently, and missed pages can be picked up.
export async function collectEvents(client, cursor = null) {
  let next = typeof cursor?.next === 'string' && cursor.next ? cursor.next : EVENT_START;
  const visited = new Set();
  const envelopes = [];
  const pages = [];
  const errors = [];
  const seenEvents = new Set();
  let boundary = null;

  while (next !== null) {
    if (visited.has(next)) {
      errors.push({url: next, message: '事件分页出现循环，停止续读'});
      boundary = next;
      break;
    }
    visited.add(next);
    const exchangeIndex = client.exchanges.length;
    let page;
    try {
      page = await client(next);
    } catch (error) {
      errors.push({url: next, message: error.message});
      boundary = next;
      break;
    }
    if (!page || typeof page !== 'object' || !Array.isArray(page.events)) {
      errors.push({url: next, message: '事件页结构无效，未接纳该页'});
      boundary = next;
      break;
    }
    if (page.next !== undefined && page.next !== null && typeof page.next !== 'string') {
      errors.push({url: next, message: '事件页 next 无效，未接纳该页'});
      boundary = next;
      break;
    }
    let evidence;
    try {
      evidence = pageEnvelope(client, next, exchangeIndex);
    } catch (error) {
      errors.push({url: next, message: error.message});
      boundary = next;
      break;
    }
    for (const event of page.events) {
      const id = event?.eventId;
      if (typeof id === 'string' && seenEvents.has(id)) continue;
      if (typeof id === 'string') seenEvents.add(id);
      envelopes.push({event, ...evidence});
    }
    pages.push({url: evidence.pageUrl, next: page.next ?? null, count: page.events.length, rawHash: evidence.rawHash, receiptId: evidence.receiptId});
    next = page.next ?? null;
  }

  const cursorPage = pages.at(-1)?.url ?? null;
  return {
    envelopes,
    pages,
    errors,
    cursor: {
      next: boundary,
      pages: pages.length,
      lastPage: cursorPage,
      pendingPages: Boolean(boundary)
    }
  };
}

export function receive(client, archive, {cursor = archive.cursor()} = {}) {
  return (async () => {
    const errors = [];
    let records = [];
    try {
      records = await collect(client);
    } catch (error) {
      if (error.status !== 404) errors.push({source: '/run-summary', message: error.message});
    }
    let batch;
    try {
      batch = await collectEvents(client, cursor);
    } catch (error) {
      errors.push({source: '/events', message: error.message});
      batch = {envelopes: [], pages: [], errors: [], cursor: {...cursor, next: cursor?.next ?? EVENT_START}};
    }
    // Only a fully read event pass advances the stored boundary; a failed pass keeps
    // the previous boundary so the missing pages can be retried without manual edits.
    const nextCursor = batch.errors.length || batch.cursor.pendingPages ? {...cursor, next: batch.cursor.next} : batch.cursor;
    errors.push(...batch.errors.map(item => ({source: '/events', ...item})));
    const result = await archive.commit(records, client.exchanges, {events: batch.envelopes, pages: batch.pages, cursor: nextCursor});
    const coverage = archive.coverage();
    return {
      ...result,
      pages: batch.pages,
      errors,
      pendingPages: Boolean(nextCursor.next),
      cursor: archive.cursor(),
      coverage: {totals: coverage.totals, pendingEvidence: coverage.pendingEvidence}
    };
  })();
}

export {applyEvent, applySummary};
