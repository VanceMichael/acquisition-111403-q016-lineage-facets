import {createHash, randomUUID} from 'node:crypto';
import {mkdir, readFile, rename, open} from 'node:fs/promises';
import {join} from 'node:path';
import {synthesizeLineage} from './lineage.mjs';
import {canonicalBytes} from './canonical.mjs';

export const digest = bytes => createHash('sha256').update(bytes).digest('hex');

async function replace(path, bytes) {
  const temporary = path + '.' + randomUUID() + '.tmp';
  const handle = await open(temporary, 'wx', 0o600);
  try { await handle.writeFile(bytes); await handle.sync(); } finally { await handle.close(); }
  await rename(temporary, path);
}

const emptyIndex = () => ({version: 2, records: [], exchanges: [], journal: [], quarantined: [], gaps: []});

function migrate(index) {
  if (index.version === 1) {
    return {
      version: 2,
      records: Array.isArray(index.records) ? index.records : [],
      exchanges: Array.isArray(index.exchanges) ? index.exchanges : [],
      journal: [], quarantined: [], gaps: []
    };
  }
  if (index.version !== 2) throw new Error('归档索引结构无效');
  for (const key of ['records', 'exchanges', 'journal', 'quarantined', 'gaps']) {
    if (!Array.isArray(index[key])) throw new Error('归档索引结构无效');
  }
  return index;
}

export async function openArchive(directory) {
  await mkdir(join(directory, 'objects'), {recursive: true});
  const indexPath = join(directory, 'index.json');
  let index = emptyIndex();
  try { index = migrate(JSON.parse(await readFile(indexPath, 'utf8'))); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }

  // 打开归档时逐份校验：交换原文、已接纳事件原文、隔离事件原文都必须与摘要一致。
  const verifyObject = async hash => {
    if (!/^[a-f0-9]{64}$/.test(hash)) throw new Error('归档摘要无效');
    const bytes = await readFile(join(directory, 'objects', hash));
    if (digest(bytes) !== hash) throw new Error('归档原文校验失败');
  };
  for (const exchange of index.exchanges) await verifyObject(exchange.hash);
  for (const entry of index.journal) await verifyObject(entry.eventHash);
  for (const entry of index.quarantined) await verifyObject(entry.eventHash);

  let queue = Promise.resolve();

  async function persist(next) {
    await replace(indexPath, canonicalBytes(next));
    index = next;
  }

  return {
    records: () => structuredClone(index.records),
    exchanges: () => structuredClone(index.exchanges),
    journal: () => structuredClone(index.journal),
    quarantined: () => structuredClone(index.quarantined),
    gaps: () => structuredClone(index.gaps),
    raw: async hash => {
      if (!/^[a-f0-9]{64}$/.test(hash)) throw new Error('原文摘要无效');
      const bytes = await readFile(join(directory, 'objects', hash));
      if (digest(bytes) !== hash) throw new Error('原文校验失败');
      return bytes;
    },
    // 由只追加日志即时合成可查询血缘；每次查询都可追溯到具体事件凭据。
    lineage: () => synthesizeLineage({
      journal: structuredClone(index.journal),
      quarantined: structuredClone(index.quarantined),
      gaps: structuredClone(index.gaps)
    }),
    // 旧边界：登记已整理摘要资料（按标识+内容摘要追加版本，不覆盖历史）。
    commit(records, exchanges) {
      const operation = queue.then(async () => {
        const next = structuredClone(index);
        for (const exchange of exchanges) {
          const hash = digest(exchange.bytes);
          await replace(join(directory, 'objects', hash), exchange.bytes);
          const receiptId = digest(Buffer.from(exchange.url + ':' + exchange.status + ':' + hash));
          if (!next.exchanges.some(existing => existing.receiptId === receiptId)) {
            next.exchanges.push({receiptId, url: exchange.url, status: exchange.status, contentType: exchange.contentType, hash});
          }
        }
        for (const record of records) {
          if (typeof record.id !== 'string' || !record.id) throw new Error('资料必须有非空标识');
          const revision = digest(Buffer.from(JSON.stringify(record)));
          if (!next.records.some(existing => existing.id === record.id && existing.revision === revision)) {
            next.records.push({...record, revision});
          }
        }
        await persist(next);
        return {records: next.records.length, exchanges: next.exchanges.length};
      });
      queue = operation.catch(() => {});
      return operation;
    },
    // 事件接纳：原文留证、只追加、幂等；同 eventId 不同内容进隔离区而不是覆盖。
    admit({admissions, gaps, exchanges}) {
      const operation = queue.then(async () => {
        const next = structuredClone(index);
        for (const exchange of exchanges) {
          const hash = digest(exchange.bytes);
          await replace(join(directory, 'objects', hash), exchange.bytes);
          const receiptId = digest(Buffer.from(exchange.url + ':' + exchange.status + ':' + hash));
          if (!next.exchanges.some(existing => existing.receiptId === receiptId)) {
            next.exchanges.push({receiptId, url: exchange.url, status: exchange.status, contentType: exchange.contentType, hash});
          }
        }
        const result = {admitted: 0, duplicate: 0, quarantined: 0, conflicts: 0, gaps: 0};
        const knownReceipts = new Set([
          ...next.journal.map(entry => entry.receiptId),
          ...next.quarantined.map(entry => entry.receiptId)
        ]);
        const knownEventIds = new Map(next.journal.map(entry => [entry.eventId, entry.receiptId]));
        for (const admission of admissions) {
          if (!admission.payload || typeof admission.eventHash !== 'string') throw new Error('接纳事件缺少可留证载荷或摘要');
          await replace(join(directory, 'objects', admission.eventHash), Buffer.from(JSON.stringify(admission.payload)));
          if (knownReceipts.has(admission.receiptId)) { result.duplicate += 1; continue; }
          if (admission.quarantined) {
            next.quarantined.push({
              receiptId: admission.receiptId, eventHash: admission.eventHash, sourceUrl: admission.sourceUrl,
              reason: admission.reason, hints: admission.hints ?? {},
              eventId: admission.hints?.eventId ?? null,
              eventType: admission.hints?.eventType ?? null,
              runId: admission.hints?.runId ?? null,
              admittedAt: new Date().toISOString()
            });
            knownReceipts.add(admission.receiptId);
            result.quarantined += 1;
            continue;
          }
          // 同一 eventId 但内容不同：证据冲突，隔离保留，绝不覆盖既有事实。
          const prior = knownEventIds.get(admission.eventId);
          if (prior && prior !== admission.receiptId) {
            next.quarantined.push({
              receiptId: admission.receiptId, eventHash: admission.eventHash, sourceUrl: admission.sourceUrl,
              reason: `eventId ${admission.eventId} 与已接纳事件 ${prior} 内容不一致`,
              hints: {eventId: admission.eventId, eventType: admission.eventType, runId: admission.runId, eventTime: admission.eventTime},
              eventId: admission.eventId, eventType: admission.eventType, runId: admission.runId,
              admittedAt: new Date().toISOString()
            });
            knownReceipts.add(admission.receiptId);
            result.quarantined += 1;
            result.conflicts += 1;
            continue;
          }
          next.journal.push({
            receiptId: admission.receiptId,
            eventHash: admission.eventHash,
            sourceUrl: admission.sourceUrl,
            eventId: admission.eventId,
            eventType: admission.eventType,
            eventTime: admission.eventTime,
            runId: admission.runId,
            admittedAt: new Date().toISOString(),
            payload: admission.payload
          });
          knownReceipts.add(admission.receiptId);
          knownEventIds.set(admission.eventId, admission.receiptId);
          result.admitted += 1;
        }
        // 缺口管理：尾指针在流读尽时关闭；历史失败页作为已发生证据保留。
        const incomingTail = gaps.find(gap => gap.kind === 'tail');
        if (!incomingTail) {
          for (const gap of next.gaps) if (gap.kind === 'tail' && !gap.clearedAt) gap.clearedAt = new Date().toISOString();
        } else {
          const openTail = next.gaps.find(existing => existing.kind === 'tail' && !existing.clearedAt);
          if (openTail) {
            openTail.url = incomingTail.url;
            openTail.attemptedAt = incomingTail.attemptedAt;
          } else {
            next.gaps.push({...incomingTail, signature: digest(Buffer.from(`tail:${incomingTail.url}`))});
          }
        }
        for (const gap of gaps) {
          if (gap.kind === 'tail') continue;
          const signature = digest(Buffer.from(`${gap.kind}:${gap.url}:${gap.status ?? ''}:${gap.message ?? ''}`));
          if (!next.gaps.some(existing => existing.signature === signature)) {
            next.gaps.push({...gap, signature});
            result.gaps += 1;
          }
        }
        await persist(next);
        return {...result, totals: {journal: next.journal.length, quarantined: next.quarantined.length, gaps: next.gaps.length, exchanges: next.exchanges.length}};
      });
      queue = operation.catch(() => {});
      return operation;
    }
  };
}
