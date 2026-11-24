import {createHash} from 'node:crypto';
import {mkdir, readFile} from 'node:fs/promises';
import {join} from 'node:path';
import {replace} from './archive.mjs';
import {createState, applyEvent, toView, computeGaps} from './lineage.mjs';

const JOURNAL_FILE = 'lineage-journal.json';
const PROJECTION_FILE = 'lineage-projection.json';

// 投影可信性的判据：投影声明的高水位前缀必须与真实日志逐条一致。
const prefixDigest = (events, upto) => createHash('sha256').update(JSON.stringify(events.slice(0, upto).map(entry => [entry.seq, entry.event.eventId, entry.hash]))).digest('hex');

function checkJournal(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value) || value.version !== 1 || !Array.isArray(value.events) || value.cursor === null || typeof value.cursor !== 'object') {
    throw new Error('事件日志结构无效');
  }
  for (const entry of value.events) {
    if (entry === null || typeof entry !== 'object' || typeof entry.seq !== 'number' || typeof entry.hash !== 'string' || entry.event === null || typeof entry.event !== 'object' || typeof entry.event.eventId !== 'string') {
      throw new Error('事件日志条目无效');
    }
  }
  return value;
}

// 可恢复的接纳边界：原始事件页入归档后，事件先写入持久日志（lineage-journal.json，
// 含来源分页游标），再投影为可查询血缘（lineage-projection.json，含高水位）。
// 日志是事实来源，投影是派生数据：打开时高水位之后的日志条目会被重放，
// 投影缺失或损坏时从日志整体重建。
export async function openJournal(directory) {
  await mkdir(directory, {recursive: true});
  const journalPath = join(directory, JOURNAL_FILE);
  const projectionPath = join(directory, PROJECTION_FILE);
  let journal = {version: 1, cursor: {next: null, passComplete: false}, events: []};
  try {
    journal = checkJournal(JSON.parse(await readFile(journalPath, 'utf8')));
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }
  let state = createState();
  let highWater = 0;
  try {
    const projection = JSON.parse(await readFile(projectionPath, 'utf8'));
    if (projection !== null && typeof projection === 'object' && projection.version === 1 && typeof projection.highWater === 'number' && projection.highWater <= journal.events.length && projection.state !== null && typeof projection.state === 'object' && projection.journalDigest === prefixDigest(journal.events, projection.highWater)) {
      state = projection.state;
      highWater = projection.highWater;
    }
  } catch (error) {
    if (error.code !== 'ENOENT') {
      // 投影损坏不致命：丢弃后从日志重建。
    }
  }
  const persistProjection = () => replace(projectionPath, Buffer.from(JSON.stringify({version: 1, highWater, journalDigest: prefixDigest(journal.events, highWater), state}, null, 2)));
  const replay = async () => {
    const before = highWater;
    for (; highWater < journal.events.length; highWater++) {
      applyEvent(state, journal.events[highWater].event);
    }
    if (highWater !== before) await persistProjection();
  };
  await replay();
  let queue = Promise.resolve();
  return {
    resumePath: () => (!journal.cursor.passComplete && journal.cursor.next) ? journal.cursor.next : '/events?offset=0',
    view: () => toView(state),
    gaps: () => computeGaps(state, journal.cursor),
    sources: () => journal.events.map(entry => ({eventId: entry.event.eventId, eventType: entry.event.eventType, eventTime: entry.event.eventTime, runId: entry.event.run.runId, seq: entry.seq, hash: entry.hash})),
    commit(accepts, cursor) {
      const operation = queue.then(async () => {
        const known = new Set(journal.events.map(entry => entry.event.eventId));
        let accepted = 0;
        let duplicates = 0;
        for (const {hash, event} of accepts) {
          if (known.has(event.eventId)) {
            duplicates++;
            continue;
          }
          known.add(event.eventId);
          journal.events.push({seq: journal.events.length + 1, hash, event});
          accepted++;
        }
        journal.cursor = cursor;
        await replace(journalPath, Buffer.from(JSON.stringify(journal, null, 2)));
        await replay();
        return {accepted, duplicates, journal: journal.events.length, projected: highWater};
      });
      queue = operation.catch(() => {});
      return operation;
    }
  };
}
