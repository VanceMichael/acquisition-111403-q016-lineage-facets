import {inspectEvent, normalizeEventTime} from './lineage.mjs';
import {digest} from './archive.mjs';

// 现有边界：只领取来源已整理的最终作业摘要。
export async function collect(client) {
 const document=await client('/run-summary');
 return [{id:document.runId,job:document.job,outputs:document.outputs}];
}

const MAX_PAGES = 100;

function absoluteURL(origin, next) {
  if (typeof next !== 'string' || !next) return null;
  const target = new URL(next, origin);
  if (target.origin !== new URL(origin).origin) throw new Error('事件分页地址超出来源范围');
  return target.pathname + target.search;
}

// 沿 /events 的 next 指针逐页接纳局部事件。
// 分页失败不抹掉已收证据：登记缺口后带着部分结果返回，缺终态/缺分面时可再次补收。
export async function collectEvents(client) {
  const admissions = [];
  const gaps = [];
  const seen = new Set();
  let path = '/events?offset=0';
  let exhausted = false;
  for (let page = 0; page < MAX_PAGES; page += 1) {
    if (seen.has(path)) {
      gaps.push({kind: 'loop', url: path, attemptedAt: new Date().toISOString(), message: '事件分页出现循环，已停止翻页'});
      exhausted = true;
      break;
    }
    seen.add(path);
    let document;
    try {
      document = await client(path);
    } catch (error) {
      gaps.push({
        kind: 'page-unavailable', url: path, attemptedAt: new Date().toISOString(),
        status: error.status ?? null, message: error.message
      });
      break;
    }
    if (!Array.isArray(document.events)) {
      gaps.push({kind: 'page-unavailable', url: path, attemptedAt: new Date().toISOString(), message: '事件页缺少 events 数组'});
      break;
    }
    for (const payload of document.events) {
      const bytes = Buffer.from(JSON.stringify(payload));
      const eventHash = digest(bytes);
      const receiptId = digest(Buffer.from(`event:${path}:${eventHash}`));
      const inspection = inspectEvent(payload);
      if (inspection.ok) {
        admissions.push({
          receiptId, eventHash, sourceUrl: path,
          eventId: payload.eventId, eventType: payload.eventType,
          eventTime: normalizeEventTime(payload.eventTime), runId: payload.run.runId,
          payload
        });
      } else {
        admissions.push({
          receiptId, eventHash, sourceUrl: path, quarantined: true,
          reason: inspection.reason, hints: inspection.hints, payload
        });
      }
    }
    if (document.next === null || document.next === undefined) {
      exhausted = true;
      break;
    }
    try {
      path = absoluteURL(client.origin, document.next);
    } catch (error) {
      gaps.push({kind: 'page-unavailable', url: String(document.next), attemptedAt: new Date().toISOString(), message: error.message});
      break;
    }
  }
  if (!exhausted) {
    gaps.push({kind: 'tail', url: path, attemptedAt: new Date().toISOString()});
  }
  return {admissions, gaps, exhausted};
}
