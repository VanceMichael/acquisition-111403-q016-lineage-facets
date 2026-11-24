import {digest} from './archive.mjs';
import {validateEvent} from './lineage.mjs';

const MAX_PAGES = 100;

// 从来源事件流分页收取通知。每一页先校验再接纳；中途失败时保留已收部分，
// 由调用方入库并把 next 存为游标，下次从断点继续补收。
export async function collectEvents(client, startPath) {
  const accepts = [];
  let path = startPath;
  let pages = 0;
  while (path) {
    if (++pages > MAX_PAGES) return {accepts, next: path, complete: false, error: '事件分页超过上限'};
    let page;
    try {
      page = await client(path);
    } catch (error) {
      return {accepts, next: path, complete: false, error: error.message};
    }
    if (page === null || typeof page !== 'object' || Array.isArray(page) || !Array.isArray(page.events)) {
      return {accepts, next: path, complete: false, error: '事件页结构无效'};
    }
    try {
      for (const event of page.events) validateEvent(event);
    } catch (error) {
      return {accepts, next: path, complete: false, error: error.message};
    }
    const exchange = client.exchanges[client.exchanges.length - 1];
    const hash = digest(exchange.bytes);
    for (const event of page.events) accepts.push({hash, event});
    path = typeof page.next === 'string' && page.next ? page.next : null;
  }
  return {accepts, next: null, complete: true};
}
