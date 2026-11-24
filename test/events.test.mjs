import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm, writeFile, readFile} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {startSource} from '../provider/server.mjs';
import {startIntake} from '../src/app.mjs';
import {openJournal} from '../src/journal.mjs';
import {collectEvents} from '../src/events.mjs';
import {createClient} from '../src/source.mjs';
import {createState} from '../src/lineage.mjs';

async function environment(run) {
  const directory = await mkdtemp(join(tmpdir(), 'events-test-'));
  const source = await startSource();
  const intake = await startIntake({origin: source.origin, directory});
  const post = async path => {
    const response = await fetch(intake.origin + path, {method: 'POST'});
    return {status: response.status, body: await response.json()};
  };
  const get = async path => (await fetch(intake.origin + path)).json();
  try {
    await run({directory, source, intake, post, get});
  } finally {
    await intake.close();
    await source.close();
    await rm(directory, {recursive: true, force: true});
  }
}

test('事件接收与持续合成形成闭环，迟到开始不倒退终态', () => environment(async ({post, get}) => {
  const collected = await post('/collect-events');
  assert.equal(collected.status, 200);
  assert.equal(collected.body.accepted, 2);
  assert.equal(collected.body.complete, true);
  const lineage = await get('/lineage');
  const run = lineage.runs.find(item => item.runId === 'run1');
  assert.equal(run.state, 'COMPLETE');
  assert.equal(run.startedAt, 10);
  assert.equal(run.endedAt, 20);
  assert.ok(run.notes.some(note => note.kind === '迟到事件'));
  const clean = lineage.datasets.find(item => item.name === 'clean');
  assert.equal(clean.latestCompletedOutput.run, 'run1');
  assert.deepEqual(clean.latestCompletedOutput.facets.rows.variants[0].fields, {count: 12});
  assert.deepEqual(clean.producedBy, [{run: 'run1', job: 'job:local/prepare', state: 'COMPLETE', at: 20}]);
  const raw = lineage.datasets.find(item => item.name === 'raw');
  assert.deepEqual(raw.facets.schema.variants[0].fields, {fields: ['id']});
  assert.deepEqual(raw.consumedBy.map(item => item.run), ['run1']);
  const gaps = await get('/gaps');
  assert.deepEqual(gaps.gaps, []);
}));

test('重复接收不会重复接纳事件', () => environment(async ({post}) => {
  await post('/collect-events');
  const again = await post('/collect-events');
  assert.equal(again.body.accepted, 0);
  assert.equal(again.body.duplicates, 2);
  assert.equal(again.body.journal, 2);
}));

test('失败重跑不抹去上次真实输出，缺口向使用者报告', () => environment(async ({source, post, get}) => {
  await post('/collect-events');
  source.setPhase('rerun');
  const collected = await post('/collect-events');
  assert.equal(collected.body.accepted, 1);
  const lineage = await get('/lineage');
  const clean = lineage.datasets.find(item => item.name === 'clean');
  assert.equal(clean.latestCompletedOutput.run, 'run1');
  assert.deepEqual(clean.latestCompletedOutput.facets.rows.variants[0].fields, {count: 12});
  const failed = lineage.runs.find(item => item.runId === 'run2');
  assert.equal(failed.state, 'FAIL');
  assert.deepEqual(failed.outputs[0].facets.rows.variants[0].fields, {count: 3});
  const gaps = await get('/gaps');
  assert.ok(gaps.gaps.some(gap => gap.kind === '缺开始事件' && gap.run === 'run2'));
}));

test('同名分面结构声明不同保留冲突', () => environment(async ({source, post, get}) => {
  await post('/collect-events');
  source.setPhase('schema-conflict');
  await post('/collect-events');
  const lineage = await get('/lineage');
  const run = lineage.runs.find(item => item.runId === 'run1');
  const rows = run.outputs[0].facets.rows;
  assert.equal(rows.conflict, true);
  assert.deepEqual(rows.variants.map(variant => variant.schemaURL), ['urn:rows:v1', 'urn:rows:v2']);
  const gaps = await get('/gaps');
  assert.ok(gaps.gaps.some(gap => gap.kind === '分面结构冲突' && gap.facet === 'rows'));
}));

test('缺终态可以补收，补到后缺口消除且部分输出保留', () => environment(async ({source, post, get}) => {
  source.setPhase('open-run');
  await post('/collect-events');
  let gaps = await get('/gaps');
  assert.ok(gaps.gaps.some(gap => gap.kind === '缺终态事件' && gap.run === 'run3'));
  let lineage = await get('/lineage');
  assert.equal(lineage.runs.find(item => item.runId === 'run3').state, 'RUNNING');
  source.setPhase('open-run-closed');
  await post('/collect-events');
  lineage = await get('/lineage');
  const run = lineage.runs.find(item => item.runId === 'run3');
  assert.equal(run.state, 'COMPLETE');
  assert.deepEqual(run.outputs[0].facets.rows.variants[0].fields, {count: 5});
  const clean = lineage.datasets.find(item => item.name === 'clean');
  assert.equal(clean.latestCompletedOutput.run, 'run3');
  gaps = await get('/gaps');
  assert.ok(!gaps.gaps.some(gap => gap.kind === '缺终态事件' && gap.run === 'run3'));
}));

test('血缘关系能追到具体运行与原始凭据', () => environment(async ({post, get}) => {
  await post('/collect-events');
  const lineage = await get('/lineage');
  const receipts = await get('/receipts');
  const hashes = new Set(receipts.map(receipt => receipt.hash));
  assert.ok(lineage.sources.length > 0);
  for (const source of lineage.sources) assert.ok(hashes.has(source.hash), `事件 ${source.eventId} 的原始页凭据`);
  const run = lineage.runs.find(item => item.runId === 'run1');
  const variantEvents = run.outputs[0].facets.rows.variants[0].events;
  assert.ok(variantEvents.every(eventId => lineage.sources.some(source => source.eventId === eventId)));
}));

test('投影丢失、损坏或滞后时从事件日志恢复', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'journal-test-'));
  const source = await startSource();
  try {
    const client = createClient(source.origin);
    const journal = await openJournal(directory);
    const result = await collectEvents(client, journal.resumePath());
    assert.equal(result.complete, true);
    await journal.commit(result.accepts, {next: null, passComplete: true});
    const before = journal.view();
    assert.ok(before.runs.length > 0);
    // 投影丢失：从日志整体重建。
    await rm(join(directory, 'lineage-projection.json'), {force: true});
    const rebuilt = await openJournal(directory);
    assert.deepEqual(rebuilt.view(), before);
    // 投影损坏（高水位与内容对不上）：摘要不符，从日志整体重建。
    await writeFile(join(directory, 'lineage-projection.json'), JSON.stringify({version: 1, highWater: 1, journalDigest: '0'.repeat(64), state: createState()}));
    const repaired = await openJournal(directory);
    assert.deepEqual(repaired.view(), before);
    // 投影滞后（日志已接纳、投影未落盘）：重放高水位之后的条目。
    const journalPath = join(directory, 'lineage-journal.json');
    const stored = JSON.parse(await readFile(journalPath, 'utf8'));
    stored.events.push({seq: stored.events.length + 1, hash: 'f'.repeat(64), event: {eventId: 'e9', eventTime: 90, eventType: 'COMPLETE', run: {runId: 'run8'}, job: {namespace: 'local', name: 'prepare'}}});
    await writeFile(journalPath, JSON.stringify(stored));
    const replayed = await openJournal(directory);
    const run8 = replayed.view().runs.find(run => run.runId === 'run8');
    assert.equal(run8.state, 'COMPLETE');
    assert.equal(replayed.view().runs.length, before.runs.length + 1);
  } finally {
    await source.close();
    await rm(directory, {recursive: true, force: true});
  }
});

test('事件流中途失败保留已收部分，从断点继续补收', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'journal-test-'));
  try {
    const pages = {
      '/events?offset=0': {next: '/events?offset=1', events: [{eventId: 'p1', eventTime: 1, eventType: 'START', run: {runId: 'run9'}, job: {namespace: 'local', name: 'prepare'}}]},
      '/events?offset=1': {next: null, events: [{eventId: 'p2', eventTime: 2, eventType: 'COMPLETE', run: {runId: 'run9'}, job: {namespace: 'local', name: 'prepare'}}]}
    };
    const stub = broken => {
      const client = async path => {
        if (broken && path === '/events?offset=1') throw new Error('来源返回 502');
        const page = pages[path];
        client.exchanges.push({url: 'http://local' + path, status: 200, contentType: 'application/json', bytes: Buffer.from(JSON.stringify(page))});
        return structuredClone(page);
      };
      client.exchanges = [];
      return client;
    };
    const journal = await openJournal(directory);
    const failed = await collectEvents(stub(true), journal.resumePath());
    assert.equal(failed.complete, false);
    assert.equal(failed.accepts.length, 1);
    await journal.commit(failed.accepts, {next: failed.next, passComplete: false});
    assert.ok(journal.gaps().some(gap => gap.kind === '事件流未收完' && gap.next === '/events?offset=1'));
    assert.ok(journal.gaps().some(gap => gap.kind === '缺终态事件' && gap.run === 'run9'));
    const resumed = await collectEvents(stub(false), journal.resumePath());
    assert.equal(resumed.complete, true);
    await journal.commit(resumed.accepts, {next: resumed.next, passComplete: true});
    assert.equal(journal.view().runs.find(run => run.runId === 'run9').state, 'COMPLETE');
    assert.deepEqual(journal.gaps(), []);
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('来源不可用时事件接收报错且不产生写入', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'events-test-'));
  const source = await startSource();
  const origin = source.origin;
  await source.close();
  const intake = await startIntake({origin, directory});
  try {
    const response = await fetch(intake.origin + '/collect-events', {method: 'POST'});
    assert.equal(response.status, 502);
    const lineage = await (await fetch(intake.origin + '/lineage')).json();
    assert.deepEqual(lineage.runs, []);
    const gaps = await (await fetch(intake.origin + '/gaps')).json();
    assert.ok(gaps.gaps.some(gap => gap.kind === '事件流未收完'));
  } finally {
    await intake.close();
    await rm(directory, {recursive: true, force: true});
  }
});
