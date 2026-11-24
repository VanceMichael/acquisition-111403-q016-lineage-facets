import test from 'node:test';
import assert from 'node:assert/strict';
import {createState, applyEvent, toView, computeGaps, validateEvent} from '../src/lineage.mjs';

const CURSOR_DONE = {next: null, passComplete: true};

function event(overrides = {}) {
  return {
    eventId: 'e1',
    eventTime: 10,
    eventType: 'START',
    run: {runId: 'run1'},
    job: {namespace: 'local', name: 'prepare'},
    ...overrides
  };
}

test('作业、运行、数据集是三种身份，同名也不共用', () => {
  const state = createState();
  applyEvent(state, event({inputs: [{namespace: 'local', name: 'prepare'}]}));
  const view = toView(state);
  assert.equal(view.jobs.length, 1);
  assert.equal(view.runs.length, 1);
  assert.equal(view.datasets.length, 1);
  assert.equal(view.jobs[0].key, 'job:local/prepare');
  assert.equal(view.runs[0].key, 'run:run1');
  assert.equal(view.datasets[0].key, 'dataset:local/prepare');
  assert.notEqual(view.jobs[0].key, view.datasets[0].key);
});

test('静态资料分面与本次输入输出分面互不覆盖', () => {
  const state = createState();
  applyEvent(state, event({
    inputs: [{
      namespace: 'local', name: 'raw',
      facets: {schema: {producer: 'p1', schemaURL: 'urn:schema:v1', fields: ['id']}},
      inputFacets: {schema: {producer: 'p1', schemaURL: 'urn:schema:v1', fields: ['id', 'ts']}, stats: {producer: 'p1', schemaURL: 'urn:stats:v1', rows: 9}}
    }]
  }));
  const view = toView(state);
  const dataset = view.datasets.find(item => item.name === 'raw');
  assert.deepEqual(dataset.facets.schema.variants[0].fields, {fields: ['id']});
  assert.equal(dataset.facets.stats, undefined);
  const edge = view.runs[0].inputs.find(item => item.dataset === dataset.key);
  assert.deepEqual(edge.facets.schema.variants[0].fields, {fields: ['id', 'ts']});
  assert.deepEqual(edge.facets.stats.variants[0].fields, {rows: 9});
});

test('同名分面结构声明不同保留冲突，不盲合并', () => {
  const state = createState();
  applyEvent(state, event({outputs: [{namespace: 'local', name: 'clean', outputFacets: {rows: {producer: 'p1', schemaURL: 'urn:rows:v1', count: 12}}}]}));
  applyEvent(state, event({eventId: 'e2', eventTime: 20, eventType: 'COMPLETE', outputs: [{namespace: 'local', name: 'clean', outputFacets: {rows: {producer: 'p1', schemaURL: 'urn:rows:v2', count: 12, unit: 'row'}}}]}));
  const view = toView(state);
  const edge = view.runs[0].outputs[0];
  assert.equal(edge.facets.rows.conflict, true);
  assert.equal(edge.facets.rows.variants.length, 2);
  assert.deepEqual(edge.facets.rows.variants.map(variant => variant.schemaURL), ['urn:rows:v1', 'urn:rows:v2']);
  assert.deepEqual(edge.facets.rows.variants[1].fields, {count: 12, unit: 'row'});
  const gaps = computeGaps(state, CURSOR_DONE);
  assert.ok(gaps.some(gap => gap.kind === '分面结构冲突' && gap.facet === 'rows'));
});

test('某次没发送的字段保留旧值，不按到达顺序被清空', () => {
  const state = createState();
  applyEvent(state, event({outputs: [{namespace: 'local', name: 'clean', outputFacets: {rows: {producer: 'p1', schemaURL: 'urn:rows:v1', count: 12, bytes: 4096}}}]}));
  applyEvent(state, event({eventId: 'e2', eventTime: 20, eventType: 'RUNNING', outputs: [{namespace: 'local', name: 'clean', outputFacets: {rows: {producer: 'p1', schemaURL: 'urn:rows:v1', count: 13}}}]}));
  const view = toView(state);
  assert.deepEqual(view.runs[0].outputs[0].facets.rows.variants[0].fields, {count: 13, bytes: 4096});
});

test('字段冲突以 eventTime 为准，不按到达顺序', () => {
  const state = createState();
  applyEvent(state, event({eventTime: 20, outputs: [{namespace: 'local', name: 'clean', outputFacets: {rows: {producer: 'p1', schemaURL: 'urn:rows:v1', count: 99}}}]}));
  applyEvent(state, event({eventId: 'e0', eventTime: 10, outputs: [{namespace: 'local', name: 'clean', outputFacets: {rows: {producer: 'p1', schemaURL: 'urn:rows:v1', count: 1}}}]}));
  const view = toView(state);
  assert.deepEqual(view.runs[0].outputs[0].facets.rows.variants[0].fields, {count: 99});
});

test('迟到的开始事件不能让终态倒退', () => {
  const state = createState();
  applyEvent(state, event({eventType: 'COMPLETE', eventTime: 20}));
  applyEvent(state, event({eventId: 'e0', eventType: 'START', eventTime: 10}));
  const view = toView(state);
  assert.equal(view.runs[0].state, 'COMPLETE');
  assert.equal(view.runs[0].terminal, true);
  assert.equal(view.runs[0].startedAt, 10);
  assert.ok(view.runs[0].notes.some(note => note.kind === '迟到事件'));
});

test('失败重跑不抹去上次真实输出', () => {
  const state = createState();
  applyEvent(state, event({eventType: 'COMPLETE', eventTime: 20, outputs: [{namespace: 'local', name: 'clean', outputFacets: {rows: {producer: 'p1', schemaURL: 'urn:rows:v1', count: 12}}}]}));
  applyEvent(state, event({eventId: 'e3', eventTime: 30, eventType: 'FAIL', run: {runId: 'run2'}, outputs: [{namespace: 'local', name: 'clean', outputFacets: {rows: {producer: 'p1', schemaURL: 'urn:rows:v1', count: 3}}}]}));
  const view = toView(state);
  const dataset = view.datasets.find(item => item.name === 'clean');
  assert.equal(dataset.latestCompletedOutput.run, 'run1');
  assert.deepEqual(dataset.latestCompletedOutput.facets.rows.variants[0].fields, {count: 12});
  assert.deepEqual(dataset.producedBy.map(item => [item.run, item.state]), [['run1', 'COMPLETE'], ['run2', 'FAIL']]);
  const failed = view.runs.find(item => item.runId === 'run2');
  assert.deepEqual(failed.outputs[0].facets.rows.variants[0].fields, {count: 3});
});

test('缺终态与缺开始都会向使用者报告', () => {
  const state = createState();
  applyEvent(state, event());
  applyEvent(state, event({eventId: 'e2', eventTime: 20, eventType: 'COMPLETE', run: {runId: 'run2'}}));
  const gaps = computeGaps(state, CURSOR_DONE);
  assert.ok(gaps.some(gap => gap.kind === '缺终态事件' && gap.run === 'run1'));
  assert.ok(gaps.some(gap => gap.kind === '缺开始事件' && gap.run === 'run2'));
});

test('事件流未收完时报告断点', () => {
  const state = createState();
  const gaps = computeGaps(state, {next: '/events?offset=3', passComplete: false});
  assert.ok(gaps.some(gap => gap.kind === '事件流未收完' && gap.next === '/events?offset=3'));
});

test('不同终态先后到达保留先到的并记录冲突', () => {
  const state = createState();
  applyEvent(state, event({eventType: 'COMPLETE', eventTime: 20}));
  applyEvent(state, event({eventId: 'e2', eventType: 'FAIL', eventTime: 30}));
  const view = toView(state);
  assert.equal(view.runs[0].state, 'COMPLETE');
  assert.ok(view.runs[0].notes.some(note => note.kind === '终态冲突'));
  assert.ok(computeGaps(state, CURSOR_DONE).some(gap => gap.kind === '终态冲突' && gap.run === 'run1'));
});

test('缺终态的运行补到终态后缺口消除，且未重发的输出保留', () => {
  const state = createState();
  applyEvent(state, event());
  applyEvent(state, event({eventId: 'e2', eventTime: 20, eventType: 'RUNNING', outputs: [{namespace: 'local', name: 'clean', outputFacets: {rows: {producer: 'p1', schemaURL: 'urn:rows:v1', count: 5}}}]}));
  assert.ok(computeGaps(state, CURSOR_DONE).some(gap => gap.kind === '缺终态事件' && gap.run === 'run1'));
  applyEvent(state, event({eventId: 'e3', eventTime: 30, eventType: 'COMPLETE'}));
  const view = toView(state);
  assert.equal(view.runs[0].state, 'COMPLETE');
  assert.deepEqual(view.runs[0].outputs[0].facets.rows.variants[0].fields, {count: 5});
  assert.ok(!computeGaps(state, CURSOR_DONE).some(gap => gap.kind === '缺终态事件' && gap.run === 'run1'));
});

test('结构不完整的事件在边界被拒绝', () => {
  assert.throws(() => validateEvent({}), /eventId/);
  assert.throws(() => validateEvent(event({eventType: 'DONE'})), /类型无效/);
  assert.throws(() => validateEvent(event({run: {}})), /run\.runId/);
  assert.throws(() => validateEvent(event({outputs: [{namespace: 'local'}]})), /namespace\/name/);
});
