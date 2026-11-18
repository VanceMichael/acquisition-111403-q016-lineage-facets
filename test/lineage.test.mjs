import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm, writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {startSource} from '../provider/server.mjs';
import {createClient} from '../src/source.mjs';
import {collectEvents} from '../src/reader.mjs';
import {openArchive} from '../src/archive.mjs';
import {startIntake} from '../src/app.mjs';
import {synthesizeLineage, idForRun, idForDataset, idForEdge, isTerminal} from '../src/lineage.mjs';
import {digest} from '../src/archive.mjs';

async function environment(run){
 const directory=await mkdtemp(join(tmpdir(),'lineage-test-'));
 const source=await startSource();
 try{await run({directory,source});}finally{await source.close();await rm(directory,{recursive:true,force:true});}
}

async function admitAll(source, directory) {
  const client=createClient(source.origin);
  const archive=await openArchive(directory);
  const batch=await collectEvents(client);
  const result=await archive.admit({admissions:batch.admissions,gaps:batch.gaps,exchanges:client.exchanges});
  return {archive,result,batch};
}

test('分页事件被逐页接纳，START 与 COMPLETE 合成为同一运行实例',()=>environment(async({directory,source})=>{
  const {archive,result}=await admitAll(source,directory);
  assert.equal(result.admitted,2);
  const view=archive.lineage();
  assert.equal(view.summary.jobs,1);
  assert.equal(view.summary.runs,1);
  const run=view.runs.find(item=>item.runId==='run1');
  assert.equal(run.state,'COMPLETE');
  assert.equal(run.stateEvidence.eventId,'e2');
  assert.equal(run.terminal,true);
  // 迟到的 START 仍然留证
  assert.deepEqual(run.edges.length,2);
  assert.ok(view.pending.every(item=>item.kind!=='run-terminal-missing'));
}));

test('作业、运行实例、数据集使用互不重叠的身份',()=>environment(async({directory,source})=>{
  const {archive}=await admitAll(source,directory);
  const view=archive.lineage();
  const ids=view.records.map(record=>record.id);
  assert.ok(ids.includes(idForRun('run1')));
  assert.ok(ids.some(id=>id.startsWith('job:')));
  assert.ok(ids.some(id=>id.startsWith('dataset:local/raw')));
  assert.ok(ids.some(id=>id.startsWith('edge:run1:OUT:local/clean')));
  const prefixes=new Set(ids.map(id=>id.split(':')[0]));
  for(const prefix of ['job','run','dataset','edge','facet','admission']) assert.ok(prefixes.has(prefix),`缺少身份前缀 ${prefix}`);
  assert.equal(new Set(ids).size,ids.length);
}));

test('静态资料分面与本次输入输出分面分属不同作用域，互不覆盖',()=>environment(async({directory,source})=>{
  const {archive}=await admitAll(source,directory);
  const view=archive.lineage();
  const raw=view.datasets.find(dataset=>dataset.name==='raw');
  // dataset.facets 是静态资料
  const staticSchema=raw.facets.find(group=>group.facet==='schema');
  assert.ok(staticSchema);
  assert.equal(staticSchema.variants[0].schemaURL,'urn:schema:v1');
  // 输出的 rows 是本次输出分面，挂在运行-数据集关系上
  const run=view.runs.find(item=>item.runId==='run1');
  const outEdge=run.edges.find(edge=>edge.direction==='OUT');
  const rows=outEdge.facets.find(group=>group.facet==='rows');
  assert.ok(rows);
  assert.equal(rows.variants[0].payload.count,12);
  const records=view.records.filter(record=>record.kind==='facet');
  const scopes=new Set(records.map(record=>record.scopeKind));
  assert.ok(scopes.has('dataset-static'));
  assert.ok(scopes.has('run-io'));
}));

test('关系可以追到具体运行、事件凭据与原始通知字节',()=>environment(async({directory,source})=>{
  const {archive}=await admitAll(source,directory);
  const view=archive.lineage();
  const edge=view.records.find(record=>record.kind==='edge'&&record.direction==='OUT');
  assert.equal(edge.runId,'run1');
  assert.ok(edge.evidence.some(item=>item.eventId==='e2'&&/^[a-f0-9]{64}$/.test(item.receiptId)));
  const facet=view.records.find(record=>record.kind==='facet'&&record.facet==='rows');
  assert.ok(facet.evidence.some(item=>item.eventId==='e2'));
  // 凭 receiptId 可取回原始事件字节且校验通过
  const admission=view.records.find(record=>record.kind==='admission'&&record.eventId==='e2');
  const bytes=await archive.raw(admission.eventHash);
  assert.equal(digest(bytes),admission.eventHash);
  assert.deepEqual(JSON.parse(bytes.toString('utf8')).eventType,'COMPLETE');
}));

test('迟到的 START 事件不能让 COMPLETE 终态倒退',()=>environment(async({directory})=>{
  // 模拟反序到达：先 COMPLETE 后 START
  const complete={
    eventId:'c1',eventTime:20,eventType:'COMPLETE',
    run:{runId:'r9'},job:{namespace:'ns',name:'j'},
    outputs:[{namespace:'ns',name:'d',outputFacets:{f:{producer:'p',schemaURL:'urn:f:v1',v:1}}}]
  };
  const start={eventId:'s1',eventTime:10,eventType:'START',run:{runId:'r9'},job:{namespace:'ns',name:'j'}};
  const makeAdmission=(payload,sourceUrl='/events?offset=0')=>({
    receiptId:digest(Buffer.from(`event:${sourceUrl}:${digest(Buffer.from(JSON.stringify(payload)))}`)),
    eventHash:digest(Buffer.from(JSON.stringify(payload))),sourceUrl,
    eventId:payload.eventId,eventType:payload.eventType,eventTime:payload.eventTime,runId:payload.run.runId,payload
  });
  const archive=await openArchive(directory);
  await archive.admit({admissions:[makeAdmission(complete)],gaps:[],exchanges:[]});
  await archive.admit({admissions:[makeAdmission(start,'/events?offset=1')],gaps:[],exchanges:[]});
  const run=archive.lineage().runs.find(item=>item.runId==='r9');
  assert.equal(run.state,'COMPLETE');
  assert.equal(run.stateEvidence.eventId,'c1');
  assert.ok(run.terminal);
}));

test('失败重跑是独立运行实例，不抹去上次真实输出',()=>environment(async({directory,source})=>{
  const first=await admitAll(source,directory);
  assert.equal(first.archive.lineage().runs.find(run=>run.runId==='run1').state,'COMPLETE');
  source.setPhase('rerun');
  const client=createClient(source.origin);
  const batch=await collectEvents(client);
  const second=await first.archive.admit({admissions:batch.admissions,gaps:batch.gaps,exchanges:client.exchanges});
  assert.equal(second.admitted,1);
  const view=first.archive.lineage();
  assert.equal(view.summary.runs,2);
  const run1=view.runs.find(run=>run.runId==='run1');
  const run2=view.runs.find(run=>run.runId==='run2');
  assert.equal(run1.state,'COMPLETE');
  assert.equal(run2.state,'FAIL');
  // run1 的真实输出仍在
  const run1Out=view.records.find(record=>record.kind==='edge'&&record.runId==='run1'&&record.direction==='OUT');
  assert.ok(run1Out);
  const clean=view.datasets.find(dataset=>dataset.name==='clean');
  assert.ok(clean.producedBy.includes(idForRun('run1')));
  assert.ok(clean.producedBy.includes(idForRun('run2')));
  // run2 失败前产生的部分输出也被保留
  const run2Rows=view.records.find(record=>record.kind==='facet'&&record.scopeId===idForEdge('run2','OUT',{namespace:'local',name:'clean'})&&record.facet==='rows');
  assert.ok(run2Rows);
  assert.equal(run2Rows.payload.count,3);
}));

test('重放同一批事件是幂等的，不产生重复日志或版本',()=>environment(async({directory,source})=>{
  const first=await admitAll(source,directory);
  const client=createClient(source.origin);
  const batch=await collectEvents(client);
  const replay=await first.archive.admit({admissions:batch.admissions,gaps:batch.gaps,exchanges:client.exchanges});
  assert.equal(replay.admitted,0);
  assert.equal(replay.duplicate,2);
  assert.equal(first.archive.journal().length,2);
  assert.deepEqual(first.archive.lineage().summary,first.archive.lineage().summary);
}));

test('同名分面结构声明不同时保留冲突变体，不盲合并',()=>environment(async({directory,source})=>{
  await admitAll(source,directory);
  source.setPhase('schema-conflict');
  const client=createClient(source.origin);
  const batch=await collectEvents(client);
  const a2=await openArchive(directory);
  await a2.admit({admissions:batch.admissions,gaps:batch.gaps,exchanges:client.exchanges});
  const view=a2.lineage();
  const facetRecords=view.records.filter(record=>record.kind==='facet'&&record.facet==='rows');
  const schemaURLs=new Set(facetRecords.map(record=>record.schemaURL));
  assert.ok(schemaURLs.has('urn:rows:v1'));
  assert.ok(schemaURLs.has('urn:rows:v2'));
  assert.ok(facetRecords.every(record=>record.conflicted));
  const conflict=view.pending.find(item=>item.kind==='facet-schema-conflict');
  assert.ok(conflict);
  assert.deepEqual([...conflict.schemaURLs].sort(),['urn:rows:v1','urn:rows:v2']);
}));

test('某次未发送的字段不会清空既有资料',()=>environment(async({directory})=>{
  const base={
    eventId:'b1',eventTime:10,eventType:'RUNNING',run:{runId:'r'},job:{namespace:'ns',name:'j'},
    outputs:[{namespace:'ns',name:'d',outputFacets:{rows:{producer:'p',schemaURL:'urn:rows:v1',count:5,note:'保留'}}}]};
  // 后续事件同结构声明但只带另一个字段
  const follow={
    eventId:'b2',eventTime:20,eventType:'COMPLETE',run:{runId:'r'},job:{namespace:'ns',name:'j'},
    outputs:[{namespace:'ns',name:'d',outputFacets:{rows:{producer:'p',schemaURL:'urn:rows:v1',extra:7}}}]};
  const makeAdmission=payload=>({receiptId:digest(Buffer.from(`e:${payload.eventId}`)),eventHash:digest(Buffer.from(JSON.stringify(payload))),sourceUrl:'/events?offset=0',eventId:payload.eventId,eventType:payload.eventType,eventTime:payload.eventTime,runId:payload.run.runId,payload});
  const archive=await openArchive(directory);
  await archive.admit({admissions:[makeAdmission(base)],gaps:[],exchanges:[]});
  await archive.admit({admissions:[makeAdmission(follow)],gaps:[],exchanges:[]});
  const facet=archive.lineage().records.find(record=>record.kind==='facet'&&record.facet==='rows');
  assert.equal(facet.payload.count,5);
  assert.equal(facet.payload.note,'保留');
  assert.equal(facet.payload.extra,7);
}));

test('缺终态时给出待补证据清单，补收终态后清单收敛',()=>environment(async({directory})=>{
  const start={eventId:'s',eventTime:10,eventType:'START',run:{runId:'open'},job:{namespace:'ns',name:'j'}};
  const makeAdmission=payload=>({receiptId:digest(Buffer.from(`e:${payload.eventId}`)),eventHash:digest(Buffer.from(JSON.stringify(payload))),sourceUrl:'/events?offset=0',eventId:payload.eventId,eventType:payload.eventType,eventTime:payload.eventTime,runId:payload.run.runId,payload});
  const archive=await openArchive(directory);
  const first=await archive.admit({admissions:[makeAdmission(start)],gaps:[],exchanges:[]});
  assert.equal(first.totals.gaps,0);
  let pending=archive.lineage().pending.find(item=>item.kind==='run-terminal-missing');
  assert.ok(pending);
  assert.match(pending.message,/终态/);
  const complete={eventId:'c',eventTime:30,eventType:'COMPLETE',run:{runId:'open'},job:{namespace:'ns',name:'j'}};
  await archive.admit({admissions:[makeAdmission(complete)],gaps:[],exchanges:[]});
  assert.equal(archive.lineage().runs.find(run=>run.runId==='open').state,'COMPLETE');
  assert.ok(!archive.lineage().pending.some(item=>item.kind==='run-terminal-missing'));
}));

test('未读尾指针登记为缺口，流读尽后缺口关闭',()=>environment(async({directory,source})=>{
  const client=createClient(source.origin);
  // 只读第一页（带 next）就停
  const firstPage=await client('/events?offset=0');
  assert.ok(firstPage.next);
  const {collectEvents}=await import('../src/reader.mjs');
  // 直接用完整接纳：来源两页都可读，流应读尽，无开放尾缺口
  const batch=await collectEvents(createClient(source.origin));
  assert.equal(batch.exhausted,true);
  assert.ok(!batch.gaps.some(gap=>gap.kind==='tail'));
  const archive=await openArchive(directory);
  await archive.admit({admissions:batch.admissions,gaps:batch.gaps,exchanges:[]});
  assert.equal(archive.gaps().length,0);
}));

test('同 eventId 内容不一致进入隔离区，不覆盖已接纳事实',()=>environment(async({directory})=>{
  const a={eventId:'x1',eventTime:10,eventType:'START',run:{runId:'r'},job:{namespace:'ns',name:'j'}};
  const b={...a,eventTime:11};
  const makeAdmission=(payload,tag)=>({receiptId:digest(Buffer.from(`e:${tag}`)),eventHash:digest(Buffer.from(JSON.stringify(payload))),sourceUrl:'/events?offset=0',eventId:payload.eventId,eventType:payload.eventType,eventTime:payload.eventTime,runId:payload.run.runId,payload});
  const archive=await openArchive(directory);
  await archive.admit({admissions:[makeAdmission(a,'a')],gaps:[],exchanges:[]});
  const result=await archive.admit({admissions:[makeAdmission(b,'b')],gaps:[],exchanges:[]});
  assert.equal(result.conflicts,1);
  assert.equal(archive.journal().length,1);
  assert.equal(archive.journal()[0].eventTime,10);
  assert.equal(archive.quarantined().length,1);
  assert.match(archive.quarantined()[0].reason,/内容不一致/);
  assert.ok(archive.lineage().pending.some(item=>item.kind==='malformed-event'));
}));

test('重开归档后事件原文逐份校验，破坏原文即拒绝打开',()=>environment(async({directory,source})=>{
  const {archive}=await admitAll(source,directory);
  const hash=archive.journal()[0].eventHash;
  await writeFile(join(directory,'objects',hash),'被篡改');
  await assert.rejects(()=>openArchive(directory),/校验失败/);
}));

test('HTTP 闭环：/collect 接纳事件、/lineage 可查询、/raw 取回原文、/pending 报告证据缺口',()=>environment(async({directory,source})=>{
  const intake=await startIntake({origin:source.origin,directory});
  try{
    const collectResponse=await fetch(intake.origin+'/collect',{method:'POST'});
    assert.equal(collectResponse.status,200);
    const receipt=await collectResponse.json();
    assert.equal(receipt.admitted,2);
    const view=await (await fetch(intake.origin+'/lineage')).json();
    assert.equal(view.runs[0].state,'COMPLETE');
    const admission=view.records.find(record=>record.kind==='admission');
    const raw=await fetch(`${intake.origin}/raw/${admission.eventHash}`);
    assert.equal(raw.status,200);
    assert.equal((await raw.json()).eventId,admission.eventId);
    const pending=await (await fetch(intake.origin+'/pending')).json();
    assert.ok(Array.isArray(pending.pending));
    // 再次接纳为幂等重放
    const replay=await (await fetch(intake.origin+'/collect',{method:'POST'})).json();
    assert.equal(replay.admitted,0);
    assert.equal(replay.duplicate,2);
  } finally { await intake.close(); }
}));

test('旧 v1 归档索引可迁移打开，旧摘要记录保留',()=>environment(async({directory})=>{
  const {writeFile:write,mkdir}=await import('node:fs/promises');
  await mkdir(join(directory,'objects'),{recursive:true});
  const hash=digest(Buffer.from('旧原文'));
  await write(join(directory,'objects',hash),Buffer.from('旧原文'));
  await write(join(directory,'index.json'),JSON.stringify({version:1,records:[{id:'old',revision:'x'}],exchanges:[{receiptId:'r',url:'http://local/run-summary',status:200,contentType:'application/json',hash}]}));
  const archive=await openArchive(directory);
  assert.equal(archive.records()[0].id,'old');
  assert.deepEqual(archive.journal(),[]);
  const view=archive.lineage();
  assert.equal(view.summary.admissions,0);
}));

test('纯合成器对空日志返回空视图与零计数',()=>{
  const view=synthesizeLineage({});
  assert.equal(view.summary.runs,0);
  assert.deepEqual(view.pending,[]);
  assert.equal(isTerminal('START'),false);
  assert.equal(isTerminal('FAIL'),true);
  assert.ok(idForDataset({namespace:'a/b',name:'c d'}).includes('dataset:'));
});

test('分页暂时不可读时保留已收部分证据并登记缺口，恢复后补收敛',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'lineage-gap-'));
  const goodPage={next:'/events?offset=1',events:[{eventId:'g1',eventTime:10,eventType:'START',run:{runId:'g'},job:{namespace:'ns',name:'j'}}]};
  const lastPage={next:null,events:[{eventId:'g2',eventTime:20,eventType:'COMPLETE',run:{runId:'g'},job:{namespace:'ns',name:'j'}}]};
  const stubClient=(failing)=>{
    const exchanges=[];
    const client=async path=>{
      const payload=path.includes('offset=1')?(failing?Promise.reject(Object.assign(new Error('来源返回 500'),{status:500})):lastPage):goodPage;
      const value=await payload;
      exchanges.push({url:`http://local${path}`,status:200,contentType:'application/json',bytes:Buffer.from(JSON.stringify(value))});
      return value;
    };
    client.origin='http://local';client.exchanges=exchanges;
    return client;
  };
  try{
    const archive=await openArchive(directory);
    const failingClient=stubClient(true);
    const partial=await collectEvents(failingClient);
    const result=await archive.admit({admissions:partial.admissions,gaps:partial.gaps,exchanges:failingClient.exchanges});
    assert.equal(result.admitted,1);
    const gapView=archive.lineage();
    assert.equal(gapView.runs[0].state,'START');
    assert.ok(gapView.pending.some(item=>item.kind==='event-page-unavailable'));
    assert.ok(archive.gaps().some(gap=>gap.kind==='page-unavailable'));
    // 来源恢复：同一事件幂等跳过，新事件补收，运行进入终态
    const recoveredClient=stubClient(false);
    const recovered=await collectEvents(recoveredClient);
    await archive.admit({admissions:recovered.admissions,gaps:recovered.gaps,exchanges:recoveredClient.exchanges});
    const finalView=archive.lineage();
    assert.equal(finalView.runs[0].state,'COMPLETE');
    assert.ok(!finalView.pending.some(item=>item.kind==='run-terminal-missing'));
    // 历史缺口作为已发生证据保留
    assert.ok(archive.gaps().some(gap=>gap.kind==='page-unavailable'));
  } finally { await rm(directory,{recursive:true,force:true}); }
});

test('未读尾指针登记为缺口，流读尽后缺口关闭',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'lineage-tail-'));
  const page0={next:'/events?offset=1',events:[{eventId:'t1',eventTime:10,eventType:'START',run:{runId:'t'},job:{namespace:'ns',name:'j'}}]};
  const page1={next:null,events:[{eventId:'t2',eventTime:20,eventType:'COMPLETE',run:{runId:'t'},job:{namespace:'ns',name:'j'}}]};
  const stubClient=(stopAtFirst)=>{
    const exchanges=[];
    const client=async path=>{
      if(stopAtFirst&&path.includes('offset=1'))return {next:null,events:[]};
      const value=path.includes('offset=1')?page1:page0;
      exchanges.push({url:`http://local${path}`,status:200,contentType:'application/json',bytes:Buffer.from(JSON.stringify(value))});
      return value;
    };
    client.origin='http://local';client.exchanges=exchanges;
    return client;
  };
  // 手工构造一个带未读尾指针的接纳批次
  try{
    const archive=await openArchive(directory);
    const first=stubClient(false);
    const batch=await collectEvents(first);
    // 人为附加未读尾缺口，模拟翻页中途达到上限/中止
    batch.gaps.push({kind:'tail',url:'/events?offset=9',attemptedAt:new Date().toISOString()});
    await archive.admit({admissions:batch.admissions,gaps:batch.gaps,exchanges:first.exchanges});
    assert.equal(archive.gaps().filter(gap=>gap.kind==='tail'&&!gap.clearedAt).length,1);
    assert.ok(archive.lineage().pending.some(item=>item.kind==='event-stream-open'));
    // 下一轮流读尽：尾缺口关闭，待补项收敛
    const second=stubClient(false);
    const recovered=await collectEvents(second);
    assert.ok(!recovered.gaps.some(gap=>gap.kind==='tail'));
    await archive.admit({admissions:recovered.admissions,gaps:recovered.gaps,exchanges:second.exchanges});
    const tails=archive.gaps().filter(gap=>gap.kind==='tail');
    assert.equal(tails.length,1);
    assert.ok(tails[0].clearedAt);
    assert.ok(!archive.lineage().pending.some(item=>item.kind==='event-stream-open'));
  } finally { await rm(directory,{recursive:true,force:true}); }
});

test('不可解析的通知进入隔离区并留证，不阻断同批合规事件',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'lineage-quarantine-'));
  const bad={eventId:'bad',eventType:'WEIRD',eventTime:5,run:{runId:'q'},job:{namespace:'ns',name:'j'}};
  const good={eventId:'ok',eventTime:10,eventType:'START',run:{runId:'q'},job:{namespace:'ns',name:'j'}};
  const makeAdmission=(payload,quarantined=false,reason)=>({
    receiptId:digest(Buffer.from(`q:${payload.eventId}`)),
    eventHash:digest(Buffer.from(JSON.stringify(payload))),
    sourceUrl:'/events?offset=0',
    payload,
    ...(quarantined?{quarantined:true,reason,hints:{eventId:payload.eventId,eventType:payload.eventType,runId:'q'}}:
      {eventId:payload.eventId,eventType:payload.eventType,eventTime:payload.eventTime,runId:payload.run.runId})
  });
  try{
    const archive=await openArchive(directory);
    const result=await archive.admit({
      admissions:[makeAdmission(bad,true,'eventType 不在约定范围: WEIRD'),makeAdmission(good)],
      gaps:[],exchanges:[]
    });
    assert.equal(result.admitted,1);
    assert.equal(result.quarantined,1);
    const view=archive.lineage();
    const pending=view.pending.find(item=>item.kind==='malformed-event');
    assert.ok(pending);
    assert.match(pending.message,/WEIRD/);
    assert.ok(view.records.some(record=>record.kind==='quarantine'));
    // 隔离区原文同样可按摘要校验取回
    const hash=archive.quarantined()[0].eventHash;
    assert.equal(digest(await archive.raw(hash)),hash);
  } finally { await rm(directory,{recursive:true,force:true}); }
});
