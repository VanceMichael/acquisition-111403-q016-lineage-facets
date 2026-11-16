import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {startSource} from '../provider/server.mjs';
import {createClient} from '../src/source.mjs';
import {collect,collectEvents,receive} from '../src/reader.mjs';
import {openArchive} from '../src/archive.mjs';
import {startIntake} from '../src/app.mjs';
import {applyEvent,conflictsOf,createLineage} from '../src/lineage.mjs';

async function environment(run){
 const directory=await mkdtemp(join(tmpdir(),'lineage-test-'));const source=await startSource();
 const get=createClient(source.origin);
 try{await run({directory,source,get});}finally{await source.close();await rm(directory,{recursive:true,force:true});}
}

function ingest(archive,envelopes,receipt='r1'){
 const pages=envelopes.map((event,index)=>({event,receiptId:`${receipt}-${index}`,pageUrl:'http://local/events?offset=0',rawHash:`hash-${receipt}-${index}`}));
 return archive.commit([],pages.map(()=>({url:'http://local/events?offset=0',status:200,contentType:'application/json',bytes:Buffer.from('{}')})),{events:pages,cursor:{next:null,pages:1,lastPage:'http://local/events?offset=0'}});
}

const event=(eventId,eventTime,eventType,extra={})=>({eventId,eventTime,eventType,run:{runId:'run1'},job:{namespace:'local',name:'prepare'},...extra});

test('作业、运行与数据集使用各自身份',async()=>{
 const lineage=createLineage();
 applyEvent(lineage,{event:event('e1',10,'START',{inputs:[{namespace:'local',name:'raw'}]}),receiptId:'r',pageUrl:'u',rawHash:'h'});
 const [jobId]=Object.keys(lineage.jobs);const [runId]=Object.keys(lineage.runs);const [datasetId]=Object.keys(lineage.datasets);
 assert.notEqual(jobId,runId);assert.notEqual(runId,datasetId);assert.notEqual(jobId,datasetId);
 assert.match(jobId,/^job:/);assert.match(runId,/^run:/);assert.match(datasetId,/^dataset:/);
 assert.equal(lineage.runs[runId].jobId,jobId);
});

test('静态资料分面与本次输入输出分面互不覆盖',async()=>{
 const lineage=createLineage();
 applyEvent(lineage,{event:event('e1',10,'RUNNING',{inputs:[{namespace:'local',name:'raw',facets:{schema:{producer:'p',schemaURL:'urn:schema:v1',fields:['id']}},inputFacets:{partition:{producer:'p',schemaURL:'urn:part:v1',value:'2024-01'}}}]}),receiptId:'r',pageUrl:'u',rawHash:'h'});
 const dataset=lineage.datasets['dataset:local/raw'];
 const relation=lineage.relations['input:run:local/prepare/run1:dataset:local/raw'];
 // A later event carries no facet at all: absence is not a deletion request.
 applyEvent(lineage,{event:event('e2',20,'COMPLETE',{inputs:[{namespace:'local',name:'raw'}]}),receiptId:'r',pageUrl:'u',rawHash:'h'});
 applyEvent(lineage,{event:event('e3',30,'COMPLETE',{inputs:[{namespace:'local',name:'raw',facets:{schema:{producer:'p',schemaURL:'urn:schema:v1',fields:['id','ts']}}}]}),receiptId:'r',pageUrl:'u',rawHash:'h'});
 assert.deepEqual(dataset.facets.map(facet=>facet.name),['schema']);
 assert.deepEqual(relation.facets.map(facet=>facet.name),['partition']);
 assert.deepEqual(dataset.facets[0].value.fields,['id','ts']);
 assert.equal(relation.facets[0].value.value,'2024-01');
 // The replaced schema revision stays recoverable with the event that carried it.
 assert.deepEqual(dataset.facets[0].observations.map(item=>item.eventId),['e1','e3']);
});

test('同名分面结构不同时分别保留并标记冲突',()=>environment(async({directory,get})=>{
 const archive=await openArchive(directory);await archive.commit(await collect(get),get.exchanges);
 const first=await collectEvents(get);await archive.commit([],[],{events:first.envelopes,cursor:first.cursor});
 await archive.commit([],[],{events:[{event:event('conflict',40,'COMPLETE',{outputs:[{namespace:'local',name:'clean',outputFacets:{rows:{producer:'p1',schemaURL:'urn:rows:v2',count:99}}}]}),receiptId:'r2',pageUrl:'u2',rawHash:'h2'}]});
 const lineage=archive.lineage();
 const relation=lineage.relations.find(item=>item.id==='output:run:local/prepare/run1:dataset:local/clean');
 const versions=relation.facets.filter(facet=>facet.name==='rows').map(facet=>facet.schemaURL).sort();
 assert.deepEqual(versions,['urn:rows:v1','urn:rows:v2']);
 const conflict=lineage.conflicts.find(item=>item.facetName==='rows'&&item.facetClass==='output');
 assert.ok(conflict);assert.deepEqual(conflict.schemaURLs.slice().sort(),['urn:rows:v1','urn:rows:v2']);
}));

test('不同producer的同名同结构分面各自保留',async()=>{
 const lineage=createLineage();
 applyEvent(lineage,{event:event('e1',10,'COMPLETE',{outputs:[{namespace:'local',name:'clean',outputFacets:{rows:{producer:'p1',schemaURL:'urn:rows:v1',count:1}}}]}),receiptId:'r1',pageUrl:'u1',rawHash:'h1'});
 applyEvent(lineage,{event:event('e2',20,'COMPLETE',{outputs:[{namespace:'local',name:'clean',outputFacets:{rows:{producer:'p2',schemaURL:'urn:rows:v1',count:2}}}]}),receiptId:'r2',pageUrl:'u2',rawHash:'h2'});
 const facets=lineage.relations['output:run:local/prepare/run1:dataset:local/clean'].facets;
 assert.deepEqual(facets.map(facet=>[facet.producer,facet.value.count]).sort(),[['p1',1],['p2',2]]);
 assert.equal(conflictsOf(lineage).length,0);
});

test('迟到开始事件不倒退终态，失败重跑不抹去上次输出',async()=>{
 const lineage=createLineage();
 applyEvent(lineage,{event:event('done',30,'COMPLETE',{outputs:[{namespace:'local',name:'clean',outputFacets:{rows:{producer:'p1',schemaURL:'urn:rows:v1',count:12}}}]}),receiptId:'r1',pageUrl:'u1',rawHash:'h1'});
 applyEvent(lineage,{event:event('start',10,'START'),receiptId:'r2',pageUrl:'u2',rawHash:'h2'});
 const run=lineage.runs['run:local/prepare/run1'];
 assert.equal(run.status,'COMPLETE');
 assert.deepEqual(run.eventTypes,['COMPLETE','START']);
 const relation=lineage.relations['output:run:local/prepare/run1:dataset:local/clean'];
 const facet=relation.facets.find(item=>item.schemaURL==='urn:rows:v1');
 assert.equal(facet.value.count,12);
 // A failed retry is a different run instance; it gets its own output evidence
 // and leaves the earlier run's real output alone.
 applyEvent(lineage,{event:{...event('failed',40,'FAIL',{outputs:[{namespace:'local',name:'clean',outputFacets:{rows:{producer:'p1',schemaURL:'urn:rows:v1',count:3}}}]}),run:{runId:'run2'}},receiptId:'r3',pageUrl:'u3',rawHash:'h3'});
 assert.equal(lineage.runs['run:local/prepare/run1'].status,'COMPLETE');
 assert.equal(relation.facets.find(item=>item.schemaURL==='urn:rows:v1').value.count,12);
 assert.equal(lineage.runs['run:local/prepare/run2'].status,'FAIL');
 const retry=lineage.relations['output:run:local/prepare/run2:dataset:local/clean'];
 assert.equal(retry.facets[0].value.count,3);
});

test('原始通知与可查询血缘之间有可恢复的接纳边界',()=>environment(async({directory,get})=>{
 const archive=await openArchive(directory);const first=await collectEvents(get);await archive.commit([],get.exchanges,{events:first.envelopes,cursor:first.cursor});
 assert.equal(archive.cursor().next,null);
 assert.equal(archive.rawEvents().length,2);
 for(const raw of archive.rawEvents()){
  assert.match(raw.rawHash,/^[a-f0-9]{64}$/);
  assert.ok(archive.exchanges().some(exchange=>exchange.hash===raw.rawHash));
 }
 const rows=archive.rawEvents().flatMap(raw=>raw.events);
 const accepted=archive.snapshot().accepted.slice().sort();
 assert.deepEqual(accepted,rows.map(item=>item.eventId).sort());
 const relation=archive.lineage().relations.find(item=>item.kind==='output');
 assert.ok(archive.exchanges().some(exchange=>exchange.receiptId===relation.facets[0].receiptId));
}));

test('分页失败时保留边界并可继续补收',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'boundary-test-'));
 let healthy=false;
 const server=createServer((request,response)=>{
  const url=new URL(request.url,'http://local');
  response.setHeader('content-type','application/json');
  if(url.pathname==='/run-summary'){response.writeHead(404);response.end('{}');return;}
  if(url.searchParams.get('offset')==='0'){
   response.end(JSON.stringify({next:'/events?offset=1',events:[event('e1',10,'START')]}));return;
  }
  if(!healthy){response.writeHead(503);response.end('{"message":"暂时不可用"}');return;}
  response.end(JSON.stringify({next:null,events:[event('e2',20,'COMPLETE')]}));
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const origin=`http://127.0.0.1:${server.address().port}`;
 const archive=await openArchive(directory);
 try{
  const first=await receive(createClient(origin),archive);
  assert.equal(first.events,1);
  assert.equal(first.pendingPages,true);
  assert.equal(first.cursor.next,'/events?offset=1');
  assert.ok(first.errors.length);
  assert.match(first.coverage.pendingEvidence.map(item=>item.code).join(','),/terminal/);
  healthy=true;
  const second=await receive(createClient(origin),archive);
  assert.equal(second.events,1);
  assert.equal(second.duplicates,0);
  assert.equal(second.pendingPages,false);
  assert.equal(archive.lineage().runs[0].status,'COMPLETE');
  assert.equal(archive.lineage().runs[0].summary,null);
 }finally{await new Promise(resolve=>server.close(resolve));await rm(directory,{recursive:true,force:true});}
});

test('缺少终态或分面时向使用者说明还缺哪些证据',()=>environment(async({directory,get})=>{
 const archive=await openArchive(directory);const batch=await collectEvents(get);await archive.commit([],[],{events:batch.envelopes,cursor:batch.cursor});
 const coverage=archive.coverage();
 assert.equal(coverage.totals.runs,1);
 assert.equal(coverage.pendingPages,false);
 const missing=coverage.runs[0].missing.map(item=>item.code).sort();
 assert.deepEqual(missing,['dataset-facets','input-facets','summary']);
 assert.equal(coverage.totals.runsWithMissingEvidence,1);
 assert.ok(coverage.pendingEvidence.length>=3);
}));

test('新建归档可容纳事件批次且重开后血缘一致',()=>environment(async({directory,get})=>{
 const archive=await openArchive(directory);const batch=await collectEvents(get);
 await archive.commit(await collect(get),get.exchanges,{events:batch.envelopes,pages:batch.pages,cursor:batch.cursor});
 const reopened=await openArchive(directory);
 assert.deepEqual(reopened.lineage(),archive.lineage());
 assert.deepEqual(reopened.cursor(),archive.cursor());
 assert.equal(reopened.rawEvents().length,2);
 const summary=reopened.lineage().runs[0].summary;
 assert.match(summary.rawHash,/^[a-f0-9]{64}$/);
 assert.ok(reopened.exchanges().some(exchange=>exchange.receiptId===summary.receiptId));
 assert.match((await reopened.raw(summary.rawHash)).toString('utf8'),/run1/);
}));

test('并发提交不会丢失已接纳事件',()=>environment(async({directory,get})=>{
 const archive=await openArchive(directory);const batch=await collectEvents(get);
 const [first,second]=batch.envelopes;
 await Promise.all([
  archive.commit([],[],{events:[first]}),
  archive.commit([],[],{events:[second]})
 ]);
 assert.deepEqual(archive.snapshot().accepted.sort(),[first.event.eventId,second.event.eventId].sort());
 const reopened=await openArchive(directory);
 assert.equal(reopened.lineage().runs[0].eventIds.length,2);
}));

test('HTTP查询接口暴露血缘、覆盖与原文',()=>environment(async({directory,source})=>{
 const intake=await startIntake({origin:source.origin,directory});
 try{
  const receipt=await(await fetch(intake.origin+'/collect',{method:'POST'})).json();
  assert.equal(receipt.cursor.next,null);
  assert.ok(receipt.coverage.pendingEvidence.length);
  const lineage=await(await fetch(intake.origin+'/lineage')).json();
  assert.ok(lineage.runs.length);assert.ok(lineage.relations.length);
  const coverage=await(await fetch(intake.origin+'/coverage')).json();
  assert.equal(coverage.totals.runs,1);
  const raw=await(await fetch(intake.origin+'/raw?hash='+receipt.pages[0].rawHash)).text();
  assert.match(raw,/eventId/);
 }finally{await intake.close();}
}));
