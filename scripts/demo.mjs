import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {startSource} from '../provider/server.mjs';
import {startIntake} from '../src/app.mjs';

const post=async url=>{const response=await fetch(url,{method:'POST'});const body=await response.json();if(!response.ok)throw new Error(JSON.stringify(body));return body;};

const directory=await mkdtemp(join(tmpdir(),'intake-demo-'));
const source=await startSource();
const intake=await startIntake({origin:source.origin,directory});
try{
 console.log('== 第一次接纳（START 迟到、COMPLETE 先到）==');
 console.log(JSON.stringify(await post(intake.origin+'/collect')));

 console.log('\n== 可查询血缘：运行实例与分面证据 ==');
 const first=await(await fetch(intake.origin+'/lineage')).json();
 console.log(JSON.stringify({summary:first.summary,runs:first.runs.map(run=>({runId:run.runId,state:run.state,stateEvidence:run.stateEvidence,edges:run.edges.map(edge=>({direction:edge.direction,datasetId:edge.datasetId,facets:edge.facets.map(group=>group.facet)}))}))},null,2));

 console.log('\n== 同一批事件重放（幂等）==');
 console.log(JSON.stringify(await post(intake.origin+'/collect')));

 console.log('\n== 来源进入 rerun 阶段：失败重跑，保留上次真实输出 ==');
 source.setPhase('rerun');
 console.log(JSON.stringify(await post(intake.origin+'/collect')));

 console.log('\n== 来源进入 schema-conflict 阶段：同名分面结构声明冲突 ==');
 source.setPhase('schema-conflict');
 console.log(JSON.stringify(await post(intake.origin+'/collect')));

 const final=await(await fetch(intake.origin+'/lineage')).json();
 console.log('\n== 最终血缘视图 ==');
 console.log(JSON.stringify({summary:final.summary},null,2));
 console.log('\n== 待补/待裁决证据 ==');
 console.log(JSON.stringify(final.pending,null,2));

 console.log('\n== 原文凭据（可按 hash 经 /raw/:hash 取回）==');
 const receipts=await(await fetch(intake.origin+'/receipts')).json();
 console.log(JSON.stringify(receipts.map(({url,status,hash})=>({url,status,hash})),null,2));
}finally{await intake.close();await source.close();await rm(directory,{recursive:true,force:true});}
