import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {startSource} from '../provider/server.mjs';
import {startIntake} from '../src/app.mjs';

const directory=await mkdtemp(join(tmpdir(),'intake-demo-'));
const source=await startSource();
const intake=await startIntake({origin:source.origin,directory});
const post=async path=>{const response=await fetch(intake.origin+path,{method:'POST'});const value=await response.json();if(!response.ok)throw new Error(JSON.stringify(value));return value;};
const get=async path=>(await fetch(intake.origin+path)).json();
const cleanOf=lineage=>lineage.datasets.find(dataset=>dataset.name==='clean');
try{
 console.log('== 首次事件接收(终态先于开始到达) ==');
 console.log('接收结果',JSON.stringify(await post('/collect-events')));
 let lineage=await get('/lineage');
 console.log('运行实例',JSON.stringify(lineage.runs.map(run=>({runId:run.runId,state:run.state,startedAt:run.startedAt,endedAt:run.endedAt}))));
 console.log('clean 当前有效产出',JSON.stringify(cleanOf(lineage).latestCompletedOutput));
 console.log('缺失证据',JSON.stringify(await get('/gaps')));

 console.log('== 失败重跑(部分输出) ==');
 source.setPhase('rerun');
 console.log('接收结果',JSON.stringify(await post('/collect-events')));
 lineage=await get('/lineage');
 console.log('clean 当前有效产出仍属上次成功运行',JSON.stringify(cleanOf(lineage).latestCompletedOutput.run),cleanOf(lineage).latestCompletedOutput.facets.rows.variants[0].fields);
 console.log('失败运行的部分输出保留为证据',JSON.stringify(lineage.runs.find(run=>run.runId==='run2').outputs[0].facets.rows.variants[0].fields));
 console.log('缺失证据',JSON.stringify(await get('/gaps')));

 console.log('== 单份资料收件(原有能力) ==');
 console.log('本次接收',JSON.stringify(await post('/collect')));
 console.log('结构化资料',JSON.stringify(await get('/records')));
}finally{await intake.close();await source.close();await rm(directory,{recursive:true,force:true});}
