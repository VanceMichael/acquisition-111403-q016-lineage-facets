import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {startSource} from '../provider/server.mjs';
import {startIntake} from '../src/app.mjs';

const directory=await mkdtemp(join(tmpdir(),'intake-demo-'));
const source=await startSource();
const intake=await startIntake({origin:source.origin,directory});
const json=async path=>{const response=await fetch(intake.origin+path);const value=await response.json();if(!response.ok)throw new Error(JSON.stringify(value));return value;};
try{
 const receipt=await(await fetch(intake.origin+'/collect',{method:'POST'})).json();
 console.log('本次接收',JSON.stringify(receipt,null,2));
 console.log('结构化资料',JSON.stringify(await json('/records'),null,2));
 console.log('血缘',JSON.stringify(await json('/lineage'),null,2));
 console.log('未到证据',JSON.stringify(await json('/coverage'),null,2));
 console.log('原文凭据',JSON.stringify(await json('/receipts'),null,2));
}finally{await intake.close();await source.close();await rm(directory,{recursive:true,force:true});}
