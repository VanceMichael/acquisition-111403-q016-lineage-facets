import {createServer} from 'node:http';
import {openArchive} from './archive.mjs';
import {openJournal} from './journal.mjs';
import {createClient} from './source.mjs';
import {collect} from './reader.mjs';
import {collectEvents} from './events.mjs';

export async function startIntake({origin,directory,port=0}) {
  const archive=await openArchive(directory);
  const journal=await openJournal(directory);
  const server=createServer(async (request,response)=>{
    const send=(value,status=200)=>{response.writeHead(status,{'content-type':'application/json; charset=utf-8'});response.end(JSON.stringify(value));};
    try {
      const path=new URL(request.url,'http://localhost').pathname;
      if(request.method==='GET'&&path==='/health'){send({status:'运行中'});return;}
      if(request.method==='GET'&&path==='/records'){send(archive.records());return;}
      if(request.method==='GET'&&path==='/receipts'){send(archive.exchanges());return;}
      if(request.method==='GET'&&path==='/lineage'){send({...journal.view(),sources:journal.sources()});return;}
      if(request.method==='GET'&&path==='/gaps'){send({gaps:journal.gaps()});return;}
      if(request.method==='POST'&&path==='/collect'){
        const client=createClient(origin);const records=await collect(client);
        send(await archive.commit(records,client.exchanges));return;
      }
      if(request.method==='POST'&&path==='/collect-events'){
        const client=createClient(origin);
        const result=await collectEvents(client,journal.resumePath());
        if(!result.complete&&!result.accepts.length){send({message:`事件接收失败: ${result.error}`,complete:false},502);return;}
        // 接纳边界：原始事件页先入归档，事件再入持久日志，最后投影为可查询血缘。
        await archive.commit([],client.exchanges);
        const summary=await journal.commit(result.accepts,{next:result.next,passComplete:result.complete});
        send({...summary,complete:result.complete,...(result.error?{error:result.error}:{})});return;
      }
      send({message:'接口不存在'},404);
    } catch(error){send({message:error.message},502);}
  });
  await new Promise(resolve=>server.listen(port,'0.0.0.0',resolve));
  return {origin:`http://127.0.0.1:${server.address().port}`,archive,journal,close:()=>new Promise(resolve=>server.close(resolve))};
}
