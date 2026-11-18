import {createServer} from 'node:http';
import {openArchive} from './archive.mjs';
import {createClient} from './source.mjs';
import {collectEvents} from './reader.mjs';

export async function startIntake({origin, directory, port=0}) {
  const archive=await openArchive(directory);
  const server=createServer(async (request,response)=>{
    const send=(value,status=200)=>{response.writeHead(status,{'content-type':'application/json; charset=utf-8'});response.end(JSON.stringify(value));};
    try {
      const url=new URL(request.url,'http://localhost');
      const path=url.pathname;
      if(request.method==='GET'&&path==='/health'){send({status:'运行中'});return;}
      // 兼容旧摘要登记查询。
      if(request.method==='GET'&&path==='/records'){send(archive.records());return;}
      if(request.method==='GET'&&path==='/receipts'){send(archive.exchanges());return;}
      // 可查询血缘：作业/运行/数据集分身份，分面带事件凭据，附待补证据清单。
      if(request.method==='GET'&&path==='/lineage'){send(archive.lineage());return;}
      if(request.method==='GET'&&path==='/pending'){
        const view=archive.lineage();
        send({summary:view.summary, pending:view.pending});return;
      }
      if(request.method==='GET'&&path.startsWith('/raw/')){
        const hash=decodeURIComponent(path.slice('/raw/'.length));
        if(!/^[a-f0-9]{64}$/.test(hash)){send({message:'原文摘要无效'},400);return;}
        try {
          const bytes=await archive.raw(hash);
          response.writeHead(200,{'content-type':'application/json; charset=utf-8'});
          response.end(bytes);
        } catch(error){send({message:error.message},404);}
        return;
      }
      if(request.method==='POST'&&path==='/collect'){
        const client=createClient(origin);
        const {admissions,gaps}=await collectEvents(client);
        const result=await archive.admit({admissions,gaps,exchanges:client.exchanges});
        const view=archive.lineage();
        send({...result, pending:view.pending.length, pendingEvidence:view.pending});
        return;
      }
      send({message:'接口不存在'},404);
    } catch(error){send({message:error.message},502);}
  });
  await new Promise(resolve=>server.listen(port,'0.0.0.0',resolve));
  return {origin:`http://127.0.0.1:${server.address().port}`,archive,close:()=>new Promise(resolve=>server.close(resolve))};
}
