import {createServer} from 'node:http';
import {openArchive} from './archive.mjs';
import {createClient} from './source.mjs';
import {receive} from './reader.mjs';

export async function startIntake({origin,directory,port=0}) {
  const archive=await openArchive(directory);
  const server=createServer(async (request,response)=>{
    const send=(value,status=200)=>{response.writeHead(status,{'content-type':'application/json; charset=utf-8'});response.end(JSON.stringify(value));};
    try {
      const url=new URL(request.url,'http://localhost');
      const path=url.pathname;
      if(request.method==='GET'&&path==='/health'){send({status:'运行中'});return;}
      if(request.method==='GET'&&path==='/records'){send(archive.records());return;}
      if(request.method==='GET'&&path==='/receipts'){send(archive.exchanges());return;}
      if(request.method==='GET'&&path==='/events'){send(archive.rawEvents());return;}
      if(request.method==='GET'&&path==='/lineage'){send(archive.lineage());return;}
      if(request.method==='GET'&&path==='/coverage'){send(archive.coverage());return;}
      if(request.method==='GET'&&path==='/status'){send(archive.snapshot());return;}
      if(request.method==='GET'&&path==='/raw'){
        const hash=url.searchParams.get('hash')||'';
        try{const bytes=await archive.raw(hash);response.writeHead(200,{'content-type':'application/octet-stream'});response.end(bytes);}
        catch(error){send({message:error.message},error.code==='ENOENT'?404:409);}
        return;
      }
      if(request.method==='POST'&&path==='/collect'){
        const client=createClient(origin);
        const cursor=url.searchParams.get('from')==='start'?null:archive.cursor();
        const result=await receive(client,archive,{cursor});
        send(result);return;
      }
      send({message:'接口不存在'},404);
    } catch(error){send({message:error.message},502);}
  });
  await new Promise(resolve=>server.listen(port,'0.0.0.0',resolve));
  return {origin:`http://127.0.0.1:${server.address().port}`,archive,close:()=>new Promise(resolve=>server.close(resolve))};
}
