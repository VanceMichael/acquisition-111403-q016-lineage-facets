export async function collect(client) {
 const document=await client("/run-summary");
 return [{id:document.runId,job:document.job,outputs:document.outputs}];
}
