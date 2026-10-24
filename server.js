const express=require("express");
const http=require("http");
const WebSocket=require("ws");
const crypto=require("crypto");
const path=require("path");
const app=express(),server=http.createServer(app);
const wss=new WebSocket.Server({server,maxPayload:4096,perMessageDeflate:false});
app.disable("x-powered-by");
app.use(express.static(path.join(__dirname,"public")));
app.get("/health",(_,res)=>res.json({status:"online",service:"Anon Chat Pro"}));
const PORT=process.env.PORT||3000,MAX_USERS=200,MAX_TEXT=500;
const FOUNDER_NAME=(process.env.FOUNDER_NAME||"fundador").trim().toLocaleLowerCase("pt-BR");
const FOUNDER_PASSWORD=process.env.FOUNDER_PASSWORD||"";
const clients=new Map(),lastMessages=new Map();
const send=(ws,obj)=>{if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj))};
function broadcast(obj,exclude=null){const s=JSON.stringify(obj);for(const c of wss.clients)if(c!==exclude&&c.readyState===WebSocket.OPEN)c.send(s)}
function users(){return [...clients.values()].map(u=>({id:u.id,name:u.name,isFounder:u.isFounder}))}
function sys(text){broadcast({type:"system",text,time:Date.now()})}
function equal(a,b){const x=Buffer.from(String(a)),y=Buffer.from(String(b));return x.length===y.length&&crypto.timingSafeEqual(x,y)}
wss.on("connection",ws=>{
 if(clients.size>=MAX_USERS){send(ws,{type:"error",text:"Chat cheio. Tente mais tarde."});return ws.close(1013,"Chat cheio")}
 const id=crypto.randomUUID();let joined=false;
 ws.on("message",(buf,binary)=>{
  if(binary||buf.length>4096)return;let d;try{d=JSON.parse(buf.toString())}catch{return}
  if(!d||typeof d.type!=="string")return;
  if(d.type==="join"){
   if(joined)return;
   const name=String(d.name||"").trim().replace(/\s+/g," ").slice(0,20);
   if(name.length<2||!/^[\p{L}\p{N}_ -]+$/u.test(name))return send(ws,{type:"error",text:"Use apelido de 2 a 20 caracteres."});
   const founderTry=name.toLocaleLowerCase("pt-BR")===FOUNDER_NAME;let isFounder=false;
   if(founderTry){if(!FOUNDER_PASSWORD)return send(ws,{type:"error",text:"Configure FOUNDER_PASSWORD nas variáveis do Render."});if(!equal(d.founderPassword||"",FOUNDER_PASSWORD))return send(ws,{type:"error",text:"Senha do fundador incorreta."});isFounder=true}
   else if(d.founderPassword)return send(ws,{type:"error",text:"Senha inválida para este apelido."});
   if([...clients.values()].some(u=>u.name.toLocaleLowerCase("pt-BR")===name.toLocaleLowerCase("pt-BR")))return send(ws,{type:"error",text:"Esse apelido já está sendo usado."});
   joined=true;clients.set(ws,{id,name,isFounder});send(ws,{type:"joined",id,name,isFounder});broadcast({type:"users",users:users()});sys(`${name} entrou no chat${isFounder?" (fundador)":""}.`);return;
  }
  const me=clients.get(ws);if(!joined||!me)return;
  if(d.type==="kick"){
   if(!me.isFounder)return send(ws,{type:"error",text:"Somente o fundador pode expulsar participantes."});
   const target=String(d.name||"").trim().toLocaleLowerCase("pt-BR");
   const entry=[...clients.entries()].find(([c,u])=>c!==ws&&u.name.toLocaleLowerCase("pt-BR")===target);
   if(!entry)return send(ws,{type:"error",text:"Participante não encontrado."});
   const [victim,v]=entry;if(v.isFounder)return send(ws,{type:"error",text:"A conta do fundador não pode ser expulsa."});
   send(victim,{type:"kicked",text:"Você foi removido do chat pelo fundador."});send(ws,{type:"admin_action",text:`${v.name} foi expulso.`});
   clients.delete(victim);lastMessages.delete(victim);try{victim.close(4001,"Expulso pelo fundador")}catch{}broadcast({type:"users",users:users()});sys(`${v.name} foi removido pelo fundador.`);return;
  }
  if(d.type!=="message")return;
  const now=Date.now();if(now-(lastMessages.get(ws)||0)<800)return send(ws,{type:"error",text:"Aguarde um instante antes de enviar outra mensagem."});
  const text=String(d.text||"").trim();if(!text||text.length>MAX_TEXT)return send(ws,{type:"error",text:"A mensagem deve ter entre 1 e 500 caracteres."});
  lastMessages.set(ws,now);broadcast({type:"message",id,name:me.name,text,time:now,isFounder:me.isFounder});
 });
 ws.on("close",()=>{const u=clients.get(ws);if(!u)return;clients.delete(ws);lastMessages.delete(ws);broadcast({type:"users",users:users()});sys(`${u.name} saiu do chat.`)});
 ws.on("error",()=>{});
});
server.listen(PORT,"0.0.0.0",()=>console.log(`ANON CHAT PRO ativo na porta ${PORT}. Fundador: ${FOUNDER_NAME}. Senha configurada: ${Boolean(FOUNDER_PASSWORD)}`));
