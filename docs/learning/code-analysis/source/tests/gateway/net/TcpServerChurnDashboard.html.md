# tests/gateway/net/TcpServerChurnDashboard.html

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；117行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~html
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ArenaServer · churn 实时监控</title>
<style>
:root{color-scheme:light dark;--bg:light-dark(#f5f7fa,#10151e);--panel:light-dark(#fff,#1a2330);--fg:light-dark(#182739,#e3ebf4);--muted:light-dark(#52657a,#a2b1c4);--line:light-dark(#d9e2ed,#344258);--blue:light-dark(#096bd1,#67b4ff);--green:light-dark(#09734d,#70ddb0);--red:light-dark(#b71931,#ff8796)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px system-ui,-apple-system,"Segoe UI",sans-serif}main{max-width:1350px;margin:auto;padding:26px}h1{font-size:24px;font-weight:600;margin:0 0 8px}h2{font-size:15px;font-weight:600;margin:0 0 12px}p{line-height:1.65;margin:8px 0;color:var(--muted)}header{display:flex;justify-content:space-between;gap:20px;align-items:center;flex-wrap:wrap}.status{font-weight:600;color:var(--blue)}.status.passed{color:var(--green)}.status.failed{color:var(--red)}.toolbar{display:flex;gap:10px;align-items:center}button{font:inherit;color:var(--fg);background:var(--panel);border:1px solid var(--line);padding:7px 12px;border-radius:6px;cursor:pointer}.progress{height:5px;background:var(--line);margin:16px 0 10px;overflow:hidden}.progress i{display:block;height:100%;background:var(--blue);width:0}.meta{display:flex;gap:16px;flex-wrap:wrap;font-variant-numeric:tabular-nums}.numbers{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:12px;margin:22px 0}.number{padding:14px;background:var(--panel);border-radius:8px}.number b{font-size:24px;display:block;margin:8px 0;font-weight:500;font-variant-numeric:tabular-nums}.number small{color:var(--muted)}.plots{display:grid;grid-template-columns:1fr 1fr;gap:16px}.panel{background:var(--panel);border-radius:8px;padding:16px;min-width:0}.chart{width:100%;height:230px;display:block;overflow:visible}.grid{stroke:var(--line);stroke-width:1}.series{stroke:var(--blue);stroke-width:2;fill:none}.axis{fill:var(--muted);font-size:12px}.focus{fill:var(--blue)}.tables{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:16px}table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}th,td{text-align:left;padding:9px 6px;border-bottom:1px solid var(--line)}th{color:var(--muted);font-weight:500}.event{padding:7px 0;border-bottom:1px solid var(--line);font-variant-numeric:tabular-nums}.error{color:var(--red)}#events{max-height:260px;overflow:auto}#error{overflow-wrap:anywhere}.tip{position:fixed;display:none;background:var(--panel);color:var(--fg);padding:8px 12px;border:1px solid var(--line);border-radius:6px;pointer-events:none;font-variant-numeric:tabular-nums;z-index:3}.foot{overflow-wrap:anywhere;margin-top:18px;font-size:12px}@media(max-width:850px){.numbers{grid-template-columns:repeat(3,1fr)}.tables{grid-template-columns:1fr}}@media(max-width:600px){main{padding:14px}.plots{grid-template-columns:1fr}.numbers{grid-template-columns:repeat(2,1fr)}.chart{height:205px}}
</style>
</head>
<body><main>
<header><div><h1>ArenaServer · churn 实时监控</h1><span id="status" class="status">连接监控中…</span></div><div class="toolbar"><span id="updated">—</span><button id="pause" type="button" aria-pressed="false">暂停图表刷新</button></div></header>
<div class="progress"><i id="progress"></i></div>
<div class="meta"><span id="elapsed">—</span><span id="config">—</span><span id="process">—</span><span id="phase">—</span></div>
<div class="numbers">
<div class="number">已验证连接<b id="completed">—</b><small id="round">—</small></div>
<div class="number">服务器 FD<b id="fd">—</b><small id="baseline">—</small></div>
<div class="number">RSS 内存<b id="rss">—</b><small id="rsspeak">—</small></div>
<div class="number">进程 CPU<b id="cpu">—</b><small>100% = 一个 CPU 核</small></div>
<div class="number">Echo P99<b id="echo">—</b><small id="echop50">—</small></div>
<div class="number">错误 / sanitizer<b id="errors">—</b><small id="closes">—</small></div>
</div>
<div class="plots">
<section class="panel"><h2>FD / 时间 · 每轮必须回到基线</h2><svg id="chart-fd" class="chart" role="img" aria-label="服务器 FD 曲线"></svg></section>
<section class="panel"><h2>RSS MiB / 时间</h2><svg id="chart-rss" class="chart" role="img" aria-label="服务器驻留内存曲线"></svg></section>
<section class="panel"><h2>进程 CPU % / 时间</h2><svg id="chart-cpu" class="chart" role="img" aria-label="服务器 CPU 曲线"></svg></section>
<section class="panel"><h2>已验证并关闭连接 / 秒</h2><svg id="chart-rate" class="chart" role="img" aria-label="churn 连接速率曲线"></svg></section>
</div>
<div class="tables">
<section class="panel"><h2>服务器线程 · 当前 CPU</h2><table><thead><tr><th>TID</th><th>名称</th><th>CPU %</th></tr></thead><tbody id="threads"></tbody></table></section>
<section class="panel"><h2>各 epoll · 当前注册数量</h2><table><thead><tr><th>epoll FD</th><th>注册 Channel</th><th>说明</th></tr></thead><tbody id="epolls"></tbody></table><p>base 含 listener / signalfd / eventfd / timerfd；worker 含 eventfd / timerfd 与所属连接。</p></section>
<section class="panel"><h2>最近 10 轮回收 · 实测 FD</h2><table><thead><tr><th>轮次</th><th>耗时 s</th><th>回收 FD</th><th>结果</th></tr></thead><tbody id="rounds"></tbody></table></section>
<section class="panel"><h2>运行事件</h2><div id="events"></div><p id="error" class="error"></p></section>
</div>
<p id="latencies">—</p>
<p>采样 1 秒；80% FIN / 20% RST，每轮校验随机 echo 与 FD 回收。曲线来自 /proc，并非函数级 trace。ASan quarantine 与分配器保留页会影响 RSS；RSS 不回落本身不能判定泄漏。延迟含客户端调度，本次为稳定性验收。</p>
<p class="foot" id="output"></p>
</main><div class="tip" id="tip"></div>
<script>
(function(){
"use strict";
var latest=null, paused=false, inFlight=false;
var labels={starting:"启动中",running:"运行中",passed:"验收通过",failed:"验收失败",paused_by_user:"用户已暂停"};
var phases={startup:"启动",connecting:"建连",echo:"校验 echo / 满载保持",closing:"FIN / RST",fd_recovery:"等待 FD 回收",shutdown:"停止与 sanitizer 检查",finished:"已结束",paused:"已暂停（历史快照）"};
function put(id,value){document.getElementById(id).textContent=value}
function fmt(v,d){return v==null?"—":Number(v).toLocaleString("zh-CN",{maximumFractionDigits:d==null?1:d})}
function time(t){return Math.floor(t/60)+":"+String(Math.floor(t%60)).padStart(2,"0")}
function svgElement(tag,attrs){var e=document.createElementNS("http://www.w3.org/2000/svg",tag);Object.keys(attrs||{}).forEach(function(k){e.setAttribute(k,attrs[k])});return e}
function text(svg,x,y,label,anchor){var e=svgElement("text",{x:x,y:y,"class":"axis","text-anchor":anchor||"end"});e.textContent=label;svg.appendChild(e)}
function draw(id,key,data){
var svg=document.getElementById(id), w=Math.max(280,svg.clientWidth), h=svg.clientHeight||230;
svg.replaceChildren();svg.setAttribute("viewBox","0 0 "+w+" "+h);
if(!data.length)return;
var left=54,right=14,top=14,bottom=30,pw=w-left-right,ph=h-top-bottom;
var maximum=Math.max.apply(null,data.map(function(d){return Number(d[key])||0}));
var ceiling=Math.max(1,maximum*1.1), xmax=Math.max(5,data[data.length-1].t);
function x(v){return left+v/xmax*pw}function y(v){return top+ph-v/ceiling*ph}
for(var i=0;i<4;i++){var n=ceiling*i/3;
svg.appendChild(svgElement("line",{x1:left,x2:w-right,y1:y(n),y2:y(n),"class":"grid"}));
text(svg,left-7,y(n)+4,fmt(n));}
for(var j=0;j<4;j++){var t=xmax*j/3;text(svg,x(t),h-8,time(t),j===0?"start":j===3?"end":"middle");}
var step=Math.max(1,Math.floor(data.length/1200)), points=data.filter(function(_,i){return i%step===0});
if(points[points.length-1]!==data[data.length-1])points.push(data[data.length-1]);
var path=points.map(function(d,i){return(i?"L":"M")+x(d.t).toFixed(2)+","+y(d[key]).toFixed(2)}).join(" ");
svg.appendChild(svgElement("path",{d:path,"class":"series"}));
var hit=svgElement("rect",{x:left,y:top,width:pw,height:ph,fill:"transparent"});
svg.appendChild(hit);
hit.addEventListener("pointermove",function(event){
var bounds=svg.getBoundingClientRect(),position=(event.clientX-bounds.left)/bounds.width*w;
var target=(position-left)/pw*xmax, best=data.reduce(function(a,b){return Math.abs(b.t-target)<Math.abs(a.t-target)?b:a});
var tip=document.getElementById("tip");
tip.textContent=time(best.t)+" · "+fmt(best[key])+" · "+(phases[best.phase]||best.phase);
tip.style.display="block";tip.style.left=Math.min(event.clientX+12,window.innerWidth-235)+"px";tip.style.top=(event.clientY+12)+"px";
});
hit.addEventListener("pointerleave",function(){document.getElementById("tip").style.display="none"});
}
function rows(id,entries){
var root=document.getElementById(id);root.replaceChildren();
entries.forEach(function(values){var row=document.createElement("tr");values.forEach(function(v){var cell=document.createElement("td");cell.textContent=v;row.appendChild(cell)});root.appendChild(row)});
}
function render(d){
latest=d;var m=d.latest||{};
put("status",labels[d.status]||d.status);document.getElementById("status").className="status "+d.status;
put("elapsed",time(d.elapsed_s)+" / "+time(d.duration_s));
put("config",d.workers+" worker · 每轮 "+d.connections_per_round+" 连接");
put("process","PID "+(d.pid||"—")+" · TCP "+(d.port||"—"));
put("phase",phases[d.phase]||d.phase);
document.getElementById("progress").style.width=Math.min(100,d.elapsed_s/d.duration_s*100)+"%";
put("completed",fmt(d.completed,0));put("round","第 "+d.round+" 轮 · 客户端打开 "+d.client_open);
put("fd",fmt(m.fd,0));put("baseline","启动基线 "+fmt(d.baseline_fd,0));
put("rss",fmt(m.rss_mib)+" MiB");put("rsspeak","VmHWM "+fmt(m.rss_peak_mib)+" MiB");
put("cpu",fmt(m.cpu_pct)+"%");put("echo",fmt(d.echo_p99_ms)+" ms");put("echop50","P50 "+fmt(d.echo_p50_ms)+" ms");
put("errors",d.errors+" / "+d.sanitizer_errors);put("closes","FIN "+fmt(d.fin,0)+" · RST "+fmt(d.rst,0));
draw("chart-fd","fd",d.samples);draw("chart-rss","rss_mib",d.samples);draw("chart-cpu","cpu_pct",d.samples);draw("chart-rate","connections_per_s",d.samples);
rows("threads",(m.threads||[]).map(function(t){return[t.tid,t.name,fmt(t.cpu_pct)]}));
rows("epolls",(m.epolls||[]).map(function(e,i){return[e.fd,e.watched,i===0?"base（FD 顺序）":"worker "+i+"（FD 顺序）"]}));
rows("rounds",d.rounds.slice(-10).reverse().map(function(r){return[r.round,r.seconds,r.fd,r.fd===d.baseline_fd?"回到基线":"异常"]}));
var events=document.getElementById("events");events.replaceChildren();
d.events.slice(-14).reverse().forEach(function(e){var row=document.createElement("div");row.className="event"+(e.error?" error":"");row.textContent=time(e.t)+" · "+e.message;events.appendChild(row)});
put("error",d.last_error||"");
put("latencies","建连 P50/P99 "+fmt(d.connect_p50_ms)+"/"+fmt(d.connect_p99_ms)+" ms · 延迟窗口 "+fmt(d.latency_window,0)+" 次 · 校验数据 "+fmt(d.bytes_verified/1048576)+" MiB");
put("output","产物："+d.output+" · metrics.csv / summary.json / report.md / server.log");
put("updated",d.status==="paused_by_user"?"暂停于 "+d.paused_at:"更新 "+new Date().toLocaleTimeString());
}
async function poll(){
if(paused||inFlight)return;inFlight=true;
try{var response=await fetch("/api/state",{cache:"no-store"});if(!response.ok)throw new Error("HTTP "+response.status);render(await response.json())}
catch(error){put("updated","监控连接中断："+error.message)}
finally{inFlight=false}
}
document.getElementById("pause").addEventListener("click",function(){
paused=!paused;this.textContent=paused?"恢复图表刷新":"暂停图表刷新";this.setAttribute("aria-pressed",String(paused));if(!paused)poll();
});
window.addEventListener("resize",function(){if(latest)render(latest)});
poll();setInterval(poll,1000);
})();
</script></body></html>
~~~
