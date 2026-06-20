"""Real-browser round-trip: headless Chromium driving actual EventSource + fetch
(optimized SSE+POST) vs a real WebSocket, same self-echo measurement as rtt.py
but through the browser's own network stack. Validates that the findings hold
with a real client, not just httpx/websockets.

    python -m benchmark.browser_rtt            # default http://127.0.0.1:8000
    python -m benchmark.browser_rtt https://127.0.0.1:8444   # h2
"""
import sys
from playwright.sync_api import sync_playwright

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 200

JS = r"""
async (n) => {
  const sort = a => a.slice().sort((x,y)=>x-y);
  const pct = (a,q) => { a=sort(a); return a[Math.min(a.length-1, Math.floor(q*a.length))]; };

  async function sse() {
    const lat = [];
    await new Promise((done) => {
      const es = new EventSource('/fast/sse/lobby/');
      let token=null, cid=null, started=false, echo=null;
      const maybe = () => { if(token && cid && !started){ started=true; run(); } };
      es.addEventListener('token', e => { token = e.data; maybe(); });
      es.addEventListener('rtc', e => {
        const m = JSON.parse(e.data).rtc;
        if (m.type==='connect'){ cid=m.channel_name; maybe(); }
        else if (m.type==='signal' && echo){ echo(); }
      });
      async function run(){
        for(let i=0;i<n;i++){
          const t=performance.now();
          const got=new Promise(r=>echo=r);
          await fetch('/fast/signal/',{method:'POST',
            headers:{'Content-Type':'application/json','X-Post-Token':token},
            body:JSON.stringify({type:'signal',recipient:cid,sender:cid,signal:{x:1}})});
          await got;
          lat.push(performance.now()-t);
        }
        es.close(); done();
      }
    });
    return {p50:pct(lat,.5), p90:pct(lat,.9), p99:pct(lat,.99)};
  }

  async function wsrun(){
    const lat=[];
    await new Promise((done)=>{
      const proto = location.protocol==='https:' ? 'wss' : 'ws';
      const ws = new WebSocket(proto+'://'+location.host+'/ws/lobby/');
      let cid=null, started=false, echo=null;
      ws.onmessage = ev => {
        const msg = JSON.parse(ev.data);
        if(msg.event!=='rtc') return;
        const m = JSON.parse(msg.data).rtc;
        if(m.type==='connect' && !started){ cid=m.channel_name; started=true; run(); }
        else if(m.type==='signal' && echo){ echo(); }
      };
      async function run(){
        for(let i=0;i<n;i++){
          const t=performance.now();
          const got=new Promise(r=>echo=r);
          ws.send(JSON.stringify({cmd:'signal',rtc:{type:'signal',recipient:cid,sender:cid,signal:{x:1}}}));
          await got;
          lat.push(performance.now()-t);
        }
        ws.close(); done();
      }
    });
    return {p50:pct(lat,.5), p90:pct(lat,.9), p99:pct(lat,.99)};
  }

  return { sse: await sse(), ws: await wsrun() };
}
"""


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox", "--ignore-certificate-errors"])
        ctx = browser.new_context(ignore_https_errors=True)
        page = ctx.new_page()
        # log in (creates the user + session cookie), then sit on /login/ which
        # does NOT auto-open the app's own SSE stream.
        page.goto(BASE + "/login/")
        page.fill("input[name=screen_name]", "Brz")
        page.fill("input[name=name]", "browseruser")
        page.click("input[type=submit]")
        page.wait_for_load_state("networkidle")
        page.goto(BASE + "/login/")
        res = page.evaluate(JS, N)
        proto = page.evaluate("() => performance.getEntriesByType('navigation')[0].nextHopProtocol")
        browser.close()
    print(f"REAL BROWSER @ {BASE}  (nav proto={proto}, n={N})")
    print(f"  SSE+POST  p50={res['sse']['p50']:.2f}  p90={res['sse']['p90']:.2f}  p99={res['sse']['p99']:.2f} ms")
    print(f"  WebSocket p50={res['ws']['p50']:.2f}  p90={res['ws']['p90']:.2f}  p99={res['ws']['p99']:.2f} ms")


if __name__ == "__main__":
    main()
