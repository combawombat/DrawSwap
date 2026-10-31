const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const GRACE_MS = 2 * 60 * 1000; // how long a dropped player has to come back
const page = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(page);
});
const wss = new WebSocketServer({ server, maxPayload: 4 * 1024 * 1024 });

const rooms = new Map();
const send = (ws, o) => {
  if (ws && ws.readyState === 1) { ws.send(JSON.stringify(o)); return true; }
  return false;
};
const clamp = (n, lo, hi, d) => Math.min(hi, Math.max(lo, Math.round(+n) || d));
const newCode = () => {
  let c;
  do c = Array.from({ length: 4 }, () => 'ABCDEFGHJKMNPQRSTUVWXYZ'[Math.random() * 23 | 0]).join('');
  while (rooms.has(c));
  return c;
};
const others = (r, p) => r.players.filter(x => x !== p);

// Deliver finished papers to waiting players. A waiter whose connection is
// down stays in the queue and is served when they resume.
function flush(r) {
  r.waiters = r.waiters.filter(w => {
    const p = r.papers[w.key];
    if (p && p.done >= w.min && send(w.p.ws, { type: 'paper', key: w.key, img: p.img })) return false;
    return true;
  });
}

wss.on('connection', ws => {
  let room = null, me = null;

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'create') {
      const c = newCode();
      me = { ws, slot: 1, token: crypto.randomUUID(), timer: null };
      room = {
        c, players: [me], papers: {}, waiters: [],
        cfg: { duration: clamp(m.duration, 5, 3600, 60), rounds: clamp(m.rounds, 2, 20, 2) }
      };
      rooms.set(c, room);
      send(ws, { type: 'created', code: c, token: me.token });

    } else if (m.type === 'join') {
      const r = rooms.get(String(m.code || '').toUpperCase());
      if (!r) return send(ws, { type: 'error', msg: 'No game with that code.' });
      if (r.players.length >= 2) return send(ws, { type: 'error', msg: 'That game already has two players.' });
      room = r; me = { ws, slot: 2, token: crypto.randomUUID(), timer: null };
      r.players.push(me);
      r.players.forEach(p => send(p.ws, { type: 'start', slot: p.slot, cfg: r.cfg, code: r.c, token: p.token }));

    } else if (m.type === 'resume') {
      const r = rooms.get(String(m.code || '').toUpperCase());
      const p = r && r.players.find(x => x.token === m.token);
      if (!p) return send(ws, { type: 'gone' });
      clearTimeout(p.timer); p.timer = null;
      if (p.ws && p.ws !== ws) { p.ws.removeAllListeners('close'); p.ws.close(); }
      p.ws = ws; room = r; me = p;
      send(ws, { type: 'resumed' });
      others(r, p).forEach(o => send(o.ws, { type: 'back' }));
      flush(r);

    } else if (room && m.type === 'save' && (m.key === 'A' || m.key === 'B') && typeof m.img === 'string') {
      room.papers[m.key] = { img: m.img, done: +m.done };
      flush(room);

    } else if (room && m.type === 'need' && (m.key === 'A' || m.key === 'B')) {
      room.waiters = room.waiters.filter(w => !(w.p === me && w.key === m.key));
      room.waiters.push({ p: me, key: m.key, min: +m.min });
      flush(room);
    }
    // 'ping' needs no handling: receiving it keeps the connection active.
  });

  ws.on('close', () => {
    if (!room || !me || me.ws !== ws) return;
    const r = room, p = me;
    p.ws = null;
    others(r, p).forEach(o => send(o.ws, { type: 'away' }));
    p.timer = setTimeout(() => {
      r.players = r.players.filter(x => x !== p);
      r.waiters = r.waiters.filter(w => w.p !== p);
      r.players.forEach(o => send(o.ws, { type: 'left' }));
      if (!r.players.length) rooms.delete(r.c);
    }, GRACE_MS);
  });
});

server.listen(PORT, () => console.log('Swap Draw running on http://localhost:' + PORT));
