import { connect } from 'cloudflare:sockets';

const USER_ID = 'd342d11e-d424-4598-a35b-2267cb486b15';

function uuidToBytes(id) {
  const hex = id.replace(/-/g, '');
  const out = new Uint8Array(16);
  for (let i = 0; i < 16; i++) out[i] = parseInt(hex.substr(i * 2, 2), 16);
  return out;
}

export default {
  async fetch(request) {
    try {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected Upgrade: websocket', { status: 426 });
      }
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      server.accept();
      handleSession(server);
      return new Response(null, { status: 101, webSocket: client });
    } catch (e) {
      return new Response('Error: ' + (e && e.message ? e.message : String(e)), { status: 500 });
    }
  },
};

function handleSession(ws) {
  const uuid = uuidToBytes(USER_ID);
  let remote = null;
  let writer = null;
  let headerDone = false;
  let queue = Promise.resolve();

  const closeAll = () => {
    try { if (writer) writer.releaseLock(); } catch (e) {}
    try { if (remote) remote.close(); } catch (e) {}
    try { ws.close(); } catch (e) {}
  };

  ws.addEventListener('message', (event) => {
    queue = queue.then(async () => {
      try {
        let data = event.data;
        if (typeof data === 'string') return;
        if (!(data instanceof ArrayBuffer)) data = await data.arrayBuffer();
        data = new Uint8Array(data);

        // بعد الترويسة: مباشرة للوجهة
        if (headerDone) {
          if (writer) await writer.write(data);
          return;
        }

        // ترويسة VLESS
        if (data.length < 24 || data[0] !== 0) return closeAll();
        for (let i = 0; i < 16; i++) {
          if (data[1 + i] !== uuid[i]) return closeAll();
        }

        let i = 18 + data[17];       // بعد الـ addons
        const cmd = data[i++];
        if (cmd !== 1) return closeAll(); // TCP فقط
        const port = (data[i] << 8) | data[i + 1];
        i += 2;
        const atype = data[i++];

        let host = '';
        if (atype === 1) {
          host = data[i] + '.' + data[i + 1] + '.' + data[i + 2] + '.' + data[i + 3];
          i += 4;
        } else if (atype === 2) {
          const len = data[i++];
          host = new TextDecoder().decode(data.slice(i, i + len));
          i += len;
        } else if (atype === 3) {
          const parts = [];
          for (let j = 0; j < 8; j++) {
            parts.push(((data[i + j * 2] << 8) | data[i + j * 2 + 1]).toString(16));
          }
          host = parts.join(':');
          i += 16;
        } else {
          return closeAll();
        }

        headerDone = true;
        remote = connect({ hostname: host, port: port });
        writer = remote.writable.getWriter();

        const payload = data.slice(i);
        if (payload.length > 0) await writer.write(payload);

        let sentHeader = false;
        remote.readable.pipeTo(new WritableStream({
          write(chunk) {
            if (!sentHeader) {
              const out = new Uint8Array(2 + chunk.byteLength);
              out[0] = 0;
              out[1] = 0;
              out.set(new Uint8Array(chunk), 2);
              ws.send(out);
              sentHeader = true;
            } else {
              ws.send(chunk);
            }
          },
          close() { closeAll(); },
          abort() { closeAll(); },
        })).catch(closeAll);
      } catch (e) {
        closeAll();
      }
    });
  });

  ws.addEventListener('close', closeAll);
  ws.addEventListener('error', closeAll);
}
