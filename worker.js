// Relays WebSocket messages between exactly two peers (host + guest) in a room.
// Game logic lives entirely in index.html on the host's device; this Worker
// only pairs sockets and forwards raw messages, plus emits join/leave events.
export class Room {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sockets = { host: null, guest: null };
  }

  async fetch(request) {
    const url = new URL(request.url);
    const role = url.searchParams.get('role');
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('expected websocket', { status: 400 });
    }
    if (role !== 'host' && role !== 'guest') {
      return new Response('invalid role', { status: 400 });
    }

    let occupied = (await this.state.storage.get('occupied')) || {};

    if (role === 'host') {
      if (occupied.host) return new Response('room already has a host', { status: 409 });
    } else {
      if (!occupied.host) return new Response('room not found', { status: 404 });
      if (occupied.guest) return new Response('room is full', { status: 409 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();

    occupied[role] = true;
    await this.state.storage.put('occupied', occupied);
    this.sockets[role] = server;

    server.addEventListener('message', (evt) => {
      const other = role === 'host' ? this.sockets.guest : this.sockets.host;
      if (other) {
        try { other.send(evt.data); } catch (e) {}
      }
    });

    const cleanup = async () => {
      this.sockets[role] = null;
      let occ = (await this.state.storage.get('occupied')) || {};
      delete occ[role];
      await this.state.storage.put('occupied', occ);
      const other = role === 'host' ? this.sockets.guest : this.sockets.host;
      if (other) {
        try { other.send(JSON.stringify({ type: 'peerLeft' })); } catch (e) {}
      }
      if (!occ.host && !occ.guest) {
        await this.state.storage.deleteAll();
      }
    };
    server.addEventListener('close', cleanup);
    server.addEventListener('error', cleanup);

    if (role === 'guest' && this.sockets.host) {
      try { this.sockets.host.send(JSON.stringify({ type: 'peerJoined' })); } catch (e) {}
    }

    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/room\/([0-9]{4})$/);
    if (match) {
      const id = env.ROOM.idFromName(match[1]);
      const stub = env.ROOM.get(id);
      return stub.fetch(request);
    }
    return env.ASSETS.fetch(request);
  }
};
