import { DurableObject } from "cloudflare:workers";

export class ChatRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        text TEXT NOT NULL,
        ts INTEGER NOT NULL
      )
    `);
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Hibernation API: keeps clients connected without burning idle compute
    this.ctx.acceptWebSocket(server);

    // Send last 40 messages to the newly joined user
    const history = this.ctx.storage.sql
      .exec("SELECT name, text, ts FROM messages ORDER BY id DESC LIMIT 40")
      .toArray()
      .reverse();

    try {
      server.send(JSON.stringify({ type: "history", messages: history }));
    } catch (_) {}

    // Broadcast updated online count to everyone
    this.broadcast({ type: "count", count: this.ctx.getWebSockets().length });

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, rawMessage) {
    try {
      const data = JSON.parse(rawMessage);
      if (data.type !== "chat") return;

      const name = String(data.name || "").trim().slice(0, 18);
      const text = String(data.text || "").trim().slice(0, 180);
      if (!name || !text) return;

      const ts = Date.now();

      // Save message and prune to keep only the latest 50
      this.ctx.storage.sql.exec(
        "INSERT INTO messages (name, text, ts) VALUES (?, ?, ?)",
        name,
        text,
        ts
      );
      this.ctx.storage.sql.exec(`
        DELETE FROM messages
        WHERE id NOT IN (SELECT id FROM messages ORDER BY id DESC LIMIT 50)
      `);

      this.broadcast({ type: "chat", name, text, ts });
    } catch (_) {}
  }

  async webSocketClose(ws, code) {
    try { ws.close(code, "Closed"); } catch (_) {}
    this.broadcast({ type: "count", count: this.ctx.getWebSockets().length });
  }

  async webSocketError(ws) {
    try { ws.close(1011, "Error"); } catch (_) {}
    this.broadcast({ type: "count", count: this.ctx.getWebSockets().length });
  }

  broadcast(payload) {
    const msg = JSON.stringify(payload);
    for (const sock of this.ctx.getWebSockets()) {
      try {
        sock.send(msg);
      } catch (_) {}
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/ws") {
      const id = env.CHAT_ROOM.idFromName("global-studio-room");
      const stub = env.CHAT_ROOM.get(id);
      return stub.fetch(request);
    }
    return new Response("Studio Chat Server Online", { status: 200 });
  }
};
