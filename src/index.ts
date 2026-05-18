import { createServer, IncomingMessage, Server, ServerResponse } from 'http';
import { WebSocketServer, WebSocket, RawData } from 'ws';
import { randomUUID } from 'crypto';

export interface StelarOptions {
  port?: number;
  server?: Server;
  namespace?: string;
  heartbeatInterval?: number;
}

export interface StelarClientInfo {
  id: string;
  room: string | null;
  lastPing: number;
}

export interface StelarContext {
  id: string;
  socket: WebSocket;
  req: IncomingMessage;
  data?: unknown;
  buffer?: Uint8Array;
  isBinary?: boolean;
  event?: string;
  error?: Error;
  emit: (event: string, data: unknown) => void;
  send: (respId: string, data: unknown) => void;
  emitBinary: (event: string, buffer: ArrayBuffer) => void;
  broadcast: (event: string, data: unknown) => void;
  broadcastBinary: (event: string, buffer: ArrayBuffer) => void;
  to: (room: string, event: string, data: unknown) => void;
  toId: (id: string, event: string, data: unknown) => void;
  getClients: (room?: string) => { id: string; room: string | null }[];
  joinRoom: (room: string) => void;
  leaveRoom: () => void;
  ack: (ackName: string, data: unknown) => void;
}

export interface StelarMiddleware {
  (ctx: StelarContext, next: () => void): void;
}

export type StelarEventHandler = (ctx: StelarContext) => void;

export type StelarWildcardHandler = (data: { event: string; data: StelarContext }) => void;

class StelarServer {
  private port: number;
  private server: Server | null = null;
  private namespace: string;
  private wss: WebSocketServer | null = null;
  private clients = new Map<WebSocket, StelarClientInfo>();
  private events: Map<string, StelarEventHandler> = new Map();
  private middlewares: StelarMiddleware[] = [];
  private heartbeatInterval: number;
  private _hbTimer: ReturnType<typeof setInterval> | null = null;
  private _wildcardHandler: StelarWildcardHandler | null = null;
  private _connectionHandler: StelarEventHandler | null = null;
  private _acks: Map<string, StelarEventHandler> = new Map();
  private _externalServers = new WeakSet<Server>();

  constructor(options: StelarOptions = {}) {
    this.port = options.port || 3000;
    this.server = options.server || null;
    this.namespace = options.namespace || '/';
    this.heartbeatInterval = options.heartbeatInterval || 30000;
  }

  static of(path: string, options: StelarOptions = {}): StelarServer {
    return new StelarServer({ ...options, namespace: path });
  }

  use(middleware: StelarMiddleware): this {
    this.middlewares.push(middleware);
    return this;
  }

  on(event: string, handler: StelarEventHandler): this {
    this.events.set(event, handler);
    return this;
  }

  onAll(handler: StelarWildcardHandler): this {
    this._wildcardHandler = handler;
    return this;
  }

  onConnection(handler: StelarEventHandler): this {
    this._connectionHandler = handler;
    return this;
  }

  onAck(name: string, handler: StelarEventHandler): this {
    this._acks.set(name, handler);
    return this;
  }

  broadcast(event: string, data: unknown): this {
    this.clients.forEach((_, client) => {
      client.send(JSON.stringify({ event, data }));
    });
    return this;
  }

  broadcastBinary(event: string, buffer: ArrayBuffer): void {
    const header = JSON.stringify({ event, _binary: true });
    const headerBytes = new TextEncoder().encode(header);
    const combined = new Uint8Array(headerBytes.length + 1 + buffer.byteLength);
    combined.set(headerBytes, 0);
    combined[headerBytes.length] = 0;
    combined.set(new Uint8Array(buffer), headerBytes.length + 1);

    this.clients.forEach((_, client) => {
      client.send(combined);
    });
  }

  to(room: string, event: string, data: unknown): this {
    this.clients.forEach((info, client) => {
      if (info.room === room) {
        client.send(JSON.stringify({ event, data }));
      }
    });
    return this;
  }

  toId(id: string, event: string, data: unknown): this {
    this.clients.forEach((info, client) => {
      if (info.id === id) {
        client.send(JSON.stringify({ event, data }));
      }
    });
    return this;
  }

  getClients(room?: string): { id: string; room: string | null }[] {
    const list: { id: string; room: string | null }[] = [];
    this.clients.forEach((info) => {
      if (!room || info.room === room) list.push({ id: info.id, room: info.room });
    });
    return list;
  }

  getPort(): number {
    const address = this.server?.address();
    if (address && typeof address === 'object') {
      return address.port;
    }
    return this.port;
  }

  private runMiddlewares(ctx: StelarContext, next: () => void): void {
    const run = (i: number): void => {
      if (i >= this.middlewares.length) return next();
      this.middlewares[i](ctx, () => run(i + 1));
    };
    run(0);
  }

  private startHeartbeat(): void {
    this._hbTimer = setInterval(() => {
      this.clients.forEach((info, client) => {
        if (info.lastPing && Date.now() - info.lastPing > this.heartbeatInterval * 2) {
          client.close();
          this.clients.delete(client);
        } else {
          client.send(JSON.stringify({ event: 'ping', data: Date.now() }));
        }
      });
    }, this.heartbeatInterval);
  }

  private handleConnection(client: WebSocket, req: IncomingMessage): void {
    const urlPath = new URL(req.url || '/', 'http://localhost').pathname;
    const nsPath = this.namespace === '/' ? '/' : this.namespace;

    if (nsPath !== '/' && urlPath !== nsPath) {
      client.close();
      return;
    }

    const clientId = randomUUID();
    const clientInfo: StelarClientInfo = { id: clientId, room: null, lastPing: Date.now() };
    this.clients.set(client, clientInfo);

    const ctx: StelarContext = {
      id: clientId,
      socket: client,
      req,
      emit: (evt, d) => client.send(JSON.stringify({ event: evt, data: d })),
      send: (respId, d) => client.send(JSON.stringify({ event: respId, data: d, _isAck: true })),
      emitBinary: (evt, buffer) => client.send(buffer),
      broadcast: (evt, d) => this.broadcast(evt, d),
      broadcastBinary: (evt, buffer) => this.broadcastBinary(evt, buffer),
      to: (room, evt, d) => this.to(room, evt, d),
      toId: (id, evt, d) => this.toId(id, evt, d),
      getClients: (room) => this.getClients(room),
      joinRoom: (room) => {
        clientInfo.room = room;
        client.send(JSON.stringify({ event: 'joined-room', data: room }));
      },
      leaveRoom: () => {
        clientInfo.room = null;
      },
      ack: (ackName, data) => {
        const ackHandler = this._acks.get(ackName);
        if (ackHandler) {
          const result = ackHandler({ ...ctx, data });
          if (result !== undefined) {
            client.send(JSON.stringify({ event: ackName, data: result, _isAck: true }));
          }
        }
      }
    };

    this.runMiddlewares(ctx, () => {
      if (this._connectionHandler) {
        this._connectionHandler(ctx);
      }
    });

    client.on('message', (raw: RawData, isBinary: boolean) => {
      if (isBinary) {
        try {
          const view = raw instanceof Uint8Array ? raw : new Uint8Array(raw as ArrayBuffer);
          let headerEnd = -1;
          for (let i = 0; i < view.length; i++) {
            if (view[i] === 0) {
              headerEnd = i;
              break;
            }
          }
          if (headerEnd === -1) return;

          const headerStr = new TextDecoder().decode(view.slice(0, headerEnd));
          const header = JSON.parse(headerStr);
          const data = view.slice(headerEnd + 1);

          const eventCtx: StelarContext = { ...ctx, data, buffer: data, isBinary: true };

          const handler = this.events.get(header.event);
          if (handler) {
            handler(eventCtx);
          } else if (this._wildcardHandler) {
            this._wildcardHandler({ event: header.event, data: eventCtx });
          }
        } catch {}
        return;
      }

      try {
        const msg = JSON.parse(raw.toString());
        const { event, data } = msg;

        if (event === 'pong') {
          clientInfo.lastPing = Date.now();
          return;
        }

        if (event === 'join-room') {
          if (typeof data === 'string') {
            clientInfo.room = data;
            client.send(JSON.stringify({ event: 'joined-room', data }));
          }
        }

        if (event === 'leave-room') {
          clientInfo.room = null;
          client.send(JSON.stringify({ event: 'left-room', data }));
        }

        if (msg._ackName && this._acks.has(msg._ackName)) {
          const ackHandler = this._acks.get(msg._ackName)!;
          const result = ackHandler({ ...ctx, data });
          if (result !== undefined) {
            client.send(JSON.stringify({ event: msg._ackName, data: result, _isAck: true }));
          }
          return;
        }

        const eventCtx: StelarContext = { ...ctx, data };

        const handler = this.events.get(event);
        if (handler) {
          handler(eventCtx);
        } else if (this._wildcardHandler) {
          this._wildcardHandler({ event, data: eventCtx });
        }
      } catch {}
    });

    client.on('close', () => {
      const info = this.clients.get(client);
      if (this.events.has('disconnect') && info) {
        const handler = this.events.get('disconnect')!;
        handler({ id: info.id, socket: client, req: req, emit: () => {}, send: () => {}, emitBinary: () => {}, broadcast: () => {}, broadcastBinary: () => {}, to: () => {}, toId: () => {}, getClients: () => [], joinRoom: () => {}, leaveRoom: () => {}, ack: () => {} });
      }
      this.clients.delete(client);
    });

    client.on('error', (err) => {
      if (this.events.has('error')) {
        const handler = this.events.get('error')!;
        handler({ id: clientId, socket: client, req: req, emit: () => {}, send: () => {}, emitBinary: () => {}, broadcast: () => {}, broadcastBinary: () => {}, to: () => {}, toId: () => {}, getClients: () => [], joinRoom: () => {}, leaveRoom: () => {}, ack: () => {}, error: err });
      }
    });
  }

  start(callback?: (port: number) => void): Promise<number> {
    return new Promise((resolve) => {
      const startServer = (httpServer: Server): void => {
        this.server = httpServer;
        this.wss = new WebSocketServer({ server: httpServer });
        this.wss.on('connection', (client, req) => this.handleConnection(client, req));
        this.startHeartbeat();

        const finalPort = this.getPort();
        if (callback) callback(finalPort);
        resolve(finalPort);
      };

      if (this.server) {
        this._externalServers.add(this.server);
        startServer(this.server);
      } else {
        const tryListen = (port: number): void => {
          this.server = createServer((_, res) => {
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            res.end('Stelar Time Real Server');
          });

          this.server.on('error', (err: NodeJS.ErrnoException) => {
            if (err.code === 'EADDRINUSE' && port < 65535) {
              tryListen(port + 1);
            }
          });

          this.server.listen(port, () => {
            this.port = port;
            startServer(this.server!);
          });
        };
        tryListen(this.port);
      }
    });
  }

  stop(): this {
    if (this._hbTimer) clearInterval(this._hbTimer);
    if (this.wss) this.wss.close();
    if (this.server && !this._externalServers.has(this.server)) this.server.close();
    return this;
  }
}

export default StelarServer;
export { StelarServer };
export { default as StelarClient } from './client.js';