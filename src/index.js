import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import { randomUUID } from 'crypto';
class StelarServer {
    constructor(options = {}) {
        this.server = null;
        this.wss = null;
        this.clients = new Map();
        this.events = new Map();
        this.middlewares = [];
        this._hbTimer = null;
        this._wildcardHandler = null;
        this._connectionHandler = null;
        this._acks = new Map();
        this._externalServers = new WeakSet();
        this.port = options.port || 3000;
        this.server = options.server || null;
        this.namespace = options.namespace || '/';
        this.heartbeatInterval = options.heartbeatInterval || 30000;
    }
    static of(path, options = {}) {
        return new StelarServer({ ...options, namespace: path });
    }
    use(middleware) {
        this.middlewares.push(middleware);
        return this;
    }
    on(event, handler) {
        this.events.set(event, handler);
        return this;
    }
    onAll(handler) {
        this._wildcardHandler = handler;
        return this;
    }
    onConnection(handler) {
        this._connectionHandler = handler;
        return this;
    }
    onAck(name, handler) {
        this._acks.set(name, handler);
        return this;
    }
    broadcast(event, data) {
        this.clients.forEach((_, client) => {
            client.send(JSON.stringify({ event, data }));
        });
        return this;
    }
    broadcastBinary(event, buffer) {
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
    to(room, event, data) {
        this.clients.forEach((info, client) => {
            if (info.room === room) {
                client.send(JSON.stringify({ event, data }));
            }
        });
        return this;
    }
    toId(id, event, data) {
        this.clients.forEach((info, client) => {
            if (info.id === id) {
                client.send(JSON.stringify({ event, data }));
            }
        });
        return this;
    }
    getClients(room) {
        const list = [];
        this.clients.forEach((info) => {
            if (!room || info.room === room)
                list.push({ id: info.id, room: info.room });
        });
        return list;
    }
    getPort() {
        const address = this.server?.address();
        if (address && typeof address === 'object') {
            return address.port;
        }
        return this.port;
    }
    runMiddlewares(ctx, next) {
        const run = (i) => {
            if (i >= this.middlewares.length)
                return next();
            this.middlewares[i](ctx, () => run(i + 1));
        };
        run(0);
    }
    startHeartbeat() {
        this._hbTimer = setInterval(() => {
            this.clients.forEach((info, client) => {
                if (info.lastPing && Date.now() - info.lastPing > this.heartbeatInterval * 2) {
                    client.close();
                    this.clients.delete(client);
                }
                else {
                    client.send(JSON.stringify({ event: 'ping', data: Date.now() }));
                }
            });
        }, this.heartbeatInterval);
    }
    handleConnection(client, req) {
        const urlPath = new URL(req.url || '/', 'http://localhost').pathname;
        const nsPath = this.namespace === '/' ? '/' : this.namespace;
        if (nsPath !== '/' && urlPath !== nsPath) {
            client.close();
            return;
        }
        const clientId = randomUUID();
        const clientInfo = { id: clientId, room: null, lastPing: Date.now() };
        this.clients.set(client, clientInfo);
        const ctx = {
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
        client.on('message', (raw, isBinary) => {
            if (isBinary) {
                try {
                    const view = raw instanceof Uint8Array ? raw : new Uint8Array(raw);
                    let headerEnd = -1;
                    for (let i = 0; i < view.length; i++) {
                        if (view[i] === 0) {
                            headerEnd = i;
                            break;
                        }
                    }
                    if (headerEnd === -1)
                        return;
                    const headerStr = new TextDecoder().decode(view.slice(0, headerEnd));
                    const header = JSON.parse(headerStr);
                    const data = view.slice(headerEnd + 1);
                    const eventCtx = { ...ctx, data, buffer: data, isBinary: true };
                    const handler = this.events.get(header.event);
                    if (handler) {
                        handler(eventCtx);
                    }
                    else if (this._wildcardHandler) {
                        this._wildcardHandler({ event: header.event, data: eventCtx });
                    }
                }
                catch { }
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
                    const ackHandler = this._acks.get(msg._ackName);
                    const result = ackHandler({ ...ctx, data });
                    if (result !== undefined) {
                        client.send(JSON.stringify({ event: msg._ackName, data: result, _isAck: true }));
                    }
                    return;
                }
                const eventCtx = { ...ctx, data };
                const handler = this.events.get(event);
                if (handler) {
                    handler(eventCtx);
                }
                else if (this._wildcardHandler) {
                    this._wildcardHandler({ event, data: eventCtx });
                }
            }
            catch { }
        });
        client.on('close', () => {
            const info = this.clients.get(client);
            if (this.events.has('disconnect') && info) {
                const handler = this.events.get('disconnect');
                handler({ id: info.id, socket: client, req: req, emit: () => { }, send: () => { }, emitBinary: () => { }, broadcast: () => { }, broadcastBinary: () => { }, to: () => { }, toId: () => { }, getClients: () => [], joinRoom: () => { }, leaveRoom: () => { }, ack: () => { } });
            }
            this.clients.delete(client);
        });
        client.on('error', (err) => {
            if (this.events.has('error')) {
                const handler = this.events.get('error');
                handler({ id: clientId, socket: client, req: req, emit: () => { }, send: () => { }, emitBinary: () => { }, broadcast: () => { }, broadcastBinary: () => { }, to: () => { }, toId: () => { }, getClients: () => [], joinRoom: () => { }, leaveRoom: () => { }, ack: () => { }, error: err });
            }
        });
    }
    start(callback) {
        return new Promise((resolve) => {
            const startServer = (httpServer) => {
                this.server = httpServer;
                this.wss = new WebSocketServer({ server: httpServer });
                this.wss.on('connection', (client, req) => this.handleConnection(client, req));
                this.startHeartbeat();
                const finalPort = this.getPort();
                if (callback)
                    callback(finalPort);
                resolve(finalPort);
            };
            if (this.server) {
                this._externalServers.add(this.server);
                startServer(this.server);
            }
            else {
                const tryListen = (port) => {
                    this.server = createServer((_, res) => {
                        res.writeHead(200, { 'Content-Type': 'text/plain' });
                        res.end('Stelar Time Real Server');
                    });
                    this.server.on('error', (err) => {
                        if (err.code === 'EADDRINUSE' && port < 65535) {
                            tryListen(port + 1);
                        }
                    });
                    this.server.listen(port, () => {
                        this.port = port;
                        startServer(this.server);
                    });
                };
                tryListen(this.port);
            }
        });
    }
    stop() {
        if (this._hbTimer)
            clearInterval(this._hbTimer);
        if (this.wss)
            this.wss.close();
        if (this.server && !this._externalServers.has(this.server))
            this.server.close();
        return this;
    }
}
export default StelarServer;
export { StelarServer };
export { default as StelarClient } from './client.js';
