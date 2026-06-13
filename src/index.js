/**
 * @stelar-time-real Server — Dual-protocol: WebSocket (RFC 6455) + binary TCP
 */
import { createServer as createHttp } from 'http';
import { createServer as createTcp } from 'net';
import { randomUUID } from 'crypto';
import { createServer as createTls } from 'tls';
import { FrameParser, encodeJsonFrame, encodeBinaryFrame, encodePingFrame, encodePongFrame, encodeAckResFrame, encodeConnectFrame, encodeDisconnectFrame, encodeErrorFrame, FRAME_JSON, FRAME_BINARY, FRAME_PING, FRAME_PONG, FRAME_ACK_REQ, FRAME_ACK_RES, FRAME_JOIN, FRAME_LEAVE, FRAME_CONNECT, ProtocolError, DEFAULT_MAX_FRAME_SIZE, } from './protocol.js';
import { WSFrameParser, buildUpgradeResponse, validateWSKey, createWSTextFrame, createWSBinaryFrame, createWSCloseFrame, createWSPingFrame, createWSPongFrame, OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG, WebSocketError, CLOSE_POLICY_VIOLATION, CLOSE_MESSAGE_TOO_BIG, CLOSE_GOING_AWAY, DEFAULT_MAX_WS_FRAME_SIZE, } from './websocket.js';
import { Logger, NULL_LOGGER } from './logger.js';
class RateLimiter {
    constructor(maxPts = 100, winMs = 1000) {
        this.maxPts = maxPts;
        this.winMs = winMs;
        this.limits = new Map();
    }
    check(id, cost = 1) {
        const now = Date.now();
        let e = this.limits.get(id);
        if (!e || now >= e.resetTime) {
            e = { count: 0, resetTime: now + this.winMs };
            this.limits.set(id, e);
        }
        if (e.count + cost > this.maxPts)
            return false;
        e.count += cost;
        return true;
    }
    cleanup() { const now = Date.now(); for (const [id, e] of this.limits)
        if (now >= e.resetTime)
            this.limits.delete(id); }
    reset(id) { this.limits.delete(id); }
    size() { return this.limits.size; }
}
class IPTracker {
    constructor(max = 50) {
        this.max = max;
        this.m = new Map();
    }
    check(ip) { return (this.m.get(ip) || 0) < this.max; }
    add(ip) { this.m.set(ip, (this.m.get(ip) || 0) + 1); }
    remove(ip) { const c = this.m.get(ip) || 0; c <= 1 ? this.m.delete(ip) : this.m.set(ip, c - 1); }
    getCount(ip) { return this.m.get(ip) || 0; }
    cleanup() { for (const [ip, c] of this.m)
        if (c <= 0)
            this.m.delete(ip); }
}
/* ── Server ── */
class StelarServer {
    constructor(o = {}) {
        this.httpServer = null;
        this.tcpServer = null;
        this.evRateLimits = new Map();
        this.clientRates = new Map();
        this.clients = new Map();
        this.byId = new Map();
        this.rooms = new Map();
        this.events = new Map();
        this.mw = [];
        this._hb = null;
        this._rc = null;
        this._wild = null;
        this._connH = null;
        this._acks = new Map();
        this._ext = new WeakSet();
        this._upgH = null;
        this._reqH = null;
        this._started = false;
        this._startTime = 0;
        this._shutting = false;
        this._sigH = { int: null, term: null };
        this._totalConns = 0;
        this._totalRecv = 0;
        this._totalSent = 0;
        this._shutdownCbs = [];
        this.port = o.port || 3000;
        this.httpServer = o.server || null;
        this.ns = o.namespace || '/';
        this.hbInterval = o.heartbeatInterval || 30000;
        this.hbTimeout = o.heartbeatTimeout || this.hbInterval * 2;
        this.tcpPort = o.tcpPort !== undefined ? o.tcpPort : false;
        this.maxConns = o.maxConnections || 10000;
        this.maxRooms = o.maxRooms || 10000;
        this.maxRoomsPerClient = o.maxRoomsPerClient || 50;
        this.maxPayload = o.maxPayloadSize || 10 * 1024 * 1024;
        this.maxFrame = o.maxFrameSize || DEFAULT_MAX_FRAME_SIZE;
        this.maxWSFrame = o.maxFrameSize || DEFAULT_MAX_WS_FRAME_SIZE;
        this.connTimeout = o.connectTimeout || 10000;
        this.doGraceful = o.gracefulShutdown !== false;
        this.shutdownMs = o.shutdownTimeout || 10000;
        this.healthPath = o.healthEndpoint !== undefined ? o.healthEndpoint : '/health';
        this.tlsOpts = o.tls;
        this.origins = o.allowedOrigins || null;
        this._crl = o.customRateLimiter || null;
        this._cit = o.customIPTracker || null;
        this._genId = o.generateClientId || null;
        this._healthFn = o.customHealthHandler || null;
        this.hooks = o.hooks || {};
        if (o.eventRateLimits)
            for (const [ev, c] of Object.entries(o.eventRateLimits))
                this.evRateLimits.set(ev, new RateLimiter(c.maxPoints, c.windowMs));
        const rl = o.rateLimit && typeof o.rateLimit === 'object' ? o.rateLimit : {};
        this.rateLimiter = o.rateLimit === false && !this._crl ? null : this._crl ? null : new RateLimiter(rl.maxPoints || 100, rl.windowMs || 1000);
        this.ipTracker = this._cit ? new IPTracker() : new IPTracker(o.maxConnectionsPerIP || 50);
        this.log = o.logger === false ? NULL_LOGGER : o.logger instanceof Logger ? o.logger : new Logger({ level: o.logger || 'info', prefix: 'stelar:server' });
    }
    static of(path, o = {}) { return new StelarServer({ ...o, namespace: path }); }
    /* ── Runtime config ── */
    updateConfig(o) {
        if (o.maxConnections !== undefined)
            this.maxConns = o.maxConnections;
        if (o.maxConnectionsPerIP !== undefined && !this._cit)
            this.ipTracker = new IPTracker(o.maxConnectionsPerIP);
        if (o.maxRooms !== undefined)
            this.maxRooms = o.maxRooms;
        if (o.maxRoomsPerClient !== undefined)
            this.maxRoomsPerClient = o.maxRoomsPerClient;
        if (o.maxPayloadSize !== undefined)
            this.maxPayload = o.maxPayloadSize;
        if (o.heartbeatInterval !== undefined)
            this.hbInterval = o.heartbeatInterval;
        if (o.heartbeatTimeout !== undefined)
            this.hbTimeout = o.heartbeatTimeout;
        if (o.allowedOrigins !== undefined)
            this.origins = o.allowedOrigins;
        if (o.rateLimit === false) {
            this.rateLimiter = null;
            this._crl = null;
        }
        else if (o.rateLimit && !this._crl)
            this.rateLimiter = new RateLimiter(o.rateLimit.maxPoints || 100, o.rateLimit.windowMs || 1000);
        if (o.customRateLimiter !== undefined) {
            this._crl = o.customRateLimiter;
            this.rateLimiter = null;
        }
        if (o.customIPTracker !== undefined)
            this._cit = o.customIPTracker;
        if (o.generateClientId !== undefined)
            this._genId = o.generateClientId;
        if (o.customHealthHandler !== undefined)
            this._healthFn = o.customHealthHandler;
        if (o.hooks !== undefined)
            this.hooks = { ...this.hooks, ...o.hooks };
        if (o.eventRateLimits !== undefined) {
            this.evRateLimits.clear();
            for (const [ev, c] of Object.entries(o.eventRateLimits))
                this.evRateLimits.set(ev, new RateLimiter(c.maxPoints, c.windowMs));
        }
        this.log.info('Config updated');
        return this;
    }
    setClientRateLimit(id, c) { this.clientRates.set(id, new RateLimiter(c.maxPoints, c.windowMs)); return this; }
    removeClientRateLimit(id) { this.clientRates.delete(id); return this; }
    setEventRateLimit(ev, c) { this.evRateLimits.set(ev, new RateLimiter(c.maxPoints, c.windowMs)); return this; }
    removeEventRateLimit(ev) { this.evRateLimits.delete(ev); return this; }
    getConfig() {
        return Object.freeze({
            maxConnections: this.maxConns, maxConnectionsPerIP: this._cit ? -1 : 50,
            maxRooms: this.maxRooms, maxRoomsPerClient: this.maxRoomsPerClient, maxPayloadSize: this.maxPayload,
            heartbeatInterval: this.hbInterval, heartbeatTimeout: this.hbTimeout, connectTimeout: this.connTimeout,
            shutdownTimeout: this.shutdownMs, hasCustomRateLimiter: this._crl !== null, hasCustomIPTracker: this._cit !== null,
            hasCustomClientIdGenerator: this._genId !== null, hasCustomHealthHandler: this._healthFn !== null,
            eventRateLimits: Array.from(this.evRateLimits.keys()), hooks: Object.keys(this.hooks), allowedOrigins: this.origins,
        });
    }
    /* ── Event registration ── */
    use(mw) { this.mw.push(mw); return this; }
    on(ev, h) { this.events.set(ev, h); return this; }
    onAll(h) { this._wild = h; return this; }
    onConnection(h) { this._connH = h; return this; }
    onDisconnect(h) { this.events.set('disconnect', h); return this; }
    onAck(name, h) { this._acks.set(name, h); return this; }
    /* ── Messaging ── */
    broadcast(event, data, excludeId) {
        if (this.hooks.onBeforeBroadcast?.({ event, data, excludeId }) === false)
            return this;
        const wsF = createWSTextFrame(JSON.stringify({ event, data }));
        const tcpF = encodeJsonFrame(event, data, this.maxFrame);
        let sent = 0;
        this.clients.forEach(r => { if (excludeId && r.info.id === excludeId)
            return; if (this._write(r, wsF, tcpF))
            sent++; });
        this._totalSent += sent;
        return this;
    }
    broadcastBinary(event, buf) { this.clients.forEach(r => this._sendBin(r, event, buf)); }
    to(room, event, data, excludeId) {
        const ids = this.rooms.get(room);
        if (!ids)
            return this;
        const wsF = createWSTextFrame(JSON.stringify({ event, data }));
        const tcpF = encodeJsonFrame(event, data, this.maxFrame);
        let sent = 0;
        for (const id of ids) {
            if (excludeId && id === excludeId)
                continue;
            const r = this.byId.get(id);
            if (r && this._write(r, wsF, tcpF))
                sent++;
        }
        this._totalSent += sent;
        return this;
    }
    toId(id, event, data) {
        const r = this.byId.get(id);
        if (r && this._sendJson(r, event, data))
            this._totalSent++;
        return this;
    }
    getClients(room) {
        const list = [];
        this.clients.forEach(r => { if (!room || r.info.rooms.has(room))
            list.push({ id: r.info.id, rooms: [...r.info.rooms] }); });
        return list;
    }
    getRoomMembers(room) { return this.rooms.get(room) ? [...this.rooms.get(room)] : []; }
    getRooms() { return [...this.rooms.keys()]; }
    getPort() { const a = this.httpServer?.address(); return a && typeof a === 'object' ? a.port : this.port; }
    getStats() {
        let ws = 0, tcp = 0;
        this.clients.forEach(r => r.protocol === 'ws' ? ws++ : tcp++);
        return {
            totalConnections: this._totalConns, activeConnections: this.clients.size,
            totalMessagesReceived: this._totalRecv, totalMessagesSent: this._totalSent,
            totalRooms: this.rooms.size, uptime: this._startTime ? Date.now() - this._startTime : 0,
            wsConnections: ws, tcpConnections: tcp, memoryUsage: process.memoryUsage(),
            rateLimiterEntries: this._crl?.size() ?? this.rateLimiter?.size() ?? 0,
        };
    }
    onShutdown(cb) { this._shutdownCbs.push(cb); return this; }
    /* ── Private: send helpers ── */
    _sendJson(r, event, data) {
        if (r.socket.destroyed || r.socket.writableEnded)
            return false;
        try {
            r.socket.write(r.protocol === 'ws' ? createWSTextFrame(JSON.stringify({ event, data })) : encodeJsonFrame(event, data, this.maxFrame));
            r.info.messagesSent++;
            return true;
        }
        catch {
            return false;
        }
    }
    _write(r, wsF, tcpF) {
        if (r.socket.destroyed || r.socket.writableEnded)
            return false;
        try {
            r.socket.write(r.protocol === 'ws' ? wsF : tcpF);
            r.info.messagesSent++;
            return true;
        }
        catch {
            return false;
        }
    }
    _sendBin(r, event, buf) {
        if (r.socket.destroyed || r.socket.writableEnded)
            return false;
        try {
            if (r.protocol === 'ws') {
                const hdr = Buffer.from(JSON.stringify({ event, _binary: true }), 'utf8');
                const combined = Buffer.alloc(hdr.length + 1 + buf.byteLength);
                hdr.copy(combined, 0);
                combined[hdr.length] = 0;
                combined.set(new Uint8Array(buf), hdr.length + 1);
                r.socket.write(createWSBinaryFrame(combined));
            }
            else {
                r.socket.write(encodeBinaryFrame(event, new Uint8Array(buf), this.maxFrame));
            }
            r.info.messagesSent++;
            return true;
        }
        catch {
            return false;
        }
    }
    _checkRate(cid, event) {
        const co = this.clientRates.get(cid);
        if (co)
            return co.check(cid);
        if (event && this.evRateLimits.has(event) && !this.evRateLimits.get(event).check(cid))
            return false;
        if (this._crl)
            return this._crl.check(cid);
        if (this.rateLimiter)
            return this.rateLimiter.check(cid);
        return true;
    }
    _getIP(socket, req) {
        if (req) {
            const fwd = req.headers['x-forwarded-for'];
            if (typeof fwd === 'string')
                return fwd.split(',')[0].trim();
        }
        return socket.remoteAddress || 'unknown';
    }
    /* ── Private: client lifecycle ── */
    _register(socket, proto, req, parser) {
        const ip = this._getIP(socket, req);
        if (this.clients.size >= this.maxConns) {
            this.hooks.onMaxConnectionsReached?.({ activeConnections: this.clients.size, max: this.maxConns, ip });
            this.log.warn('Max connections reached', { active: this.clients.size, max: this.maxConns });
            if (proto === 'ws')
                try {
                    socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Server full'));
                }
                catch { }
            socket.destroy();
            return null;
        }
        const tracker = this._cit || this.ipTracker;
        if (!tracker.check(ip)) {
            this.log.warn('Max connections per IP', { ip });
            if (proto === 'ws')
                try {
                    socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Too many connections'));
                }
                catch { }
            socket.destroy();
            return null;
        }
        const id = this._genId ? this._genId() : randomUUID();
        const info = { id, rooms: new Set(), lastPing: Date.now(), protocol: proto, connectedAt: Date.now(), metadata: new Map(), messagesReceived: 0, messagesSent: 0, remoteAddress: ip };
        const record = { info, socket, parser, protocol: proto };
        this.clients.set(socket, record);
        this.byId.set(id, record);
        tracker.add(ip);
        this._totalConns++;
        return record;
    }
    _unregister(r, ctx) {
        this.hooks.onClientDisconnect?.({ clientId: r.info.id, ip: r.info.remoteAddress, protocol: r.info.protocol, rooms: new Set(r.info.rooms) });
        for (const room of r.info.rooms) {
            const m = this.rooms.get(room);
            if (m) {
                m.delete(r.info.id);
                if (!m.size)
                    this.rooms.delete(room);
            }
        }
        r.info.rooms.clear();
        this.byId.delete(r.info.id);
        this.clients.delete(r.socket);
        (this._cit || this.ipTracker).remove(r.info.remoteAddress);
        if (this._crl)
            this._crl.reset(r.info.id);
        else
            this.rateLimiter?.reset(r.info.id);
        this.clientRates.delete(r.info.id);
        const h = this.events.get('disconnect');
        if (h)
            try {
                h({ ...ctx, event: 'disconnect' });
            }
            catch (e) {
                this.log.error('Disconnect handler error', { error: String(e) });
            }
    }
    _joinRoom(r, room) {
        if (this.hooks.onClientJoinRoom?.({ clientId: r.info.id, room, metadata: r.info.metadata }) === false)
            return;
        if (r.info.rooms.size >= this.maxRoomsPerClient) {
            this.hooks.onMaxRoomsPerClientReached?.({ clientId: r.info.id, room, currentRooms: r.info.rooms.size, max: this.maxRoomsPerClient });
            return;
        }
        if (this.rooms.size >= this.maxRooms && !this.rooms.has(room)) {
            this.hooks.onMaxRoomsReached?.({ clientId: r.info.id, room, totalRooms: this.rooms.size, max: this.maxRooms });
            return;
        }
        r.info.rooms.add(room);
        if (!this.rooms.has(room))
            this.rooms.set(room, new Set());
        this.rooms.get(room).add(r.info.id);
        this._sendJson(r, 'joined-room', room);
    }
    _leaveRoom(r, room) {
        if (this.hooks.onClientLeaveRoom?.({ clientId: r.info.id, room }) === false)
            return;
        r.info.rooms.delete(room);
        const m = this.rooms.get(room);
        if (m) {
            m.delete(r.info.id);
            if (!m.size)
                this.rooms.delete(room);
        }
        this._sendJson(r, 'left-room', room);
    }
    /* ── Private: context & middleware ── */
    _buildCtx(r, req) {
        const s = this;
        const ctx = {
            id: r.info.id, socket: r.socket, req, clientInfo: r.info,
            emit: (ev, d) => { if (s._sendJson(r, ev, d))
                s._totalSent++; },
            send: (rid, d) => { if (s._sendJson(r, rid, { data: d, _isAck: true }))
                s._totalSent++; },
            emitBinary: (ev, buf) => { if (s._sendBin(r, ev, buf))
                s._totalSent++; },
            broadcast: (ev, d) => s.broadcast(ev, d, r.info.id),
            broadcastBinary: (ev, buf) => s.broadcastBinary(ev, buf),
            to: (room, ev, d) => s.to(room, ev, d, r.info.id),
            toId: (id, ev, d) => s.toId(id, ev, d),
            getClients: (room) => s.getClients(room),
            joinRoom: (room) => s._joinRoom(r, room),
            leaveRoom: (room) => s._leaveRoom(r, room),
            setMetadata: (k, v) => r.info.metadata.set(k, v),
            getMetadata: (k) => r.info.metadata.get(k),
            ack: (name, d) => {
                const h = s._acks.get(name);
                if (!h)
                    return;
                let res;
                try {
                    res = h({ ...ctx, data: d });
                }
                catch (e) {
                    s.log.error('ACK handler error', { name, error: String(e) });
                    return;
                }
                if (res !== undefined) {
                    try {
                        if (r.protocol === 'ws') {
                            const p = { event: name, data: res, _isAck: true };
                            if (ctx._correlationId)
                                p._correlationId = ctx._correlationId;
                            r.socket.write(createWSTextFrame(JSON.stringify(p)));
                        }
                        else {
                            r.socket.write(ctx._correlationId
                                ? encodeAckResFrame(name, { data: res, _correlationId: ctx._correlationId }, s.maxFrame)
                                : encodeAckResFrame(name, res, s.maxFrame));
                        }
                        s._totalSent++;
                    }
                    catch (e) {
                        s.log.error('ACK send error', { name, error: String(e) });
                    }
                }
            },
        };
        return ctx;
    }
    _runMw(ctx, next) {
        const run = (i) => { if (i >= this.mw.length)
            return next(); try {
            this.mw[i](ctx, () => run(i + 1));
        }
        catch {
            ctx.socket.destroy();
        } };
        run(0);
    }
    /* ── Private: event dispatch (shared by WS & TCP) ── */
    _dispatch(r, ctx, event, data, correlationId) {
        if (event === 'pong') {
            r.info.lastPing = Date.now();
            return;
        }
        if (event === 'join-room') {
            if (data)
                this._joinRoom(r, String(data));
            return;
        }
        if (event === 'leave-room') {
            if (data)
                this._leaveRoom(r, String(data));
            return;
        }
        const ectx = { ...ctx, data, event, _correlationId: correlationId };
        const h = this.events.get(event);
        if (h)
            try {
                h(ectx);
            }
            catch (e) {
                this.log.error('Event handler error', { event, error: String(e) });
            }
        if (this._wild)
            try {
                this._wild({ event, data: ectx });
            }
            catch (e) {
                this.log.error('Wildcard error', { error: String(e) });
            }
    }
    /* ── Private: heartbeat ── */
    _startHeartbeat() {
        this._hb = setInterval(() => {
            const now = Date.now();
            this.clients.forEach(r => {
                if (now - r.info.lastPing > this.hbTimeout) {
                    r.socket.destroy();
                }
                else
                    try {
                        r.socket.write(r.protocol === 'ws' ? createWSPingFrame() : encodePingFrame());
                    }
                    catch { }
            });
        }, this.hbInterval);
        this._hb?.unref?.();
    }
    /* ── Private: WS upgrade ── */
    _wsUpgrade(req, socket, head) {
        const path = new URL(req.url || '/', 'http://localhost').pathname;
        const nsPath = this.ns === '/' ? '/' : this.ns;
        if (nsPath !== '/' && path !== nsPath) {
            socket.destroy();
            return;
        }
        if (this.origins && !this.origins.includes(req.headers['origin'] || '')) {
            socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
            socket.destroy();
            return;
        }
        const key = req.headers['sec-websocket-key'];
        if (!key || !validateWSKey(key)) {
            socket.destroy();
            return;
        }
        try {
            const extra = {};
            const origin = req.headers['origin'];
            if (origin && this.origins?.includes(origin))
                extra['Access-Control-Allow-Origin'] = origin;
            socket.write(buildUpgradeResponse(key, extra));
        }
        catch {
            socket.destroy();
            return;
        }
        const timer = setTimeout(() => { if (!this.clients.has(socket))
            socket.destroy(); }, this.connTimeout);
        timer.unref();
        const r = this._register(socket, 'ws', req, new WSFrameParser(this.maxWSFrame));
        if (!r) {
            clearTimeout(timer);
            return;
        }
        const ctx = this._buildCtx(r, req);
        this.hooks.onClientConnect?.({ clientId: r.info.id, ip: r.info.remoteAddress, protocol: 'ws', metadata: r.info.metadata });
        this._runMw(ctx, () => { if (this._connH)
            try {
                this._connH(ctx);
            }
            catch (e) {
                this.log.error('Connection handler error', { error: String(e) });
            } });
        this.log.info('WS connected', { clientId: r.info.id, ip: r.info.remoteAddress });
        if (head.length > 0)
            this._processWS(r, head, ctx);
        socket.on('data', (d) => { clearTimeout(timer); this._processWS(r, d, ctx); });
        socket.on('close', () => { clearTimeout(timer); this._unregister(r, ctx); });
        socket.on('error', (e) => { this.log.warn('WS error', { clientId: r.info.id, error: e.message }); this._handleErr(r, ctx, e); });
        socket.on('drain', () => socket.resume());
    }
    _processWS(r, data, ctx) {
        let frames;
        try {
            frames = r.parser.feed(data);
        }
        catch (e) {
            if (e instanceof WebSocketError) {
                this.log.warn('WS protocol error', { code: e.code, message: e.message });
                try {
                    r.socket.write(createWSCloseFrame(e.code, e.message));
                }
                catch { }
            }
            else
                this.log.error('WS parse error', { error: String(e) });
            r.socket.destroy();
            return;
        }
        for (const f of frames) {
            if (!r.socket.destroyed)
                this._handleWSFrame(r, f, ctx);
        }
    }
    _handleWSFrame(r, frame, ctx) {
        const { opcode, payload } = frame;
        if (opcode === OP_PING) {
            try {
                r.socket.write(createWSPongFrame(payload));
            }
            catch { }
            return;
        }
        if (opcode === OP_CLOSE) {
            try {
                r.socket.write(createWSCloseFrame());
            }
            catch { }
            r.socket.end();
            return;
        }
        if (opcode === OP_PONG) {
            r.info.lastPing = Date.now();
            return;
        }
        if (!this._checkRate(r.info.id)) {
            this.log.warn('Rate limit exceeded', { clientId: r.info.id });
            if (this.hooks.onRateLimitExceeded?.({ clientId: r.info.id, protocol: 'ws' }) === false)
                return;
            try {
                r.socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Rate limit exceeded'));
            }
            catch { }
            r.socket.destroy();
            return;
        }
        if (opcode === OP_TEXT) {
            r.info.messagesReceived++;
            this._totalRecv++;
            if (payload.length > this.maxPayload) {
                this.hooks.onPayloadTooLarge?.({ clientId: r.info.id, size: payload.length, max: this.maxPayload });
                try {
                    r.socket.write(createWSCloseFrame(CLOSE_MESSAGE_TOO_BIG));
                }
                catch { }
                r.socket.destroy();
                return;
            }
            let msg;
            try {
                msg = JSON.parse(payload.toString('utf8'));
            }
            catch {
                this.hooks.onInvalidMessage?.({ clientId: r.info.id, reason: 'Invalid JSON', protocol: 'ws' });
                return;
            }
            const event = String(msg.event || ''), data = msg.data, corrId = msg._correlationId ? String(msg._correlationId) : undefined;
            if (!event)
                return;
            if (!this._checkRate(r.info.id, event)) {
                this.log.warn('Event rate limit', { clientId: r.info.id, event });
                if (this.hooks.onRateLimitExceeded?.({ clientId: r.info.id, event, protocol: 'ws' }) === false)
                    return;
                try {
                    r.socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Rate limit exceeded'));
                }
                catch { }
                r.socket.destroy();
                return;
            }
            if (msg._ackName && this._acks.has(String(msg._ackName))) {
                const name = String(msg._ackName), h = this._acks.get(name);
                let res;
                try {
                    res = h({ ...ctx, data, _correlationId: corrId });
                }
                catch (e) {
                    this.log.error('ACK handler error', { name, error: String(e) });
                    return;
                }
                if (res !== undefined) {
                    const p = { event: name, data: res, _isAck: true };
                    if (corrId)
                        p._correlationId = corrId;
                    try {
                        r.socket.write(createWSTextFrame(JSON.stringify(p)));
                        this._totalSent++;
                    }
                    catch { }
                }
                return;
            }
            this._dispatch(r, ctx, event, data, corrId);
        }
        if (opcode === OP_BINARY) {
            r.info.messagesReceived++;
            this._totalRecv++;
            if (payload.length > this.maxPayload) {
                this.hooks.onPayloadTooLarge?.({ clientId: r.info.id, size: payload.length, max: this.maxPayload });
                return;
            }
            try {
                let end = -1;
                for (let i = 0; i < payload.length; i++)
                    if (payload[i] === 0) {
                        end = i;
                        break;
                    }
                if (end === -1)
                    return;
                const hdr = JSON.parse(payload.subarray(0, end).toString('utf8'));
                const buf = payload.subarray(end + 1);
                if (hdr.event && !this._checkRate(r.info.id, hdr.event)) {
                    this.log.warn('Binary rate limit', { clientId: r.info.id, event: hdr.event });
                    if (this.hooks.onRateLimitExceeded?.({ clientId: r.info.id, event: hdr.event, protocol: 'ws' }) === false)
                        return;
                    return;
                }
                const ectx = { ...ctx, data: buf, buffer: buf, isBinary: true, event: hdr.event };
                const h = this.events.get(hdr.event);
                if (h)
                    try {
                        h(ectx);
                    }
                    catch { }
                if (this._wild)
                    try {
                        this._wild({ event: hdr.event, data: ectx });
                    }
                    catch { }
            }
            catch {
                this.hooks.onInvalidMessage?.({ clientId: r.info.id, reason: 'Invalid binary frame', protocol: 'ws' });
            }
        }
    }
    /* ── Private: TCP connection ── */
    _tcpConnect(socket) {
        const r = this._register(socket, 'tcp', null, new FrameParser(this.maxFrame));
        if (!r)
            return;
        const ctx = this._buildCtx(r, null);
        try {
            socket.write(encodeConnectFrame(r.info.id));
        }
        catch {
            socket.destroy();
            return;
        }
        this.hooks.onClientConnect?.({ clientId: r.info.id, ip: r.info.remoteAddress, protocol: 'tcp', metadata: r.info.metadata });
        this._runMw(ctx, () => { if (this._connH)
            try {
                this._connH(ctx);
            }
            catch (e) {
                this.log.error('TCP connection handler error', { error: String(e) });
            } });
        this.log.info('TCP connected', { clientId: r.info.id, ip: r.info.remoteAddress });
        socket.on('data', (d) => this._processTCP(r, d, ctx));
        socket.on('close', () => this._unregister(r, ctx));
        socket.on('error', (e) => { this.log.warn('TCP error', { clientId: r.info.id, error: e.message }); this._handleErr(r, ctx, e); });
        socket.on('drain', () => socket.resume());
    }
    _processTCP(r, data, ctx) {
        let frames;
        try {
            frames = r.parser.feed(data);
        }
        catch (e) {
            if (e instanceof ProtocolError) {
                this.log.warn('TCP protocol error', { code: e.code, message: e.message });
                try {
                    r.socket.write(encodeErrorFrame(e.message));
                }
                catch { }
            }
            r.socket.destroy();
            return;
        }
        for (const f of frames) {
            if (!r.socket.destroyed)
                this._handleTCPFrame(r, f, ctx);
        }
    }
    _handleTCPFrame(r, frame, ctx) {
        const { type, event, payload } = frame;
        if (type === FRAME_PING) {
            try {
                r.socket.write(encodePongFrame());
            }
            catch { }
            r.info.lastPing = Date.now();
            return;
        }
        if (type === FRAME_PONG) {
            r.info.lastPing = Date.now();
            return;
        }
        if (type === FRAME_CONNECT)
            return;
        if (!this._checkRate(r.info.id, event)) {
            this.log.warn('TCP rate limit', { clientId: r.info.id, event });
            if (this.hooks.onRateLimitExceeded?.({ clientId: r.info.id, event: event || undefined, protocol: 'tcp' }) === false)
                return;
            try {
                r.socket.write(encodeErrorFrame('Rate limit exceeded'));
            }
            catch { }
            r.socket.destroy();
            return;
        }
        if (type === FRAME_JOIN) {
            if (payload.toString('utf8'))
                this._joinRoom(r, payload.toString('utf8'));
            return;
        }
        if (type === FRAME_LEAVE) {
            if (payload.toString('utf8'))
                this._leaveRoom(r, payload.toString('utf8'));
            return;
        }
        if (payload.length > this.maxPayload) {
            this.hooks.onPayloadTooLarge?.({ clientId: r.info.id, event, size: payload.length, max: this.maxPayload });
            return;
        }
        r.info.messagesReceived++;
        this._totalRecv++;
        if (type === FRAME_JSON) {
            let data;
            try {
                data = JSON.parse(payload.toString('utf8'));
            }
            catch {
                this.hooks.onInvalidMessage?.({ clientId: r.info.id, reason: 'Invalid JSON', protocol: 'tcp' });
                return;
            }
            this._dispatch(r, ctx, event, data);
            return;
        }
        if (type === FRAME_ACK_REQ) {
            if (this._acks.has(event)) {
                try {
                    const parsed = JSON.parse(payload.toString('utf8'));
                    const data = parsed && typeof parsed === 'object' && 'data' in parsed ? parsed.data : parsed;
                    const corrId = parsed && typeof parsed === 'object' && '_correlationId' in parsed ? String(parsed._correlationId) : undefined;
                    const h = this._acks.get(event);
                    const res = h({ ...ctx, data, _correlationId: corrId });
                    if (res !== undefined) {
                        r.socket.write(corrId ? encodeAckResFrame(event, { data: res, _correlationId: corrId }, this.maxFrame) : encodeAckResFrame(event, res, this.maxFrame));
                        this._totalSent++;
                    }
                }
                catch (e) {
                    this.log.error('TCP ACK handler error', { event, error: String(e) });
                }
            }
            return;
        }
        if (type === FRAME_ACK_RES) {
            try {
                const raw = JSON.parse(payload.toString('utf8'));
                const data = raw && typeof raw === 'object' && 'data' in raw ? raw.data : raw;
                const corrId = raw && typeof raw === 'object' && '_correlationId' in raw ? String(raw._correlationId) : undefined;
                const h = this._acks.get(corrId || event);
                if (h)
                    try {
                        h({ ...ctx, data });
                    }
                    catch { }
            }
            catch { }
            return;
        }
        if (type === FRAME_BINARY) {
            const ectx = { ...ctx, data: payload, buffer: payload, isBinary: true, event };
            const h = this.events.get(event);
            if (h)
                try {
                    h(ectx);
                }
                catch { }
            if (this._wild)
                try {
                    this._wild({ event, data: ectx });
                }
                catch { }
        }
    }
    _handleErr(r, ctx, err) {
        const h = this.events.get('error');
        if (h)
            try {
                h({ ...ctx, error: err, event: 'error' });
            }
            catch { }
    }
    /* ── Private: health check ── */
    _health(req, res) {
        if (this._healthFn) {
            try {
                this._healthFn(req, res, this.getStats());
            }
            catch {
                if (!res.headersSent) {
                    res.writeHead(500);
                    res.end('{"status":"error"}');
                }
            }
            return;
        }
        const origin = req.headers['origin'];
        if (origin && (!this.origins || this.origins.includes(origin))) {
            res.setHeader('Access-Control-Allow-Origin', origin);
            res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
            res.setHeader('Access-Control-Max-Age', '86400');
        }
        if (req.method === 'OPTIONS') {
            res.writeHead(204);
            res.end();
            return;
        }
        if (this.healthPath && req.url === this.healthPath && req.method === 'GET') {
            const s = this.getStats();
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ status: 'ok', ...s, uptimeSeconds: Math.floor(s.uptime / 1000), memoryMB: Math.round(s.memoryUsage.heapUsed / 1024 / 1024 * 100) / 100 }));
            return;
        }
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Stelar Time Real v3 Server');
    }
    /* ── Private: graceful shutdown ── */
    _emitShutdown(sig, force) {
        if (!this._shutdownCbs.length) {
            process.exit(force ? 1 : 0);
            return;
        }
        for (const cb of this._shutdownCbs)
            try {
                cb(sig, force);
            }
            catch { }
    }
    _setupShutdown() {
        if (!this.doGraceful)
            return;
        let done = false;
        const shutdown = (sig) => {
            if (done)
                return;
            done = true;
            this._shutting = true;
            this.log.info(`Received ${sig}, shutting down...`);
            this.stop();
            if (!this.clients.size) {
                this.log.info('Shutdown complete');
                this._emitShutdown(sig, false);
                return;
            }
            this.log.info(`Waiting for ${this.clients.size} connections (timeout: ${this.shutdownMs}ms)`);
            this.clients.forEach(r => { try {
                r.socket.write(r.protocol === 'ws' ? createWSCloseFrame(CLOSE_GOING_AWAY, 'Shutting down') : encodeDisconnectFrame());
                r.socket.end();
            }
            catch { } });
            const forceT = setTimeout(() => { this.clients.forEach(r => { try {
                r.socket.destroy();
            }
            catch { } }); this.clients.clear(); this.byId.clear(); this._emitShutdown(sig, true); }, this.shutdownMs);
            forceT.unref();
            const check = setInterval(() => { if (!this.clients.size) {
                clearInterval(check);
                clearTimeout(forceT);
                this._emitShutdown(sig, false);
            } }, 100);
            check.unref();
        };
        this._sigH.int = () => shutdown('SIGINT');
        this._sigH.term = () => shutdown('SIGTERM');
        process.on('SIGINT', this._sigH.int);
        process.on('SIGTERM', this._sigH.term);
    }
    _removeSignals() {
        if (this._sigH.int) {
            process.off('SIGINT', this._sigH.int);
            this._sigH.int = null;
        }
        if (this._sigH.term) {
            process.off('SIGTERM', this._sigH.term);
            this._sigH.term = null;
        }
    }
    /* ── Start / Stop ── */
    start(cb) {
        if (this._started) {
            const p = this.getPort();
            cb?.(p);
            return Promise.resolve(p);
        }
        this._started = true;
        this._startTime = Date.now();
        return new Promise(resolve => {
            const onHttp = (srv) => {
                this.httpServer = srv;
                this._reqH = (req, res) => this._health(req, res);
                this._upgH = (req, socket, head) => this._wsUpgrade(req, socket, head);
                srv.on('request', this._reqH);
                srv.on('upgrade', this._upgH);
                this._startHeartbeat();
                this._rc = setInterval(() => {
                    if (this._crl)
                        this._crl.cleanup();
                    else
                        this.rateLimiter?.cleanup();
                    (this._cit || this.ipTracker).cleanup();
                    for (const [id, l] of this.clientRates) {
                        l.cleanup();
                        if (!this.byId.has(id))
                            this.clientRates.delete(id);
                    }
                    for (const [, l] of this.evRateLimits)
                        l.cleanup();
                }, 30000);
                this._rc?.unref?.();
                this._setupShutdown();
                const p = this.getPort();
                this.log.info('Server started', { port: p, namespace: this.ns, tls: !!this.tlsOpts });
                cb?.(p);
                resolve(p);
            };
            if (this.httpServer) {
                this._ext.add(this.httpServer);
                onHttp(this.httpServer);
            }
            else {
                const tryListen = (port) => {
                    const srv = createHttp();
                    srv.on('error', (e) => { if (e.code === 'EADDRINUSE' && port < 65535)
                        tryListen(port + 1);
                    else
                        this.log.error('HTTP error', { error: e.message }); });
                    srv.listen(port, () => { this.port = port; onHttp(srv); });
                };
                tryListen(this.port);
            }
            if (this.tcpPort !== false) {
                const p = typeof this.tcpPort === 'number' ? this.tcpPort : this.port + 1;
                this._startTCP(p);
            }
        });
    }
    _startTCP(port, attempts = 0) {
        const handler = (s) => this._tcpConnect(s);
        const startPlain = (p, a) => {
            const srv = createTcp(handler);
            srv.on('error', (e) => { if (e.code === 'EADDRINUSE' && a < 10) {
                this.tcpServer = null;
                this._startTCP(p + 1, a + 1);
            }
            else
                this.log.error('TCP error', { error: e.message }); });
            srv.listen(p, () => { this.tcpServer = srv; this.log.info('TCP started', { port: p }); });
        };
        if (this.tlsOpts) {
            try {
                const srv = createTls(this.tlsOpts, handler);
                this.tcpServer = srv;
                this.tcpServer.on('error', (e) => { if (e.code === 'EADDRINUSE' && attempts < 10) {
                    this.tcpServer = null;
                    this._startTCP(port + 1, attempts + 1);
                }
                else
                    this.log.error('TLS TCP error', { error: e.message }); });
                this.tcpServer.listen(port, () => this.log.info('TLS TCP started', { port }));
            }
            catch {
                startPlain(port, attempts);
            }
        }
        else
            startPlain(port, attempts);
    }
    stop() {
        if (this._hb) {
            clearInterval(this._hb);
            this._hb = null;
        }
        if (this._rc) {
            clearInterval(this._rc);
            this._rc = null;
        }
        this.clients.forEach(r => { if (!r.socket.destroyed)
            r.socket.destroy(); });
        this.clients.clear();
        this.byId.clear();
        this.rooms.clear();
        this.clientRates.clear();
        if (this.httpServer) {
            if (this._upgH)
                this.httpServer.off('upgrade', this._upgH);
            if (this._reqH)
                this.httpServer.off('request', this._reqH);
            if (!this._ext.has(this.httpServer))
                this.httpServer.close();
            this.httpServer = null;
            this._upgH = null;
            this._reqH = null;
        }
        if (this.tcpServer) {
            this.tcpServer.close();
            this.tcpServer = null;
        }
        this._started = false;
        this._removeSignals();
        this.log.info('Server stopped');
        return this;
    }
}
export default StelarServer;
export { StelarServer };
export { default as StelarClient } from './client.js';
export { Logger, NULL_LOGGER } from './logger.js';
export { ProtocolError, validateEventName, DEFAULT_MAX_FRAME_SIZE, MAX_EVENT_LENGTH, HEADER_SIZE } from './protocol.js';
export { WebSocketError, DEFAULT_MAX_WS_FRAME_SIZE, CLOSE_NORMAL, CLOSE_GOING_AWAY, CLOSE_PROTOCOL_ERROR, CLOSE_POLICY_VIOLATION, CLOSE_MESSAGE_TOO_BIG, CLOSE_INVALID_PAYLOAD, CLOSE_UNSUPPORTED } from './websocket.js';
