/**
 * @stelar-time-real Client — Browser WS / Node WS / binary TCP
 */
import { FrameParser, encodeJsonFrame, encodeBinaryFrame, encodeAckReqFrame, encodePingFrame, encodePongFrame, encodeJoinFrame, encodeLeaveFrame, FRAME_JSON, FRAME_BINARY, FRAME_PING, FRAME_PONG, FRAME_ACK_RES, FRAME_CONNECT, validateEventName, DEFAULT_MAX_FRAME_SIZE, } from './protocol.js';
import { WSFrameParser, generateWSKey, createWSTextFrameMasked, createWSBinaryFrameMasked, createWSCloseFrameMasked, createWSPongFrameMasked, OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG, } from './websocket.js';
import { Logger, NULL_LOGGER } from './logger.js';
const isNode = typeof process !== 'undefined' && process.versions?.node != null;
/* Lazy-load Node modules for browser compat */
let _http, _net, _tls, _https;
async function loadModules() {
    if (!_http) {
        _http = await import('http');
        _net = await import('net');
        _tls = await import('tls');
        _https = await import('https');
    }
}
class MsgQueue {
    constructor(max = 100) {
        this.max = max;
        this.q = [];
    }
    push(m) { if (this.q.length >= this.max)
        this.q.shift(); this.q.push(m); return true; }
    drain() { const m = this.q; this.q = []; return m; }
    get length() { return this.q.length; }
    clear() { this.q = []; }
}
class StelarClient {
    constructor(urlOrPort = 'localhost:3000', o = {}) {
        this.events = new Map();
        this._wild = null;
        this._acks = new Map();
        this._state = 'disconnected';
        this._reconnAttempts = 0;
        this._hb = null;
        this._manualClose = false;
        this.id = null;
        this._reconnTimer = null;
        this._ackCounter = 0;
        this._sent = 0;
        this._recv = 0;
        this._connTime = 0;
        this._lastErr = null;
        this._ws = null;
        this._nodeSock = null;
        this._wsParser = null;
        this._tcpSock = null;
        this._tcpParser = null;
        if (typeof urlOrPort === 'number')
            this.url = `ws://localhost:${urlOrPort}`;
        else if (urlOrPort.includes('://'))
            this.url = urlOrPort.startsWith('http') ? 'ws' + urlOrPort.slice(4) : urlOrPort;
        else
            this.url = `ws://${urlOrPort}`;
        this.opts = {
            reconnection: o.reconnection !== false, reconnectionAttempts: o.reconnectionAttempts || 10,
            reconnectionDelay: o.reconnectionDelay || 1000, maxReconnectionDelay: o.maxReconnectionDelay || 30000,
            heartbeatInterval: o.heartbeatInterval || 30000, ackTimeout: o.ackTimeout || 5000,
            mode: o.mode || 'ws', maxPayloadSize: o.maxPayloadSize || 10 * 1024 * 1024,
            maxFrameSize: o.maxFrameSize || DEFAULT_MAX_FRAME_SIZE, messageQueueSize: o.messageQueueSize || 100,
            logger: o.logger !== undefined ? o.logger : 'warn', tls: o.tls || false,
            rejectUnauthorized: o.rejectUnauthorized !== false, headers: o.headers || {},
            customReconnectDelay: o.customReconnectDelay, hooks: o.hooks || {},
        };
        this._mq = new MsgQueue(this.opts.messageQueueSize);
        this.log = o.logger === false ? NULL_LOGGER : o.logger instanceof Logger ? o.logger : new Logger({ level: o.logger || 'warn', prefix: 'stelar:client' });
    }
    getState() { return this._state; }
    getId() { return this.id; }
    getUrl() { return this.url; }
    getMessagesSent() { return this._sent; }
    getMessagesReceived() { return this._recv; }
    getLastError() { return this._lastErr; }
    getQueueSize() { return this._mq.length; }
    getConnectTime() { return this._connTime; }
    setUrl(u) { this.url = u; return this; }
    updateOptions(o) {
        for (const k of ['reconnection', 'reconnectionAttempts', 'reconnectionDelay', 'maxReconnectionDelay', 'heartbeatInterval', 'ackTimeout', 'maxPayloadSize', 'maxFrameSize', 'messageQueueSize', 'headers'])
            if (o[k] !== undefined)
                this.opts[k] = o[k];
        if (o.customReconnectDelay !== undefined)
            this.opts.customReconnectDelay = o.customReconnectDelay;
        if (o.hooks !== undefined)
            this.opts.hooks = { ...this.opts.hooks, ...o.hooks };
        return this;
    }
    getOptions() {
        return Object.freeze({
            reconnection: this.opts.reconnection, reconnectionAttempts: this.opts.reconnectionAttempts,
            reconnectionDelay: this.opts.reconnectionDelay, maxReconnectionDelay: this.opts.maxReconnectionDelay,
            heartbeatInterval: this.opts.heartbeatInterval, ackTimeout: this.opts.ackTimeout, mode: this.opts.mode,
            maxPayloadSize: this.opts.maxPayloadSize, messageQueueSize: this.opts.messageQueueSize,
            hasCustomReconnectDelay: !!this.opts.customReconnectDelay, hooks: Object.keys(this.opts.hooks),
        });
    }
    on(ev, h) { this.events.set(ev, h); return this; }
    off(ev, h) { if (this.events.get(ev) === h)
        this.events.delete(ev); return this; }
    once(ev, h) { const w = (d) => { this.off(ev, w); h(d); }; this.on(ev, w); return this; }
    onAll(h) { this._wild = h; return this; }
    onAck(name, h) { this._acks.set(name, { handler: h, timer: null }); return this; }
    removeAllListeners(ev) { ev ? this.events.delete(ev) : this.events.clear(); return this; }
    emit(event, data, opts = {}) {
        try {
            if (event)
                validateEventName(event);
        }
        catch {
            this.log.warn('Invalid event', { event });
            return this;
        }
        if (this.opts.hooks.onBeforeEmit?.({ event, data }) === false)
            return this;
        try {
            const s = JSON.stringify(data);
            if (s.length > this.opts.maxPayloadSize) {
                this.log.warn('Payload too large', { event });
                return this;
            }
        }
        catch {
            return this;
        }
        if (this._state !== 'connected') {
            if (this.opts.reconnection) {
                this._mq.push({ event, data, opts, ts: Date.now() });
                this.opts.hooks.onMessageQueued?.({ event, data, queueSize: this._mq.length });
            }
            return this;
        }
        try {
            const send = (wsPayload, tcpPayload) => {
                if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed)
                    this._tcpSock.write(tcpPayload());
                else if (this._nodeSock && !this._nodeSock.destroyed)
                    this._nodeSock.write(wsPayload());
                else if (this._ws && this._ws.readyState === WebSocket.OPEN)
                    this._ws.send(JSON.stringify({ event, data, ...(opts.ack ? { _ackName: opts.ack } : {}), ...(opts._correlationId ? { _correlationId: opts._correlationId } : {}) }));
                else {
                    this._mq.push({ event, data, opts, ts: Date.now() });
                    return;
                }
                this._sent++;
            };
            if (opts.ack) {
                send(() => { const p = { event, data, _ackName: opts.ack }; if (opts._correlationId)
                    p._correlationId = opts._correlationId; return createWSTextFrameMasked(JSON.stringify(p)); }, () => { const d = { event, data }; if (opts._correlationId)
                    d._correlationId = opts._correlationId; return encodeAckReqFrame(opts.ack, d, this.opts.maxFrameSize); });
            }
            else {
                send(() => { const p = { event, data }; if (opts._correlationId)
                    p._correlationId = opts._correlationId; return createWSTextFrameMasked(JSON.stringify(p)); }, () => encodeJsonFrame(event, data, this.opts.maxFrameSize));
            }
        }
        catch (e) {
            this.log.error('Emit error', { event, error: String(e) });
            this.opts.hooks.onError?.({ error: e instanceof Error ? e : new Error(String(e)), context: 'emit' });
            this._mq.push({ event, data, opts, ts: Date.now() });
        }
        return this;
    }
    emitBinary(event, data) {
        if (data.byteLength > this.opts.maxPayloadSize) {
            this.log.warn('Binary too large', { event });
            return this;
        }
        if (this.opts.hooks.onBeforeEmit?.({ event, data }) === false)
            return this;
        if (this._state !== 'connected')
            return this;
        try {
            if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed) {
                this._tcpSock.write(encodeBinaryFrame(event, new Uint8Array(data), this.opts.maxFrameSize));
            }
            else if (this._nodeSock && !this._nodeSock.destroyed) {
                const hdr = Buffer.from(JSON.stringify({ event }), 'utf8');
                const c = Buffer.alloc(hdr.length + 1 + data.byteLength);
                hdr.copy(c, 0);
                c[hdr.length] = 0;
                c.set(new Uint8Array(data), hdr.length + 1);
                this._nodeSock.write(createWSBinaryFrameMasked(c));
            }
            else if (this._ws && this._ws.readyState === WebSocket.OPEN) {
                const hdr = new TextEncoder().encode(JSON.stringify({ event }));
                const c = new Uint8Array(hdr.length + 1 + data.byteLength);
                c.set(hdr, 0);
                c[hdr.length] = 0;
                c.set(new Uint8Array(data), hdr.length + 1);
                this._ws.send(c);
            }
            this._sent++;
        }
        catch (e) {
            this.log.error('Binary emit error', { event, error: String(e) });
        }
        return this;
    }
    sendFile(f) { return this.emitBinary('file', f); }
    sendImage(b) { return this.emitBinary('image', b); }
    request(event, data, ackName) {
        return new Promise((resolve, reject) => {
            const corrId = `${ackName}#${++this._ackCounter}`;
            const t = setTimeout(() => { this._acks.delete(corrId); reject(new Error(`ACK '${ackName}' timeout`)); }, this.opts.ackTimeout);
            t.unref();
            this._acks.set(corrId, { handler: (d) => { clearTimeout(t); this._acks.delete(corrId); resolve(d); }, timer: t });
            this.emit(event, data, { ack: ackName, _correlationId: corrId });
        });
    }
    joinRoom(room) {
        if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed)
            try {
                this._tcpSock.write(encodeJoinFrame(room, this.opts.maxFrameSize));
            }
            catch { }
        else
            this.emit('join-room', room);
        return this;
    }
    leaveRoom(room) {
        if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed)
            try {
                this._tcpSock.write(encodeLeaveFrame(room));
            }
            catch { }
        else
            this.emit('leave-room', room);
        return this;
    }
    connect(cb) {
        if (this._state === 'connected' || this._state === 'connecting')
            return this;
        if (this._state === 'reconnecting' && this._reconnTimer) {
            clearTimeout(this._reconnTimer);
            this._reconnTimer = null;
        }
        this._manualClose = false;
        this._setState('connecting');
        if (this.opts.mode === 'tcp' && isNode)
            this._connectTCP();
        else if (isNode)
            this._connectNodeWS();
        else
            this._connectBrowser();
        if (cb) {
            const check = setInterval(() => { if (this._state === 'connected') {
                clearInterval(check);
                cb();
            } }, 50);
            const safety = setTimeout(() => clearInterval(check), this.opts.ackTimeout);
            safety.unref();
        }
        return this;
    }
    disconnect() {
        this._manualClose = true;
        if (this._tcpSock && !this._tcpSock.destroyed)
            try {
                this._tcpSock.destroy();
            }
            catch { }
        if (this._nodeSock && !this._nodeSock.destroyed) {
            try {
                this._nodeSock.write(createWSCloseFrameMasked());
            }
            catch { }
            try {
                this._nodeSock.end();
            }
            catch { }
        }
        if (this._ws)
            try {
                this._ws.close();
            }
            catch { }
        this._fullCleanup();
        this._setState('disconnected');
        return this;
    }
    isConnected() { return this._state === 'connected'; }
    /* ── Private ── */
    _setState(s) {
        const prev = this._state;
        this._state = s;
        if (prev !== s) {
            this.log.debug('State', { from: prev, to: s });
            this.opts.hooks.onStateChange?.({ from: prev, to: s });
        }
    }
    _startHB() {
        this._stopHB();
        this._hb = setInterval(() => {
            if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed)
                try {
                    this._tcpSock.write(encodePingFrame());
                }
                catch { }
            else if (this._nodeSock && !this._nodeSock.destroyed)
                try {
                    this._nodeSock.write(createWSTextFrameMasked(JSON.stringify({ event: 'pong', data: Date.now() })));
                }
                catch { }
            else if (this._ws && this._ws.readyState === WebSocket.OPEN)
                this._ws.send(JSON.stringify({ event: 'pong', data: Date.now() }));
        }, this.opts.heartbeatInterval);
        this._hb?.unref?.();
    }
    _stopHB() { if (this._hb) {
        clearInterval(this._hb);
        this._hb = null;
    } }
    _getDelay() {
        const base = this.opts.reconnectionDelay, max = this.opts.maxReconnectionDelay;
        const def = Math.min(base * Math.pow(1.5, this._reconnAttempts - 1), max);
        const custom = this.opts.hooks.onReconnectDelay?.({ attempt: this._reconnAttempts, defaultDelay: def });
        if (typeof custom === 'number')
            return custom;
        if (this.opts.customReconnectDelay)
            return this.opts.customReconnectDelay(this._reconnAttempts, base, max);
        return Math.floor(def + def * 0.2 * Math.random());
    }
    _drain() {
        if (!this._mq.length)
            return;
        const msgs = this._mq.drain();
        this.log.info('Draining queue', { count: msgs.length });
        for (const m of msgs) {
            try {
                const p = { event: m.event, data: m.data };
                if (m.opts.ack)
                    p._ackName = m.opts.ack;
                if (m.opts._correlationId)
                    p._correlationId = m.opts._correlationId;
                if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed) {
                    this._tcpSock.write(m.opts.ack ? encodeAckReqFrame(m.opts.ack, p, this.opts.maxFrameSize) : encodeJsonFrame(m.event, m.data, this.opts.maxFrameSize));
                }
                else if (this._nodeSock && !this._nodeSock.destroyed) {
                    this._nodeSock.write(createWSTextFrameMasked(JSON.stringify(p)));
                }
                else if (this._ws && this._ws.readyState === WebSocket.OPEN) {
                    this._ws.send(JSON.stringify(p));
                }
                this._sent++;
            }
            catch (e) {
                this.log.error('Drain error', { event: m.event, error: String(e) });
            }
        }
        this.opts.hooks.onQueueDrained?.({ count: msgs.length });
    }
    _cleanupAcks() { for (const [, e] of this._acks)
        if (e.timer)
            clearTimeout(e.timer); this._acks.clear(); }
    _fullCleanup() {
        this._stopHB();
        this._cleanupAcks();
        if (this._reconnTimer) {
            clearTimeout(this._reconnTimer);
            this._reconnTimer = null;
        }
        this._nodeSock = null;
        this._wsParser = null;
        this._tcpSock = null;
        this._tcpParser = null;
        this._ws = null;
    }
    _tryReconnect(fn) {
        if (this._manualClose || !this.opts.reconnection)
            return;
        if (this._reconnAttempts >= this.opts.reconnectionAttempts) {
            this.log.warn('Max reconnect attempts');
            this.events.get('reconnect_failed')?.(undefined);
            return;
        }
        this._reconnAttempts++;
        this._setState('reconnecting');
        const delay = this._getDelay();
        this.log.info('Reconnecting', { attempt: this._reconnAttempts, delay });
        this.events.get('reconnecting')?.(this._reconnAttempts);
        this._reconnTimer = setTimeout(() => { this._reconnTimer = null; if (!this._manualClose)
            fn(); }, delay);
    }
    _onConnected() {
        this._setState('connected');
        this._reconnAttempts = 0;
        this._connTime = Date.now();
        this.events.get('connect')?.(undefined);
        this._startHB();
        this._drain();
    }
    /* ── Browser WS ── */
    _connectBrowser() {
        try {
            const ws = new WebSocket(this.url);
            ws.binaryType = 'arraybuffer';
            ws.onopen = () => this._onConnected();
            ws.onmessage = (e) => { this._recv++; this._handleBrowserMsg(e); };
            ws.onclose = (e) => { this._setState('disconnected'); this._fullCleanup(); this.events.get('disconnect')?.({ code: e.code, reason: e.reason }); this._tryReconnect(() => this._connectBrowser()); };
            ws.onerror = () => { this._lastErr = new Error('WebSocket error'); this.events.get('error')?.(this._lastErr); this.opts.hooks.onError?.({ error: this._lastErr, context: 'browser-ws' }); };
            this._ws = ws;
        }
        catch (e) {
            this._lastErr = e instanceof Error ? e : new Error(String(e));
            this._setState('disconnected');
            this._tryReconnect(() => this._connectBrowser());
        }
    }
    _handleBrowserMsg(e) {
        try {
            if (e.data instanceof ArrayBuffer) {
                const v = new Uint8Array(e.data);
                let end = -1;
                for (let i = 0; i < v.length; i++)
                    if (v[i] === 0) {
                        end = i;
                        break;
                    }
                if (end === -1)
                    return;
                const hdr = JSON.parse(new TextDecoder().decode(v.slice(0, end)));
                const buf = v.slice(end + 1).buffer;
                this.opts.hooks.onMessage?.({ event: hdr.event, data: buf, isBinary: true });
                this.events.get(hdr.event)?.(buf);
                this._wild?.({ event: hdr.event, data: buf, isBinary: true, buffer: buf });
                return;
            }
            const msg = JSON.parse(e.data), { event, data, _isAck } = msg;
            if (event === 'ping')
                return;
            this.opts.hooks.onMessage?.({ event, data, isBinary: false });
            if (_isAck) {
                const key = msg._correlationId || event;
                if (this._acks.has(key)) {
                    const entry = this._acks.get(key);
                    if (entry.timer)
                        clearTimeout(entry.timer);
                    this._acks.delete(key);
                    entry.handler(data);
                    return;
                }
            }
            this.events.get(event)?.(data);
            this._wild?.({ event, data });
        }
        catch { }
    }
    /* ── Node WS ── */
    async _connectNodeWS() {
        try {
            await loadModules();
            if (!_http)
                return;
            const parsed = new URL(this.url), secure = parsed.protocol === 'wss:' || this.opts.tls;
            const key = generateWSKey();
            const hdrs = { Upgrade: 'websocket', Connection: 'Upgrade', 'Sec-WebSocket-Key': key, 'Sec-WebSocket-Version': '13', ...this.opts.headers };
            const mod = secure && _https ? _https : _http;
            const req = mod.request({ hostname: parsed.hostname, port: parseInt(parsed.port) || (secure ? 443 : 80), path: parsed.pathname + parsed.search, method: 'GET', headers: hdrs, rejectUnauthorized: this.opts.rejectUnauthorized });
            req.setTimeout(this.opts.ackTimeout, () => req.destroy(new Error('Timeout')));
            req.on('upgrade', (_res, socket, head) => {
                this._nodeSock = socket;
                this._wsParser = new WSFrameParser(this.opts.maxFrameSize);
                if (head.length > 0)
                    this._processNodeWS(head);
                socket.on('data', (d) => this._processNodeWS(d));
                socket.on('close', () => { this._setState('disconnected'); this._fullCleanup(); this.events.get('disconnect')?.(undefined); this._tryReconnect(() => this._connectNodeWS()); });
                socket.on('error', (e) => { this._lastErr = e; this.events.get('error')?.(e); this.opts.hooks.onError?.({ error: e, context: 'node-ws' }); });
                socket.on('drain', () => socket.resume());
                this.log.info('Node WS connected', { secure });
                this._onConnected();
            });
            req.on('error', (e) => { this._lastErr = e; this.events.get('error')?.(e); this._tryReconnect(() => this._connectNodeWS()); });
            req.end();
        }
        catch (e) {
            this._lastErr = e instanceof Error ? e : new Error(String(e));
            this._tryReconnect(() => this._connectNodeWS());
        }
    }
    _processNodeWS(data) {
        if (!this._wsParser)
            return;
        let frames;
        try {
            frames = this._wsParser.feed(data);
        }
        catch {
            this.log.error('WS parse error');
            return;
        }
        for (const f of frames) {
            this._recv++;
            this._handleNodeFrame(f);
        }
    }
    _handleNodeFrame(f) {
        if (f.opcode === OP_PING) {
            if (this._nodeSock && !this._nodeSock.destroyed)
                try {
                    this._nodeSock.write(createWSPongFrameMasked());
                }
                catch { }
            return;
        }
        if (f.opcode === OP_PONG)
            return;
        if (f.opcode === OP_CLOSE) {
            if (this._nodeSock && !this._nodeSock.destroyed)
                try {
                    this._nodeSock.end();
                }
                catch { }
            return;
        }
        if (f.opcode === OP_TEXT) {
            try {
                const msg = JSON.parse(f.payload.toString('utf8')), { event, data, _isAck } = msg;
                if (event === 'ping')
                    return;
                this.opts.hooks.onMessage?.({ event, data, isBinary: false });
                if (_isAck) {
                    const key = msg._correlationId || event;
                    if (this._acks.has(key)) {
                        const e = this._acks.get(key);
                        if (e.timer)
                            clearTimeout(e.timer);
                        this._acks.delete(key);
                        e.handler(data);
                        return;
                    }
                }
                this.events.get(event)?.(data);
                this._wild?.({ event, data });
            }
            catch { }
            return;
        }
        if (f.opcode === OP_BINARY) {
            try {
                let end = -1;
                for (let i = 0; i < f.payload.length; i++)
                    if (f.payload[i] === 0) {
                        end = i;
                        break;
                    }
                if (end === -1)
                    return;
                const hdr = JSON.parse(f.payload.subarray(0, end).toString('utf8'));
                const buf = f.payload.subarray(end + 1).buffer;
                this.opts.hooks.onMessage?.({ event: hdr.event, data: buf, isBinary: true });
                this.events.get(hdr.event)?.(buf);
                this._wild?.({ event: hdr.event, data: buf, isBinary: true, buffer: buf });
            }
            catch { }
        }
    }
    /* ── TCP ── */
    async _connectTCP() {
        try {
            await loadModules();
            if (!_net)
                return;
            const parsed = new URL(this.url), port = parseInt(parsed.port) + 1 || 3001, host = parsed.hostname || 'localhost';
            const sockOpts = { host, port };
            const socket = this.opts.tls && _tls ? _tls.connect({ ...sockOpts, rejectUnauthorized: this.opts.rejectUnauthorized }) : _net.createConnection(sockOpts);
            socket.setTimeout(this.opts.ackTimeout, () => socket.destroy(new Error('TCP timeout')));
            socket.on('connect', () => { socket.setTimeout(0); this._tcpParser = new FrameParser(this.opts.maxFrameSize); this.log.info('TCP connected', { host, port }); this._onConnected(); });
            socket.on('data', (d) => { if (!this._tcpParser)
                return; let frames; try {
                frames = this._tcpParser.feed(d);
            }
            catch {
                this.log.error('TCP parse error');
                socket.destroy();
                return;
            } for (const f of frames) {
                this._recv++;
                this._handleTCPFrame(f);
            } });
            socket.on('close', () => { this._setState('disconnected'); this._fullCleanup(); this.events.get('disconnect')?.(undefined); this._tryReconnect(() => this._connectTCP()); });
            socket.on('error', (e) => { this._lastErr = e; this.events.get('error')?.(e); this.opts.hooks.onError?.({ error: e, context: 'tcp' }); });
            socket.on('drain', () => socket.resume());
            this._tcpSock = socket;
        }
        catch (e) {
            this._lastErr = e instanceof Error ? e : new Error(String(e));
            this._tryReconnect(() => this._connectTCP());
        }
    }
    _handleTCPFrame(f) {
        const { type, event, payload } = f;
        if (type === FRAME_PING) {
            if (this._tcpSock && !this._tcpSock.destroyed)
                try {
                    this._tcpSock.write(encodePongFrame());
                }
                catch { }
            return;
        }
        if (type === FRAME_PONG)
            return;
        if (type === FRAME_CONNECT) {
            this.id = payload.toString('utf8');
            return;
        }
        if (type === FRAME_ACK_RES) {
            try {
                const raw = JSON.parse(payload.toString('utf8'));
                const data = raw && typeof raw === 'object' && 'data' in raw ? raw.data : raw;
                const corrId = raw && typeof raw === 'object' && '_correlationId' in raw ? String(raw._correlationId) : undefined;
                const key = corrId || event;
                if (this._acks.has(key)) {
                    const e = this._acks.get(key);
                    if (e.timer)
                        clearTimeout(e.timer);
                    this._acks.delete(key);
                    e.handler(data);
                }
            }
            catch { }
            return;
        }
        if (type === FRAME_JSON) {
            try {
                const data = JSON.parse(payload.toString('utf8'));
                this.opts.hooks.onMessage?.({ event, data, isBinary: false });
                this.events.get(event)?.(data);
                this._wild?.({ event, data });
            }
            catch { }
            return;
        }
        if (type === FRAME_BINARY) {
            const copy = Buffer.from(payload);
            const buf = copy.buffer.slice(copy.byteOffset, copy.byteOffset + copy.byteLength);
            this.opts.hooks.onMessage?.({ event, data: buf, isBinary: true });
            this.events.get(event)?.(buf);
            this._wild?.({ event, data: buf, isBinary: true, buffer: buf });
        }
    }
}
export default StelarClient;
