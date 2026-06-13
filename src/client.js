/**
 * @stelar-time-real Client
 *
 * Dual-environment: Browser (native WebSocket) + Node.js (manual WS or TCP binary).
 * No external dependencies.
 */
import { FrameParser, encodeJsonFrame, encodeBinaryFrame, encodeAckReqFrame, encodePingFrame, encodePongFrame, encodeJoinFrame, encodeLeaveFrame, FRAME_JSON, FRAME_BINARY, FRAME_PING, FRAME_PONG, FRAME_ACK_RES, FRAME_CONNECT, validateEventName, DEFAULT_MAX_FRAME_SIZE, } from './protocol.js';
import { WSFrameParser, generateWSKey, createWSTextFrameMasked, createWSBinaryFrameMasked, createWSCloseFrameMasked, createWSPongFrameMasked, OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG, } from './websocket.js';
import { Logger, NULL_LOGGER } from './logger.js';
const isNode = typeof process !== 'undefined' && process.versions?.node != null;
/* Lazy-load Node.js modules so browser builds don't fail */
let _http = null;
let _net = null;
let _tls = null;
let _https = null;
async function loadNodeModules() {
    if (!_http) {
        _http = await import('http');
        _net = await import('net');
        _tls = await import('tls');
        _https = await import('https');
    }
}
class MessageQueue {
    constructor(maxSize = 100) {
        this.queue = [];
        this.maxSize = maxSize;
    }
    push(msg) {
        if (this.queue.length >= this.maxSize) {
            this.queue.shift();
        }
        this.queue.push(msg);
        return true;
    }
    drain() {
        const msgs = this.queue;
        this.queue = [];
        return msgs;
    }
    get length() {
        return this.queue.length;
    }
    clear() {
        this.queue = [];
    }
}
class StelarClient {
    constructor(urlOrPort = 'localhost:3000', options = {}) {
        this.events = new Map();
        this._wildcardHandler = null;
        this._acks = new Map();
        this._state = 'disconnected';
        this._reconnectAttempts = 0;
        this._hbTimer = null;
        this._isManualClose = false;
        this.id = null;
        this._reconnectTimer = null;
        this._messagesSent = 0;
        this._messagesReceived = 0;
        this._connectTime = 0;
        this._lastError = null;
        this._ws = null;
        this._nodeSocket = null;
        this._wsParser = null;
        this._tcpSocket = null;
        this._tcpParser = null;
        if (typeof urlOrPort === 'number') {
            this.url = `ws://localhost:${urlOrPort}`;
        }
        else if (urlOrPort.includes('://')) {
            this.url = urlOrPort.startsWith('http') ? 'ws' + urlOrPort.slice(4) : urlOrPort;
        }
        else {
            this.url = `ws://${urlOrPort}`;
        }
        this.options = {
            reconnection: options.reconnection !== false,
            reconnectionAttempts: options.reconnectionAttempts || 10,
            reconnectionDelay: options.reconnectionDelay || 1000,
            maxReconnectionDelay: options.maxReconnectionDelay || 30000,
            heartbeatInterval: options.heartbeatInterval || 30000,
            ackTimeout: options.ackTimeout || 5000,
            mode: options.mode || 'ws',
            maxPayloadSize: options.maxPayloadSize || 10 * 1024 * 1024,
            maxFrameSize: options.maxFrameSize || DEFAULT_MAX_FRAME_SIZE,
            messageQueueSize: options.messageQueueSize || 100,
            logger: options.logger !== undefined ? options.logger : 'warn',
            tls: options.tls || false,
            rejectUnauthorized: options.rejectUnauthorized !== false,
            headers: options.headers || {},
            customReconnectDelay: options.customReconnectDelay,
            hooks: options.hooks || {},
        };
        this._messageQueue = new MessageQueue(this.options.messageQueueSize);
        if (this.options.logger === false) {
            this.log = NULL_LOGGER;
        }
        else if (this.options.logger instanceof Logger) {
            this.log = this.options.logger;
        }
        else {
            this.log = new Logger({
                level: this.options.logger || 'warn',
                prefix: 'stelar:client',
            });
        }
    }
    getState() { return this._state; }
    getId() { return this.id; }
    getUrl() { return this.url; }
    getMessagesSent() { return this._messagesSent; }
    getMessagesReceived() { return this._messagesReceived; }
    getLastError() { return this._lastError; }
    getQueueSize() { return this._messageQueue.length; }
    getConnectTime() { return this._connectTime; }
    setUrl(url) {
        this.url = url;
        return this;
    }
    /** Update client options at runtime. Changes take effect immediately. */
    updateOptions(options) {
        if (options.reconnection !== undefined)
            this.options.reconnection = options.reconnection;
        if (options.reconnectionAttempts !== undefined)
            this.options.reconnectionAttempts = options.reconnectionAttempts;
        if (options.reconnectionDelay !== undefined)
            this.options.reconnectionDelay = options.reconnectionDelay;
        if (options.maxReconnectionDelay !== undefined)
            this.options.maxReconnectionDelay = options.maxReconnectionDelay;
        if (options.heartbeatInterval !== undefined)
            this.options.heartbeatInterval = options.heartbeatInterval;
        if (options.ackTimeout !== undefined)
            this.options.ackTimeout = options.ackTimeout;
        if (options.maxPayloadSize !== undefined)
            this.options.maxPayloadSize = options.maxPayloadSize;
        if (options.maxFrameSize !== undefined)
            this.options.maxFrameSize = options.maxFrameSize;
        if (options.messageQueueSize !== undefined)
            this.options.messageQueueSize = options.messageQueueSize;
        if (options.headers !== undefined)
            this.options.headers = options.headers;
        if (options.customReconnectDelay !== undefined)
            this.options.customReconnectDelay = options.customReconnectDelay;
        if (options.hooks !== undefined)
            this.options.hooks = { ...this.options.hooks, ...options.hooks };
        return this;
    }
    /** Read-only snapshot of current client options. */
    getOptions() {
        return Object.freeze({
            reconnection: this.options.reconnection,
            reconnectionAttempts: this.options.reconnectionAttempts,
            reconnectionDelay: this.options.reconnectionDelay,
            maxReconnectionDelay: this.options.maxReconnectionDelay,
            heartbeatInterval: this.options.heartbeatInterval,
            ackTimeout: this.options.ackTimeout,
            mode: this.options.mode,
            maxPayloadSize: this.options.maxPayloadSize,
            messageQueueSize: this.options.messageQueueSize,
            hasCustomReconnectDelay: !!this.options.customReconnectDelay,
            hooks: Object.keys(this.options.hooks),
        });
    }
    on(event, handler) {
        this.events.set(event, handler);
        return this;
    }
    off(event, handler) {
        if (this.events.get(event) === handler) {
            this.events.delete(event);
        }
        return this;
    }
    once(event, handler) {
        const wrapped = (data) => {
            this.off(event, wrapped);
            handler(data);
        };
        this.on(event, wrapped);
        return this;
    }
    onAll(handler) {
        this._wildcardHandler = handler;
        return this;
    }
    onAck(name, handler) {
        this._acks.set(name, { handler, timer: null });
        return this;
    }
    removeAllListeners(event) {
        if (event) {
            this.events.delete(event);
        }
        else {
            this.events.clear();
        }
        return this;
    }
    emit(event, data, opts = {}) {
        try {
            if (event)
                validateEventName(event);
        }
        catch {
            this.log.warn('Invalid event name', { event });
            return this;
        }
        if (this.options.hooks.onBeforeEmit) {
            const result = this.options.hooks.onBeforeEmit({ event, data });
            if (result === false)
                return this;
        }
        try {
            const serialized = JSON.stringify(data);
            if (serialized.length > this.options.maxPayloadSize) {
                this.log.warn('Payload exceeds max size', { event, size: serialized.length });
                return this;
            }
        }
        catch {
            this.log.warn('Failed to serialize data', { event });
            return this;
        }
        if (this._state !== 'connected') {
            if (this.options.reconnection) {
                this._messageQueue.push({ event, data, opts, timestamp: Date.now() });
                this.log.debug('Message queued', { event, queueSize: this._messageQueue.length });
                if (this.options.hooks.onMessageQueued) {
                    this.options.hooks.onMessageQueued({ event, data, queueSize: this._messageQueue.length });
                }
            }
            return this;
        }
        try {
            if (this.options.mode === 'tcp' && this._tcpSocket && !this._tcpSocket.destroyed) {
                if (opts.ack) {
                    this._tcpSocket.write(encodeAckReqFrame(opts.ack, { event, data }, this.options.maxFrameSize));
                }
                else {
                    this._tcpSocket.write(encodeJsonFrame(event, data, this.options.maxFrameSize));
                }
            }
            else if (this._nodeSocket && !this._nodeSocket.destroyed) {
                const payload = { event, data };
                if (opts.ack)
                    payload._ackName = opts.ack;
                this._nodeSocket.write(createWSTextFrameMasked(JSON.stringify(payload)));
            }
            else if (this._ws && this._ws.readyState === WebSocket.OPEN) {
                const payload = { event, data };
                if (opts.ack)
                    payload._ackName = opts.ack;
                this._ws.send(JSON.stringify(payload));
            }
            else {
                this._messageQueue.push({ event, data, opts, timestamp: Date.now() });
                return this;
            }
            this._messagesSent++;
        }
        catch (err) {
            this.log.error('Emit error', { event, error: String(err) });
            if (this.options.hooks.onError) {
                this.options.hooks.onError({ error: err instanceof Error ? err : new Error(String(err)), context: 'emit' });
            }
            this._messageQueue.push({ event, data, opts, timestamp: Date.now() });
        }
        return this;
    }
    emitBinary(event, data) {
        if (data.byteLength > this.options.maxPayloadSize) {
            this.log.warn('Binary payload exceeds max size', { event, size: data.byteLength });
            return this;
        }
        if (this.options.hooks.onBeforeEmit) {
            const result = this.options.hooks.onBeforeEmit({ event, data });
            if (result === false)
                return this;
        }
        if (this._state !== 'connected')
            return this;
        try {
            if (this.options.mode === 'tcp' && this._tcpSocket && !this._tcpSocket.destroyed) {
                this._tcpSocket.write(encodeBinaryFrame(event, new Uint8Array(data), this.options.maxFrameSize));
            }
            else if (this._nodeSocket && !this._nodeSocket.destroyed) {
                const header = JSON.stringify({ event });
                const headerBytes = Buffer.from(header, 'utf8');
                const combined = Buffer.alloc(headerBytes.length + 1 + data.byteLength);
                headerBytes.copy(combined, 0);
                combined[headerBytes.length] = 0;
                combined.set(new Uint8Array(data), headerBytes.length + 1);
                this._nodeSocket.write(createWSBinaryFrameMasked(combined));
            }
            else if (this._ws && this._ws.readyState === WebSocket.OPEN) {
                const header = JSON.stringify({ event });
                const headerBytes = new TextEncoder().encode(header);
                const combined = new Uint8Array(headerBytes.length + 1 + data.byteLength);
                combined.set(headerBytes, 0);
                combined[headerBytes.length] = 0;
                combined.set(new Uint8Array(data), headerBytes.length + 1);
                this._ws.send(combined);
            }
            this._messagesSent++;
        }
        catch (err) {
            this.log.error('Binary emit error', { event, error: String(err) });
            if (this.options.hooks.onError) {
                this.options.hooks.onError({ error: err instanceof Error ? err : new Error(String(err)), context: 'emitBinary' });
            }
        }
        return this;
    }
    sendFile(file) {
        return this.emitBinary('file', file);
    }
    sendImage(blob) {
        return this.emitBinary('image', blob);
    }
    /** Send a request and wait for an ACK response. Rejects on timeout. */
    request(event, data, ackName) {
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                this._acks.delete(ackName);
                reject(new Error(`ACK '${ackName}' timeout after ${this.options.ackTimeout}ms`));
            }, this.options.ackTimeout);
            timeout.unref();
            const handler = (responseData) => {
                clearTimeout(timeout);
                this._acks.delete(ackName);
                resolve(responseData);
            };
            this._acks.set(ackName, { handler, timer: timeout });
            this.emit(event, data, { ack: ackName });
        });
    }
    joinRoom(room) {
        if (this.options.mode === 'tcp' && this._tcpSocket && !this._tcpSocket.destroyed) {
            try {
                this._tcpSocket.write(encodeJoinFrame(room, this.options.maxFrameSize));
            }
            catch { }
        }
        else {
            this.emit('join-room', room);
        }
        return this;
    }
    leaveRoom(room) {
        if (this.options.mode === 'tcp' && this._tcpSocket && !this._tcpSocket.destroyed) {
            try {
                this._tcpSocket.write(encodeLeaveFrame(room));
            }
            catch { }
        }
        else {
            this.emit('leave-room', room);
        }
        return this;
    }
    startHeartbeat() {
        this.stopHeartbeat();
        this._hbTimer = setInterval(() => {
            if (this.options.mode === 'tcp' && this._tcpSocket && !this._tcpSocket.destroyed) {
                try {
                    this._tcpSocket.write(encodePingFrame());
                }
                catch { }
            }
            else if (this._nodeSocket && !this._nodeSocket.destroyed) {
                try {
                    this._nodeSocket.write(createWSTextFrameMasked(JSON.stringify({ event: 'pong', data: Date.now() })));
                }
                catch { }
            }
            else if (this._ws && this._ws.readyState === WebSocket.OPEN) {
                this._ws.send(JSON.stringify({ event: 'pong', data: Date.now() }));
            }
        }, this.options.heartbeatInterval);
        if (this._hbTimer && typeof this._hbTimer === 'object' && 'unref' in this._hbTimer) {
            this._hbTimer.unref();
        }
    }
    stopHeartbeat() {
        if (this._hbTimer) {
            clearInterval(this._hbTimer);
            this._hbTimer = null;
        }
    }
    _getReconnectDelay() {
        const base = this.options.reconnectionDelay;
        const max = this.options.maxReconnectionDelay;
        if (this.options.hooks.onReconnectDelay) {
            const defaultDelay = Math.min(base * Math.pow(1.5, this._reconnectAttempts - 1), max);
            const customDelay = this.options.hooks.onReconnectDelay({ attempt: this._reconnectAttempts, defaultDelay });
            if (typeof customDelay === 'number')
                return customDelay;
        }
        if (this.options.customReconnectDelay) {
            return this.options.customReconnectDelay(this._reconnectAttempts, base, max);
        }
        const delay = Math.min(base * Math.pow(1.5, this._reconnectAttempts - 1), max);
        const jitter = delay * 0.2 * Math.random();
        return Math.floor(delay + jitter);
    }
    _drainQueue() {
        if (this._messageQueue.length === 0)
            return;
        const messages = this._messageQueue.drain();
        this.log.info('Draining message queue', { count: messages.length });
        for (const msg of messages) {
            try {
                if (this.options.mode === 'tcp' && this._tcpSocket && !this._tcpSocket.destroyed) {
                    if (msg.opts.ack) {
                        this._tcpSocket.write(encodeAckReqFrame(msg.opts.ack, { event: msg.event, data: msg.data }, this.options.maxFrameSize));
                    }
                    else {
                        this._tcpSocket.write(encodeJsonFrame(msg.event, msg.data, this.options.maxFrameSize));
                    }
                }
                else if (this._nodeSocket && !this._nodeSocket.destroyed) {
                    const payload = { event: msg.event, data: msg.data };
                    if (msg.opts.ack)
                        payload._ackName = msg.opts.ack;
                    this._nodeSocket.write(createWSTextFrameMasked(JSON.stringify(payload)));
                }
                else if (this._ws && this._ws.readyState === WebSocket.OPEN) {
                    const payload = { event: msg.event, data: msg.data };
                    if (msg.opts.ack)
                        payload._ackName = msg.opts.ack;
                    this._ws.send(JSON.stringify(payload));
                }
                this._messagesSent++;
            }
            catch (err) {
                this.log.error('Queue drain error', { event: msg.event, error: String(err) });
            }
        }
        if (this.options.hooks.onQueueDrained) {
            this.options.hooks.onQueueDrained({ count: messages.length });
        }
    }
    _setState(state) {
        const prev = this._state;
        this._state = state;
        if (prev !== state) {
            this.log.debug('State changed', { from: prev, to: state });
            if (this.options.hooks.onStateChange) {
                this.options.hooks.onStateChange({ from: prev, to: state });
            }
        }
    }
    _cleanupAcks() {
        for (const [, entry] of this._acks) {
            if (entry.timer)
                clearTimeout(entry.timer);
        }
        this._acks.clear();
    }
    _fullCleanup() {
        this.stopHeartbeat();
        this._cleanupAcks();
        if (this._reconnectTimer) {
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;
        }
        this._nodeSocket = null;
        this._wsParser = null;
        this._tcpSocket = null;
        this._tcpParser = null;
        this._ws = null;
    }
    connect(callback) {
        if (this._state === 'connected' || this._state === 'connecting') {
            return this;
        }
        if (this._state === 'reconnecting' && this._reconnectTimer) {
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;
        }
        this._isManualClose = false;
        this._setState('connecting');
        if (this.options.mode === 'tcp' && isNode) {
            this._connectTCP();
        }
        else if (isNode) {
            this._connectNodeWS();
        }
        else {
            this._connectBrowser();
        }
        if (callback) {
            const check = setInterval(() => {
                if (this._state === 'connected') {
                    clearInterval(check);
                    callback();
                }
            }, 50);
            const safetyTimeout = setTimeout(() => clearInterval(check), this.options.ackTimeout);
            safetyTimeout.unref();
        }
        return this;
    }
    disconnect() {
        this._isManualClose = true;
        if (this._tcpSocket && !this._tcpSocket.destroyed) {
            try {
                this._tcpSocket.destroy();
            }
            catch { }
        }
        if (this._nodeSocket && !this._nodeSocket.destroyed) {
            try {
                this._nodeSocket.write(createWSCloseFrameMasked());
            }
            catch { }
            try {
                this._nodeSocket.end();
            }
            catch { }
        }
        if (this._ws) {
            try {
                this._ws.close();
            }
            catch { }
        }
        this._fullCleanup();
        this._setState('disconnected');
        return this;
    }
    isConnected() {
        return this._state === 'connected';
    }
    _connectBrowser() {
        try {
            const ws = new WebSocket(this.url);
            ws.binaryType = 'arraybuffer';
            ws.onopen = () => {
                this._setState('connected');
                this._reconnectAttempts = 0;
                this._connectTime = Date.now();
                this.log.info('Browser WS connected');
                const handler = this.events.get('connect');
                if (handler)
                    handler(undefined);
                this.startHeartbeat();
                this._drainQueue();
            };
            ws.onmessage = (e) => {
                this._messagesReceived++;
                this._handleBrowserMessage(e);
            };
            ws.onclose = (e) => {
                this._setState('disconnected');
                this._fullCleanup();
                const handler = this.events.get('disconnect');
                if (handler)
                    handler({ code: e.code, reason: e.reason });
                this._tryReconnect(() => this._connectBrowser());
            };
            ws.onerror = () => {
                this._lastError = new Error('WebSocket error');
                const handler = this.events.get('error');
                if (handler)
                    handler(this._lastError);
                if (this.options.hooks.onError) {
                    this.options.hooks.onError({ error: this._lastError, context: 'browser-ws' });
                }
            };
            this._ws = ws;
        }
        catch (err) {
            this._lastError = err instanceof Error ? err : new Error(String(err));
            this._setState('disconnected');
            this._tryReconnect(() => this._connectBrowser());
        }
    }
    _handleBrowserMessage(e) {
        try {
            if (e.data instanceof ArrayBuffer) {
                const view = new Uint8Array(e.data);
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
                const buffer = view.slice(headerEnd + 1).buffer;
                if (this.options.hooks.onMessage) {
                    this.options.hooks.onMessage({ event: header.event, data: buffer, isBinary: true });
                }
                const handler = this.events.get(header.event);
                if (handler)
                    handler(buffer);
                if (this._wildcardHandler) {
                    this._wildcardHandler({ event: header.event, data: buffer, isBinary: true, buffer });
                }
                return;
            }
            const msg = JSON.parse(e.data);
            const { event, data, _isAck } = msg;
            if (event === 'ping')
                return;
            if (this.options.hooks.onMessage) {
                this.options.hooks.onMessage({ event, data, isBinary: false });
            }
            if (_isAck && this._acks.has(event)) {
                const entry = this._acks.get(event);
                if (entry.timer)
                    clearTimeout(entry.timer);
                this._acks.delete(event);
                entry.handler(data);
                return;
            }
            const handler = this.events.get(event);
            if (handler)
                handler(data);
            if (this._wildcardHandler)
                this._wildcardHandler({ event, data });
        }
        catch { }
    }
    async _connectNodeWS() {
        try {
            await loadNodeModules();
            if (!_http)
                return;
            const parsed = new URL(this.url);
            const isSecure = parsed.protocol === 'wss:' || this.options.tls;
            const key = generateWSKey();
            const requestHeaders = {
                'Upgrade': 'websocket',
                'Connection': 'Upgrade',
                'Sec-WebSocket-Key': key,
                'Sec-WebSocket-Version': '13',
                ...this.options.headers,
            };
            const reqModule = isSecure && _https ? _https : _http;
            const req = reqModule.request({
                hostname: parsed.hostname,
                port: parseInt(parsed.port) || (isSecure ? 443 : 80),
                path: parsed.pathname + parsed.search,
                method: 'GET',
                headers: requestHeaders,
                rejectUnauthorized: this.options.rejectUnauthorized,
            });
            req.setTimeout(this.options.ackTimeout, () => {
                req.destroy(new Error('Connection timeout'));
            });
            req.on('upgrade', (_res, socket, head) => {
                this._nodeSocket = socket;
                this._wsParser = new WSFrameParser(this.options.maxFrameSize);
                if (head.length > 0) {
                    this._processNodeWSData(head);
                }
                socket.on('data', (data) => {
                    this._processNodeWSData(data);
                });
                socket.on('close', () => {
                    this._setState('disconnected');
                    this._fullCleanup();
                    const handler = this.events.get('disconnect');
                    if (handler)
                        handler(undefined);
                    this._tryReconnect(() => this._connectNodeWS());
                });
                socket.on('error', (err) => {
                    this._lastError = err;
                    const handler = this.events.get('error');
                    if (handler)
                        handler(err);
                    if (this.options.hooks.onError) {
                        this.options.hooks.onError({ error: err, context: 'node-ws' });
                    }
                });
                socket.on('drain', () => {
                    socket.resume();
                });
                this._setState('connected');
                this._reconnectAttempts = 0;
                this._connectTime = Date.now();
                this.log.info('Node.js WS connected', { secure: isSecure });
                const connectHandler = this.events.get('connect');
                if (connectHandler)
                    connectHandler(undefined);
                this.startHeartbeat();
                this._drainQueue();
            });
            req.on('error', (err) => {
                this._lastError = err;
                const handler = this.events.get('error');
                if (handler)
                    handler(err);
                this._tryReconnect(() => this._connectNodeWS());
            });
            req.end();
        }
        catch (err) {
            this._lastError = err instanceof Error ? err : new Error(String(err));
            this._tryReconnect(() => this._connectNodeWS());
        }
    }
    _processNodeWSData(data) {
        if (!this._wsParser)
            return;
        let frames;
        try {
            frames = this._wsParser.feed(data);
        }
        catch {
            this.log.error('WS frame parse error');
            return;
        }
        for (const frame of frames) {
            this._messagesReceived++;
            this._handleNodeWSFrame(frame);
        }
    }
    _handleNodeWSFrame(frame) {
        const { opcode, payload } = frame;
        if (opcode === OP_PING) {
            if (this._nodeSocket && !this._nodeSocket.destroyed) {
                try {
                    this._nodeSocket.write(createWSPongFrameMasked());
                }
                catch { }
            }
            return;
        }
        if (opcode === OP_PONG)
            return;
        if (opcode === OP_CLOSE) {
            if (this._nodeSocket && !this._nodeSocket.destroyed) {
                try {
                    this._nodeSocket.end();
                }
                catch { }
            }
            return;
        }
        if (opcode === OP_TEXT) {
            try {
                const msg = JSON.parse(payload.toString('utf8'));
                const { event, data, _isAck } = msg;
                if (event === 'ping')
                    return;
                if (this.options.hooks.onMessage) {
                    this.options.hooks.onMessage({ event, data, isBinary: false });
                }
                if (_isAck && this._acks.has(event)) {
                    const entry = this._acks.get(event);
                    if (entry.timer)
                        clearTimeout(entry.timer);
                    this._acks.delete(event);
                    entry.handler(data);
                    return;
                }
                const handler = this.events.get(event);
                if (handler)
                    handler(data);
                if (this._wildcardHandler)
                    this._wildcardHandler({ event, data });
            }
            catch { }
            return;
        }
        if (opcode === OP_BINARY) {
            try {
                let headerEnd = -1;
                for (let i = 0; i < payload.length; i++) {
                    if (payload[i] === 0) {
                        headerEnd = i;
                        break;
                    }
                }
                if (headerEnd === -1)
                    return;
                const headerStr = payload.subarray(0, headerEnd).toString('utf8');
                const header = JSON.parse(headerStr);
                const buffer = payload.subarray(headerEnd + 1).buffer;
                if (this.options.hooks.onMessage) {
                    this.options.hooks.onMessage({ event: header.event, data: buffer, isBinary: true });
                }
                const handler = this.events.get(header.event);
                if (handler)
                    handler(buffer);
                if (this._wildcardHandler)
                    this._wildcardHandler({ event: header.event, data: buffer, isBinary: true, buffer });
            }
            catch { }
        }
    }
    /** TCP mode: connects to WS port + 1 by convention. */
    async _connectTCP() {
        try {
            await loadNodeModules();
            if (!_net)
                return;
            const parsed = new URL(this.url);
            const port = parseInt(parsed.port) + 1 || 3001;
            const host = parsed.hostname || 'localhost';
            const socketOptions = { host, port };
            let socket;
            if (this.options.tls && _tls) {
                socketOptions.rejectUnauthorized = this.options.rejectUnauthorized;
                socket = _tls.connect(socketOptions);
            }
            else {
                socket = _net.createConnection(socketOptions);
            }
            socket.setTimeout(this.options.ackTimeout, () => {
                socket.destroy(new Error('TCP connection timeout'));
            });
            socket.on('connect', () => {
                socket.setTimeout(0);
                this._setState('connected');
                this._reconnectAttempts = 0;
                this._connectTime = Date.now();
                this._tcpParser = new FrameParser(this.options.maxFrameSize);
                this.log.info('TCP connected', { host, port, tls: this.options.tls });
                const handler = this.events.get('connect');
                if (handler)
                    handler(undefined);
                this.startHeartbeat();
                this._drainQueue();
            });
            socket.on('data', (data) => {
                if (!this._tcpParser)
                    return;
                let frames;
                try {
                    frames = this._tcpParser.feed(data);
                }
                catch {
                    this.log.error('TCP frame parse error');
                    socket.destroy();
                    return;
                }
                for (const frame of frames) {
                    this._messagesReceived++;
                    this._handleTCPFrame(frame);
                }
            });
            socket.on('close', () => {
                this._setState('disconnected');
                this._fullCleanup();
                const handler = this.events.get('disconnect');
                if (handler)
                    handler(undefined);
                this._tryReconnect(() => this._connectTCP());
            });
            socket.on('error', (err) => {
                this._lastError = err;
                const handler = this.events.get('error');
                if (handler)
                    handler(err);
                if (this.options.hooks.onError) {
                    this.options.hooks.onError({ error: err, context: 'tcp' });
                }
            });
            socket.on('drain', () => {
                socket.resume();
            });
            this._tcpSocket = socket;
        }
        catch (err) {
            this._lastError = err instanceof Error ? err : new Error(String(err));
            this._tryReconnect(() => this._connectTCP());
        }
    }
    _handleTCPFrame(frame) {
        const { type, event, payload } = frame;
        if (type === FRAME_PING) {
            if (this._tcpSocket && !this._tcpSocket.destroyed) {
                try {
                    this._tcpSocket.write(encodePongFrame());
                }
                catch { }
            }
            return;
        }
        if (type === FRAME_PONG)
            return;
        if (type === FRAME_CONNECT) {
            this.id = payload.toString('utf8');
            return;
        }
        if (type === FRAME_ACK_RES) {
            if (this._acks.has(event)) {
                try {
                    const data = JSON.parse(payload.toString('utf8'));
                    const entry = this._acks.get(event);
                    if (entry.timer)
                        clearTimeout(entry.timer);
                    this._acks.delete(event);
                    entry.handler(data);
                }
                catch { }
            }
            return;
        }
        if (type === FRAME_JSON) {
            try {
                const data = JSON.parse(payload.toString('utf8'));
                if (this.options.hooks.onMessage) {
                    this.options.hooks.onMessage({ event, data, isBinary: false });
                }
                const handler = this.events.get(event);
                if (handler)
                    handler(data);
                if (this._wildcardHandler)
                    this._wildcardHandler({ event, data });
            }
            catch { }
            return;
        }
        if (type === FRAME_BINARY) {
            const copy = Buffer.from(payload);
            const buffer = copy.buffer.slice(copy.byteOffset, copy.byteOffset + copy.byteLength);
            if (this.options.hooks.onMessage) {
                this.options.hooks.onMessage({ event, data: buffer, isBinary: true });
            }
            const handler = this.events.get(event);
            if (handler)
                handler(buffer);
            if (this._wildcardHandler)
                this._wildcardHandler({ event, data: buffer, isBinary: true, buffer });
        }
    }
    _tryReconnect(connectFn) {
        if (this._isManualClose)
            return;
        if (!this.options.reconnection)
            return;
        if (this._reconnectAttempts >= this.options.reconnectionAttempts) {
            this.log.warn('Max reconnection attempts reached', { attempts: this._reconnectAttempts });
            const handler = this.events.get('reconnect_failed');
            if (handler)
                handler(undefined);
            return;
        }
        this._reconnectAttempts++;
        this._setState('reconnecting');
        const delay = this._getReconnectDelay();
        this.log.info('Reconnecting', { attempt: this._reconnectAttempts, delay });
        const handler = this.events.get('reconnecting');
        if (handler)
            handler(this._reconnectAttempts);
        this._reconnectTimer = setTimeout(() => {
            this._reconnectTimer = null;
            if (!this._isManualClose) {
                connectFn();
            }
        }, delay);
    }
}
export default StelarClient;
