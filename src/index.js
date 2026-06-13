/**
 * @stelar-time-real Server
 *
 * Dual-protocol real-time server: WebSocket (RFC 6455) + custom binary TCP.
 * Zero external dependencies — uses only Node.js built-in modules.
 */
import { createServer as createHttpServer } from 'http';
import { createServer as createTcpServer } from 'net';
import { randomUUID } from 'crypto';
import { createServer as createTlsServer } from 'tls';
import { FrameParser, encodeJsonFrame, encodeBinaryFrame, encodePingFrame, encodePongFrame, encodeAckResFrame, encodeConnectFrame, encodeDisconnectFrame, encodeErrorFrame, FRAME_JSON, FRAME_BINARY, FRAME_PING, FRAME_PONG, FRAME_ACK_REQ, FRAME_ACK_RES, FRAME_JOIN, FRAME_LEAVE, FRAME_CONNECT, ProtocolError, DEFAULT_MAX_FRAME_SIZE, } from './protocol.js';
import { WSFrameParser, buildUpgradeResponse, validateWSKey, createWSTextFrame, createWSBinaryFrame, createWSCloseFrame, createWSPingFrame, createWSPongFrame, OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG, WebSocketError, CLOSE_POLICY_VIOLATION, CLOSE_MESSAGE_TOO_BIG, CLOSE_NORMAL, CLOSE_GOING_AWAY, DEFAULT_MAX_WS_FRAME_SIZE, } from './websocket.js';
import { Logger, NULL_LOGGER } from './logger.js';
class RateLimiter {
    constructor(maxPoints = 100, windowMs = 1000) {
        this.limits = new Map();
        this.maxPoints = maxPoints;
        this.windowMs = windowMs;
    }
    check(id, cost = 1) {
        const now = Date.now();
        let entry = this.limits.get(id);
        if (!entry || now >= entry.resetTime) {
            entry = { count: 0, resetTime: now + this.windowMs };
            this.limits.set(id, entry);
        }
        if (entry.count + cost > this.maxPoints) {
            return false;
        }
        entry.count += cost;
        return true;
    }
    cleanup() {
        const now = Date.now();
        for (const [id, entry] of this.limits) {
            if (now >= entry.resetTime) {
                this.limits.delete(id);
            }
        }
    }
    reset(id) {
        this.limits.delete(id);
    }
    size() {
        return this.limits.size;
    }
}
class IPConnectionTracker {
    constructor(maxPerIP = 50) {
        this.ipCounts = new Map();
        this.maxPerIP = maxPerIP;
    }
    check(ip) {
        const current = this.ipCounts.get(ip) || 0;
        return current < this.maxPerIP;
    }
    add(ip) {
        this.ipCounts.set(ip, (this.ipCounts.get(ip) || 0) + 1);
    }
    remove(ip) {
        const current = this.ipCounts.get(ip) || 0;
        if (current <= 1) {
            this.ipCounts.delete(ip);
        }
        else {
            this.ipCounts.set(ip, current - 1);
        }
    }
    getCount(ip) {
        return this.ipCounts.get(ip) || 0;
    }
    cleanup() {
        for (const [ip, count] of this.ipCounts) {
            if (count <= 0)
                this.ipCounts.delete(ip);
        }
    }
}
class StelarServer {
    constructor(options = {}) {
        this.httpServer = null;
        this.tcpServer = null;
        this.clients = new Map();
        this.clientsById = new Map();
        this.rooms = new Map(); // room -> Set of client IDs
        this.events = new Map();
        this.middlewares = [];
        this._hbTimer = null;
        this._rateCleanupTimer = null;
        this._wildcardHandler = null;
        this._connectionHandler = null;
        this._acks = new Map();
        this._externalServers = new WeakSet();
        this._upgradeHandler = null;
        this._requestHandler = null;
        this._started = false;
        this._startTime = 0;
        this._shuttingDown = false;
        this._sigintHandler = null;
        this._sigtermHandler = null;
        this._totalConnections = 0;
        this._totalMessagesReceived = 0;
        this._totalMessagesSent = 0;
        this._shutdownCallbacks = [];
        this.port = options.port || 3000;
        this.httpServer = options.server || null;
        this.namespace = options.namespace || '/';
        this.heartbeatInterval = options.heartbeatInterval || 30000;
        this.heartbeatTimeout = options.heartbeatTimeout || this.heartbeatInterval * 2;
        this.tcpPort = options.tcpPort !== undefined ? options.tcpPort : false;
        this.maxConnections = options.maxConnections || 10000;
        this.maxRooms = options.maxRooms || 10000;
        this.maxRoomsPerClient = options.maxRoomsPerClient || 50;
        this.maxPayloadSize = options.maxPayloadSize || 10 * 1024 * 1024; // 10 MB
        this.maxFrameSize = options.maxFrameSize || DEFAULT_MAX_FRAME_SIZE;
        this.maxWSFrameSize = options.maxFrameSize || DEFAULT_MAX_WS_FRAME_SIZE;
        this.connectTimeout = options.connectTimeout || 10000;
        this.doGracefulShutdown = options.gracefulShutdown !== false;
        this.shutdownTimeout = options.shutdownTimeout || 10000;
        this.healthEndpoint = options.healthEndpoint !== undefined ? options.healthEndpoint : '/health';
        this.tlsOptions = options.tls;
        this.allowedOrigins = options.allowedOrigins || null;
        this._customRateLimiter = options.customRateLimiter || null;
        this._customIPTracker = options.customIPTracker || null;
        this._generateClientId = options.generateClientId || null;
        this._customHealthHandler = options.customHealthHandler || null;
        this.hooks = options.hooks || {};
        this.eventRateLimiters = new Map();
        this._clientRateOverrides = new Map();
        if (options.eventRateLimits) {
            for (const [event, config] of Object.entries(options.eventRateLimits)) {
                this.eventRateLimiters.set(event, new RateLimiter(config.maxPoints, config.windowMs));
            }
        }
        if (options.rateLimit === false && !this._customRateLimiter) {
            this.rateLimiter = null;
        }
        else if (!this._customRateLimiter) {
            const rl = options.rateLimit || {};
            this.rateLimiter = new RateLimiter(rl.maxPoints || 100, rl.windowMs || 1000);
        }
        else {
            this.rateLimiter = null;
        }
        if (!this._customIPTracker) {
            this.ipTracker = new IPConnectionTracker(options.maxConnectionsPerIP || 50);
        }
        else {
            this.ipTracker = new IPConnectionTracker(50); // unused when custom tracker is set
        }
        if (options.logger === false) {
            this.log = NULL_LOGGER;
        }
        else if (options.logger instanceof Logger) {
            this.log = options.logger;
        }
        else {
            this.log = new Logger({
                level: options.logger || 'info',
                prefix: 'stelar:server',
            });
        }
    }
    static of(path, options = {}) {
        return new StelarServer({ ...options, namespace: path });
    }
    /** Update server configuration at runtime. */
    updateConfig(options) {
        if (options.maxConnections !== undefined)
            this.maxConnections = options.maxConnections;
        if (options.maxConnectionsPerIP !== undefined && !this._customIPTracker) {
            this.ipTracker = new IPConnectionTracker(options.maxConnectionsPerIP);
        }
        if (options.maxRooms !== undefined)
            this.maxRooms = options.maxRooms;
        if (options.maxRoomsPerClient !== undefined)
            this.maxRoomsPerClient = options.maxRoomsPerClient;
        if (options.maxPayloadSize !== undefined)
            this.maxPayloadSize = options.maxPayloadSize;
        if (options.heartbeatInterval !== undefined)
            this.heartbeatInterval = options.heartbeatInterval;
        if (options.heartbeatTimeout !== undefined)
            this.heartbeatTimeout = options.heartbeatTimeout;
        if (options.allowedOrigins !== undefined)
            this.allowedOrigins = options.allowedOrigins;
        if (options.rateLimit === false) {
            this.rateLimiter = null;
            this._customRateLimiter = null;
        }
        else if (options.rateLimit && !this._customRateLimiter) {
            const rl = options.rateLimit;
            this.rateLimiter = new RateLimiter(rl.maxPoints || 100, rl.windowMs || 1000);
        }
        if (options.customRateLimiter !== undefined) {
            this._customRateLimiter = options.customRateLimiter;
            this.rateLimiter = null;
        }
        if (options.customIPTracker !== undefined) {
            this._customIPTracker = options.customIPTracker;
        }
        if (options.generateClientId !== undefined) {
            this._generateClientId = options.generateClientId;
        }
        if (options.customHealthHandler !== undefined) {
            this._customHealthHandler = options.customHealthHandler;
        }
        if (options.hooks !== undefined) {
            this.hooks = { ...this.hooks, ...options.hooks };
        }
        if (options.eventRateLimits !== undefined) {
            this.eventRateLimiters.clear();
            for (const [event, config] of Object.entries(options.eventRateLimits)) {
                this.eventRateLimiters.set(event, new RateLimiter(config.maxPoints, config.windowMs));
            }
        }
        this.log.info('Server configuration updated');
        return this;
    }
    /** Set a per-client rate limit override. */
    setClientRateLimit(clientId, config) {
        this._clientRateOverrides.set(clientId, new RateLimiter(config.maxPoints, config.windowMs));
        return this;
    }
    /** Remove a per-client rate limit override, falling back to the global limiter. */
    removeClientRateLimit(clientId) {
        this._clientRateOverrides.delete(clientId);
        return this;
    }
    /** Set a per-event rate limit. */
    setEventRateLimit(event, config) {
        this.eventRateLimiters.set(event, new RateLimiter(config.maxPoints, config.windowMs));
        return this;
    }
    /** Remove a per-event rate limit. */
    removeEventRateLimit(event) {
        this.eventRateLimiters.delete(event);
        return this;
    }
    /** Get the current server configuration as a read-only object. */
    getConfig() {
        return Object.freeze({
            maxConnections: this.maxConnections,
            maxConnectionsPerIP: this._customIPTracker ? -1 : this.ipTracker.maxPerIP || 50,
            maxRooms: this.maxRooms,
            maxRoomsPerClient: this.maxRoomsPerClient,
            maxPayloadSize: this.maxPayloadSize,
            heartbeatInterval: this.heartbeatInterval,
            heartbeatTimeout: this.heartbeatTimeout,
            connectTimeout: this.connectTimeout,
            shutdownTimeout: this.shutdownTimeout,
            hasCustomRateLimiter: this._customRateLimiter !== null,
            hasCustomIPTracker: this._customIPTracker !== null,
            hasCustomClientIdGenerator: this._generateClientId !== null,
            hasCustomHealthHandler: this._customHealthHandler !== null,
            eventRateLimits: Array.from(this.eventRateLimiters.keys()),
            hooks: Object.keys(this.hooks),
            allowedOrigins: this.allowedOrigins,
        });
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
    onDisconnect(handler) {
        this.events.set('disconnect', handler);
        return this;
    }
    onAck(name, handler) {
        this._acks.set(name, handler);
        return this;
    }
    broadcast(event, data, excludeId) {
        if (this.hooks.onBeforeBroadcast) {
            const result = this.hooks.onBeforeBroadcast({ event, data, excludeId });
            if (result === false)
                return this;
        }
        let sent = 0;
        this.clients.forEach((record) => {
            if (excludeId && record.info.id === excludeId)
                return;
            if (this._sendJsonToClient(record, event, data))
                sent++;
        });
        this._totalMessagesSent += sent;
        return this;
    }
    broadcastBinary(event, buffer) {
        this.clients.forEach((record) => {
            this._sendBinaryRaw(record, event, buffer);
        });
    }
    to(room, event, data, excludeId) {
        const memberIds = this.rooms.get(room);
        if (!memberIds)
            return this;
        let sent = 0;
        for (const clientId of memberIds) {
            if (excludeId && clientId === excludeId)
                continue;
            const record = this.clientsById.get(clientId);
            if (record && this._sendJsonToClient(record, event, data))
                sent++;
        }
        this._totalMessagesSent += sent;
        return this;
    }
    toId(id, event, data) {
        const record = this.clientsById.get(id);
        if (record && this._sendJsonToClient(record, event, data)) {
            this._totalMessagesSent++;
        }
        return this;
    }
    getClients(room) {
        const list = [];
        this.clients.forEach((record) => {
            if (!room || record.info.rooms.has(room)) {
                list.push({ id: record.info.id, rooms: Array.from(record.info.rooms) });
            }
        });
        return list;
    }
    getRoomMembers(room) {
        const members = this.rooms.get(room);
        return members ? Array.from(members) : [];
    }
    getRooms() {
        return Array.from(this.rooms.keys());
    }
    getPort() {
        const address = this.httpServer?.address();
        if (address && typeof address === 'object') {
            return address.port;
        }
        return this.port;
    }
    getStats() {
        let wsConns = 0;
        let tcpConns = 0;
        this.clients.forEach((r) => {
            if (r.protocol === 'ws')
                wsConns++;
            else
                tcpConns++;
        });
        return {
            totalConnections: this._totalConnections,
            activeConnections: this.clients.size,
            totalMessagesReceived: this._totalMessagesReceived,
            totalMessagesSent: this._totalMessagesSent,
            totalRooms: this.rooms.size,
            uptime: this._startTime ? Date.now() - this._startTime : 0,
            wsConnections: wsConns,
            tcpConnections: tcpConns,
            memoryUsage: process.memoryUsage(),
            rateLimiterEntries: this._getRateLimiterSize(),
        };
    }
    _getRateLimiterSize() {
        if (this._customRateLimiter)
            return this._customRateLimiter.size();
        return this.rateLimiter?.size() || 0;
    }
    /** Check rate limit. Priority: per-client override > event-specific > custom/global. */
    _checkRateLimit(clientId, event) {
        const clientOverride = this._clientRateOverrides.get(clientId);
        if (clientOverride) {
            return clientOverride.check(clientId);
        }
        if (event && this.eventRateLimiters.has(event)) {
            const eventLimiter = this.eventRateLimiters.get(event);
            if (!eventLimiter.check(clientId))
                return false;
        }
        if (this._customRateLimiter) {
            return this._customRateLimiter.check(clientId);
        }
        if (this.rateLimiter) {
            return this.rateLimiter.check(clientId);
        }
        return true;
    }
    _sendJsonToClient(record, event, data) {
        if (record.socket.destroyed || record.socket.writableEnded)
            return false;
        try {
            if (record.protocol === 'ws') {
                const json = JSON.stringify({ event, data });
                record.socket.write(createWSTextFrame(json));
            }
            else {
                record.socket.write(encodeJsonFrame(event, data, this.maxFrameSize));
            }
            record.info.messagesSent++;
            return true;
        }
        catch (err) {
            this.log.error('Send error', { clientId: record.info.id, error: String(err) });
            return false;
        }
    }
    _sendBinaryRaw(record, event, buffer) {
        if (record.socket.destroyed || record.socket.writableEnded)
            return false;
        try {
            if (record.protocol === 'ws') {
                const header = JSON.stringify({ event, _binary: true });
                const headerBytes = Buffer.from(header, 'utf8');
                const combined = Buffer.alloc(headerBytes.length + 1 + buffer.byteLength);
                headerBytes.copy(combined, 0);
                combined[headerBytes.length] = 0;
                combined.set(new Uint8Array(buffer), headerBytes.length + 1);
                record.socket.write(createWSBinaryFrame(combined));
            }
            else {
                record.socket.write(encodeBinaryFrame(event, new Uint8Array(buffer), this.maxFrameSize));
            }
            record.info.messagesSent++;
            return true;
        }
        catch (err) {
            this.log.error('Binary send error', { clientId: record.info.id, error: String(err) });
            return false;
        }
    }
    _joinRoom(record, room) {
        if (this.hooks.onClientJoinRoom) {
            const result = this.hooks.onClientJoinRoom({
                clientId: record.info.id,
                room,
                metadata: record.info.metadata,
            });
            if (result === false) {
                this.log.info('Room join rejected by hook', { clientId: record.info.id, room });
                return;
            }
        }
        if (record.info.rooms.size >= this.maxRoomsPerClient) {
            if (this.hooks.onMaxRoomsPerClientReached) {
                const result = this.hooks.onMaxRoomsPerClientReached({
                    clientId: record.info.id,
                    room,
                    currentRooms: record.info.rooms.size,
                    max: this.maxRoomsPerClient,
                });
                if (result === false)
                    return;
            }
            this.log.warn('Client exceeded max rooms', { clientId: record.info.id, room, max: this.maxRoomsPerClient });
            return;
        }
        if (this.rooms.size >= this.maxRooms && !this.rooms.has(room)) {
            if (this.hooks.onMaxRoomsReached) {
                const result = this.hooks.onMaxRoomsReached({
                    clientId: record.info.id,
                    room,
                    totalRooms: this.rooms.size,
                    max: this.maxRooms,
                });
                if (result === false)
                    return;
            }
            this.log.warn('Server exceeded max rooms', { room, max: this.maxRooms });
            return;
        }
        record.info.rooms.add(room);
        if (!this.rooms.has(room)) {
            this.rooms.set(room, new Set());
        }
        this.rooms.get(room).add(record.info.id);
        this._sendJsonToClient(record, 'joined-room', room);
    }
    _leaveRoom(record, room) {
        if (this.hooks.onClientLeaveRoom) {
            const result = this.hooks.onClientLeaveRoom({
                clientId: record.info.id,
                room,
            });
            if (result === false) {
                this.log.info('Room leave rejected by hook', { clientId: record.info.id, room });
                return;
            }
        }
        record.info.rooms.delete(room);
        const members = this.rooms.get(room);
        if (members) {
            members.delete(record.info.id);
            if (members.size === 0) {
                this.rooms.delete(room);
            }
        }
        this._sendJsonToClient(record, 'left-room', room);
    }
    _removeFromAllRooms(record) {
        for (const room of record.info.rooms) {
            const members = this.rooms.get(room);
            if (members) {
                members.delete(record.info.id);
                if (members.size === 0) {
                    this.rooms.delete(room);
                }
            }
        }
        record.info.rooms.clear();
    }
    _buildCtx(record, req) {
        const self = this;
        const ctx = {
            id: record.info.id,
            socket: record.socket,
            req,
            clientInfo: record.info,
            emit: (evt, d) => { if (self._sendJsonToClient(record, evt, d))
                self._totalMessagesSent++; },
            send: (respId, d) => { if (self._sendJsonToClient(record, respId, { data: d, _isAck: true }))
                self._totalMessagesSent++; },
            emitBinary: (evt, buf) => { if (self._sendBinaryRaw(record, evt, buf))
                self._totalMessagesSent++; },
            broadcast: (evt, d) => self.broadcast(evt, d, record.info.id),
            broadcastBinary: (evt, buf) => self.broadcastBinary(evt, buf),
            to: (room, evt, d) => self.to(room, evt, d, record.info.id),
            toId: (id, evt, d) => self.toId(id, evt, d),
            getClients: (room) => self.getClients(room),
            joinRoom: (room) => self._joinRoom(record, room),
            leaveRoom: (room) => self._leaveRoom(record, room),
            setMetadata: (key, value) => record.info.metadata.set(key, value),
            getMetadata: (key) => record.info.metadata.get(key),
            ack: (ackName, d) => {
                const ackHandler = self._acks.get(ackName);
                if (ackHandler) {
                    const result = ackHandler({ ...ctx, data: d });
                    if (result !== undefined) {
                        try {
                            if (record.protocol === 'ws') {
                                record.socket.write(createWSTextFrame(JSON.stringify({ event: ackName, data: result, _isAck: true })));
                            }
                            else {
                                record.socket.write(encodeAckResFrame(ackName, result, self.maxFrameSize));
                            }
                            self._totalMessagesSent++;
                        }
                        catch (err) {
                            self.log.error('ACK send error', { ackName, error: String(err) });
                        }
                    }
                }
            }
        };
        return ctx;
    }
    runMiddlewares(ctx, next) {
        const run = (i) => {
            if (i >= this.middlewares.length)
                return next();
            try {
                this.middlewares[i](ctx, () => run(i + 1));
            }
            catch (err) {
                this.log.error('Middleware error', { error: String(err), clientId: ctx.id });
                ctx.socket.destroy();
            }
        };
        run(0);
    }
    startHeartbeat() {
        this._hbTimer = setInterval(() => {
            const now = Date.now();
            this.clients.forEach((record) => {
                if (now - record.info.lastPing > this.heartbeatTimeout) {
                    this.log.info('Client heartbeat timeout', { clientId: record.info.id });
                    record.socket.destroy();
                }
                else {
                    try {
                        if (record.protocol === 'ws') {
                            record.socket.write(createWSPingFrame());
                        }
                        else {
                            record.socket.write(encodePingFrame());
                        }
                    }
                    catch {
                        // socket may have closed
                    }
                }
            });
        }, this.heartbeatInterval);
        if (this._hbTimer && typeof this._hbTimer === 'object' && 'unref' in this._hbTimer) {
            this._hbTimer.unref();
        }
    }
    _getClientIP(socket, req) {
        if (req) {
            const forwarded = req.headers['x-forwarded-for'];
            if (typeof forwarded === 'string') {
                return forwarded.split(',')[0].trim();
            }
        }
        return socket.remoteAddress || 'unknown';
    }
    _registerClient(socket, protocol, req, parser) {
        if (this.clients.size >= this.maxConnections) {
            const clientIP = this._getClientIP(socket, req);
            if (this.hooks.onMaxConnectionsReached) {
                this.hooks.onMaxConnectionsReached({
                    activeConnections: this.clients.size,
                    max: this.maxConnections,
                    ip: clientIP,
                });
            }
            this.log.warn('Max connections reached, rejecting', { active: this.clients.size, max: this.maxConnections });
            try {
                if (protocol === 'ws') {
                    socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Server full'));
                }
            }
            catch { /* ignore */ }
            socket.destroy();
            return null;
        }
        const clientIP = this._getClientIP(socket, req);
        const ipTracker = this._customIPTracker || this.ipTracker;
        if (!ipTracker.check(clientIP)) {
            this.log.warn('Max connections per IP reached, rejecting', { ip: clientIP });
            try {
                if (protocol === 'ws') {
                    socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Too many connections from this IP'));
                }
            }
            catch { /* ignore */ }
            socket.destroy();
            return null;
        }
        const clientId = this._generateClientId ? this._generateClientId() : randomUUID();
        const info = {
            id: clientId,
            rooms: new Set(),
            lastPing: Date.now(),
            protocol,
            connectedAt: Date.now(),
            metadata: new Map(),
            messagesReceived: 0,
            messagesSent: 0,
            remoteAddress: clientIP,
        };
        const record = { info, socket, parser, protocol };
        this.clients.set(socket, record);
        this.clientsById.set(clientId, record);
        ipTracker.add(clientIP);
        this._totalConnections++;
        return record;
    }
    _unregisterClient(record, ctx) {
        if (this.hooks.onClientDisconnect) {
            this.hooks.onClientDisconnect({
                clientId: record.info.id,
                ip: record.info.remoteAddress,
                protocol: record.info.protocol,
                rooms: new Set(record.info.rooms),
            });
        }
        this._removeFromAllRooms(record);
        this.clientsById.delete(record.info.id);
        this.clients.delete(record.socket);
        const ipTracker = this._customIPTracker || this.ipTracker;
        ipTracker.remove(record.info.remoteAddress);
        if (this._customRateLimiter) {
            this._customRateLimiter.reset(record.info.id);
        }
        else if (this.rateLimiter) {
            this.rateLimiter.reset(record.info.id);
        }
        this._clientRateOverrides.delete(record.info.id);
        if (this.events.has('disconnect')) {
            const handler = this.events.get('disconnect');
            try {
                handler({ ...ctx, event: 'disconnect' });
            }
            catch (err) {
                this.log.error('Disconnect handler error', { error: String(err) });
            }
        }
    }
    _checkOrigin(req) {
        if (!this.allowedOrigins)
            return true;
        const origin = req.headers['origin'];
        if (!origin)
            return true;
        return this.allowedOrigins.includes(origin);
    }
    handleWSUpgrade(req, socket, head) {
        const urlPath = new URL(req.url || '/', 'http://localhost').pathname;
        const nsPath = this.namespace === '/' ? '/' : this.namespace;
        if (nsPath !== '/' && urlPath !== nsPath) {
            this.log.debug('Rejected WS: wrong namespace', { path: urlPath, expected: nsPath });
            socket.destroy();
            return;
        }
        if (!this._checkOrigin(req)) {
            this.log.warn('Rejected WS: origin not allowed', { origin: req.headers['origin'] });
            socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
            socket.destroy();
            return;
        }
        const key = req.headers['sec-websocket-key'];
        if (!key || !validateWSKey(key)) {
            this.log.warn('Invalid WebSocket key');
            socket.destroy();
            return;
        }
        try {
            const extraHeaders = {};
            const origin = req.headers['origin'];
            if (origin && this.allowedOrigins && this.allowedOrigins.includes(origin)) {
                extraHeaders['Access-Control-Allow-Origin'] = origin;
            }
            socket.write(buildUpgradeResponse(key, extraHeaders));
        }
        catch {
            socket.destroy();
            return;
        }
        const connectTimer = setTimeout(() => {
            if (!this.clients.has(socket)) {
                this.log.warn('WS connect timeout');
                socket.destroy();
            }
        }, this.connectTimeout);
        connectTimer.unref();
        const record = this._registerClient(socket, 'ws', req, new WSFrameParser(this.maxWSFrameSize));
        if (!record) {
            clearTimeout(connectTimer);
            return;
        }
        const ctx = this._buildCtx(record, req);
        if (this.hooks.onClientConnect) {
            this.hooks.onClientConnect({
                clientId: record.info.id,
                ip: record.info.remoteAddress,
                protocol: 'ws',
                metadata: record.info.metadata,
            });
        }
        this.runMiddlewares(ctx, () => {
            if (this._connectionHandler) {
                try {
                    this._connectionHandler(ctx);
                }
                catch (err) {
                    this.log.error('Connection handler error', { error: String(err) });
                }
            }
        });
        this.log.info('WS client connected', { clientId: record.info.id, ip: record.info.remoteAddress });
        if (head.length > 0) {
            this._processWSData(record, head, ctx, req);
        }
        socket.on('data', (data) => {
            clearTimeout(connectTimer);
            this._processWSData(record, data, ctx, req);
        });
        socket.on('close', () => {
            clearTimeout(connectTimer);
            this.log.debug('WS client socket closed', { clientId: record.info.id });
            this._unregisterClient(record, ctx);
        });
        socket.on('error', (err) => {
            this.log.warn('WS client error', { clientId: record.info.id, error: err.message });
            this._handleError(record, ctx, err);
        });
        socket.on('drain', () => {
            socket.resume();
        });
    }
    _processWSData(record, data, ctx, req) {
        let frames;
        try {
            frames = record.parser.feed(data);
        }
        catch (err) {
            if (err instanceof WebSocketError) {
                this.log.warn('WS protocol error', { clientId: record.info.id, code: err.code, message: err.message });
                try {
                    record.socket.write(createWSCloseFrame(err.code, err.message));
                }
                catch { /* ignore */ }
            }
            else {
                this.log.error('WS frame parse error', { clientId: record.info.id, error: String(err) });
            }
            record.socket.destroy();
            return;
        }
        for (const frame of frames) {
            if (record.socket.destroyed)
                break;
            this._handleWSFrame(record, frame, ctx, req);
        }
    }
    _handleWSFrame(record, frame, ctx, _req) {
        const { opcode, payload } = frame;
        if (opcode === OP_PING) {
            try {
                record.socket.write(createWSPongFrame(payload));
            }
            catch { /* ignore */ }
            return;
        }
        if (opcode === OP_CLOSE) {
            try {
                record.socket.write(createWSCloseFrame(CLOSE_NORMAL));
            }
            catch { /* ignore */ }
            record.socket.end();
            return;
        }
        if (opcode === OP_PONG) {
            record.info.lastPing = Date.now();
            return;
        }
        if (!this._checkRateLimit(record.info.id)) {
            this.log.warn('Rate limit exceeded', { clientId: record.info.id });
            if (this.hooks.onRateLimitExceeded) {
                const result = this.hooks.onRateLimitExceeded({
                    clientId: record.info.id,
                    protocol: 'ws',
                });
                if (result === false)
                    return;
            }
            try {
                record.socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Rate limit exceeded'));
            }
            catch { /* ignore */ }
            record.socket.destroy();
            return;
        }
        if (opcode === OP_TEXT) {
            record.info.messagesReceived++;
            this._totalMessagesReceived++;
            if (payload.length > this.maxPayloadSize) {
                if (this.hooks.onPayloadTooLarge) {
                    this.hooks.onPayloadTooLarge({ clientId: record.info.id, size: payload.length, max: this.maxPayloadSize });
                }
                this.log.warn('Payload too large', { clientId: record.info.id, size: payload.length });
                try {
                    record.socket.write(createWSCloseFrame(CLOSE_MESSAGE_TOO_BIG, 'Payload too large'));
                }
                catch { /* ignore */ }
                record.socket.destroy();
                return;
            }
            let msg;
            try {
                msg = JSON.parse(payload.toString('utf8'));
            }
            catch {
                if (this.hooks.onInvalidMessage) {
                    this.hooks.onInvalidMessage({ clientId: record.info.id, reason: 'Invalid JSON', protocol: 'ws' });
                }
                this.log.warn('Invalid JSON from client', { clientId: record.info.id });
                return;
            }
            const event = String(msg.event || '');
            const data = msg.data;
            if (!event)
                return;
            if (event && !this._checkRateLimit(record.info.id, event)) {
                this.log.warn('Event rate limit exceeded', { clientId: record.info.id, event });
                if (this.hooks.onRateLimitExceeded) {
                    const result = this.hooks.onRateLimitExceeded({
                        clientId: record.info.id,
                        event,
                        protocol: 'ws',
                    });
                    if (result === false)
                        return;
                }
                try {
                    record.socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Rate limit exceeded'));
                }
                catch { /* ignore */ }
                record.socket.destroy();
                return;
            }
            if (event === 'pong') {
                record.info.lastPing = Date.now();
                return;
            }
            if (event === 'join-room') {
                const room = String(data);
                if (room)
                    this._joinRoom(record, room);
                return;
            }
            if (event === 'leave-room') {
                const room = String(data);
                if (room)
                    this._leaveRoom(record, room);
                return;
            }
            if (msg._ackName && this._acks.has(String(msg._ackName))) {
                const ackName = String(msg._ackName);
                const ackHandler = this._acks.get(ackName);
                try {
                    const result = ackHandler({ ...ctx, data });
                    if (result !== undefined) {
                        record.socket.write(createWSTextFrame(JSON.stringify({ event: ackName, data: result, _isAck: true })));
                        this._totalMessagesSent++;
                    }
                }
                catch (err) {
                    this.log.error('ACK handler error', { ackName, error: String(err) });
                }
                return;
            }
            const eventCtx = { ...ctx, data, event };
            const handler = this.events.get(event);
            if (handler) {
                try {
                    handler(eventCtx);
                }
                catch (err) {
                    this.log.error('Event handler error', { event, error: String(err) });
                }
            }
            if (this._wildcardHandler) {
                try {
                    this._wildcardHandler({ event, data: eventCtx });
                }
                catch (err) {
                    this.log.error('Wildcard handler error', { event, error: String(err) });
                }
            }
            return;
        }
        if (opcode === OP_BINARY) {
            record.info.messagesReceived++;
            this._totalMessagesReceived++;
            if (payload.length > this.maxPayloadSize) {
                if (this.hooks.onPayloadTooLarge) {
                    this.hooks.onPayloadTooLarge({ clientId: record.info.id, size: payload.length, max: this.maxPayloadSize });
                }
                this.log.warn('Binary payload too large', { clientId: record.info.id, size: payload.length });
                return;
            }
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
                const buffer = payload.subarray(headerEnd + 1);
                if (header.event && !this._checkRateLimit(record.info.id, header.event)) {
                    this.log.warn('Binary event rate limit exceeded', { clientId: record.info.id, event: header.event });
                    if (this.hooks.onRateLimitExceeded) {
                        const result = this.hooks.onRateLimitExceeded({ clientId: record.info.id, event: header.event, protocol: 'ws' });
                        if (result === false)
                            return;
                    }
                    return;
                }
                const eventCtx = { ...ctx, data: buffer, buffer, isBinary: true, event: header.event };
                const handler = this.events.get(header.event);
                if (handler) {
                    try {
                        handler(eventCtx);
                    }
                    catch (err) {
                        this.log.error('Binary handler error', { error: String(err) });
                    }
                }
                if (this._wildcardHandler) {
                    try {
                        this._wildcardHandler({ event: header.event, data: eventCtx });
                    }
                    catch (err) {
                        this.log.error('Wildcard handler error', { error: String(err) });
                    }
                }
            }
            catch {
                if (this.hooks.onInvalidMessage) {
                    this.hooks.onInvalidMessage({ clientId: record.info.id, reason: 'Invalid binary frame', protocol: 'ws' });
                }
                this.log.warn('Invalid binary frame from client', { clientId: record.info.id });
            }
        }
    }
    handleTCPConnection(socket) {
        const record = this._registerClient(socket, 'tcp', null, new FrameParser(this.maxFrameSize));
        if (!record)
            return;
        const ctx = this._buildCtx(record, null);
        try {
            socket.write(encodeConnectFrame(record.info.id));
        }
        catch {
            socket.destroy();
            return;
        }
        if (this.hooks.onClientConnect) {
            this.hooks.onClientConnect({
                clientId: record.info.id,
                ip: record.info.remoteAddress,
                protocol: 'tcp',
                metadata: record.info.metadata,
            });
        }
        this.runMiddlewares(ctx, () => {
            if (this._connectionHandler) {
                try {
                    this._connectionHandler(ctx);
                }
                catch (err) {
                    this.log.error('TCP connection handler error', { error: String(err) });
                }
            }
        });
        this.log.info('TCP client connected', { clientId: record.info.id, ip: record.info.remoteAddress });
        socket.on('data', (data) => {
            this._processTCPData(record, data, ctx);
        });
        socket.on('close', () => {
            this.log.debug('TCP client socket closed', { clientId: record.info.id });
            this._unregisterClient(record, ctx);
        });
        socket.on('error', (err) => {
            this.log.warn('TCP client error', { clientId: record.info.id, error: err.message });
            this._handleError(record, ctx, err);
        });
        socket.on('drain', () => {
            socket.resume();
        });
    }
    _processTCPData(record, data, ctx) {
        let frames;
        try {
            frames = record.parser.feed(data);
        }
        catch (err) {
            if (err instanceof ProtocolError) {
                this.log.warn('TCP protocol error', { clientId: record.info.id, code: err.code, message: err.message });
                try {
                    record.socket.write(encodeErrorFrame(err.message));
                }
                catch { /* ignore */ }
            }
            record.socket.destroy();
            return;
        }
        for (const frame of frames) {
            if (record.socket.destroyed)
                break;
            this._handleTCPFrame(record, frame, ctx);
        }
    }
    _handleTCPFrame(record, frame, ctx) {
        const { type, event, payload } = frame;
        if (type === FRAME_PING) {
            try {
                record.socket.write(encodePongFrame());
            }
            catch { /* ignore */ }
            record.info.lastPing = Date.now();
            return;
        }
        if (type === FRAME_PONG) {
            record.info.lastPing = Date.now();
            return;
        }
        if (!this._checkRateLimit(record.info.id, event)) {
            this.log.warn('TCP rate limit exceeded', { clientId: record.info.id, event });
            if (this.hooks.onRateLimitExceeded) {
                const result = this.hooks.onRateLimitExceeded({
                    clientId: record.info.id,
                    event: event || undefined,
                    protocol: 'tcp',
                });
                if (result === false)
                    return;
            }
            try {
                record.socket.write(encodeErrorFrame('Rate limit exceeded'));
            }
            catch { /* ignore */ }
            record.socket.destroy();
            return;
        }
        if (type === FRAME_JOIN) {
            const room = payload.toString('utf8');
            if (room)
                this._joinRoom(record, room);
            return;
        }
        if (type === FRAME_LEAVE) {
            const room = payload.toString('utf8');
            if (room)
                this._leaveRoom(record, room);
            return;
        }
        if (type === FRAME_CONNECT) {
            return;
        }
        if (payload.length > this.maxPayloadSize) {
            if (this.hooks.onPayloadTooLarge) {
                this.hooks.onPayloadTooLarge({ clientId: record.info.id, event, size: payload.length, max: this.maxPayloadSize });
            }
            this.log.warn('TCP payload too large', { clientId: record.info.id, size: payload.length });
            return;
        }
        record.info.messagesReceived++;
        this._totalMessagesReceived++;
        if (type === FRAME_JSON) {
            let data;
            try {
                data = JSON.parse(payload.toString('utf8'));
            }
            catch {
                if (this.hooks.onInvalidMessage) {
                    this.hooks.onInvalidMessage({ clientId: record.info.id, reason: 'Invalid JSON', protocol: 'tcp' });
                }
                this.log.warn('Invalid TCP JSON', { clientId: record.info.id });
                return;
            }
            const eventCtx = { ...ctx, data, event };
            const handler = this.events.get(event);
            if (handler) {
                try {
                    handler(eventCtx);
                }
                catch (err) {
                    this.log.error('TCP event handler error', { event, error: String(err) });
                }
            }
            if (this._wildcardHandler) {
                try {
                    this._wildcardHandler({ event, data: eventCtx });
                }
                catch (err) {
                    this.log.error('TCP wildcard handler error', { error: String(err) });
                }
            }
            return;
        }
        if (type === FRAME_ACK_REQ) {
            if (this._acks.has(event)) {
                try {
                    const parsed = JSON.parse(payload.toString('utf8'));
                    const data = parsed && typeof parsed === 'object' && 'data' in parsed ? parsed.data : parsed;
                    const ackHandler = this._acks.get(event);
                    const result = ackHandler({ ...ctx, data });
                    if (result !== undefined) {
                        record.socket.write(encodeAckResFrame(event, result, this.maxFrameSize));
                        this._totalMessagesSent++;
                    }
                }
                catch (err) {
                    this.log.error('TCP ACK handler error', { event, error: String(err) });
                }
            }
            return;
        }
        if (type === FRAME_ACK_RES) {
            if (this._acks.has(event)) {
                try {
                    const data = JSON.parse(payload.toString('utf8'));
                    const ackHandler = this._acks.get(event);
                    ackHandler({ ...ctx, data });
                }
                catch { /* ignore */ }
            }
            return;
        }
        if (type === FRAME_BINARY) {
            const eventCtx = { ...ctx, data: payload, buffer: payload, isBinary: true, event };
            const handler = this.events.get(event);
            if (handler) {
                try {
                    handler(eventCtx);
                }
                catch (err) {
                    this.log.error('TCP binary handler error', { event, error: String(err) });
                }
            }
            if (this._wildcardHandler) {
                try {
                    this._wildcardHandler({ event, data: eventCtx });
                }
                catch (err) {
                    this.log.error('TCP wildcard handler error', { error: String(err) });
                }
            }
            return;
        }
    }
    _handleError(record, ctx, err) {
        if (this.events.has('error')) {
            const handler = this.events.get('error');
            try {
                handler({ ...ctx, error: err, event: 'error' });
            }
            catch (handlerErr) {
                this.log.error('Error handler threw', { error: String(handlerErr) });
            }
        }
    }
    _handleHealthCheck(req, res) {
        if (this._customHealthHandler) {
            const stats = this.getStats();
            try {
                this._customHealthHandler(req, res, stats);
            }
            catch (err) {
                this.log.error('Custom health handler error', { error: String(err) });
                if (!res.headersSent) {
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ status: 'error', message: 'Health check handler failed' }));
                }
            }
            return;
        }
        const origin = req.headers['origin'];
        if (origin && (!this.allowedOrigins || this.allowedOrigins.includes(origin))) {
            res.setHeader('Access-Control-Allow-Origin', origin);
            res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
            res.setHeader('Access-Control-Max-Age', '86400');
        }
        if (req.method === 'OPTIONS') {
            res.writeHead(204);
            res.end();
            return;
        }
        if (this.healthEndpoint && req.url === this.healthEndpoint && req.method === 'GET') {
            const stats = this.getStats();
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                status: 'ok',
                ...stats,
                uptimeSeconds: Math.floor(stats.uptime / 1000),
                memoryMB: Math.round(stats.memoryUsage.heapUsed / 1024 / 1024 * 100) / 100,
            }));
            return;
        }
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Stelar Time Real v3 Server');
    }
    /** Register a callback for when graceful shutdown completes. */
    onShutdown(callback) {
        this._shutdownCallbacks.push(callback);
        return this;
    }
    _emitShutdown(signal, force) {
        for (const cb of this._shutdownCallbacks) {
            try {
                cb(signal, force);
            }
            catch { /* ignore */ }
        }
        if (this._shutdownCallbacks.length === 0) {
            process.exit(force ? 1 : 0);
        }
    }
    _setupGracefulShutdown() {
        if (!this.doGracefulShutdown)
            return;
        let isShuttingDown = false;
        const shutdown = (signal) => {
            if (isShuttingDown)
                return;
            isShuttingDown = true;
            this._shuttingDown = true;
            this.log.info(`Received ${signal}, shutting down gracefully...`);
            this.stop();
            const clientCount = this.clients.size;
            if (clientCount === 0) {
                this.log.info('No active connections, shutdown complete');
                this._emitShutdown(signal, false);
                return;
            }
            this.log.info(`Waiting for ${clientCount} connections to close (timeout: ${this.shutdownTimeout}ms)`);
            this.clients.forEach((record) => {
                try {
                    if (record.protocol === 'ws') {
                        record.socket.write(createWSCloseFrame(CLOSE_GOING_AWAY, 'Server shutting down'));
                    }
                    else {
                        record.socket.write(encodeDisconnectFrame());
                    }
                    record.socket.end();
                }
                catch { /* ignore */ }
            });
            const forceTimeout = setTimeout(() => {
                this.log.warn('Shutdown timeout reached, force closing remaining connections');
                this.clients.forEach((record) => {
                    try {
                        record.socket.destroy();
                    }
                    catch { /* ignore */ }
                });
                this.clients.clear();
                this.clientsById.clear();
                this._emitShutdown(signal, true);
            }, this.shutdownTimeout);
            forceTimeout.unref();
            const checkInterval = setInterval(() => {
                if (this.clients.size === 0) {
                    clearInterval(checkInterval);
                    clearTimeout(forceTimeout);
                    this.log.info('All connections closed, shutdown complete');
                    this._emitShutdown(signal, false);
                }
            }, 100);
            checkInterval.unref();
        };
        this._sigintHandler = () => shutdown('SIGINT');
        this._sigtermHandler = () => shutdown('SIGTERM');
        process.on('SIGINT', this._sigintHandler);
        process.on('SIGTERM', this._sigtermHandler);
    }
    _removeSignalHandlers() {
        if (this._sigintHandler) {
            process.off('SIGINT', this._sigintHandler);
            this._sigintHandler = null;
        }
        if (this._sigtermHandler) {
            process.off('SIGTERM', this._sigtermHandler);
            this._sigtermHandler = null;
        }
    }
    start(callback) {
        if (this._started) {
            const port = this.getPort();
            if (callback)
                callback(port);
            return Promise.resolve(port);
        }
        this._started = true;
        this._startTime = Date.now();
        return new Promise((resolve) => {
            const startHttpServer = (httpServer) => {
                this.httpServer = httpServer;
                this._requestHandler = (req, res) => {
                    this._handleHealthCheck(req, res);
                };
                httpServer.on('request', this._requestHandler);
                this._upgradeHandler = (req, socket, head) => {
                    this.handleWSUpgrade(req, socket, head);
                };
                httpServer.on('upgrade', this._upgradeHandler);
                this.startHeartbeat();
                this._rateCleanupTimer = setInterval(() => {
                    if (this._customRateLimiter) {
                        this._customRateLimiter.cleanup();
                    }
                    else if (this.rateLimiter) {
                        this.rateLimiter.cleanup();
                    }
                    const ipTracker = this._customIPTracker || this.ipTracker;
                    ipTracker.cleanup();
                    for (const [clientId, limiter] of this._clientRateOverrides) {
                        limiter.cleanup();
                        if (!this.clientsById.has(clientId)) {
                            this._clientRateOverrides.delete(clientId);
                        }
                    }
                    for (const [, limiter] of this.eventRateLimiters) {
                        limiter.cleanup();
                    }
                }, 30000);
                if (this._rateCleanupTimer && typeof this._rateCleanupTimer === 'object' && 'unref' in this._rateCleanupTimer) {
                    this._rateCleanupTimer.unref();
                }
                this._setupGracefulShutdown();
                const finalPort = this.getPort();
                this.log.info('Server started', { port: finalPort, namespace: this.namespace, tls: !!this.tlsOptions });
                if (callback)
                    callback(finalPort);
                resolve(finalPort);
            };
            if (this.httpServer) {
                this._externalServers.add(this.httpServer);
                startHttpServer(this.httpServer);
            }
            else {
                const tryListen = (port) => {
                    const httpServer = this.tlsOptions
                        ? createHttpServer()
                        : createHttpServer();
                    httpServer.on('error', (err) => {
                        if (err.code === 'EADDRINUSE' && port < 65535) {
                            tryListen(port + 1);
                        }
                        else {
                            this.log.error('HTTP server error', { error: err.message });
                        }
                    });
                    httpServer.listen(port, () => {
                        this.port = port;
                        startHttpServer(httpServer);
                    });
                };
                tryListen(this.port);
            }
            if (this.tcpPort !== false) {
                const tcpPortNum = typeof this.tcpPort === 'number' ? this.tcpPort : this.port + 1;
                this._startTCPServer(tcpPortNum);
            }
        });
    }
    _startTCPServer(port, attempts = 0) {
        const tcpHandler = (socket) => this.handleTCPConnection(socket);
        if (this.tlsOptions) {
            try {
                const tlsServer = createTlsServer(this.tlsOptions, tcpHandler);
                this.tcpServer = tlsServer;
                this.tcpServer.on('error', (err) => {
                    if (err.code === 'EADDRINUSE' && attempts < 10) {
                        this.log.info(`TLS TCP port ${port} in use, trying ${port + 1}`);
                        this.tcpServer = null;
                        this._startTCPServer(port + 1, attempts + 1);
                    }
                    else {
                        this.log.error('TLS TCP server error', { error: err.message });
                    }
                });
                this.tcpServer.listen(port, () => {
                    this.log.info('TLS TCP server started', { port });
                });
            }
            catch (err) {
                this.log.error('Failed to create TLS TCP server', { error: String(err) });
                this._startPlainTCPServer(port, attempts, tcpHandler);
            }
        }
        else {
            this._startPlainTCPServer(port, attempts, tcpHandler);
        }
    }
    _startPlainTCPServer(port, attempts, tcpHandler) {
        this.tcpServer = createTcpServer(tcpHandler);
        this.tcpServer.on('error', (err) => {
            if (err.code === 'EADDRINUSE' && attempts < 10) {
                this.log.info(`TCP port ${port} in use, trying ${port + 1}`);
                this.tcpServer = null;
                this._startTCPServer(port + 1, attempts + 1);
            }
            else {
                this.log.error('TCP server error', { error: err.message });
            }
        });
        this.tcpServer.listen(port, () => {
            this.log.info('TCP server started', { port });
        });
    }
    stop() {
        if (this._hbTimer) {
            clearInterval(this._hbTimer);
            this._hbTimer = null;
        }
        if (this._rateCleanupTimer) {
            clearInterval(this._rateCleanupTimer);
            this._rateCleanupTimer = null;
        }
        this.clients.forEach((record) => {
            if (!record.socket.destroyed) {
                record.socket.destroy();
            }
        });
        this.clients.clear();
        this.clientsById.clear();
        this.rooms.clear();
        this._clientRateOverrides.clear();
        if (this.httpServer) {
            if (this._upgradeHandler) {
                this.httpServer.off('upgrade', this._upgradeHandler);
                this._upgradeHandler = null;
            }
            if (this._requestHandler) {
                this.httpServer.off('request', this._requestHandler);
                this._requestHandler = null;
            }
            if (!this._externalServers.has(this.httpServer)) {
                this.httpServer.close();
            }
            this.httpServer = null;
        }
        if (this.tcpServer) {
            this.tcpServer.close();
            this.tcpServer = null;
        }
        this._started = false;
        this._removeSignalHandlers();
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
