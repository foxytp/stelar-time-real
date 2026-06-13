/** @stelar-time-real Server — Dual-protocol: WebSocket (RFC 6455) + binary TCP */

import { createServer as createHttp, IncomingMessage, Server as HttpServer, ServerResponse } from 'http';
import { createServer as createTcp, Server as TcpServer, Socket as NetSocket } from 'net';
import { randomUUID } from 'crypto';
import { createServer as createTls, TlsOptions } from 'tls';

import {
  FrameParser, ParsedFrame, encodeJsonFrame, encodeBinaryFrame, encodePingFrame, encodePongFrame,
  encodeAckResFrame, encodeConnectFrame, encodeDisconnectFrame, encodeJoinFrame, encodeLeaveFrame,
  encodeErrorFrame, FRAME_JSON, FRAME_BINARY, FRAME_PING, FRAME_PONG, FRAME_ACK_REQ,
  FRAME_JOIN, FRAME_LEAVE, FRAME_CONNECT, ProtocolError, DEFAULT_MAX_FRAME_SIZE,
} from './protocol.js';

import {
  WSFrameParser, WSFrame, buildUpgradeResponse, validateWSKey, createWSTextFrame,
  createWSBinaryFrame, createWSCloseFrame, createWSPingFrame, createWSPongFrame,
  OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG, WebSocketError,
  CLOSE_PROTOCOL_ERROR, CLOSE_POLICY_VIOLATION, CLOSE_MESSAGE_TOO_BIG, CLOSE_NORMAL, CLOSE_GOING_AWAY,
  DEFAULT_MAX_WS_FRAME_SIZE, clientWantsCompression,
} from './websocket.js';

import { Logger, NULL_LOGGER, type LogLevel } from './logger.js';

/* ── Interfaces ── */

export interface IRateLimiter { check(id: string, cost?: number): boolean; reset(id: string): void; cleanup(): void; size(): number; }
export interface IIPTracker { check(ip: string): boolean; add(ip: string): void; remove(ip: string): void; getCount(ip: string): number; cleanup(): void; }

export interface StelarHooks {
  onRateLimitExceeded?: (i: { clientId: string; event?: string; protocol: 'ws' | 'tcp' }) => boolean | void;
  onMaxConnectionsReached?: (i: { activeConnections: number; max: number; ip: string }) => void;
  onMaxRoomsReached?: (i: { clientId: string; room: string; totalRooms: number; max: number }) => boolean | void;
  onMaxRoomsPerClientReached?: (i: { clientId: string; room: string; currentRooms: number; max: number }) => boolean | void;
  onPayloadTooLarge?: (i: { clientId: string; event?: string; size: number; max: number }) => void;
  onInvalidMessage?: (i: { clientId: string; reason: string; protocol: 'ws' | 'tcp' }) => void;
  onClientJoinRoom?: (i: { clientId: string; room: string; metadata: Map<string, unknown> }) => boolean | void;
  onClientLeaveRoom?: (i: { clientId: string; room: string }) => boolean | void;
  onBeforeBroadcast?: (i: { event: string; data: unknown; excludeId?: string }) => boolean | void;
  onClientConnect?: (i: { clientId: string; ip: string; protocol: 'ws' | 'tcp'; metadata: Map<string, unknown> }) => void;
  onClientDisconnect?: (i: { clientId: string; ip: string; protocol: 'ws' | 'tcp'; rooms: Set<string> }) => void;
}

export type EventRateLimits = Record<string, { maxPoints: number; windowMs: number }>;

export interface StelarOptions {
  port?: number; server?: HttpServer; namespace?: string;
  heartbeatInterval?: number; heartbeatTimeout?: number; tcpPort?: number | false;
  maxConnections?: number; maxConnectionsPerIP?: number; maxRooms?: number;
  maxRoomsPerClient?: number; maxPayloadSize?: number; maxFrameSize?: number;
  rateLimit?: { maxPoints?: number; windowMs?: number } | false; connectTimeout?: number;
  gracefulShutdown?: boolean; shutdownTimeout?: number; healthEndpoint?: string | false;
  logger?: Logger | LogLevel | false; tls?: TlsOptions; allowedOrigins?: string[];
  customRateLimiter?: IRateLimiter; customIPTracker?: IIPTracker;
  generateClientId?: () => string; eventRateLimits?: EventRateLimits;
  hooks?: StelarHooks; customHealthHandler?: (req: IncomingMessage, res: ServerResponse, stats: StelarStats) => void;
  compression?: boolean;
}

export interface StelarClientInfo {
  id: string; rooms: Set<string>; lastPing: number; protocol: 'ws' | 'tcp';
  connectedAt: number; metadata: Map<string, unknown>; messagesReceived: number;
  messagesSent: number; remoteAddress: string;
}

export interface StelarContext {
  id: string; socket: NetSocket; req: IncomingMessage | null; data?: unknown;
  buffer?: Uint8Array; isBinary?: boolean; event?: string; error?: Error;
  _correlationId?: string; clientInfo: StelarClientInfo;
  emit: (event: string, data: unknown) => void;
  send: (respId: string, data: unknown) => void;
  emitBinary: (event: string, buffer: ArrayBuffer) => void;
  broadcast: (event: string, data: unknown) => void;
  broadcastBinary: (event: string, buffer: ArrayBuffer) => void;
  to: (room: string, event: string, data: unknown) => void;
  toId: (id: string, event: string, data: unknown) => void;
  getClients: (room?: string) => { id: string; rooms: string[] }[];
  joinRoom: (room: string) => void; leaveRoom: (room: string) => void;
  setMetadata: (key: string, value: unknown) => void; getMetadata: (key: string) => unknown;
  ack: (ackName: string, data: unknown) => void;
}

export interface StelarMiddleware { (ctx: StelarContext, next: () => void): void; }
export type StelarEventHandler = (ctx: StelarContext) => void;
export type StelarWildcardHandler = (data: { event: string; data: StelarContext }) => void;

export interface StelarStats {
  totalConnections: number; activeConnections: number;
  totalMessagesReceived: number; totalMessagesSent: number;
  totalRooms: number; uptime: number; wsConnections: number;
  tcpConnections: number; memoryUsage: NodeJS.MemoryUsage; rateLimiterEntries: number;
}

/* ── Internal ── */

interface ClientRecord {
  info: StelarClientInfo; socket: NetSocket; parser: WSFrameParser | FrameParser;
  protocol: 'ws' | 'tcp'; compress: boolean;
  _hbTimer: ReturnType<typeof setInterval> | null;
  _writePaused: boolean; _writeQueue: Buffer[];
}

/** WS binary framing: [4B headerLen BE][header JSON][binary payload] — length-prefixed, not null-delimited */
function encodeWSBinary(event: string, data: Uint8Array | Buffer): Buffer {
  const hdr = Buffer.from(JSON.stringify({ event, _binary: true }), 'utf8');
  const payload = Buffer.from(data);
  const frame = Buffer.alloc(4 + hdr.length + payload.length);
  frame.writeUInt32BE(hdr.length, 0);
  hdr.copy(frame, 4);
  payload.copy(frame, 4 + hdr.length);
  return frame;
}

function parseWSBinary(payload: Buffer): { event: string; buffer: Buffer } | null {
  if (payload.length < 4) return null;
  const hdrLen = payload.readUInt32BE(0);
  if (hdrLen > payload.length - 4) return null;
  try {
    const hdr = JSON.parse(payload.subarray(4, 4 + hdrLen).toString('utf8'));
    return { event: hdr.event, buffer: payload.subarray(4 + hdrLen) };
  } catch { return null; }
}

class RateLimiter implements IRateLimiter {
  private limits = new Map<string, { count: number; resetTime: number }>();
  constructor(private maxPts = 100, private winMs = 1000) {}
  check(id: string, cost = 1): boolean {
    const now = Date.now(); let e = this.limits.get(id);
    if (!e || now >= e.resetTime) { e = { count: 0, resetTime: now + this.winMs }; this.limits.set(id, e); }
    if (e.count + cost > this.maxPts) return false;
    e.count += cost; return true;
  }
  cleanup() { const now = Date.now(); for (const [id, e] of this.limits) if (now >= e.resetTime) this.limits.delete(id); }
  reset(id: string) { this.limits.delete(id); }
  size() { return this.limits.size; }
}

class IPTracker implements IIPTracker {
  private m = new Map<string, number>();
  constructor(private max = 50) {}
  check(ip: string) { return (this.m.get(ip) || 0) < this.max; }
  add(ip: string) { this.m.set(ip, (this.m.get(ip) || 0) + 1); }
  remove(ip: string) { const c = this.m.get(ip) || 0; c <= 1 ? this.m.delete(ip) : this.m.set(ip, c - 1); }
  getCount(ip: string) { return this.m.get(ip) || 0; }
  cleanup() { for (const [ip, c] of this.m) if (c <= 0) this.m.delete(ip); }
}

/* ── Server ── */

class StelarServer {
  private port: number;
  private httpServer: HttpServer | null = null;
  private tcpServer: TcpServer | null = null;
  private ns: string;
  private hbInterval: number;
  private hbTimeout: number;
  private tcpPort: number | false;
  private maxConns: number;
  private maxRooms: number;
  private maxRoomsPerClient: number;
  private maxPayload: number;
  private maxFrame: number;
  private maxWSFrame: number;
  private connTimeout: number;
  private doGraceful: boolean;
  private shutdownMs: number;
  private healthPath: string | false;
  private tlsOpts: TlsOptions | undefined;
  private origins: string[] | null;
  private _crl: IRateLimiter | null;
  private _cit: IIPTracker | null;
  private _genId: (() => string) | null;
  private _healthFn: ((req: IncomingMessage, res: ServerResponse, stats: StelarStats) => void) | null;
  private hooks: StelarHooks;
  private evRateLimits = new Map<string, RateLimiter>();
  private clientRates = new Map<string, RateLimiter>();
  private doCompress: boolean;

  private clients = new Map<NetSocket, ClientRecord>();
  private byId = new Map<string, ClientRecord>();
  private rooms = new Map<string, Set<string>>();
  private events = new Map<string, StelarEventHandler>();
  private mw: StelarMiddleware[] = [];
  private _rc: ReturnType<typeof setInterval> | null = null;
  private _wild: StelarWildcardHandler | null = null;
  private _connH: StelarEventHandler | null = null;
  private _acks = new Map<string, StelarEventHandler>();
  private _ext = new WeakSet<HttpServer>();
  private _upgH: ((req: IncomingMessage, socket: NetSocket, head: Buffer) => void) | null = null;
  private _reqH: ((req: IncomingMessage, res: ServerResponse) => void) | null = null;
  private _started = false;
  private _startTime = 0;
  private _shutting = false;
  private _sigH: { int: (() => void) | null; term: (() => void) | null } = { int: null, term: null };
  private rateLimiter: RateLimiter | null;
  private ipTracker: IPTracker;
  private _totalConns = 0;
  private _totalRecv = 0;
  private _totalSent = 0;
  private _shutdownCbs: Array<(sig: string, force: boolean) => void> = [];
  private log: Logger;

  constructor(o: StelarOptions = {}) {
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
    this.doCompress = o.compression || false;
    if (o.eventRateLimits) for (const [ev, c] of Object.entries(o.eventRateLimits)) this.evRateLimits.set(ev, new RateLimiter(c.maxPoints, c.windowMs));
    const rl = o.rateLimit && typeof o.rateLimit === 'object' ? o.rateLimit : {};
    this.rateLimiter = o.rateLimit === false && !this._crl ? null : this._crl ? null : new RateLimiter(rl.maxPoints || 100, rl.windowMs || 1000);
    this.ipTracker = this._cit ? new IPTracker() : new IPTracker(o.maxConnectionsPerIP || 50);
    this.log = o.logger === false ? NULL_LOGGER : o.logger instanceof Logger ? o.logger : new Logger({ level: (o.logger as LogLevel) || 'info', prefix: 'stelar:server' });
  }

  static of(path: string, o: StelarOptions = {}) { return new StelarServer({ ...o, namespace: path }); }

  /* ── Runtime config ── */

  updateConfig(o: Partial<StelarOptions>): this {
    if (o.maxConnections !== undefined) this.maxConns = o.maxConnections;
    if (o.maxConnectionsPerIP !== undefined && !this._cit) this.ipTracker = new IPTracker(o.maxConnectionsPerIP);
    if (o.maxRooms !== undefined) this.maxRooms = o.maxRooms;
    if (o.maxRoomsPerClient !== undefined) this.maxRoomsPerClient = o.maxRoomsPerClient;
    if (o.maxPayloadSize !== undefined) this.maxPayload = o.maxPayloadSize;
    if (o.heartbeatInterval !== undefined) this.hbInterval = o.heartbeatInterval;
    if (o.heartbeatTimeout !== undefined) this.hbTimeout = o.heartbeatTimeout;
    if (o.allowedOrigins !== undefined) this.origins = o.allowedOrigins;
    if (o.compression !== undefined) this.doCompress = o.compression;
    if (o.rateLimit === false) { this.rateLimiter = null; this._crl = null; }
    else if (o.rateLimit && !this._crl) this.rateLimiter = new RateLimiter(o.rateLimit.maxPoints || 100, o.rateLimit.windowMs || 1000);
    if (o.customRateLimiter !== undefined) { this._crl = o.customRateLimiter; this.rateLimiter = null; }
    if (o.customIPTracker !== undefined) this._cit = o.customIPTracker;
    if (o.generateClientId !== undefined) this._genId = o.generateClientId;
    if (o.customHealthHandler !== undefined) this._healthFn = o.customHealthHandler;
    if (o.hooks !== undefined) this.hooks = { ...this.hooks, ...o.hooks };
    if (o.eventRateLimits !== undefined) { this.evRateLimits.clear(); for (const [ev, c] of Object.entries(o.eventRateLimits)) this.evRateLimits.set(ev, new RateLimiter(c.maxPoints, c.windowMs)); }
    this.log.info('Config updated');
    return this;
  }

  setClientRateLimit(id: string, c: { maxPoints: number; windowMs: number }) { this.clientRates.set(id, new RateLimiter(c.maxPoints, c.windowMs)); return this; }
  removeClientRateLimit(id: string) { this.clientRates.delete(id); return this; }
  setEventRateLimit(ev: string, c: { maxPoints: number; windowMs: number }) { this.evRateLimits.set(ev, new RateLimiter(c.maxPoints, c.windowMs)); return this; }
  removeEventRateLimit(ev: string) { this.evRateLimits.delete(ev); return this; }

  getConfig() {
    return Object.freeze({
      maxConnections: this.maxConns, maxConnectionsPerIP: this._cit ? -1 : 50,
      maxRooms: this.maxRooms, maxRoomsPerClient: this.maxRoomsPerClient, maxPayloadSize: this.maxPayload,
      heartbeatInterval: this.hbInterval, heartbeatTimeout: this.hbTimeout, connectTimeout: this.connTimeout,
      shutdownTimeout: this.shutdownMs, compression: this.doCompress,
      hasCustomRateLimiter: this._crl !== null, hasCustomIPTracker: this._cit !== null,
      hasCustomClientIdGenerator: this._genId !== null, hasCustomHealthHandler: this._healthFn !== null,
      eventRateLimits: Array.from(this.evRateLimits.keys()), hooks: Object.keys(this.hooks), allowedOrigins: this.origins,
    });
  }

  /* ── Event registration ── */

  use(mw: StelarMiddleware) { this.mw.push(mw); return this; }
  on(ev: string, h: StelarEventHandler) { this.events.set(ev, h); return this; }
  onAll(h: StelarWildcardHandler) { this._wild = h; return this; }
  onConnection(h: StelarEventHandler) { this._connH = h; return this; }
  onDisconnect(h: StelarEventHandler) { this.events.set('disconnect', h); return this; }
  onAck(name: string, h: StelarEventHandler) { this._acks.set(name, h); return this; }

  /* ── Messaging ── */

  broadcast(event: string, data: unknown, excludeId?: string): this {
    if (this.hooks.onBeforeBroadcast?.({ event, data, excludeId }) === false) return this;
    const json = JSON.stringify({ event, data });
    const wsF = createWSTextFrame(json);
    const wsFC = this.doCompress ? createWSTextFrame(json, true) : wsF;
    const tcpF = encodeJsonFrame(event, data, this.maxFrame);
    let sent = 0;
    this.clients.forEach(r => { if (excludeId && r.info.id === excludeId) return; if (this._write(r, r.compress ? wsFC : wsF, tcpF)) sent++; });
    this._totalSent += sent;
    return this;
  }

  broadcastBinary(event: string, buf: ArrayBuffer) {
    const safeCopy = Buffer.from(new Uint8Array(buf));
    this.clients.forEach(r => this._sendBin(r, event, safeCopy));
  }

  to(room: string, event: string, data: unknown, excludeId?: string): this {
    const ids = this.rooms.get(room);
    if (!ids) return this;
    const json = JSON.stringify({ event, data });
    const wsF = createWSTextFrame(json);
    const wsFC = this.doCompress ? createWSTextFrame(json, true) : wsF;
    const tcpF = encodeJsonFrame(event, data, this.maxFrame);
    let sent = 0;
    for (const id of ids) { if (excludeId && id === excludeId) continue; const r = this.byId.get(id); if (r && this._write(r, r.compress ? wsFC : wsF, tcpF)) sent++; }
    this._totalSent += sent;
    return this;
  }

  toId(id: string, event: string, data: unknown): this {
    const r = this.byId.get(id);
    if (r && this._sendJson(r, event, data)) this._totalSent++;
    return this;
  }

  getClients(room?: string) {
    const list: { id: string; rooms: string[] }[] = [];
    this.clients.forEach(r => { if (!room || r.info.rooms.has(room)) list.push({ id: r.info.id, rooms: [...r.info.rooms] }); });
    return list;
  }

  getRoomMembers(room: string) { return this.rooms.get(room) ? [...this.rooms.get(room)!] : []; }
  getRooms() { return [...this.rooms.keys()]; }
  getPort() { const a = this.httpServer?.address(); return a && typeof a === 'object' ? a.port : this.port; }

  getStats(): StelarStats {
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

  onShutdown(cb: (sig: string, force: boolean) => void) { this._shutdownCbs.push(cb); return this; }

  /* ── Private: backpressure-aware write ── */

  private _write(r: ClientRecord, wsF: Buffer, tcpF: Buffer): boolean {
    if (r.socket.destroyed || r.socket.writableEnded) return false;
    if (r._writePaused) { r._writeQueue.push(r.protocol === 'ws' ? wsF : tcpF); return true; }
    try {
      const ok = r.socket.write(r.protocol === 'ws' ? wsF : tcpF);
      if (!ok) r._writePaused = true;
      r.info.messagesSent++; return true;
    } catch { return false; }
  }

  private _sendJson(r: ClientRecord, event: string, data: unknown): boolean {
    if (r.socket.destroyed || r.socket.writableEnded) return false;
    try {
      const frame = r.protocol === 'ws' ? createWSTextFrame(JSON.stringify({ event, data }), r.compress) : encodeJsonFrame(event, data, this.maxFrame);
      if (r._writePaused) { r._writeQueue.push(frame); r.info.messagesSent++; return true; }
      const ok = r.socket.write(frame);
      if (!ok) r._writePaused = true;
      r.info.messagesSent++; return true;
    } catch { return false; }
  }

  private _sendBin(r: ClientRecord, event: string, buf: Buffer): boolean {
    if (r.socket.destroyed || r.socket.writableEnded) return false;
    try {
      if (r.protocol === 'ws') {
        const frame = encodeWSBinary(event, buf);
        if (r._writePaused) { r._writeQueue.push(createWSBinaryFrame(frame)); r.info.messagesSent++; return true; }
        const ok = r.socket.write(createWSBinaryFrame(frame));
        if (!ok) r._writePaused = true;
      } else {
        const frame = encodeBinaryFrame(event, buf, this.maxFrame);
        if (r._writePaused) { r._writeQueue.push(frame); r.info.messagesSent++; return true; }
        const ok = r.socket.write(frame);
        if (!ok) r._writePaused = true;
      }
      r.info.messagesSent++; return true;
    } catch { return false; }
  }

  private _flushQueue(r: ClientRecord) {
    r._writePaused = false;
    while (r._writeQueue.length) {
      const buf = r._writeQueue.shift()!;
      if (!r.socket.destroyed && !r.socket.writableEnded) {
        try {
          const ok = r.socket.write(buf);
          if (!ok) { r._writePaused = true; break; }
        } catch { break; }
      }
    }
  }

  private _checkRate(cid: string, event?: string): boolean {
    const co = this.clientRates.get(cid);
    if (co) return co.check(cid);
    if (event && this.evRateLimits.has(event) && !this.evRateLimits.get(event)!.check(cid)) return false;
    if (this._crl) return this._crl.check(cid);
    if (this.rateLimiter) return this.rateLimiter.check(cid);
    return true;
  }

  private _getIP(socket: NetSocket, req: IncomingMessage | null): string {
    if (req) { const fwd = req.headers['x-forwarded-for']; if (typeof fwd === 'string') return fwd.split(',')[0].trim(); }
    return socket.remoteAddress || 'unknown';
  }

  /* ── Private: per-client heartbeat ── */

  private _startClientHB(r: ClientRecord) {
    r._hbTimer = setInterval(() => {
      if (r.socket.destroyed) { this._stopClientHB(r); return; }
      const now = Date.now();
      if (now - r.info.lastPing > this.hbTimeout) { r.socket.destroy(); return; }
      try { r.socket.write(r.protocol === 'ws' ? createWSPingFrame() : encodePingFrame()); } catch {}
    }, this.hbInterval);
    r._hbTimer.unref();
  }

  private _stopClientHB(r: ClientRecord) { if (r._hbTimer) { clearInterval(r._hbTimer); r._hbTimer = null; } }

  /* ── Private: client lifecycle ── */

  private _register(socket: NetSocket, proto: 'ws' | 'tcp', req: IncomingMessage | null, parser: WSFrameParser | FrameParser, compress = false): ClientRecord | null {
    const ip = this._getIP(socket, req);
    if (this.clients.size >= this.maxConns) {
      this.hooks.onMaxConnectionsReached?.({ activeConnections: this.clients.size, max: this.maxConns, ip });
      this.log.warn('Max connections reached', { active: this.clients.size, max: this.maxConns });
      if (proto === 'ws') try { socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Server full')); } catch {}
      socket.destroy(); return null;
    }
    const tracker = this._cit || this.ipTracker;
    if (!tracker.check(ip)) {
      this.log.warn('Max connections per IP', { ip });
      if (proto === 'ws') try { socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Too many connections')); } catch {}
      socket.destroy(); return null;
    }
    const id = this._genId ? this._genId() : randomUUID();
    const info: StelarClientInfo = { id, rooms: new Set(), lastPing: Date.now(), protocol: proto, connectedAt: Date.now(), metadata: new Map(), messagesReceived: 0, messagesSent: 0, remoteAddress: ip };
    const record: ClientRecord = { info, socket, parser, protocol: proto, compress, _hbTimer: null, _writePaused: false, _writeQueue: [] };
    this.clients.set(socket, record); this.byId.set(id, record); tracker.add(ip); this._totalConns++;
    return record;
  }

  private _unregister(r: ClientRecord, ctx: StelarContext) {
    this._stopClientHB(r);
    this.hooks.onClientDisconnect?.({ clientId: r.info.id, ip: r.info.remoteAddress, protocol: r.info.protocol, rooms: new Set(r.info.rooms) });
    for (const room of r.info.rooms) { const m = this.rooms.get(room); if (m) { m.delete(r.info.id); if (!m.size) this.rooms.delete(room); } }
    r.info.rooms.clear();
    this.byId.delete(r.info.id); this.clients.delete(r.socket);
    (this._cit || this.ipTracker).remove(r.info.remoteAddress);
    if (this._crl) this._crl.reset(r.info.id); else this.rateLimiter?.reset(r.info.id);
    this.clientRates.delete(r.info.id);
    const h = this.events.get('disconnect');
    if (h) try { h({ ...ctx, event: 'disconnect' }); } catch (e) { this.log.error('Disconnect handler error', { error: String(e) }); }
  }

  private _joinRoom(r: ClientRecord, room: string) {
    if (this.hooks.onClientJoinRoom?.({ clientId: r.info.id, room, metadata: r.info.metadata }) === false) return;
    if (r.info.rooms.size >= this.maxRoomsPerClient) { this.hooks.onMaxRoomsPerClientReached?.({ clientId: r.info.id, room, currentRooms: r.info.rooms.size, max: this.maxRoomsPerClient }); return; }
    if (this.rooms.size >= this.maxRooms && !this.rooms.has(room)) { this.hooks.onMaxRoomsReached?.({ clientId: r.info.id, room, totalRooms: this.rooms.size, max: this.maxRooms }); return; }
    r.info.rooms.add(room);
    if (!this.rooms.has(room)) this.rooms.set(room, new Set());
    this.rooms.get(room)!.add(r.info.id);
    this._sendJson(r, 'joined-room', room);
  }

  private _leaveRoom(r: ClientRecord, room: string) {
    if (this.hooks.onClientLeaveRoom?.({ clientId: r.info.id, room }) === false) return;
    r.info.rooms.delete(room);
    const m = this.rooms.get(room);
    if (m) { m.delete(r.info.id); if (!m.size) this.rooms.delete(room); }
    this._sendJson(r, 'left-room', room);
  }

  /* ── Private: context & middleware ── */

  private _buildCtx(r: ClientRecord, req: IncomingMessage | null): StelarContext {
    const s = this;
    const ctx: StelarContext = {
      id: r.info.id, socket: r.socket, req, clientInfo: r.info,
      emit: (ev, d) => { if (s._sendJson(r, ev, d)) s._totalSent++; },
      send: (rid, d) => { if (s._sendJson(r, rid, { data: d, _isAck: true, _correlationId: ctx._correlationId })) s._totalSent++; },
      emitBinary: (ev, buf) => { if (s._sendBin(r, ev, Buffer.from(new Uint8Array(buf)))) s._totalSent++; },
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
        if (!h) return;
        let res: unknown;
        try { res = h({ ...ctx, data: d }); } catch (e) { s.log.error('ACK handler error', { name, error: String(e) }); return; }
        if (res !== undefined) {
          try {
            if (r.protocol === 'ws') {
              const p: Record<string, unknown> = { event: name, data: res, _isAck: true };
              if (ctx._correlationId) p._correlationId = ctx._correlationId;
              r.socket.write(createWSTextFrame(JSON.stringify(p), r.compress));
            } else {
              r.socket.write(ctx._correlationId
                ? encodeAckResFrame(name, { data: res, _correlationId: ctx._correlationId }, s.maxFrame)
                : encodeAckResFrame(name, res, s.maxFrame));
            }
            s._totalSent++;
          } catch (e) { s.log.error('ACK send error', { name, error: String(e) }); }
        }
      },
    };
    return ctx;
  }

  private _runMw(ctx: StelarContext, next: () => void) {
    const run = (i: number) => { if (i >= this.mw.length) return next(); try { this.mw[i](ctx, () => run(i + 1)); } catch { ctx.socket.destroy(); } };
    run(0);
  }

  /* ── Private: event dispatch ── */

  private _dispatch(r: ClientRecord, ctx: StelarContext, event: string, data: unknown, correlationId?: string) {
    if (event === 'pong') { r.info.lastPing = Date.now(); return; }
    if (event === 'join-room') { if (data) this._joinRoom(r, String(data)); return; }
    if (event === 'leave-room') { if (data) this._leaveRoom(r, String(data)); return; }
    const ectx: StelarContext = { ...ctx, data, event, _correlationId: correlationId };
    const h = this.events.get(event);
    if (h) try { h(ectx); } catch (e) { this.log.error('Event handler error', { event, error: String(e) }); }
    if (this._wild) try { this._wild({ event, data: ectx }); } catch (e) { this.log.error('Wildcard error', { error: String(e) }); }
  }

  /* ── Private: WS upgrade ── */

  private _wsUpgrade(req: IncomingMessage, socket: NetSocket, head: Buffer) {
    const path = new URL(req.url || '/', 'http://localhost').pathname;
    const nsPath = this.ns === '/' ? '/' : this.ns;
    if (nsPath !== '/' && path !== nsPath) { socket.destroy(); return; }
    if (this.origins && !this.origins.includes(req.headers['origin'] || '')) { socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); socket.destroy(); return; }
    const key = req.headers['sec-websocket-key'] as string;
    if (!key || !validateWSKey(key)) { socket.destroy(); return; }
    const clientCompress = this.doCompress && clientWantsCompression(req.headers['sec-websocket-extensions'] as string);
    try {
      const extra: Record<string, string> = {};
      const origin = req.headers['origin'];
      if (origin && this.origins?.includes(origin)) extra['Access-Control-Allow-Origin'] = origin;
      socket.write(buildUpgradeResponse(key, extra, clientCompress));
    } catch { socket.destroy(); return; }

    const timer = setTimeout(() => { if (!this.clients.has(socket)) socket.destroy(); }, this.connTimeout);
    timer.unref();

    const r = this._register(socket, 'ws', req, new WSFrameParser(this.maxWSFrame), clientCompress);
    if (!r) { clearTimeout(timer); return; }
    const ctx = this._buildCtx(r, req);

    this.hooks.onClientConnect?.({ clientId: r.info.id, ip: r.info.remoteAddress, protocol: 'ws', metadata: r.info.metadata });
    this._runMw(ctx, () => { if (this._connH) try { this._connH(ctx); } catch (e) { this.log.error('Connection handler error', { error: String(e) }); } });
    this.log.info('WS connected', { clientId: r.info.id, ip: r.info.remoteAddress, compressed: clientCompress });
    this._startClientHB(r);

    if (head.length > 0) this._processWS(r, head, ctx);
    socket.on('data', (d: Buffer) => { clearTimeout(timer); this._processWS(r, d, ctx); });
    socket.on('close', () => { clearTimeout(timer); this._unregister(r, ctx); });
    socket.on('error', (e: Error) => { this.log.warn('WS error', { clientId: r.info.id, error: e.message }); this._handleErr(r, ctx, e); });
    socket.on('drain', () => this._flushQueue(r));
  }

  private _processWS(r: ClientRecord, data: Buffer, ctx: StelarContext) {
    let frames: WSFrame[];
    try { frames = (r.parser as WSFrameParser).feed(data); } catch (e) {
      if (e instanceof WebSocketError) { this.log.warn('WS protocol error', { code: e.code, message: e.message }); try { r.socket.write(createWSCloseFrame(e.code, e.message)); } catch {} }
      else this.log.error('WS parse error', { error: String(e) });
      r.socket.destroy(); return;
    }
    for (const f of frames) { if (!r.socket.destroyed) this._handleWSFrame(r, f, ctx); }
  }

  private _handleWSFrame(r: ClientRecord, frame: WSFrame, ctx: StelarContext) {
    const { opcode, payload } = frame;

    if (opcode === OP_PING) { try { r.socket.write(createWSPongFrame(payload)); } catch {} return; }
    if (opcode === OP_CLOSE) { try { r.socket.write(createWSCloseFrame()); } catch {} r.socket.end(); return; }
    if (opcode === OP_PONG) { r.info.lastPing = Date.now(); return; }

    if (!this._checkRate(r.info.id)) {
      this.log.warn('Rate limit exceeded', { clientId: r.info.id });
      if (this.hooks.onRateLimitExceeded?.({ clientId: r.info.id, protocol: 'ws' }) === false) return;
      try { r.socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Rate limit exceeded')); } catch {} r.socket.destroy(); return;
    }

    if (opcode === OP_TEXT) {
      r.info.messagesReceived++; this._totalRecv++;
      if (payload.length > this.maxPayload) { this.hooks.onPayloadTooLarge?.({ clientId: r.info.id, size: payload.length, max: this.maxPayload }); try { r.socket.write(createWSCloseFrame(CLOSE_MESSAGE_TOO_BIG)); } catch {} r.socket.destroy(); return; }
      let msg: Record<string, unknown>;
      try { msg = JSON.parse(payload.toString('utf8')); } catch { this.hooks.onInvalidMessage?.({ clientId: r.info.id, reason: 'Invalid JSON', protocol: 'ws' }); return; }
      const event = String(msg.event || ''), data = msg.data, corrId = msg._correlationId ? String(msg._correlationId) : undefined;
      if (!event) return;
      if (!this._checkRate(r.info.id, event)) {
        this.log.warn('Event rate limit', { clientId: r.info.id, event });
        if (this.hooks.onRateLimitExceeded?.({ clientId: r.info.id, event, protocol: 'ws' }) === false) return;
        try { r.socket.write(createWSCloseFrame(CLOSE_POLICY_VIOLATION, 'Rate limit exceeded')); } catch {} r.socket.destroy(); return;
      }
      if (msg._ackName && this._acks.has(String(msg._ackName))) {
        const name = String(msg._ackName), h = this._acks.get(name)!;
        let res: unknown;
        try { res = h({ ...ctx, data, _correlationId: corrId }); } catch (e) { this.log.error('ACK handler error', { name, error: String(e) }); return; }
        if (res !== undefined) {
          const p: Record<string, unknown> = { event: name, data: res, _isAck: true };
          if (corrId) p._correlationId = corrId;
          try { r.socket.write(createWSTextFrame(JSON.stringify(p), r.compress)); this._totalSent++; } catch {}
        }
        return;
      }
      this._dispatch(r, ctx, event, data, corrId);
    }

    if (opcode === OP_BINARY) {
      r.info.messagesReceived++; this._totalRecv++;
      if (payload.length > this.maxPayload) { this.hooks.onPayloadTooLarge?.({ clientId: r.info.id, size: payload.length, max: this.maxPayload }); return; }
      const parsed = parseWSBinary(payload);
      if (!parsed) { this.hooks.onInvalidMessage?.({ clientId: r.info.id, reason: 'Invalid binary frame', protocol: 'ws' }); return; }
      if (parsed.event && !this._checkRate(r.info.id, parsed.event)) {
        this.log.warn('Binary rate limit', { clientId: r.info.id, event: parsed.event });
        if (this.hooks.onRateLimitExceeded?.({ clientId: r.info.id, event: parsed.event, protocol: 'ws' }) === false) return;
        return;
      }
      const ectx: StelarContext = { ...ctx, data: parsed.buffer, buffer: parsed.buffer, isBinary: true, event: parsed.event };
      const h = this.events.get(parsed.event);
      if (h) try { h(ectx); } catch {}
      if (this._wild) try { this._wild({ event: parsed.event, data: ectx }); } catch {}
    }
  }

  /* ── Private: TCP connection ── */

  private _tcpConnect(socket: NetSocket) {
    const r = this._register(socket, 'tcp', null, new FrameParser(this.maxFrame));
    if (!r) return;
    const ctx = this._buildCtx(r, null);
    try { socket.write(encodeConnectFrame(r.info.id)); } catch { socket.destroy(); return; }
    this.hooks.onClientConnect?.({ clientId: r.info.id, ip: r.info.remoteAddress, protocol: 'tcp', metadata: r.info.metadata });
    this._runMw(ctx, () => { if (this._connH) try { this._connH(ctx); } catch (e) { this.log.error('TCP connection handler error', { error: String(e) }); } });
    this.log.info('TCP connected', { clientId: r.info.id, ip: r.info.remoteAddress });
    this._startClientHB(r);
    socket.on('data', (d: Buffer) => this._processTCP(r, d, ctx));
    socket.on('close', () => this._unregister(r, ctx));
    socket.on('error', (e: Error) => { this.log.warn('TCP error', { clientId: r.info.id, error: e.message }); this._handleErr(r, ctx, e); });
    socket.on('drain', () => this._flushQueue(r));
  }

  private _processTCP(r: ClientRecord, data: Buffer, ctx: StelarContext) {
    let frames: ParsedFrame[];
    try { frames = (r.parser as FrameParser).feed(data); } catch (e) {
      if (e instanceof ProtocolError) { this.log.warn('TCP protocol error', { code: e.code, message: e.message }); try { r.socket.write(encodeErrorFrame(e.message)); } catch {} }
      r.socket.destroy(); return;
    }
    for (const f of frames) { if (!r.socket.destroyed) this._handleTCPFrame(r, f, ctx); }
  }

  private _handleTCPFrame(r: ClientRecord, frame: ParsedFrame, ctx: StelarContext) {
    const { type, event, payload } = frame;
    if (type === FRAME_PING) { try { r.socket.write(encodePongFrame()); } catch {} r.info.lastPing = Date.now(); return; }
    if (type === FRAME_PONG) { r.info.lastPing = Date.now(); return; }
    if (type === FRAME_CONNECT) return;

    if (!this._checkRate(r.info.id, event)) {
      this.log.warn('TCP rate limit', { clientId: r.info.id, event });
      if (this.hooks.onRateLimitExceeded?.({ clientId: r.info.id, event: event || undefined, protocol: 'tcp' }) === false) return;
      try { r.socket.write(encodeErrorFrame('Rate limit exceeded')); } catch {} r.socket.destroy(); return;
    }

    if (type === FRAME_JOIN) { if (payload.toString('utf8')) this._joinRoom(r, payload.toString('utf8')); return; }
    if (type === FRAME_LEAVE) { if (payload.toString('utf8')) this._leaveRoom(r, payload.toString('utf8')); return; }
    if (payload.length > this.maxPayload) { this.hooks.onPayloadTooLarge?.({ clientId: r.info.id, event, size: payload.length, max: this.maxPayload }); return; }

    r.info.messagesReceived++; this._totalRecv++;

    if (type === FRAME_JSON) {
      let data: unknown;
      try { data = JSON.parse(payload.toString('utf8')); } catch { this.hooks.onInvalidMessage?.({ clientId: r.info.id, reason: 'Invalid JSON', protocol: 'tcp' }); return; }
      this._dispatch(r, ctx, event, data);
      return;
    }

    if (type === FRAME_ACK_REQ) {
      if (this._acks.has(event)) {
        try {
          const parsed = JSON.parse(payload.toString('utf8'));
          const data = parsed && typeof parsed === 'object' && 'data' in parsed ? parsed.data : parsed;
          const corrId = parsed && typeof parsed === 'object' && '_correlationId' in parsed ? String(parsed._correlationId) : undefined;
          const h = this._acks.get(event)!;
          const res = h({ ...ctx, data, _correlationId: corrId });
          if (res !== undefined) {
            r.socket.write(corrId ? encodeAckResFrame(event, { data: res, _correlationId: corrId }, this.maxFrame) : encodeAckResFrame(event, res, this.maxFrame));
            this._totalSent++;
          }
        } catch (e) { this.log.error('TCP ACK handler error', { event, error: String(e) }); }
      }
      return;
    }

    if (type === FRAME_BINARY) {
      const ectx: StelarContext = { ...ctx, data: payload, buffer: payload, isBinary: true, event };
      const h = this.events.get(event);
      if (h) try { h(ectx); } catch {}
      if (this._wild) try { this._wild({ event, data: ectx }); } catch {}
    }
  }

  private _handleErr(r: ClientRecord, ctx: StelarContext, err: Error) {
    const h = this.events.get('error');
    if (h) try { h({ ...ctx, error: err, event: 'error' }); } catch {}
  }

  /* ── Private: health check ── */

  private _health(req: IncomingMessage, res: ServerResponse) {
    if (this._healthFn) { try { this._healthFn(req, res, this.getStats()); } catch { if (!res.headersSent) { res.writeHead(500); res.end('{"status":"error"}'); } } return; }
    const origin = req.headers['origin'];
    if (origin && (!this.origins || this.origins.includes(origin))) { res.setHeader('Access-Control-Allow-Origin', origin); res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS'); res.setHeader('Access-Control-Max-Age', '86400'); }
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
    if (this.healthPath && req.url === this.healthPath && req.method === 'GET') {
      const s = this.getStats();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', ...s, uptimeSeconds: Math.floor(s.uptime / 1000), memoryMB: Math.round(s.memoryUsage.heapUsed / 1024 / 1024 * 100) / 100 }));
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('Stelar Time Real v3 Server');
  }

  /* ── Private: graceful shutdown ── */

  private _emitShutdown(sig: string, force: boolean) {
    if (!this._shutdownCbs.length) { process.exit(force ? 1 : 0); return; }
    for (const cb of this._shutdownCbs) try { cb(sig, force); } catch {}
  }

  private _setupShutdown() {
    if (!this.doGraceful) return;
    let done = false;
    const shutdown = (sig: string) => {
      if (done) return; done = true; this._shutting = true;
      this.log.info(`Received ${sig}, shutting down...`);
      this.stop();
      if (!this.clients.size) { this.log.info('Shutdown complete'); this._emitShutdown(sig, false); return; }
      this.log.info(`Waiting for ${this.clients.size} connections (timeout: ${this.shutdownMs}ms)`);
      this.clients.forEach(r => { try { r.socket.write(r.protocol === 'ws' ? createWSCloseFrame(CLOSE_GOING_AWAY, 'Shutting down') : encodeDisconnectFrame()); r.socket.end(); } catch {} });
      const forceT = setTimeout(() => { this.clients.forEach(r => { try { r.socket.destroy(); } catch {} }); this.clients.clear(); this.byId.clear(); this._emitShutdown(sig, true); }, this.shutdownMs);
      forceT.unref();
      const check = setInterval(() => { if (!this.clients.size) { clearInterval(check); clearTimeout(forceT); this._emitShutdown(sig, false); } }, 100);
      check.unref();
    };
    this._sigH.int = () => shutdown('SIGINT'); this._sigH.term = () => shutdown('SIGTERM');
    process.on('SIGINT', this._sigH.int); process.on('SIGTERM', this._sigH.term);
  }

  private _removeSignals() {
    if (this._sigH.int) { process.off('SIGINT', this._sigH.int); this._sigH.int = null; }
    if (this._sigH.term) { process.off('SIGTERM', this._sigH.term); this._sigH.term = null; }
  }

  /* ── Start / Stop ── */

  start(cb?: (port: number) => void): Promise<number> {
    if (this._started) { const p = this.getPort(); cb?.(p); return Promise.resolve(p); }
    this._started = true; this._startTime = Date.now();
    return new Promise(resolve => {
      const onHttp = (srv: HttpServer) => {
        this.httpServer = srv;
        this._reqH = (req, res) => this._health(req, res);
        this._upgH = (req, socket, head) => this._wsUpgrade(req, socket, head);
        srv.on('request', this._reqH); srv.on('upgrade', this._upgH);
        this._rc = setInterval(() => {
          if (this._crl) this._crl.cleanup(); else this.rateLimiter?.cleanup();
          (this._cit || this.ipTracker).cleanup();
          for (const [id, l] of this.clientRates) { l.cleanup(); if (!this.byId.has(id)) this.clientRates.delete(id); }
          for (const [, l] of this.evRateLimits) l.cleanup();
        }, 30000);
        this._rc?.unref?.();
        this._setupShutdown();
        const p = this.getPort(); this.log.info('Server started', { port: p, namespace: this.ns, tls: !!this.tlsOpts, compression: this.doCompress }); cb?.(p); resolve(p);
      };
      if (this.httpServer) { this._ext.add(this.httpServer); onHttp(this.httpServer); }
      else {
        const tryListen = (port: number) => {
          const srv = createHttp();
          srv.on('error', (e: NodeJS.ErrnoException) => { if (e.code === 'EADDRINUSE' && port < 65535) tryListen(port + 1); else this.log.error('HTTP error', { error: e.message }); });
          srv.listen(port, () => { this.port = port; onHttp(srv); });
        };
        tryListen(this.port);
      }
      if (this.tcpPort !== false) { const p = typeof this.tcpPort === 'number' ? this.tcpPort : this.port + 1; this._startTCP(p); }
    });
  }

  private _startTCP(port: number, attempts = 0) {
    const handler = (s: NetSocket) => this._tcpConnect(s);
    const startPlain = (p: number, a: number) => {
      const srv = createTcp(handler);
      srv.on('error', (e: NodeJS.ErrnoException) => { if (e.code === 'EADDRINUSE' && a < 10) { this.tcpServer = null; this._startTCP(p + 1, a + 1); } else this.log.error('TCP error', { error: e.message }); });
      srv.listen(p, () => { this.tcpServer = srv; this.log.info('TCP started', { port: p }); });
    };
    if (this.tlsOpts) {
      try {
        const srv = createTls(this.tlsOpts, handler);
        this.tcpServer = srv as unknown as TcpServer;
        this.tcpServer.on('error', (e: NodeJS.ErrnoException) => { if (e.code === 'EADDRINUSE' && attempts < 10) { this.tcpServer = null; this._startTCP(port + 1, attempts + 1); } else this.log.error('TLS TCP error', { error: e.message }); });
        this.tcpServer.listen(port, () => this.log.info('TLS TCP started', { port }));
      } catch { startPlain(port, attempts); }
    } else startPlain(port, attempts);
  }

  stop(): this {
    this.clients.forEach(r => this._stopClientHB(r));
    if (this._rc) { clearInterval(this._rc); this._rc = null; }
    this.clients.forEach(r => { if (!r.socket.destroyed) r.socket.destroy(); });
    this.clients.clear(); this.byId.clear(); this.rooms.clear(); this.clientRates.clear();
    if (this.httpServer) {
      if (this._upgH) this.httpServer.off('upgrade', this._upgH);
      if (this._reqH) this.httpServer.off('request', this._reqH);
      if (!this._ext.has(this.httpServer)) this.httpServer.close();
      this.httpServer = null; this._upgH = null; this._reqH = null;
    }
    if (this.tcpServer) { this.tcpServer.close(); this.tcpServer = null; }
    this._started = false; this._removeSignals(); this.log.info('Server stopped');
    return this;
  }
}

export default StelarServer;
export { StelarServer };
export { default as StelarClient } from './client.js';
export { Logger, NULL_LOGGER, type LogLevel } from './logger.js';
export { ProtocolError, validateEventName, DEFAULT_MAX_FRAME_SIZE, MAX_EVENT_LENGTH, HEADER_SIZE } from './protocol.js';
export { WebSocketError, DEFAULT_MAX_WS_FRAME_SIZE, CLOSE_NORMAL, CLOSE_GOING_AWAY, CLOSE_PROTOCOL_ERROR, CLOSE_POLICY_VIOLATION, CLOSE_MESSAGE_TOO_BIG, CLOSE_INVALID_PAYLOAD, CLOSE_UNSUPPORTED } from './websocket.js';
