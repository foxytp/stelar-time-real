/** @stelar-time-real Client — Browser WS / Node WS / binary TCP */

import {
  FrameParser, encodeJsonFrame, encodeBinaryFrame, encodeAckReqFrame,
  encodePingFrame, encodePongFrame, encodeJoinFrame, encodeLeaveFrame,
  FRAME_JSON, FRAME_BINARY, FRAME_PING, FRAME_PONG, FRAME_ACK_RES,
  FRAME_CONNECT, validateEventName, DEFAULT_MAX_FRAME_SIZE, ProtocolError,
} from './protocol.js';

import {
  WSFrameParser, generateWSKey, createWSTextFrameMasked,
  createWSBinaryFrameMasked, createWSCloseFrameMasked,
  createWSPongFrameMasked, OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG,
  CLOSE_NORMAL, DEFAULT_MAX_WS_FRAME_SIZE, clientWantsCompression,
  createWSTextFrame, buildUpgradeResponse,
} from './websocket.js';

import { Logger, NULL_LOGGER, type LogLevel } from './logger.js';

const isNode = typeof process !== 'undefined' && process.versions?.node != null;

export interface StelarClientHooks {
  onBeforeEmit?: (i: { event: string; data: unknown }) => boolean | void;
  onMessage?: (i: { event: string; data: unknown; isBinary: boolean }) => void;
  onStateChange?: (i: { from: ConnectionState; to: ConnectionState }) => void;
  onReconnectDelay?: (i: { attempt: number; defaultDelay: number }) => number | void;
  onMessageQueued?: (i: { event: string; data: unknown; queueSize: number }) => void;
  onQueueDrained?: (i: { count: number }) => void;
  onError?: (i: { error: Error; context: string }) => void;
}

export interface StelarClientOptions {
  reconnection?: boolean; reconnectionAttempts?: number; reconnectionDelay?: number;
  maxReconnectionDelay?: number; heartbeatInterval?: number; ackTimeout?: number;
  mode?: 'ws' | 'tcp'; maxPayloadSize?: number; maxFrameSize?: number;
  messageQueueSize?: number; logger?: Logger | LogLevel | false; tls?: boolean;
  rejectUnauthorized?: boolean; headers?: Record<string, string>;
  compression?: boolean;
  customReconnectDelay?: (attempt: number, baseDelay: number, maxDelay: number) => number;
  hooks?: StelarClientHooks;
}

export interface StelarEmitOptions { ack?: string; _correlationId?: string; }
export type StelarEventHandler = (data: unknown) => void;
export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting';

let _http: typeof import('http') | null, _net: typeof import('net') | null,
  _tls: typeof import('tls') | null, _https: typeof import('https') | null;

async function loadModules() {
  if (!_http) { _http = await import('http'); _net = await import('net'); _tls = await import('tls'); _https = await import('https'); }
}

interface QMsg { event: string; data: unknown; opts: StelarEmitOptions; ts: number; }

class MsgQueue {
  private q: QMsg[] = [];
  constructor(private max = 100) {}
  push(m: QMsg) { if (this.q.length >= this.max) this.q.shift(); this.q.push(m); return true; }
  drain() { const m = this.q; this.q = []; return m; }
  get length() { return this.q.length; }
  clear() { this.q = []; }
}

/** WS binary framing: [4B headerLen BE][header JSON][binary payload] — length-prefixed, not null-delimited */
function encodeWSBinary(event: string, data: Uint8Array | ArrayBuffer): Buffer {
  const hdr = Buffer.from(JSON.stringify({ event }), 'utf8');
  const payload = new Uint8Array(data);
  const frame = Buffer.alloc(4 + hdr.length + payload.length);
  frame.writeUInt32BE(hdr.length, 0);
  hdr.copy(frame, 4);
  frame.set(payload, 4 + hdr.length);
  return frame;
}

function parseWSBinary(payload: Buffer): { event: string; buffer: ArrayBuffer } | null {
  if (payload.length < 4) return null;
  const hdrLen = payload.readUInt32BE(0);
  if (hdrLen > payload.length - 4) return null;
  try {
    const hdr = JSON.parse(payload.subarray(4, 4 + hdrLen).toString('utf8'));
    const buf = payload.subarray(4 + hdrLen);
    return { event: hdr.event, buffer: buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength) as ArrayBuffer };
  } catch { return null; }
}

class StelarClient {
  private url: string;
  private opts: Required<Omit<StelarClientOptions, 'customReconnectDelay' | 'hooks'>> & {
    customReconnectDelay?: (a: number, b: number, m: number) => number; hooks: StelarClientHooks;
  };
  private events = new Map<string, StelarEventHandler>();
  private _wild: ((d: { event: string; data: unknown; isBinary?: boolean; buffer?: ArrayBuffer }) => void) | null = null;
  private _acks = new Map<string, { handler: StelarEventHandler; timer: ReturnType<typeof setTimeout> }>();
  private _state: ConnectionState = 'disconnected';
  private _reconnAttempts = 0;
  private _hb: ReturnType<typeof setInterval> | null = null;
  private _manualClose = false;
  private id: string | null = null;
  private _mq: MsgQueue;
  private _reconnTimer: ReturnType<typeof setTimeout> | null = null;
  private _ackCounter = 0;
  private _sent = 0; private _recv = 0; private _connTime = 0; private _lastErr: Error | null = null;
  private _ws: WebSocket | null = null;
  private _nodeSock: InstanceType<typeof import('net').Socket> | null = null;
  private _wsParser: WSFrameParser | null = null;
  private _tcpSock: InstanceType<typeof import('net').Socket> | null = null;
  private _tcpParser: FrameParser | null = null;
  private _compress = false;
  private _serverCompress = false;
  private _writePaused = false;
  private _writeQueue: Buffer[] = [];
  private log: Logger;

  constructor(urlOrPort: string | number = 'localhost:3000', o: StelarClientOptions = {}) {
    if (typeof urlOrPort === 'number') this.url = `ws://localhost:${urlOrPort}`;
    else if (urlOrPort.includes('://')) this.url = urlOrPort.startsWith('http') ? 'ws' + urlOrPort.slice(4) : urlOrPort;
    else this.url = `ws://${urlOrPort}`;
    this.opts = {
      reconnection: o.reconnection !== false, reconnectionAttempts: o.reconnectionAttempts || 10,
      reconnectionDelay: o.reconnectionDelay || 1000, maxReconnectionDelay: o.maxReconnectionDelay || 30000,
      heartbeatInterval: o.heartbeatInterval || 30000, ackTimeout: o.ackTimeout || 5000,
      mode: o.mode || 'ws', maxPayloadSize: o.maxPayloadSize || 10 * 1024 * 1024,
      maxFrameSize: o.maxFrameSize || DEFAULT_MAX_FRAME_SIZE, messageQueueSize: o.messageQueueSize || 100,
      logger: o.logger !== undefined ? o.logger as any : 'warn', tls: o.tls || false,
      rejectUnauthorized: o.rejectUnauthorized !== false, headers: o.headers || {},
      compression: o.compression || false,
      customReconnectDelay: o.customReconnectDelay, hooks: o.hooks || {},
    };
    this._mq = new MsgQueue(this.opts.messageQueueSize);
    this.log = o.logger === false ? NULL_LOGGER : o.logger instanceof Logger ? o.logger : new Logger({ level: (o.logger as LogLevel) || 'warn', prefix: 'stelar:client' });
  }

  getState() { return this._state; }
  getId() { return this.id; }
  getUrl() { return this.url; }
  getMessagesSent() { return this._sent; }
  getMessagesReceived() { return this._recv; }
  getLastError() { return this._lastErr; }
  getQueueSize() { return this._mq.length; }
  getConnectTime() { return this._connTime; }
  setUrl(u: string) { this.url = u; return this; }

  updateOptions(o: Partial<StelarClientOptions>): this {
    for (const k of ['reconnection', 'reconnectionAttempts', 'reconnectionDelay', 'maxReconnectionDelay', 'heartbeatInterval', 'ackTimeout', 'maxPayloadSize', 'maxFrameSize', 'messageQueueSize', 'headers', 'compression'] as const)
      if ((o as any)[k] !== undefined) (this.opts as any)[k] = (o as any)[k];
    if (o.customReconnectDelay !== undefined) this.opts.customReconnectDelay = o.customReconnectDelay;
    if (o.hooks !== undefined) this.opts.hooks = { ...this.opts.hooks, ...o.hooks };
    return this;
  }

  getOptions() {
    return Object.freeze({
      reconnection: this.opts.reconnection, reconnectionAttempts: this.opts.reconnectionAttempts,
      reconnectionDelay: this.opts.reconnectionDelay, maxReconnectionDelay: this.opts.maxReconnectionDelay,
      heartbeatInterval: this.opts.heartbeatInterval, ackTimeout: this.opts.ackTimeout, mode: this.opts.mode,
      maxPayloadSize: this.opts.maxPayloadSize, messageQueueSize: this.opts.messageQueueSize,
      compression: this.opts.compression,
      hasCustomReconnectDelay: !!this.opts.customReconnectDelay, hooks: Object.keys(this.opts.hooks),
    });
  }

  on(ev: string, h: StelarEventHandler) { this.events.set(ev, h); return this; }
  off(ev: string, h: StelarEventHandler) { if (this.events.get(ev) === h) this.events.delete(ev); return this; }
  once(ev: string, h: StelarEventHandler) { const w = (d: unknown) => { this.off(ev, w); h(d); }; this.on(ev, w); return this; }
  onAll(h: (d: { event: string; data: unknown; isBinary?: boolean; buffer?: ArrayBuffer }) => void) { this._wild = h; return this; }
  onAck(name: string, h: StelarEventHandler) { this._acks.set(name, { handler: h, timer: null as any }); return this; }
  removeAllListeners(ev?: string) { ev ? this.events.delete(ev) : this.events.clear(); return this; }

  emit(event: string, data?: unknown, opts: StelarEmitOptions = {}): this {
    try { if (event) validateEventName(event); } catch { this.log.warn('Invalid event', { event }); return this; }
    if (this.opts.hooks.onBeforeEmit?.({ event, data }) === false) return this;
    try { const s = JSON.stringify(data); if (s.length > this.opts.maxPayloadSize) { this.log.warn('Payload too large', { event }); return this; } } catch { return this; }
    if (this._state !== 'connected') {
      if (this.opts.reconnection) { this._mq.push({ event, data, opts, ts: Date.now() }); this.opts.hooks.onMessageQueued?.({ event, data, queueSize: this._mq.length }); }
      return this;
    }
    try {
      const send = (wsPayload: () => Buffer, tcpPayload: () => Buffer) => {
        if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed) this._writeTCP(tcpPayload());
        else if (this._nodeSock && !this._nodeSock.destroyed) this._writeNodeWS(wsPayload());
        else if (this._ws && this._ws.readyState === WebSocket.OPEN) this._ws.send(JSON.stringify({ event, data, ...(opts.ack ? { _ackName: opts.ack } : {}), ...(opts._correlationId ? { _correlationId: opts._correlationId } : {}) }));
        else { this._mq.push({ event, data, opts, ts: Date.now() }); return; }
        this._sent++;
      };
      if (opts.ack) {
        send(
          () => { const p: Record<string, unknown> = { event, data, _ackName: opts.ack }; if (opts._correlationId) p._correlationId = opts._correlationId; return createWSTextFrameMasked(JSON.stringify(p), this._compress); },
          () => { const d: Record<string, unknown> = { event, data }; if (opts._correlationId) d._correlationId = opts._correlationId; return encodeAckReqFrame(opts.ack!, d, this.opts.maxFrameSize); },
        );
      } else {
        send(
          () => { const p: Record<string, unknown> = { event, data }; if (opts._correlationId) p._correlationId = opts._correlationId; return createWSTextFrameMasked(JSON.stringify(p), this._compress); },
          () => encodeJsonFrame(event, data, this.opts.maxFrameSize),
        );
      }
    } catch (e) {
      this.log.error('Emit error', { event, error: String(e) });
      this.opts.hooks.onError?.({ error: e instanceof Error ? e : new Error(String(e)), context: 'emit' });
      this._mq.push({ event, data, opts, ts: Date.now() });
    }
    return this;
  }

  emitBinary(event: string, data: ArrayBuffer): this {
    if (data.byteLength > this.opts.maxPayloadSize) { this.log.warn('Binary too large', { event }); return this; }
    if (this.opts.hooks.onBeforeEmit?.({ event, data }) === false) return this;
    if (this._state !== 'connected') return this;
    try {
      const safeCopy = Buffer.from(new Uint8Array(data));
      if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed) {
        this._writeTCP(encodeBinaryFrame(event, safeCopy, this.opts.maxFrameSize));
      } else if (this._nodeSock && !this._nodeSock.destroyed) {
        this._writeNodeWS(createWSBinaryFrameMasked(encodeWSBinary(event, safeCopy)));
      } else if (this._ws && this._ws.readyState === WebSocket.OPEN) {
        const frame = encodeWSBinary(event, safeCopy);
        this._ws.send(frame);
      }
      this._sent++;
    } catch (e) { this.log.error('Binary emit error', { event, error: String(e) }); }
    return this;
  }

  sendFile(f: ArrayBuffer) { return this.emitBinary('file', f); }
  sendImage(b: ArrayBuffer) { return this.emitBinary('image', b); }

  request(event: string, data: unknown, ackName: string): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const corrId = `${ackName}#${++this._ackCounter}`;
      const t = setTimeout(() => { this._acks.delete(corrId); reject(new Error(`ACK '${ackName}' timeout`)); }, this.opts.ackTimeout);
      t.unref();
      this._acks.set(corrId, { handler: (d) => { clearTimeout(t); this._acks.delete(corrId); resolve(d); }, timer: t });
      this.emit(event, data, { ack: ackName, _correlationId: corrId });
    });
  }

  joinRoom(room: string) {
    if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed) try { this._tcpSock.write(encodeJoinFrame(room, this.opts.maxFrameSize)); } catch {}
    else this.emit('join-room', room);
    return this;
  }

  leaveRoom(room: string) {
    if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed) try { this._tcpSock.write(encodeLeaveFrame(room)); } catch {}
    else this.emit('leave-room', room);
    return this;
  }

  connect(cb?: () => void): this {
    if (this._state === 'connected' || this._state === 'connecting') return this;
    if (this._state === 'reconnecting' && this._reconnTimer) { clearTimeout(this._reconnTimer); this._reconnTimer = null; }
    this._manualClose = false; this._setState('connecting');
    if (this.opts.mode === 'tcp' && isNode) this._connectTCP();
    else if (isNode) this._connectNodeWS();
    else this._connectBrowser();
    if (cb) {
      const check = setInterval(() => { if (this._state === 'connected') { clearInterval(check); cb(); } }, 50);
      const safety = setTimeout(() => clearInterval(check), this.opts.ackTimeout); safety.unref();
    }
    return this;
  }

  disconnect(): this {
    this._manualClose = true;
    if (this._tcpSock && !this._tcpSock.destroyed) try { this._tcpSock.destroy(); } catch {}
    if (this._nodeSock && !this._nodeSock.destroyed) { try { this._nodeSock.write(createWSCloseFrameMasked()); } catch {} try { this._nodeSock.end(); } catch {} }
    if (this._ws) try { this._ws.close(); } catch {}
    this._fullCleanup(); this._setState('disconnected'); return this;
  }

  isConnected() { return this._state === 'connected'; }

  /* ── Private ── */

  private _setState(s: ConnectionState) {
    const prev = this._state; this._state = s;
    if (prev !== s) { this.log.debug('State', { from: prev, to: s }); this.opts.hooks.onStateChange?.({ from: prev, to: s }); }
  }

  private _startHB() {
    this._stopHB();
    this._hb = setInterval(() => {
      if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed) try { this._tcpSock.write(encodePingFrame()); } catch {}
      else if (this._nodeSock && !this._nodeSock.destroyed) try { this._nodeSock.write(createWSTextFrameMasked(JSON.stringify({ event: 'pong', data: Date.now() }), this._compress)); } catch {}
      else if (this._ws && this._ws.readyState === WebSocket.OPEN) this._ws.send(JSON.stringify({ event: 'pong', data: Date.now() }));
    }, this.opts.heartbeatInterval);
    this._hb?.unref?.();
  }

  private _stopHB() { if (this._hb) { clearInterval(this._hb); this._hb = null; } }

  private _getDelay(): number {
    const base = this.opts.reconnectionDelay, max = this.opts.maxReconnectionDelay;
    const def = Math.min(base * Math.pow(1.5, this._reconnAttempts - 1), max);
    const custom = this.opts.hooks.onReconnectDelay?.({ attempt: this._reconnAttempts, defaultDelay: def });
    if (typeof custom === 'number') return custom;
    if (this.opts.customReconnectDelay) return this.opts.customReconnectDelay(this._reconnAttempts, base, max);
    return Math.floor(def + def * 0.2 * Math.random());
  }

  private _drain() {
    if (!this._mq.length) return;
    const msgs = this._mq.drain();
    this.log.info('Draining queue', { count: msgs.length });
    for (const m of msgs) {
      try {
        const p: Record<string, unknown> = { event: m.event, data: m.data };
        if (m.opts.ack) p._ackName = m.opts.ack;
        if (m.opts._correlationId) p._correlationId = m.opts._correlationId;
        if (this.opts.mode === 'tcp' && this._tcpSock && !this._tcpSock.destroyed) {
          this._tcpSock.write(m.opts.ack ? encodeAckReqFrame(m.opts.ack, p, this.opts.maxFrameSize) : encodeJsonFrame(m.event, m.data, this.opts.maxFrameSize));
        } else if (this._nodeSock && !this._nodeSock.destroyed) {
          this._nodeSock.write(createWSTextFrameMasked(JSON.stringify(p), this._compress));
        } else if (this._ws && this._ws.readyState === WebSocket.OPEN) {
          this._ws.send(JSON.stringify(p));
        }
        this._sent++;
      } catch (e) { this.log.error('Drain error', { event: m.event, error: String(e) }); }
    }
    this.opts.hooks.onQueueDrained?.({ count: msgs.length });
  }

  private _cleanupAcks() { for (const [, e] of this._acks) if (e.timer) clearTimeout(e.timer); this._acks.clear(); }

  private _fullCleanup() {
    this._stopHB(); this._cleanupAcks();
    if (this._reconnTimer) { clearTimeout(this._reconnTimer); this._reconnTimer = null; }
    this._nodeSock = null; this._wsParser = null; this._tcpSock = null; this._tcpParser = null; this._ws = null;
    this._writePaused = false; this._writeQueue = [];
  }

  private _tryReconnect(fn: () => void) {
    if (this._manualClose || !this.opts.reconnection) return;
    if (this._reconnAttempts >= this.opts.reconnectionAttempts) { this.log.warn('Max reconnect attempts'); this.events.get('reconnect_failed')?.(undefined); return; }
    this._reconnAttempts++; this._setState('reconnecting');
    const delay = this._getDelay();
    this.log.info('Reconnecting', { attempt: this._reconnAttempts, delay });
    this.events.get('reconnecting')?.(this._reconnAttempts);
    this._reconnTimer = setTimeout(() => { this._reconnTimer = null; if (!this._manualClose) fn(); }, delay);
  }

  private _onConnected() {
    this._setState('connected'); this._reconnAttempts = 0; this._connTime = Date.now();
    this.events.get('connect')?.(undefined); this._startHB(); this._drain();
  }

  /* ── Backpressure-aware writes ── */

  private _writeTCP(buf: Buffer) {
    if (!this._tcpSock || this._tcpSock.destroyed) return;
    if (this._writePaused) { this._writeQueue.push(buf); return; }
    const ok = this._tcpSock.write(buf);
    if (!ok) this._writePaused = true;
  }

  private _writeNodeWS(buf: Buffer) {
    if (!this._nodeSock || this._nodeSock.destroyed) return;
    if (this._writePaused) { this._writeQueue.push(buf); return; }
    const ok = this._nodeSock.write(buf);
    if (!ok) this._writePaused = true;
  }

  private _flushQueue() {
    this._writePaused = false;
    while (this._writeQueue.length) {
      const buf = this._writeQueue.shift()!;
      const sock = this.opts.mode === 'tcp' ? this._tcpSock : this._nodeSock;
      if (sock && !sock.destroyed) {
        const ok = sock.write(buf);
        if (!ok) { this._writePaused = true; break; }
      }
    }
  }

  /* ── Browser WS ── */

  private _connectBrowser() {
    try {
      const ws = new WebSocket(this.url); ws.binaryType = 'arraybuffer';
      ws.onopen = () => this._onConnected();
      ws.onmessage = (e) => { this._recv++; this._handleBrowserMsg(e); };
      ws.onclose = (e) => { this._setState('disconnected'); this._fullCleanup(); this.events.get('disconnect')?.({ code: e.code, reason: e.reason }); this._tryReconnect(() => this._connectBrowser()); };
      ws.onerror = () => { this._lastErr = new Error('WebSocket error'); this.events.get('error')?.(this._lastErr); this.opts.hooks.onError?.({ error: this._lastErr!, context: 'browser-ws' }); };
      this._ws = ws;
    } catch (e) { this._lastErr = e instanceof Error ? e : new Error(String(e)); this._setState('disconnected'); this._tryReconnect(() => this._connectBrowser()); }
  }

  private _handleBrowserMsg(e: MessageEvent) {
    try {
      if (e.data instanceof ArrayBuffer) {
        const buf = Buffer.from(e.data);
        const parsed = parseWSBinary(buf);
        if (!parsed) return;
        this.opts.hooks.onMessage?.({ event: parsed.event, data: parsed.buffer, isBinary: true });
        this.events.get(parsed.event)?.(parsed.buffer);
        this._wild?.({ event: parsed.event, data: parsed.buffer, isBinary: true, buffer: parsed.buffer });
        return;
      }
      const msg = JSON.parse(e.data as string), { event, data, _isAck } = msg;
      if (event === 'ping') return;
      this.opts.hooks.onMessage?.({ event, data, isBinary: false });
      if (_isAck) {
        const key = msg._correlationId || event;
        if (this._acks.has(key)) { const entry = this._acks.get(key)!; if (entry.timer) clearTimeout(entry.timer); this._acks.delete(key); entry.handler(data); return; }
      }
      this.events.get(event)?.(data);
      this._wild?.({ event, data });
    } catch {}
  }

  /* ── Node WS ── */

  private async _connectNodeWS() {
    try {
      await loadModules(); if (!_http) return;
      const parsed = new URL(this.url), secure = parsed.protocol === 'wss:' || this.opts.tls;
      const key = generateWSKey();
      const hdrs: Record<string, string> = { Upgrade: 'websocket', Connection: 'Upgrade', 'Sec-WebSocket-Key': key, 'Sec-WebSocket-Version': '13', ...this.opts.headers };
      if (this.opts.compression) hdrs['Sec-WebSocket-Extensions'] = 'permessage-deflate; client_no_context_takeover; server_no_context_takeover';
      const mod = secure && _https ? _https : _http;
      const req = mod.request({ hostname: parsed.hostname, port: parseInt(parsed.port) || (secure ? 443 : 80), path: parsed.pathname + parsed.search, method: 'GET', headers: hdrs, rejectUnauthorized: this.opts.rejectUnauthorized });
      req.setTimeout(this.opts.ackTimeout, () => req.destroy(new Error('Timeout')));
      req.on('upgrade', (res, socket, head) => {
        const extHeader = res.headers['sec-websocket-extensions'];
        this._serverCompress = this.opts.compression && !!extHeader && clientWantsCompression(extHeader as string);
        this._compress = this._serverCompress;
        this._nodeSock = socket; this._wsParser = new WSFrameParser(this.opts.maxFrameSize);
        if (head.length > 0) this._processNodeWS(head);
        socket.on('data', (d: Buffer) => this._processNodeWS(d));
        socket.on('close', () => { this._setState('disconnected'); this._fullCleanup(); this.events.get('disconnect')?.(undefined); this._tryReconnect(() => this._connectNodeWS()); });
        socket.on('error', (e: Error) => { this._lastErr = e; this.events.get('error')?.(e); this.opts.hooks.onError?.({ error: e, context: 'node-ws' }); });
        socket.on('drain', () => this._flushQueue());
        this.log.info('Node WS connected', { secure, compressed: this._compress });
        this._onConnected();
      });
      req.on('error', (e: Error) => { this._lastErr = e; this.events.get('error')?.(e); this._tryReconnect(() => this._connectNodeWS()); });
      req.end();
    } catch (e) { this._lastErr = e instanceof Error ? e : new Error(String(e)); this._tryReconnect(() => this._connectNodeWS()); }
  }

  private _processNodeWS(data: Buffer) {
    if (!this._wsParser) return;
    let frames; try { frames = this._wsParser.feed(data); } catch { this.log.error('WS parse error'); return; }
    for (const f of frames) { this._recv++; this._handleNodeFrame(f); }
  }

  private _handleNodeFrame(f: { opcode: number; payload: Buffer }) {
    if (f.opcode === OP_PING) { if (this._nodeSock && !this._nodeSock.destroyed) try { this._nodeSock.write(createWSPongFrameMasked()); } catch {} return; }
    if (f.opcode === OP_PONG) return;
    if (f.opcode === OP_CLOSE) { if (this._nodeSock && !this._nodeSock.destroyed) try { this._nodeSock.end(); } catch {} return; }

    if (f.opcode === OP_TEXT) {
      try {
        const msg = JSON.parse(f.payload.toString('utf8')), { event, data, _isAck } = msg;
        if (event === 'ping') return;
        this.opts.hooks.onMessage?.({ event, data, isBinary: false });
        if (_isAck) {
          const key = msg._correlationId || event;
          if (this._acks.has(key)) { const e = this._acks.get(key)!; if (e.timer) clearTimeout(e.timer); this._acks.delete(key); e.handler(data); return; }
        }
        this.events.get(event)?.(data);
        this._wild?.({ event, data });
      } catch {}
      return;
    }

    if (f.opcode === OP_BINARY) {
      const parsed = parseWSBinary(f.payload);
      if (!parsed) return;
      this.opts.hooks.onMessage?.({ event: parsed.event, data: parsed.buffer, isBinary: true });
      this.events.get(parsed.event)?.(parsed.buffer);
      this._wild?.({ event: parsed.event, data: parsed.buffer, isBinary: true, buffer: parsed.buffer });
    }
  }

  /* ── TCP ── */

  private async _connectTCP() {
    try {
      await loadModules(); if (!_net) return;
      const parsed = new URL(this.url), port = parseInt(parsed.port) + 1 || 3001, host = parsed.hostname || 'localhost';
      const sockOpts: { host: string; port: number; rejectUnauthorized?: boolean } = { host, port };
      const socket = this.opts.tls && _tls ? _tls.connect({ ...sockOpts, rejectUnauthorized: this.opts.rejectUnauthorized }) as any : _net.createConnection(sockOpts);
      socket.setTimeout(this.opts.ackTimeout, () => socket.destroy(new Error('TCP timeout')));
      socket.on('connect', () => { socket.setTimeout(0); this._tcpParser = new FrameParser(this.opts.maxFrameSize); this.log.info('TCP connected', { host, port }); this._onConnected(); });
      socket.on('data', (d: Buffer) => { if (!this._tcpParser) return; let frames; try { frames = this._tcpParser.feed(d); } catch { this.log.error('TCP parse error'); socket.destroy(); return; } for (const f of frames) { this._recv++; this._handleTCPFrame(f); } });
      socket.on('close', () => { this._setState('disconnected'); this._fullCleanup(); this.events.get('disconnect')?.(undefined); this._tryReconnect(() => this._connectTCP()); });
      socket.on('error', (e: Error) => { this._lastErr = e; this.events.get('error')?.(e); this.opts.hooks.onError?.({ error: e, context: 'tcp' }); });
      socket.on('drain', () => this._flushQueue());
      this._tcpSock = socket;
    } catch (e) { this._lastErr = e instanceof Error ? e : new Error(String(e)); this._tryReconnect(() => this._connectTCP()); }
  }

  private _handleTCPFrame(f: { type: number; event: string; payload: Buffer }) {
    const { type, event, payload } = f;
    if (type === FRAME_PING) { if (this._tcpSock && !this._tcpSock.destroyed) try { this._tcpSock.write(encodePongFrame()); } catch {} return; }
    if (type === FRAME_PONG) return;
    if (type === FRAME_CONNECT) { this.id = payload.toString('utf8'); return; }
    if (type === FRAME_ACK_RES) {
      try {
        const raw = JSON.parse(payload.toString('utf8'));
        const data = raw && typeof raw === 'object' && 'data' in raw ? raw.data : raw;
        const corrId = raw && typeof raw === 'object' && '_correlationId' in raw ? String(raw._correlationId) : undefined;
        const key = corrId || event;
        if (this._acks.has(key)) { const e = this._acks.get(key)!; if (e.timer) clearTimeout(e.timer); this._acks.delete(key); e.handler(data); }
      } catch {}
      return;
    }
    if (type === FRAME_JSON) {
      try {
        const data = JSON.parse(payload.toString('utf8'));
        this.opts.hooks.onMessage?.({ event, data, isBinary: false });
        this.events.get(event)?.(data);
        this._wild?.({ event, data });
      } catch {}
      return;
    }
    if (type === FRAME_BINARY) {
      const copy = Buffer.from(payload);
      const buf = copy.buffer.slice(copy.byteOffset, copy.byteOffset + copy.byteLength) as ArrayBuffer;
      this.opts.hooks.onMessage?.({ event, data: buf, isBinary: true });
      this.events.get(event)?.(buf);
      this._wild?.({ event, data: buf, isBinary: true, buffer: buf });
    }
  }
}

export default StelarClient;
