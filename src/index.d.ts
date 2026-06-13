/** @stelar-time-real Server — Dual-protocol: WebSocket (RFC 6455) + binary TCP */
import { IncomingMessage, Server as HttpServer, ServerResponse } from 'http';
import { Socket as NetSocket } from 'net';
import { TlsOptions } from 'tls';
import { Logger, type LogLevel } from './logger.js';
export interface IRateLimiter {
    check(id: string, cost?: number): boolean;
    reset(id: string): void;
    cleanup(): void;
    size(): number;
}
export interface IIPTracker {
    check(ip: string): boolean;
    add(ip: string): void;
    remove(ip: string): void;
    getCount(ip: string): number;
    cleanup(): void;
}
export interface StelarHooks {
    onRateLimitExceeded?: (i: {
        clientId: string;
        event?: string;
        protocol: 'ws' | 'tcp';
    }) => boolean | void;
    onMaxConnectionsReached?: (i: {
        activeConnections: number;
        max: number;
        ip: string;
    }) => void;
    onMaxRoomsReached?: (i: {
        clientId: string;
        room: string;
        totalRooms: number;
        max: number;
    }) => boolean | void;
    onMaxRoomsPerClientReached?: (i: {
        clientId: string;
        room: string;
        currentRooms: number;
        max: number;
    }) => boolean | void;
    onPayloadTooLarge?: (i: {
        clientId: string;
        event?: string;
        size: number;
        max: number;
    }) => void;
    onInvalidMessage?: (i: {
        clientId: string;
        reason: string;
        protocol: 'ws' | 'tcp';
    }) => void;
    onClientJoinRoom?: (i: {
        clientId: string;
        room: string;
        metadata: Map<string, unknown>;
    }) => boolean | void;
    onClientLeaveRoom?: (i: {
        clientId: string;
        room: string;
    }) => boolean | void;
    onBeforeBroadcast?: (i: {
        event: string;
        data: unknown;
        excludeId?: string;
    }) => boolean | void;
    onClientConnect?: (i: {
        clientId: string;
        ip: string;
        protocol: 'ws' | 'tcp';
        metadata: Map<string, unknown>;
    }) => void;
    onClientDisconnect?: (i: {
        clientId: string;
        ip: string;
        protocol: 'ws' | 'tcp';
        rooms: Set<string>;
    }) => void;
}
export type EventRateLimits = Record<string, {
    maxPoints: number;
    windowMs: number;
}>;
export interface StelarOptions {
    port?: number;
    server?: HttpServer;
    namespace?: string;
    heartbeatInterval?: number;
    heartbeatTimeout?: number;
    tcpPort?: number | false;
    maxConnections?: number;
    maxConnectionsPerIP?: number;
    maxRooms?: number;
    maxRoomsPerClient?: number;
    maxPayloadSize?: number;
    maxFrameSize?: number;
    rateLimit?: {
        maxPoints?: number;
        windowMs?: number;
    } | false;
    connectTimeout?: number;
    gracefulShutdown?: boolean;
    shutdownTimeout?: number;
    healthEndpoint?: string | false;
    logger?: Logger | LogLevel | false;
    tls?: TlsOptions;
    allowedOrigins?: string[];
    customRateLimiter?: IRateLimiter;
    customIPTracker?: IIPTracker;
    generateClientId?: () => string;
    eventRateLimits?: EventRateLimits;
    hooks?: StelarHooks;
    customHealthHandler?: (req: IncomingMessage, res: ServerResponse, stats: StelarStats) => void;
    compression?: boolean;
}
export interface StelarClientInfo {
    id: string;
    rooms: Set<string>;
    lastPing: number;
    protocol: 'ws' | 'tcp';
    connectedAt: number;
    metadata: Map<string, unknown>;
    messagesReceived: number;
    messagesSent: number;
    remoteAddress: string;
}
export interface StelarContext {
    id: string;
    socket: NetSocket;
    req: IncomingMessage | null;
    data?: unknown;
    buffer?: Uint8Array;
    isBinary?: boolean;
    event?: string;
    error?: Error;
    _correlationId?: string;
    clientInfo: StelarClientInfo;
    emit: (event: string, data: unknown) => void;
    send: (respId: string, data: unknown) => void;
    emitBinary: (event: string, buffer: ArrayBuffer) => void;
    broadcast: (event: string, data: unknown) => void;
    broadcastBinary: (event: string, buffer: ArrayBuffer) => void;
    to: (room: string, event: string, data: unknown) => void;
    toId: (id: string, event: string, data: unknown) => void;
    getClients: (room?: string) => {
        id: string;
        rooms: string[];
    }[];
    joinRoom: (room: string) => void;
    leaveRoom: (room: string) => void;
    setMetadata: (key: string, value: unknown) => void;
    getMetadata: (key: string) => unknown;
    ack: (ackName: string, data: unknown) => void;
}
export interface StelarMiddleware {
    (ctx: StelarContext, next: () => void): void;
}
export type StelarEventHandler = (ctx: StelarContext) => void;
export type StelarWildcardHandler = (data: {
    event: string;
    data: StelarContext;
}) => void;
export interface StelarStats {
    totalConnections: number;
    activeConnections: number;
    totalMessagesReceived: number;
    totalMessagesSent: number;
    totalRooms: number;
    uptime: number;
    wsConnections: number;
    tcpConnections: number;
    memoryUsage: NodeJS.MemoryUsage;
    rateLimiterEntries: number;
}
declare class StelarServer {
    private port;
    private httpServer;
    private tcpServer;
    private ns;
    private hbInterval;
    private hbTimeout;
    private tcpPort;
    private maxConns;
    private maxRooms;
    private maxRoomsPerClient;
    private maxPayload;
    private maxFrame;
    private maxWSFrame;
    private connTimeout;
    private doGraceful;
    private shutdownMs;
    private healthPath;
    private tlsOpts;
    private origins;
    private _crl;
    private _cit;
    private _genId;
    private _healthFn;
    private hooks;
    private evRateLimits;
    private clientRates;
    private doCompress;
    private clients;
    private byId;
    private rooms;
    private events;
    private mw;
    private _rc;
    private _wild;
    private _connH;
    private _acks;
    private _ext;
    private _upgH;
    private _reqH;
    private _started;
    private _startTime;
    private _shutting;
    private _sigH;
    private rateLimiter;
    private ipTracker;
    private _totalConns;
    private _totalRecv;
    private _totalSent;
    private _shutdownCbs;
    private log;
    constructor(o?: StelarOptions);
    static of(path: string, o?: StelarOptions): StelarServer;
    updateConfig(o: Partial<StelarOptions>): this;
    setClientRateLimit(id: string, c: {
        maxPoints: number;
        windowMs: number;
    }): this;
    removeClientRateLimit(id: string): this;
    setEventRateLimit(ev: string, c: {
        maxPoints: number;
        windowMs: number;
    }): this;
    removeEventRateLimit(ev: string): this;
    getConfig(): Readonly<{
        maxConnections: number;
        maxConnectionsPerIP: -1 | 50;
        maxRooms: number;
        maxRoomsPerClient: number;
        maxPayloadSize: number;
        heartbeatInterval: number;
        heartbeatTimeout: number;
        connectTimeout: number;
        shutdownTimeout: number;
        compression: boolean;
        hasCustomRateLimiter: boolean;
        hasCustomIPTracker: boolean;
        hasCustomClientIdGenerator: boolean;
        hasCustomHealthHandler: boolean;
        eventRateLimits: string[];
        hooks: string[];
        allowedOrigins: string[] | null;
    }>;
    use(mw: StelarMiddleware): this;
    on(ev: string, h: StelarEventHandler): this;
    onAll(h: StelarWildcardHandler): this;
    onConnection(h: StelarEventHandler): this;
    onDisconnect(h: StelarEventHandler): this;
    onAck(name: string, h: StelarEventHandler): this;
    broadcast(event: string, data: unknown, excludeId?: string): this;
    broadcastBinary(event: string, buf: ArrayBuffer): void;
    to(room: string, event: string, data: unknown, excludeId?: string): this;
    toId(id: string, event: string, data: unknown): this;
    getClients(room?: string): {
        id: string;
        rooms: string[];
    }[];
    getRoomMembers(room: string): string[];
    getRooms(): string[];
    getPort(): number;
    getStats(): StelarStats;
    onShutdown(cb: (sig: string, force: boolean) => void): this;
    private _write;
    private _sendJson;
    private _sendBin;
    private _flushQueue;
    private _checkRate;
    private _getIP;
    private _startClientHB;
    private _stopClientHB;
    private _register;
    private _unregister;
    private _joinRoom;
    private _leaveRoom;
    private _buildCtx;
    private _runMw;
    private _dispatch;
    private _wsUpgrade;
    private _processWS;
    private _handleWSFrame;
    private _tcpConnect;
    private _processTCP;
    private _handleTCPFrame;
    private _handleErr;
    private _health;
    private _emitShutdown;
    private _setupShutdown;
    private _removeSignals;
    start(cb?: (port: number) => void): Promise<number>;
    private _startTCP;
    stop(): this;
}
export default StelarServer;
export { StelarServer };
export { default as StelarClient } from './client.js';
export { Logger, NULL_LOGGER, type LogLevel } from './logger.js';
export { ProtocolError, validateEventName, DEFAULT_MAX_FRAME_SIZE, MAX_EVENT_LENGTH, HEADER_SIZE } from './protocol.js';
export { WebSocketError, DEFAULT_MAX_WS_FRAME_SIZE, CLOSE_NORMAL, CLOSE_GOING_AWAY, CLOSE_PROTOCOL_ERROR, CLOSE_POLICY_VIOLATION, CLOSE_MESSAGE_TOO_BIG, CLOSE_INVALID_PAYLOAD, CLOSE_UNSUPPORTED } from './websocket.js';
//# sourceMappingURL=index.d.ts.map