/**
 * @stelar-time-real Server
 *
 * Dual-protocol real-time server: WebSocket (RFC 6455) + custom binary TCP.
 * Zero external dependencies — uses only Node.js built-in modules.
 */
import { IncomingMessage, Server as HttpServer, ServerResponse } from 'http';
import { Socket as NetSocket } from 'net';
import type { TlsOptions } from 'tls';
import { Logger, type LogLevel } from './logger.js';
export interface IRateLimiter {
    /** Returns true if the action is allowed */
    check(id: string, cost?: number): boolean;
    /** Reset rate limit for a specific client */
    reset(id: string): void;
    /** Clean up expired entries */
    cleanup(): void;
    /** Number of tracked entries */
    size(): number;
}
export interface IIPTracker {
    /** Returns true if connection from this IP is allowed */
    check(ip: string): boolean;
    /** Register a new connection from this IP */
    add(ip: string): void;
    /** Unregister a connection from this IP */
    remove(ip: string): void;
    /** Get current connection count for this IP */
    getCount(ip: string): number;
    /** Clean up stale entries */
    cleanup(): void;
}
export interface StelarHooks {
    /** Called when a client exceeds rate limit. Return false to skip disconnect. */
    onRateLimitExceeded?: (info: {
        clientId: string;
        event?: string;
        protocol: 'ws' | 'tcp';
    }) => boolean | void;
    /** Called when max connections is reached. */
    onMaxConnectionsReached?: (info: {
        activeConnections: number;
        max: number;
        ip: string;
    }) => void;
    /** Called when global max rooms is reached. Return false to reject room creation. */
    onMaxRoomsReached?: (info: {
        clientId: string;
        room: string;
        totalRooms: number;
        max: number;
    }) => boolean | void;
    /** Called when per-client max rooms is reached. Return false to reject join. */
    onMaxRoomsPerClientReached?: (info: {
        clientId: string;
        room: string;
        currentRooms: number;
        max: number;
    }) => boolean | void;
    /** Called when a payload exceeds maxPayloadSize. */
    onPayloadTooLarge?: (info: {
        clientId: string;
        event?: string;
        size: number;
        max: number;
    }) => void;
    /** Called when a client sends an invalid message. */
    onInvalidMessage?: (info: {
        clientId: string;
        reason: string;
        protocol: 'ws' | 'tcp';
    }) => void;
    /** Called before a client joins a room. Return false to reject. */
    onClientJoinRoom?: (info: {
        clientId: string;
        room: string;
        metadata: Map<string, unknown>;
    }) => boolean | void;
    /** Called before a client leaves a room. Return false to reject. */
    onClientLeaveRoom?: (info: {
        clientId: string;
        room: string;
    }) => boolean | void;
    /** Called before a broadcast. Return false to cancel. */
    onBeforeBroadcast?: (info: {
        event: string;
        data: unknown;
        excludeId?: string;
    }) => boolean | void;
    /** Called when a new client connects. */
    onClientConnect?: (info: {
        clientId: string;
        ip: string;
        protocol: 'ws' | 'tcp';
        metadata: Map<string, unknown>;
    }) => void;
    /** Called when a client disconnects. */
    onClientDisconnect?: (info: {
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
    maxEventNameLength?: number;
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
    /** Custom rate limiter implementation. Replaces the built-in token bucket. */
    customRateLimiter?: IRateLimiter;
    /** Custom IP connection tracker. Replaces the built-in per-IP counter. */
    customIPTracker?: IIPTracker;
    /** Custom function to generate client IDs. Defaults to UUID v4. */
    generateClientId?: () => string;
    /** Per-event rate limits. Each event can have different maxPoints and windowMs. */
    eventRateLimits?: EventRateLimits;
    /** Hook callbacks for server events. */
    hooks?: StelarHooks;
    /** Custom health check handler. Receives (req, res, stats). */
    customHealthHandler?: (req: IncomingMessage, res: ServerResponse, stats: StelarStats) => void;
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
    private namespace;
    private heartbeatInterval;
    private heartbeatTimeout;
    private tcpPort;
    private maxConnections;
    private maxRooms;
    private maxRoomsPerClient;
    private maxPayloadSize;
    private maxFrameSize;
    private maxWSFrameSize;
    private connectTimeout;
    private doGracefulShutdown;
    private shutdownTimeout;
    private healthEndpoint;
    private tlsOptions;
    private allowedOrigins;
    private _customRateLimiter;
    private _customIPTracker;
    private _generateClientId;
    private _customHealthHandler;
    private hooks;
    private eventRateLimiters;
    private _clientRateOverrides;
    private clients;
    private clientsById;
    private rooms;
    private events;
    private middlewares;
    private _hbTimer;
    private _rateCleanupTimer;
    private _wildcardHandler;
    private _connectionHandler;
    private _acks;
    private _externalServers;
    private _upgradeHandler;
    private _requestHandler;
    private _started;
    private _startTime;
    private _shuttingDown;
    private _sigintHandler;
    private _sigtermHandler;
    private rateLimiter;
    private ipTracker;
    private _totalConnections;
    private _totalMessagesReceived;
    private _totalMessagesSent;
    private log;
    constructor(options?: StelarOptions);
    static of(path: string, options?: StelarOptions): StelarServer;
    /** Update server configuration at runtime. */
    updateConfig(options: Partial<StelarOptions>): this;
    /** Set a per-client rate limit override. */
    setClientRateLimit(clientId: string, config: {
        maxPoints: number;
        windowMs: number;
    }): this;
    /** Remove a per-client rate limit override, falling back to the global limiter. */
    removeClientRateLimit(clientId: string): this;
    /** Set a per-event rate limit. */
    setEventRateLimit(event: string, config: {
        maxPoints: number;
        windowMs: number;
    }): this;
    /** Remove a per-event rate limit. */
    removeEventRateLimit(event: string): this;
    /** Get the current server configuration as a read-only object. */
    getConfig(): Readonly<{
        maxConnections: number;
        maxConnectionsPerIP: number;
        maxRooms: number;
        maxRoomsPerClient: number;
        maxPayloadSize: number;
        heartbeatInterval: number;
        heartbeatTimeout: number;
        connectTimeout: number;
        shutdownTimeout: number;
        hasCustomRateLimiter: boolean;
        hasCustomIPTracker: boolean;
        hasCustomClientIdGenerator: boolean;
        hasCustomHealthHandler: boolean;
        eventRateLimits: string[];
        hooks: string[];
        allowedOrigins: string[] | null;
    }>;
    use(middleware: StelarMiddleware): this;
    on(event: string, handler: StelarEventHandler): this;
    onAll(handler: StelarWildcardHandler): this;
    onConnection(handler: StelarEventHandler): this;
    onDisconnect(handler: StelarEventHandler): this;
    onAck(name: string, handler: StelarEventHandler): this;
    broadcast(event: string, data: unknown, excludeId?: string): this;
    broadcastBinary(event: string, buffer: ArrayBuffer): void;
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
    private _getRateLimiterSize;
    /** Check rate limit. Priority: per-client override > event-specific > custom/global. */
    private _checkRateLimit;
    private _sendJsonToClient;
    private _sendBinaryRaw;
    private _joinRoom;
    private _leaveRoom;
    private _removeFromAllRooms;
    private _buildCtx;
    private runMiddlewares;
    private startHeartbeat;
    private _getClientIP;
    private _registerClient;
    private _unregisterClient;
    private _checkOrigin;
    private handleWSUpgrade;
    private _processWSData;
    private _handleWSFrame;
    private handleTCPConnection;
    private _processTCPData;
    private _handleTCPFrame;
    private _handleError;
    private _handleHealthCheck;
    private _shutdownCallbacks;
    /** Register a callback for when graceful shutdown completes. */
    onShutdown(callback: (signal: string, force: boolean) => void): this;
    private _emitShutdown;
    private _setupGracefulShutdown;
    private _removeSignalHandlers;
    start(callback?: (port: number) => void): Promise<number>;
    private _startTCPServer;
    private _startPlainTCPServer;
    stop(): this;
}
export default StelarServer;
export { StelarServer };
export { default as StelarClient } from './client.js';
export { Logger, NULL_LOGGER, type LogLevel } from './logger.js';
export { ProtocolError, validateEventName, DEFAULT_MAX_FRAME_SIZE, MAX_EVENT_LENGTH, HEADER_SIZE } from './protocol.js';
export { WebSocketError, DEFAULT_MAX_WS_FRAME_SIZE, CLOSE_NORMAL, CLOSE_GOING_AWAY, CLOSE_PROTOCOL_ERROR, CLOSE_POLICY_VIOLATION, CLOSE_MESSAGE_TOO_BIG, CLOSE_INVALID_PAYLOAD, CLOSE_UNSUPPORTED } from './websocket.js';
//# sourceMappingURL=index.d.ts.map