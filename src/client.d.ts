/**
 * @stelar-time-real Client
 *
 * Dual-environment: Browser (native WebSocket) + Node.js (manual WS or TCP binary).
 * No external dependencies.
 */
import { Logger, type LogLevel } from './logger.js';
export interface StelarClientHooks {
    /** Return false to cancel the emit. */
    onBeforeEmit?: (info: {
        event: string;
        data: unknown;
    }) => boolean | void;
    /** Called on every incoming message. */
    onMessage?: (info: {
        event: string;
        data: unknown;
        isBinary: boolean;
    }) => void;
    /** Called when connection state changes. */
    onStateChange?: (info: {
        from: ConnectionState;
        to: ConnectionState;
    }) => void;
    /** Return a custom delay (ms) to override built-in backoff. */
    onReconnectDelay?: (info: {
        attempt: number;
        defaultDelay: number;
    }) => number | void;
    /** Called when a message is queued while disconnected. */
    onMessageQueued?: (info: {
        event: string;
        data: unknown;
        queueSize: number;
    }) => void;
    /** Called after queued messages are flushed on reconnection. */
    onQueueDrained?: (info: {
        count: number;
    }) => void;
    /** Called on any client-side error. */
    onError?: (info: {
        error: Error;
        context: string;
    }) => void;
}
export interface StelarClientOptions {
    reconnection?: boolean;
    reconnectionAttempts?: number;
    reconnectionDelay?: number;
    maxReconnectionDelay?: number;
    heartbeatInterval?: number;
    ackTimeout?: number;
    mode?: 'ws' | 'tcp';
    maxPayloadSize?: number;
    maxFrameSize?: number;
    messageQueueSize?: number;
    logger?: Logger | LogLevel | false;
    tls?: boolean;
    rejectUnauthorized?: boolean;
    headers?: Record<string, string>;
    /** Custom backoff function: (attempt, baseDelay, maxDelay) => delayMs */
    customReconnectDelay?: (attempt: number, baseDelay: number, maxDelay: number) => number;
    hooks?: StelarClientHooks;
}
export interface StelarEmitOptions {
    ack?: string;
}
export type StelarEventHandler = (data: unknown) => void;
export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting';
declare class StelarClient {
    private url;
    private options;
    private events;
    private _wildcardHandler;
    private _acks;
    private _state;
    private _reconnectAttempts;
    private _hbTimer;
    private _isManualClose;
    private id;
    private _messageQueue;
    private _reconnectTimer;
    private _messagesSent;
    private _messagesReceived;
    private _connectTime;
    private _lastError;
    private _ws;
    private _nodeSocket;
    private _wsParser;
    private _tcpSocket;
    private _tcpParser;
    private log;
    constructor(urlOrPort?: string | number, options?: StelarClientOptions);
    getState(): ConnectionState;
    getId(): string | null;
    getUrl(): string;
    getMessagesSent(): number;
    getMessagesReceived(): number;
    getLastError(): Error | null;
    getQueueSize(): number;
    getConnectTime(): number;
    setUrl(url: string): this;
    /** Update client options at runtime. Changes take effect immediately. */
    updateOptions(options: Partial<StelarClientOptions>): this;
    /** Read-only snapshot of current client options. */
    getOptions(): Readonly<{
        reconnection: boolean;
        reconnectionAttempts: number;
        reconnectionDelay: number;
        maxReconnectionDelay: number;
        heartbeatInterval: number;
        ackTimeout: number;
        mode: string;
        maxPayloadSize: number;
        messageQueueSize: number;
        hasCustomReconnectDelay: boolean;
        hooks: string[];
    }>;
    on(event: string, handler: StelarEventHandler): this;
    off(event: string, handler: StelarEventHandler): this;
    once(event: string, handler: StelarEventHandler): this;
    onAll(handler: (data: {
        event: string;
        data: unknown;
        isBinary?: boolean;
        buffer?: ArrayBuffer;
    }) => void): this;
    onAck(name: string, handler: StelarEventHandler): this;
    removeAllListeners(event?: string): this;
    emit(event: string, data?: unknown, opts?: StelarEmitOptions): this;
    emitBinary(event: string, data: ArrayBuffer): this;
    sendFile(file: ArrayBuffer): this;
    sendImage(blob: ArrayBuffer): this;
    /** Send a request and wait for an ACK response. Rejects on timeout. */
    request(event: string, data: unknown, ackName: string): Promise<unknown>;
    joinRoom(room: string): this;
    leaveRoom(room: string): this;
    private startHeartbeat;
    private stopHeartbeat;
    private _getReconnectDelay;
    private _drainQueue;
    private _setState;
    private _cleanupAcks;
    private _fullCleanup;
    connect(callback?: () => void): this;
    disconnect(): this;
    isConnected(): boolean;
    private _connectBrowser;
    private _handleBrowserMessage;
    private _connectNodeWS;
    private _processNodeWSData;
    private _handleNodeWSFrame;
    /** TCP mode: connects to WS port + 1 by convention. */
    private _connectTCP;
    private _handleTCPFrame;
    private _tryReconnect;
}
export default StelarClient;
//# sourceMappingURL=client.d.ts.map