/**
 * @stelar-time-real Client — Browser WS / Node WS / binary TCP
 */
import { Logger, type LogLevel } from './logger.js';
export interface StelarClientHooks {
    onBeforeEmit?: (i: {
        event: string;
        data: unknown;
    }) => boolean | void;
    onMessage?: (i: {
        event: string;
        data: unknown;
        isBinary: boolean;
    }) => void;
    onStateChange?: (i: {
        from: ConnectionState;
        to: ConnectionState;
    }) => void;
    onReconnectDelay?: (i: {
        attempt: number;
        defaultDelay: number;
    }) => number | void;
    onMessageQueued?: (i: {
        event: string;
        data: unknown;
        queueSize: number;
    }) => void;
    onQueueDrained?: (i: {
        count: number;
    }) => void;
    onError?: (i: {
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
    customReconnectDelay?: (attempt: number, baseDelay: number, maxDelay: number) => number;
    hooks?: StelarClientHooks;
}
export interface StelarEmitOptions {
    ack?: string;
    _correlationId?: string;
}
export type StelarEventHandler = (data: unknown) => void;
export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting';
declare class StelarClient {
    private url;
    private opts;
    private events;
    private _wild;
    private _acks;
    private _state;
    private _reconnAttempts;
    private _hb;
    private _manualClose;
    private id;
    private _mq;
    private _reconnTimer;
    private _ackCounter;
    private _sent;
    private _recv;
    private _connTime;
    private _lastErr;
    private _ws;
    private _nodeSock;
    private _wsParser;
    private _tcpSock;
    private _tcpParser;
    private log;
    constructor(urlOrPort?: string | number, o?: StelarClientOptions);
    getState(): ConnectionState;
    getId(): string | null;
    getUrl(): string;
    getMessagesSent(): number;
    getMessagesReceived(): number;
    getLastError(): Error | null;
    getQueueSize(): number;
    getConnectTime(): number;
    setUrl(u: string): this;
    updateOptions(o: Partial<StelarClientOptions>): this;
    getOptions(): Readonly<{
        reconnection: boolean;
        reconnectionAttempts: number;
        reconnectionDelay: number;
        maxReconnectionDelay: number;
        heartbeatInterval: number;
        ackTimeout: number;
        mode: "ws" | "tcp";
        maxPayloadSize: number;
        messageQueueSize: number;
        hasCustomReconnectDelay: boolean;
        hooks: string[];
    }>;
    on(ev: string, h: StelarEventHandler): this;
    off(ev: string, h: StelarEventHandler): this;
    once(ev: string, h: StelarEventHandler): this;
    onAll(h: (d: {
        event: string;
        data: unknown;
        isBinary?: boolean;
        buffer?: ArrayBuffer;
    }) => void): this;
    onAck(name: string, h: StelarEventHandler): this;
    removeAllListeners(ev?: string): this;
    emit(event: string, data?: unknown, opts?: StelarEmitOptions): this;
    emitBinary(event: string, data: ArrayBuffer): this;
    sendFile(f: ArrayBuffer): this;
    sendImage(b: ArrayBuffer): this;
    request(event: string, data: unknown, ackName: string): Promise<unknown>;
    joinRoom(room: string): this;
    leaveRoom(room: string): this;
    connect(cb?: () => void): this;
    disconnect(): this;
    isConnected(): boolean;
    private _setState;
    private _startHB;
    private _stopHB;
    private _getDelay;
    private _drain;
    private _cleanupAcks;
    private _fullCleanup;
    private _tryReconnect;
    private _onConnected;
    private _connectBrowser;
    private _handleBrowserMsg;
    private _connectNodeWS;
    private _processNodeWS;
    private _handleNodeFrame;
    private _connectTCP;
    private _handleTCPFrame;
}
export default StelarClient;
//# sourceMappingURL=client.d.ts.map