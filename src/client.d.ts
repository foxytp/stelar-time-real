export interface StelarClientOptions {
    reconnection?: boolean;
    reconnectionAttempts?: number;
    reconnectionDelay?: number;
    heartbeatInterval?: number;
    ackTimeout?: number;
}
export interface StelarEmitOptions {
    ack?: string;
}
export type StelarEventHandler = (data: unknown) => void;
export type StelarBinaryHandler = (buffer: ArrayBuffer) => void;
declare class StelarClient {
    private url;
    private options;
    private ws;
    private events;
    private _wildcardHandler;
    private connected;
    private id;
    private _reconnectAttempts;
    private _hbTimer;
    private _isManualClose;
    private _acks;
    constructor(urlOrPort?: string | number, options?: StelarClientOptions);
    setUrl(url: string): this;
    on(event: string, handler: StelarEventHandler): this;
    off(event: string, handler: StelarEventHandler): this;
    onAll(handler: (data: {
        event: string;
        data: unknown;
        isBinary?: boolean;
        buffer?: ArrayBuffer;
    }) => void): this;
    onAck(name: string, handler: StelarEventHandler): this;
    emit(event: string, data?: unknown, opts?: StelarEmitOptions): this;
    emitBinary(event: string, data: ArrayBuffer): this;
    sendFile(file: ArrayBuffer): this;
    sendImage(blob: ArrayBuffer): this;
    request(event: string, data: unknown, ackName: string): Promise<unknown>;
    joinRoom(room: string): this;
    leaveRoom(): this;
    private startHeartbeat;
    private stopHeartbeat;
    private _connect;
    connect(callback?: () => void): this;
    disconnect(): this;
    isConnected(): boolean;
    getUrl(): string;
}
export default StelarClient;
//# sourceMappingURL=client.d.ts.map