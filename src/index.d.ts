import { IncomingMessage, Server } from 'http';
import { WebSocket } from 'ws';
export interface StelarOptions {
    port?: number;
    server?: Server;
    namespace?: string;
    heartbeatInterval?: number;
}
export interface StelarClientInfo {
    id: string;
    room: string | null;
    lastPing: number;
}
export interface StelarContext {
    id: string;
    socket: WebSocket;
    req: IncomingMessage;
    data?: unknown;
    buffer?: Uint8Array;
    isBinary?: boolean;
    event?: string;
    error?: Error;
    emit: (event: string, data: unknown) => void;
    send: (respId: string, data: unknown) => void;
    emitBinary: (event: string, buffer: ArrayBuffer) => void;
    broadcast: (event: string, data: unknown) => void;
    broadcastBinary: (event: string, buffer: ArrayBuffer) => void;
    to: (room: string, event: string, data: unknown) => void;
    toId: (id: string, event: string, data: unknown) => void;
    getClients: (room?: string) => {
        id: string;
        room: string | null;
    }[];
    joinRoom: (room: string) => void;
    leaveRoom: () => void;
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
declare class StelarServer {
    private port;
    private server;
    private namespace;
    private wss;
    private clients;
    private events;
    private middlewares;
    private heartbeatInterval;
    private _hbTimer;
    private _wildcardHandler;
    private _connectionHandler;
    private _acks;
    private _externalServers;
    constructor(options?: StelarOptions);
    static of(path: string, options?: StelarOptions): StelarServer;
    use(middleware: StelarMiddleware): this;
    on(event: string, handler: StelarEventHandler): this;
    onAll(handler: StelarWildcardHandler): this;
    onConnection(handler: StelarEventHandler): this;
    onAck(name: string, handler: StelarEventHandler): this;
    broadcast(event: string, data: unknown): this;
    broadcastBinary(event: string, buffer: ArrayBuffer): void;
    to(room: string, event: string, data: unknown): this;
    toId(id: string, event: string, data: unknown): this;
    getClients(room?: string): {
        id: string;
        room: string | null;
    }[];
    getPort(): number;
    private runMiddlewares;
    private startHeartbeat;
    private handleConnection;
    start(callback?: (port: number) => void): Promise<number>;
    stop(): this;
}
export default StelarServer;
export { StelarServer };
export { default as StelarClient } from './client.js';
//# sourceMappingURL=index.d.ts.map