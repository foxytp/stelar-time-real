/** @stelar-time-real WebSocket (RFC 6455) + permessage-deflate (RFC 7692) */
export declare const DEFAULT_MAX_WS_FRAME_SIZE: number;
export declare const OP_CONTINUATION = 0, OP_TEXT = 1, OP_BINARY = 2, OP_CLOSE = 8, OP_PING = 9, OP_PONG = 10;
export declare const CLOSE_NORMAL = 1000, CLOSE_GOING_AWAY = 1001, CLOSE_PROTOCOL_ERROR = 1002, CLOSE_UNSUPPORTED = 1003, CLOSE_INVALID_PAYLOAD = 1007, CLOSE_POLICY_VIOLATION = 1008, CLOSE_MESSAGE_TOO_BIG = 1009, CLOSE_INTERNAL_ERROR = 1011;
export declare class WebSocketError extends Error {
    code: number;
    constructor(message: string, code?: number);
}
export declare const computeAcceptKey: (key: string) => string;
export declare const generateWSKey: () => string;
export declare const validateWSKey: (key: string) => boolean;
export declare function buildUpgradeResponse(key: string, headers?: Record<string, string>, compress?: boolean): string;
/** Parse client's Sec-WebSocket-Extensions header to check for permessage-deflate */
export declare function clientWantsCompression(extensionsHeader: string | undefined): boolean;
export interface WSFrame {
    fin: boolean;
    opcode: number;
    payload: Buffer;
    masked: boolean;
    rsv1: boolean;
}
export declare function wsCompress(data: Buffer): Buffer;
export declare function wsDecompress(data: Buffer): Buffer;
export declare function parseWSFrame(buf: Buffer, max?: number): {
    frame: WSFrame;
    consumed: number;
} | null;
export declare function createWSFrame(opcode: number, payload: Buffer | string, masked?: boolean, compress?: boolean): Buffer;
export declare const createWSTextFrame: (msg: string, compress?: boolean) => Buffer<ArrayBufferLike>;
export declare const createWSBinaryFrame: (data: Buffer) => Buffer<ArrayBufferLike>;
export declare const createWSCloseFrame: (code?: number, reason?: string) => Buffer<ArrayBufferLike>;
export declare const createWSPingFrame: (data?: Buffer) => Buffer<ArrayBufferLike>;
export declare const createWSPongFrame: (data?: Buffer) => Buffer<ArrayBufferLike>;
export declare const createWSTextFrameMasked: (msg: string, compress?: boolean) => Buffer<ArrayBufferLike>;
export declare const createWSBinaryFrameMasked: (data: Buffer) => Buffer<ArrayBufferLike>;
export declare const createWSCloseFrameMasked: (code?: number, reason?: string) => Buffer<ArrayBufferLike>;
export declare const createWSPingFrameMasked: () => Buffer<ArrayBufferLike>;
export declare const createWSPongFrameMasked: () => Buffer<ArrayBufferLike>;
/** Streaming WS frame parser — O(1) append, avoids Buffer.concat O(n²) */
export declare class WSFrameParser {
    private chunks;
    private len;
    private max;
    private received;
    constructor(max?: number);
    private _compact;
    feed(data: Buffer): WSFrame[];
    reset(): void;
    getBytesReceived(): number;
}
//# sourceMappingURL=websocket.d.ts.map