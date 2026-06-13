/**
 * @stelar-time-real WebSocket Protocol (RFC 6455)
 *
 * Hand-crafted implementation with no external dependencies.
 * Uses Node.js built-in crypto for handshake and frame masking.
 */
export declare const DEFAULT_MAX_WS_FRAME_SIZE: number;
export declare const OP_CONTINUATION = 0;
export declare const OP_TEXT = 1;
export declare const OP_BINARY = 2;
export declare const OP_CLOSE = 8;
export declare const OP_PING = 9;
export declare const OP_PONG = 10;
export declare const CLOSE_NORMAL = 1000;
export declare const CLOSE_GOING_AWAY = 1001;
export declare const CLOSE_PROTOCOL_ERROR = 1002;
export declare const CLOSE_UNSUPPORTED = 1003;
export declare const CLOSE_INVALID_PAYLOAD = 1007;
export declare const CLOSE_POLICY_VIOLATION = 1008;
export declare const CLOSE_MESSAGE_TOO_BIG = 1009;
export declare const CLOSE_INTERNAL_ERROR = 1011;
export declare class WebSocketError extends Error {
    code: number;
    constructor(message: string, code?: number);
}
/** Compute Sec-WebSocket-Accept from client key per RFC 6455 Section 4.2.2 */
export declare function computeAcceptKey(key: string): string;
export declare function generateWSKey(): string;
export declare function buildUpgradeResponse(key: string, headers?: Record<string, string>): string;
/** Validate Sec-WebSocket-Key: must be 16 bytes base64 encoded */
export declare function validateWSKey(key: string): boolean;
export interface WSFrame {
    fin: boolean;
    opcode: number;
    payload: Buffer;
    masked: boolean;
}
/** Parse a single WebSocket frame from buffer. Returns null if incomplete. */
export declare function parseWSFrame(buf: Buffer, maxFrameSize?: number): {
    frame: WSFrame;
    consumed: number;
} | null;
/** Create an unmasked WS frame (server-to-client per RFC 6455 Section 5.3) */
export declare function createWSFrame(opcode: number, payload: Buffer | string): Buffer;
/** Create a masked WS frame (client-to-server per RFC 6455 Section 5.3) */
export declare function createWSFrameMasked(opcode: number, payload: Buffer | string): Buffer;
export declare function createWSTextFrame(message: string): Buffer;
export declare function createWSBinaryFrame(data: Buffer): Buffer;
export declare function createWSCloseFrame(code?: number, reason?: string): Buffer;
export declare function createWSPingFrame(data?: Buffer): Buffer;
export declare function createWSPongFrame(data?: Buffer): Buffer;
export declare function createWSTextFrameMasked(message: string): Buffer;
export declare function createWSBinaryFrameMasked(data: Buffer): Buffer;
export declare function createWSCloseFrameMasked(code?: number, reason?: string): Buffer;
export declare function createWSPingFrameMasked(): Buffer;
export declare function createWSPongFrameMasked(): Buffer;
/** Streaming parser for WebSocket frames. Buffers partial data and emits complete frames. */
export declare class WSFrameParser {
    private buf;
    private maxFrameSize;
    private totalBytesReceived;
    constructor(maxFrameSize?: number);
    feed(data: Buffer): WSFrame[];
    reset(): void;
    getBytesReceived(): number;
}
//# sourceMappingURL=websocket.d.ts.map