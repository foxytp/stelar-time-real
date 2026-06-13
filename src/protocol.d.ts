/** @stelar-time-real Binary Protocol — Frame: [4B totalLen BE][1B type][2B eventLen BE][event][payload] */
export declare const FRAME_JSON = 1, FRAME_BINARY = 2, FRAME_PING = 3, FRAME_PONG = 4, FRAME_ACK_REQ = 5, FRAME_ACK_RES = 6, FRAME_CONNECT = 7, FRAME_DISCONNECT = 8, FRAME_JOIN = 9, FRAME_LEAVE = 10, FRAME_ERROR = 11;
export declare const MAX_EVENT_LENGTH = 256;
export declare const DEFAULT_MAX_FRAME_SIZE: number;
export declare const HEADER_SIZE = 7;
export interface ParsedFrame {
    type: number;
    event: string;
    payload: Buffer;
}
export declare class ProtocolError extends Error {
    code: string;
    constructor(message: string, code?: string);
}
export declare function validateEventName(event: string): void;
export declare const encodeJsonFrame: (event: string, data: unknown, max?: number) => Buffer<ArrayBufferLike>;
export declare const encodeBinaryFrame: (event: string, data: Uint8Array | Buffer, max?: number) => Buffer<ArrayBufferLike>;
export declare const encodePingFrame: () => Buffer<ArrayBufferLike>;
export declare const encodePongFrame: () => Buffer<ArrayBufferLike>;
export declare const encodeAckReqFrame: (name: string, data: unknown, max?: number) => Buffer<ArrayBufferLike>;
export declare const encodeAckResFrame: (name: string, data: unknown, max?: number) => Buffer<ArrayBufferLike>;
export declare const encodeConnectFrame: (id: string) => Buffer<ArrayBufferLike>;
export declare const encodeDisconnectFrame: () => Buffer<ArrayBufferLike>;
export declare const encodeJoinFrame: (room: string, max?: number) => Buffer<ArrayBufferLike>;
export declare const encodeLeaveFrame: (room: string) => Buffer<ArrayBufferLike>;
export declare const encodeErrorFrame: (msg: string) => Buffer<ArrayBufferLike>;
/** O(1) append streaming parser — avoids Buffer.concat O(n²) on many small chunks */
export declare class FrameParser {
    private chunks;
    private len;
    private max;
    private received;
    constructor(max?: number);
    private _compact;
    feed(data: Buffer): ParsedFrame[];
    reset(): void;
    getBytesReceived(): number;
}
//# sourceMappingURL=protocol.d.ts.map