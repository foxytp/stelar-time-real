/**
 * @stelar-time-real Binary Protocol
 *
 * Frame format:
 * [4B totalLen BE][1B type][2B eventLen BE][eventLen bytes event][payload]
 *
 * Min frame: 7 bytes (header only). Max event name: 256 bytes.
 */
export declare const FRAME_JSON = 1;
export declare const FRAME_BINARY = 2;
export declare const FRAME_PING = 3;
export declare const FRAME_PONG = 4;
export declare const FRAME_ACK_REQ = 5;
export declare const FRAME_ACK_RES = 6;
export declare const FRAME_CONNECT = 7;
export declare const FRAME_DISCONNECT = 8;
export declare const FRAME_JOIN = 9;
export declare const FRAME_LEAVE = 10;
export declare const FRAME_ERROR = 11;
/** Max event name length in bytes */
export declare const MAX_EVENT_LENGTH = 256;
/** Default max frame size: 10 MB */
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
/** Validates event name format. Throws ProtocolError on invalid input. */
export declare function validateEventName(event: string): void;
export declare function encodeJsonFrame(event: string, data: unknown, maxFrameSize?: number): Buffer;
export declare function encodeBinaryFrame(event: string, data: Uint8Array | Buffer, maxFrameSize?: number): Buffer;
export declare function encodePingFrame(): Buffer;
export declare function encodePongFrame(): Buffer;
export declare function encodeAckReqFrame(ackName: string, data: unknown, maxFrameSize?: number): Buffer;
export declare function encodeAckResFrame(ackName: string, data: unknown, maxFrameSize?: number): Buffer;
export declare function encodeConnectFrame(clientId: string): Buffer;
export declare function encodeDisconnectFrame(): Buffer;
export declare function encodeJoinFrame(room: string, maxFrameSize?: number): Buffer;
export declare function encodeLeaveFrame(room: string): Buffer;
export declare function encodeErrorFrame(message: string): Buffer;
/** Streaming frame parser for TCP connections. Buffers partial data and emits complete frames. */
export declare class FrameParser {
    private buf;
    private maxFrameSize;
    private totalBytesReceived;
    constructor(maxFrameSize?: number);
    feed(data: Buffer): ParsedFrame[];
    reset(): void;
    getBytesReceived(): number;
}
//# sourceMappingURL=protocol.d.ts.map