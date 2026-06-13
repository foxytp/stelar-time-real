/**
 * @stelar-time-real Binary Protocol
 *
 * Frame format:
 * [4B totalLen BE][1B type][2B eventLen BE][eventLen bytes event][payload]
 *
 * Min frame: 7 bytes (header only). Max event name: 256 bytes.
 */
export const FRAME_JSON = 0x01;
export const FRAME_BINARY = 0x02;
export const FRAME_PING = 0x03;
export const FRAME_PONG = 0x04;
export const FRAME_ACK_REQ = 0x05;
export const FRAME_ACK_RES = 0x06;
export const FRAME_CONNECT = 0x07;
export const FRAME_DISCONNECT = 0x08;
export const FRAME_JOIN = 0x09;
export const FRAME_LEAVE = 0x0A;
export const FRAME_ERROR = 0x0B;
/** Max event name length in bytes */
export const MAX_EVENT_LENGTH = 256;
/** Default max frame size: 10 MB */
export const DEFAULT_MAX_FRAME_SIZE = 10 * 1024 * 1024;
export const HEADER_SIZE = 7;
export class ProtocolError extends Error {
    constructor(message, code = 'PROTOCOL_ERROR') {
        super(message);
        this.name = 'ProtocolError';
        this.code = code;
    }
}
/** Validates event name format. Throws ProtocolError on invalid input. */
export function validateEventName(event) {
    if (typeof event !== 'string') {
        throw new ProtocolError('Event name must be a string', 'INVALID_EVENT');
    }
    if (event.length === 0) {
        throw new ProtocolError('Event name cannot be empty', 'EMPTY_EVENT');
    }
    if (event.length > MAX_EVENT_LENGTH) {
        throw new ProtocolError(`Event name exceeds ${MAX_EVENT_LENGTH} bytes`, 'EVENT_TOO_LONG');
    }
    if (!/^[\w\-./:]+$/.test(event)) {
        throw new ProtocolError('Event name contains invalid characters', 'INVALID_EVENT_CHARS');
    }
    if (['ping', 'pong', 'connect', 'disconnect', 'error'].includes(event)) {
        throw new ProtocolError(`Event "${event}" is reserved`, 'RESERVED_EVENT');
    }
}
function validatePayloadSize(payload, maxSize) {
    if (payload.length > maxSize) {
        throw new ProtocolError(`Payload exceeds max size (${maxSize} bytes)`, 'PAYLOAD_TOO_LARGE');
    }
}
function encodeFrame(type, event, payload, maxFrameSize = DEFAULT_MAX_FRAME_SIZE) {
    if (event.length > MAX_EVENT_LENGTH) {
        throw new ProtocolError(`Event name exceeds ${MAX_EVENT_LENGTH} bytes`, 'EVENT_TOO_LONG');
    }
    const eventBuf = Buffer.from(event, 'utf8');
    const totalLen = HEADER_SIZE + eventBuf.length + payload.length;
    if (totalLen > maxFrameSize) {
        throw new ProtocolError(`Frame exceeds max size (${maxFrameSize} bytes)`, 'FRAME_TOO_LARGE');
    }
    const frame = Buffer.alloc(totalLen);
    frame.writeUInt32BE(totalLen, 0);
    frame[4] = type;
    frame.writeUInt16BE(eventBuf.length, 5);
    if (eventBuf.length > 0)
        eventBuf.copy(frame, HEADER_SIZE);
    if (payload.length > 0)
        payload.copy(frame, HEADER_SIZE + eventBuf.length);
    return frame;
}
export function encodeJsonFrame(event, data, maxFrameSize) {
    validateEventName(event);
    const payload = Buffer.from(JSON.stringify(data), 'utf8');
    if (maxFrameSize)
        validatePayloadSize(payload, maxFrameSize);
    return encodeFrame(FRAME_JSON, event, payload, maxFrameSize);
}
export function encodeBinaryFrame(event, data, maxFrameSize) {
    validateEventName(event);
    const payload = Buffer.from(data);
    if (maxFrameSize)
        validatePayloadSize(payload, maxFrameSize);
    return encodeFrame(FRAME_BINARY, event, payload, maxFrameSize);
}
export function encodePingFrame() {
    const f = Buffer.alloc(HEADER_SIZE);
    f.writeUInt32BE(HEADER_SIZE, 0);
    f[4] = FRAME_PING;
    f.writeUInt16BE(0, 5);
    return f;
}
export function encodePongFrame() {
    const f = Buffer.alloc(HEADER_SIZE);
    f.writeUInt32BE(HEADER_SIZE, 0);
    f[4] = FRAME_PONG;
    f.writeUInt16BE(0, 5);
    return f;
}
export function encodeAckReqFrame(ackName, data, maxFrameSize) {
    const payload = Buffer.from(JSON.stringify(data), 'utf8');
    if (maxFrameSize)
        validatePayloadSize(payload, maxFrameSize);
    return encodeFrame(FRAME_ACK_REQ, ackName, payload, maxFrameSize);
}
export function encodeAckResFrame(ackName, data, maxFrameSize) {
    const payload = Buffer.from(JSON.stringify(data), 'utf8');
    if (maxFrameSize)
        validatePayloadSize(payload, maxFrameSize);
    return encodeFrame(FRAME_ACK_RES, ackName, payload, maxFrameSize);
}
export function encodeConnectFrame(clientId) {
    return encodeFrame(FRAME_CONNECT, 'connect', Buffer.from(clientId, 'utf8'));
}
export function encodeDisconnectFrame() {
    const f = Buffer.alloc(HEADER_SIZE);
    f.writeUInt32BE(HEADER_SIZE, 0);
    f[4] = FRAME_DISCONNECT;
    f.writeUInt16BE(0, 5);
    return f;
}
export function encodeJoinFrame(room, maxFrameSize) {
    const payload = Buffer.from(room, 'utf8');
    return encodeFrame(FRAME_JOIN, 'join-room', payload, maxFrameSize);
}
export function encodeLeaveFrame(room) {
    const payload = room ? Buffer.from(room, 'utf8') : Buffer.alloc(0);
    return encodeFrame(FRAME_LEAVE, 'leave-room', payload);
}
export function encodeErrorFrame(message) {
    return encodeFrame(FRAME_ERROR, 'error', Buffer.from(message, 'utf8'));
}
/** Streaming frame parser for TCP connections. Buffers partial data and emits complete frames. */
export class FrameParser {
    constructor(maxFrameSize = DEFAULT_MAX_FRAME_SIZE) {
        this.buf = Buffer.alloc(0);
        this.totalBytesReceived = 0;
        this.maxFrameSize = maxFrameSize;
    }
    feed(data) {
        this.totalBytesReceived += data.length;
        this.buf = Buffer.concat([this.buf, data]);
        if (this.buf.length > this.maxFrameSize * 2) {
            this.buf = Buffer.alloc(0);
            throw new ProtocolError(`Input buffer exceeded limit (${this.maxFrameSize * 2} bytes)`, 'BUFFER_OVERFLOW');
        }
        const frames = [];
        while (this.buf.length >= HEADER_SIZE) {
            const totalLen = this.buf.readUInt32BE(0);
            if (totalLen < HEADER_SIZE) {
                this.buf = Buffer.alloc(0);
                throw new ProtocolError(`Invalid frame size: ${totalLen}`, 'INVALID_FRAME_SIZE');
            }
            if (totalLen > this.maxFrameSize) {
                this.buf = Buffer.alloc(0);
                throw new ProtocolError(`Frame exceeds max size (${this.maxFrameSize} bytes)`, 'FRAME_TOO_LARGE');
            }
            if (this.buf.length < totalLen)
                break;
            const type = this.buf[4];
            const eventLen = this.buf.readUInt16BE(5);
            if (HEADER_SIZE + eventLen > totalLen) {
                this.buf = Buffer.alloc(0);
                throw new ProtocolError('Event length exceeds frame bounds', 'INVALID_EVENT_LENGTH');
            }
            if (eventLen > MAX_EVENT_LENGTH) {
                this.buf = Buffer.alloc(0);
                throw new ProtocolError(`Event name exceeds ${MAX_EVENT_LENGTH} bytes`, 'EVENT_TOO_LONG');
            }
            const event = eventLen > 0
                ? this.buf.subarray(HEADER_SIZE, HEADER_SIZE + eventLen).toString('utf8')
                : '';
            const payloadStart = HEADER_SIZE + eventLen;
            const payload = totalLen > payloadStart
                ? Buffer.from(this.buf.subarray(payloadStart, totalLen))
                : Buffer.alloc(0);
            frames.push({ type, event, payload });
            this.buf = totalLen < this.buf.length
                ? Buffer.from(this.buf.subarray(totalLen))
                : Buffer.alloc(0);
        }
        return frames;
    }
    reset() {
        this.buf = Buffer.alloc(0);
        this.totalBytesReceived = 0;
    }
    getBytesReceived() {
        return this.totalBytesReceived;
    }
}
