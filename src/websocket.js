/**
 * @stelar-time-real WebSocket Protocol (RFC 6455)
 *
 * Hand-crafted implementation with no external dependencies.
 * Uses Node.js built-in crypto for handshake and frame masking.
 */
import { createHash, randomBytes } from 'crypto';
const WS_MAGIC = '258EAFA5-E914-47DA-95CA-5AB5A7E3A741';
export const DEFAULT_MAX_WS_FRAME_SIZE = 10 * 1024 * 1024;
export const OP_CONTINUATION = 0x0;
export const OP_TEXT = 0x1;
export const OP_BINARY = 0x2;
export const OP_CLOSE = 0x8;
export const OP_PING = 0x9;
export const OP_PONG = 0xA;
export const CLOSE_NORMAL = 1000;
export const CLOSE_GOING_AWAY = 1001;
export const CLOSE_PROTOCOL_ERROR = 1002;
export const CLOSE_UNSUPPORTED = 1003;
export const CLOSE_INVALID_PAYLOAD = 1007;
export const CLOSE_POLICY_VIOLATION = 1008;
export const CLOSE_MESSAGE_TOO_BIG = 1009;
export const CLOSE_INTERNAL_ERROR = 1011;
export class WebSocketError extends Error {
    constructor(message, code = CLOSE_INTERNAL_ERROR) {
        super(message);
        this.name = 'WebSocketError';
        this.code = code;
    }
}
/** Compute Sec-WebSocket-Accept from client key per RFC 6455 Section 4.2.2 */
export function computeAcceptKey(key) {
    return createHash('sha1').update(key + WS_MAGIC).digest('base64');
}
export function generateWSKey() {
    return randomBytes(16).toString('base64');
}
export function buildUpgradeResponse(key, headers) {
    const accept = computeAcceptKey(key);
    const lines = [
        'HTTP/1.1 101 Switching Protocols',
        'Upgrade: websocket',
        'Connection: Upgrade',
        `Sec-WebSocket-Accept: ${accept}`,
    ];
    if (headers) {
        for (const [k, v] of Object.entries(headers)) {
            lines.push(`${k}: ${v}`);
        }
    }
    lines.push('', '');
    return lines.join('\r\n');
}
/** Validate Sec-WebSocket-Key: must be 16 bytes base64 encoded */
export function validateWSKey(key) {
    if (typeof key !== 'string')
        return false;
    const decoded = Buffer.from(key, 'base64');
    return decoded.length === 16;
}
/** Parse a single WebSocket frame from buffer. Returns null if incomplete. */
export function parseWSFrame(buf, maxFrameSize = DEFAULT_MAX_WS_FRAME_SIZE) {
    if (buf.length < 2)
        return null;
    const firstByte = buf[0];
    const secondByte = buf[1];
    const fin = (firstByte & 0x80) !== 0;
    const rsv1 = (firstByte & 0x40) !== 0;
    const rsv2 = (firstByte & 0x20) !== 0;
    const rsv3 = (firstByte & 0x10) !== 0;
    const opcode = firstByte & 0x0F;
    const masked = (secondByte & 0x80) !== 0;
    let payloadLen = secondByte & 0x7F;
    if (rsv1 || rsv2 || rsv3) {
        throw new WebSocketError('RSV bits set without extension negotiation', CLOSE_PROTOCOL_ERROR);
    }
    let offset = 2;
    if (payloadLen === 126) {
        if (buf.length < 4)
            return null;
        payloadLen = buf.readUInt16BE(2);
        offset = 4;
    }
    else if (payloadLen === 127) {
        if (buf.length < 10)
            return null;
        const high = buf.readUInt32BE(2);
        const low = buf.readUInt32BE(6);
        payloadLen = high * 0x100000000 + low;
        if (payloadLen > Number.MAX_SAFE_INTEGER) {
            throw new WebSocketError('Frame payload too large for JavaScript', CLOSE_MESSAGE_TOO_BIG);
        }
        offset = 10;
    }
    if (payloadLen > maxFrameSize) {
        throw new WebSocketError(`Frame payload exceeds max size (${maxFrameSize} bytes)`, CLOSE_MESSAGE_TOO_BIG);
    }
    let maskKey;
    if (masked) {
        if (buf.length < offset + 4)
            return null;
        maskKey = buf.subarray(offset, offset + 4);
        offset += 4;
    }
    if (buf.length < offset + payloadLen)
        return null;
    let payload = Buffer.from(buf.subarray(offset, offset + payloadLen));
    if (masked && maskKey) {
        for (let i = 0; i < payloadLen; i++) {
            payload[i] ^= maskKey[i & 3];
        }
    }
    return {
        frame: { fin, opcode, payload, masked },
        consumed: offset + payloadLen,
    };
}
/** Create an unmasked WS frame (server-to-client per RFC 6455 Section 5.3) */
export function createWSFrame(opcode, payload) {
    const data = typeof payload === 'string' ? Buffer.from(payload, 'utf8') : payload;
    let header;
    if (data.length < 126) {
        header = Buffer.alloc(2);
        header[0] = 0x80 | opcode;
        header[1] = data.length;
    }
    else if (data.length < 65536) {
        header = Buffer.alloc(4);
        header[0] = 0x80 | opcode;
        header[1] = 126;
        header.writeUInt16BE(data.length, 2);
    }
    else {
        header = Buffer.alloc(10);
        header[0] = 0x80 | opcode;
        header[1] = 127;
        header.writeUInt32BE(Math.floor(data.length / 0x100000000), 2);
        header.writeUInt32BE(data.length & 0xFFFFFFFF, 6);
    }
    return Buffer.concat([header, data]);
}
/** Create a masked WS frame (client-to-server per RFC 6455 Section 5.3) */
export function createWSFrameMasked(opcode, payload) {
    const data = typeof payload === 'string' ? Buffer.from(payload, 'utf8') : payload;
    const maskKey = randomBytes(4);
    let header;
    if (data.length < 126) {
        header = Buffer.alloc(6);
        header[0] = 0x80 | opcode;
        header[1] = 0x80 | data.length;
        maskKey.copy(header, 2);
    }
    else if (data.length < 65536) {
        header = Buffer.alloc(8);
        header[0] = 0x80 | opcode;
        header[1] = 0x80 | 126;
        header.writeUInt16BE(data.length, 2);
        maskKey.copy(header, 4);
    }
    else {
        header = Buffer.alloc(14);
        header[0] = 0x80 | opcode;
        header[1] = 0x80 | 127;
        header.writeUInt32BE(Math.floor(data.length / 0x100000000), 2);
        header.writeUInt32BE(data.length & 0xFFFFFFFF, 6);
        maskKey.copy(header, 10);
    }
    const outData = Buffer.from(data);
    for (let i = 0; i < outData.length; i++) {
        outData[i] ^= maskKey[i & 3];
    }
    return Buffer.concat([header, outData]);
}
/* Server-to-client frame helpers (unmasked) */
export function createWSTextFrame(message) {
    return createWSFrame(OP_TEXT, message);
}
export function createWSBinaryFrame(data) {
    return createWSFrame(OP_BINARY, data);
}
export function createWSCloseFrame(code = CLOSE_NORMAL, reason = '') {
    const buf = Buffer.alloc(2 + Buffer.byteLength(reason));
    buf.writeUInt16BE(code, 0);
    if (reason.length > 0)
        buf.write(reason, 2, 'utf8');
    return createWSFrame(OP_CLOSE, buf);
}
export function createWSPingFrame(data) {
    return createWSFrame(OP_PING, data || Buffer.alloc(0));
}
export function createWSPongFrame(data) {
    return createWSFrame(OP_PONG, data || Buffer.alloc(0));
}
/* Client-to-server frame helpers (masked) */
export function createWSTextFrameMasked(message) {
    return createWSFrameMasked(OP_TEXT, message);
}
export function createWSBinaryFrameMasked(data) {
    return createWSFrameMasked(OP_BINARY, data);
}
export function createWSCloseFrameMasked(code = CLOSE_NORMAL, reason = '') {
    const buf = Buffer.alloc(2 + Buffer.byteLength(reason));
    buf.writeUInt16BE(code, 0);
    if (reason.length > 0)
        buf.write(reason, 2, 'utf8');
    return createWSFrameMasked(OP_CLOSE, buf);
}
export function createWSPingFrameMasked() {
    return createWSFrameMasked(OP_PING, Buffer.alloc(0));
}
export function createWSPongFrameMasked() {
    return createWSFrameMasked(OP_PONG, Buffer.alloc(0));
}
/** Streaming parser for WebSocket frames. Buffers partial data and emits complete frames. */
export class WSFrameParser {
    constructor(maxFrameSize = DEFAULT_MAX_WS_FRAME_SIZE) {
        this.buf = Buffer.alloc(0);
        this.totalBytesReceived = 0;
        this.maxFrameSize = maxFrameSize;
    }
    feed(data) {
        this.totalBytesReceived += data.length;
        this.buf = Buffer.concat([this.buf, data]);
        if (this.buf.length > this.maxFrameSize * 2) {
            this.buf = Buffer.alloc(0);
            throw new WebSocketError(`Input buffer exceeded limit`, CLOSE_POLICY_VIOLATION);
        }
        const frames = [];
        while (this.buf.length > 0) {
            try {
                const result = parseWSFrame(this.buf, this.maxFrameSize);
                if (!result)
                    break;
                const { frame, consumed } = result;
                if (frame.opcode === OP_CONTINUATION) {
                    this.buf = consumed < this.buf.length
                        ? Buffer.from(this.buf.subarray(consumed))
                        : Buffer.alloc(0);
                    continue;
                }
                frames.push(frame);
                this.buf = consumed < this.buf.length
                    ? Buffer.from(this.buf.subarray(consumed))
                    : Buffer.alloc(0);
            }
            catch (err) {
                this.buf = Buffer.alloc(0);
                throw err;
            }
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
