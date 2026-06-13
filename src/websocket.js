/**
 * @stelar-time-real WebSocket (RFC 6455)
 */
import { createHash, randomBytes } from 'crypto';
const WS_MAGIC = '258EAFA5-E914-47DA-95CA-5AB5A7E3A741';
export const DEFAULT_MAX_WS_FRAME_SIZE = 10 * 1024 * 1024;
export const OP_CONTINUATION = 0x0, OP_TEXT = 0x1, OP_BINARY = 0x2, OP_CLOSE = 0x8, OP_PING = 0x9, OP_PONG = 0xA;
export const CLOSE_NORMAL = 1000, CLOSE_GOING_AWAY = 1001, CLOSE_PROTOCOL_ERROR = 1002, CLOSE_UNSUPPORTED = 1003, CLOSE_INVALID_PAYLOAD = 1007, CLOSE_POLICY_VIOLATION = 1008, CLOSE_MESSAGE_TOO_BIG = 1009, CLOSE_INTERNAL_ERROR = 1011;
export class WebSocketError extends Error {
    constructor(message, code = CLOSE_INTERNAL_ERROR) { super(message); this.name = 'WebSocketError'; this.code = code; }
}
export const computeAcceptKey = (key) => createHash('sha1').update(key + WS_MAGIC).digest('base64');
export const generateWSKey = () => randomBytes(16).toString('base64');
export const validateWSKey = (key) => typeof key === 'string' && Buffer.from(key, 'base64').length === 16;
export function buildUpgradeResponse(key, headers) {
    const lines = ['HTTP/1.1 101 Switching Protocols', 'Upgrade: websocket', 'Connection: Upgrade', `Sec-WebSocket-Accept: ${computeAcceptKey(key)}`];
    if (headers)
        for (const [k, v] of Object.entries(headers))
            lines.push(`${k}: ${v}`);
    lines.push('', '');
    return lines.join('\r\n');
}
export function parseWSFrame(buf, max = DEFAULT_MAX_WS_FRAME_SIZE) {
    if (buf.length < 2)
        return null;
    const b0 = buf[0], b1 = buf[1];
    const fin = !!(b0 & 0x80), rsv = b0 & 0x70, opcode = b0 & 0x0F, masked = !!(b1 & 0x80);
    let len = b1 & 0x7F, off = 2;
    if (rsv)
        throw new WebSocketError('RSV bits set', CLOSE_PROTOCOL_ERROR);
    if (len === 126) {
        if (buf.length < 4)
            return null;
        len = buf.readUInt16BE(2);
        off = 4;
    }
    else if (len === 127) {
        if (buf.length < 10)
            return null;
        len = buf.readUInt32BE(2) * 0x100000000 + buf.readUInt32BE(6);
        off = 10;
    }
    if (len > max)
        throw new WebSocketError(`Frame exceeds max (${max})`, CLOSE_MESSAGE_TOO_BIG);
    let mk;
    if (masked) {
        if (buf.length < off + 4)
            return null;
        mk = buf.subarray(off, off + 4);
        off += 4;
    }
    if (buf.length < off + len)
        return null;
    const payload = Buffer.from(buf.subarray(off, off + len));
    if (masked && mk)
        for (let i = 0; i < len; i++)
            payload[i] ^= mk[i & 3];
    return { frame: { fin, opcode, payload, masked }, consumed: off + len };
}
export function createWSFrame(opcode, payload, masked = false) {
    const d = typeof payload === 'string' ? Buffer.from(payload, 'utf8') : payload;
    const mk = masked ? randomBytes(4) : undefined;
    const base = masked ? 0x80 : 0;
    let h;
    if (d.length < 126) {
        h = Buffer.alloc(masked ? 6 : 2);
        h[0] = 0x80 | opcode;
        h[1] = base | d.length;
        if (mk)
            mk.copy(h, 2);
    }
    else if (d.length < 65536) {
        h = Buffer.alloc(masked ? 8 : 4);
        h[0] = 0x80 | opcode;
        h[1] = base | 126;
        h.writeUInt16BE(d.length, 2);
        if (mk)
            mk.copy(h, 4);
    }
    else {
        h = Buffer.alloc(masked ? 14 : 10);
        h[0] = 0x80 | opcode;
        h[1] = base | 127;
        h.writeUInt32BE(Math.floor(d.length / 0x100000000), 2);
        h.writeUInt32BE(d.length & 0xFFFFFFFF, 6);
        if (mk)
            mk.copy(h, 10);
    }
    if (mk)
        for (let i = 0; i < d.length; i++)
            d[i] ^= mk[i & 3];
    return Buffer.concat([h, d]);
}
/* Server (unmasked) */
export const createWSTextFrame = (msg) => createWSFrame(OP_TEXT, msg);
export const createWSBinaryFrame = (data) => createWSFrame(OP_BINARY, data);
export const createWSCloseFrame = (code = CLOSE_NORMAL, reason = '') => {
    const b = Buffer.alloc(2 + Buffer.byteLength(reason));
    b.writeUInt16BE(code, 0);
    if (reason)
        b.write(reason, 2, 'utf8');
    return createWSFrame(OP_CLOSE, b);
};
export const createWSPingFrame = (data) => createWSFrame(OP_PING, data || Buffer.alloc(0));
export const createWSPongFrame = (data) => createWSFrame(OP_PONG, data || Buffer.alloc(0));
/* Client (masked) */
export const createWSTextFrameMasked = (msg) => createWSFrame(OP_TEXT, msg, true);
export const createWSBinaryFrameMasked = (data) => createWSFrame(OP_BINARY, data, true);
export const createWSCloseFrameMasked = (code = CLOSE_NORMAL, reason = '') => {
    const b = Buffer.alloc(2 + Buffer.byteLength(reason));
    b.writeUInt16BE(code, 0);
    if (reason)
        b.write(reason, 2, 'utf8');
    return createWSFrame(OP_CLOSE, b, true);
};
export const createWSPingFrameMasked = () => createWSFrame(OP_PING, Buffer.alloc(0), true);
export const createWSPongFrameMasked = () => createWSFrame(OP_PONG, Buffer.alloc(0), true);
export class WSFrameParser {
    constructor(max = DEFAULT_MAX_WS_FRAME_SIZE) {
        this.buf = Buffer.alloc(0);
        this.received = 0;
        this.max = max;
    }
    feed(data) {
        this.received += data.length;
        this.buf = Buffer.concat([this.buf, data]);
        if (this.buf.length > this.max * 2) {
            this.buf = Buffer.alloc(0);
            throw new WebSocketError('Buffer overflow', CLOSE_POLICY_VIOLATION);
        }
        const frames = [];
        while (this.buf.length > 0) {
            try {
                const r = parseWSFrame(this.buf, this.max);
                if (!r)
                    break;
                this.buf = r.consumed < this.buf.length ? Buffer.from(this.buf.subarray(r.consumed)) : Buffer.alloc(0);
                if (r.frame.opcode !== OP_CONTINUATION)
                    frames.push(r.frame);
            }
            catch (e) {
                this.buf = Buffer.alloc(0);
                throw e;
            }
        }
        return frames;
    }
    reset() { this.buf = Buffer.alloc(0); this.received = 0; }
    getBytesReceived() { return this.received; }
}
