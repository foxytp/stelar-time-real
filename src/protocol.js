/**
 * @stelar-time-real Binary Protocol
 * Frame: [4B totalLen BE][1B type][2B eventLen BE][event][payload]
 */
export const FRAME_JSON = 0x01, FRAME_BINARY = 0x02, FRAME_PING = 0x03, FRAME_PONG = 0x04, FRAME_ACK_REQ = 0x05, FRAME_ACK_RES = 0x06, FRAME_CONNECT = 0x07, FRAME_DISCONNECT = 0x08, FRAME_JOIN = 0x09, FRAME_LEAVE = 0x0A, FRAME_ERROR = 0x0B;
export const MAX_EVENT_LENGTH = 256;
export const DEFAULT_MAX_FRAME_SIZE = 10 * 1024 * 1024;
export const HEADER_SIZE = 7;
export class ProtocolError extends Error {
    constructor(message, code = 'PROTOCOL_ERROR') {
        super(message);
        this.name = 'ProtocolError';
        this.code = code;
    }
}
export function validateEventName(event) {
    if (typeof event !== 'string')
        throw new ProtocolError('Event name must be a string', 'INVALID_EVENT');
    if (!event)
        throw new ProtocolError('Event name cannot be empty', 'EMPTY_EVENT');
    if (event.length > MAX_EVENT_LENGTH)
        throw new ProtocolError(`Event name exceeds ${MAX_EVENT_LENGTH} bytes`, 'EVENT_TOO_LONG');
    if (!/^[a-zA-Z0-9\-./:]+$/.test(event))
        throw new ProtocolError('Event name contains invalid characters', 'INVALID_EVENT_CHARS');
    if (['ping', 'pong', 'connect', 'disconnect', 'error'].includes(event))
        throw new ProtocolError(`Event "${event}" is reserved`, 'RESERVED_EVENT');
}
function encode(type, event, payload, max = DEFAULT_MAX_FRAME_SIZE) {
    const eb = Buffer.from(event, 'utf8');
    if (eb.length > MAX_EVENT_LENGTH)
        throw new ProtocolError(`Event name exceeds ${MAX_EVENT_LENGTH} bytes`, 'EVENT_TOO_LONG');
    const total = HEADER_SIZE + eb.length + payload.length;
    if (total > max)
        throw new ProtocolError(`Frame exceeds max size (${max} bytes)`, 'FRAME_TOO_LARGE');
    const f = Buffer.alloc(total);
    f.writeUInt32BE(total, 0);
    f[4] = type;
    f.writeUInt16BE(eb.length, 5);
    if (eb.length)
        eb.copy(f, HEADER_SIZE);
    if (payload.length)
        payload.copy(f, HEADER_SIZE + eb.length);
    return f;
}
const emptyFrame = (type) => {
    const f = Buffer.alloc(HEADER_SIZE);
    f.writeUInt32BE(HEADER_SIZE, 0);
    f[4] = type;
    f.writeUInt16BE(0, 5);
    return f;
};
export const encodeJsonFrame = (event, data, max) => (validateEventName(event), encode(FRAME_JSON, event, Buffer.from(JSON.stringify(data), 'utf8'), max));
export const encodeBinaryFrame = (event, data, max) => (validateEventName(event), encode(FRAME_BINARY, event, Buffer.from(data), max));
export const encodePingFrame = () => emptyFrame(FRAME_PING);
export const encodePongFrame = () => emptyFrame(FRAME_PONG);
export const encodeAckReqFrame = (name, data, max) => encode(FRAME_ACK_REQ, name, Buffer.from(JSON.stringify(data), 'utf8'), max);
export const encodeAckResFrame = (name, data, max) => encode(FRAME_ACK_RES, name, Buffer.from(JSON.stringify(data), 'utf8'), max);
export const encodeConnectFrame = (id) => encode(FRAME_CONNECT, 'connect', Buffer.from(id, 'utf8'));
export const encodeDisconnectFrame = () => emptyFrame(FRAME_DISCONNECT);
export const encodeJoinFrame = (room, max) => encode(FRAME_JOIN, 'join-room', Buffer.from(room, 'utf8'), max);
export const encodeLeaveFrame = (room) => encode(FRAME_LEAVE, 'leave-room', room ? Buffer.from(room, 'utf8') : Buffer.alloc(0));
export const encodeErrorFrame = (msg) => encode(FRAME_ERROR, 'error', Buffer.from(msg, 'utf8'));
export class FrameParser {
    constructor(max = DEFAULT_MAX_FRAME_SIZE) {
        this.buf = Buffer.alloc(0);
        this.received = 0;
        this.max = max;
    }
    feed(data) {
        this.received += data.length;
        this.buf = Buffer.concat([this.buf, data]);
        if (this.buf.length > this.max * 2) {
            this.buf = Buffer.alloc(0);
            throw new ProtocolError(`Buffer overflow (${this.max * 2})`, 'BUFFER_OVERFLOW');
        }
        const frames = [];
        while (this.buf.length >= HEADER_SIZE) {
            const total = this.buf.readUInt32BE(0);
            if (total < HEADER_SIZE || total > this.max) {
                this.buf = Buffer.alloc(0);
                throw new ProtocolError(`Invalid frame size: ${total}`, total < HEADER_SIZE ? 'INVALID_FRAME_SIZE' : 'FRAME_TOO_LARGE');
            }
            if (this.buf.length < total)
                break;
            const el = this.buf.readUInt16BE(5);
            if (HEADER_SIZE + el > total || el > MAX_EVENT_LENGTH) {
                this.buf = Buffer.alloc(0);
                throw new ProtocolError('Invalid event length', 'INVALID_EVENT_LENGTH');
            }
            frames.push({
                type: this.buf[4],
                event: el ? this.buf.subarray(HEADER_SIZE, HEADER_SIZE + el).toString('utf8') : '',
                payload: total > HEADER_SIZE + el ? Buffer.from(this.buf.subarray(HEADER_SIZE + el, total)) : Buffer.alloc(0),
            });
            this.buf = total < this.buf.length ? Buffer.from(this.buf.subarray(total)) : Buffer.alloc(0);
        }
        return frames;
    }
    reset() { this.buf = Buffer.alloc(0); this.received = 0; }
    getBytesReceived() { return this.received; }
}
