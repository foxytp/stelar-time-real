/** @stelar-time-real Binary Protocol — Frame: [4B totalLen BE][1B type][2B eventLen BE][event][payload] */

export const FRAME_JSON = 0x01, FRAME_BINARY = 0x02, FRAME_PING = 0x03,
  FRAME_PONG = 0x04, FRAME_ACK_REQ = 0x05, FRAME_ACK_RES = 0x06,
  FRAME_CONNECT = 0x07, FRAME_DISCONNECT = 0x08, FRAME_JOIN = 0x09,
  FRAME_LEAVE = 0x0A, FRAME_ERROR = 0x0B;

export const MAX_EVENT_LENGTH = 256;
export const DEFAULT_MAX_FRAME_SIZE = 10 * 1024 * 1024;
export const HEADER_SIZE = 7;

export interface ParsedFrame { type: number; event: string; payload: Buffer; }

export class ProtocolError extends Error {
  code: string;
  constructor(message: string, code = 'PROTOCOL_ERROR') { super(message); this.name = 'ProtocolError'; this.code = code; }
}

export function validateEventName(event: string): void {
  if (typeof event !== 'string') throw new ProtocolError('Event name must be a string', 'INVALID_EVENT');
  if (!event) throw new ProtocolError('Event name cannot be empty', 'EMPTY_EVENT');
  if (event.length > MAX_EVENT_LENGTH) throw new ProtocolError(`Event name exceeds ${MAX_EVENT_LENGTH} bytes`, 'EVENT_TOO_LONG');
  if (!/^[a-zA-Z0-9\-./:]+$/.test(event)) throw new ProtocolError('Event name contains invalid characters', 'INVALID_EVENT_CHARS');
  if (['ping', 'pong', 'connect', 'disconnect', 'error'].includes(event)) throw new ProtocolError(`Event "${event}" is reserved`, 'RESERVED_EVENT');
}

function encode(type: number, event: string, payload: Buffer, max = DEFAULT_MAX_FRAME_SIZE): Buffer {
  const eb = Buffer.from(event, 'utf8');
  if (eb.length > MAX_EVENT_LENGTH) throw new ProtocolError(`Event name exceeds ${MAX_EVENT_LENGTH} bytes`, 'EVENT_TOO_LONG');
  const total = HEADER_SIZE + eb.length + payload.length;
  if (total > max) throw new ProtocolError(`Frame exceeds max size (${max})`, 'FRAME_TOO_LARGE');
  const f = Buffer.alloc(total);
  f.writeUInt32BE(total, 0); f[4] = type; f.writeUInt16BE(eb.length, 5);
  if (eb.length) eb.copy(f, HEADER_SIZE);
  if (payload.length) payload.copy(f, HEADER_SIZE + eb.length);
  return f;
}

const emptyFrame = (type: number): Buffer => {
  const f = Buffer.alloc(HEADER_SIZE);
  f.writeUInt32BE(HEADER_SIZE, 0); f[4] = type; f.writeUInt16BE(0, 5);
  return f;
};

export const encodeJsonFrame = (event: string, data: unknown, max?: number) =>
  (validateEventName(event), encode(FRAME_JSON, event, Buffer.from(JSON.stringify(data), 'utf8'), max));

export const encodeBinaryFrame = (event: string, data: Uint8Array | Buffer, max?: number) =>
  (validateEventName(event), encode(FRAME_BINARY, event, Buffer.from(data), max));

export const encodePingFrame = () => emptyFrame(FRAME_PING);
export const encodePongFrame = () => emptyFrame(FRAME_PONG);
export const encodeAckReqFrame = (name: string, data: unknown, max?: number) =>
  encode(FRAME_ACK_REQ, name, Buffer.from(JSON.stringify(data), 'utf8'), max);
export const encodeAckResFrame = (name: string, data: unknown, max?: number) =>
  encode(FRAME_ACK_RES, name, Buffer.from(JSON.stringify(data), 'utf8'), max);
export const encodeConnectFrame = (id: string) => encode(FRAME_CONNECT, 'connect', Buffer.from(id, 'utf8'));
export const encodeDisconnectFrame = () => emptyFrame(FRAME_DISCONNECT);
export const encodeJoinFrame = (room: string, max?: number) => encode(FRAME_JOIN, 'join-room', Buffer.from(room, 'utf8'), max);
export const encodeLeaveFrame = (room: string) => encode(FRAME_LEAVE, 'leave-room', room ? Buffer.from(room, 'utf8') : Buffer.alloc(0));
export const encodeErrorFrame = (msg: string) => encode(FRAME_ERROR, 'error', Buffer.from(msg, 'utf8'));

/** O(1) append streaming parser — avoids Buffer.concat O(n²) on many small chunks */
export class FrameParser {
  private chunks: Buffer[] = [];
  private len = 0;
  private max: number;
  private received = 0;

  constructor(max = DEFAULT_MAX_FRAME_SIZE) { this.max = max; }

  private _compact(): Buffer {
    if (this.chunks.length <= 1) return this.chunks[0] || Buffer.alloc(0);
    const buf = Buffer.concat(this.chunks);
    this.chunks = [buf];
    return buf;
  }

  feed(data: Buffer): ParsedFrame[] {
    this.received += data.length;
    this.chunks.push(data);
    this.len += data.length;
    if (this.len > this.max * 2) { this.chunks = []; this.len = 0; throw new ProtocolError(`Buffer overflow (${this.max * 2})`, 'BUFFER_OVERFLOW'); }
    const frames: ParsedFrame[] = [];
    while (this.len >= HEADER_SIZE) {
      const buf = this._compact();
      const total = buf.readUInt32BE(0);
      if (total < HEADER_SIZE || total > this.max) { this.chunks = []; this.len = 0; throw new ProtocolError(`Invalid frame size: ${total}`, total < HEADER_SIZE ? 'INVALID_FRAME_SIZE' : 'FRAME_TOO_LARGE'); }
      if (buf.length < total) break;
      const el = buf.readUInt16BE(5);
      if (HEADER_SIZE + el > total || el > MAX_EVENT_LENGTH) { this.chunks = []; this.len = 0; throw new ProtocolError('Invalid event length', 'INVALID_EVENT_LENGTH'); }
      frames.push({
        type: buf[4],
        event: el ? buf.subarray(HEADER_SIZE, HEADER_SIZE + el).toString('utf8') : '',
        payload: total > HEADER_SIZE + el ? Buffer.from(buf.subarray(HEADER_SIZE + el, total)) : Buffer.alloc(0),
      });
      if (total < buf.length) { this.chunks = [Buffer.from(buf.subarray(total))]; this.len = this.chunks[0].length; }
      else { this.chunks = []; this.len = 0; }
    }
    return frames;
  }

  reset() { this.chunks = []; this.len = 0; this.received = 0; }
  getBytesReceived() { return this.received; }
}
