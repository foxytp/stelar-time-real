class StelarClient {
    constructor(urlOrPort = 'localhost:3000', options = {}) {
        this.ws = null;
        this.events = new Map();
        this._wildcardHandler = null;
        this.connected = false;
        this.id = null;
        this._reconnectAttempts = 0;
        this._hbTimer = null;
        this._isManualClose = false;
        this._acks = new Map();
        if (typeof urlOrPort === 'number') {
            this.url = `ws://localhost:${urlOrPort}`;
        }
        else if (urlOrPort.includes('://')) {
            this.url = urlOrPort.startsWith('http') ? 'ws' + urlOrPort.slice(4) : urlOrPort;
        }
        else {
            this.url = `ws://${urlOrPort}`;
        }
        this.options = {
            reconnection: options.reconnection !== false,
            reconnectionAttempts: options.reconnectionAttempts || 5,
            reconnectionDelay: options.reconnectionDelay || 1000,
            heartbeatInterval: options.heartbeatInterval || 30000,
            ackTimeout: options.ackTimeout || 5000
        };
    }
    setUrl(url) {
        this.url = url;
        return this;
    }
    on(event, handler) {
        this.events.set(event, handler);
        return this;
    }
    off(event, handler) {
        if (this.events.get(event) === handler) {
            this.events.delete(event);
        }
        return this;
    }
    onAll(handler) {
        this._wildcardHandler = handler;
        return this;
    }
    onAck(name, handler) {
        this._acks.set(name, handler);
        return this;
    }
    emit(event, data, opts = {}) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            const payload = { event, data };
            if (opts.ack) {
                payload._ackName = opts.ack;
            }
            this.ws.send(JSON.stringify(payload));
        }
        return this;
    }
    emitBinary(event, data) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            const header = JSON.stringify({ event });
            const headerBytes = new TextEncoder().encode(header);
            const combined = new Uint8Array(headerBytes.length + 1 + data.byteLength);
            combined.set(headerBytes, 0);
            combined[headerBytes.length] = 0;
            combined.set(new Uint8Array(data), headerBytes.length + 1);
            this.ws.send(combined);
        }
        return this;
    }
    sendFile(file) {
        return this.emitBinary('file', file);
    }
    sendImage(blob) {
        return this.emitBinary('image', blob);
    }
    request(event, data, ackName) {
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(new Error(`ACK '${ackName}' timeout`));
            }, this.options.ackTimeout);
            const handler = (responseData) => {
                clearTimeout(timeout);
                this._acks.delete(ackName);
                resolve(responseData);
            };
            this._acks.set(ackName, handler);
            this.emit(event, data, { ack: ackName });
        });
    }
    joinRoom(room) {
        this.emit('join-room', room);
        return this;
    }
    leaveRoom() {
        this.emit('leave-room', {});
        return this;
    }
    startHeartbeat() {
        this._hbTimer = setInterval(() => {
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                this.emit('pong', Date.now());
            }
        }, this.options.heartbeatInterval);
    }
    stopHeartbeat() {
        if (this._hbTimer) {
            clearInterval(this._hbTimer);
            this._hbTimer = null;
        }
    }
    _connect() {
        this._isManualClose = false;
        this.ws = new WebSocket(this.url);
        this.ws.onopen = () => {
            this.connected = true;
            this._reconnectAttempts = 0;
            const handler = this.events.get('connect');
            if (handler)
                handler(undefined);
            this.startHeartbeat();
        };
        this.ws.binaryType = 'arraybuffer';
        this.ws.onmessage = (e) => {
            try {
                if (e.data instanceof ArrayBuffer) {
                    const view = new Uint8Array(e.data);
                    let headerEnd = -1;
                    for (let i = 0; i < view.length; i++) {
                        if (view[i] === 0) {
                            headerEnd = i;
                            break;
                        }
                    }
                    if (headerEnd === -1)
                        return;
                    const headerStr = new TextDecoder().decode(view.slice(0, headerEnd));
                    const header = JSON.parse(headerStr);
                    const buffer = view.slice(headerEnd + 1);
                    const handler = this.events.get(header.event);
                    if (handler) {
                        handler(buffer.buffer);
                    }
                    else if (this._wildcardHandler) {
                        this._wildcardHandler({ event: header.event, data: buffer.buffer, isBinary: true, buffer: buffer.buffer });
                    }
                    return;
                }
                const msg = JSON.parse(e.data);
                const { event, data, _isAck } = msg;
                if (event === 'ping')
                    return;
                if (_isAck && this._acks.has(event)) {
                    const handler = this._acks.get(event);
                    handler(data);
                    return;
                }
                const handler = this.events.get(event);
                if (handler)
                    handler(data);
                if (this._wildcardHandler) {
                    this._wildcardHandler({ event, data });
                }
            }
            catch { }
        };
        this.ws.onclose = () => {
            this.connected = false;
            this.stopHeartbeat();
            const handler = this.events.get('disconnect');
            if (handler)
                handler(undefined);
            if (!this._isManualClose && this.options.reconnection && this._reconnectAttempts < this.options.reconnectionAttempts) {
                this._reconnectAttempts++;
                const reconHandler = this.events.get('reconnecting');
                if (reconHandler)
                    reconHandler(this._reconnectAttempts);
                setTimeout(() => this._connect(), this.options.reconnectionDelay * this._reconnectAttempts);
            }
        };
        this.ws.onerror = (err) => {
            const handler = this.events.get('error');
            if (handler)
                handler(err);
        };
    }
    connect(callback) {
        this._connect();
        if (callback) {
            const checkConnection = setInterval(() => {
                if (this.connected) {
                    clearInterval(checkConnection);
                    callback();
                }
            }, 100);
        }
        return this;
    }
    disconnect() {
        this._isManualClose = true;
        this.stopHeartbeat();
        if (this.ws)
            this.ws.close();
        return this;
    }
    isConnected() {
        return this.connected;
    }
    getUrl() {
        return this.url;
    }
}
export default StelarClient;
