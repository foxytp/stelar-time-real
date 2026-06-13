# stelar-time-real v3

**Real-time library for production.** Zero dependencies. Custom binary TCP protocol + Manual WebSocket (RFC 6455 implemented from scratch). No external packages.

![npm](https://img.shields.io/npm/v/stelar-time-real)
![license](https://img.shields.io/npm/l/stelar-time-real)
![zero-deps](https://img.shields.io/badge/dependencies-0-green)
![production](https://img.shields.io/badge/status-production_ready-blue)

---

## What is stelar-time-real?

stelar-time-real is a real-time communication library designed from scratch for production. It does not wrap nor depend on any external library — it implements its own binary TCP protocol and its own WebSocket (RFC 6455) using exclusively Node.js built-in modules (`http`, `net`, `crypto`, `tls`).

This means total control: no dependencies that break, no third-party vulnerabilities, no bloat, and no surprises. Every byte that travels across the network is controlled by the library.

### What is it for?

- Real-time chat (messaging, notifications, typing indicators)
- Collaborative applications (editors, whiteboards, shared documents)
- Binary data streaming (images, files, audio, video)
- Internal microservices with ultra-fast communication via TCP
- Live dashboards (metrics, monitoring, trading)
- Real-time multiplayer games
- Social networks, Discord-like platforms
- IoT and connected devices

---

## Main Features

### Dual Protocol

The server supports **two protocols simultaneously** on different ports:

- **WebSocket** — For browsers and web clients. Complete manual implementation of RFC 6455: handshake, framing, masking, close codes, RSV bits validation, max frame size enforcement.
- **Custom TCP** — For server-to-server and Node.js microservices communication. Custom binary protocol with minimal overhead (7-byte header). Ultra-low latency.

Both protocols share the same server API. A WebSocket client and a TCP client can be in the same room, receive the same broadcasts, and interact as if they were the same type of connection.

### Zero Dependencies

```
dependencies: {}
```

No `ws`, no `engine.io`, nothing. Just pure Node.js. This means:

- No vulnerabilities in third-party dependencies
- No breaking changes from external updates
- No supply chain attacks
- Minimal size in node_modules
- Instant installation

### Production-Ready

Every feature was designed with a real environment in mind — with users, attacks, and errors:

| Feature | Description |
|---------|-------------|
| **Rate Limiting** | Token bucket per client. Limits how many messages each client can send per time window. Prevents spam and abuse. |
| **Per-IP Throttling** | Limits simultaneous connections from the same IP address. Prevents brute force attacks and bots. |
| **Max Connections** | Global limit of concurrent connections. The server rejects new connections when the limit is reached. |
| **Max Rooms** | Global limit of rooms and per-client limit. Prevents a single client from creating thousands of rooms and consuming memory. |
| **Graceful Shutdown** | Captures SIGINT/SIGTERM, stops accepting new connections, waits for existing ones to close (with configurable timeout), and cleans up all resources. |
| **Health Check** | HTTP `/health` endpoint with live server statistics. Compatible with Kubernetes, Docker, and load balancers. |
| **Server Metrics** | `getStats()` method with: active connections, messages sent/received, rooms, uptime, memory usage, rate limiter entries. |
| **TLS/SSL** | Native support for `wss://` and TCP over TLS. Simple configuration with key and cert. |
| **Origin Checking** | Whitelist of allowed origins for WebSocket connections. Prevents CSRF and cross-origin abuse. |
| **CORS** | Automatic CORS headers on the health endpoint with support for OPTIONS preflight. |
| **Input Validation** | Validation of event names (non-empty strings), max payload size, max frame size. |
| **Backpressure Handling** | Handles the socket's `drain` event. No data is lost when the network buffer is full. |
| **Message Queue** | On the client: message queue when disconnected. Sent automatically upon reconnection. Configurable size, discards oldest if full. |
| **Exponential Backoff** | Smart reconnection with exponential backoff and jitter. Prevents thundering herd when the server restarts. |
| **O(1) Client Lookup** | Client lookup by ID in constant time using an indexed Map. Scalable to tens of thousands of clients. |
| **No Signal Handler Leaks** | SIGINT/SIGTERM handlers are properly cleaned up when calling `stop()`. Multiple instances don't cause `MaxListenersExceeded`. |
| **Timer unref** | All internal timers use `.unref()`. They don't prevent the Node.js process from terminating naturally. |
| **Custom Rate Limiter** | `IRateLimiter` interface to replace the built-in rate limiter with your own implementation (Redis, MongoDB, etc). |
| **Custom IP Tracker** | `IIPTracker` interface to replace the built-in IP tracker with your own logic. |
| **Custom Client ID** | `generateClientId` function to generate IDs with your own format. |
| **Event Rate Limits** | Rate limits per individual event. Each event can have its own message limit. |
| **Per-Client Rate Limits** | Rate limits per individual client with `setClientRateLimit()`. Override the global limit for specific clients. |
| **Hook System** | Callbacks for every server event: rate limit exceeded, max connections, payload too large, join/leave room, etc. |
| **Custom Health Handler** | `customHealthHandler` function to replace the built-in health check with your own logic. |
| **Runtime Config** | `updateConfig()` method to change server configuration on the fly, without restarting. |
| **Client Hooks** | Hooks on the client: `onBeforeEmit`, `onMessage`, `onStateChange`, `onReconnectDelay`, `onMessageQueued`, `onQueueDrained`, `onError`. |
| **Custom Reconnect** | `customReconnectDelay` function or `onReconnectDelay` hook to control client reconnection logic. |
| **Client Runtime Config** | `updateOptions()` method to change client configuration on the fly. |

---

## Installation

```bash
npm install stelar-time-real
```

---

## Quick Start

### Basic Server

```javascript
import express from 'express';
import { StelarServer } from 'stelar-time-real';

const app = express();
const server = app.listen(3000);

const stelar = new StelarServer({ server });

stelar.onConnection((client) => {
  console.log('Connected:', client.id);
  client.emit('welcome', { message: 'Welcome to the server!' });
});

stelar.on('chat', (ctx) => {
  ctx.broadcast('chat', ctx.data, ctx.id);
});

await stelar.start();
```

### Server with Production Configuration

```javascript
import express from 'express';
import { StelarServer } from 'stelar-time-real';

const app = express();
const server = app.listen(3000);

const stelar = new StelarServer({
  server,
  maxConnections: 10000,
  maxConnectionsPerIP: 50,
  maxRooms: 10000,
  maxRoomsPerClient: 50,
  maxPayloadSize: 10 * 1024 * 1024,
  rateLimit: { maxPoints: 100, windowMs: 1000 },
  healthEndpoint: '/health',
  heartbeatInterval: 30000,
  heartbeatTimeout: 60000,
  gracefulShutdown: true,
  shutdownTimeout: 10000,
  allowedOrigins: ['https://mydomain.com'],
  logger: 'info',
});

// Authentication middleware
stelar.use((ctx, next) => {
  const token = ctx.req?.headers?.authorization;
  if (!token) return ctx.ack('error', { message: 'Token required' });
  next();
});

stelar.onConnection((client) => {
  console.log(`[${client.protocol}] Client connected: ${client.id} from ${client.remoteAddress}`);
  client.setMetadata('role', 'user');
  client.emit('welcome', { id: client.id });
});

stelar.onDisconnect((client) => {
  console.log('Client disconnected:', client.id);
});

stelar.on('chat', (ctx) => {
  ctx.broadcast('chat', ctx.data, ctx.id);
});

stelar.onAck('getUser', (ctx) => {
  return { id: ctx.data.id, name: 'John', role: ctx.getMetadata('role') };
});

await stelar.start();
console.log('Server ready on port', stelar.getPort());
```

### Client (Browser or Node.js)

```javascript
import { StelarClient } from 'stelar-time-real';

const client = new StelarClient('localhost:3000', {
  reconnection: true,
  reconnectionAttempts: 10,
  reconnectionDelay: 1000,
  maxReconnectionDelay: 30000,
  ackTimeout: 5000,
  messageQueueSize: 100,
});

client.on('connect', () => console.log('Connected!'));
client.on('disconnect', () => console.log('Disconnected'));
client.on('welcome', (data) => console.log('Welcome:', data));

client.connect();

// Send message
client.emit('chat', { message: 'Hello everyone!' });

// Request-response with Promise
const user = await client.request('getUser', { id: 1 }, 'getUser');
console.log(user); // { id: 1, name: 'John', role: 'user' }

// Join rooms
client.joinRoom('general');
client.joinRoom('random');

// Send binary
const buffer = Buffer.from('binary data');
client.emitBinary('file', buffer);
```

### Client with TCP Mode (Node.js only — maximum efficiency)

```javascript
const client = new StelarClient('localhost:3001', {
  mode: 'tcp',
  reconnection: true,
});

client.on('connect', () => console.log('TCP connected!'));
client.connect();
```

TCP mode uses the custom binary protocol instead of WebSocket. Less overhead, lower latency, ideal for server-to-server communication.

### Client with TLS/WSS (secure connections)

```javascript
// WSS — Secure WebSocket
const client = new StelarClient('wss://secure.mydomain.com', {
  tls: true,
  rejectUnauthorized: true,
});

// TCP + TLS
const client = new StelarClient('secure.mydomain.com:3001', {
  mode: 'tcp',
  tls: true,
  rejectUnauthorized: true,
});
```

### Server with TLS

```javascript
import { readFileSync } from 'fs';

const stelar = new StelarServer({
  port: 3000,
  tls: {
    key: readFileSync('server-key.pem'),
    cert: readFileSync('server-cert.pem'),
  },
  tcpPort: 3001,
});
```

---

## Architecture

### Dual Protocol

```
                        stelar-time-real Server
                       ┌──────────────────────────┐
                       │                          │
   Browsers  ──────►   │  Port 3000 (WebSocket)  │
   (ws://)              │         │                │
                       │    Same logic            │
   Node.js     ──────►  │  Port 3001 (Custom TCP) │
   (tcp mode)           │         │                │
                       │                          │
                       └──────────────────────────┘
```

Both protocols share:
- Same event handlers
- Same rooms
- Same broadcast system
- Same ACK system
- Same middleware
- Same metrics

A WebSocket client and a TCP client can be in the same room and communicate without issues.

### WebSocket Mode vs TCP Mode

| Aspect | WebSocket | Custom TCP |
|---------|-----------|------------|
| Browser | Yes | No |
| Node.js | Yes | Yes |
| Overhead per frame | 2-14 bytes (RFC 6455) | 7 bytes (custom header) |
| Latency | Low | Ultra low |
| TLS | wss:// | Native TLS |
| Use case | Frontend, web apps | Microservices, backend |

### Binary Protocol Format (TCP)

```
┌──────────────┬──────────┬───────────────┬──────────────┬──────────────┐
│ totalLen (4B) │ type (1B)│ eventLen (2B) │ event (N B)  │ payload      │
│ Big Endian   │          │ Big Endian    │ UTF-8 string │ JSON/Binary  │
└──────────────┴──────────┴───────────────┴──────────────┴──────────────┘
```

**11 frame types:**

| Type | Code | Description |
|------|--------|-------------|
| JSON | 0 | Event with JSON payload |
| Binary | 1 | Pure binary data |
| Ping | 2 | Client heartbeat |
| Pong | 3 | Server response |
| ACK Request | 4 | Request expecting response |
| ACK Response | 5 | Response to an ACK request |
| Connect | 6 | Initial connection frame |
| Disconnect | 7 | Disconnection frame |
| Join Room | 8 | Join a room |
| Leave Room | 9 | Leave a room |
| Error | 10 | Error frame |

### Manual WebSocket (RFC 6455)

stelar-time-real implements WebSocket from scratch using only Node.js `http` and `crypto`. It doesn't use the `ws` library or any other.

The implementation includes:
- **Handshake** — Calculates Sec-WebSocket-Accept with SHA-1 per RFC 6455
- **Framing** — Frame parsing and creation (text, binary, ping, pong, close)
- **Masking** — Applies/removes XOR mask (required client→server)
- **Fragmentation** — Fragmented frame handling
- **Close codes** — All close codes supported
- **Validation** — RSV bits, opcode validation, max frame size
- **PING/PONG** — Server responds PONG to PING correctly

---

## Complete API

### StelarServer — Options

```javascript
new StelarServer({
  // Connection
  port: 3000,                     // HTTP/WebSocket port
  server: httpServer,             // Existing HTTP server (alternative to port)
  namespace: '/',                 // Namespace path
  tcpPort: 3001,                  // TCP port (false = disabled)

  // Limits
  maxConnections: 10000,          // Maximum concurrent connections
  maxConnectionsPerIP: 50,        // Maximum connections per IP address
  maxRooms: 10000,                // Maximum global rooms
  maxRoomsPerClient: 50,          // Maximum rooms per client
  maxPayloadSize: 10 * 1024 * 1024,  // Maximum payload size (10MB)
  maxFrameSize: 10 * 1024 * 1024,    // Maximum WebSocket frame size (10MB)

  // Rate Limiting
  rateLimit: {
    maxPoints: 100,               // Maximum points (messages) per window
    windowMs: 1000,               // Time window in milliseconds
  },

  // Timeouts
  heartbeatInterval: 30000,       // Ping interval (30s)
  heartbeatTimeout: 60000,        // Timeout before disconnecting (60s)
  connectTimeout: 10000,          // Initial connection timeout (10s)

  // Production
  healthEndpoint: '/health',      // Health check URL (false = disabled)
  gracefulShutdown: true,         // Capture SIGINT/SIGTERM
  shutdownTimeout: 10000,         // Maximum wait time when closing (10s)
  allowedOrigins: ['https://mydomain.com'],  // Allowed origins (null = all)
  tls: { key, cert },             // TLS options for wss:// and TCP TLS

  // Logging
  logger: 'info',                 // Level: 'debug'|'info'|'warn'|'error'|'silent'
                                  // Also accepts Logger instance or false
});
```

### StelarServer — Methods

#### Events

| Method | Description |
|--------|-------------|
| `.on(event, handler)` | Listen to client events |
| `.onAll(handler)` | Listen to all events |
| `.onConnection(handler)` | Client connected |
| `.onDisconnect(handler)` | Client disconnected |
| `.onAck(name, handler)` | Register ACK handler (returns value to client) |

#### Message Sending

| Method | Description |
|--------|-------------|
| `.broadcast(event, data, excludeId?)` | Send to all clients (optionally exclude one) |
| `.to(room, event, data, excludeId?)` | Send to a room (optionally exclude) |
| `.toId(id, event, data)` | Send to a specific client — O(1) lookup |
| `.broadcastBinary(event, buffer)` | Broadcast binary data |

#### Information

| Method | Description |
|--------|-------------|
| `.getClients(room?)` | Client list with their rooms |
| `.getRoomMembers(room)` | Client IDs in a room |
| `.getRooms()` | List of active rooms |
| `.getStats()` | Server statistics |
| `.getPort()` | Port the server is running on |

#### Lifecycle

| Method | Description |
|--------|-------------|
| `.use(middleware)` | Add connection middleware |
| `.start(callback?)` | Start server, returns `Promise<number>` with the port |
| `.stop()` | Stop server, close connections, clean up handlers |

### StelarContext (ctx) — Inside handlers

Every event handler receives a context (`ctx`) with all available information and actions:

```javascript
stelar.on('message', (ctx) => {
  // Client information
  ctx.id                        // Unique client ID
  ctx.socket                    // Raw net.Socket
  ctx.req                       // HTTP request (null for TCP)
  ctx.data                      // Received data
  ctx.clientInfo                // Client info
  ctx.clientInfo.rooms          // Client's room Set
  ctx.clientInfo.metadata       // Custom metadata Map
  ctx.clientInfo.remoteAddress  // Client's IP address
  ctx.clientInfo.protocol       // 'ws' or 'tcp'

  // Actions — Send messages
  ctx.emit('event', data)               // Send to this client
  ctx.send('response', data)            // Respond to ACK
  ctx.emitBinary('event', buffer)       // Send binary
  ctx.broadcast('event', data)          // Send to all (excluding self)
  ctx.broadcastBinary('event', buf)     // Binary broadcast
  ctx.to('room', 'event', data)         // Send to a room
  ctx.toId('id', 'event', data)         // Send to specific client (O(1))

  // Actions — Rooms
  ctx.joinRoom('room')                  // Join a room
  ctx.leaveRoom('room')                 // Leave a room
  ctx.getClients('room')                // List room clients

  // Actions — Metadata
  ctx.setMetadata('role', 'admin')      // Store custom data
  ctx.getMetadata('role')               // Read custom data

  // Actions — ACK
  ctx.ack('myAck', data)                // Respond to an ACK request
});
```

### StelarClient — Options

```javascript
new StelarClient(urlOrPort, {
  // Connection
  reconnection: true,            // Auto reconnect
  reconnectionAttempts: 10,      // Maximum attempts
  reconnectionDelay: 1000,       // Base delay (ms)
  maxReconnectionDelay: 30000,   // Maximum delay (ms)
  heartbeatInterval: 30000,      // Heartbeat interval

  // Protocol
  mode: 'ws',                    // 'ws' or 'tcp'
  maxPayloadSize: 10 * 1024 * 1024,
  maxFrameSize: 10 * 1024 * 1024,

  // ACK
  ackTimeout: 5000,              // ACK timeout (ms)

  // Message queue
  messageQueueSize: 100,         // Queued messages when disconnected

  // Security
  tls: false,                    // Enable TLS for wss:// or TCP TLS
  rejectUnauthorized: true,      // Validate TLS certificate

  // Custom headers
  headers: {},                   // Headers for WebSocket handshake

  // Logging
  logger: 'warn',                // Log level
});
```

### StelarClient — Methods

#### Events

| Method | Description |
|--------|-------------|
| `.on(event, handler)` | Listen to event |
| `.off(event, handler)` | Remove listener |
| `.once(event, handler)` | Listen once |
| `.onAll(handler)` | Listen to all events |
| `.onAck(name, handler)` | Listen to ACK responses |

#### Sending

| Method | Description |
|--------|-------------|
| `.emit(event, data, opts?)` | Send event (`opts.ack` for ACK) |
| `.emitBinary(event, data)` | Send binary data |
| `.sendFile(file)` | Send file |
| `.sendImage(blob)` | Send image |
| `.request(event, data, ackName)` | Request-response with Promise |

#### Rooms

| Method | Description |
|--------|-------------|
| `.joinRoom(room)` | Join a room |
| `.leaveRoom(room)` | Leave a room |

#### Lifecycle

| Method | Description |
|--------|-------------|
| `.connect(callback?)` | Connect to server |
| `.disconnect()` | Disconnect and clean up all resources |

#### State and Metrics

| Method | Description |
|--------|-------------|
| `.isConnected()` | Is connected? |
| `.getState()` | State: `'disconnected'` \| `'connecting'` \| `'connected'` \| `'reconnecting'` |
| `.getId()` | ID assigned by the server |
| `.getUrl()` | Server URL |
| `.setUrl(url)` | Change URL before connecting |
| `.getMessagesSent()` | Total messages sent |
| `.getMessagesReceived()` | Total messages received |
| `.getLastError()` | Last error |
| `.getConnectTime()` | Timestamp of last successful connection |
| `.getQueueSize()` | Pending messages in queue |
| `.removeAllListeners(event?)` | Clear listeners |

### Client Events

```javascript
client.on('connect', () => {
  // Connection established
});

client.on('disconnect', (info) => {
  // info = { code, reason } for WebSocket
});

client.on('reconnecting', (attempt) => {
  // Reconnection attempt number `attempt`
});

client.on('reconnect_failed', () => {
  // Reconnection attempts exhausted
});

client.on('error', (err) => {
  // Connection or protocol error
});
```

---

## Health Check

The health check endpoint is designed to integrate with orchestrators like Kubernetes, Docker Swarm, or any load balancer.

```bash
curl http://localhost:3000/health
```

Response:

```json
{
  "status": "ok",
  "totalConnections": 150,
  "activeConnections": 42,
  "totalMessagesReceived": 5000,
  "totalMessagesSent": 4800,
  "totalRooms": 12,
  "uptime": 3600000,
  "uptimeSeconds": 3600,
  "wsConnections": 38,
  "tcpConnections": 4,
  "memoryMB": 10.54,
  "memoryUsage": {
    "heapUsed": 11062016,
    "heapTotal": 17301504,
    "rss": 24576000,
    "external": 1245184
  },
  "rateLimiterEntries": 42
}
```

CORS is automatic on the health endpoint. If `allowedOrigins` is configured, the `Access-Control-Allow-Origin` header is added for matching origins. OPTIONS preflight requests return 204.

---

## Middleware

The middleware system allows validating connections before a client is accepted:

```javascript
// Token authentication
stelar.use((ctx, next) => {
  const token = ctx.req?.headers?.authorization;
  if (!token) {
    return ctx.ack('error', { message: 'Token required' });
  }
  // Validate token...
  ctx.setMetadata('userId', getUserIdFromToken(token));
  next();
});

// Custom rate limiting
stelar.use((ctx, next) => {
  const ip = ctx.req?.headers?.['x-forwarded-for'] || ctx.socket.remoteAddress;
  if (isBlocked(ip)) {
    return ctx.socket.destroy();
  }
  next();
});

// Logging
stelar.use((ctx, next) => {
  console.log(`New connection from ${ctx.clientInfo.remoteAddress}`);
  next();
});
```

Multiple middlewares execute in order. If a middleware doesn't call `next()`, the connection is rejected.

---

## Rooms

Rooms are communication channels. A client can be in multiple rooms simultaneously:

```javascript
// Server
stelar.on('joinChannel', (ctx) => {
  ctx.joinRoom(ctx.data.channel);
  ctx.to(ctx.data.channel, 'userJoined', { userId: ctx.id });
});

stelar.on('channelMessage', (ctx) => {
  const rooms = ctx.clientInfo.rooms;
  for (const room of rooms) {
    ctx.to(room, 'channelMessage', ctx.data, ctx.id);
  }
});

// Client
client.joinRoom('general');
client.joinRoom('random');
client.joinRoom('project-alpha');
```

Rooms are automatically cleaned up when the last client leaves or disconnects. No manual resource release needed.

---

## ACK (Request-Response)

The ACK system enables reliable request-response communication over the real-time protocol:

```javascript
// Server — Register ACK handler
stelar.onAck('getUsers', (ctx) => {
  return { users: ['John', 'Mary', 'Peter'] };
});

stelar.onAck('validateToken', (ctx) => {
  const valid = validateToken(ctx.data.token);
  if (!valid) throw new Error('Invalid token');
  return { userId: 123 };
});

// Client — Send request and wait for response
const users = await client.request('getUsers', {}, 'getUsers');
console.log(users); // { users: ['John', 'Mary', 'Peter'] }

try {
  const result = await client.request('validateToken', { token: 'abc' }, 'validateToken');
} catch (err) {
  console.log('Invalid token');
}
```

ACK requests have configurable timeout (`ackTimeout`). If the server doesn't respond within that time, the Promise is rejected.

---

## Binary Data

Send files, images, audio, or any binary data without base64 overhead:

```javascript
// Server — Receive and forward binary
stelar.on('file', (ctx) => {
  ctx.broadcastBinary('file', ctx.data); // ctx.data is a Buffer
});

// Client — Send binary
const imageBuffer = await fs.readFile('photo.png');
client.emitBinary('file', imageBuffer);

// Client — Receive binary
client.on('file', (buffer) => {
  console.log('File received:', buffer.length, 'bytes');
  fs.writeFile('received.png', buffer);
});
```

---

## Server Metrics

```javascript
const stats = stelar.getStats();
console.log(stats);

// {
//   totalConnections: 150,
//   activeConnections: 42,
//   totalMessagesReceived: 5000,
//   totalMessagesSent: 4800,
//   totalRooms: 12,
//   uptime: 3600000,
//   uptimeSeconds: 3600,
//   wsConnections: 38,
//   tcpConnections: 4,
//   memoryMB: 10.54,
//   memoryUsage: { ... },
//   rateLimiterEntries: 42
// }
```

---

## Client Metrics

```javascript
console.log('Messages sent:', client.getMessagesSent());
console.log('Messages received:', client.getMessagesReceived());
console.log('Connection time:', client.getConnectTime());
console.log('Last error:', client.getLastError());
console.log('Messages in queue:', client.getQueueSize());
console.log('State:', client.getState());
console.log('Connected?', client.isConnected());
```

---

## Horizontal Scalability

stelar-time-real runs on a single server per instance. To scale to multiple instances, use Redis Pub/Sub as a bridge:

```javascript
import { StelarServer } from 'stelar-time-real';
import Redis from 'redis';

const redis = Redis.createClient();
const stelar = new StelarServer({ port: 3000, tcpPort: 3001 });

// When a broadcast happens on this instance, publish to Redis
stelar.onAll((ctx) => {
  redis.publish('stelar:events', JSON.stringify({
    event: ctx.eventName,
    data: ctx.data,
    excludeId: ctx.id,
  }));
});

// When another instance publishes, emit locally
redis.subscribe('stelar:events', (message) => {
  const { event, data, excludeId } = JSON.parse(message);
  stelar.broadcast(event, data, excludeId);
});
```

---

## Performance

Measurements with stress test (50 WebSocket + 20 TCP clients):

| Metric | Value |
|---------|-------|
| Simultaneous connections | 70 |
| RAM per client | ~58 KB |
| Throughput | 3,425 msg/sec |
| Stable heap | ~10 MB |
| Memory leaks | None detected |
| MaxListeners warnings | 0 |

The library uses ~58KB per connected client. A server with 1GB of RAM can handle approximately 17,000 simultaneous connections.

---

## Project Structure

```
stelar-time-real/
├── src/
│   ├── index.ts        # Server (StelarServer, RateLimiter, IPConnectionTracker)
│   ├── client.ts       # Client (StelarClient, MessageQueue)
│   ├── protocol.ts     # Binary TCP protocol (encode/decode, FrameParser)
│   ├── websocket.ts    # Manual WebSocket RFC 6455 (WSFrameParser, framing)
│   └── logger.ts       # Logger with levels
├── package.json
├── tsconfig.json
└── README.md
```

---

## TypeScript

stelar-time-real is written in TypeScript and includes type definitions (.d.ts). You don't need to install separate @types:

```typescript
import { StelarServer, StelarClient, StelarStats } from 'stelar-time-real';

const server: StelarServer = new StelarServer({ port: 3000 });
const stats: StelarStats = server.getStats();
```

---

## Tests

```bash
# Production tests (54 assertions, 16 suites)
node test-production.mjs

# Stress test (70 clients, throughput, memory)
node test-stress.mjs
```

Coverage: server start/stop, health check, CORS, WS connect/emit/broadcast, TCP connect/emit/reply, rooms, ACK, max connections, rate limiting, server stats, max rooms, O(1) lookup, client metrics, binary data, origin checking, middleware.

---

## Extensible Configuration

stelar-time-real v3.2 gives you total control over every aspect of the server and client. You can replace entire components, add hooks to customize behavior, and change configuration at runtime.

### Custom Rate Limiter

Replace the built-in rate limiter (token bucket) with your own implementation. Ideal for using Redis, MongoDB, or any other store:

```javascript
import { StelarServer, IRateLimiter } from 'stelar-time-real';

// Your own rate limiter with Redis
class RedisRateLimiter implements IRateLimiter {
  private redis; // your Redis connection

  constructor(redisClient) {
    this.redis = redisClient;
  }

  async check(id, cost = 1) {
    const key = `ratelimit:${id}`;
    const current = await this.redis.incr(key);
    if (current === 1) {
      await this.redis.expire(key, 1); // 1 second window
    }
    return current <= 100; // 100 per second
  }

  async reset(id) {
    await this.redis.del(`ratelimit:${id}`);
  }

  async cleanup() {
    // Redis handles expiration automatically
  }

  async size() {
    return 0; // Not applicable with Redis
  }
}

const stelar = new StelarServer({
  port: 3000,
  customRateLimiter: new RedisRateLimiter(redisClient),
});
```

### Custom IP Tracker

Replace the per-IP connection tracker with your own logic. Useful for using a database of blocked IPs or whitelist logic:

```javascript
class CustomIPTracker implements IIPTracker {
  private blockedIPs = new Set(['1.2.3.4', '5.6.7.8']);
  private vipIPs = new Set(['10.0.0.1']);
  private counts = new Map<string, number>();

  check(ip) {
    if (this.blockedIPs.has(ip)) return false; // Blocked IP
    if (this.vipIPs.has(ip)) return true; // VIP no limit
    return (this.counts.get(ip) || 0) < 20; // 20 for normal
  }

  add(ip) { this.counts.set(ip, (this.counts.get(ip) || 0) + 1); }
  remove(ip) { /* ... */ }
  getCount(ip) { return this.counts.get(ip) || 0; }
  cleanup() { /* clean expired entries */ }
}

const stelar = new StelarServer({
  port: 3000,
  customIPTracker: new CustomIPTracker(),
});
```

### Custom Client ID Generator

Generate client IDs with your own format. By default uses UUID v4:

```javascript
const stelar = new StelarServer({
  port: 3000,
  generateClientId: () => {
    return `user_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  },
});
```

### Event-Specific Rate Limits

Each event can have its own rate limit, independent from the global one:

```javascript
const stelar = new StelarServer({
  port: 3000,
  rateLimit: { maxPoints: 100, windowMs: 1000 }, // Global: 100 msg/sec
  eventRateLimits: {
    'chat': { maxPoints: 5, windowMs: 1000 },        // Chat: 5 msg/sec
    'file-upload': { maxPoints: 2, windowMs: 10000 }, // Files: 2 every 10s
    'typing': { maxPoints: 10, windowMs: 1000 },      // Typing: 10 msg/sec
    'location': { maxPoints: 1, windowMs: 5000 },     // Location: 1 every 5s
  },
});

// You can also add/remove at runtime:
stelar.setEventRateLimit('voice', { maxPoints: 50, windowMs: 1000 });
stelar.removeEventRateLimit('voice');
```

### Per-Client Rate Limits

Give specific clients different rate limits. Useful for premium vs free users:

```javascript
stelar.onConnection((ctx) => {
  const role = ctx.getMetadata('role');

  // Premium user: 500 msg/sec
  if (role === 'premium') {
    stelar.setClientRateLimit(ctx.id, { maxPoints: 500, windowMs: 1000 });
  }
  // Verified bot: 1000 msg/sec
  else if (role === 'bot') {
    stelar.setClientRateLimit(ctx.id, { maxPoints: 1000, windowMs: 1000 });
  }
  // Normal user: uses global rate limit (100 msg/sec)
});

// Remove override (reverts to global):
stelar.removeClientRateLimit(clientId);
```

Rate limiting priority is: **per-client override > event-specific > global > custom rate limiter**.

### Hook System (Server)

Hooks let you customize what happens when the server detects an event. Each hook can return `false` to cancel the default action:

```javascript
const stelar = new StelarServer({
  port: 3000,
  hooks: {
    // When a client exceeds the rate limit
    // Return false to NOT disconnect (e.g.: just warn)
    onRateLimitExceeded: ({ clientId, event, protocol }) => {
      console.warn(`Rate limit: ${clientId} on event ${event}`);
      // return false; // Uncomment to NOT disconnect the client
    },

    // When maximum connections is reached
    onMaxConnectionsReached: ({ activeConnections, max, ip }) => {
      console.error(`Server full: ${activeConnections}/${max} from ${ip}`);
      // Send alert to Slack, etc.
    },

    // When a client tries to join a room
    // Return false to REJECT the join
    onClientJoinRoom: ({ clientId, room, metadata }) => {
      const role = metadata.get('role');
      if (room.startsWith('admin-') && role !== 'admin') {
        return false; // Reject: admins only
      }
    },

    // When a client leaves a room
    // Return false to REJECT the leave
    onClientLeaveRoom: ({ clientId, room }) => {
      // Custom logic...
    },

    // When global maximum rooms is reached
    onMaxRoomsReached: ({ clientId, room, totalRooms, max }) => {
      console.warn(`Max rooms: ${totalRooms}/${max}`);
    },

    // When a client exceeds rooms per client
    onMaxRoomsPerClientReached: ({ clientId, room, currentRooms, max }) => {
      console.warn(`Client ${clientId}: ${currentRooms}/${max} rooms`);
    },

    // When a payload is too large
    onPayloadTooLarge: ({ clientId, event, size, max }) => {
      console.warn(`Large payload: ${size} bytes from ${clientId}`);
    },

    // When an invalid message is received
    onInvalidMessage: ({ clientId, reason, protocol }) => {
      console.warn(`Invalid message from ${clientId}: ${reason}`);
    },

    // Before a broadcast
    // Return false to CANCEL the broadcast
    onBeforeBroadcast: ({ event, data, excludeId }) => {
      if (event === 'spam') return false; // Cancel spam broadcast
    },

    // When a client connects
    onClientConnect: ({ clientId, ip, protocol, metadata }) => {
      console.log(`Connected: ${clientId} via ${protocol} from ${ip}`);
    },

    // When a client disconnects
    onClientDisconnect: ({ clientId, ip, protocol, rooms }) => {
      console.log(`Disconnected: ${clientId} was in ${rooms.size} rooms`);
    },
  },
});
```

### Custom Health Check

Replace the built-in health check with your own handler. Useful for adding database checks, disk space, etc:

```javascript
const stelar = new StelarServer({
  port: 3000,
  customHealthHandler: (req, res, stats) => {
    // stats contains all server statistics

    const dbConnected = await checkDatabase();
    const diskSpace = checkDiskSpace();

    res.writeHead(dbConnected && diskSpace > 100 ? 200 : 503, {
      'Content-Type': 'application/json',
    });
    res.end(JSON.stringify({
      status: dbConnected && diskSpace > 100 ? 'healthy' : 'degraded',
      server: stats,
      database: dbConnected ? 'connected' : 'disconnected',
      diskSpaceMB: diskSpace,
      version: '3.2.0',
    }));
  },
});
```

### Runtime Configuration

Change server configuration without restarting:

```javascript
const stelar = new StelarServer({ port: 3000, maxConnections: 100 });
await stelar.start();

// Later... you need more capacity
stelar.updateConfig({
  maxConnections: 500,
  maxRooms: 5000,
  rateLimit: { maxPoints: 200, windowMs: 1000 },
  allowedOrigins: ['https://app.com', 'https://admin.app.com'],
});

// Change hooks at runtime
stelar.updateConfig({
  hooks: {
    onRateLimitExceeded: ({ clientId }) => {
      banUser(clientId); // Auto-ban instead of disconnecting
      return false; // Don't disconnect, you already banned them
    },
  },
});

// View current configuration
const config = stelar.getConfig();
console.log(config);
// {
//   maxConnections: 500,
//   maxRooms: 5000,
//   hasCustomRateLimiter: false,
//   eventRateLimits: [],
//   hooks: ['onRateLimitExceeded'],
//   ...
// }
```

### Client Hooks

Customize client behavior with hooks:

```javascript
const client = new StelarClient('localhost:3000', {
  hooks: {
    // Before sending a message — return false to cancel
    onBeforeEmit: ({ event, data }) => {
      if (event === 'debug') return false; // Don't send debug in production
      console.log(`Sending: ${event}`);
    },

    // When any message is received
    onMessage: ({ event, data, isBinary }) => {
      metrics.increment('messages.received');
      if (isBinary) metrics.increment('binary.received');
    },

    // When connection state changes
    onStateChange: ({ from, to }) => {
      console.log(`State: ${from} -> ${to}`);
      if (to === 'reconnecting') showReconnectingUI();
      if (to === 'connected') hideReconnectingUI();
    },

    // Customize reconnection delay
    onReconnectDelay: ({ attempt, defaultDelay }) => {
      // Business hours: fast reconnection
      const hour = new Date().getHours();
      if (hour >= 9 && hour <= 18) return 500;
      return defaultDelay; // Off-hours: normal delay
    },

    // When a message is queued (disconnected)
    onMessageQueued: ({ event, queueSize }) => {
      console.log(`Message queued: ${event} (queue: ${queueSize})`);
    },

    // When queue is drained after reconnecting
    onQueueDrained: ({ count }) => {
      console.log(`${count} messages sent after reconnecting`);
    },

    // When an error occurs
    onError: ({ error, context }) => {
      errorReporter.report(error, { context });
    },
  },
});
```

### Custom Reconnect Delay

Control exactly how long to wait before each reconnection attempt:

```javascript
// Option 1: Custom function
const client = new StelarClient('localhost:3000', {
  customReconnectDelay: (attempt, baseDelay, maxDelay) => {
    // Fast retry for first 3 attempts, then slow
    if (attempt <= 3) return 200;
    if (attempt <= 10) return 2000;
    return 30000; // 30s for later attempts
  },
});

// Option 2: Via hook (can change at runtime)
const client = new StelarClient('localhost:3000', {
  hooks: {
    onReconnectDelay: ({ attempt, defaultDelay }) => {
      return Math.min(100 * attempt, 10000); // Linear instead of exponential
    },
  },
});
```

### Client Runtime Configuration

Change client configuration without reconnecting:

```javascript
const client = new StelarClient('localhost:3000');
client.connect();

// Later... adjust timeouts
client.updateOptions({
  heartbeatInterval: 15000,
  ackTimeout: 10000,
  maxPayloadSize: 50 * 1024 * 1024, // 50MB
  hooks: {
    onBeforeEmit: ({ event }) => {
      if (event === 'log') return false; // No longer send logs
    },
  },
});

// View current configuration
const opts = client.getOptions();
console.log(opts);
```

---

## License

MIT — Stelar
