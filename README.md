# stelar-time-real

Your own custom real-time system. A lightweight, dependency-free library for real-time communication via WebSockets.

![npm](https://img.shields.io/npm/v/stelar-time-real)
![license](https://img.shields.io/npm/l/stelar-time-real)
![size](https://img.shields.io/bundlephobia/min/stelar-time-real)

## Why stelar-time-real?

- ⚡ **Ultra lightweight** - Only ~13MB of heap
- 🚀 **No dependencies** - Uses only native `ws` (WebSocket)
- 🎯 **Fully customizable** - You control everything, no one else's code
- 🔌 **Compatible** - Works with Express, Fastify, native HTTP, etc.
- 💓 **Heartbeat included** - Automatically detects disconnections
- 🌐 **Namespaces** - Multiple independent channels (`/chat`, `/game`, etc.)
- ⚡ **Ultra fast ACK** - Request-response with Promises, no overhead
- 📦 **Binaries** - Send images, files, audio, video without base64 overhead

## Installation

```bash
npm install stelar-time-real
```

## Quick Start

### One import for everything

```javascript
import StelarServer, { StelarClient } from 'stelar-time-real';
```

### Server

```javascript
import express from 'express';
import { StelarServer } from 'stelar-time-real';

const app = express();
const server = app.listen(3000);

const stelar = new StelarServer({ server });

stelar.onConnection((client) => {
  console.log('New client:', client.id);
  client.emit('welcome', 'Hello! Welcome to stelar-time-real');
});

stelar.on('message', (ctx) => {
  ctx.broadcast('message', ctx.data);
});

stelar.start();
```

### Client

```javascript
import { StelarClient } from 'stelar-time-real';

const client = new StelarClient('localhost:3000');

client.on('connect', () => {
  console.log('Connected!');
});

client.on('welcome', (msg) => {
  console.log(msg);
});

client.connect();
```

## Full API

### StelarServer (Server Side)

#### Constructor

```javascript
new StelarServer({ server, port, heartbeatInterval })
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| server | http.Server | null | Your existing HTTP server |
| port | number | 3000 | Port if you don't pass server |
| heartbeatInterval | number | 30000 | Ping interval in ms |

#### Methods

**`.use(middleware)`**
Add middleware to validate connections.

```javascript
stelar.use((ctx, next) => {
  const token = ctx.req.headers['x-token'];
  if (token === 'secret') {
    next();
  } else {
    ctx.socket.close();
  }
});
```

**`.on(event, handler)`**
Listen for client events.

```javascript
stelar.on('chat', (ctx) => {
  console.log('Message:', ctx.data);
  ctx.broadcast('chat', ctx.data);
});
```

**`.onAll(handler)`**
Listen for all events (useful for debug).

```javascript
stelar.onAll(({ event, data }) => {
  console.log(`Event: ${event}`, data);
});
```

**`.onConnection(handler)`**
Execute when a client connects.

```javascript
stelar.onConnection((client) => {
  client.emit('welcome', 'Hello!');
});
```

**`.broadcast(event, data)`**
Send to all clients.

```javascript
stelar.broadcast('chat', { message: 'Hello everyone' });
```

**`.to(room, event, data)`**
Send to a specific room.

```javascript
stelar.to('room-1', 'chat', { message: 'Hello room 1' });
```

**`.toId(id, event, data)`**
Send to a specific client by ID.

```javascript
stelar.toId('abc123', 'private', 'Just for you');
```

**`.getClients(room)`**
Get list of clients.

```javascript
const all = stelar.getClients();
const room = stelar.getClients('my-room');
```

**`.getPort()`**
Get the port where it's running.

```javascript
console.log('Port:', stelar.getPort());
```

**`.start(callback)`**
Start the WebSocket server.

```javascript
await stelar.start();
console.log('Started!');
```

**`.stop()`**
Stop the server.

```javascript
stelar.stop();
```

#### Context (ctx) in handlers

When you listen to an event, you receive a `ctx` with:

```javascript
stelar.on('message', (ctx) => {
  ctx.id          // Unique client ID
  ctx.socket      // Client's WebSocket
  ctx.req         // Original HTTP request
  ctx.data        // Received data

  // Available methods:
  ctx.emit('event', data)     // Send to this client only
  ctx.send('response', data)  // Reply to an ACK
  ctx.broadcast('event', data) // Send to everyone
  ctx.to('room', 'event', data) // Send to a room
  ctx.toId('id', 'event', data) // Send to specific client
  ctx.getClients('room')      // See clients in room
  ctx.joinRoom('room')        // Join room
  ctx.leaveRoom()             // Leave room
  ctx.ack('myAck', data)     // Reply to a custom ACK
});
```

#### Namespaces

Create independent channels:

```javascript
import { StelarServer } from 'stelar-time-real';

// Main namespace
const main = new StelarServer({ server, namespace: '/' });

// Chat namespace
const chat = StelarServer.of('/chat', { server });
chat.on('message', (ctx) => {
  ctx.broadcast('message', ctx.data);
});

// Game namespace
const game = StelarServer.of('/game', { server });
game.on('move', (ctx) => {
  ctx.to(ctx.data.room, 'move', ctx.data);
});
```

#### ACK (Request-Response)

Ultra efficient system with Promises:

**Server:**

```javascript
// Register an ACK handler
stelar.onAck('getUser', (ctx) => {
  return { id: ctx.data.id, name: 'John' };
});

// Or with more complex logic
stelar.onAck('saveData', (ctx) => {
  const result = saveToDatabase(ctx.data);
  return { success: true, id: result.id };
});
```

**Client:**

```javascript
// Using request() - returns Promise
const user = await client.request('getUser', { id: 1 }, 'userData');
console.log(user); // { id: 1, name: 'John' }

// Or emit with callback
client.emit('getUser', { id: 1 }, { ack: 'userData' });
client.on('userData', (data) => {
  console.log(data);
});

// ACK from server to client
client.onAck('serverPush', (data) => {
  console.log('Server sent:', data);
});
```

---

### StelarClient (Client Side)

#### Constructor

```javascript
new StelarClient(urlOrPort, options)
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| urlOrPort | string/number | localhost:3000 | Server URL or port |
| options.reconnection | boolean | true | Auto reconnect |
| options.reconnectionAttempts | number | 5 | Reconnection attempts |
| options.reconnectionDelay | number | 1000 | Delay between attempts (ms) |
| options.heartbeatInterval | number | 30000 | Ping interval |

```javascript
// Just port
const client = new StelarClient(3000);

// Full URL
const client = new StelarClient('ws://mydomain.com/ws');

// With options
const client = new StelarClient(3000, {
  reconnection: true,
  reconnectionAttempts: 10,
  reconnectionDelay: 2000
});
```

#### Methods

**`.on(event, handler)`**
Listen for server events.

```javascript
client.on('welcome', (data) => {
  console.log(data);
});
```

**`.onAll(handler)`**
Listen for all events.

```javascript
client.onAll(({ event, data }) => {
  console.log(`${event}:`, data);
});
```

**`.onAck(name, handler)`**
Listen for ACK responses from the server.

```javascript
client.onAck('userData', (data) => {
  console.log('Data received:', data);
});
```

**`.emit(event, data, opts)`**
Send events to the server. Supports `opts.ack` for ACKs.

```javascript
client.emit('chat', { message: 'Hello!' });
client.emit('getUser', { id: 1 }, { ack: 'userData' });
```

**`.request(event, data, ackName)`**
Send and wait for response as Promise.

```javascript
const result = await client.request('getUser', { id: 1 }, 'userData');
console.log(result); // { id: 1, name: 'John' }

// With optional timeout
const client = new StelarClient(3000, { ackTimeout: 10000 });
```

**`.joinRoom(room)`**
Join a room.

```javascript
client.joinRoom('room-1');
```

**`.leaveRoom()`**
Leave current room.

```javascript
client.leaveRoom();
```

**`.connect(callback)`**
Connect to the server.

```javascript
client.connect(() => {
  console.log('Connected!');
});
```

**`.disconnect()`**
Manually disconnect.

```javascript
client.disconnect();
```

**`.isConnected()`**
Check connection status.

```javascript
if (client.isConnected()) {
  console.log('Connected');
}
```

**`.getUrl()`**
Get connection URL.

```javascript
console.log(client.getUrl());
```

#### Client Events

```javascript
client.on('connect', () => {});       // When connected
client.on('disconnect', () => {});     // When disconnected
client.on('reconnecting', (attempt) => {}); // When trying to reconnect
client.on('error', (err) => {});      // When there's an error
```

---

## Examples

### Basic Chat

**server.js**
```javascript
import express from 'express';
import { StelarServer } from 'stelar-time-real';

const app = express();
const server = app.listen(3000);

const stelar = new StelarServer({ server });

stelar.onConnection((client) => {
  client.broadcast('system', 'A user joined');
});

stelar.on('chat', (ctx) => {
  ctx.broadcast('chat', ctx.data);
});

stelar.start();
console.log('Chat at http://localhost:3000');
```

**cliente.html**
```html
<script type="module">
  import { StelarClient } from 'stelar-time-real';

  const client = new StelarClient(3000);

  client.on('connect', () => console.log('Connected'));
  client.on('chat', (msg) => console.log('Chat:', msg));
  client.on('system', (msg) => console.log('System:', msg));

  client.connect();

  // Send messages
  function send(message) {
    client.emit('chat', message);
  }
</script>
```

### Room System

```javascript
// Server
stelar.on('join-room', (ctx) => {
  const room = ctx.data.room;
  ctx.joinRoom(room);
  ctx.emit('welcome', `You joined ${room}`);
});

stelar.on('room-message', (ctx) => {
  ctx.to(ctx.data.room, 'room-message', ctx.data.message);
});

// Client
client.on('join-room', (room) => client.joinRoom(room));
```

### With Auth Middleware

```javascript
stelar.use((ctx, next) => {
  const token = ctx.req.headers['authorization'];
  if (token && token.startsWith('Bearer ')) {
    next(); // Allow connection
  } else {
    ctx.socket.close(); // Reject
  }
});
```

### With Auto Reconnection

```javascript
import { StelarClient } from 'stelar-time-real';

const client = new StelarClient('localhost:3000', {
  reconnection: true,
  reconnectionAttempts: 5,
  reconnectionDelay: 1000
});

client.on('connect', () => console.log('Connected!'));
client.on('disconnect', () => console.log('Disconnected'));
client.on('reconnecting', (attempt) => console.log(`Retrying ${attempt}/5`));

client.connect();
```

### Send Binary Files

```javascript
// Server - receive image
stelar.on('image', (ctx) => {
  // ctx.buffer is a Uint8Array
  console.log('Received:', ctx.buffer.byteLength, 'bytes');
  // Save or process the image
  saveImage(ctx.buffer);

  // Respond to client
  ctx.emit('imageSaved', { success: true });
});

// Client - send image
const input = document.querySelector('input[type="file"]');
input.addEventListener('change', async (e) => {
  const file = e.target.files[0];
  const buffer = await file.arrayBuffer();
  client.emitBinary('image', buffer);
});

// Client - receive image
client.on('image', (buffer) => {
  const blob = new Blob([buffer], { type: 'image/png' });
  const url = URL.createObjectURL(blob);
  document.getElementById('img').src = url;
});
```

### Binary Broadcast

```javascript
// Server - share file with everyone
stelar.on('upload', (ctx) => {
  ctx.broadcastBinary('file', ctx.buffer);
});

// Client - send file
const fileData = await file.arrayBuffer();
client.emitBinary('upload', fileData);
```

---

## Difference with Socket.io

| Feature | stelar-time-real | Socket.io |
|---------|------------------|-----------|
| Heap size | ~13 MB | ~50-100 MB |
| Dependencies | ws (1) | multiple |
| Configuration | minimal | complex |
| Flexibility | total | opinionated |
| Ideal for | own projects | quick production |

## License

MIT - Stelar

## Author

Stelar