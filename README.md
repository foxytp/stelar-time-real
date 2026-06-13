# stelar-time-real v3

**Librería en tiempo real para producción.** Zero dependencias. Protocolo binario TCP custom + WebSocket manual (RFC 6455 implementado desde cero). Sin paquetes externos.

![npm](https://img.shields.io/npm/v/stelar-time-real)
![license](https://img.shields.io/npm/l/stelar-time-real)
![zero-deps](https://img.shields.io/badge/dependencies-0-green)
![production](https://img.shields.io/badge/status-production_ready-blue)

---

## Que es stelar-time-real?

stelar-time-real es una librería de comunicación en tiempo real diseñada desde cero para producción. No envuelve ni depende de ninguna librería externa — implementa su propio protocolo binario TCP y su propio WebSocket (RFC 6455) usando exclusivamente los módulos built-in de Node.js (`http`, `net`, `crypto`, `tls`).

Esto significa control total: sin dependencias que se rompan, sin vulnerabilidades de terceros, sin bloat, y sin sorpresas. Cada byte que viaja por la red es controlado por la librería.

### Para que sirve?

- Chat en tiempo real (mensajería, notificaciones, typing indicators)
- Aplicaciones colaborativas (editores, pizarras, documentos compartidos)
- Streaming de datos binarios (imágenes, archivos, audio, video)
- Microservicios internos con comunicación ultra rápida via TCP
- Dashboards en vivo (métricas, monitoreo, trading)
- Juegos multijugador en tiempo real
- Redes sociales, plataformas tipo Discord
- IoT y dispositivos conectados

---

## Características Principales

### Protocolo Dual

El servidor soporta **dos protocolos simultáneamente** en puertos diferentes:

- **WebSocket** — Para navegadores y clientes web. Implementación manual completa de RFC 6455: handshake, framing, masking, close codes, validación de RSV bits, max frame size enforcement.
- **TCP Custom** — Para comunicación entre servidores y microservicios Node.js. Protocolo binario propio con overhead mínimo (7 bytes de header). Latencia ultra baja.

Ambos protocolos comparten la misma API del servidor. Un cliente WebSocket y un cliente TCP pueden estar en el mismo room, recibir los mismos broadcasts, e interactuar como si fueran el mismo tipo de conexión.

### Zero Dependencias

```
dependencies: {}
```

No hay `ws`, no hay `engine.io`, no hay nada. Solo Node.js puro. Esto significa:

- Sin vulnerabilidades en dependencias de terceros
- Sin breaking changes por actualizaciones ajenas
- Sin supply chain attacks
- Tamaño mínimo en node_modules
- Instalación instantánea

### Preparada para Producción

Cada feature fue diseñada pensando en un entorno real con usuarios, ataques, y errores:

| Feature | Descripción |
|---------|-------------|
| **Rate Limiting** | Token bucket por cliente. Limita cuántos mensajes puede enviar cada cliente por ventana de tiempo. Previene spam y abuso. |
| **Per-IP Throttling** | Limita conexiones simultáneas desde la misma dirección IP. Previene ataques de fuerza bruta y bots. |
| **Max Connections** | Límite global de conexiones concurrentes. El servidor rechaza nuevas conexiones cuando se alcanza el límite. |
| **Max Rooms** | Límite global de rooms y límite por cliente. Previene que un solo cliente cree miles de rooms y consuma memoria. |
| **Graceful Shutdown** | Captura SIGINT/SIGTERM, deja de aceptar conexiones nuevas, espera a que las existentes se cierren (con timeout configurable), y limpia todos los recursos. |
| **Health Check** | Endpoint HTTP `/health` con estadísticas del servidor en vivo. Compatible con Kubernetes, Docker, y load balancers. |
| **Server Metrics** | Método `getStats()` con: conexiones activas, mensajes enviados/recibidos, rooms, uptime, uso de memoria, entradas del rate limiter. |
| **TLS/SSL** | Soporte nativo para `wss://` y TCP sobre TLS. Configuración simple con key y cert. |
| **Origin Checking** | Whitelist de orígenes permitidos para conexiones WebSocket. Previene CSRF y cross-origin abuse. |
| **CORS** | Headers CORS automáticos en el health endpoint con soporte para OPTIONS preflight. |
| **Input Validation** | Validación de nombres de eventos (strings no vacíos), tamaño máximo de payload, tamaño máximo de frames. |
| **Backpressure Handling** | Manejo del evento `drain` del socket. No se pierden datos cuando el buffer de red está lleno. |
| **Message Queue** | En el cliente: cola de mensajes cuando está desconectado. Se envían automáticamente al reconectar. Tamaño configurable, descarta los más viejos si se llena. |
| **Exponential Backoff** | Reconexión inteligente con backoff exponencial y jitter. Evita thundering herd cuando el servidor se reinicia. |
| **O(1) Client Lookup** | Búsqueda de cliente por ID en tiempo constante usando un Map indexado. Escalable a decenas de miles de clientes. |
| **No Signal Handler Leaks** | Los handlers de SIGINT/SIGTERM se limpian correctamente al hacer `stop()`. Múltiples instancias no causan `MaxListenersExceeded`. |
| **Timer unref** | Todos los timers internos usan `.unref()`. No impiden que el proceso de Node.js termine naturalmente. |
| **Custom Rate Limiter** | Interfaz `IRateLimiter` para reemplazar el rate limiter built-in con tu propia implementación (Redis, MongoDB, etc). |
| **Custom IP Tracker** | Interfaz `IIPTracker` para reemplazar el tracker de IP built-in con tu propia lógica. |
| **Custom Client ID** | Función `generateClientId` para generar IDs con tu propio formato. |
| **Event Rate Limits** | Rate limits por evento individual. Cada evento puede tener su propio límite de mensajes. |
| **Per-Client Rate Limits** | Rate limits por cliente individual con `setClientRateLimit()`. Override del límite global para clientes específicos. |
| **Hook System** | Callbacks para cada evento del servidor: rate limit excedido, max connections, payload too large, join/leave room, etc. |
| **Custom Health Handler** | Función `customHealthHandler` para reemplazar el health check built-in con tu propia lógica. |
| **Runtime Config** | Método `updateConfig()` para cambiar la configuración del servidor en caliente, sin reiniciar. |
| **Client Hooks** | Hooks en el cliente: `onBeforeEmit`, `onMessage`, `onStateChange`, `onReconnectDelay`, `onMessageQueued`, `onQueueDrained`, `onError`. |
| **Custom Reconnect** | Función `customReconnectDelay` o hook `onReconnectDelay` para controlar la lógica de reconexión del cliente. |
| **Client Runtime Config** | Método `updateOptions()` para cambiar la configuración del cliente en caliente. |

---

## Instalación

```bash
npm install stelar-time-real
```

---

## Quick Start

### Servidor basico

```javascript
import express from 'express';
import { StelarServer } from 'stelar-time-real';

const app = express();
const server = app.listen(3000);

const stelar = new StelarServer({ server });

stelar.onConnection((client) => {
  console.log('Conectado:', client.id);
  client.emit('welcome', { message: 'Bienvenido al servidor!' });
});

stelar.on('chat', (ctx) => {
  ctx.broadcast('chat', ctx.data, ctx.id);
});

await stelar.start();
```

### Servidor con configuracion de produccion

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
  allowedOrigins: ['https://midominio.com'],
  logger: 'info',
});

// Middleware de autenticación
stelar.use((ctx, next) => {
  const token = ctx.req?.headers?.authorization;
  if (!token) return ctx.ack('error', { message: 'Token requerido' });
  next();
});

stelar.onConnection((client) => {
  console.log(`[${client.protocol}] Cliente conectado: ${client.id} desde ${client.remoteAddress}`);
  client.setMetadata('role', 'user');
  client.emit('welcome', { id: client.id });
});

stelar.onDisconnect((client) => {
  console.log('Cliente desconectado:', client.id);
});

stelar.on('chat', (ctx) => {
  ctx.broadcast('chat', ctx.data, ctx.id);
});

stelar.onAck('getUser', (ctx) => {
  return { id: ctx.data.id, name: 'Juan', role: ctx.getMetadata('role') };
});

await stelar.start();
console.log('Servidor listo en puerto', stelar.getPort());
```

### Cliente (Navegador o Node.js)

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

client.on('connect', () => console.log('Conectado!'));
client.on('disconnect', () => console.log('Desconectado'));
client.on('welcome', (data) => console.log('Bienvenido:', data));

client.connect();

// Enviar mensaje
client.emit('chat', { message: 'Hola a todos!' });

// Request-response con Promise
const user = await client.request('getUser', { id: 1 }, 'getUser');
console.log(user); // { id: 1, name: 'Juan', role: 'user' }

// Unirse a rooms
client.joinRoom('general');
client.joinRoom('random');

// Enviar binario
const buffer = Buffer.from('datos binarios');
client.emitBinary('file', buffer);
```

### Cliente con modo TCP (Node.js unicamente — maxima eficiencia)

```javascript
const client = new StelarClient('localhost:3001', {
  mode: 'tcp',
  reconnection: true,
});

client.on('connect', () => console.log('TCP conectado!'));
client.connect();
```

El modo TCP usa el protocolo binario custom en vez de WebSocket. Menos overhead, menor latencia, ideal para comunicación entre servidores.

### Cliente con TLS/WSS (conexiones seguras)

```javascript
// WSS — WebSocket seguro
const client = new StelarClient('wss://secure.midominio.com', {
  tls: true,
  rejectUnauthorized: true,
});

// TCP + TLS
const client = new StelarClient('secure.midominio.com:3001', {
  mode: 'tcp',
  tls: true,
  rejectUnauthorized: true,
});
```

### Servidor con TLS

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

## Arquitectura

### Protocolo Dual

```
                        Servidor stelar-time-real
                       ┌──────────────────────────┐
                       │                          │
   Navegadores  ──────►  Puerto 3000 (WebSocket)  │
   (ws://)              │         │                │
                       │    Misma lógica          │
   Node.js      ──────►  Puerto 3001 (TCP Custom) │
   (modo tcp)           │         │                │
                       │                          │
                       └──────────────────────────┘
```

Ambos protocolos comparten:
- Mismos event handlers
- Mismos rooms
- Mismo sistema de broadcast
- Mismo sistema de ACK
- Mismo middleware
- Mismas métricas

Un cliente WebSocket y un cliente TCP pueden estar en el mismo room y comunicarse sin problemas.

### WebSocket Mode vs TCP Mode

| Aspecto | WebSocket | TCP Custom |
|---------|-----------|------------|
| Navegador | Si | No |
| Node.js | Si | Si |
| Overhead por frame | 2-14 bytes (RFC 6455) | 7 bytes (header custom) |
| Latencia | Baja | Ultra baja |
| TLS | wss:// | TLS nativo |
| Caso de uso | Frontend, apps web | Microservicios, backend |

### Formato del Protocolo Binario (TCP)

```
┌──────────────┬──────────┬───────────────┬──────────────┬──────────────┐
│ totalLen (4B) │ type (1B)│ eventLen (2B) │ event (N B)  │ payload      │
│ Big Endian   │          │ Big Endian    │ UTF-8 string │ JSON/Binary  │
└──────────────┴──────────┴───────────────┴──────────────┴──────────────┘
```

**11 tipos de frame:**

| Tipo | Código | Descripción |
|------|--------|-------------|
| JSON | 0 | Evento con payload JSON |
| Binary | 1 | Datos binarios puros |
| Ping | 2 | Heartbeat del cliente |
| Pong | 3 | Respuesta del servidor |
| ACK Request | 4 | Petición que espera respuesta |
| ACK Response | 5 | Respuesta a una petición ACK |
| Connect | 6 | Frame de conexión inicial |
| Disconnect | 7 | Frame de desconexión |
| Join Room | 8 | Unirse a un room |
| Leave Room | 9 | Salir de un room |
| Error | 10 | Frame de error |

### WebSocket Manual (RFC 6455)

stelar-time-real implementa WebSocket desde cero usando solo `http` y `crypto` de Node.js. No usa la librería `ws` ni ninguna otra.

La implementación incluye:
- **Handshake** — Calcula el Sec-WebSocket-Accept con SHA-1 según RFC 6455
- **Framing** — Parseo y creación de frames (text, binary, ping, pong, close)
- **Masking** — Aplica/desaplica XOR mask (requerido cliente→servidor)
- **Fragmentación** — Manejo de frames fragmentados
- **Close codes** — Todos los códigos de cierre soportados
- **Validación** — RSV bits, opcode validation, max frame size
- **PING/PONG** — El servidor responde PONG a PING correctamente

---

## API Completa

### StelarServer — Opciones

```javascript
new StelarServer({
  // Conexión
  port: 3000,                     // Puerto HTTP/WebSocket
  server: httpServer,             // Servidor HTTP existente (alternativa a port)
  namespace: '/',                 // Path del namespace
  tcpPort: 3001,                  // Puerto TCP (false = deshabilitado)

  // Límites
  maxConnections: 10000,          // Máximo de conexiones concurrentes
  maxConnectionsPerIP: 50,        // Máximo de conexiones por dirección IP
  maxRooms: 10000,                // Máximo de rooms globales
  maxRoomsPerClient: 50,          // Máximo de rooms por cliente
  maxPayloadSize: 10 * 1024 * 1024,  // Tamaño máximo de payload (10MB)
  maxFrameSize: 10 * 1024 * 1024,    // Tamaño máximo de frame WebSocket (10MB)

  // Rate Limiting
  rateLimit: {
    maxPoints: 100,               // Máximo de puntos (mensajes) por ventana
    windowMs: 1000,               // Ventana de tiempo en milisegundos
  },

  // Timeouts
  heartbeatInterval: 30000,       // Intervalo de ping (30s)
  heartbeatTimeout: 60000,        // Timeout antes de desconectar (60s)
  connectTimeout: 10000,          // Timeout de conexión inicial (10s)

  // Producción
  healthEndpoint: '/health',      // URL del health check (false = deshabilitado)
  gracefulShutdown: true,         // Capturar SIGINT/SIGTERM
  shutdownTimeout: 10000,         // Tiempo máximo de espera al cerrar (10s)
  allowedOrigins: ['https://midominio.com'],  // Orígenes permitidos (null = todos)
  tls: { key, cert },             // Opciones TLS para wss:// y TCP TLS

  // Logging
  logger: 'info',                 // Nivel: 'debug'|'info'|'warn'|'error'|'silent'
                                  // También acepta instancia de Logger o false
});
```

### StelarServer — Métodos

#### Eventos

| Método | Descripción |
|--------|-------------|
| `.on(event, handler)` | Escuchar eventos de clientes |
| `.onAll(handler)` | Escuchar todos los eventos |
| `.onConnection(handler)` | Cliente conectado |
| `.onDisconnect(handler)` | Cliente desconectado |
| `.onAck(name, handler)` | Registrar handler ACK (retorna valor al cliente) |

#### Envío de mensajes

| Método | Descripción |
|--------|-------------|
| `.broadcast(event, data, excludeId?)` | Enviar a todos los clientes (opcionalmente excluir uno) |
| `.to(room, event, data, excludeId?)` | Enviar a un room (opcionalmente excluir) |
| `.toId(id, event, data)` | Enviar a un cliente específico — búsqueda O(1) |
| `.broadcastBinary(event, buffer)` | Broadcast de datos binarios |

#### Información

| Método | Descripción |
|--------|-------------|
| `.getClients(room?)` | Lista de clientes con sus rooms |
| `.getRoomMembers(room)` | IDs de clientes en un room |
| `.getRooms()` | Lista de rooms activos |
| `.getStats()` | Estadísticas del servidor |
| `.getPort()` | Puerto en el que corre el servidor |

#### Lifecycle

| Método | Descripción |
|--------|-------------|
| `.use(middleware)` | Agregar middleware de conexión |
| `.start(callback?)` | Iniciar servidor, retorna `Promise<number>` con el puerto |
| `.stop()` | Detener servidor, cerrar conexiones, limpiar handlers |

### StelarContext (ctx) — Dentro de los handlers

Cada handler de evento recibe un contexto (`ctx`) con toda la información y acciones disponibles:

```javascript
stelar.on('message', (ctx) => {
  // Información del cliente
  ctx.id                        // ID único del cliente
  ctx.socket                    // net.Socket crudo
  ctx.req                       // HTTP request (null para TCP)
  ctx.data                      // Datos recibidos
  ctx.clientInfo                // Info del cliente
  ctx.clientInfo.rooms          // Set de rooms del cliente
  ctx.clientInfo.metadata       // Map de metadata custom
  ctx.clientInfo.remoteAddress  // Dirección IP del cliente
  ctx.clientInfo.protocol       // 'ws' o 'tcp'

  // Acciones — Enviar mensajes
  ctx.emit('event', data)               // Enviar a este cliente
  ctx.send('response', data)            // Responder a ACK
  ctx.emitBinary('event', buffer)       // Enviar binario
  ctx.broadcast('event', data)          // Enviar a todos (excluyéndose)
  ctx.broadcastBinary('event', buf)     // Broadcast binario
  ctx.to('room', 'event', data)         // Enviar a un room
  ctx.toId('id', 'event', data)         // Enviar a cliente específico (O(1))

  // Acciones — Rooms
  ctx.joinRoom('room')                  // Unirse a un room
  ctx.leaveRoom('room')                 // Salir de un room
  ctx.getClients('room')                // Listar clientes del room

  // Acciones — Metadata
  ctx.setMetadata('role', 'admin')      // Guardar dato custom
  ctx.getMetadata('role')               // Leer dato custom

  // Acciones — ACK
  ctx.ack('myAck', data)                // Responder a una petición ACK
});
```

### StelarClient — Opciones

```javascript
new StelarClient(urlOrPort, {
  // Conexión
  reconnection: true,            // Auto reconectar
  reconnectionAttempts: 10,      // Máximo de intentos
  reconnectionDelay: 1000,       // Delay base (ms)
  maxReconnectionDelay: 30000,   // Delay máximo (ms)
  heartbeatInterval: 30000,      // Intervalo de heartbeat

  // Protocolo
  mode: 'ws',                    // 'ws' o 'tcp'
  maxPayloadSize: 10 * 1024 * 1024,
  maxFrameSize: 10 * 1024 * 1024,

  // ACK
  ackTimeout: 5000,              // Timeout de ACK (ms)

  // Cola de mensajes
  messageQueueSize: 100,         // Mensajes en cola cuando está desconectado

  // Seguridad
  tls: false,                    // Habilitar TLS para wss:// o TCP TLS
  rejectUnauthorized: true,      // Validar certificado TLS

  // Headers custom
  headers: {},                   // Headers para el handshake WebSocket

  // Logging
  logger: 'warn',                // Nivel de log
});
```

### StelarClient — Métodos

#### Eventos

| Método | Descripción |
|--------|-------------|
| `.on(event, handler)` | Escuchar evento |
| `.off(event, handler)` | Remover listener |
| `.once(event, handler)` | Escuchar una sola vez |
| `.onAll(handler)` | Escuchar todos los eventos |
| `.onAck(name, handler)` | Escuchar respuestas ACK |

#### Envío

| Método | Descripción |
|--------|-------------|
| `.emit(event, data, opts?)` | Enviar evento (`opts.ack` para ACK) |
| `.emitBinary(event, data)` | Enviar datos binarios |
| `.sendFile(file)` | Enviar archivo |
| `.sendImage(blob)` | Enviar imagen |
| `.request(event, data, ackName)` | Request-response con Promise |

#### Rooms

| Método | Descripción |
|--------|-------------|
| `.joinRoom(room)` | Unirse a un room |
| `.leaveRoom(room)` | Salir de un room |

#### Lifecycle

| Método | Descripción |
|--------|-------------|
| `.connect(callback?)` | Conectar al servidor |
| `.disconnect()` | Desconectar y limpiar todos los recursos |

#### Estado y métricas

| Método | Descripción |
|--------|-------------|
| `.isConnected()` | Está conectado? |
| `.getState()` | Estado: `'disconnected'` \| `'connecting'` \| `'connected'` \| `'reconnecting'` |
| `.getId()` | ID asignado por el servidor |
| `.getUrl()` | URL del servidor |
| `.setUrl(url)` | Cambiar URL antes de conectar |
| `.getMessagesSent()` | Total de mensajes enviados |
| `.getMessagesReceived()` | Total de mensajes recibidos |
| `.getLastError()` | Último error |
| `.getConnectTime()` | Timestamp de la última conexión exitosa |
| `.getQueueSize()` | Mensajes pendientes en la cola |
| `.removeAllListeners(event?)` | Limpiar listeners |

### Eventos del Cliente

```javascript
client.on('connect', () => {
  // Conexión establecida
});

client.on('disconnect', (info) => {
  // info = { code, reason } para WebSocket
});

client.on('reconnecting', (attempt) => {
  // Intento número `attempt` de reconexión
});

client.on('reconnect_failed', () => {
  // Se agotaron los intentos de reconexión
});

client.on('error', (err) => {
  // Error de conexión o protocolo
});
```

---

## Health Check

El endpoint de health check está diseñado para integrarse con orquestadores como Kubernetes, Docker Swarm, o cualquier load balancer.

```bash
curl http://localhost:3000/health
```

Respuesta:

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

CORS es automático en el health endpoint. Si `allowedOrigins` está configurado, se agrega el header `Access-Control-Allow-Origin` para los orígenes coincidentes. Las peticiones OPTIONS preflight retornan 204.

---

## Middleware

El sistema de middleware permite validar conexiones antes de que un cliente sea aceptado:

```javascript
// Autenticación con token
stelar.use((ctx, next) => {
  const token = ctx.req?.headers?.authorization;
  if (!token) {
    return ctx.ack('error', { message: 'Token requerido' });
  }
  // Validar token...
  ctx.setMetadata('userId', getUserIdFromToken(token));
  next();
});

// Rate limiting custom
stelar.use((ctx, next) => {
  const ip = ctx.req?.headers?.['x-forwarded-for'] || ctx.socket.remoteAddress;
  if (isBlocked(ip)) {
    return ctx.socket.destroy();
  }
  next();
});

// Logging
stelar.use((ctx, next) => {
  console.log(`Nueva conexión desde ${ctx.clientInfo.remoteAddress}`);
  next();
});
```

Múltiples middlewares se ejecutan en orden. Si un middleware no llama a `next()`, la conexión se rechaza.

---

## Rooms

Los rooms son canales de comunicación. Un cliente puede estar en múltiples rooms simultáneamente:

```javascript
// Servidor
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

// Cliente
client.joinRoom('general');
client.joinRoom('random');
client.joinRoom('project-alpha');
```

Los rooms se limpian automáticamente cuando el último cliente sale o se desconecta. No hay que liberar recursos manualmente.

---

## ACK (Request-Response)

El sistema de ACK permite comunicación request-response confiable sobre el protocolo en tiempo real:

```javascript
// Servidor — Registrar handler ACK
stelar.onAck('getUsers', (ctx) => {
  return { users: ['Juan', 'Maria', 'Pedro'] };
});

stelar.onAck('validateToken', (ctx) => {
  const valid = validateToken(ctx.data.token);
  if (!valid) throw new Error('Token inválido');
  return { userId: 123 };
});

// Cliente — Enviar petición y esperar respuesta
const users = await client.request('getUsers', {}, 'getUsers');
console.log(users); // { users: ['Juan', 'Maria', 'Pedro'] }

try {
  const result = await client.request('validateToken', { token: 'abc' }, 'validateToken');
} catch (err) {
  console.log('Token inválido');
}
```

Las peticiones ACK tienen timeout configurable (`ackTimeout`). Si el servidor no responde en ese tiempo, la Promise se rechaza.

---

## Datos Binarios

Enviar archivos, imágenes, audio, o cualquier dato binario sin overhead de base64:

```javascript
// Servidor — Recibir y reenviar binario
stelar.on('file', (ctx) => {
  ctx.broadcastBinary('file', ctx.data); // ctx.data es un Buffer
});

// Cliente — Enviar binario
const imageBuffer = await fs.readFile('photo.png');
client.emitBinary('file', imageBuffer);

// Cliente — Recibir binario
client.on('file', (buffer) => {
  console.log('Archivo recibido:', buffer.length, 'bytes');
  fs.writeFile('received.png', buffer);
});
```

---

## Métricas del Servidor

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

## Métricas del Cliente

```javascript
console.log('Mensajes enviados:', client.getMessagesSent());
console.log('Mensajes recibidos:', client.getMessagesReceived());
console.log('Hora de conexión:', client.getConnectTime());
console.log('Último error:', client.getLastError());
console.log('Mensajes en cola:', client.getQueueSize());
console.log('Estado:', client.getState());
console.log('Conectado?', client.isConnected());
```

---

## Escalabilidad Horizontal

stelar-time-real funciona en un solo servidor por instancia. Para escalar a múltiples instancias, usa Redis Pub/Sub como puente:

```javascript
import { StelarServer } from 'stelar-time-real';
import Redis from 'redis';

const redis = Redis.createClient();
const stelar = new StelarServer({ port: 3000, tcpPort: 3001 });

// Cuando un broadcast se hace en esta instancia, publicar en Redis
stelar.onAll((ctx) => {
  redis.publish('stelar:events', JSON.stringify({
    event: ctx.eventName,
    data: ctx.data,
    excludeId: ctx.id,
  }));
});

// Cuando otra instancia publica, emitir localmente
redis.subscribe('stelar:events', (message) => {
  const { event, data, excludeId } = JSON.parse(message);
  stelar.broadcast(event, data, excludeId);
});
```

---

## Performance

Mediciones con stress test (50 WebSocket + 20 TCP clientes):

| Métrica | Valor |
|---------|-------|
| Conexiones simultáneas | 70 |
| RAM por cliente | ~58 KB |
| Throughput | 3,425 msg/sec |
| Heap estable | ~10 MB |
| Memory leaks | No detectados |
| MaxListeners warnings | 0 |

La librería usa ~58KB por cliente conectado. Un servidor con 1GB de RAM puede manejar aproximadamente 17,000 conexiones simultáneas.

---

## Estructura del Proyecto

```
stelar-time-real/
├── src/
│   ├── index.ts        # Servidor (StelarServer, RateLimiter, IPConnectionTracker)
│   ├── client.ts       # Cliente (StelarClient, MessageQueue)
│   ├── protocol.ts     # Protocolo binario TCP (encode/decode, FrameParser)
│   ├── websocket.ts    # WebSocket manual RFC 6455 (WSFrameParser, framing)
│   └── logger.ts       # Logger con niveles
├── package.json
├── tsconfig.json
└── README.md
```

---

## TypeScript

stelar-time-real está escrita en TypeScript e incluye definiciones de tipos (.d.ts). No necesitas instalar @types separados:

```typescript
import { StelarServer, StelarClient, StelarStats } from 'stelar-time-real';

const server: StelarServer = new StelarServer({ port: 3000 });
const stats: StelarStats = server.getStats();
```

---

## Tests

```bash
# Tests de producción (54 assertions, 16 suites)
node test-production.mjs

# Stress test (70 clientes, throughput, memoria)
node test-stress.mjs
```

Cobertura: server start/stop, health check, CORS, WS connect/emit/broadcast, TCP connect/emit/reply, rooms, ACK, max connections, rate limiting, server stats, max rooms, O(1) lookup, client metrics, binary data, origin checking, middleware.

---

## Configuracion Extensible

stelar-time-real v3.2 te da control total sobre cada aspecto del servidor y el cliente. Puedes reemplazar componentes enteros, agregar hooks para personalizar el comportamiento, y cambiar la configuración en runtime.

### Custom Rate Limiter

Reemplaza el rate limiter built-in (token bucket) con tu propia implementación. Ideal para usar Redis, MongoDB, o cualquier otro store:

```javascript
import { StelarServer, IRateLimiter } from 'stelar-time-real';

// Tu propio rate limiter con Redis
class RedisRateLimiter implements IRateLimiter {
  private redis; // tu conexión Redis

  constructor(redisClient) {
    this.redis = redisClient;
  }

  async check(id, cost = 1) {
    const key = `ratelimit:${id}`;
    const current = await this.redis.incr(key);
    if (current === 1) {
      await this.redis.expire(key, 1); // 1 segundo ventana
    }
    return current <= 100; // 100 por segundo
  }

  async reset(id) {
    await this.redis.del(`ratelimit:${id}`);
  }

  async cleanup() {
    // Redis maneja la expiración automáticamente
  }

  async size() {
    return 0; // No aplicable con Redis
  }
}

const stelar = new StelarServer({
  port: 3000,
  customRateLimiter: new RedisRateLimiter(redisClient),
});
```

### Custom IP Tracker

Reemplaza el per-IP connection tracker con tu propia lógica. Útil para usar una base de datos de IPs bloqueadas o lógica de whitelist:

```javascript
class CustomIPTracker implements IIPTracker {
  private blockedIPs = new Set(['1.2.3.4', '5.6.7.8']);
  private vipIPs = new Set(['10.0.0.1']);
  private counts = new Map<string, number>();

  check(ip) {
    if (this.blockedIPs.has(ip)) return false; // IP bloqueada
    if (this.vipIPs.has(ip)) return true; // VIP sin límite
    return (this.counts.get(ip) || 0) < 20; // 20 para normales
  }

  add(ip) { this.counts.set(ip, (this.counts.get(ip) || 0) + 1); }
  remove(ip) { /* ... */ }
  getCount(ip) { return this.counts.get(ip) || 0; }
  cleanup() { /* limpiar entradas expiradas */ }
}

const stelar = new StelarServer({
  port: 3000,
  customIPTracker: new CustomIPTracker(),
});
```

### Custom Client ID Generator

Genera IDs de cliente con tu propio formato. Por defecto usa UUID v4:

```javascript
const stelar = new StelarServer({
  port: 3000,
  generateClientId: () => {
    return `user_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  },
});
```

### Event-Specific Rate Limits

Cada evento puede tener su propio rate limit, independiente del global:

```javascript
const stelar = new StelarServer({
  port: 3000,
  rateLimit: { maxPoints: 100, windowMs: 1000 }, // Global: 100 msg/sec
  eventRateLimits: {
    'chat': { maxPoints: 5, windowMs: 1000 },        // Chat: 5 msg/sec
    'file-upload': { maxPoints: 2, windowMs: 10000 }, // Archivos: 2 cada 10s
    'typing': { maxPoints: 10, windowMs: 1000 },      // Typing: 10 msg/sec
    'location': { maxPoints: 1, windowMs: 5000 },     // Ubicación: 1 cada 5s
  },
});

// También puedes agregar/remover en runtime:
stelar.setEventRateLimit('voice', { maxPoints: 50, windowMs: 1000 });
stelar.removeEventRateLimit('voice');
```

### Per-Client Rate Limits

Dale a clientes específicos rate limits diferentes. Útil para usuarios premium vs gratuitos:

```javascript
stelar.onConnection((ctx) => {
  const role = ctx.getMetadata('role');

  // Usuario premium: 500 msg/sec
  if (role === 'premium') {
    stelar.setClientRateLimit(ctx.id, { maxPoints: 500, windowMs: 1000 });
  }
  // Usuario bot verificado: 1000 msg/sec
  else if (role === 'bot') {
    stelar.setClientRateLimit(ctx.id, { maxPoints: 1000, windowMs: 1000 });
  }
  // Usuario normal: usa el rate limit global (100 msg/sec)
});

// Remover override (vuelve al global):
stelar.removeClientRateLimit(clientId);
```

La prioridad de rate limiting es: **per-client override > event-specific > global > custom rate limiter**.

### Hook System (Servidor)

Hooks te permiten personalizar lo que pasa cuando el servidor detecta un evento. Cada hook puede retornar `false` para cancelar la acción por defecto:

```javascript
const stelar = new StelarServer({
  port: 3000,
  hooks: {
    // Cuando un cliente excede el rate limit
    // Return false para NO desconectar (ej: solo warn)
    onRateLimitExceeded: ({ clientId, event, protocol }) => {
      console.warn(`Rate limit: ${clientId} en evento ${event}`);
      // return false; // Descomenta para NO desconectar al cliente
    },

    // Cuando se alcanza el máximo de conexiones
    onMaxConnectionsReached: ({ activeConnections, max, ip }) => {
      console.error(`Servidor lleno: ${activeConnections}/${max} desde ${ip}`);
      // Enviar alerta a Slack, etc.
    },

    // Cuando un cliente intenta unirse a un room
    // Return false para RECHAZAR el join
    onClientJoinRoom: ({ clientId, room, metadata }) => {
      const role = metadata.get('role');
      if (room.startsWith('admin-') && role !== 'admin') {
        return false; // Rechazar: solo admins
      }
    },

    // Cuando un cliente sale de un room
    // Return false para RECHAZAR el leave
    onClientLeaveRoom: ({ clientId, room }) => {
      // Lógica custom...
    },

    // Cuando se alcanza el máximo de rooms global
    onMaxRoomsReached: ({ clientId, room, totalRooms, max }) => {
      console.warn(`Max rooms: ${totalRooms}/${max}`);
    },

    // Cuando un cliente excede rooms por cliente
    onMaxRoomsPerClientReached: ({ clientId, room, currentRooms, max }) => {
      console.warn(`Cliente ${clientId}: ${currentRooms}/${max} rooms`);
    },

    // Cuando un payload es demasiado grande
    onPayloadTooLarge: ({ clientId, event, size, max }) => {
      console.warn(`Payload grande: ${size} bytes de ${clientId}`);
    },

    // Cuando se recibe un mensaje inválido
    onInvalidMessage: ({ clientId, reason, protocol }) => {
      console.warn(`Mensaje inválido de ${clientId}: ${reason}`);
    },

    // Antes de un broadcast
    // Return false para CANCELAR el broadcast
    onBeforeBroadcast: ({ event, data, excludeId }) => {
      if (event === 'spam') return false; // Cancelar broadcast de spam
    },

    // Cuando un cliente se conecta
    onClientConnect: ({ clientId, ip, protocol, metadata }) => {
      console.log(`Conectado: ${clientId} via ${protocol} desde ${ip}`);
    },

    // Cuando un cliente se desconecta
    onClientDisconnect: ({ clientId, ip, protocol, rooms }) => {
      console.log(`Desconectado: ${clientId} estaba en ${rooms.size} rooms`);
    },
  },
});
```

### Custom Health Check

Reemplaza el health check built-in con tu propio handler. Útil para agregar checks de base de datos, disk space, etc:

```javascript
const stelar = new StelarServer({
  port: 3000,
  customHealthHandler: (req, res, stats) => {
    // stats contiene todas las estadísticas del servidor

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

Cambia la configuración del servidor sin reiniciar:

```javascript
const stelar = new StelarServer({ port: 3000, maxConnections: 100 });
await stelar.start();

// Más tarde... necesitas más capacidad
stelar.updateConfig({
  maxConnections: 500,
  maxRooms: 5000,
  rateLimit: { maxPoints: 200, windowMs: 1000 },
  allowedOrigins: ['https://app.com', 'https://admin.app.com'],
});

// Cambiar hooks en runtime
stelar.updateConfig({
  hooks: {
    onRateLimitExceeded: ({ clientId }) => {
      banUser(clientId); // Ban automático en vez de desconectar
      return false; // No desconectar, ya lo baneaste
    },
  },
});

// Ver configuración actual
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

Personaliza el comportamiento del cliente con hooks:

```javascript
const client = new StelarClient('localhost:3000', {
  hooks: {
    // Antes de enviar un mensaje — return false para cancelar
    onBeforeEmit: ({ event, data }) => {
      if (event === 'debug') return false; // No enviar debug en producción
      console.log(`Enviando: ${event}`);
    },

    // Cuando se recibe cualquier mensaje
    onMessage: ({ event, data, isBinary }) => {
      metrics.increment('messages.received');
      if (isBinary) metrics.increment('binary.received');
    },

    // Cuando cambia el estado de conexión
    onStateChange: ({ from, to }) => {
      console.log(`Estado: ${from} -> ${to}`);
      if (to === 'reconnecting') showReconnectingUI();
      if (to === 'connected') hideReconnectingUI();
    },

    // Personalizar el delay de reconexión
    onReconnectDelay: ({ attempt, defaultDelay }) => {
      // Horario laboral: reconexión rápida
      const hour = new Date().getHours();
      if (hour >= 9 && hour <= 18) return 500;
      return defaultDelay; // Fuera de horario: delay normal
    },

    // Cuando un mensaje se encola (desconectado)
    onMessageQueued: ({ event, queueSize }) => {
      console.log(`Mensaje encolado: ${event} (cola: ${queueSize})`);
    },

    // Cuando se drena la cola después de reconectar
    onQueueDrained: ({ count }) => {
      console.log(`${count} mensajes enviados después de reconectar`);
    },

    // Cuando ocurre un error
    onError: ({ error, context }) => {
      errorReporter.report(error, { context });
    },
  },
});
```

### Custom Reconnect Delay

Controla exactamente cuánto esperar antes de cada reintento de reconexión:

```javascript
// Opción 1: Función custom
const client = new StelarClient('localhost:3000', {
  customReconnectDelay: (attempt, baseDelay, maxDelay) => {
    // Retry rápido los primeros 3 intentos, luego lento
    if (attempt <= 3) return 200;
    if (attempt <= 10) return 2000;
    return 30000; // 30s para intentos posteriores
  },
});

// Opción 2: Via hook (puedes cambiar en runtime)
const client = new StelarClient('localhost:3000', {
  hooks: {
    onReconnectDelay: ({ attempt, defaultDelay }) => {
      return Math.min(100 * attempt, 10000); // Lineal en vez de exponencial
    },
  },
});
```

### Client Runtime Configuration

Cambia la configuración del cliente sin reconectar:

```javascript
const client = new StelarClient('localhost:3000');
client.connect();

// Más tarde... necesitas ajustar timeouts
client.updateOptions({
  heartbeatInterval: 15000,
  ackTimeout: 10000,
  maxPayloadSize: 50 * 1024 * 1024, // 50MB
  hooks: {
    onBeforeEmit: ({ event }) => {
      if (event === 'log') return false; // Ya no enviar logs
    },
  },
});

// Ver configuración actual
const opts = client.getOptions();
console.log(opts);
```

---

## License

MIT — Stelar
