# stelar-time-real v3 — Producción

Librería de tiempo real sin dependencias con protocolo binario TCP propio y WebSocket manual. Lista para producción. No necesita `ws`.

![npm](https://img.shields.io/npm/v/stelar-time-real)
![license](https://img.shields.io/npm/l/stelar-time-real)
![zero-deps](https://img.shields.io/badge/dependencias-0-green)
![production](https://img.shields.io/badge/status-producci%C3%B3n-blue)

## Features de Producción

- **Cero dependencias** — No usa `ws`, ni paquetes externos. Solo módulos nativos: `http`, `net`, `crypto`
- **Protocolo dual** — WebSocket (manual RFC 6455) + TCP binario custom
- **Rate limiting** — Token bucket por cliente con límites configurables
- **Max connections** — Rechaza clientes nuevos al alcanzar el límite
- **Múltiples rooms por cliente** — Join/leave rooms específicos, auto-cleanup al desconectar
- **Metadata de cliente** — Guardá datos custom por cliente (rol, username, etc.)
- **Graceful shutdown** — Handlers SIGINT/SIGTERM, cierra conexiones limpiamente
- **Health check endpoint** — HTTP `/health` con stats del servidor
- **Métricas del servidor** — Conexiones activas, mensajes, rooms, uptime
- **Validación de entrada** — Nombres de evento, max payload, max frame size
- **Backpressure** — Manejo del evento drain del socket
- **Cola de mensajes** — Encola mensajes al estar desconectado, auto-drain al reconectar
- **Backoff exponencial** — Reconexión inteligente con jitter
- **Timeouts configurables** — Heartbeat, connect, ACK timeouts
- **Logger** — Logger estructurado con niveles (debug/info/warn/error/silent)
- **Middleware** — Auth, validación, logging en conexiones
- **Sistema ACK** — Request-response con Promesas y cleanup de timeouts
- **Soporte binario** — Envía imágenes, archivos, audio, video sin overhead de base64

## Instalación

```bash
npm install stelar-time-real
```

## Inicio Rápido

### Servidor

```javascript
import express from 'express';
import { StelarServer } from 'stelar-time-real';

const app = express();
const server = app.listen(3000);

const stelar = new StelarServer({
  server,
  maxConnections: 10000,
  maxRoomsPerClient: 50,
  maxPayloadSize: 10 * 1024 * 1024,
  rateLimit: { maxPoints: 100, windowMs: 1000 },
  healthEndpoint: '/health',
  heartbeatInterval: 30000,
  heartbeatTimeout: 60000,
  logger: 'info',
});

stelar.onConnection((client) => {
  client.setMetadata('role', 'user');
  client.emit('bienvenida', 'Hola desde Stelar!');
});

stelar.on('chat', (ctx) => {
  ctx.broadcast('chat', ctx.data, ctx.id); // excluye emisor
});

stelar.onAck('getUser', (ctx) => {
  return { id: ctx.data.id, name: 'John' };
});

await stelar.start();
```

### Cliente (Navegador o Node.js)

```javascript
import { StelarClient } from 'stelar-time-real';

const client = new StelarClient('localhost:3000', {
  reconnection: true,
  reconnectionAttempts: 10,
  reconnectionDelay: 1000,
  maxReconnectionDelay: 30000,
  messageQueueSize: 100,
});

client.on('connect', () => console.log('Conectado!'));
client.connect();

// Request-response con Promise
const user = await client.request('getUser', { id: 1 }, 'getUser');

// Múltiples rooms
client.joinRoom('general');
client.joinRoom('random');
```

### Cliente modo TCP (solo Node.js — máxima eficiencia)

```javascript
const client = new StelarClient('localhost:3000', { mode: 'tcp' });
client.connect();
```

## Health Check

```bash
curl http://localhost:3000/health
```

```json
{
  "status": "ok",
  "activeConnections": 42,
  "totalRooms": 12,
  "uptimeSeconds": 3600,
  "wsConnections": 38,
  "tcpConnections": 4
}
```

## Comparación

| Feature | stelar-time-real v3 | Socket.io | paquete ws |
|---------|---------------------|-----------|------------|
| Dependencias | **0** | Múltiples | 1 (ws) |
| Uso de heap | **~2-3 MB** | ~50-100 MB | ~13 MB |
| Protocolo TCP custom | Si | No | No |
| Rate limiting | Incluido | Plugin | Plugin |
| Health check | Incluido | Plugin | Manual |
| Métricas servidor | Incluido | Plugin | Manual |
| Cola de mensajes | Incluido | No | No |
| Backoff exponencial | Incluido | Si | No |
| Graceful shutdown | Incluido | Manual | Manual |
| Max connections | Incluido | Manual | Manual |
| Múltiples rooms/cliente | Si | Si | Manual |
| Metadata de cliente | Si | Manual | Manual |

## Licencia

MIT — Stelar
