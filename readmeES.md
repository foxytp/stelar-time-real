# stelar-time-real

Tu propio sistema de tiempo real personalizado. Una librería ligera y sin dependencias para comunicación en tiempo real via WebSockets.

![npm](https://img.shields.io/npm/v/stelar-time-real)
![license](https://img.shields.io/npm/l/stelar-time-real)
![size](https://img.shields.io/bundlephobia/min/stelar-time-real)

## ¿Por qué stelar-time-real?

- ⚡ **Ultra ligera** - Solo ~13MB de heap
- 🚀 **Sin dependencias** - Usa solo `ws` (WebSocket nativo)
- 🎯 **Personalizable** - Vos controlás todo, nada de código ajeno
- 🔌 **Compatible** - Funciona con Express, Fastify, HTTP nativo, etc.
- 💓 **Heartbeat incluido** - Detecta desconexiones automáticamente
- 🌐 **Namespaces** - Múltiples canales independientes (`/chat`, `/game`, etc.)
- ⚡ **ACK ultra rápido** - Request-response con Promesas, sin overhead
- 📦 **Binarios** - Envía imágenes, archivos, audio, video sin overhead de base64

## Instalación

```bash
npm install stelar-time-real
```

## Inicio Rápido

### Un solo import para todo

```javascript
import StelarServer, { StelarClient } from 'stelar-time-real';
```

### Servidor

```javascript
import express from 'express';
import { StelarServer } from 'stelar-time-real';

const app = express();
const server = app.listen(3000);

const stelar = new StelarServer({ server });

stelar.onConnection((client) => {
  console.log('Nuevo cliente:', client.id);
  client.emit('bienvenida', '¡Hola! Bienvenido a stelar-time-real');
});

stelar.on('mensaje', (ctx) => {
  ctx.broadcast('mensaje', ctx.data);
});

stelar.start();
```

### Cliente

```javascript
import { StelarClient } from 'stelar-time-real';

const client = new StelarClient('localhost:3000');

client.on('connect', () => {
  console.log('Conectado!');
});

client.on('bienvenida', (msg) => {
  console.log(msg);
});

client.connect();
```

## API Completa

### StelarServer (Lado del Servidor)

#### Constructor

```javascript
new StelarServer({ server, port, heartbeatInterval })
```

| Opción | Tipo | Default | Descripción |
|--------|------|---------|-------------|
| server | http.Server | null | Tu server HTTP existente |
| port | number | 3000 | Puerto si no pasás server |
| heartbeatInterval | number | 30000 | Intervalo de ping en ms |

#### Métodos

**`.use(middleware)`**
Agregar middleware para validar conexiones.

```javascript
stelar.use((ctx, next) => {
  const token = ctx.req.headers['x-token'];
  if (token === 'secreto') {
    next();
  } else {
    ctx.socket.close();
  }
});
```

**`.on(event, handler)`**
Escuchar eventos del cliente.

```javascript
stelar.on('chat', (ctx) => {
  console.log('Mensaje:', ctx.data);
  ctx.broadcast('chat', ctx.data);
});
```

**`.onAll(handler)`**
Escuchar todos los eventos (útil para debug).

```javascript
stelar.onAll(({ event, data }) => {
  console.log(`Evento: ${event}`, data);
});
```

**`.onConnection(handler)`**
Ejecutar cuando un cliente se conecta.

```javascript
stelar.onConnection((client) => {
  client.emit('bienvenida', 'Hola!');
});
```

**`.broadcast(event, data)`**
Enviar a todos los clientes.

```javascript
stelar.broadcast('chat', { mensaje: 'Hola a todos' });
```

**`.to(room, event, data)`**
Enviar a una sala específica.

```javascript
stelar.to('sala-1', 'chat', { mensaje: 'Hola sala 1' });
```

**`.toId(id, event, data)`**
Enviar a un cliente específico por ID.

```javascript
stelar.toId('abc123', 'privado', 'Solo para ti');
```

**`.getClients(room)`**
Obtener lista de clientes.

```javascript
const todos = stelar.getClients();
const sala = stelar.getClients('mi-sala');
```

**`.getPort()`**
Obtener el puerto donde está corriendo.

```javascript
console.log('Puerto:', stelar.getPort());
```

**`.start(callback)`**
Iniciar el servidor WebSocket.

```javascript
await stelar.start();
console.log('Iniciado!');
```

**`.stop()`**
Detener el servidor.

```javascript
stelar.stop();
```

#### Contexto (ctx) en handlers

Cuando escuchás un evento, recibís un `ctx` con:

```javascript
stelar.on('mensaje', (ctx) => {
  ctx.id          // ID único del cliente
  ctx.socket      // WebSocket del cliente
  ctx.req         // Request HTTP original
  ctx.data        // Datos recibidos

  // Métodos disponibles:
  ctx.emit('evento', data)     // Enviar solo a este cliente
  ctx.send('respuesta', data)  // Responder a un ACK
  ctx.broadcast('evento', data) // Enviar a todos
  ctx.to('sala', 'evento', data) // Enviar a una sala
  ctx.toId('id', 'evento', data) // Enviar a un cliente específico
  ctx.getClients('sala')      // Ver clientes en sala
  ctx.joinRoom('sala')        // Unir a sala
  ctx.leaveRoom()             // Salir de sala
  ctx.ack('miAck', data)     // Responder a un ACK personalizado
});
```

#### Namespaces

Crear canales independientes:

```javascript
import { StelarServer } from 'stelar-time-real';

// Namespace principal
const main = new StelarServer({ server, namespace: '/' });

// Namespace de chat
const chat = StelarServer.of('/chat', { server });
chat.on('message', (ctx) => {
  ctx.broadcast('message', ctx.data);
});

// Namespace de game
const game = StelarServer.of('/game', { server });
game.on('move', (ctx) => {
  ctx.to(ctx.data.room, 'move', ctx.data);
});
```

#### ACK (Request-Response)

Sistema ultra eficiente con Promesas:

**Servidor:**

```javascript
// Registrar un ACK handler
stelar.onAck('getUser', (ctx) => {
  return { id: ctx.data.id, name: 'John' };
});

// O con lógica más compleja
stelar.onAck('saveData', (ctx) => {
  const result = saveToDatabase(ctx.data);
  return { success: true, id: result.id };
});
```

**Cliente:**

```javascript
// Usando request() - retorna Promise
const user = await client.request('getUser', { id: 1 }, 'userData');
console.log(user); // { id: 1, name: 'John' }

// O emitiendo con callback
client.emit('getUser', { id: 1 }, { ack: 'userData' });
client.on('userData', (data) => {
  console.log(data);
});

// ACK desde el servidor al cliente
client.onAck('serverPush', (data) => {
  console.log('El servidor envió:', data);
});
```

---

### StelarClient (Lado del Cliente)

#### Constructor

```javascript
new StelarClient(urlOrPort, options)
```

| Param | Tipo | Default | Descripción |
|-------|------|---------|-------------|
| urlOrPort | string/number | localhost:3000 | URL o puerto del servidor |
| options.reconnection | boolean | true | Reconectar automáticamente |
| options.reconnectionAttempts | number | 5 | Intentos de reconexión |
| options.reconnectionDelay | number | 1000 | Delay entre intentos (ms) |
| options.heartbeatInterval | number | 30000 | Intervalo de ping |

```javascript
// Solo puerto
const client = new StelarClient(3000);

// URL completa
const client = new StelarClient('ws://midominio.com/ws');

// Con opciones
const client = new StelarClient(3000, {
  reconnection: true,
  reconnectionAttempts: 10,
  reconnectionDelay: 2000
});
```

#### Métodos

**`.on(event, handler)`**
Escuchar eventos del servidor.

```javascript
client.on('bienvenida', (data) => {
  console.log(data);
});
```

**`.onAll(handler)`**
Escuchar todos los eventos.

```javascript
client.onAll(({ event, data }) => {
  console.log(`${event}:`, data);
});
```

**`.onAck(name, handler)`**
Escuchar respuestas de ACK del servidor.

```javascript
client.onAck('userData', (data) => {
  console.log('Datos recibidos:', data);
});
```

**`.emit(event, data, opts)`**
Enviar eventos al servidor. Soporta `opts.ack` para ACKs.

```javascript
client.emit('chat', { mensaje: 'Hola!' });
client.emit('getUser', { id: 1 }, { ack: 'userData' });
```

**`.request(event, data, ackName)`**
Enviar y esperar respuesta como Promise.

```javascript
const result = await client.request('getUser', { id: 1 }, 'userData');
console.log(result); // { id: 1, name: 'John' }

// Con timeout opcional
const client = new StelarClient(3000, { ackTimeout: 10000 });
```

**`.joinRoom(room)`**
Unirse a una sala.

```javascript
client.joinRoom('sala-1');
```

**`.leaveRoom()`**
Salir de la sala actual.

```javascript
client.leaveRoom();
```

**`.connect(callback)`**
Conectar al servidor.

```javascript
client.connect(() => {
  console.log('Conectado!');
});
```

**`.disconnect()`**
Desconectar manualmente.

```javascript
client.disconnect();
```

**`.isConnected()`**
Verificar estado de conexión.

```javascript
if (client.isConnected()) {
  console.log('Conectado');
}
```

**`.getUrl()`**
Obtener la URL de conexión.

```javascript
console.log(client.getUrl());
```

#### Eventos del Cliente

```javascript
client.on('connect', () => {});       // Cuando se conecta
client.on('disconnect', () => {});     // Cuando se desconecta
client.on('reconnecting', (attempt) => {}); // Cuando intenta reconectar
client.on('error', (err) => {});      // Cuando hay error
```

---

## Ejemplos

### Chat Básico

**server.js**
```javascript
import express from 'express';
import { StelarServer } from 'stelar-time-real';

const app = express();
const server = app.listen(3000);

const stelar = new StelarServer({ server });

stelar.onConnection((client) => {
  client.broadcast('system', 'Un usuario se unió');
});

stelar.on('chat', (ctx) => {
  ctx.broadcast('chat', ctx.data);
});

stelar.start();
console.log('Chat en http://localhost:3000');
```

**cliente.html**
```html
<script type="module">
  import { StelarClient } from 'stelar-time-real';

  const client = new StelarClient(3000);

  client.on('connect', () => console.log('Conectado'));
  client.on('chat', (msg) => console.log('Chat:', msg));
  client.on('system', (msg) => console.log('Sistema:', msg));

  client.connect();

  // Enviar mensajes
  function enviar(mensaje) {
    client.emit('chat', mensaje);
  }
</script>
```

### Sistema de Rooms

```javascript
// Servidor
stelar.on('unirse-sala', (ctx) => {
  const sala = ctx.data.sala;
  ctx.joinRoom(sala);
  ctx.emit('bienvenida', `Te uniste a ${sala}`);
});

stelar.on('mensaje-sala', (ctx) => {
  ctx.to(ctx.data.sala, 'mensaje-sala', ctx.data.mensaje);
});

// Cliente
client.on('unirse-sala', (sala) => client.joinRoom(sala));
```

### Con Middleware de Auth

```javascript
stelar.use((ctx, next) => {
  const token = ctx.req.headers['authorization'];
  if (token && token.startsWith('Bearer ')) {
    next(); // Permitir conexión
  } else {
    ctx.socket.close(); // Rechazar
  }
});
```

### Con Reconexión Automática

```javascript
import { StelarClient } from 'stelar-time-real';

const client = new StelarClient('localhost:3000', {
  reconnection: true,
  reconnectionAttempts: 5,
  reconnectionDelay: 1000
});

client.on('connect', () => console.log('Conectado!'));
client.on('disconnect', () => console.log('Desconectado'));
client.on('reconnecting', (attempt) => console.log(`Reintentando ${attempt}/5`));

client.connect();
```

### Enviar Archivos Binarios

```javascript
// Servidor - recibir imagen
stelar.on('image', (ctx) => {
  // ctx.buffer es un Uint8Array
  console.log('Recibido:', ctx.buffer.byteLength, 'bytes');
  // Guardar o procesar la imagen
  saveImage(ctx.buffer);

  // Responder al cliente
  ctx.emit('imageSaved', { success: true });
});

// Cliente - enviar imagen
const input = document.querySelector('input[type="file"]');
input.addEventListener('change', async (e) => {
  const file = e.target.files[0];
  const buffer = await file.arrayBuffer();
  client.emitBinary('image', buffer);
});

// Cliente - recibir imagen
client.on('image', (buffer) => {
  const blob = new Blob([buffer], { type: 'image/png' });
  const url = URL.createObjectURL(blob);
  document.getElementById('img').src = url;
});
```

### Broadcast de Binarios

```javascript
// Servidor - compartir archivo con todos
stelar.on('upload', (ctx) => {
  ctx.broadcastBinary('file', ctx.buffer);
});

// Cliente - enviar archivo
const fileData = await file.arrayBuffer();
client.emitBinary('upload', fileData);
```

---

## Diferencia con Socket.io

| Característica | stelar-time-real | Socket.io |
|----------------|------------------|-----------|
| Tamaño heap | ~13 MB | ~50-100 MB |
| Dependencias | ws (1) | múltiples |
| Configuración | mínima | compleja |
| Flexibilidad | total | opinionada |
| Ideal para | proyectos propios | producción rápida |

## Licencia

MIT - Stelar

## Autor

Stelar