import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer } from 'http';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = createServer(app);
const port = process.env.PORT || 8080;

// Раздаем статический index.html для виджета
app.use(express.static(__dirname));

// WebSocket сервер для приема подключений от пользователей (из OBS)
const wss = new WebSocketServer({ server });

// Пул активных стримов
// Ключ: username (чтобы не дублировать подключения, если 10 человек смотрят 1 стрим)
const tikToolsConnections = new Map();

wss.on('connection', (clientWs, req) => {
  // 1. Читаем личный ключ и юзернейм из ссылки, которую ввел пользователь
  const url = new URL(req.url, `http://${req.headers.host}`);
  const apiKey = url.searchParams.get('key');
  const username = url.searchParams.get('u');

  // Если пользователь забыл указать данные - отключаем его
  if (!username || !apiKey) {
    clientWs.send(JSON.stringify({ type: 'error', message: 'Missing key or username in URL' }));
    clientWs.close();
    return;
  }

  console.log(`[Client Connected] OBS requested: ${username}`);

  // 2. Если мы еще не слушаем этого тиктокера - создаем новое подключение к TikTools с ключом клиента
  if (!tikToolsConnections.has(username)) {
    connectToTikTools(username, apiKey);
  }

  // 3. Добавляем зрителя (OBS клиента) в комнату этого тиктокера
  const ttConn = tikToolsConnections.get(username);
  ttConn.clients.add(clientWs);

  // 4. Когда пользователь закрывает OBS
  clientWs.on('close', () => {
    console.log(`[Client Disconnected] Left room: ${username}`);
    const conn = tikToolsConnections.get(username);
    if (conn) {
      conn.clients.delete(clientWs);
      
      // ОПТИМИЗАЦИЯ: Если стримера больше никто не смотрит, разрываем соединение с TikTools
      // Это спасет сервер от перегрузки, когда у вас будут тысячи пользователей
      if (conn.clients.size === 0) {
        console.log(`[Cleanup] No one is watching ${username}. Closing TikTools connection.`);
        if (conn.ws && conn.ws.readyState === WebSocket.OPEN) {
          conn.ws.close();
        }
        if (conn.reconnectTimer) {
          clearTimeout(conn.reconnectTimer);
        }
        tikToolsConnections.delete(username);
      }
    }
  });
});

// Функция подключения к официальному серверу TikTools
function connectToTikTools(username, apiKey) {
  console.log(`[TikTools] Opening connection for ${username}...`);
  
  // Отправляем ключ конкретного пользователя в TikTools
  const wsUrl = `wss://api.tik.tools/?uniqueId=${username}&apiKey=${apiKey}`;
  const ttWs = new WebSocket(wsUrl);

  const connectionData = {
    ws: ttWs,
    clients: new Set(),
    reconnectTimer: null
  };
  
  // Сохраняем существующие клиенты, если переподключаемся
  if (tikToolsConnections.has(username)) {
    connectionData.clients = tikToolsConnections.get(username).clients;
  }
  
  tikToolsConnections.set(username, connectionData);

  ttWs.on('open', () => {
    console.log(`[TikTools] Connected to live stream: ${username}`);
  });

  ttWs.on('message', (message) => {
    try {
      const payload = JSON.parse(message);
      
      // Обработка ошибок от самого TikTools (например, если пользователь ввел неверный ключ)
      if (payload.type === 'error' || payload.error) {
         broadcastToClients(username, { type: 'error', message: payload.message || 'Invalid API Key or TikTools error' });
         return;
      }

      let broadcastData = null;

      // Нормализуем данные
      if (payload.event === 'like' && payload.data) {
        broadcastData = {
          type: 'like',
          nickname: payload.data.user?.nickname || 'User',
          avatar: payload.data.user?.avatarThumb?.urlList?.[0] || '',
          amount: payload.data.likeCount || 1
        };
      } else if (payload.event === 'gift' && payload.data) {
        broadcastData = {
          type: 'gift',
          nickname: payload.data.user?.nickname || 'User',
          avatar: payload.data.user?.avatarThumb?.urlList?.[0] || '',
          giftName: payload.data.gift?.name || 'Gift',
          giftImage: payload.data.gift?.image?.urlList?.[0] || '',
          combo: payload.data.repeatCount || 1
        };
      }

      // Отправляем данные только тем OBS, которые смотрят именно этого юзера
      if (broadcastData) {
        broadcastToClients(username, broadcastData);
      }
    } catch (e) {
      // Игнорируем неформатированные системные сообщения
    }
  });

  ttWs.on('close', () => {
    const conn = tikToolsConnections.get(username);
    // Если соединение оборвалось, но OBS пользователей еще открыто - пробуем восстановить
    if (conn && conn.clients.size > 0) {
      console.log(`[TikTools] Disconnected from ${username}. Reconnecting in 3s...`);
      conn.reconnectTimer = setTimeout(() => connectToTikTools(username, apiKey), 3000);
    } else {
      tikToolsConnections.delete(username);
    }
  });

  ttWs.on('error', (err) => {
    console.error(`[TikTools] Error on ${username}:`, err.message);
  });
}

function broadcastToClients(username, data) {
  const conn = tikToolsConnections.get(username);
  if (conn) {
    const msgString = JSON.stringify(data);
    conn.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(msgString);
      }
    });
  }
}

server.listen(port, '0.0.0.0', () => {
  console.log(`Server running on port ${port}`);
});
