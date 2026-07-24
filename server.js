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

// Раздаем статический index.html (фронтенд)
app.use(express.static(__dirname));

// WebSocket сервер для приема подключений от пользователей (из OBS/браузеров)
const wss = new WebSocketServer({ server });

// Пул активных стримов
// Ключ: username, Значение: { ws: WebSocket, clients: Set, reconnectTimer: Timeout, apiKey: string }
const activeStreams = new Map();

wss.on('connection', (clientWs, req) => {
  try {
    // 1. Читаем личный ключ и юзернейм из ссылки, которую ввел пользователь
    const url = new URL(req.url, `http://${req.headers.host}`);
    const apiKey = url.searchParams.get('key');
    const username = url.searchParams.get('u');

    // Если параметры не переданы - отключаем клиента
    if (!username || !apiKey) {
      clientWs.send(JSON.stringify({ type: 'error', message: 'Missing key or username in URL' }));
      clientWs.close(4000, 'Missing Credentials');
      return;
    }

    // 2. Ищем или создаем комнату для этого тиктокера
    let stream = activeStreams.get(username);

    if (!stream) {
      stream = {
        ws: null,
        clients: new Set(),
        reconnectTimer: null,
        apiKey: apiKey // Сохраняем ключ для подключения к TikTools
      };
      activeStreams.set(username, stream);
      
      // Запускаем единственное подключение к TikTools для этой комнаты
      connectToTikTools(username);
    }

    // 3. Добавляем зрителя в рассылку
    stream.clients.add(clientWs);
    console.log(`[Client Connected] OBS joined room: ${username}. Total viewers: ${stream.clients.size}`);

    // 4. Очистка при отключении клиента (закрытии OBS)
    clientWs.on('close', () => {
      if (activeStreams.has(username)) {
        const currentStream = activeStreams.get(username);
        currentStream.clients.delete(clientWs);
        console.log(`[Client Disconnected] OBS left room: ${username}. Total viewers: ${currentStream.clients.size}`);

        // ОПТИМИЗАЦИЯ: Если стримера больше никто не смотрит, разрываем соединение с TikTools
        if (currentStream.clients.size === 0) {
          console.log(`[Cleanup] Room ${username} is empty. Closing TikTools connection.`);
          if (currentStream.ws) {
            currentStream.ws.close(1000, 'Room empty');
          }
          if (currentStream.reconnectTimer) {
            clearTimeout(currentStream.reconnectTimer);
          }
          activeStreams.delete(username);
        }
      }
    });

  } catch (e) {
    console.error('Connection error:', e);
  }
});

function connectToTikTools(username) {
  const stream = activeStreams.get(username);
  if (!stream) return;

  // Если уже есть старое соединение (при реконнекте), принудительно закрываем
  if (stream.ws) {
    try { stream.ws.close(); } catch(e) {}
  }

  console.log(`[TikTools] Connecting to stream: ${username}`);
  
  // Формируем URL для TikTools API
  const wsUrl = `wss://api.tik.tools?uniqueId=${username}&apiKey=${stream.apiKey}`;
  const ttWs = new WebSocket(wsUrl);
  stream.ws = ttWs;

  ttWs.on('open', () => {
    console.log(`[TikTools] Connected to ${username}`);
    broadcast(username, { type: 'system', message: 'Connected to TikTok via TikTools' });
  });

  ttWs.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      
      // Обработка системных ошибок от TikTools (например, неверный ключ)
      if (msg.type === 'error' || msg.error) {
         broadcast(username, { type: 'error', message: msg.message || 'Invalid API Key or TikTools error' });
         return;
      }

      let broadcastData = null;

      // Нормализуем данные Лайков
      if (msg.event === 'like' && msg.data) {
        broadcastData = {
          type: 'like',
          nickname: msg.data.user?.nickname || msg.data.user?.uniqueId || 'User',
          avatar: msg.data.user?.profilePictureUrl || 'https://p16-sign-va.tiktokcdn.com/tos-maliva-avt-0068/7065997232230301701~c5_100x100.jpeg',
          amount: msg.data.likeCount || 1
        };
      } 
      // Нормализуем данные Подарков
      else if (msg.event === 'gift' && msg.data) {
        broadcastData = {
          type: 'gift',
          nickname: msg.data.user?.nickname || msg.data.user?.uniqueId || 'User',
          avatar: msg.data.user?.profilePictureUrl || 'https://p16-sign-va.tiktokcdn.com/tos-maliva-avt-0068/7065997232230301701~c5_100x100.jpeg',
          giftName: msg.data.giftName || 'Gift',
          // В TikTools URL картинки подарка не всегда отправляется, ставим красивую затычку на всякий случай
          giftImage: msg.data.giftPictureUrl || 'https://cdn-icons-png.flaticon.com/512/3503/3503816.png',
          combo: msg.data.repeatCount || 1
        };
      }

      // Если есть полезные данные - рассылаем всем OBS клиентам в этой комнате
      if (broadcastData) {
        broadcast(username, broadcastData);
      }
    } catch (e) {
      // Игнорируем ошибки парсинга системных сообщений
    }
  });

  ttWs.on('close', (code, reason) => {
    console.log(`[TikTools] Disconnected from ${username} (Code: ${code})`);
    
    // Если соединение оборвалось, но OBS пользователей еще открыто - пробуем восстановить
    const currentStream = activeStreams.get(username);
    if (currentStream && currentStream.clients.size > 0 && code !== 1000) {
        console.log(`[TikTools] Reconnecting ${username} in 5 seconds...`);
        currentStream.reconnectTimer = setTimeout(() => {
            connectToTikTools(username);
        }, 5000);
    } else if (currentStream && currentStream.clients.size === 0) {
        activeStreams.delete(username);
    }
  });

  ttWs.on('error', (err) => {
    console.error(`[TikTools] Error on ${username}:`, err.message);
  });
}

// Функция массовой рассылки для конкретной комнаты
function broadcast(username, data) {
  const stream = activeStreams.get(username);
  if (stream && stream.clients) {
    const payload = JSON.stringify(data);
    stream.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
      }
    });
  }
}

server.listen(port, '0.0.0.0', () => {
  console.log(`🚀 TikTools Proxy Server running on port ${port}`);
});
