import express from 'express';
import { WebSocketServer } from 'ws';
import { createServer } from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import { TikTokLive } from '@tiktool/live'; // Подключаем официальный SDK

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = createServer(app);
const port = process.env.PORT || 8080;

// Раздаем статический index.html (фронтенд)
app.use(express.static(__dirname));

// WebSocket сервер для связи с OBS браузерами
const wss = new WebSocketServer({ server });

// Пул активных стримов
// Ключ: username, Значение: { liveClient: TikTokLive, clients: Set, apiKey: string }
const activeStreams = new Map();

// Механизм Ping-Pong для удержания соединения на Railway/Cloudflare
const pingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => {
  clearInterval(pingInterval);
});

wss.on('connection', (clientWs, req) => {
  clientWs.isAlive = true;
  clientWs.on('pong', () => { clientWs.isAlive = true; });

  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const apiKey = url.searchParams.get('key');
    const username = url.searchParams.get('u');

    if (!username || !apiKey) {
      clientWs.send(JSON.stringify({ type: 'error', message: 'Missing key or username in URL' }));
      clientWs.close(4000, 'Missing Credentials');
      return;
    }

    let stream = activeStreams.get(username);

    // Если этот тиктокер еще не отслеживается сервером - запускаем SDK
    if (!stream) {
      console.log(`[TikTokLive] Initializing SDK for: ${username}`);
      
      const liveClient = new TikTokLive({
        uniqueId: username,
        apiKey: apiKey,
        mode: 'relayed' // Используем Relay режим (через сервера TikTools)
      });

      stream = {
        liveClient: liveClient,
        clients: new Set(),
        apiKey: apiKey
      };
      
      activeStreams.set(username, stream);

      // --- ОБРАБОТЧИКИ СОБЫТИЙ SDK ---
      
      liveClient.on('connected', () => {
        console.log(`[TikTokLive] Connected to stream: ${username}`);
      });

      // SDK автоматически пытается переподключиться при обрыве
      liveClient.on('disconnected', (reason) => {
        console.log(`[TikTokLive] Disconnected from ${username}: ${reason}. SDK will auto-reconnect.`);
      });

      liveClient.on('error', (err) => {
        console.error(`[TikTokLive Error] ${username}:`, err.message);
        broadcast(username, { type: 'error', message: err.message || 'TikTok API Error' });
      });

      // Перехватываем лайки и отправляем на фронтенд
      liveClient.on('like', (event) => {
        broadcast(username, {
          type: 'like',
          nickname: event.user?.nickname || event.user?.uniqueId || 'User',
          avatar: event.user?.profilePictureUrl || 'https://p16-sign-va.tiktokcdn.com/tos-maliva-avt-0068/7065997232230301701~c5_100x100.jpeg',
          amount: event.likeCount || 1
        });
      });

      // Перехватываем подарки и отправляем на фронтенд
      liveClient.on('gift', (event) => {
        broadcast(username, {
          type: 'gift',
          nickname: event.user?.nickname || event.user?.uniqueId || 'User',
          avatar: event.user?.profilePictureUrl || 'https://p16-sign-va.tiktokcdn.com/tos-maliva-avt-0068/7065997232230301701~c5_100x100.jpeg',
          giftName: event.giftName || 'Gift',
          giftImage: event.giftPictureUrl || 'https://cdn-icons-png.flaticon.com/512/3503/3503816.png',
          combo: event.repeatCount || 1
        });
      });

      // Запускаем подключение SDK
      liveClient.connect().catch(err => {
        console.error(`[TikTokLive Connect Error] ${username}:`, err.message);
        broadcast(username, { type: 'error', message: `Failed to connect: ${err.message}` });
      });
    }

    // Добавляем OBS зрителя в рассылку
    stream.clients.add(clientWs);
    console.log(`[Client Connected] OBS joined room: ${username}. Total OBS viewers: ${stream.clients.size}`);

    // Отключение зрителя (закрытие OBS)
    clientWs.on('close', () => {
      if (activeStreams.has(username)) {
        const currentStream = activeStreams.get(username);
        currentStream.clients.delete(clientWs);
        console.log(`[Client Disconnected] OBS left room: ${username}. Total OBS viewers: ${currentStream.clients.size}`);

        // ОПТИМИЗАЦИЯ: Если этот виджет больше никто не открыл - выключаем SDK
        if (currentStream.clients.size === 0) {
          console.log(`[Cleanup] Room ${username} is empty. Disconnecting SDK.`);
          currentStream.liveClient.disconnect();
          activeStreams.delete(username);
        }
      }
    });

  } catch (e) {
    console.error('Connection error:', e);
  }
});

// Массовая рассылка готового JSON всем OBS клиентам одной комнаты
function broadcast(username, data) {
  const stream = activeStreams.get(username);
  if (stream && stream.clients) {
    const payload = JSON.stringify(data);
    stream.clients.forEach(client => {
      // 1 означает WebSocket.OPEN
      if (client.readyState === 1) { 
        client.send(payload);
      }
    });
  }
}

server.listen(port, '0.0.0.0', () => {
  console.log(`🚀 TikTools Server (SDK version) running on port ${port}`);
});
