// TagLive Bridge - Chat + RTMP Pusher
// Cloady: Node + ffmpeg
const http = require('http');
const WebSocket = require('ws');
const { spawn } = require('child_process');

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', service: 'TagLive Bridge', rtmp: 'ready' }));
    return;
  }
  res.writeHead(426, { 'Content-Type': 'text/plain' });
  res.end('Upgrade Required - Use WebSocket');
});

const wss = new WebSocket.Server({ server });

// One push per connection
wss.on('connection', (ws) => {
  console.log('Client connected');
  let ffmpeg = null;
  let isPushing = false;
  let tiktokUsername = '';

  const stopPush = () => {
    if (ffmpeg) {
      try { ffmpeg.stdin.end(); } catch {}
      try { ffmpeg.kill('SIGINT'); } catch {}
      ffmpeg = null;
    }
    isPushing = false;
  };

  ws.on('message', (data, isBinary) => {
    // BINARY = video chunks from TagLive canvas
    if (isBinary) {
      if (ffmpeg && isPushing) {
        try { ffmpeg.stdin.write(data); } catch {}
      }
      return;
    }

    // JSON = control messages
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    // 1. TikTok Chat Join (old function - still works)
    if (msg.type === 'join') {
      tiktokUsername = msg.username || '';
      console.log('Chat join:', tiktokUsername);
      ws.send(JSON.stringify({ type: 'connected', username: tiktokUsername }));
      // NOTE: Chat via tiktok-live-connector goes here if you add it.
      // For now, push mode is the main TikTok Live mode.
      return;
    }

    // 2. START RTMP PUSH - This is the new one
    if (msg.type === 'start_push') {
      const rtmpUrl = (msg.rtmpUrl || '').trim();
      const streamKey = (msg.streamKey || '').trim();
      if (!rtmpUrl || !streamKey) {
        ws.send(JSON.stringify({ type: 'error', message: 'Missing RTMP URL or Stream Key' }));
        return;
      }
      // Prevent double push
      stopPush();

      // Build full RTMP destination: rtmp://.../live/ + key
      // TikTok gives Server URL like rtmp://... and Key separate
      const dest = rtmpUrl.endsWith('/') ? rtmpUrl + streamKey : rtmpUrl + '/' + streamKey;
      console.log('Starting RTMP push to:', rtmpUrl);

      // ffmpeg takes webm from browser -> converts to RTMP for TikTok
      ffmpeg = spawn('ffmpeg', [
        '-i', 'pipe:0',
        '-c:v', 'libx264',
        '-preset', 'veryfast',
        '-tune', 'zerolatency',
        '-pix_fmt', 'yuv420p',
        '-r', '30',
        '-g', '60',
        '-b:v', '2500k',
        '-maxrate', '2500k',
        '-bufsize', '5000k',
        '-c:a', 'aac',
        '-b:a', '128k',
        '-ar', '44100',
        '-f', 'flv',
        dest
      ]);

      ffmpeg.stderr.on('data', (d) => {
        // console.log('ffmpeg:', d.toString().slice(0,200));
      });
      ffmpeg.on('close', (code) => {
        console.log('ffmpeg closed', code);
        isPushing = false;
        try { ws.send(JSON.stringify({ type: 'push_stopped', code })); } catch {}
      });
      ffmpeg.on('error', (e) => {
        console.log('ffmpeg error', e.message);
        try { ws.send(JSON.stringify({ type: 'error', message: 'ffmpeg failed: ' + e.message })); } catch {}
      });

      isPushing = true;
      ws.send(JSON.stringify({ type: 'push_started', dest: rtmpUrl }));
      return;
    }

    // 3. STOP PUSH
    if (msg.type === 'stop_push') {
      stopPush();
      ws.send(JSON.stringify({ type: 'push_stopped' }));
      return;
    }
  });

  ws.on('close', () => {
    stopPush();
    console.log('Client disconnected');
  });
  ws.on('error', () => { stopPush(); });
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log('TagLive Bridge listening on ' + PORT));
