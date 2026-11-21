// TagLive Bridge - Chat + RTMP Pusher - FIXED
// Cloady: Node + ffmpeg
// FIX: One ffmpeg per live, never killed on chunk gap, forced keyframes, heartbeat back to TagLive
// FIX: No discardcorrupt, low bitrate for slow 4G, drop on stdin backpressure so no skip / cut
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
  let bytesIn = 0;
  let lastBytesIn = 0;
  let heartbeatTimer = null;
  let intentionalStop = false;
  let lastFfmpegErr = '';

  const clearHeartbeat = () => {
    if (heartbeatTimer) { try { clearInterval(heartbeatTimer); } catch {} }
    heartbeatTimer = null;
  };

  const startHeartbeat = () => {
    clearHeartbeat();
    // FIX: Tell TagLive every 5s that ffmpeg is still alive + still receiving bytes.
    // If bytes stop growing, TagLive will honestly show Stalled DURING the live.
    heartbeatTimer = setInterval(() => {
      if (!isPushing || !ffmpeg) return;
      try {
        ws.send(JSON.stringify({
          type: 'push_alive',
          bytesIn,
          bytesDelta: bytesIn - lastBytesIn,
          ffmpeg_ok: true
        }));
        lastBytesIn = bytesIn;
      } catch {}
    }, 5000);
  };

  const stopPush = (silent) => {
    intentionalStop = true;
    clearHeartbeat();
    if (ffmpeg) {
      try { ffmpeg.stdin.end(); } catch {}
      try { ffmpeg.kill('SIGINT'); } catch {}
      ffmpeg = null;
    }
    isPushing = false;
    if (!silent) {
      try { ws.send(JSON.stringify({ type: 'push_stopped' })); } catch {}
    }
  };

  ws.on('message', (data, isBinary) => {
    // BINARY = video chunks from TagLive canvas
    if (isBinary) {
      if (ffmpeg && isPushing) {
        bytesIn += data.length;
        try {
          // FIX: If ffmpeg stdin is already backed up over 1MB, drop THIS chunk.
          // Old code let the buffer pile up, then ffmpeg burst / skipped. Dropping one small chunk is smoother.
          if (ffmpeg.stdin && ffmpeg.stdin.writableLength > 1000000) return;
          ffmpeg.stdin.write(data);
        } catch {
          // EPIPE / stdin closed - do NOT kill ffmpeg here, let ffmpeg close handler decide
        }
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
      return;
    }

    // 2. START RTMP PUSH
    if (msg.type === 'start_push') {
      const rtmpUrl = (msg.rtmpUrl || '').trim();
      const streamKey = (msg.streamKey || '').trim();
      if (!rtmpUrl || !streamKey) {
        ws.send(JSON.stringify({ type: 'error', message: 'Missing RTMP URL or Stream Key' }));
        return;
      }
      // Prevent double push - only stop if one is actually running
      if (ffmpeg) stopPush(true);
      intentionalStop = false;
      bytesIn = 0;
      lastBytesIn = 0;
      lastFfmpegErr = '';

      const dest = rtmpUrl.endsWith('/') ? rtmpUrl + streamKey : rtmpUrl + '/' + streamKey;
      console.log('Starting RTMP push to:', rtmpUrl);

      // FIX: -f webm + probesize/analyzeduration so ffmpeg does NOT guess format and stall on timestamp jump.
      // FIX: +genpts (NOT +discardcorrupt) - repair timestamps, do NOT throw away frames = no skip.
      // FIX: 500k video / 64k audio to MATCH slow 4G + 480p. 800k/1200k starved ffmpeg and caused cut.
      // FIX: -g 30 at 15fps = keyframe every 2s. Facebook needs this, else it freezes on first frame.
      // FIX: -fps_mode cfr + -async 1 so static canvas / verse change does not stall timestamps.
      ffmpeg = spawn('ffmpeg', [
        '-fflags', '+genpts+nobuffer',
        '-probesize', '32',
        '-analyzeduration', '0',
        '-f', 'webm',
        '-i', 'pipe:0',
        '-c:v', 'libx264',
        '-preset', 'veryfast',
        '-tune', 'zerolatency',
        '-pix_fmt', 'yuv420p',
        '-r', '15',
        '-fps_mode', 'cfr',
        '-g', '30',
        '-keyint_min', '30',
        '-sc_threshold', '0',
        '-b:v', '500k',
        '-maxrate', '500k',
        '-bufsize', '1000k',
        '-c:a', 'aac',
        '-b:a', '64k',
        '-ar', '44100',
        '-async', '1',
        '-f', 'flv',
        dest
      ]);

      // FIX: Swallow EPIPE, never crash bridge on stdin close
      if (ffmpeg.stdin) {
        ffmpeg.stdin.on('error', () => {});
      }

      ffmpeg.stderr.on('data', (d) => {
        // Keep only last line for debugging, do not flood logs
        try { lastFfmpegErr = d.toString().slice(-300); } catch {}
      });
      ffmpeg.on('close', (code) => {
        console.log('ffmpeg closed', code, 'last:', lastFfmpegErr.slice(-200));
        clearHeartbeat();
        isPushing = false;
        ffmpeg = null;
        // FIX: Only tell TagLive push_stopped if it was NOT intentional.
        // Intentional stop already sends push_stopped via stopPush(). Double send confused Part 1A.
        if (!intentionalStop) {
          try { ws.send(JSON.stringify({ type: 'push_stopped', code, ffmpeg_log: lastFfmpegErr })); } catch {}
        }
      });
      ffmpeg.on('error', (e) => {
        console.log('ffmpeg error', e.message);
        clearHeartbeat();
        isPushing = false;
        if (!intentionalStop) {
          try { ws.send(JSON.stringify({ type: 'error', message: 'ffmpeg failed: ' + e.message })); } catch {}
        }
      });

      isPushing = true;
      ws.send(JSON.stringify({ type: 'push_started', dest: rtmpUrl }));
      startHeartbeat();
      return;
    }

    // 3. STOP PUSH - only place that kills ffmpeg
    if (msg.type === 'stop_push') {
      stopPush(false);
      return;
    }
  });

  ws.on('close', () => {
    clearHeartbeat();
    // FIX: WS close alone does NOT kill ffmpeg immediately - give ffmpeg 2s to flush to Facebook.
    // Old code killed ffmpeg instantly on WS close, which finalized Facebook VOD early.
    setTimeout(() => {
      if (ffmpeg) stopPush(true);
    }, 2000);
    console.log('Client disconnected');
  });
  ws.on('error', () => { clearHeartbeat(); });
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log('TagLive Bridge listening on ' + PORT));
