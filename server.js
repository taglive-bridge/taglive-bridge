// TagLive Bridge - Browser -> WebSocket -> FFmpeg -> Facebook RTMP
const WebSocket = require('ws')
const { spawn } = require('child_process')

const PORT = process.env.PORT || 10000
const wss = new WebSocket.Server({ port: PORT })

console.log('TagLive Bridge running on', PORT)

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost')
  const rtmp = url.searchParams.get('rtmp')
  if (!rtmp) { ws.close(); return }

  console.log('New live:', rtmp.slice(0, 70))

  // FFmpeg: take webm from browser, convert to RTMP for Facebook
  const ffmpeg = spawn('ffmpeg', [
    '-i', 'pipe:0',
    '-c:v', 'libx264', '-preset', 'veryfast', '-tune', 'zerolatency',
    '-pix_fmt', 'yuv420p',
    '-c:a', 'aac', '-ar', '44100', '-b:a', '128k',
    '-f', 'flv', rtmp
  ])

  ffmpeg.stderr.on('data', d => console.log(d.toString().slice(0, 200)))

  ws.on('message', (data) => {
    try { if (ffmpeg.stdin.writable) ffmpeg.stdin.write(data) } catch {}
  })

  const cleanup = () => { try { ffmpeg.kill('SIGINT') } catch {} }
  ws.on('close', cleanup)
  ws.on('error', cleanup)
})
