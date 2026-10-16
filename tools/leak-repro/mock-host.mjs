// Mock Elgato Stream Deck host + mock TikOps streamdeck-service.
// Usage: node mock-host.mjs <pluginDir> <durationSec> [keyPressEverySec]
//   <pluginDir> is an unpacked com.tikops.streamdeck.sdPlugin folder (unzip the .streamDeckPlugin).
// Env:
//   PLUGIN_NODE=<path>   Node binary used to run the plugin (manifest asks for Node 24).
//   REPLY_DELAY_MS=<n>   Delay before answering getGlobalSettings; with a delay, getGlobalSettings/s
//                        divided by (1000 / n) is the number of concurrent request loops.
//   NO_ID=1              Omit message ids from didReceiveGlobalSettings replies.
//   PUSH_GLOBALS=1       Push a rotated access token at t=3s, as the app does after re-pairing.
// A healthy build sends ~0 getGlobalSettings/s while idle.
import { WebSocketServer } from "ws";
import http from "node:http";
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const pluginDir = path.resolve(process.argv[2]);
const durationSec = Number(process.argv[3] ?? 30);
const keyEvery = Number(process.argv[4] ?? 0);
const replyDelay = Number(process.env.REPLY_DELAY_MS ?? 0);
const nodeBin = process.env.PLUGIN_NODE ?? process.execPath;

// ---------- mock TikOps service ----------
let ttsEnabled = true;
let soundsEnabled = true;
const svc = http.createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    res.setHeader("Content-Type", "application/json");
    if (req.url.startsWith("/api/v1/streamdeck/resources")) {
      res.end(JSON.stringify({
        actions: [], wheels: [], timers: [], goals: [], sounds: [], tts_voices: [],
        operations: { tts: [{ id: "toggle", label: "Toggle" }], sound: [{ id: "toggle", label: "Toggle" }] },
        tts_state: { toggle: ttsEnabled }, sounds_state: { toggle: soundsEnabled },
        tiktok_live_state: { connected: false }, spotify_state: { toggle_enabled: true },
      }));
    } else if (req.url.startsWith("/api/v1/streamdeck/invocations")) {
      const b = JSON.parse(body || "{}");
      if (b.resource_type === "tts") { ttsEnabled = !ttsEnabled; broadcastState("tts", { enabled: ttsEnabled }); }
      if (b.resource_type === "sound") { soundsEnabled = !soundsEnabled; broadcastState("sounds", { enabled: soundsEnabled }); }
      res.end(JSON.stringify({ ok: true }));
    } else {
      res.statusCode = 404; res.end("{}");
    }
  });
});
const liveWss = new WebSocketServer({ noServer: true });
svc.on("upgrade", (req, sock, head) => liveWss.handleUpgrade(req, sock, head, (ws) => liveWss.emit("connection", ws)));
let liveConns = 0;
liveWss.on("connection", (ws, req) => {
  liveConns++;
  ws.send(JSON.stringify({ type: "snapshot", state: { tts: { enabled: ttsEnabled }, sounds: { enabled: soundsEnabled }, tiktok_live: { connected: false }, spotify: { toggle_enabled: true } } }));
});
function broadcastState(topic, payload) {
  for (const c of liveWss.clients) c.send(JSON.stringify({ type: "state", topic, payload }));
}
setInterval(() => { for (const c of liveWss.clients) c.send(JSON.stringify({ type: "ping" })); }, 5000);
await new Promise((r) => svc.listen(0, "127.0.0.1", r));
const svcUrl = `http://127.0.0.1:${svc.address().port}`;

// ---------- mock Stream Deck host ----------
const pluginUUID = "com.tikops.streamdeck";
const globals = { accessToken: "test-token", installationId: "inst-1", serviceUrl: svcUrl };
const keys = [
  { action: "com.tikops.streamdeck.tts", context: "ctx-tts", settings: { operation: "toggle" } },
  { action: "com.tikops.streamdeck.sound", context: "ctx-sound", settings: { operation: "toggle" } },
];
const counts = {};
let total = 0;
const sdWss = new WebSocketServer({ port: 0, host: "127.0.0.1" });
await new Promise((r) => sdWss.on("listening", r));
let pluginSock = null;
sdWss.on("connection", (ws) => {
  pluginSock = ws;
  ws.on("message", (raw) => {
    const msg = JSON.parse(raw.toString());
    total++;
    counts[msg.event] = (counts[msg.event] ?? 0) + 1;
    switch (msg.event) {
      case "registerPlugin":
        ws.send(JSON.stringify({ event: "deviceDidConnect", device: "dev1", deviceInfo: { name: "SD", type: 0, size: { columns: 5, rows: 3 } } }));
        for (const k of keys) {
          ws.send(JSON.stringify({ event: "willAppear", action: k.action, context: k.context, device: "dev1",
            payload: { settings: k.settings, coordinates: { column: 0, row: 0 }, controller: "Keypad", isInMultiAction: false, state: 0 } }));
        }
        break;
      case "getGlobalSettings":
        setTimeout(() => ws.send(JSON.stringify({ event: "didReceiveGlobalSettings", ...(process.env.NO_ID ? {} : { id: msg.id }), payload: { settings: globals } })), replyDelay);
        break;
      case "setGlobalSettings":
        Object.assign(globals, msg.payload);
        break;
      case "getSettings": {
        const k = keys.find((x) => x.context === msg.context);
        ws.send(JSON.stringify({ event: "didReceiveSettings", id: msg.id, action: k?.action, context: msg.context, device: "dev1",
          payload: { settings: k?.settings ?? {}, coordinates: { column: 0, row: 0 }, controller: "Keypad", isInMultiAction: false } }));
        break;
      }
    }
  });
});

const info = {
  application: { font: "", language: "en", platform: "windows", platformVersion: "10", version: "7.1.0.0" },
  plugin: { uuid: pluginUUID, version: "0.2.1.0" },
  devicePixelRatio: 1, colors: {},
  devices: [{ id: "dev1", name: "SD", type: 0, size: { columns: 5, rows: 3 } }],
};
const child = spawn(nodeBin, [path.join(pluginDir, "bin/plugin.js"),
  "-port", String(sdWss.address().port), "-pluginUUID", pluginUUID, "-registerEvent", "registerPlugin", "-info", JSON.stringify(info)],
  { cwd: pluginDir, stdio: ["ignore", "ignore", "inherit"] });

function rssMb(pid) {
  try {
    const s = fs.readFileSync(`/proc/${pid}/status`, "utf8");
    return Math.round(Number(/VmRSS:\s+(\d+)/.exec(s)[1]) / 1024);
  } catch { return -1; }
}

if (process.env.PUSH_GLOBALS) {
  setTimeout(() => {
    globals.accessToken = "rotated-token";
    pluginSock?.send(JSON.stringify({ event: "didReceiveGlobalSettings", payload: { settings: globals } }));
  }, 3000);
}
if (keyEvery > 0) {
  setInterval(() => {
    if (!pluginSock) return;
    for (const k of keys) {
      for (const ev of ["keyDown", "keyUp"]) {
        pluginSock.send(JSON.stringify({ event: ev, action: k.action, context: k.context, device: "dev1",
          payload: { settings: k.settings, coordinates: { column: 0, row: 0 }, isInMultiAction: false, state: 0 } }));
      }
    }
  }, keyEvery * 1000);
}

let last = 0;
let lastGgs = 0;
let sec = 0;
console.log("sec\tmsgs/s\tgetGlobalSettings/s\tpluginRSS_MB");
const t = setInterval(() => {
  sec++;
  const ggs = counts.getGlobalSettings ?? 0;
  console.log(`${sec}\t${total - last}\t${ggs - lastGgs}\t${rssMb(child.pid)}`);
  last = total; lastGgs = ggs;
  if (sec >= durationSec) {
    clearInterval(t);
    console.log("totals:", JSON.stringify(counts), "liveStateConnections:", liveConns);
    child.kill("SIGKILL");
    process.exit(0);
  }
}, 1000);
