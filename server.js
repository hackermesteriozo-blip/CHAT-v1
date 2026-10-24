
"use strict";

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { randomUUID } = require("crypto");
const os = require("os");

// ======================================================
// ANON CHAT PRO | CONFIGURAÇÃO
// ======================================================

const CONFIG = Object.freeze({
  name: "ANON CHAT PRO",
  version: "2.0.0",
  port: Number(process.env.PORT) || 3000,

  maxConnections: 200,
  maxPayload: 2048,
  maxMessageLength: 500,
  minNameLength: 2,
  maxNameLength: 20,

  messageInterval: 1000,
  rateWindow: 10_000,
  maxMessagesPerWindow: 8,

  heartbeatInterval: 30_000,
  shutdownTimeout: 10_000
});

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
  server,
  maxPayload: CONFIG.maxPayload,
  perMessageDeflate: false,
  clientTracking: true
});

const clients = new Map();
const messageHistory = new Map();

let shuttingDown = false;
const startedAt = Date.now();

// ======================================================
// TERMINAL PROFISSIONAL
// ======================================================

const COLORS = {
  reset: "\x1b[0m",
  cyan: "\x1b[36m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  red: "\x1b[31m",
  gray: "\x1b[90m",
  purple: "\x1b[35m"
};

function log(level, message) {
  const styles = {
    INFO: COLORS.cyan,
    OK: COLORS.green,
    WARN: COLORS.yellow,
    ERROR: COLORS.red,
    CHAT: COLORS.purple
  };

  const color = styles[level] || COLORS.gray;
  const timestamp = new Date().toLocaleString("pt-BR", {
    timeZone: "UTC",
    hour12: false
  });

  console.log(
    `${COLORS.gray}[${timestamp} UTC]${COLORS.reset} ` +
    `${color}[${level}]${COLORS.reset} ${message}`
  );
}

function printBanner() {
  console.log(`
${COLORS.cyan}
  ╔══════════════════════════════════════════╗
  ║             ANON CHAT PRO                ║
  ║          PUBLIC REALTIME CHAT            ║
  ╠══════════════════════════════════════════╣
  ║  Version: ${CONFIG.version.padEnd(30)}║
  ║  Runtime: Node.js                        ║
  ║  Transport: WebSocket                    ║
  ║  Environment: ${(
    process.env.NODE_ENV || "production"
  ).padEnd(24)}║
  ╚══════════════════════════════════════════╝
${COLORS.reset}`);
}

// ======================================================
// HTTP | ROTAS PÚBLICAS
// ======================================================

app.disable("x-powered-by");
app.disable("etag");

app.get("/health", (req, res) => {
  const online = [...clients.values()]
    .filter(user => user.joined).length;

  res.set("Cache-Control", "no-store");

  res.status(shuttingDown ? 503 : 200).json({
    status: shuttingDown ? "shutting_down" : "online",
    service: CONFIG.name,
    version: CONFIG.version,
    uptimeSeconds: Math.floor(
      (Date.now() - startedAt) / 1000
    ),
    connectedUsers: online,
    maxConnections: CONFIG.maxConnections,
    timestamp: new Date().toISOString()
  });
});

app.get("/", (req, res) => {
  res.set("Cache-Control", "no-cache");
  res.sendFile(require("path").join(__dirname, "index.html"));
});

// Não disponibiliza arquivos internos do servidor.
app.use((req, res) => {
  res.status(404).json({
    error: "Página não encontrada."
  });
});

// ======================================================
// UTILITÁRIOS
// ======================================================

function send(ws, data) {
  if (ws.readyState !== WebSocket.OPEN) return;

  try {
    ws.send(JSON.stringify(data), error => {
      if (error) {
        log("WARN", "Falha ao enviar dados ao cliente.");
      }
    });
  } catch {
    log("WARN", "Erro ao preparar resposta WebSocket.");
  }
}

function broadcast(data, exclude = null) {
  const payload = JSON.stringify(data);

  for (const client of wss.clients) {
    if (
      client !== exclude &&
      client.readyState === WebSocket.OPEN
    ) {
      try {
        client.send(payload, error => {
          if (error) {
            log("WARN", "Falha ao enviar broadcast.");
          }
        });
      } catch {
        log("WARN", "Falha no broadcast.");
      }
    }
  }
}

function getOnlineUsers() {
  return [...clients.values()]
    .filter(user => user.joined)
    .map(user => ({
      id: user.id,
      name: user.name
    }));
}

function broadcastUsers() {
  broadcast({
    type: "users",
    users: getOnlineUsers()
  });
}

function systemMessage(text) {
  broadcast({
    type: "system",
    text,
    time: Date.now()
  });
}

function normalizeName(value) {
  if (typeof value !== "string") return "";

  return value
    .normalize("NFC")
    .trim()
    .replace(/\s+/g, " ");
}

function validName(name) {
  if (
    name.length < CONFIG.minNameLength ||
    name.length > CONFIG.maxNameLength
  ) {
    return false;
  }

  return /^[\p{L}\p{N}_ -]+$/u.test(name);
}

function nameExists(name, currentSocket) {
  const normalized = name.toLocaleLowerCase("pt-BR");

  for (const [socket, user] of clients) {
    if (
      socket !== currentSocket &&
      user.joined &&
      user.name.toLocaleLowerCase("pt-BR") === normalized
    ) {
      return true;
    }
  }

  return false;
}

function sendError(ws, text) {
  send(ws, {
    type: "error",
    text
  });
}

// ======================================================
// CONTROLE DE MENSAGENS
// ======================================================

function allowMessage(ws) {
  const now = Date.now();
  const state = messageHistory.get(ws);

  if (!state) {
    messageHistory.set(ws, {
      lastMessage: now,
      timestamps: [now]
    });

    return true;
  }

  // Intervalo mínimo entre mensagens.
  if (now - state.lastMessage < CONFIG.messageInterval) {
    return false;
  }

  // Janela móvel para controlar spam.
  state.timestamps = state.timestamps.filter(
    timestamp => now - timestamp < CONFIG.rateWindow
  );

  if (
    state.timestamps.length >= CONFIG.maxMessagesPerWindow
  ) {
    return false;
  }

  state.lastMessage = now;
  state.timestamps.push(now);

  return true;
}

// ======================================================
// CONEXÕES WEBSOCKET
// ======================================================

wss.on("connection", (ws, request) => {
  if (shuttingDown) {
    ws.close(1012, "Servidor reiniciando");
    return;
  }

  if (wss.clients.size > CONFIG.maxConnections) {
    sendError(ws, "O chat está cheio. Tente novamente mais tarde.");
    ws.close(1013, "Limite de conexões");
    return;
  }

  const id = randomUUID();

  const user = {
    id,
    name: null,
    joined: false,
    alive: true,
    connectedAt: Date.now()
  };

  clients.set(ws, user);
  messageHistory.set(ws, {
    lastMessage: 0,
    timestamps: []
  });

  ws.on("pong", () => {
    user.alive = true;
  });

  ws.on("message", (buffer, isBinary) => {
    if (isBinary || buffer.length > CONFIG.maxPayload) {
      sendError(ws, "Formato ou tamanho de mensagem inválido.");
      return;
    }

    let data;

    try {
      data = JSON.parse(buffer.toString("utf8"));
    } catch {
      sendError(ws, "Mensagem inválida.");
      return;
    }

    if (
      !data ||
      typeof data !== "object" ||
      Array.isArray(data) ||
      typeof data.type !== "string"
    ) {
      sendError(ws, "Formato de mensagem inválido.");
      return;
    }

    // ------------------------------
    // ENTRAR NO CHAT
    // ------------------------------

    if (data.type === "join") {
      if (user.joined) {
        sendError(ws, "Você já entrou no chat.");
        return;
      }

      const name = normalizeName(data.name);

      if (!validName(name)) {
        sendError(
          ws,
          "O apelido deve ter de 2 a 20 caracteres e usar apenas letras, números, espaços, hífen ou underline."
        );
        return;
      }

      if (nameExists(name, ws)) {
        sendError(ws, "Esse apelido já está em uso.");
        return;
      }

      user.name = name;
      user.joined = true;

      send(ws, {
        type: "joined",
        id: user.id,
        name: user.name
      });

      broadcastUsers();

      systemMessage(`${user.name} entrou no chat.`);

      log("CHAT", `${user.name} entrou na conversa.`);
      return;
    }

    // Impede mensagens de quem não entrou.
    if (!user.joined) {
      sendError(ws, "Entre no chat antes de enviar mensagens.");
      return;
    }

    // ------------------------------
    // ENVIAR MENSAGEM
    // ------------------------------

    if (data.type === "message") {
      if (typeof data.text !== "string") {
        sendError(ws, "O conteúdo da mensagem é inválido.");
        return;
      }

      const text = data.text.trim();

      if (
        !text ||
        text.length > CONFIG.maxMessageLength
      ) {
        sendError(
          ws,
          "A mensagem deve ter entre 1 e 500 caracteres."
        );
        return;
      }

      if (!allowMessage(ws)) {
        sendError(
          ws,
          "Você está enviando mensagens muito rápido. Aguarde um pouco."
        );
        return;
      }

      broadcast({
        type: "message",
        id: user.id,
        name: user.name,
        text,
        time: Date.now()
      });

      return;
    }

    sendError(ws, "Comando desconhecido.");
  });

  ws.on("close", () => {
    const disconnectedUser = clients.get(ws);

    clients.delete(ws);
    messageHistory.delete(ws);

    if (disconnectedUser?.joined) {
      broadcastUsers();

      systemMessage(
        `${disconnectedUser.name} saiu do chat.`
      );

      log("INFO", `${disconnectedUser.name} desconectou.`);
    }
  });

  ws.on("error", error => {
    log(
      "WARN",
      `Erro WebSocket: ${error.message}`
    );
  });
});

// ======================================================
// HEARTBEAT | DETECTAR CONEXÕES INATIVAS
// ======================================================

const heartbeat = setInterval(() => {
  for (const [ws, user] of clients) {
    if (ws.readyState !== WebSocket.OPEN) {
      clients.delete(ws);
      messageHistory.delete(ws);
      continue;
    }

    if (!user.alive) {
      log("WARN", "Conexão inativa encerrada.");
      ws.terminate();
      continue;
    }

    user.alive = false;

    try {
      ws.ping();
    } catch {
      ws.terminate();
    }
  }
}, CONFIG.heartbeatInterval);

heartbeat.unref();

// ======================================================
// ERROS DO SERVIDOR
// ======================================================

server.on("error", error => {
  log("ERROR", `Erro HTTP: ${error.message}`);
});

wss.on("error", error => {
  log("ERROR", `Erro WebSocket Server: ${error.message}`);
});

// ======================================================
// INICIALIZAÇÃO
// ======================================================

server.listen(CONFIG.port, "0.0.0.0", () => {
  printBanner();

  log("OK", `Servidor iniciado na porta ${CONFIG.port}`);
  log("OK", "HTTP e WebSocket prontos.");
  log("OK", `Limite: ${CONFIG.maxConnections} conexões.`);
  log("OK", "Rota de monitoramento: /health");
});

// ======================================================
// ENCERRAMENTO SEGURO
// ======================================================

let closing = false;

function shutdown(signal) {
  if (closing) return;
  closing = true;
  shuttingDown = true;

  log("WARN", `Recebido ${signal}. Encerrando servidor...`);

  clearInterval(heartbeat);

  const forceExit = setTimeout(() => {
    log("WARN", "Tempo limite de encerramento atingido.");

    for (const ws of wss.clients) {
      ws.terminate();
    }

    process.exit(1);
  }, CONFIG.shutdownTimeout);

  forceExit.unref();

  for (const ws of wss.clients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.close(1001, "Servidor encerrando");
    }
  }

  wss.close(() => {
    server.close(() => {
      clearTimeout(forceExit);
      log("OK", "Servidor encerrado com segurança.");
      process.exit(0);
    });
  });
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
