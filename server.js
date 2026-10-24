
"use strict";

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");
const path = require("path");

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
  server,
  maxPayload: 4096,
  perMessageDeflate: false,
  clientTracking: true
});

// ==============================
// CONFIGURAÇÕES
// ==============================

const PORT = Number(process.env.PORT) || 3000;
const MAX_USERS = 200;
const MAX_TEXT = 500;
const MAX_NAME = 20;
const MESSAGE_INTERVAL = 800;
const HEARTBEAT_INTERVAL = 30000;

const FOUNDER_NAME = normalizeName(
  process.env.FOUNDER_NAME || "fundador"
);

const FOUNDER_PASSWORD = process.env.FOUNDER_PASSWORD || "";

const clients = new Map();
const lastMessages = new Map();
const lastJoinAttempts = new WeakMap();

app.disable("x-powered-by");

app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("X-Frame-Options", "DENY");
  next();
});

// O index.html deve ficar dentro da pasta public.
app.use(express.static(path.join(__dirname, "public")));

app.get("/health", (req, res) => {
  res.status(200).json({
    status: "online",
    service: "Anon Chat Pro",
    users: clients.size,
    maxUsers: MAX_USERS,
    uptime: Math.floor(process.uptime())
  });
});

app.get("/favicon.ico", (req, res) => {
  res.status(204).end();
});

// ==============================
// FUNÇÕES AUXILIARES
// ==============================

function normalizeName(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLocaleLowerCase("pt-BR");
}

function send(ws, data) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;

  try {
    ws.send(JSON.stringify(data), (error) => {
      if (error) {
        console.error("[WS] Erro ao enviar mensagem:", error.message);
      }
    });

    return true;
  } catch (error) {
    console.error("[WS] Falha no envio:", error.message);
    return false;
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
        client.send(payload);
      } catch (error) {
        console.error("[WS] Erro de transmissão:", error.message);
      }
    }
  }
}

function getUsers() {
  return [...clients.values()].map((user) => ({
    id: user.id,
    name: user.name,
    isFounder: user.isFounder
  }));
}

function broadcastUsers() {
  broadcast({
    type: "users",
    users: getUsers()
  });
}

function systemMessage(text) {
  broadcast({
    type: "system",
    text,
    time: Date.now()
  });
}

function secureEqual(firstValue, secondValue) {
  const first = Buffer.from(String(firstValue));
  const second = Buffer.from(String(secondValue));

  if (first.length !== second.length) {
    return false;
  }

  return crypto.timingSafeEqual(first, second);
}

function sendError(ws, text) {
  send(ws, {
    type: "error",
    text
  });
}

function closeClient(ws, code = 1000, reason = "Conexão encerrada") {
  try {
    if (
      ws.readyState === WebSocket.OPEN ||
      ws.readyState === WebSocket.CONNECTING
    ) {
      ws.close(code, reason);
    }
  } catch {
    // A conexão pode já estar encerrada.
  }
}

function removeClient(ws, announce = true) {
  const user = clients.get(ws);

  if (!user) return;

  clients.delete(ws);
  lastMessages.delete(ws);

  if (announce) {
    broadcastUsers();
    systemMessage(`${user.name} saiu do chat.`);
  }
}

// ==============================
// CONEXÃO WEBSOCKET
// ==============================

wss.on("connection", (ws) => {
  const id = crypto.randomUUID();

  let joined = false;
  let alive = true;

  ws.isAlive = true;

  ws.on("pong", () => {
    alive = true;
    ws.isAlive = true;
  });

  // ============================
  // RECEBIMENTO DE MENSAGENS
  // ============================

  ws.on("message", (buffer, isBinary) => {
    if (isBinary || buffer.length > 4096) {
      sendError(ws, "Mensagem inválida ou muito grande.");
      return;
    }

    let data;

    try {
      data = JSON.parse(buffer.toString("utf8"));
    } catch {
      sendError(ws, "Formato de mensagem inválido.");
      return;
    }

    if (
      !data ||
      typeof data !== "object" ||
      Array.isArray(data) ||
      typeof data.type !== "string"
    ) {
      sendError(ws, "Solicitação inválida.");
      return;
    }

    // ==========================
    // ENTRAR NO CHAT
    // ==========================

    if (data.type === "join") {
      if (joined) {
        sendError(ws, "Você já entrou no chat.");
        return;
      }

      // Limita tentativas de entrada por conexão.
      const now = Date.now();
      const previousAttempt = lastJoinAttempts.get(ws) || 0;

      if (now - previousAttempt < 1000) {
        sendError(ws, "Aguarde um instante antes de tentar novamente.");
        return;
      }

      lastJoinAttempts.set(ws, now);

      if (clients.size >= MAX_USERS) {
        sendError(ws, "O chat está cheio. Tente novamente mais tarde.");
        closeClient(ws, 1013, "Chat cheio");
        return;
      }

      const rawName = String(data.name || "").trim();
      const name = rawName.replace(/\s+/g, " ");

      if (
        name.length < 2 ||
        name.length > MAX_NAME ||
        !/^[\p{L}\p{N}_ -]+$/u.test(name)
      ) {
        sendError(
          ws,
          `Use um apelido válido de 2 a ${MAX_NAME} caracteres, sem símbolos especiais.`
        );
        return;
      }

      const normalizedName = normalizeName(name);
      const founderAttempt = normalizedName === FOUNDER_NAME;

      let isFounder = false;

      if (founderAttempt) {
        if (!FOUNDER_PASSWORD) {
          sendError(
            ws,
            "O acesso do fundador ainda não foi configurado no servidor."
          );
          return;
        }

        const suppliedPassword =
          typeof data.founderPassword === "string"
            ? data.founderPassword
            : "";

        if (!secureEqual(suppliedPassword, FOUNDER_PASSWORD)) {
          sendError(ws, "Senha do fundador incorreta.");
          return;
        }

        isFounder = true;
      } else if (
        typeof data.founderPassword === "string" &&
        data.founderPassword.length > 0
      ) {
        sendError(
          ws,
          "Para usar a senha de fundador, informe o apelido configurado para o fundador."
        );
        return;
      }

      const nameTaken = [...clients.values()].some(
        (user) => normalizeName(user.name) === normalizedName
      );

      if (nameTaken) {
        sendError(ws, "Esse apelido já está sendo usado.");
        return;
      }

      joined = true;

      clients.set(ws, {
        id,
        name,
        isFounder
      });

      send(ws, {
        type: "joined",
        id,
        name,
        isFounder
      });

      broadcastUsers();

      systemMessage(
        `${name} entrou no chat${isFounder ? " (fundador)" : ""}.`
      );

      console.log(
        `[ENTRADA] ${name}${isFounder ? " [FUNDADOR]" : ""} | Total: ${clients.size}`
      );

      return;
    }

    // Nenhum comando pode ser usado antes de entrar.
    const me = clients.get(ws);

    if (!joined || !me) {
      sendError(ws, "Entre no chat antes de realizar essa ação.");
      return;
    }

    // ==========================
    // EXPULSAR PARTICIPANTE
    // ==========================

    if (data.type === "kick") {
      if (!me.isFounder) {
        sendError(
          ws,
          "Somente o fundador pode remover participantes."
        );
        return;
      }

      const targetName = normalizeName(data.name);

      if (!targetName) {
        sendError(ws, "Informe o apelido do participante.");
        return;
      }

      const entry = [...clients.entries()].find(
        ([client, user]) =>
          client !== ws &&
          normalizeName(user.name) === targetName
      );

      if (!entry) {
        sendError(ws, "Participante não encontrado.");
        return;
      }

      const [victim, user] = entry;

      if (user.isFounder) {
        sendError(ws, "A conta do fundador não pode ser removida.");
        return;
      }

      // Notifica antes de fechar a conexão.
      send(victim, {
        type: "kicked",
        text: "Você foi removido do chat pelo fundador."
      });

      send(ws, {
        type: "admin_action",
        text: `${user.name} foi removido do chat.`
      });

      // Retira imediatamente o usuário da lista.
      clients.delete(victim);
      lastMessages.delete(victim);

      broadcastUsers();

      systemMessage(`${user.name} foi removido pelo fundador.`);

      console.log(
        `[MODERAÇÃO] ${user.name} removido por ${me.name}`
      );

      closeClient(victim, 4001, "Removido pelo fundador");

      return;
    }

    // ==========================
    // ENVIAR MENSAGEM
    // ==========================

    if (data.type === "message") {
      const now = Date.now();
      const previous = lastMessages.get(ws) || 0;

      if (now - previous < MESSAGE_INTERVAL) {
        sendError(
          ws,
          "Aguarde um instante antes de enviar outra mensagem."
        );
        return;
      }

      if (typeof data.text !== "string") {
        sendError(ws, "O conteúdo da mensagem é inválido.");
        return;
      }

      const text = data.text.trim();

      if (!text || text.length > MAX_TEXT) {
        sendError(
          ws,
          `A mensagem deve ter entre 1 e ${MAX_TEXT} caracteres.`
        );
        return;
      }

      lastMessages.set(ws, now);

      broadcast({
        type: "message",
        id: me.id,
        name: me.name,
        text,
        time: now,
        isFounder: me.isFounder
      });

      return;
    }

    sendError(ws, "Comando desconhecido.");
  });

  // ============================
  // ENCERRAMENTO DA CONEXÃO
  // ============================

  ws.on("close", () => {
    removeClient(ws, true);
    console.log(`[CONEXÃO] Cliente desconectado | Total: ${clients.size}`);
  });

  ws.on("error", (error) => {
    console.error("[WS] Erro de conexão:", error.message);
  });
});

// ==============================
// VERIFICAÇÃO DE CONEXÕES ATIVAS
// ==============================

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }

    ws.isAlive = false;

    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    }
  }
}, HEARTBEAT_INTERVAL);

wss.on("close", () => {
  clearInterval(heartbeat);
});

// ==============================
// ERROS DO SERVIDOR HTTP
// ==============================

server.on("error", (error) => {
  console.error("[HTTP] Erro:", error.message);

  if (error.code === "EADDRINUSE") {
    console.error(`[HTTP] A porta ${PORT} já está em uso.`);
  }
});

process.on("uncaughtException", (error) => {
  console.error("[FATAL] Erro inesperado:", error);
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  console.error("[PROMISE] Erro não tratado:", reason);
});

// ==============================
// INICIALIZAÇÃO
// ==============================

server.listen(PORT, "0.0.0.0", () => {
  console.log("");
  console.log("╔══════════════════════════════════════╗");
  console.log("║           ANON CHAT PRO              ║");
  console.log("║        SERVIDOR WEBSOCKET             ║");
  console.log("╚══════════════════════════════════════╝");
  console.log("");
  console.log(`  Status:             ONLINE`);
  console.log(`  Porta:              ${PORT}`);
  console.log(`  Limite de usuários: ${MAX_USERS}`);
  console.log(`  Limite de mensagem: ${MAX_TEXT} caracteres`);
  console.log(`  Nome do fundador:   ${FOUNDER_NAME}`);
  console.log(
    `  Senha configurada:  ${FOUNDER_PASSWORD ? "SIM" : "NÃO"}`
  );
  console.log(`  Health check:       /health`);
  console.log("");
  console.log("  ANON CHAT PRO iniciado com sucesso.");
  console.log("");
});
