
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({
  server,
  maxPayload: 2048,
  perMessageDeflate: false
});

app.disable("x-powered-by");
app.use(express.static(__dirname));

app.get("/health", (req, res) => {
  res.json({
    status: "online",
    service: "Anon Chat"
  });
});

const PORT = process.env.PORT || 3000;
const clients = new Map();
const lastMessages = new Map();
const MAX_USERS = 200;
const MAX_MESSAGE_LENGTH = 500;
const NAME_PATTERN = /^[\p{L}\p{N}_ -]+$/u;

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(data, exclude = null) {
  const payload = JSON.stringify(data);

  for (const client of wss.clients) {
    if (
      client !== exclude &&
      client.readyState === WebSocket.OPEN
    ) {
      client.send(payload);
    }
  }
}

function userList() {
  return [...clients.values()].map(user => ({
    id: user.id,
    name: user.name
  }));
}

function systemMessage(text) {
  broadcast({
    type: "system",
    text,
    time: Date.now()
  });
}

wss.on("connection", (ws) => {
  if (clients.size >= MAX_USERS) {
    send(ws, {
      type: "error",
      text: "O chat está cheio. Tente novamente mais tarde."
    });
    ws.close(1013, "Chat cheio");
    return;
  }

  const id = require("crypto").randomUUID();
  let joined = false;

  ws.on("message", (buffer, isBinary) => {
    if (isBinary || buffer.length > 2048) {
      return;
    }

    let data;

    try {
      data = JSON.parse(buffer.toString());
    } catch {
      return;
    }

    if (!data || typeof data.type !== "string") {
      return;
    }

    if (data.type === "join") {
      if (joined) return;

      const name = String(data.name || "")
        .trim()
        .replace(/\s+/g, " ")
        .slice(0, 20);

      if (
        name.length < 2 ||
        !NAME_PATTERN.test(name)
      ) {
        send(ws, {
          type: "error",
          text: "Use um apelido de 2 a 20 caracteres."
        });
        return;
      }

      const usedNames = [...clients.values()]
        .map(user => user.name.toLowerCase());

      if (usedNames.includes(name.toLowerCase())) {
        send(ws, {
          type: "error",
          text: "Esse apelido já está sendo usado."
        });
        return;
      }

      joined = true;

      clients.set(ws, {
        id,
        name
      });

      send(ws, {
        type: "joined",
        id,
        name
      });

      send(ws, {
        type: "users",
        users: userList()
      });

      broadcast({
        type: "users",
        users: userList()
      });

      systemMessage(`${name} entrou no chat.`);
      return;
    }

    if (data.type !== "message" || !joined) {
      return;
    }

    const now = Date.now();
    const previous = lastMessages.get(ws) || 0;

    if (now - previous < 1000) {
      send(ws, {
        type: "error",
        text: "Aguarde um instante antes de enviar outra mensagem."
      });
      return;
    }

    const text = String(data.text || "").trim();

    if (!text || text.length > MAX_MESSAGE_LENGTH) {
      send(ws, {
        type: "error",
        text: "A mensagem deve ter entre 1 e 500 caracteres."
      });
      return;
    }

    lastMessages.set(ws, now);

    broadcast({
      type: "message",
      id,
      name: clients.get(ws).name,
      text,
      time: now
    });
  });

  ws.on("close", () => {
    const user = clients.get(ws);

    if (user) {
      clients.delete(ws);
      lastMessages.delete(ws);

      broadcast({
        type: "users",
        users: userList()
      });

      systemMessage(`${user.name} saiu do chat.`);
    }
  });

  ws.on("error", () => {});
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("--------------------------------");
  console.log("       ANON CHAT PUBLICO");
  console.log("--------------------------------");
  console.log(`Servidor ativo na porta ${PORT}`);
});
