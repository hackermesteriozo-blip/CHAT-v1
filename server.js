
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
  perMessageDeflate: false
});

app.disable("x-powered-by");
app.use(express.static(path.join(__dirname, "public")));

app.get("/health", (req, res) => {
  res.json({
    status: "online",
    service: "Anon Chat Pro"
  });
});

const PORT = process.env.PORT || 3000;
const MAX_USERS = 200;
const MAX_TEXT = 500;

const FOUNDER_NAME = (
  process.env.FOUNDER_NAME || "fundador"
).trim().toLocaleLowerCase("pt-BR");

const FOUNDER_PASSWORD =
  process.env.FOUNDER_PASSWORD || "";

const clients = new Map();
const lastMessages = new Map();

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

function users() {
  return [...clients.values()].map(user => ({
    id: user.id,
    name: user.name,
    isFounder: user.isFounder
  }));
}

function systemMessage(text) {
  broadcast({
    type: "system",
    text,
    time: Date.now()
  });
}

function secureEqual(a, b) {
  const first = Buffer.from(String(a));
  const second = Buffer.from(String(b));

  return (
    first.length === second.length &&
    crypto.timingSafeEqual(first, second)
  );
}

wss.on("connection", ws => {
  if (clients.size >= MAX_USERS) {
    send(ws, {
      type: "error",
      text: "Chat cheio. Tente mais tarde."
    });

    ws.close(1013, "Chat cheio");
    return;
  }

  const id = crypto.randomUUID();
  let joined = false;

  ws.on("message", (buffer, isBinary) => {
    if (isBinary || buffer.length > 4096) {
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

    // ENTRADA NO CHAT
    if (data.type === "join") {
      if (joined) return;

      const name = String(data.name || "")
        .trim()
        .replace(/\s+/g, " ")
        .slice(0, 20);

      if (
        name.length < 2 ||
        !/^[\p{L}\p{N}_ -]+$/u.test(name)
      ) {
        send(ws, {
          type: "error",
          text: "Use apelido de 2 a 20 caracteres."
        });
        return;
      }

      const founderAttempt =
        name.toLocaleLowerCase("pt-BR") ===
        FOUNDER_NAME;

      let isFounder = false;

      if (founderAttempt) {
        if (!FOUNDER_PASSWORD) {
          send(ws, {
            type: "error",
            text:
              "Configure FOUNDER_PASSWORD nas variáveis do Render."
          });
          return;
        }

        if (
          !secureEqual(
            data.founderPassword || "",
            FOUNDER_PASSWORD
          )
        ) {
          send(ws, {
            type: "error",
            text: "Senha do fundador incorreta."
          });
          return;
        }

        isFounder = true;
      } else if (data.founderPassword) {
        send(ws, {
          type: "error",
          text: "Senha inválida para este apelido."
        });
        return;
      }

      const nameTaken = [...clients.values()].some(
        user =>
          user.name.toLocaleLowerCase("pt-BR") ===
          name.toLocaleLowerCase("pt-BR")
      );

      if (nameTaken) {
        send(ws, {
          type: "error",
          text: "Esse apelido já está sendo usado."
        });
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

      broadcast({
        type: "users",
        users: users()
      });

      systemMessage(
        `${name} entrou no chat${
          isFounder ? " (fundador)" : ""
        }.`
      );

      return;
    }

    const me = clients.get(ws);

    if (!joined || !me) return;

    // EXPULSAR PARTICIPANTE
    if (data.type === "kick") {
      if (!me.isFounder) {
        send(ws, {
          type: "error",
          text:
            "Somente o fundador pode expulsar participantes."
        });
        return;
      }

      const target = String(data.name || "")
        .trim()
        .toLocaleLowerCase("pt-BR");

      const entry = [...clients.entries()].find(
        ([client, user]) =>
          client !== ws &&
          user.name.toLocaleLowerCase("pt-BR") === target
      );

      if (!entry) {
        send(ws, {
          type: "error",
          text: "Participante não encontrado."
        });
        return;
      }

      const [victim, user] = entry;

      if (user.isFounder) {
        send(ws, {
          type: "error",
          text: "A conta do fundador não pode ser expulsa."
        });
        return;
      }

      send(victim, {
        type: "kicked",
        text: "Você foi removido do chat pelo fundador."
      });

      send(ws, {
        type: "admin_action",
        text: `${user.name} foi expulso.`
      });

      clients.delete(victim);
      lastMessages.delete(victim);

      try {
        victim.close(4001, "Expulso pelo fundador");
      } catch {}

      broadcast({
        type: "users",
        users: users()
      });

      systemMessage(
        `${user.name} foi removido pelo fundador.`
      );

      return;
    }

    // ENVIO DE MENSAGENS
    if (data.type !== "message") return;

    const now = Date.now();
    const previous = lastMessages.get(ws) || 0;

    if (now - previous < 800) {
      send(ws, {
        type: "error",
        text:
          "Aguarde um instante antes de enviar outra mensagem."
      });
      return;
    }

    const text = String(data.text || "").trim();

    if (!text || text.length > MAX_TEXT) {
      send(ws, {
        type: "error",
        text:
          "A mensagem deve ter entre 1 e 500 caracteres."
      });
      return;
    }

    lastMessages.set(ws, now);

    broadcast({
      type: "message",
      id,
      name: me.name,
      text,
      time: now,
      isFounder: me.isFounder
    });
  });

  ws.on("close", () => {
    const user = clients.get(ws);

    if (!user) return;

    clients.delete(ws);
    lastMessages.delete(ws);

    broadcast({
      type: "users",
      users: users()
    });

    systemMessage(`${user.name} saiu do chat.`);
  });

  ws.on("error", () => {});
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("================================");
  console.log("        ANON CHAT PRO");
  console.log("================================");
  console.log(`Servidor ativo na porta ${PORT}`);
  console.log(`Fundador: ${FOUNDER_NAME}`);
  console.log(
    `Senha configurada: ${Boolean(FOUNDER_PASSWORD)}`
  );
});
