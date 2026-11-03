// 데모 채팅 화면. 빌드 단계 없이 브라우저 기본 기능(EventSource, fetch)만 쓴다.
(() => {
  const $ = (id) => document.getElementById(id);
  const list = $("messages");
  const LEDGER_KEY = "chat.ledger.v1";

  // ---- 이 브라우저가 보낸 메시지의 장부 (유실 검증용) ----
  // client_msg_id -> { body, acked, seen }
  const ledger = new Map(JSON.parse(localStorage.getItem(LEDGER_KEY) || "[]"));
  const saveLedger = () => localStorage.setItem(LEDGER_KEY, JSON.stringify([...ledger]));

  const author = $("author");
  author.value = localStorage.getItem("chat.author") || `guest-${Math.random().toString(36).slice(2, 6)}`;
  author.addEventListener("change", () => localStorage.setItem("chat.author", author.value.trim()));

  // crypto.randomUUID는 HTTPS에서만 쓸 수 있어서, 사설 IP로 접속하는 경우를 위해 직접 만든다
  const newId = () => {
    const b = crypto.getRandomValues(new Uint8Array(16));
    return [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
  };

  let reconnects = 0;
  let pending = 0;

  function renderCounters() {
    let acked = 0, seen = 0;
    for (const e of ledger.values()) { if (e.acked) acked++; if (e.seen) seen++; }
    $("v-sent").textContent = ledger.size;
    $("v-acked").textContent = acked;
    $("v-seen").textContent = seen;
    $("v-pending").textContent = pending;
    $("v-reconnects").textContent = reconnects;
  }

  // ---- 메시지 표시 ----
  const rendered = new Map(); // client_msg_id -> li
  const cls = (s) => String(s).replace(/[^a-z0-9-]/gi, "");

  function renderMessage(m, { isPending = false } = {}) {
    let li = rendered.get(m.client_msg_id);
    const isNew = !li;
    if (isNew) {
      li = document.createElement("li");
      rendered.set(m.client_msg_id, li);
    }
    li.className = isPending ? "pending" : `db-${cls(m.db_node)}`;
    li.replaceChildren();

    const meta = document.createElement("span");
    meta.className = "meta";
    meta.textContent = isPending ? "[전송 중]" : `[${m.app_env} · ${m.db_node}]`;
    const who = document.createElement("span");
    who.className = "author";
    who.textContent = m.author;
    const body = document.createElement("span");
    body.textContent = m.body;
    li.append(meta, who, body);

    const mine = ledger.get(m.client_msg_id);
    if (mine) {
      const tick = document.createElement("span");
      tick.className = "tick";
      tick.textContent = mine.seen ? "✓✓" : mine.acked ? "✓" : "…";
      li.append(tick);
    }

    if (isNew) {
      const atBottom = list.scrollHeight - list.scrollTop - list.clientHeight < 80;
      list.append(li);
      if (atBottom) list.scrollTop = list.scrollHeight;
    }
  }

  // ---- 수신: SSE ----
  // EventSource는 네트워크 오류면 Last-Event-ID를 붙여 스스로 재접속하지만,
  // 서버가 200이 아닌 상태(예: 503 draining)를 돌려주면 영구히 닫힌다. 그 경우 직접 다시 연다.
  let lastId = 0;
  let es;
  const conn = $("conn");

  function setConn(text, ok) {
    conn.textContent = text;
    conn.classList.toggle("down", !ok);
  }

  function onStatus(ev) {
    const s = JSON.parse(ev.data);
    setConn(`연결됨 · 앱 ${s.app_env} · DB ${s.db_node}`, true);
  }

  function connect() {
    es = new EventSource(lastId ? `/api/stream?after=${lastId}` : "/api/stream");
    es.addEventListener("hello", onStatus);
    es.addEventListener("status", onStatus);
    es.addEventListener("bye", () => setConn("서버 교체 중… 재접속합니다", false));
    es.addEventListener("message", (ev) => {
      const m = JSON.parse(ev.data);
      if (m.id > lastId) lastId = m.id;
      const mine = ledger.get(m.client_msg_id);
      if (mine && !mine.seen) { mine.seen = true; mine.acked = true; saveLedger(); }
      renderMessage(m);
      renderCounters();
    });
    es.onerror = () => {
      setConn("연결 끊김 · 재접속 중…", false);
      reconnects++;
      renderCounters();
      if (es.readyState === EventSource.CLOSED) {
        setTimeout(connect, 1000);
      }
    };
  }
  connect();

  // ---- 전송: 같은 client_msg_id로 성공할 때까지 재전송 (서버가 중복을 막는다) ----
  async function send(entryId, payload) {
    let delay = 300;
    for (;;) {
      try {
        const res = await fetch("/api/messages", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (res.ok) {
          const m = await res.json();
          const e = ledger.get(entryId);
          e.acked = true;
          saveLedger();
          renderMessage(m);
          return;
        }
        if (res.status >= 400 && res.status < 500) {
          alertLine(`전송 거부: ${(await res.json()).error}`);
          return;
        }
      } catch (_) { /* 네트워크 오류: 재시도 */ }
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, 3000);
    }
  }

  function alertLine(text) {
    const li = document.createElement("li");
    li.textContent = text;
    li.className = "pending";
    list.append(li);
  }

  $("composer").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const body = $("body").value.trim();
    const name = author.value.trim() || "guest";
    if (!body) return;
    $("body").value = "";
    localStorage.setItem("chat.author", name);

    const id = newId();
    ledger.set(id, { body, acked: false, seen: false });
    saveLedger();
    renderMessage({ client_msg_id: id, author: name, body }, { isPending: true });
    pending++;
    renderCounters();
    await send(id, { client_msg_id: id, author: name, body });
    pending--;
    renderCounters();
  });

  // ---- 검증 ----
  $("verify").addEventListener("click", async () => {
    const ids = [...ledger.keys()];
    const out = $("v-result");
    if (!ids.length) { out.textContent = "보낸 메시지가 없습니다."; return; }
    try {
      const res = await fetch("/api/verify", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ client_msg_ids: ids }),
      });
      const r = await res.json();
      const lost = r.missing.length;
      out.className = lost ? "bad" : "ok";
      out.textContent = `보낸 ${r.checked}건 중 DB에 없음 ${lost}건`;
    } catch (e) {
      out.className = "bad";
      out.textContent = `대조 실패: ${e}`;
    }
  });

  async function refreshStats() {
    try {
      const st = await (await fetch("/api/stats")).json();
      const dl = $("stats");
      dl.replaceChildren();
      const row = (k, v) => {
        const dt = document.createElement("dt"); dt.textContent = k;
        const dd = document.createElement("dd"); dd.textContent = v;
        dl.append(dt, dd);
      };
      row("합계", st.total);
      for (const [k, v] of Object.entries(st.by_db_node).sort()) row(`DB ${k}`, v);
      for (const [k, v] of Object.entries(st.by_app_env).sort()) row(`앱 ${k}`, v);
    } catch (_) { /* 전환 중에는 잠시 실패할 수 있다 */ }
  }
  setInterval(refreshStats, 3000);
  refreshStats();
  renderCounters();
})();
